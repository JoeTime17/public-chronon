package ai.chronon.integrations.cloud_azure

import ai.chronon.api.{GroupBy, MetaData, TilingUtils}
import ai.chronon.api.Extensions.GroupByOps
import ai.chronon.integrations.cloud_azure.CosmosKVStore._
import ai.chronon.integrations.cloud_azure.CosmosKVStoreConstants._
import ai.chronon.online.KVStore
import ai.chronon.online.KVStore._
import ai.chronon.online.metrics.Metrics
import com.azure.cosmos._
import com.azure.cosmos.models._
import com.azure.cosmos.util.CosmosPagedFlux
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.collection.concurrent.TrieMap
import scala.concurrent.Future
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}
import java.util.UUID

/** Azure Cosmos DB KVStore implementation. We store a few kinds of data in our KV store:
  * 1) Entity data - An example is thrift serialized Groupby / Join configs. If entities are updated / rewritten, we
  * serve the latest version.
  * 2) Daily batch data - Daily batch IRs written out by GroupByUpload jobs for feature fetching.
  * 3) Streaming tiles for feature fetching.
  *
  * We have multi use-case tables for the _BATCH and _STREAMING feature datasets.
  * To ensure that data from different groupBys are isolated from each other, we use hierarchical partition keys:
  * - For batch data: 2-level partition key: [dataset, keyHash]
  * - For streaming tiles: 2-level partition key: [dataset, keyHashDay]
  *
  * In case of time series data that is likely to see many data points per day (e.g. streaming tiles), we
  * bucket the data by day. This is done by including the day timestamp in the partition key as well as the row key.
  * Within a day bucket, data points are stored as individual documents with the timestamp of the data point. This allows us to query
  * time ranges within a day without scanning large amounts of data. This isn't the ideal time-series data querying, but
  * seemingly the best we can do as CosmosDB doesn't support this natively.
  */
class CosmosKVStoreImpl(
    database: CosmosAsyncDatabase,
    conf: Map[String, String] = Map.empty
) extends KVStore {

  @transient override lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  // Container cache from container name to CosmosAsyncContainer
  private val containerCache = new TrieMap[String, CosmosAsyncContainer]()

  protected val metricsContext: Metrics.Context =
    Metrics.Context(Metrics.Environment.KVStore).withSuffix("cosmos")

  protected val containerToContext = new TrieMap[String, Metrics.Context]()

  protected val enableTtl: Boolean = true
  private val dataTTLSeconds = conf.getOrElse(PropTTLSeconds, DefaultDataTTLSeconds.toString).toInt
  private val slowOperationThresholdMs = conf.getOrElse("cosmos.slow_operation_threshold_ms", "1000").toLong

  private def logDiagnostics(
      operation: String,
      diagnostics: com.azure.cosmos.CosmosDiagnostics,
      requestId: String,
      additionalContext: Map[String, Any] = Map.empty
  ): Unit = {
    val contextStr = if (additionalContext.nonEmpty) {
      additionalContext.map { case (k, v) => s"$k=$v" }.mkString(", ")
    } else ""

    logger.info(s"[$operation] [requestId=$requestId] $contextStr")
    logger.info(s"[$operation] [requestId=$requestId] Diagnostics: ${diagnostics.toString}")
  }

  private def logCosmosException(
      operation: String,
      exception: CosmosException,
      requestId: String,
      additionalContext: Map[String, Any] = Map.empty
  ): Unit = {
    val contextStr = additionalContext.map { case (k, v) => s"$k=$v" }.mkString(", ")

    logger.error(
      s"[$operation] [requestId=$requestId] CosmosException: statusCode=${exception.getStatusCode}, " +
        s"subStatusCode=${exception.getSubStatusCode}, message=${exception.getMessage}, $contextStr"
    )
    logger.error(s"[$operation] [requestId=$requestId] Diagnostics: ${exception.getDiagnostics.toString}")
    logger.debug(s"[$operation] [requestId=$requestId] Full exception:", exception)
  }

  /** CosmosDB documents store data as Json. For binary data, we need to extract the byte array
    * Cosmos may store as String (Base64) or Array[Byte] depending on SDK version
    */
  private def extractBytes(value: Any): Array[Byte] = {
    value match {
      case bytes: Array[Byte] => bytes
      case str: String        => java.util.Base64.getDecoder.decode(str)
      case _                  => throw new IllegalArgumentException(s"Cannot convert ${value.getClass} to byte array")
    }
  }

  override def create(dataset: String): Unit = create(dataset, Map.empty)

  override def create(dataset: String, props: Map[String, Any]): Unit = {
    val requestId = UUID.randomUUID().toString
    val containerName = mapDatasetToContainer(dataset)
    val tableType = getTableType(dataset)

    logger.info(s"[create] [requestId=$requestId] container=$containerName, dataset=$dataset, tableType=$tableType")

    // Check if emulator mode via props or endpoint detection
    val isEmulator = props.get(PropEmulatorMode).exists(_.toString.toBoolean) ||
      CosmosKVStoreConstants.isEmulator(
        sys.env.getOrElse(CosmosKVStoreConstants.EnvCosmosEndpoint,
                          props.getOrElse(CosmosKVStoreConstants.EnvCosmosEndpoint, "").toString)
      )

    if (isEmulator) {
      createContainerTestEmulator(containerName, tableType, requestId)
    } else {
      createContainerProdDB(containerName, tableType, requestId, props)
    }
  }

  private def createContainerProdDB(
      containerName: String,
      tableType: TableType,
      requestId: String,
      props: Map[String, Any]
  ): Unit = {
    try {
      val partitionKeyDef = tableType match {
        case BatchTable =>
          val paths = new java.util.ArrayList[String]()
          paths.add("/dataset")
          paths.add("/keyHash")
          new PartitionKeyDefinition()
            .setPaths(paths)
            .setVersion(PartitionKeyDefinitionVersion.V2)
            .setKind(PartitionKind.MULTI_HASH)

        case StreamingTable =>
          val paths = new java.util.ArrayList[String]()
          paths.add("/dataset")
          paths.add("/keyHashDay")
          new PartitionKeyDefinition()
            .setPaths(paths)
            .setVersion(PartitionKeyDefinitionVersion.V2)
            .setKind(PartitionKind.MULTI_HASH)

        case _ =>
          val paths = new java.util.ArrayList[String]()
          paths.add("/keyHash")
          new PartitionKeyDefinition()
            .setPaths(paths)
            .setVersion(PartitionKeyDefinitionVersion.V2)
            .setKind(PartitionKind.HASH)
      }

      val containerProperties = new CosmosContainerProperties(containerName, partitionKeyDef)
      if (enableTtl) {
        containerProperties.setDefaultTimeToLiveInSeconds(dataTTLSeconds)
      }

      val throughputProps = props.get(PropThroughput) match {
        case Some(manualRU: Int) =>
          ThroughputProperties.createManualThroughput(manualRU)
        case _ =>
          val maxRU = props.getOrElse(PropAutoscale, DefaultAutoscaleMaxRU).asInstanceOf[Int]
          ThroughputProperties.createAutoscaledThroughput(maxRU)
      }

      submitCreateContainer(containerName, containerProperties, throughputProps, requestId)
    } catch {
      case e: CosmosException =>
        logCosmosException("create", e, requestId, Map("container" -> containerName))
        metricsContext.increment("create.failures", Map("exception" -> e.getClass.getName))
        throw e
      case e: Exception =>
        logger.error(s"[create] [requestId=$requestId] Unexpected error: container=$containerName", e)
        metricsContext.increment("create.failures", Map("exception" -> e.getClass.getName))
        throw e
    }
  }

  // Create container with emulator-compatible settings (V1 partition keys, manual throughput, no TTL)
  private def createContainerTestEmulator(
      containerName: String,
      tableType: TableType,
      requestId: String
  ): Unit = {
    try {
      // Use same partition key paths as production, but without V2/kind settings
      val partitionKeyDef = tableType match {
        case BatchTable =>
          val paths = new java.util.ArrayList[String]()
          paths.add("/dataset")
          paths.add("/keyHash")
          new PartitionKeyDefinition()
            .setPaths(paths)
            .setKind(com.azure.cosmos.models.PartitionKind.MULTI_HASH) // Hierarchical partition key

        case StreamingTable =>
          val paths = new java.util.ArrayList[String]()
          paths.add("/dataset")
          paths.add("/keyHashDay")
          new PartitionKeyDefinition()
            .setPaths(paths)
            .setKind(com.azure.cosmos.models.PartitionKind.MULTI_HASH) // Hierarchical partition key

        case _ =>
          val paths = new java.util.ArrayList[String]()
          paths.add("/keyHash")
          new PartitionKeyDefinition().setPaths(paths) // Single partition key, no need to set kind
      }

      val containerProperties = new CosmosContainerProperties(containerName, partitionKeyDef)
      // Fixed manual throughput for emulator
      val throughputProps = ThroughputProperties.createManualThroughput(400)

      submitCreateContainer(containerName, containerProperties, throughputProps, requestId)
    } catch {
      case e: CosmosException =>
        logCosmosException("create", e, requestId, Map("container" -> containerName))
        metricsContext.increment("create.failures", Map("exception" -> e.getClass.getName))
        throw e
      case e: Exception =>
        logger.error(s"[create] [requestId=$requestId] Unexpected error: container=$containerName", e)
        metricsContext.increment("create.failures", Map("exception" -> e.getClass.getName))
        throw e
    }
  }

  private def submitCreateContainer(
      containerName: String,
      containerProperties: CosmosContainerProperties,
      throughputProps: ThroughputProperties,
      requestId: String
  ): Unit = {
    val startTime = System.currentTimeMillis()
    val mono = database.createContainerIfNotExists(containerProperties, throughputProps)
    val future = monoToScalaFuture(mono)

    val response = Await.result(future, 120.seconds)
    val duration = System.currentTimeMillis() - startTime

    logDiagnostics("create",
                   response.getDiagnostics,
                   requestId,
                   Map(
                     "container" -> containerName,
                     "durationMs" -> duration
                   ))

    logger.info(s"[create] [requestId=$requestId] Success in ${duration}ms")
    metricsContext.increment("create.successes")
    metricsContext.distribution("create.latency", duration)
  }

  private def getOrCreateContainerRef(containerName: String): CosmosAsyncContainer = {
    containerCache.getOrElseUpdate(containerName, {
                                     database.getContainer(containerName)
                                   })
  }

  private def monoToScalaFuture[T](mono: reactor.core.publisher.Mono[T]): Future[T] = {
    val promise = scala.concurrent.Promise[T]()
    mono.subscribe(
      new reactor.core.publisher.BaseSubscriber[T]() {
        override def hookOnNext(value: T): Unit = {
          promise.success(value)
        }
        override def hookOnError(throwable: Throwable): Unit = {
          promise.failure(throwable)
        }
        override def hookOnComplete(): Unit = {
          if (!promise.isCompleted) {
            promise.failure(new NoSuchElementException("Mono completed without emitting a value"))
          }
        }
      }
    )
    promise.future
  }

  override def multiGet(requests: Seq[GetRequest]): Future[Seq[GetResponse]] = {
    val batchRequestId = UUID.randomUUID().toString
    logger.debug(s"[multiGet] [requestId=$batchRequestId] Starting with ${requests.size} requests")

    val requestGroups = requests.groupBy { req =>
      val containerName = mapDatasetToContainer(req.dataset)
      val tableType = getTableType(req.dataset)
      tableType match {
        case StreamingTable =>
          (containerName, req.dataset, req.startTsMillis, req.endTsMillis)
        case _ =>
          (containerName, req.dataset, None, None)
      }
    }

    logger.debug(s"[multiGet] [requestId=$batchRequestId] Grouped into ${requestGroups.size} batches")

    val groupFutures = requestGroups.zipWithIndex.map {
      case (((containerName, dataset, startTs, endTs), groupRequests), idx) =>
        val groupRequestId = s"$batchRequestId-g$idx"
        logger.debug(
          s"[multiGet] [requestId=$groupRequestId] container=$containerName, dataset=$dataset, requests=${groupRequests.size}")
        readDocumentsMultiGet(containerName, dataset, groupRequests, startTs, endTs, groupRequestId)
    }.toSeq

    Future.sequence(groupFutures).map { results =>
      logger.debug(s"[multiGet] [requestId=$batchRequestId] Completed successfully")
      results.flatten
    }
  }

  private def readDocumentsMultiGet(
      containerName: String,
      dataset: String,
      requests: Seq[GetRequest],
      startTsMillis: Option[Long],
      endTsMillis: Option[Long],
      requestId: String
  ): Future[Seq[GetResponse]] = {

    val container = getOrCreateContainerRef(containerName)
    val datasetMetricsContext = containerToContext.getOrElseUpdate(
      containerName,
      metricsContext.copy(dataset = containerName)
    )
    val tableType = getTableType(dataset)
    val startTime = System.currentTimeMillis()

    logger.debug(s"[multiGet] [requestId=$requestId] tableType=$tableType, requests=${requests.size}")

    val responseFutures: Seq[Future[GetResponse]] = tableType match {
      case MetadataTable =>
        requests.zipWithIndex.map { case (request, idx) =>
          val itemRequestId = s"$requestId-i$idx"
          val keyHash = buildKeyHash(request.keyBytes)
          val docId = buildBatchDocumentId(request.dataset, keyHash)

          val partitionKey = new PartitionKeyBuilder()
            .add(keyHash)
            .build()

          val itemStartTime = System.currentTimeMillis()
          logger.debug(s"[multiGet] [requestId=$itemRequestId] Reading metadata: docId=$docId")

          monoToScalaFuture(
            container
              .readItem(docId, partitionKey, classOf[java.util.Map[String, Object]])
          ).map { response =>
            val duration = System.currentTimeMillis() - itemStartTime
            val doc = response.getItem
            val valueBytes = extractBytes(doc.get("valueBytes"))
            val tsMillis = doc.get("tsMillis").asInstanceOf[Number].longValue()

            if (duration > slowOperationThresholdMs) {
              logger.warn(s"[multiGet] [requestId=$itemRequestId] Slow read: ${duration}ms")
              logDiagnostics("multiGet.slow",
                             response.getDiagnostics,
                             itemRequestId,
                             Map(
                               "durationMs" -> duration,
                               "docId" -> docId
                             ))
            }

            GetResponse(request, Success(Seq(TimedValue(valueBytes, tsMillis))))
          }.recover {
            case e: CosmosException if e.getStatusCode == 404 =>
              logger.debug(s"[multiGet] [requestId=$itemRequestId] Not found: docId=$docId")
              GetResponse(request, Success(Seq.empty))
            case e: CosmosException =>
              logCosmosException("multiGet", e, itemRequestId, Map("docId" -> docId, "keyHash" -> keyHash))
              GetResponse(request, Failure(e))
            case e: Exception =>
              logger.error(s"[multiGet] [requestId=$itemRequestId] Unexpected error: docId=$docId", e)
              GetResponse(request, Failure(e))
          }
        }

      case BatchTable =>
        requests.zipWithIndex.map { case (request, idx) =>
          val itemRequestId = s"$requestId-i$idx"
          val keyHash = buildKeyHash(request.keyBytes)
          val docId = buildBatchDocumentId(request.dataset, keyHash)

          val partitionKey = new PartitionKeyBuilder()
            .add(request.dataset)
            .add(keyHash)
            .build()

          val itemStartTime = System.currentTimeMillis()
          logger.debug(s"[multiGet] [requestId=$itemRequestId] Reading batch: docId=$docId, dataset=${request.dataset}")

          monoToScalaFuture(
            container
              .readItem(docId, partitionKey, classOf[java.util.Map[String, Object]])
          ).map { response =>
            val duration = System.currentTimeMillis() - itemStartTime
            val doc = response.getItem
            val valueBytes = extractBytes(doc.get("valueBytes"))
            val tsMillis = doc.get("tsMillis").asInstanceOf[Number].longValue()

            if (duration > slowOperationThresholdMs) {
              logger.warn(s"[multiGet] [requestId=$itemRequestId] Slow read: ${duration}ms")
              logDiagnostics("multiGet.slow",
                             response.getDiagnostics,
                             itemRequestId,
                             Map(
                               "durationMs" -> duration,
                               "docId" -> docId
                             ))
            }

            GetResponse(request, Success(Seq(TimedValue(valueBytes, tsMillis))))
          }.recover {
            case e: CosmosException if e.getStatusCode == 404 =>
              logger.debug(s"[multiGet] [requestId=$itemRequestId] Not found: docId=$docId")
              GetResponse(request, Success(Seq.empty))
            case e: CosmosException =>
              logCosmosException("multiGet",
                                 e,
                                 itemRequestId,
                                 Map("docId" -> docId, "dataset" -> request.dataset, "keyHash" -> keyHash))
              GetResponse(request, Failure(e))
            case e: Exception =>
              logger.error(s"[multiGet] [requestId=$itemRequestId] Unexpected error: docId=$docId", e)
              GetResponse(request, Failure(e))
          }
        }

      case StreamingTable if startTsMillis.isDefined =>
        val reqStartTs = startTsMillis.get
        val endTs = endTsMillis.getOrElse(System.currentTimeMillis())

        requests.zipWithIndex.map { case (request, idx) =>
          val itemRequestId = s"$requestId-i$idx"
          val tileKey = TilingUtils.deserializeTileKey(request.keyBytes)
          val baseKeyBytes = tileKey.keyBytes.asScala.map(_.toByte).toArray
          val keyHash = buildKeyHash(baseKeyBytes)
          val tileSizeMs = tileKey.tileSizeMillis

          logger.debug(
            s"[multiGet] [requestId=$itemRequestId] Querying streaming tiles: dataset=${request.dataset}, keyHash=$keyHash, tileSizeMs=$tileSizeMs")

          queryTimeSeriesDataAsync(container,
                                   request.dataset,
                                   keyHash,
                                   reqStartTs,
                                   endTs,
                                   Some(tileSizeMs),
                                   itemRequestId)
            .map {
              case Success(timedValues) =>
                logger.debug(
                  s"[multiGet] [requestId=$itemRequestId] Retrieved ${timedValues.size} streaming tile values")
                GetResponse(request, Success(timedValues))
              case Failure(e) =>
                logger.error(s"[multiGet] [requestId=$itemRequestId] Error querying streaming tiles", e)
                GetResponse(request, Failure(e))
            }
        }

      case _ =>
        logger.warn(s"[multiGet] [requestId=$requestId] Unsupported table type: $tableType, returning empty results")
        requests.map(req =>
          Future.successful(
            GetResponse(req, Failure(new IllegalArgumentException(s"Unsupported table type: $tableType")))))
    }

    Future
      .sequence(responseFutures)
      .map { responses =>
        val duration = System.currentTimeMillis() - startTime
        datasetMetricsContext.distribution("multiGet.latency", duration)
        datasetMetricsContext.increment("multiGet.successes")

        if (duration > slowOperationThresholdMs) {
          logger.warn(s"[multiGet] [requestId=$requestId] Slow batch: ${duration}ms for ${requests.size} requests")
        } else {
          logger.debug(s"[multiGet] [requestId=$requestId] Completed in ${duration}ms")
        }

        responses
      }
      .recover { case e: Exception =>
        val duration = System.currentTimeMillis() - startTime
        logger.error(s"[multiGet] [requestId=$requestId] Failed after ${duration}ms for container $containerName", e)
        datasetMetricsContext.increment("multiGet.cosmos_errors", Map("exception" -> e.getClass.getName))
        requests.map(req => GetResponse(req, Failure(e)))
      }
  }

  private def queryTimeSeriesDataAsync(
      container: CosmosAsyncContainer,
      dataset: String,
      keyHash: String,
      startTs: Long,
      endTs: Long,
      maybeTileSize: Option[Long],
      requestId: String
  ): Future[Try[Seq[TimedValue]]] = {

    val millisPerDay = 86400000L
    val startDay = roundToDay(startTs)
    val endDay = roundToDay(endTs)

    val dayRange = startDay to endDay by millisPerDay
    logger.debug(
      s"[queryTimeSeries] [requestId=$requestId] Querying ${dayRange.size} days: dataset=$dataset, keyHash=$keyHash, startTs=$startTs, endTs=$endTs")

    val dayFutures = dayRange.zipWithIndex.map { case (dayTs, dayIdx) =>
      val dayRequestId = s"$requestId-day$dayIdx"
      val keyHashDay = s"${keyHash}_$dayTs"

      val query = maybeTileSize match {
        case Some(tileSize) =>
          """SELECT c.valueBytes, c.tsMillis
            |FROM c
            |WHERE c.dataset = @dataset
            |  AND c.keyHashDay = @keyHashDay
            |  AND c.tsMillis >= @startTs
            |  AND c.tsMillis <= @endTs
            |  AND c.tileSizeMs = @tileSize""".stripMargin
        case None =>
          """SELECT c.valueBytes, c.tsMillis
            |FROM c
            |WHERE c.dataset = @dataset
            |  AND c.keyHashDay = @keyHashDay
            |  AND c.tsMillis >= @startTs
            |  AND c.tsMillis <= @endTs""".stripMargin
      }

      val querySpec = new SqlQuerySpec(query)
      querySpec.setParameters(new java.util.ArrayList[SqlParameter]())
      querySpec.getParameters.add(new SqlParameter("@dataset", dataset))
      querySpec.getParameters.add(new SqlParameter("@keyHashDay", keyHashDay))
      querySpec.getParameters.add(new SqlParameter("@startTs", startTs))
      querySpec.getParameters.add(new SqlParameter("@endTs", endTs))

      maybeTileSize.foreach { ts =>
        querySpec.getParameters.add(new SqlParameter("@tileSize", ts))
      }

      val partitionKey = new PartitionKeyBuilder()
        .add(dataset)
        .add(keyHashDay)
        .build()

      val options = new CosmosQueryRequestOptions()
      options.setPartitionKey(partitionKey)

      val flux: CosmosPagedFlux[java.util.Map[String, Object]] =
        container.queryItems(querySpec, options, classOf[java.util.Map[String, Object]])

      val queryStartTime = System.currentTimeMillis()
      logger.debug(
        s"[queryTimeSeries] [requestId=$dayRequestId] Executing query for day $dayTs, keyHashDay=$keyHashDay")

      monoToScalaFuture(flux.collectList())
        .map { javaList =>
          val duration = System.currentTimeMillis() - queryStartTime
          val resultCount = javaList.size()

          logger.debug(
            s"[queryTimeSeries] [requestId=$dayRequestId] Retrieved $resultCount results in ${duration}ms for day $dayTs")

          if (duration > slowOperationThresholdMs) {
            logger.warn(
              s"[queryTimeSeries] [requestId=$dayRequestId] Slow query: ${duration}ms for day $dayTs, resultCount=$resultCount")
          }

          Success(javaList.asScala.map { doc =>
            val valueBytes = extractBytes(doc.get("valueBytes"))
            val tsMillis = doc.get("tsMillis").asInstanceOf[Number].longValue()
            TimedValue(valueBytes, tsMillis)
          }.toSeq)
        }
        .recover {
          case e: CosmosException =>
            logCosmosException("queryTimeSeries",
                               e,
                               dayRequestId,
                               Map(
                                 "dataset" -> dataset,
                                 "keyHashDay" -> keyHashDay,
                                 "dayTs" -> dayTs,
                                 "startTs" -> startTs,
                                 "endTs" -> endTs
                               ))
            Failure(e)
          case e: Exception =>
            logger.error(s"[queryTimeSeries] [requestId=$dayRequestId] Error querying day $dayTs", e)
            Failure(e)
        }
    }

    Future.sequence(dayFutures).map { results =>
      // Check if any day failed
      val failures = results.collect { case Failure(e) => e }
      if (failures.nonEmpty) {
        logger.error(s"[queryTimeSeries] [requestId=$requestId] ${failures.size} out of ${results.size} days failed")
        Failure(failures.head)
      } else {
        val combined = results.collect { case Success(values) => values }.flatten
        logger.debug(
          s"[queryTimeSeries] [requestId=$requestId] Combined ${combined.size} total values from ${dayRange.size} days")
        Success(combined)
      }
    }
  }

  override def multiPut(requests: Seq[PutRequest]): Future[Seq[Boolean]] = {
    val batchRequestId = UUID.randomUUID().toString
    logger.debug(s"[multiPut] [requestId=$batchRequestId] Starting with ${requests.size} requests")

    val requestsByContainer = requests.groupBy(req => mapDatasetToContainer(req.dataset))
    logger.debug(s"[multiPut] [requestId=$batchRequestId] Grouped into ${requestsByContainer.size} containers")

    val containerFutures = requestsByContainer.zipWithIndex.map {
      case ((containerName, containerRequests), containerIdx) =>
        val containerRequestId = s"$batchRequestId-c$containerIdx"
        logger.debug(
          s"[multiPut] [requestId=$containerRequestId] container=$containerName, requests=${containerRequests.size}")

        val container = getOrCreateContainerRef(containerName)
        val datasetMetricsContext = containerToContext.getOrElseUpdate(
          containerName,
          metricsContext.copy(dataset = containerName)
        )

        val putFutures = containerRequests.zipWithIndex.map { case (request, itemIdx) =>
          val itemRequestId = s"$containerRequestId-i$itemIdx"
          val tableType = getTableType(request.dataset)
          val timestampInRequest = request.tsMillis.getOrElse(System.currentTimeMillis())
          val startTime = System.currentTimeMillis()

          val (document, partitionKey, docId) = tableType match {
            case StreamingTable =>
              val tileKey = TilingUtils.deserializeTileKey(request.keyBytes)
              val baseKeyBytes = tileKey.keyBytes.asScala.map(_.toByte).toArray
              val keyHash = buildKeyHash(baseKeyBytes)
              val tileStartTs = tileKey.tileStartTimestampMillis
              val tileSizeMs = tileKey.tileSizeMillis
              val dayTs = roundToDay(tileStartTs)
              val keyHashDay = s"${keyHash}_${dayTs}"
              val id = buildTimeSeriesDocumentId(request.dataset, keyHash, tileStartTs, tileSizeMs)

              val doc = new java.util.HashMap[String, Object]()
              doc.put("id", id)
              doc.put("dataset", request.dataset)
              doc.put("keyHashDay", keyHashDay)
              doc.put("keyHash", keyHash)
              doc.put("dayTs", dayTs.asInstanceOf[Object])
              doc.put("keyBytes", baseKeyBytes)
              doc.put("valueBytes", request.valueBytes)
              doc.put("tsMillis", tileStartTs.asInstanceOf[Object])
              doc.put("tileSizeMs", tileKey.tileSizeMillis.asInstanceOf[Object])
              doc.put("ttl", dataTTLSeconds.asInstanceOf[Object])

              val pk = new PartitionKeyBuilder()
                .add(request.dataset)
                .add(keyHashDay)
                .build()

              (doc, pk, id)

            case BatchTable =>
              val keyHash = buildKeyHash(request.keyBytes)
              val id = buildBatchDocumentId(request.dataset, keyHash)

              val doc = new java.util.HashMap[String, Object]()
              doc.put("id", id)
              doc.put("dataset", request.dataset)
              doc.put("keyHash", keyHash)
              doc.put("keyBytes", request.keyBytes)
              doc.put("valueBytes", request.valueBytes)
              doc.put("tsMillis", timestampInRequest.asInstanceOf[Object])
              doc.put("ttl", dataTTLSeconds.asInstanceOf[Object])

              val pk = new PartitionKeyBuilder()
                .add(request.dataset)
                .add(keyHash)
                .build()

              (doc, pk, id)

            case MetadataTable =>
              val keyHash = buildKeyHash(request.keyBytes)
              val id = buildBatchDocumentId(request.dataset, keyHash)

              val doc = new java.util.HashMap[String, Object]()
              doc.put("id", id)
              doc.put("dataset", request.dataset)
              doc.put("keyHash", keyHash)
              doc.put("keyBytes", request.keyBytes)
              doc.put("valueBytes", request.valueBytes)
              doc.put("tsMillis", timestampInRequest.asInstanceOf[Object])
              doc.put("ttl", dataTTLSeconds.asInstanceOf[Object])

              val pk = new PartitionKeyBuilder()
                .add(keyHash)
                .build()

              (doc, pk, id)
          }

          logger.debug(
            s"[multiPut] [requestId=$itemRequestId] Upserting: dataset=${request.dataset}, tableType=$tableType, docId=$docId")

          try {
            val upsertMono = container.upsertItem(document, partitionKey, new CosmosItemRequestOptions())
            monoToScalaFuture(upsertMono)
              .map { response =>
                val duration = System.currentTimeMillis() - startTime
                datasetMetricsContext.distribution("multiPut.latency", duration)
                datasetMetricsContext.increment("multiPut.successes")

                if (duration > slowOperationThresholdMs) {
                  logger.warn(s"[multiPut] [requestId=$itemRequestId] Slow upsert: ${duration}ms")
                  logDiagnostics("multiPut.slow",
                                 response.getDiagnostics,
                                 itemRequestId,
                                 Map(
                                   "durationMs" -> duration,
                                   "docId" -> docId,
                                   "dataset" -> request.dataset
                                 ))
                } else {
                  logger.debug(s"[multiPut] [requestId=$itemRequestId] Success in ${duration}ms")
                }
                true
              }
              .recover {
                case e: CosmosException =>
                  logCosmosException("multiPut",
                                     e,
                                     itemRequestId,
                                     Map(
                                       "container" -> containerName,
                                       "dataset" -> request.dataset,
                                       "tableType" -> tableType.toString,
                                       "docId" -> docId
                                     ))
                  datasetMetricsContext.increment("multiPut.failures", Map("exception" -> e.getClass.getName))
                  false
                case e: Exception =>
                  logger.error(
                    s"[multiPut] [requestId=$itemRequestId] Unexpected error: container=$containerName, docId=$docId",
                    e)
                  datasetMetricsContext.increment("multiPut.failures", Map("exception" -> e.getClass.getName))
                  false
              }
          } catch {
            case e: Exception =>
              logger.error(
                s"[multiPut] [requestId=$itemRequestId] Error creating upsert: container=$containerName, docId=$docId",
                e)
              Future.successful(false)
          }
        }

        Future.sequence(putFutures)
    }.toSeq

    Future
      .sequence(containerFutures)
      .map { results =>
        val flattened = results.flatten
        val successCount = flattened.count(_ == true)
        val failureCount = flattened.count(_ == false)

        logger.info(
          s"[multiPut] [requestId=$batchRequestId] Completed: total=${requests.size}, success=$successCount, failures=$failureCount")

        if (failureCount > 0) {
          logger.error(
            s"[multiPut] [requestId=$batchRequestId] ${failureCount} out of ${requests.size} requests failed")
        }

        flattened
      }
      .recover { case e: Exception =>
        logger.error(s"[multiPut] [requestId=$batchRequestId] Batch operation failed", e)
        Seq.fill(requests.size)(false)
      }
  }

  override def bulkPut(sourceOfflineTable: String, destinationOnlineDataSet: String, partition: String): Unit = {
    logger.info(
      s"Triggering Cosmos bulk load for dataset: $destinationOnlineDataSet, table: $sourceOfflineTable, partition: $partition")

    val startTs = System.currentTimeMillis()

    try {
      // Determine container and endpoint
      val groupBy = new GroupBy().setMetaData(new MetaData().setName(destinationOnlineDataSet))
      val batchDataset = groupBy.batchDataset
      val containerName = mapDatasetToContainer(batchDataset)

      val loaderArgs = Array(
        "--table-name",
        sourceOfflineTable,
        "--dataset",
        batchDataset,
        "--end-ds",
        partition,
        "--cosmos-endpoint",
        CosmosKVStoreFactory.getOrElseThrow(EnvCosmosEndpoint, conf),
        "--cosmos-key",
        CosmosKVStoreFactory.getOrElseThrow(EnvCosmosKey, conf),
        "--cosmos-database",
        CosmosKVStoreFactory.getOptional(EnvCosmosDatabase, conf).getOrElse(DefaultDatabaseName),
        "--cosmos-container",
        containerName,
        "--ttl",
        dataTTLSeconds.toString
      )

      Spark2CosmosLoader.main(loaderArgs)

      logger.info("Cosmos bulk load completed successfully")
      metricsContext.distribution("bulkPut.latency", System.currentTimeMillis() - startTs)
      metricsContext.increment("bulkPut.successes")

    } catch {
      case e: Exception =>
        logger.error(s"Failed to run Cosmos bulk load for $sourceOfflineTable", e)
        metricsContext.increment("bulkPut.failures", Map("exception" -> e.getClass.getName))
        throw e
    }
  }

  override def list(request: ListRequest): Future[ListResponse] = {
    logger.info(s"Performing list for ${request.dataset}")

    val listLimit = request.props.get("limit") match {
      case Some(value: Int)    => value
      case Some(value: String) => value.toInt
      case _                   => DefaultMaxItemsPerPage
    }

    val maybeListEntityType = request.props.get("entity_type")
    val maybeContinuationToken = request.props.get("continuation-key").map(_.asInstanceOf[String])

    val containerName = mapDatasetToContainer(request.dataset)
    val container = getOrCreateContainerRef(containerName)
    val datasetMetricsContext = containerToContext.getOrElseUpdate(
      containerName,
      metricsContext.copy(dataset = containerName)
    )

    val startTime = System.currentTimeMillis()

    val query = maybeListEntityType match {
      case Some(entityType) =>
        """SELECT c.keyBytes, c.valueBytes
          |FROM c
          |WHERE c.dataset = @dataset
          |  AND STARTSWITH(c.id, @prefix)""".stripMargin
      case None =>
        """SELECT c.keyBytes, c.valueBytes
        |FROM c
          |WHERE c.dataset = @dataset""".stripMargin
    }

    val querySpec = new SqlQuerySpec(query)
    querySpec.setParameters(new java.util.ArrayList[SqlParameter]())
    querySpec.getParameters.add(new SqlParameter("@dataset", request.dataset))

    maybeListEntityType.foreach { et =>
      querySpec.getParameters.add(new SqlParameter("@prefix", s"${request.dataset}_${et}/"))
    }

    val options = new CosmosQueryRequestOptions()
    options.setMaxDegreeOfParallelism(-1) // Parallel execution

    val pagedFlux = container.queryItems(querySpec, options, classOf[java.util.Map[String, Object]])

    val pageFuture = maybeContinuationToken match {
      case Some(token) =>
        monoToScalaFuture(pagedFlux.byPage(token, listLimit).next())
      case None =>
        monoToScalaFuture(pagedFlux.byPage(listLimit).next())
    }

    pageFuture
      .map { page =>
        val listValues = page.getResults.asScala.map { doc =>
          val keyBytes = extractBytes(doc.get("keyBytes"))
          val valueBytes = extractBytes(doc.get("valueBytes"))
          ListValue(keyBytes, valueBytes)
        }.toSeq

        datasetMetricsContext.distribution("list.latency", System.currentTimeMillis() - startTime)
        datasetMetricsContext.increment("list.successes")

        val propsMap: Map[String, Any] = Option(page.getContinuationToken) match {
          case Some(token) if listValues.size >= listLimit =>
            Map("continuation-key" -> token)
          case _ =>
            Map.empty
        }

        ListResponse(request, Success(listValues), propsMap)
      }
      .recover { case e: Exception =>
        logger.error(s"Error listing values from container $containerName", e)
        datasetMetricsContext.increment("list.cosmos_errors", Map("exception" -> e.getClass.getName))
        ListResponse(request, Failure(e), Map.empty)
      }
  }
}
