package ai.chronon.online.fetcher

import ai.chronon.api.Extensions._
import ai.chronon.api._
import ai.chronon.online.KVStore.{GetRequest, GetResponse, TimedValue}
import ai.chronon.online.OnlineDerivationUtil.applyDeriveFunc
import ai.chronon.online.fetcher.Fetcher.{ColumnSpec, PrefixedRequest, Request, Response}
import ai.chronon.online.fetcher.FetcherCache.{BatchResponses, CachedBatchResponse}
import ai.chronon.online._
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/** Fetches GroupBy data from batch and streaming datasets
  * Handles Tiled and untiled fetches
  * @param fetchContext: Contains members to expose the outside world & config to this class
  */
class GroupByFetcher(fetchContext: FetchContext, metadataStore: MetadataStore)
    extends GroupByResponseHandler(fetchContext, metadataStore) {

  implicit val executionContext: ExecutionContext = fetchContext.getOrCreateExecutionContext

  @transient private implicit lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  override def isCachingEnabled(groupBy: GroupBy): Boolean = {
    if (fetchContext.debug) {
      configuredBatchIrCacheSize match {
        case Some(cacheSize) =>
          logger.info(s"Online IR caching is enabled with cache size = $cacheSize")
        case None =>
          logger.info("Online IR caching is disabled")
      }
    }

    isCacheSizeConfigured
  }

  /** Convert a groupBy request into a batch kv request and optionally a streaming kv request
    */
  private def toLambdaKvRequest(request: Fetcher.Request): Try[LambdaKvRequest] = metadataStore
    .getGroupByServingInfo(request.name)
    .recover { case ex: Throwable =>
      metadataStore.getGroupByServingInfo.refresh(request.name)
      logger.error(s"Couldn't fetch GroupByServingInfo for ${request.name}", ex)
      request.context.foreach(_.incrementException(ex))
      throw ex

    }
    .map { groupByServingInfo =>
      val context =
        request.context.getOrElse(
          metrics.Metrics.Context(metrics.Metrics.Environment.GroupByFetching, groupByServingInfo.groupBy))
      context.increment("group_by_request.count")

      var batchKeyBytes: Array[Byte] = null
      var streamingKeyBytes: Array[Byte] = null

      try {
        // The formats of key bytes for batch requests and key bytes for streaming requests may differ based
        // on the KVStore implementation, so we encode each distinctly.
        batchKeyBytes = fetchContext.kvStore.createKeyBytes(request.keys,
                                                            groupByServingInfo,
                                                            groupByServingInfo.groupByOps.batchDataset)
        streamingKeyBytes = fetchContext.kvStore.createKeyBytes(request.keys,
                                                                groupByServingInfo,
                                                                groupByServingInfo.groupByOps.streamingDataset)

      } catch {

        // TODO: only gets hit in cli path - make this code path just use avro schema to decode keys directly in cli
        // TODO: Remove this code block
        case ex: Exception =>
          val castedKeys = groupByServingInfo.keyChrononSchema.cast(request.keys)

          try {
            batchKeyBytes = fetchContext.kvStore.createKeyBytes(castedKeys,
                                                                groupByServingInfo,
                                                                groupByServingInfo.groupByOps.batchDataset)

            streamingKeyBytes = fetchContext.kvStore.createKeyBytes(castedKeys,
                                                                    groupByServingInfo,
                                                                    groupByServingInfo.groupByOps.streamingDataset)
          } catch {
            case exInner: Exception =>
              exInner.addSuppressed(ex)
              throw new RuntimeException("Couldn't encode request keys or casted keys", exInner)
          }

      }

      val batchRequest = GetRequest(batchKeyBytes, groupByServingInfo.groupByOps.batchDataset)

      val streamingRequestOpt = groupByServingInfo.groupByOps.inferredAccuracy match {
        // fetch batch(ir) and streaming(input) and aggregate
        case Accuracy.TEMPORAL =>
          // Build a tile key for the streaming request
          // When we build support for layering, we can expand this out into a utility that builds n tile keys for n layers
          val keyBytes = if (fetchContext.isTilingEnabled) {

            val tileKey = TilingUtils.buildTileKey(
              groupByServingInfo.groupByOps.streamingDataset,
              streamingKeyBytes,
              Some(groupByServingInfo.smallestTailHopMillis),
              None
            )

            TilingUtils.serializeTileKey(tileKey)
          } else {
            streamingKeyBytes
          }

          Some(
            GetRequest(keyBytes,
                       groupByServingInfo.groupByOps.streamingDataset,
                       Some(groupByServingInfo.batchEndTsMillis)))

        // no further aggregation is required - the value in KvStore is good as is
        case Accuracy.SNAPSHOT => None

      }

      val castedRequest = request.copy(keys = groupByServingInfo.keyChrononSchema.cast(request.keys))
      LambdaKvRequest(groupByServingInfo, castedRequest, batchRequest, streamingRequestOpt, request.atMillis, context)

    }

  private def attemptDerivations(request: Fetcher.Request,
                                 responseMap: Map[String, AnyRef],
                                 requestContext: RequestContext): Map[String, AnyRef] = {

    val derivedMapTry: Try[Map[String, AnyRef]] = Try {
      applyDeriveFunc(requestContext.servingInfo.deriveFunc, request, responseMap)
    }

    derivedMapTry match {
      case Success(derivedMap) =>
        derivedMap
      case Failure(exception) =>
        requestContext.metricsContext.incrementException(exception)
        logger.error(s"Failed to derive values for request: ${request.name}@${request.keys}", exception)
        Map(s"derivation${FetcherUtil.FeatureExceptionSuffix}" -> exception.traceString.asInstanceOf[AnyRef])
    }
  }

  // 1. fetch GroupByServingInfo
  // 2. encodes keys as keyAvroSchema
  // 3. Based on accuracy, fetches streaming + batch data and aggregates further.
  // 4. Finally converted to outputSchema
  def fetchGroupBys(requests: Seq[Fetcher.Request]): Future[Seq[Fetcher.Response]] = {

    // split a groupBy level request into its kvStore level requests
    val groupByRequestToKvRequest: Seq[(Fetcher.Request, Try[LambdaKvRequest])] = requests.iterator
      .filter(r => r.keys == null || r.keys.values == null || r.keys.values.exists(_ != null))
      .map { request =>
        val groupByRequestMetaTry: Try[LambdaKvRequest] = toLambdaKvRequest(request)

        if (groupByRequestMetaTry.isFailure)
          request.context.foreach(_.increment("group_by_serving_info_failure.count"))

        request -> groupByRequestMetaTry
      }
      .toSeq

    // If caching is enabled, we check if any of the GetRequests are already cached. If so, we store them in a Map
    // and avoid the work of re-fetching them. It is mainly for batch data requests.
    val cachedRequests: Map[GetRequest, CachedBatchResponse] = getCachedRequests(groupByRequestToKvRequest)
    // Collect cache metrics once per fetchGroupBys call; Caffeine metrics aren't tagged by groupBy
    maybeBatchIrCache.foreach(cache =>
      LRUCache.collectCaffeineCacheMetrics(caffeineMetricsContext, cache.cache, cache.cacheName))

    val allRequestsToFetch: Seq[GetRequest] = groupByRequestToKvRequest.flatMap {
      case (_, Success(LambdaKvRequest(_, _, batchRequest, streamingRequestOpt, _, _))) =>
        // If a batch request is cached, don't include it in the list of requests to fetch because the batch IRs already cached
        if (cachedRequests.contains(batchRequest)) streamingRequestOpt else Some(batchRequest) ++ streamingRequestOpt

      case _ => Seq.empty
    }

    val startTimeMs = System.currentTimeMillis()
    val kvResponseFuture: Future[Seq[GetResponse]] = if (allRequestsToFetch.nonEmpty) {
      val rawFuture = fetchContext.kvStore.multiGet(allRequestsToFetch)
      if (fetchContext.kvTimeoutMillis > 0)
        FetcherTimeout.withTimeout(rawFuture, fetchContext.kvTimeoutMillis)
      else
        rawFuture
    } else {
      Future(Seq.empty[GetResponse])
    }

    kvResponseFuture
      .map { kvResponses: Seq[GetResponse] =>
        val multiGetMillis = System.currentTimeMillis() - startTimeMs

        val responsesMap: Map[GetRequest, Try[Seq[TimedValue]]] = kvResponses.map { response =>
          response.request -> response.values
        }.toMap

        val totalResponseValueBytes =
          responsesMap.iterator
            .map(_._2)
            .filter(v => v.isSuccess && v.get != null)
            .flatMap(_.get.map(v => Option(v.bytes).map(_.length).getOrElse(0)))
            .sum

        val responses: Seq[Response] = groupByRequestToKvRequest.iterator.map { case (request, requestMetaTry) =>
          val responseMapTry: Try[Map[String, AnyRef]] = requestMetaTry.map { requestMeta =>
            val LambdaKvRequest(groupByServingInfo, castedRequest, batchRequest, streamingRequestOpt, _, context) =
              requestMeta

            context.count("multi_get.batch.size", allRequestsToFetch.length)
            context.distribution("multi_get.bytes", totalResponseValueBytes)
            context.distribution("multi_get.response.length", kvResponses.length)
            context.distribution("multi_get.latency.millis", multiGetMillis)

            // pick the batch version with highest timestamp
            val batchResponses: BatchResponses =
              // Check if the get request was cached. If so, use the cache. Otherwise, try to get it from response.
              cachedRequests.get(batchRequest) match {
                case None =>
                  BatchResponses(
                    responsesMap
                      .getOrElse(
                        batchRequest,
                        // Fail if response is neither in responsesMap nor in cache
                        Failure(new IllegalStateException(
                          s"Couldn't find corresponding response for $batchRequest in responseMap or cache"))
                      ))
                case Some(cachedResponse: CachedBatchResponse) => cachedResponse
              }

            val streamingResponsesOpt =
              streamingRequestOpt.map(responsesMap.getOrElse(_, Success(Seq.empty)).getOrElse(Seq.empty))

            val queryTs = request.atMillis.getOrElse(System.currentTimeMillis())
            val requestContext = RequestContext(groupByServingInfo, queryTs, startTimeMs, context, request.keys)

            val groupByResponse: Map[String, AnyRef] =
              try {
                if (fetchContext.debug)
                  logger.info(
                    s"Constructing response for groupBy: ${groupByServingInfo.groupByOps.metaData.getName} " +
                      s"for keys: ${request.keys}")

                decodeAndMerge(batchResponses, streamingResponsesOpt, requestContext)

              } catch {

                case ex: Exception =>
                  // not all exceptions are due to stale schema, so we want to control how often we hit kv store
                  metadataStore.getGroupByServingInfo.refresh(groupByServingInfo.groupByOps.metaData.name)
                  context.incrementException(ex)
                  ex.printStackTrace()
                  throw ex

              }

            if (groupByServingInfo.groupBy.hasDerivations) {
              attemptDerivations(castedRequest, groupByResponse, requestContext = requestContext)
            } else {
              groupByResponse
            }

          }

          Response(request, responseMapTry)
        }.toList
        responses
      }
  }

  /** Fetch method to simulate a random access interface for Chronon
    * by distributing requests to relevant GroupBys. This is a batch
    * API which allows the caller to provide a sequence of ColumnSpec
    * queries and receive a mapping of results.
    *
    * TODO: Metrics
    * TODO: Collection identifier for metrics
    * TODO: Consider removing prefix interface for this method
    * TODO: Consider using simpler response type since mapping is redundant
    *
    * @param columnSpecs – batch of ColumnSpec queries
    * @return Future map of query to GroupBy response
    */
  def fetchColumns(
      columnSpecs: Seq[ColumnSpec]
  ): Future[Map[ColumnSpec, Response]] = {
    val startTimeMs = System.currentTimeMillis()

    // Generate a mapping from ColumnSpec query --> GroupBy request
    val groupByRequestsByQuery: Map[ColumnSpec, Request] =
      columnSpecs.map { case query =>
        val prefix = query.prefix.getOrElse("")
        val requestName = s"${query.groupByName}.${query.columnName}"
        val keyMap = query.keyMapping.getOrElse(Map())
        query -> PrefixedRequest(prefix, Request(requestName, keyMap, Some(startTimeMs), None)).request
      }.toMap

    // Start I/O and generate a mapping from query --> GroupBy response
    val groupByResponsesFuture = fetchGroupBys(groupByRequestsByQuery.values.toList)
    groupByResponsesFuture.map { groupByResponses =>
      val resultsByRequest = groupByResponses.iterator.map { response => response.request -> response.values }.toMap
      val responseByQuery = groupByRequestsByQuery.map { case (query, request) =>
        val results = resultsByRequest
          .getOrElse(
            request,
            Failure(new IllegalStateException(s"Couldn't find a groupBy response for $request in response map"))
          )
          .map { valueMap =>
            if (valueMap != null) {
              valueMap.map { case (aggName, aggValue) =>
                val resultKey = query.prefix.map(p => s"${p}_${aggName}").getOrElse(aggName)
                resultKey -> aggValue
              }
            } else {
              Map.empty[String, AnyRef]
            }
          }
          .recoverWith { // capture exception as a key
            case ex: Throwable =>
              if (fetchContext.debug || Math.random() < 0.001) {
                logger.error(s"Failed to fetch $request", ex)
              }
              Failure(ex)
          }
        val response = Response(request, results)
        query -> response
      }

      responseByQuery
    }
  }

}

case class LambdaKvRequest(groupByServingInfoParsed: GroupByServingInfoParsed,
                           castedGroupByRequest: Fetcher.Request,
                           batchRequest: GetRequest,
                           streamingRequestOpt: Option[GetRequest],
                           endTs: Option[Long],
                           context: metrics.Metrics.Context)
