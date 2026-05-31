/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.online.fetcher

import ai.chronon.api.Constants._
import ai.chronon.api.Extensions._
import ai.chronon.api.ScalaJavaConversions.IteratorOps
import ai.chronon.api._
import ai.chronon.api.thrift.TBase
import ai.chronon.online.KVStore.{ListRequest, ListResponse, PutRequest}
import ai.chronon.online.OnlineDerivationUtil.buildDerivedFields
import ai.chronon.online._
import ai.chronon.online.serde._
import ai.chronon.online.metrics.{Metrics, TTLCache}
import org.slf4j.{Logger, LoggerFactory}

import java.nio.charset.StandardCharsets
import scala.collection.immutable.SortedMap
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

// [timestamp -> {metric name -> metric value}]
case class DataMetrics(series: Seq[(Long, SortedMap[String, Any])])

case class ConfPathOrName(confPath: Option[String] = None, confName: Option[String] = None) {

  if (confPath.isEmpty && confName.isEmpty) {
    throw new IllegalArgumentException("confPath and confName cannot be both empty")
  }

  def computeConfKey(confKeyword: String): String = {
    if (confName.isDefined) {
      s"$confKeyword/" + confName.get

    } else {
      s"$confKeyword/" + confPath.get.split("/").takeRight(1).head
    }
  }
}

class MetadataStore(fetchContext: FetchContext) {

  @transient implicit lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  private val CONF_BATCH_SIZE = 50

  implicit val executionContext: ExecutionContext = fetchContext.getOrCreateExecutionContext

  def getConf[T <: TBase[_, _]: Manifest](confPathOrName: ConfPathOrName): Try[T] = {
    val clazz = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]

    val confTypeKeyword = clazz match {
      case j if j == classOf[Join]              => JoinFolder
      case g if g == classOf[GroupBy]           => GroupByFolder
      case sq if sq == classOf[StagingQuery]    => StagingQueryFolder
      case mt if mt == classOf[ModelTransforms] => ModelFolder
      case _                                    => throw new IllegalArgumentException(s"Unsupported conf type: $clazz")
    }

    val confKey = confPathOrName.computeConfKey(confTypeKeyword)
    fetchContext.kvStore
      .getString(confKey, fetchContext.metadataDataset, fetchContext.timeoutMillis)
      .map(conf => ThriftJsonCodec.fromJsonStr[T](conf, false, clazz))
      .recoverWith { case th: Throwable =>
        Failure(
          new RuntimeException(
            s"Couldn't fetch ${clazz.getName} for key $confKey. Perhaps metadata upload wasn't successful.",
            th
          ))
      }
  }

  lazy val getJoinConf: TTLCache[String, Try[JoinOps]] = new TTLCache[String, Try[JoinOps]](
    { name =>
      import ai.chronon.online.metrics
      val startTimeMs = System.currentTimeMillis()
      val result = getConf[Join](ConfPathOrName(confName = Some(name)))
        .recover { case e: java.util.NoSuchElementException =>
          logger.error(
            s"Failed to fetch conf for join $name at joins/$name, please check metadata upload to make sure the join metadata for $name has been uploaded")
          throw e
        }
        .map(new JoinOps(_))
      val context =
        if (result.isSuccess) metrics.Metrics.Context(metrics.Metrics.Environment.MetaDataFetching, result.get.join)
        else metrics.Metrics.Context(metrics.Metrics.Environment.MetaDataFetching, join = name)
      // Throw exception after metrics. No join metadata is bound to be a critical failure.
      // This will ensure that a Failure is never cached in the getJoinConf TTLCache
      if (result.isFailure) {
        context.withSuffix("join").incrementException(result.failed.get)
        throw result.failed.get
      }
      context
        .withSuffix("join")
        .distribution(metrics.Metrics.Name.LatencyMillis, System.currentTimeMillis() - startTimeMs)
      result
    },
    { join =>
      import ai.chronon.online.metrics
      metrics.Metrics.Context(environment = "join.meta.fetch", join = join)
    },
    ttlMillis = fetchContext.joinConfTtlMillis
  )

  def putJoinConf(join: Join): Future[Boolean] = {
    val joinConfKeyForKvStore = join.keyNameForKvStore
    logger.info(s"uploading join conf to dataset: ${fetchContext.metadataDataset} by key:${joinConfKeyForKvStore}")
    fetchContext.kvStore.put(
      PutRequest(joinConfKeyForKvStore.getBytes(Constants.UTF8),
                 ThriftJsonCodec.toJsonStr(join).getBytes(Constants.UTF8),
                 fetchContext.metadataDataset))
  }

  def putModelTransformsConf(modelTransforms: ModelTransforms): Future[Boolean] = {
    val modelTransformsConfKeyForKvStore = modelTransforms.keyNameForKvStore
    logger.info(
      s"uploading model transforms conf to dataset: ${fetchContext.metadataDataset} by key:${modelTransformsConfKeyForKvStore}")
    fetchContext.kvStore.put(
      PutRequest(
        modelTransformsConfKeyForKvStore.getBytes(Constants.UTF8),
        ThriftJsonCodec.toJsonStr(modelTransforms).getBytes(Constants.UTF8),
        fetchContext.metadataDataset
      ))
  }

  lazy val getModelTransformsConf: TTLCache[String, Try[ModelTransforms]] = new TTLCache[String, Try[ModelTransforms]](
    { name =>
      val startTimeMs = System.currentTimeMillis()
      val result = getConf[ModelTransforms](ConfPathOrName(confName = Some(name)))
        .recover { case e: java.util.NoSuchElementException =>
          logger.error(
            s"Failed to fetch conf for model transforms $name at models/$name, please check metadata upload to make sure the model transforms metadata for $name has been uploaded")
          throw e
        }
      val context = metrics.Metrics.Context(metrics.Metrics.Environment.MetaDataFetching, modelTransforms = name)
      // Throw exception after metrics. No model transforms metadata is bound to be a critical failure.
      // This will ensure that a Failure is never cached in the getModelTransformsConf TTLCache
      if (result.isFailure) {
        context.withSuffix("model_transforms").incrementException(result.failed.get)
        throw result.failed.get
      }
      context
        .withSuffix("model_transforms")
        .distribution(metrics.Metrics.Name.LatencyMillis, System.currentTimeMillis() - startTimeMs)
      result
    },
    { modelTransforms =>
      metrics.Metrics.Context(environment = "model_transforms.meta.fetch", modelTransforms = modelTransforms)
    }
  )

  def listJoins(isOnline: Boolean = true): Future[Seq[String]] = {
    import ai.chronon.online.metrics

    val context = metrics.Metrics.Context(metrics.Metrics.Environment.MetaDataFetching)
    val startTimeMs = System.currentTimeMillis()

    def parseJoins(response: ListResponse): Seq[String] = {
      val result = response.values
        .map { seqListValues =>
          seqListValues
            .map(kv => new String(kv.valueBytes, StandardCharsets.UTF_8))
            .map(v => ThriftJsonCodec.fromJsonStr[Join](v, check = false, classOf[Join]))
            .filter(_.join.metaData.online == isOnline)
            .map(_.metaData.name)

        }
        .recover { case e: Exception =>
          import ai.chronon.online.metrics
          logger.error("Failed to list & parse joins from list response", e)
          context.withSuffix("join_list").increment(metrics.Metrics.Name.Exception)
          throw e
        }

      result.get
    }

    def doRetrieveAllListConfs(acc: mutable.ArrayBuffer[String],
                               paginationKey: Option[Any] = None): Future[Seq[String]] = {
      val propsMap = {
        paginationKey match {
          case Some(key) => Map(ListEntityType -> JoinFolder, ContinuationKey -> key)
          case None      => Map(ListEntityType -> JoinFolder)
        }
      }

      val listRequest = ListRequest(fetchContext.metadataDataset, propsMap)
      fetchContext.kvStore.list(listRequest).flatMap { response =>
        val joinSeq: Seq[String] = parseJoins(response)
        val newAcc = acc ++ joinSeq
        if (response.resultProps.contains(ContinuationKey)) {
          doRetrieveAllListConfs(newAcc, response.resultProps.get(ContinuationKey))
        } else {
          import ai.chronon.online.metrics
          context
            .withSuffix("join_list")
            .distribution(metrics.Metrics.Name.LatencyMillis, System.currentTimeMillis() - startTimeMs)
          Future.successful(newAcc.toSeq)
        }
      }
    }

    doRetrieveAllListConfs(new mutable.ArrayBuffer[String]())
  }

  private def buildJoinPartCodec(joinConf: Join, joinPart: JoinPartOps, servingInfo: GroupByServingInfoParsed)
      : (Iterable[StructField], Iterable[StructField], JoinRequestKeys.KeyMapping) = {
    val keySchema = servingInfo.keyCodec.chrononSchema.asInstanceOf[StructType]
    val keyMapping = JoinRequestKeys.buildKeyMapping(joinConf, joinPart, servingInfo)
    val joinKeyFields = keyMapping.requestKeyFields

    val baseValueSchema: StructType = if (servingInfo.groupBy.aggregations == null) {
      servingInfo.selectedChrononSchema
    } else {
      servingInfo.outputChrononSchema
    }
    val valueFields = if (!servingInfo.groupBy.hasDerivations) {
      baseValueSchema.fields
    } else {
      buildDerivedFields(servingInfo.groupBy.derivationsScala, keySchema, baseValueSchema).toArray
    }
    val joinValueFields = valueFields.map(joinPart.constructJoinPartSchema)

    (joinKeyFields, joinValueFields, keyMapping)
  }

  // key and value schemas
  def buildJoinCodecCache(onCreateFunc: Option[Try[JoinCodec] => Unit]): TTLCache[String, Try[JoinCodec]] = {

    val codecBuilder = { joinName: String =>
      val startTimeMs = System.currentTimeMillis()
      val result: Try[JoinCodec] =
        try {
          getJoinConf(joinName)
            .map(_.join)
            .map(join => buildJoinCodec(join, refreshOnFail = true))
        } catch {
          case th: Throwable =>
            getJoinConf.refresh(joinName)
            Failure(
              new RuntimeException(
                s"Couldn't fetch joinName = ${joinName} or build join codec due to ${th.traceString}",
                th
              ))
        }
      val context = Metrics.Context(Metrics.Environment.MetaDataFetching, join = joinName).withSuffix("join_codec")
      if (result.isFailure) {
        context.incrementException(result.failed.get)
      } else {
        context.distribution(Metrics.Name.LatencyMillis, System.currentTimeMillis() - startTimeMs)
      }
      result
    }

    new TTLCache[String, Try[JoinCodec]](
      codecBuilder,
      { join: String =>
        import ai.chronon.online.metrics
        metrics.Metrics.Context(environment = "join.codec.fetch", join = join)
      },
      ttlMillis = fetchContext.joinCodecTtlMillis,
      onCreateFunc = onCreateFunc
    )
  }

  def buildJoinCodec(joinConf: Join, refreshOnFail: Boolean): JoinCodec = {
    val keyFields = new mutable.LinkedHashSet[StructField]
    val valueFields = new mutable.ListBuffer[StructField]
    val valueInfos = mutable.ListBuffer.empty[JoinCodec.ValueInfo]
    val joinPartKeyMappings = mutable.Map.empty[String, JoinRequestKeys.KeyMapping]
    var hasPartialFailure = false
    // collect keyFields and valueFields from joinParts/GroupBys
    joinConf.joinPartOps.foreach { joinPart =>
      getGroupByServingInfo(joinPart.groupBy.metaData.getName)
        .map { servingInfo =>
          val (keys, values, keyMapping) = buildJoinPartCodec(joinConf, joinPart, servingInfo)

          keys.foreach(k => keyFields.add(k))
          values.foreach(v => valueFields.append(v))
          joinPartKeyMappings.put(JoinRequestKeys.partKey(joinConf, joinPart), keyMapping)
          joinPartKeyMappings.put(JoinRequestKeys.partKey(joinConf, joinPart, servingInfo), keyMapping)

          val leftKeys = JoinRequestKeys.valueInfoLeftKeys(joinConf, joinPart)
          values.foreach { v =>
            val schemaString = SparkConversions.fromChrononType(v.fieldType).catalogString
            valueInfos.append(JoinCodec
              .ValueInfo(v.name, joinPart.groupBy.metaData.getName, joinPart.prefix, leftKeys.toArray, schemaString))
          }
        }
        .recoverWith {
          case exception: Throwable => {
            if (refreshOnFail) {
              getGroupByServingInfo.refresh(joinPart.groupBy.metaData.getName)
              hasPartialFailure = true
              Success(())
            } else {
              Failure(new Exception(
                s"Failure to build join codec for join ${joinConf.metaData.name} due to bad groupBy serving info for ${joinPart.groupBy.metaData.name}",
                exception))
            }
          }
        }
        .get
    }

    // gather key schema and value schema from external sources.
    Option(joinConf.join.onlineExternalParts).foreach { externals =>
      externals
        .iterator()
        .toScala
        .foreach { part =>
          val source = part.source

          def buildFields(schema: TDataType, prefix: String = ""): Seq[StructField] =
            DataType
              .fromTDataType(schema)
              .asInstanceOf[StructType]
              .fields
              .map(f => StructField(prefix + f.name, f.fieldType))

          val keyStructFields =
            buildFields(source.getKeySchema).map(f => f.copy(name = part.rightToLeft.getOrElse(f.name, f.name)))
          keyStructFields.foreach(keyFields.add)
          val leftKeys = keyStructFields.map(_.name)

          buildFields(source.getValueSchema, part.fullName + "_").foreach { f =>
            val schemaString = SparkConversions.fromChrononType(f.fieldType).catalogString
            valueInfos.append(
              JoinCodec.ValueInfo(f.name, part.source.metadata.getName, part.prefix, leftKeys.toArray, schemaString))
            valueFields.append(f)
          }

        }
    }

    val joinName = joinConf.metaData.nameToFilePath
    val keySchema = StructType(s"${joinName.sanitize}_key", keyFields.toArray)
    val keyCodec = AvroCodec.of(AvroConversions.fromChrononSchema(keySchema).toString)
    val baseValueSchema = StructType(s"${joinName.sanitize}_value", valueFields.toArray)
    val baseValueCodec = serde.AvroCodec.of(AvroConversions.fromChrononSchema(baseValueSchema).toString)
    JoinCodec(joinConf,
              keySchema,
              baseValueSchema,
              keyCodec,
              baseValueCodec,
              valueInfos.toArray,
              joinPartKeyMappings.toMap,
              hasPartialFailure)
  }

  def getSchemaFromKVStore(dataset: String, key: String): serde.AvroCodec = {
    fetchContext.kvStore
      .getString(key, dataset, fetchContext.timeoutMillis)
      .recover { case e: java.util.NoSuchElementException =>
        logger.error(s"Failed to retrieve $key for $dataset. Is it possible that hasn't been uploaded?")
        throw e
      }
      .map(AvroCodec.of(_))
      .get
  }

  lazy val getStatsSchemaFromKVStore: TTLCache[(String, String), serde.AvroCodec] =
    new TTLCache[(String, String), serde.AvroCodec](
      { case (dataset, key) => getSchemaFromKVStore(dataset, key) },
      { _ => Metrics.Context(environment = "stats.serving_info.fetch") }
    )

  // pull and cache groupByServingInfo from the groupBy uploads
  lazy val getGroupByServingInfo: TTLCache[String, Try[GroupByServingInfoParsed]] =
    new TTLCache[String, Try[GroupByServingInfoParsed]](
      { name =>
        val startTimeMs = System.currentTimeMillis()
        val batchDataset = s"${name.sanitize.toUpperCase()}_BATCH"
        val metaData =
          fetchContext.kvStore
            .getString(Constants.GroupByServingInfoKey, batchDataset, fetchContext.timeoutMillis)
            .recover {
              case e: java.util.NoSuchElementException =>
                logger.error(
                  s"Failed to fetch metadata for $batchDataset, is it possible Group By Upload for $name has not succeeded?")
                throw e
              case e: Throwable =>
                logger.error(s"Failed to fetch metadata for $batchDataset", e)
                throw e
            }
        logger.info(s"Fetched ${Constants.GroupByServingInfoKey} from : $batchDataset")
        if (metaData.isFailure) {
          Metrics
            .Context(Metrics.Environment.MetaDataFetching, groupBy = name)
            .withSuffix("group_by")
            .incrementException(metaData.failed.get)
          Failure(
            new RuntimeException(s"Couldn't fetch group by serving info for $batchDataset, " +
                                   "please make sure a batch upload was successful",
                                 metaData.failed.get))
        } else {
          import ai.chronon.online.metrics
          val groupByServingInfo = ThriftJsonCodec
            .fromJsonStr[GroupByServingInfo](metaData.get, check = false, classOf[GroupByServingInfo])
          metrics.Metrics
            .Context(metrics.Metrics.Environment.MetaDataFetching, groupByServingInfo.groupBy)
            .withSuffix("group_by")
            .distribution(metrics.Metrics.Name.LatencyMillis, System.currentTimeMillis() - startTimeMs)
          Success(new GroupByServingInfoParsed(groupByServingInfo))
        }
      },
      { gb =>
        import ai.chronon.online.metrics
        metrics.Metrics.Context(environment = "group_by.serving_info.fetch", groupBy = gb)
      }
    )

  def put(
      kVPairs: Map[String, Seq[String]],
      datasetName: String = MetadataDataset,
      batchSize: Int = CONF_BATCH_SIZE
  ): Future[Seq[Boolean]] = {
    val puts = kVPairs.map {
      case (k, v) => {
        logger.info(s"""Putting metadata for
             |dataset: $datasetName
             |key: $k
             |conf: $v""".stripMargin)
        val kBytes = k.getBytes()
        // The value is a single string by default
        val vBytes = v.head.getBytes()
        PutRequest(keyBytes = kBytes,
                   valueBytes = vBytes,
                   dataset = datasetName,
                   tsMillis = Some(System.currentTimeMillis()))
      }
    }.toSeq
    val putsBatches = puts.grouped(batchSize).toSeq
    logger.info(s"Putting ${puts.size} configs to KV Store, dataset=$datasetName")
    val futures = putsBatches.map(batch => fetchContext.kvStore.multiPut(batch))
    Future.sequence(futures).map(_.flatten)
  }

  def create(dataset: String): Unit = {
    try {
      logger.info(s"Creating dataset: $dataset")
      // TODO: this is actually just an async task. it doesn't block and thus we don't actually
      //  know if it successfully created the dataset
      fetchContext.kvStore.create(dataset)

      logger.info(s"Successfully created dataset: $dataset")
    } catch {
      case e: Exception =>
        logger.error(s"Failed to create dataset: $dataset", e)
        throw e
    }
  }
}
