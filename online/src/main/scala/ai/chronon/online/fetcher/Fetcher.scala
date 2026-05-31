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

import ai.chronon.api
import ai.chronon.api.Constants.UTF8
import ai.chronon.api.Extensions.{ExternalPartOps, JoinOps, ModelTransformsOps, StringOps, ThrowableOps}
import ai.chronon.api._
import ai.chronon.online.OnlineDerivationUtil.applyDeriveFunc
import ai.chronon.online._
import ai.chronon.online.fetcher.Fetcher.{
  AvroResponseValue,
  BaseResponse,
  GroupBySchemaResponse,
  GroupByStatusResponse,
  JoinSchemaResponse,
  Request,
  Response,
  ResponseV2,
  ResponseWithContext
}
import ai.chronon.online.fetcher.FeaturesResponseType.ResponseType
import ai.chronon.online.metrics.{Metrics, TTLCache}
import ai.chronon.online.serde._
import com.google.gson.Gson
import org.apache.avro.generic.GenericRecord
import org.slf4j.{Logger, LoggerFactory}

import java.util.function.Consumer
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

// Identifies the type for the features response. ex: map, avro encoded string of the map, avro byte array of the map.
object FeaturesResponseType extends Enumeration {
  type ResponseType = Value
  val Map, AvroBytes, AvroString = Value
}

object Fetcher {

  import ai.chronon.online.metrics

  case class Request(name: String,
                     keys: Map[String, AnyRef],
                     atMillis: Option[Long] = None,
                     context: Option[metrics.Metrics.Context] = None)

  case class PrefixedRequest(prefix: String, request: Request)

  trait BaseResponse

  case class Response(request: Request, values: Try[Map[String, AnyRef]]) extends BaseResponse
  case class ResponseWithContext(request: Request,
                                 derivedValues: Map[String, AnyRef],
                                 baseValues: Map[String, AnyRef]) {
    def combinedValues: Map[String, AnyRef] = baseValues ++ derivedValues
  }

  // Identifies the type of Avro response. currently supports the feature map encoded as avro bytes or avro string (base64 encoded)
  sealed trait AvroResponseValue
  object AvroResponseValue {
    // TODO: do pure Avro binary
    case class AvroBytes(value: Try[Array[Byte]]) extends AvroResponseValue
    case class AvroString(value: Try[String]) extends AvroResponseValue
  }

  case class ResponseV2(request: Request,
                        value: AvroResponseValue,
                        errors: Try[scala.collection.immutable.Map[String, String]])
      extends BaseResponse {

    def valuesAvroString: Try[String] = value match {
      case AvroResponseValue.AvroString(v) => v
    }

    def getResponseValueType: ResponseType = value match {
      case AvroResponseValue.AvroBytes(_)  => FeaturesResponseType.AvroBytes
      case AvroResponseValue.AvroString(_) => FeaturesResponseType.AvroString
    }
  }

  case class ColumnSpec(groupByName: String,
                        columnName: String,
                        prefix: Option[String],
                        keyMapping: Option[Map[String, AnyRef]])

  def logResponseStats(response: Response, context: metrics.Metrics.Context): Unit = {
    import ai.chronon.online.metrics
    val responseMap = response.values.get
    var exceptions = 0
    var nulls = 0
    responseMap.foreach { case (_, v) =>
      if (v == null) nulls += 1
      else if (v.isInstanceOf[Throwable]) exceptions += 1
    }
    context.distribution(metrics.Metrics.Name.FetchNulls, nulls)
    context.distribution(metrics.Metrics.Name.FetchExceptions, exceptions)
    context.distribution(metrics.Metrics.Name.FetchCount, responseMap.size)
  }

  def logFeatureNullRates(responseMap: Map[String, AnyRef], context: metrics.Metrics.Context): Unit = {
    import ai.chronon.online.metrics

    responseMap.foreach { case (featureName, value) =>
      if (!featureName.endsWith(FetcherUtil.FeatureExceptionSuffix)) {
        if (value == null)
          context.increment(
            metrics.Metrics.Name.FeatureNulls,
            Map(metrics.Metrics.Tag.Feature -> featureName)
          )
        context.increment(
          metrics.Metrics.Name.FeatureCount,
          Map(metrics.Metrics.Tag.Feature -> featureName)
        )
      }
    }
  }

  /** Response for a join schema request
    * @param joinName - Name of the join
    * @param keySchema - Avro schema string for the key
    * @param valueSchema - Avro schema string for the value
    * @param schemaHash - Hash of the join schema payload (used to track updates to key / value schema fields or types)
    * @param valueInfos - Per feature column metadata (e.g. group name, corresponding left lookup keys, ..)
    */
  case class JoinSchemaResponse(joinName: String,
                                keySchema: String,
                                valueSchema: String,
                                schemaHash: String,
                                valueInfos: Array[JoinCodec.ValueInfo])

  /** Response for a groupBy schema request. The inputSchema and selectedSchema fields are both returned to show schema
    * shape before and after select expressions, alongside groupByName, keySchema, and valueSchema.
    */
  case class GroupBySchemaResponse(groupByName: String,
                                   keySchema: String,
                                   valueSchema: String,
                                   inputSchema: String,
                                   selectedSchema: String)

  /** Response for a groupBy status request.
    * @param groupByName - Name of the groupBy
    * @param batchEndDate - Date through which batch upload data is available in the KV store
    */
  case class GroupByStatusResponse(groupByName: String, batchEndDate: String)
}

private[online] case class FetcherResponseWithTs[T <: BaseResponse](responses: Seq[T], endTs: Long)

// BaseFetcher + Logging + External service calls
class Fetcher(val kvStore: KVStore,
              metaDataSet: String,
              timeoutMillis: Long = 10000,
              logFunc: Consumer[LoggableResponse] = null,
              debug: Boolean = false,
              val externalSourceRegistry: ExternalSourceRegistry = null,
              val modelPlatformProvider: ModelPlatformProvider = null,
              callerName: String = null,
              flagStore: FlagStore = null,
              disableErrorThrows: Boolean = false,
              executionContextOverride: ExecutionContext = null,
              joinConfTtlMillis: Long = TTLCache.DefaultTtlMillis,
              joinCodecTtlMillis: Long = TTLCache.DefaultTtlMillis) {

  @transient implicit lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  private val fetchContext: FetchContext =
    FetchContext(kvStore,
                 metaDataSet,
                 timeoutMillis,
                 debug,
                 flagStore,
                 disableErrorThrows,
                 executionContextOverride,
                 joinConfTtlMillis,
                 joinCodecTtlMillis)

  implicit private val executionContext: ExecutionContext = fetchContext.getOrCreateExecutionContext
  val metadataStore: MetadataStore = new MetadataStore(fetchContext)
  private val joinPartFetcher = new JoinPartFetcher(fetchContext, metadataStore)

  lazy val joinCodecCache: TTLCache[String, Try[JoinCodec]] = metadataStore.buildJoinCodecCache(
    Some(logControlEvent)
  )

  private class JoinConfCacheKey(val joinName: String, val confHash: String, val join: api.Join) {
    override def equals(obj: Any): Boolean = obj match {
      case other: JoinConfCacheKey => joinName == other.joinName && confHash == other.confHash
      case _                       => false
    }

    override def hashCode(): Int = (joinName, confHash).hashCode()
  }

  private lazy val joinConfCodecCache: TTLCache[JoinConfCacheKey, Try[JoinCodec]] =
    new TTLCache[JoinConfCacheKey, Try[JoinCodec]](
      cacheKey => Try(metadataStore.buildJoinCodec(cacheKey.join, refreshOnFail = true)),
      cacheKey => Metrics.Context(environment = "join.codec.fetch", join = cacheKey.joinName),
      ttlMillis = fetchContext.joinCodecTtlMillis
    )

  private def joinConfCacheKey(join: api.Join): JoinConfCacheKey =
    new JoinConfCacheKey(join.metaData.getName, ThriftJsonCodec.md5Digest(join), join)

  // Generic withTs method that works with any TimestampableResponse
  private[online] def withTs[T <: BaseResponse](responses: Future[Seq[T]]): Future[FetcherResponseWithTs[T]] = {
    responses.map { response =>
      FetcherResponseWithTs(response, System.currentTimeMillis())
    }
  }

  def fetchGroupBys(requests: Seq[Request]): Future[Seq[Response]] = {
    joinPartFetcher.fetchGroupBys(requests)
  }

  def fetchJoin(requests: Seq[Request], joinConf: Option[api.Join] = None): Future[Seq[Response]] = {
    val ts = System.currentTimeMillis()
    val cachedJoinCodecsByName = mutable.Map.empty[String, Try[JoinCodec]]
    val joinCodecForName: String => Option[Try[JoinCodec]] = joinConf match {
      case Some(join) =>
        lazy val codecTry = joinConfCodecCache(joinConfCacheKey(join))
        _ => Some(codecTry)
      case None =>
        joinName => Some(cachedJoinCodecsByName.getOrElseUpdate(joinName, joinCodecCache(joinName)))
    }
    val internalResponsesF = joinPartFetcher.fetchJoins(requests, joinConf, joinCodecForName)
    val externalResponsesF = fetchExternal(requests)
    val combinedResponsesF =
      internalResponsesF.zip(externalResponsesF).map { case (internalResponses, externalResponses) =>
        val zipped = if (externalResponses == null) {
          internalResponses.map(_ -> null)
        } else {
          internalResponses.zip(externalResponses)
        }

        val derivedResults = zipped.map { case (internalResponse, externalResponse) =>
          val cleanInternalRequest = internalResponse.request.copy(context = None)
          val internalMap = internalResponse.values.getOrElse(Map(
            s"join_part_fetch${FetcherUtil.FeatureExceptionSuffix}" -> internalResponse.values.failed.get.traceString))

          val baseMap = if (externalResponse != null) {

            assert(
              cleanInternalRequest == externalResponse.request,
              s"""
                   |Logic error. Responses are not aligned to requests
                   |mismatching requests:  $cleanInternalRequest, ${externalResponse.request}
                   |  requests:            ${requests.map(_.name)}
                   |  internalResponses:   ${internalResponses.map(_.request.name)}
                   |  externalResponses:   ${externalResponses.map(_.request.name)}""".stripMargin
            )

            val externalMap = externalResponse.values.getOrElse(Map(
              s"external_part_fetch${FetcherUtil.FeatureExceptionSuffix}" -> externalResponse.values.failed.get.traceString))

            internalMap ++ externalMap
          } else {
            internalMap
          }

          applyDerivations(ts, internalResponse.request, baseMap)
        }

        val ctx = Metrics.Context(Metrics.Environment.JoinFetching)
        ctx.distribution("overall.latency.millis", System.currentTimeMillis() - ts)
        derivedResults
      }

    combinedResponsesF
      .map(_.iterator.map(logResponse(_, ts)).toSeq)
  }

  def fetchModelTransforms(requests: scala.Seq[Request],
                           modelTransformsConf: Option[api.ModelTransforms] = None): Future[scala.Seq[Response]] = {
    val modelTransformsFetcher = new ModelTransformsFetcher(modelPlatformProvider, debug)

    modelTransformsConf match {
      case Some(modelTransforms) =>
        fetchModelTransformsWithConf(requests, modelTransforms, modelTransformsFetcher)

      case None =>
        // Track original indices to maintain order
        val indexedRequests = requests.zipWithIndex
        val groupedByModelTransforms = indexedRequests.groupBy { case (req, _) => req.name }

        // Process each model transforms group
        val futuresWithIndices = groupedByModelTransforms.map { case (modelTransformsName, requestsWithIndices) =>
          val requestsOnly = requestsWithIndices.map(_._1)
          val indices = requestsWithIndices.map(_._2)

          // Look up the ModelTransforms conf from metadata store
          val modelTransformsConfTry = metadataStore.getModelTransformsConf(modelTransformsName)

          modelTransformsConfTry match {
            case Success(modelTransforms) =>
              val responseFuture = fetchModelTransformsWithConf(requestsOnly, modelTransforms, modelTransformsFetcher)
              // Pair responses with their original indices
              responseFuture.map(responses => responses.zip(indices))

            case Failure(exception) =>
              // Failed to fetch model transforms conf - refresh cache and return failure responses
              metadataStore.getModelTransformsConf.refresh(modelTransformsName)
              val failedResponses = requestsWithIndices.map { case (req, idx) =>
                (Response(
                   req,
                   Failure(
                     new IllegalArgumentException(
                       s"Failed to fetch model transforms conf for $modelTransformsName. Please ensure metadata upload succeeded.",
                       exception))
                 ),
                 idx)
              }
              Future.successful(failedResponses)
          }
        }

        // Combine all futures and reorder by original index
        Future.sequence(futuresWithIndices).map { allResponsesWithIndices =>
          val flattened = allResponsesWithIndices.flatten.toSeq
          val sortedByIndex = flattened.sortBy(_._2)
          sortedByIndex.map(_._1)
        }
    }
  }

  private def fetchModelTransformsWithConf(
      requests: scala.Seq[Request],
      modelTransforms: api.ModelTransforms,
      modelTransformsFetcher: ModelTransformsFetcher): Future[scala.Seq[Response]] = {
    val maybeJoinSource = modelTransforms.joinSource

    if (maybeJoinSource.nonEmpty) {
      val join = maybeJoinSource.get.join
      val joinRequests = requests.map { request =>
        Request(join.metaData.name, request.keys, request.atMillis, request.context)
      }

      val joinFuture = fetchJoin(joinRequests, Some(join))
      joinFuture.flatMap { joinResponses =>
        modelTransformsFetcher.fetchJoinSourceModelTransforms(requests, modelTransforms, joinResponses)
      }
    } else {
      modelTransformsFetcher.fetchModelTransforms(requests, modelTransforms)
    }
  }

  private def convertJoinFeaturesResponseToAvroBytes(features: Map[String, AnyRef],
                                                     joinName: String): Try[Array[Byte]] = {
    val startTime = System.currentTimeMillis()
    val ctx =
      Metrics.Context(Metrics.Environment.JoinSchemaFetching, join = joinName)
    val joinCodecTry = joinCodecCache(joinName)

    joinCodecTry.flatMap { joinCodec =>
      Try {
        val response = encode(joinCodec.valueSchema, joinCodec.valueCodec, features)
        ctx.distribution("avroconversionbytes.latency.millis", System.currentTimeMillis() - startTime)
        response
      }.recover { case exception =>
        logger.error(s"Failed to convert features to avro for $joinName", exception)
        throw exception
      }
    }
  }

  private def convertJoinFeaturesResponseToAvroString(features: Map[String, AnyRef], joinName: String): Try[String] = {
    val startTime = System.currentTimeMillis()
    val ctx =
      Metrics.Context(Metrics.Environment.JoinSchemaFetching, join = joinName)
    val joinCodecTry = joinCodecCache(joinName)

    joinCodecTry.flatMap { joinCodec =>
      Try {
        val avroBytes = encode(joinCodec.valueSchema, joinCodec.valueCodec, features)
        val avroString = java.util.Base64.getEncoder.encodeToString(avroBytes)
        ctx.distribution("avroconversionstring.latency.millis", System.currentTimeMillis() - startTime)
        avroString
      }.recover { case exception =>
        logger.error(s"Failed to convert features to avro for $joinName", exception)
        throw exception
      }
    }
  }

  def fetchJoinV2(requests: Seq[Request],
                  joinConf: Option[api.Join] = None,
                  responseType: ResponseType = FeaturesResponseType.Map): Future[Seq[ResponseV2]] = {
    val rawResponse = fetchJoin(requests, joinConf)

    responseType match {
      case FeaturesResponseType.AvroBytes => {
        rawResponse.map(
          _.iterator
            .map(r => {
              val errors = r.values match {
                case Failure(exception) => Failure(exception)
                case Success(valueMap) =>
                  val exceptionMap = FetcherUtil.filterFeatureMapForErrors(valueMap)
                  if (exceptionMap.nonEmpty) Success(exceptionMap) else Success(Map.empty[String, String])
              }
              ResponseV2(r.request,
                         AvroResponseValue.AvroBytes(r.values.flatMap(v => {
                           convertJoinFeaturesResponseToAvroBytes(v, r.request.name)
                         })),
                         errors)
            })
            .toSeq)
      }
      case FeaturesResponseType.AvroString =>
        rawResponse.map(
          _.iterator
            .map(r => {
              val errors = r.values match {
                case Failure(exception) => Failure(exception)
                case Success(valueMap) =>
                  val exceptionMap = FetcherUtil.filterFeatureMapForErrors(valueMap)
                  if (exceptionMap.nonEmpty) Success(exceptionMap) else Success(Map.empty[String, String])
              }
              ResponseV2(r.request,
                         AvroResponseValue.AvroString(r.values.flatMap(v => {
                           convertJoinFeaturesResponseToAvroString(v, r.request.name)
                         })),
                         errors)
            })
            .toSeq)
    }
  }

  private def applyDerivations(ts: Long, request: Request, baseMap: Map[String, AnyRef]): ResponseWithContext = {

    val derivationStartTs = System.currentTimeMillis()
    val joinName = request.name
    val ctx = Metrics.Context(Metrics.Environment.JoinFetching, join = joinName)
    val joinCodecTry = joinCodecCache(request.name)

    joinCodecTry match {
      case Success(joinCodec) =>
        ctx.distribution("derivation_codec.latency.millis", System.currentTimeMillis() - derivationStartTs)

        val derivedMapTry: Try[Map[String, AnyRef]] = Try {
          applyDeriveFunc(joinCodec.deriveFunc, request, baseMap)
        }

        val derivedMap: Map[String, AnyRef] = derivedMapTry match {
          case Success(derivedMap) => derivedMap
          case Failure(exception) =>
            ctx.incrementException(exception)

            val renameOnlyDerivedMapTry: Try[Map[String, AnyRef]] = Try {
              joinCodec
                .renameOnlyDeriveFunc(request.keys, baseMap)
                .mapValues(_.asInstanceOf[AnyRef])
                .toMap
            }

            val renameOnlyDerivedMap: Map[String, AnyRef] =
              renameOnlyDerivedMapTry match {
                case Success(renameOnlyDerivedMap) =>
                  renameOnlyDerivedMap
                case Failure(exception) =>
                  ctx.incrementException(exception)
                  Map(
                    s"derivation_rename${FetcherUtil.FeatureExceptionSuffix}" -> exception.traceString
                      .asInstanceOf[AnyRef])
              }

            val derivedExceptionMap: Map[String, AnyRef] =
              Map(
                s"derivation_fetch${FetcherUtil.FeatureExceptionSuffix}" -> exception.traceString
                  .asInstanceOf[AnyRef])

            renameOnlyDerivedMap ++ derivedExceptionMap
        }

        // Preserve exceptions from baseMap
        val baseMapExceptions = baseMap.filter(_._1.endsWith(FetcherUtil.FeatureExceptionSuffix))
        val finalizedDerivedMap = derivedMap ++ baseMapExceptions
        val requestEndTs = System.currentTimeMillis()
        ctx.distribution("derivation.latency.millis", requestEndTs - derivationStartTs)
        ctx.distribution("request.latency.millis", requestEndTs - ts)

        val response = ResponseWithContext(request, finalizedDerivedMap, baseMap)
        // Refresh joinCodec if it has partial failure
        if (joinCodec.hasPartialFailure) {
          joinCodecCache.refresh(joinName)
        }
        response

      case Failure(exception) =>
        // more validation logic will be covered in compile.py to avoid this case
        joinCodecCache.refresh(joinName)
        ctx.incrementException(exception)
        ResponseWithContext(request,
                            Map(s"join_codec_fetch${FetcherUtil.FeatureExceptionSuffix}" -> exception.traceString),
                            Map.empty)

    }
  }

  private def encode(schema: StructType,
                     codec: AvroCodec,
                     dataMap: Map[String, AnyRef],
                     cast: Boolean = false,
                     tries: Int = 3): Array[Byte] = {
    def encodeOnce(schema: StructType,
                   codec: AvroCodec,
                   dataMap: Map[String, AnyRef],
                   cast: Boolean = false): Array[Byte] = {
      val data = schema.castArr(dataMap)
      val avroRecord =
        AvroConversions.fromChrononRow(data, schema, codec.schema).asInstanceOf[GenericRecord]
      codec.encodeBinary(avroRecord)
    }

    @tailrec
    def tryOnce(lastTry: Try[Array[Byte]], tries: Int): Try[Array[Byte]] = {

      if (tries == 0 || (lastTry != null && lastTry.isSuccess))
        return lastTry

      val binary = encodeOnce(schema, codec, dataMap, cast)

      tryOnce(Try(codec.decodeRow(binary)).map(_ => binary), tries - 1)
    }

    tryOnce(null, tries).get
  }

  private def logResponse(resp: ResponseWithContext, ts: Long): Response = {

    val joinCodecTry = joinCodecCache(resp.request.name)

    val loggingTry: Try[Unit] = joinCodecTry
      .map(codec => {
        val metaData = codec.conf.join.metaData
        val samplePercent = if (metaData.isSetSamplePercent) metaData.getSamplePercent else 0

        if (samplePercent > 0)
          encodeAndPublishLog(resp, ts, codec, samplePercent)

      })

    loggingTry.failed.map { exception =>
      // to handle GroupByServingInfo staleness that results in encoding failure
      joinCodecCache.refresh(resp.request.name)

      resp.request.context.foreach(
        _.incrementException(new RuntimeException(s"Logging failed due to: ${exception.traceString}", exception)))
    }

    if (joinCodecTry.isSuccess && joinCodecTry.get.hasPartialFailure) {
      joinCodecCache.refresh(resp.request.name)
    }

    Response(resp.request, Success(resp.derivedValues))
  }

  private def encodeAndPublishLog(resp: ResponseWithContext,
                                  ts: Long,
                                  codec: JoinCodec,
                                  samplePercent: Double): Unit = {

    val loggingStartTs = System.currentTimeMillis()
    val loggingTs = resp.request.atMillis.getOrElse(ts)

    val keyBytes = encode(codec.keySchema, codec.keyCodec, resp.request.keys, cast = true)

    val hash = if (samplePercent > 0) {
      Math.abs(HashUtils.md5Long(keyBytes))
    } else {
      -1
    }

    val shouldPublishLog = (hash > 0) && ((hash % (100 * 1000)) <= (samplePercent * 1000))

    if (shouldPublishLog || debug) {
      val values = if (codec.conf.join.logFullValues) {
        resp.combinedValues
      } else {
        resp.derivedValues
      }

      if (debug) {
        logger.info(s"Logging ${resp.request.keys} : ${hash % 100000}: $samplePercent")
        val gson = new Gson()
        val valuesFormatted =
          values.map { case (k, v) => s"$k -> ${gson.toJson(v)}" }.mkString(", ")
        logger.info(s"""Sampled join fetch
               |Key Map: ${resp.request.keys}
               |Value Map: [$valuesFormatted]
               |""".stripMargin)
      }

      val valueBytes = encode(codec.valueSchema, codec.valueCodec, values)

      val loggableResponse = LoggableResponse(
        keyBytes,
        valueBytes,
        resp.request.name,
        loggingTs,
        codec.loggingSchemaHash
      )

      if (logFunc != null) {
        logFunc.accept(loggableResponse)

        val joinContext = resp.request.context

        joinContext.foreach(context => context.increment("logging_request.count"))
        joinContext.foreach(context =>
          context.distribution("logging_request.latency.millis", System.currentTimeMillis() - loggingStartTs))
        joinContext.foreach(context =>
          context.distribution("logging_request.overall.latency.millis", System.currentTimeMillis() - ts))

        if (debug) {
          logger.info(s"Logged data with schema_hash ${codec.loggingSchemaHash}")
        }
      }
    }
  }

  // Pulling external features in a batched fashion across services in-parallel
  private def fetchExternal(joinRequests: Seq[Request]): Future[Seq[Response]] = {

    val startTime = System.currentTimeMillis()
    val resultMap = new mutable.LinkedHashMap[Request, Try[mutable.HashMap[String, Any]]]
    var invalidCount = 0
    val validRequests = new ListBuffer[Request]

    // step-1 handle invalid requests and collect valid ones
    joinRequests.foreach { request =>
      val joinName = request.name
      val joinConfTry: Try[JoinOps] = metadataStore.getJoinConf(request.name)
      if (joinConfTry.isFailure) {
        metadataStore.getJoinConf.refresh(request.name)
        resultMap.update(
          request,
          Failure(
            new IllegalArgumentException(
              s"Failed to fetch join conf for $joinName. Please ensure metadata upload succeeded",
              joinConfTry.failed.get))
        )
        invalidCount += 1
      } else if (joinConfTry.get.join.onlineExternalParts == null) {
        resultMap.update(request, Success(mutable.HashMap.empty[String, Any]))
      } else {
        resultMap.update(request, Success(mutable.HashMap.empty[String, Any]))
        validRequests.append(request)
      }
    }

    // early exit if no external requests detected
    if (validRequests.isEmpty) { return Future.successful(null) }

    // step-2 dedup external requests across joins
    val externalToJoinRequests: Seq[ExternalToJoinRequest] = validRequests.flatMap { joinRequest =>
      val joinConf = metadataStore.getJoinConf(joinRequest.name)
      if (joinConf.isFailure) {
        metadataStore.getJoinConf.refresh(joinRequest.name)
      }
      val parts =
        metadataStore
          .getJoinConf(joinRequest.name)
          .get
          .join
          .onlineExternalParts // cheap since it is cached, valid since step-1

      parts.iterator().asScala.map { part =>
        // Selected left-key derivation is currently scoped to internal GroupBy join parts. External parts retain the
        // existing keyMapping behavior and require callers to provide any selected aliases used as external keys.
        val externalRequest = Try(part.applyMapping(joinRequest.keys)) match {
          case Success(mappedKeys)                     => Left(Request(part.source.metadata.name, mappedKeys))
          case Failure(exception: KeyMissingException) => Right(exception)
          case Failure(otherException)                 => throw otherException
        }
        ExternalToJoinRequest(externalRequest, joinRequest, part)
      }

    }.toSeq

    val validExternalRequestToJoinRequestMap = externalToJoinRequests
      .filter(_.externalRequest.isLeft)
      .groupBy(_.externalRequest.left.get)
      .mapValues(_.toSeq)
      .toMap

    val context =
      Metrics.Context(
        environment = Metrics.Environment.JoinFetching,
        join = validRequests.iterator.map(_.name.sanitize).toSeq.distinct.mkString(",")
      )
    context.distribution("response.external_pre_processing.latency", System.currentTimeMillis() - startTime)
    context.count("response.external_invalid_joins.count", invalidCount)
    val responseFutures =
      externalSourceRegistry.fetchRequests(validExternalRequestToJoinRequestMap.keys.toSeq, context)

    // step-3 walk the response, find all the joins to update and the result map
    responseFutures.map { responses =>
      responses.foreach { response =>
        val responseTry: Try[Map[String, Any]] = response.values
        val joinsToUpdate: Seq[ExternalToJoinRequest] =
          validExternalRequestToJoinRequestMap(response.request)

        joinsToUpdate.foreach { externalToJoin =>
          val resultValueMap: mutable.HashMap[String, Any] =
            resultMap(externalToJoin.joinRequest).get
          val prefix = externalToJoin.part.fullName + "_"
          responseTry match {
            case Failure(exception) =>
              resultValueMap.update(prefix + "exception", exception)
              externalToJoin.context.incrementException(exception)
            case Success(responseMap) =>
              externalToJoin.context.count("response.value_count", responseMap.size)
              responseMap.foreach { case (name, value) =>
                resultValueMap.update(prefix + name, value)
              }
          }
        }
      }

      externalToJoinRequests
        .filter(_.externalRequest.isRight)
        .foreach(externalToJoin => {

          val resultValueMap: mutable.HashMap[String, Any] =
            resultMap(externalToJoin.joinRequest).get
          val KeyMissingException = externalToJoin.externalRequest.right.get
          resultValueMap.update(externalToJoin.part.fullName + "_" + "exception", KeyMissingException)
          externalToJoin.context.incrementException(KeyMissingException)

        })

      // step-4 convert the resultMap into Responses
      joinRequests.map { req =>
        Metrics
          .Context(Metrics.Environment.JoinFetching, join = req.name)
          .distribution("external.latency.millis", System.currentTimeMillis() - startTime)
        Response(req, resultMap(req).map(_.mapValues(_.asInstanceOf[AnyRef]).toMap))
      }
    }
  }

  def fetchJoinSchema(joinName: String): Try[JoinSchemaResponse] = {
    val startTime = System.currentTimeMillis()
    val ctx =
      Metrics.Context(Metrics.Environment.JoinSchemaFetching, join = joinName)

    val joinCodecTry = joinCodecCache(joinName)

    val joinSchemaResponse = joinCodecTry
      .map { joinCodec =>
        val response = JoinSchemaResponse(joinName,
                                          joinCodec.keyCodec.schemaStr,
                                          joinCodec.valueCodec.schemaStr,
                                          joinCodec.loggingSchemaHash,
                                          joinCodec.valueInfos.toArray)
        if (joinCodec.hasPartialFailure) {
          joinCodecCache.refresh(joinName)
        }
        ctx.distribution("response.latency.millis", System.currentTimeMillis() - startTime)
        response
      }
      .recover { case exception =>
        logger.error(s"Failed to fetch join schema for $joinName", exception)
        ctx.incrementException(exception)
        throw exception
      }

    joinSchemaResponse
  }

  def fetchGroupBySchema(groupByName: String): Try[GroupBySchemaResponse] = {
    val startTime = System.currentTimeMillis()
    val ctx =
      Metrics.Context(Metrics.Environment.MetaDataFetching, groupBy = groupByName).withSuffix("group_by_schema")

    val groupByConfTry = metadataStore.getConf[api.GroupBy](ConfPathOrName(confName = Some(groupByName)))

    groupByConfTry
      .flatMap { groupByConf =>
        if (!groupByConf.metaData.online) {
          Failure(
            new IllegalArgumentException(
              s"GroupBy $groupByName is not online. Fetcher schema is only available for online GroupBys. " +
                "Use the Iceberg catalog schema via eval for the offline table schema, or enable online=True and upload the GroupBy."))
        } else {
          metadataStore.getGroupByServingInfo(groupByName)
        }
      }
      .map { servingInfo =>
        val response = GroupBySchemaResponse(groupByName,
                                             servingInfo.keyAvroSchema,
                                             servingInfo.responseAvroSchema,
                                             servingInfo.inputAvroSchema,
                                             servingInfo.selectedAvroSchema)
        ctx.distribution(Metrics.Name.LatencyMillis, System.currentTimeMillis() - startTime)
        response
      }
      .recover { case exception =>
        logger.error(s"Failed to fetch groupBy schema for $groupByName", exception)
        ctx.incrementException(exception)
        throw exception
      }
  }

  def fetchGroupByStatus(groupByName: String): Try[GroupByStatusResponse] = {
    val startTime = System.currentTimeMillis()
    val ctx =
      Metrics.Context(Metrics.Environment.MetaDataFetching, groupBy = groupByName).withSuffix("group_by_status")

    val groupByConfTry = metadataStore.getConf[api.GroupBy](ConfPathOrName(confName = Some(groupByName)))

    groupByConfTry
      .flatMap { groupByConf =>
        if (!groupByConf.metaData.online) {
          Failure(
            new IllegalArgumentException(
              s"GroupBy $groupByName is not online. Fetcher status is only available for online GroupBys. " +
                "Enable online=True and upload the GroupBy."))
        } else {
          metadataStore.getGroupByServingInfo(groupByName)
        }
      }
      .map { servingInfo =>
        val response = GroupByStatusResponse(groupByName, servingInfo.batchEndDate)
        ctx.distribution(Metrics.Name.LatencyMillis, System.currentTimeMillis() - startTime)
        response
      }
      .recover { case exception =>
        logger.error(s"Failed to fetch groupBy status for $groupByName", exception)
        ctx.incrementException(exception)
        throw exception
      }
  }

  private def logControlEvent(encTry: Try[JoinCodec]): Unit = {
    if (encTry.isFailure) return

    val enc = encTry.get
    val ts = System.currentTimeMillis()
    val controlEvent = LoggableResponse(
      enc.loggingSchemaHash.getBytes(UTF8),
      enc.loggingSchema.getBytes(UTF8),
      Constants.SchemaPublishEvent,
      ts,
      null
    )
    if (logFunc != null) {
      logFunc.accept(controlEvent)
      if (debug) {
        logger.info(s"schema data logged successfully with schema_hash ${enc.loggingSchemaHash}")
      }
    }
  }

  private case class ExternalToJoinRequest(externalRequest: Either[Request, KeyMissingException],
                                           joinRequest: Request,
                                           part: ExternalPart) {

    lazy val context: Metrics.Context =
      Metrics.Context(Metrics.Environment.JoinFetching, join = joinRequest.name, groupBy = part.fullName)
  }
}
