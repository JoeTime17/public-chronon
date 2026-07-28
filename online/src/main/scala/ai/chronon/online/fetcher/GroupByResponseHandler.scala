package ai.chronon.online.fetcher
import ai.chronon.aggregator.windowing
import ai.chronon.aggregator.windowing.{FinalBatchIr, SawtoothOnlineAggregator, TiledIr}
import ai.chronon.api.Extensions.WindowOps
import ai.chronon.api.ScalaJavaConversions.{IteratorOps, JMapOps}
import ai.chronon.api.{DataModel, Row, Window}
import ai.chronon.online.serde.AvroConversions
import ai.chronon.online.GroupByServingInfoParsed
import ai.chronon.online.KVStore.TimedValue
import ai.chronon.online.metrics.Metrics.Name
import ai.chronon.online.fetcher.FetcherCache.{BatchResponses, CachedBatchResponse, KvStoreBatchResponse}
import ai.chronon.online.metrics.Metrics
import ai.chronon.online.{ERROR, ThrottledLogging, WARN}
import com.google.gson.Gson
import org.slf4j.{Logger, LoggerFactory}

import java.util
import scala.util.{Failure, Success, Try}

class GroupByResponseHandler(fetchContext: FetchContext, metadataStore: MetadataStore)
    extends FetcherCache
    with ThrottledLogging {

  @transient private implicit lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  case class RequestContext(
      servingInfo: GroupByServingInfoParsed,
      queryTimeMs: Long, // event time
      startTimeMs: Long, // clock time
      metricsContext: Metrics.Context,
      keys: Map[String, Any]
  )

  def decodeAndMerge(batchResponses: BatchResponses,
                     streamingResponsesOpt: Option[Seq[TimedValue]],
                     requestContext: RequestContext): Map[String, AnyRef] = {

    val newServingInfo = getServingInfo(requestContext.servingInfo, batchResponses)

    // Batch metrics
    batchResponses match {
      case kvStoreResponse: KvStoreBatchResponse =>
        kvStoreResponse.response.map(
          reportKvResponse(requestContext.metricsContext.withSuffix("batch"), _, requestContext.queryTimeMs)
        )
      case _: CachedBatchResponse => // no-op;
    }

    // The bulk upload may not have removed an older batch values. We manually discard all but the latest one.
    val batchBytes: Array[Byte] = batchResponses.getBatchBytes(newServingInfo.batchEndTsMillis)

    val responseMap: Map[String, AnyRef] =
      if (newServingInfo.groupBy.aggregations == null || streamingResponsesOpt.isEmpty) { // no-agg

        val batchResponseDecodeStartTime = System.currentTimeMillis()
        val response = getMapResponseFromBatchResponse(batchResponses,
                                                       batchBytes,
                                                       newServingInfo.outputCodec.decodeMap,
                                                       newServingInfo,
                                                       requestContext.keys)
        requestContext.metricsContext.distribution("group_by.batchir_decode.latency.millis",
                                                   System.currentTimeMillis() - batchResponseDecodeStartTime)
        response

      } else { // temporal accurate

        val streamingResponses = streamingResponsesOpt.get
        val output: Array[Any] = mergeWithStreaming(batchResponses,
                                                    streamingResponses,
                                                    batchBytes,
                                                    requestContext.copy(servingInfo = newServingInfo))

        val fieldNames = newServingInfo.outputCodec.fieldNames
        if (output != null) {
          fieldNames.iterator
            .zip(output.iterator.map(v => if (v == null) null else v.asInstanceOf[AnyRef]))
            .toMap
        } else {
          fieldNames.map(_ -> null).toMap
        }
      }

    requestContext.metricsContext.distribution("group_by.latency.millis",
                                               System.currentTimeMillis() - requestContext.startTimeMs)
    responseMap
  }

  private def mergeWithStreaming(batchResponses: BatchResponses,
                                 streamingResponses: Seq[TimedValue],
                                 batchBytes: Array[Byte],
                                 requestContext: RequestContext): Array[Any] = {

    val servingInfo = requestContext.servingInfo
    val mutations: Boolean = servingInfo.groupByOps.dataModel == DataModel.ENTITIES
    val aggregator: SawtoothOnlineAggregator = servingInfo.aggregator

    if (aggregator.batchEndTs > requestContext.queryTimeMs) {
      requestContext.metricsContext.incrementException(
        new IllegalArgumentException(
          s"Request time of $requestContext.queryTimeMs is less than batch time ${aggregator.batchEndTs}" +
            s" for groupBy ${servingInfo.groupByOps.metaData.getName}"))
    } else if (
      // Check if there's no streaming data.
      (streamingResponses == null || streamingResponses.isEmpty) &&
      // Check if there's no batch data. This is only possible if the batch response is from a KV Store request
      // (KvStoreBatchResponse) that returned null bytes. It's not possible to have null batch data with cached batch
      // responses as we only cache non-null data.
      (batchResponses.isInstanceOf[KvStoreBatchResponse] && batchBytes == null)
    ) {
      if (fetchContext.debug)
        logger.info("Both batch and streaming data are null")
      return aggregator.windowedAggregator.finalize(aggregator.windowedAggregator.init)
    }

    // Streaming metrics
    reportKvResponse(requestContext.metricsContext.withSuffix("streaming"),
                     streamingResponses,
                     requestContext.queryTimeMs)

    // If caching is enabled, we try to fetch the batch IR from the cache so we avoid the work of decoding it.
    val batchIrDecodeStartTime = System.currentTimeMillis()
    val batchIr: FinalBatchIr =
      getBatchIrFromBatchResponse(batchResponses, batchBytes, servingInfo, toBatchIr, requestContext.keys)
    requestContext.metricsContext.distribution("group_by.batchir_decode.latency.millis",
                                               System.currentTimeMillis() - batchIrDecodeStartTime)

    // check if we have late batch data for this GroupBy resulting in degraded counters
    val degradedCount = checkLateBatchData(
      requestContext.queryTimeMs,
      servingInfo.groupBy.metaData.name,
      servingInfo.batchEndTsMillis,
      aggregator.tailBufferMillis,
      aggregator.perWindowAggs.map(_.window)
    )
    requestContext.metricsContext.count("group_by.degraded_counter.count", degradedCount)

    if (fetchContext.isTilingEnabled) {
      mergeTiledIrsFromStreaming(requestContext, servingInfo, streamingResponses, aggregator, batchIr)
    } else {
      mergeRawEventsFromStreaming(requestContext.queryTimeMs,
                                  servingInfo,
                                  streamingResponses,
                                  mutations,
                                  aggregator,
                                  batchIr)
    }
  }

  private def mergeRawEventsFromStreaming(queryTimeMs: Long,
                                          servingInfo: GroupByServingInfoParsed,
                                          streamingResponses: Seq[TimedValue],
                                          mutations: Boolean,
                                          aggregator: SawtoothOnlineAggregator,
                                          batchIr: FinalBatchIr): Array[Any] = {

    val selectedCodec = servingInfo.groupByOps.dataModel match {
      case DataModel.EVENTS   => servingInfo.valueAvroCodec
      case DataModel.ENTITIES => servingInfo.mutationValueAvroCodec
    }

    def decodeRow(timedValue: TimedValue): Row = {
      val gbName = servingInfo.groupByOps.metaData.getName
      Try(selectedCodec.decodeRow(timedValue.bytes, timedValue.millis, mutations)) match {
        case Success(row) => row
        case Failure(_) =>
          logThrottled(ERROR,
                       s"decode_streaming_row_$gbName",
                       s"Failed to decode streaming row for groupBy $gbName" +
                         "Streaming rows will be ignored")

          if (servingInfo.groupByOps.dontThrowOnDecodeFailFlag) {
            null
          } else {
            throw new RuntimeException(s"Failed to decode streaming row for groupBy $gbName")
          }
      }
    }

    val streamingRows: Array[Row] =
      if (streamingResponses == null) Array.empty
      else
        streamingResponses.iterator
          .filter(tVal => tVal.millis >= servingInfo.batchEndTsMillis)
          .map(decodeRow)
          .filter(_ != null)
          .toArray

    if (fetchContext.debug) {
      val gson = new Gson()
      logger.info(s"""
                     |batch ir: ${gson.toJson(batchIr)}
                     |streamingRows: ${gson.toJson(streamingRows)}
                     |batchEnd in millis: ${servingInfo.batchEndTsMillis}
                     |queryTime in millis: $queryTimeMs
                     |""".stripMargin)
    }

    aggregator.lambdaAggregateFinalized(batchIr, streamingRows.iterator, queryTimeMs, mutations)
  }

  private def mergeTiledIrsFromStreaming(requestContext: RequestContext,
                                         servingInfo: GroupByServingInfoParsed,
                                         streamingResponses: Seq[TimedValue],
                                         aggregator: SawtoothOnlineAggregator,
                                         batchIr: FinalBatchIr): Array[Any] = {

    val streamingIrs: Iterator[TiledIr] = if (streamingResponses == null) {
      null
    } else {

      val allStreamingIrDecodeStartTime = System.currentTimeMillis()

      val tileIterator = streamingResponses.iterator
        .filter(tVal => tVal.millis >= servingInfo.batchEndTsMillis)
        .flatMap { tVal =>
          Try(servingInfo.tiledCodec.decodeTileIr(tVal.bytes)) match {
            case Success((tile, _)) => Array(TiledIr(tVal.millis, tile))
            case Failure(_) =>
              logThrottled(
                ERROR,
                s"decode_tile_ir_${servingInfo.groupByOps.metaData.getName}",
                s"Failed to decode tile ir for groupBy ${servingInfo.groupByOps.metaData.getName}" +
                  "Streaming tiled IRs will be ignored"
              )
              val groupByFlag: Option[Boolean] = Option(fetchContext.flagStore)
                .map(_.isSet(
                  "disable_streaming_decoding_error_throws",
                  Map(
                    "group_by_streaming_dataset" -> servingInfo.groupByServingInfo.groupBy.getMetaData.getName).toJava))
              if (groupByFlag.getOrElse(fetchContext.disableErrorThrows)) {
                Array.empty[TiledIr]
              } else {
                throw new RuntimeException(
                  s"Failed to decode tile ir for groupBy ${servingInfo.groupByOps.metaData.getName}")
              }
          }
        }
        .toArray
        .iterator

      requestContext.metricsContext.distribution("group_by.all_streamingir_decode.latency.millis",
                                                 System.currentTimeMillis() - allStreamingIrDecodeStartTime)

      tileIterator
    }

    if (fetchContext.debug) {
      val gson = new Gson()
      logger.info(s"""
                     |batch ir: ${gson.toJson(batchIr)}
                     |streamingIrs: ${if (streamingIrs != null) { gson.toJson(streamingIrs) }
                    else { "null" }}
                     |batchEnd in millis: ${servingInfo.batchEndTsMillis}
                     |queryTime in millis: ${requestContext.queryTimeMs}
                     |""".stripMargin)
    }

    val aggregatorStartTime = System.currentTimeMillis()
    val result = aggregator.lambdaAggregateFinalizedTiled(batchIr, streamingIrs, requestContext.queryTimeMs)
    requestContext.metricsContext.distribution("group_by.aggregator.latency.millis",
                                               System.currentTimeMillis() - aggregatorStartTime)
    result
  }

  private def reportKvResponse(ctx: Metrics.Context, response: Seq[TimedValue], queryTsMillis: Long): Unit = {
    if (response == null) return
    val latestResponseTs = response.iterator.map(_.millis).reduceOption(_ max _)
    val responseBytes = response.iterator.map(_.bytes.length).sum
    val context = ctx.withSuffix("response")
    context.distribution(Name.RowCount, response.length)
    context.distribution(Name.Bytes, responseBytes)
    latestResponseTs.foreach { ts =>
      context.distribution(Name.FreshnessMillis, queryTsMillis - ts)
      context.distribution(Name.FreshnessMinutes, (queryTsMillis - ts) / 60000)
    }
  }

  /** Get the latest serving information based on a batch response.
    *
    * The underlying metadata store used to store the latest GroupByServingInfoParsed will be updated if needed.
    *
    * @param existingServingInfo The previous serving information before fetching the latest KV store data.
    * @param batchResponses the latest batch responses (either a fresh KV store response or a cached batch ir).
    * @return the GroupByServingInfoParsed containing the latest serving information.
    */
  private[online] def getServingInfo(existingServingInfo: GroupByServingInfoParsed,
                                     batchResponses: BatchResponses): GroupByServingInfoParsed = {

    batchResponses match {

      case _: CachedBatchResponse =>
        // If there was cached batch data, there's no point in trying to update the serving info; it would be the same.
        // However, there's one edge case to be handled. If all batch requests are cached, and we never hit the kv store,
        // we will never try to update the serving info. In that case, if new batch data were to land, we would never
        // know of it. So, we force a refresh here to ensure that we are still periodically asynchronously hitting the
        // KV store to update the serving info. (See CHIP-1)
        metadataStore.getGroupByServingInfo.refresh(existingServingInfo.groupByOps.metaData.name)
        existingServingInfo

      case batchTimedValuesTry: KvStoreBatchResponse =>
        batchTimedValuesTry.response match {

          case Failure(_)                                       => existingServingInfo
          case Success(value) if value == null || value.isEmpty => existingServingInfo

          case Success(value) if value.iterator.map(_.millis).max <= existingServingInfo.batchEndTsMillis =>
            existingServingInfo

          case Success(_) =>
            metadataStore.getGroupByServingInfo
              .force(existingServingInfo.groupBy.metaData.name)
              .getOrElse(existingServingInfo)
        }
    }
  }

  private def toBatchIr(bytes: Array[Byte], gbInfo: GroupByServingInfoParsed): FinalBatchIr = {
    if (bytes == null) return null
    val batchRecord = gbInfo.irAvroToChrononRowConverter(gbInfo.irCodec.decode(bytes))
    val collapsed = gbInfo.aggregator.windowedAggregator.denormalize(batchRecord(0).asInstanceOf[Array[Any]])
    val tailHops = batchRecord(1)
      .asInstanceOf[util.ArrayList[Any]]
      .iterator()
      .toScala
      .map(
        _.asInstanceOf[util.ArrayList[Any]]
          .iterator()
          .toScala
          .map(hop => gbInfo.aggregator.baseAggregator.denormalizeInPlace(hop.asInstanceOf[Array[Any]]))
          .toArray)
      .toArray
    windowing.FinalBatchIr(collapsed, tailHops)
  }

  // This method checks if there's a longer gap between the batch end and the query time than the tail buffer duration
  // This indicates we're missing batch data for too long and if there are groupBy aggregations that include a longer
  // lookback window than the tail buffer duration, it means that we are serving degraded counters.
  private[online] def checkLateBatchData(queryTimeMs: Long,
                                         groupByName: String,
                                         batchEndTsMillis: Long,
                                         tailBufferMillis: Long,
                                         windows: Seq[Window]): Long = {
    val groupByContainsLongerWinThanTailBuffer = windows.exists(p => p.millis > tailBufferMillis)
    if (queryTimeMs > (tailBufferMillis + batchEndTsMillis) && groupByContainsLongerWinThanTailBuffer) {
      logThrottled(
        WARN,
        s"stale_batch_data_$groupByName",
        s"Encountered a request for $groupByName at $queryTimeMs which is more than $tailBufferMillis ms after the " +
          s"batch dataset landing at $batchEndTsMillis. "
      )
      1L
    } else
      0L
  }
}
