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

package ai.chronon.aggregator.windowing

import ai.chronon.api.Extensions.AggregationPartOps
import ai.chronon.api.Extensions.WindowOps
import ai.chronon.api._
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import scala.collection.mutable

// Wrapper class for handling Irs in the tiled chronon use case
case class TiledIr(ts: Long, ir: Array[Any])

// batchEndTs = upload time of the batch data as derived from GroupByServingInfo & Cached
// cache = Jul-22 / latest = Jul-23, streaming data = 22 - now (filter < jul 23)
class SawtoothOnlineAggregator(val batchEndTs: Long,
                               aggregations: Seq[Aggregation],
                               inputSchema: Seq[(String, DataType)],
                               resolution: Resolution = FiveMinuteResolution,
                               tailBufferMillis: Long = new Window(2, TimeUnit.DAYS).millis)
    extends SawtoothMutationAggregator(aggregations: Seq[Aggregation],
                                       inputSchema: Seq[(String, DataType)],
                                       resolution: Resolution,
                                       tailBufferMillis: Long) {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  // logically, batch response is arranged like so
  // sum-90d =>  sum_ir_88d, [(sum_ir_1d, ts)] -> 1d is the hopSize for 90d
  // sum-1d =>   null, [(sum_ir_1h, ts)]  -> 1h is the hopSize for 1d
  // sum-1h =>   null, [(sum_ir_5min, ts)] -> 5min is the hopSize for 1h
  // sum-7d =>   sum_ir_5d, [(sum_ir_1h, ts)]
  // practically - the ts part is very repetitive - often repeating itself across all features once
  //              - the additional tuple nesting comes with its own overhead
  // So we store the tail hops separately.
  // 1d_irs - [(txn-sum, login-count, ..., ts)]
  // 1h_irs - [(txn-sum, login-count, ..., ts)]
  // 5min_irs - [(txn-sum, login-count, ..., ts)]

  val batchTailTs: Array[Option[Long]] = tailTs(batchEndTs)

  logger.info(s"Batch End: ${TsUtils.toStr(batchEndTs)}")
  logger.info("Window Tails: ")
  for (i <- windowMappings.indices) {
    logger.info(s"  ${windowMappings(i).aggregationPart.outputColumnName} -> ${batchTailTs(i).map(TsUtils.toStr)}")
  }

  def update(batchIr: BatchIr, row: Row): BatchIr = update(batchEndTs, batchIr, row, batchTailTs)

  def normalizeBatchIr(batchIr: BatchIr): FinalBatchIr =
    FinalBatchIr(
      windowedAggregator.normalize(batchIr.collapsed),
      Option(batchIr.tailHops)
        .map(hopsAggregator.toTimeSortedArray)
        .map(_.map(_.map { baseAggregator.normalizeInPlace }))
        .orNull
    )

  def denormalizeBatchIr(batchIr: FinalBatchIr): FinalBatchIr =
    FinalBatchIr(
      windowedAggregator.denormalize(batchIr.collapsed),
      Option(batchIr.tailHops)
        .map(_.map(_.map { baseAggregator.denormalizeInPlace }))
        .orNull
    )

  // TODO: We can cache aggregated values in a very interesting way
  //   - we would need to cache (tailHops, collapsed,  cumulative_streamingRows, latestStreamingTs)
  //   - upon a cache hit we would need to
  //        1. Scan everything from streaming only after latestStreamingTs
  def lambdaAggregateIr(finalBatchIr: FinalBatchIr,
                        streamingRows: Iterator[Row],
                        queryTs: Long,
                        hasReversal: Boolean = false): Array[Any] = {
    // null handling
    if (finalBatchIr == null && streamingRows == null) return null
    val batchIr = Option(finalBatchIr).getOrElse(normalizeBatchIr(init))
    val headRows = Option(streamingRows).getOrElse(Array.empty[Row].iterator)

    if (batchEndTs > queryTs) {
      throw new IllegalArgumentException(s"Request time of $queryTs is less than batch time $batchEndTs")
    }

    // initialize with collapsed
    val resultIr = windowedAggregator.clone(batchIr.collapsed)

    // add head events
    while (headRows.hasNext) {
      val row = headRows.next()
      val rowTs = row.ts // unbox long only once

      val shouldSelect = if (hasReversal) {
        // mutation case
        val mutationTs = row.mutationTs
        val rowBeforeQuery = queryTs > rowTs && queryTs > mutationTs
        val rowAfterBatchEnd = mutationTs >= batchEndTs
        rowBeforeQuery && rowAfterBatchEnd
      } else {
        // event case
        val rowBeforeQuery = queryTs > rowTs
        val rowAfterBatchEnd = rowTs >= batchEndTs
        rowBeforeQuery && rowAfterBatchEnd
      }

      if (shouldSelect) {
        updateIr(resultIr, row, queryTs, hasReversal)
      }
    }
    mergeTailHops(resultIr, queryTs, batchEndTs, batchIr)
    resultIr
  }

  def lambdaAggregateIrTiled(finalBatchIr: FinalBatchIr,
                             streamingTiledIrs: Iterator[TiledIr],
                             queryTs: Long): Array[Any] = {
    // null handling
    if (finalBatchIr == null && streamingTiledIrs == null) return null
    val batchIr = Option(finalBatchIr).getOrElse(normalizeBatchIr(init))
    val tiledIrs = Option(streamingTiledIrs).getOrElse(Array.empty[TiledIr].iterator)

    if (batchEndTs > queryTs) {
      throw new IllegalArgumentException(s"Request time of $queryTs is less than batch time $batchEndTs")
    }

    // initialize with collapsed
    val resultIr = windowedAggregator.clone(batchIr.collapsed)

    // add streaming tiled irs
    while (tiledIrs.hasNext) {
      val tiledIr = tiledIrs.next()
      val tiledIrTs = tiledIr.ts // unbox long only once
      if (queryTs > tiledIrTs && tiledIrTs >= batchEndTs) {
        updateIrTiled(resultIr, tiledIr, queryTs)
      }
    }
    mergeTailHops(resultIr, queryTs, batchEndTs, batchIr)
    resultIr
  }

  def lambdaAggregateFinalized(finalBatchIr: FinalBatchIr,
                               streamingRows: Iterator[Row],
                               ts: Long,
                               hasReversal: Boolean = false): Array[Any] = {
    windowedAggregator.finalize(lambdaAggregateIr(finalBatchIr, streamingRows, ts, hasReversal = hasReversal))
  }

  def lambdaAggregateFinalizedTiled(finalBatchIr: FinalBatchIr,
                                    streamingTiledIrs: Iterator[TiledIr],
                                    ts: Long): Array[Any] = {
    windowedAggregator.finalize(lambdaAggregateIrTiled(finalBatchIr, streamingTiledIrs, ts))
  }

  // example: window size = 30d
  //          collapsed = 28d
  //          tail (tailBufferMillis = 2d) = [1d hops covering (28d, 30d)]
  // tails hops could be shared (between 4d, 5d), collapsed are always separate per output column

  // output cols = [txn_count_5d, txn_count_7d, ...]
  //             = [collapsed_5d, collapsed_7d, ...]
  //             tailHops = only 1 array of ir for collapsed
  //               [  tail_1d  ]
  //               [tail_4d_ago]
  //               [tail_5d_ago]
  //               [tail_6d_ago]
  //               [tail_7d_ago]

  // nullCounts = {txn_count_5d -> 100, txn_count_7d -> 200, ...}

  // batch upload boundary is the (end_date + 24h) of the batch being uploaded
  def updateNullCounts(batchIr: FinalBatchIr, nullCounts: mutable.Map[String, Long]): Unit = {
    var i: Int = 0

    // 1 Aggregation can have multiple windows (output columns), 1 aggregation part maps to 1 output col
    // windowed agg will unpack [7d, 1d, 1h, None] into separate aggregationParts,
    // it will have one part per output col
    while (i < windowedAggregator.length) {
      val window = windowMappings(i).aggregationPart.window

      val hasNonNullCollapsed = batchIr.collapsed(i) != null
      val hasNonNull = if (window != null) { // no hops for unwindowed
        val windowMillis = windowMappings(i).millis
        val hopIndex = tailHopIndices(i)
        val queryTail = TsUtils.round(batchEndTs - windowMillis, hopSizes(hopIndex))

        val hopIrs = batchIr.tailHops(hopIndex)
        var idx: Int = 0

        lazy val hasNonNullTailHop = {
          // check if any of the relevant tail hops for this window has a non-null value
          var hasNonNullTailHop_ = false

          while (idx < hopIrs.length && !hasNonNullTailHop_) {
            val hopIr: HopsAggregator.HopIr = hopIrs(idx)

            import ai.chronon.aggregator.windowing.HopsAggregator.HopIrOps
            val hopStartTimeStamp = hopIr.getTs

            // Only want to inspect tail hops that fall within the tailBuffer (default 2d), and after the queryTail.
            // Hop-aligned to mirror mergeTailHops' cutoff for non-hop-aligned batch ends.
            if (queryTail + tailBufferMillis > hopStartTimeStamp && hopStartTimeStamp >= queryTail) {
              val tailHop = hopIr(baseIrIndices(i))
              if (tailHop != null) {
                hasNonNullTailHop_ = true
              }
            }
            idx += 1
          }
          hasNonNullTailHop_
        }

        hasNonNullCollapsed || hasNonNullTailHop

      } else {
        hasNonNullCollapsed
      }

      val colName = windowMappings(i).aggregationPart.outputColumnName
      if (!hasNonNull) {
        val currentCount = nullCounts.getOrElse(colName, 0L)
        nullCounts.update(colName, currentCount + 1L)
      }

      i += 1
    }
  }

}
