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

package ai.chronon.online

import ai.chronon.aggregator.row.{RowAggregator, StatsGenerator}
import ai.chronon.api.Extensions._
import ai.chronon.api.{Constants, Operation, SerdeUtils, StructType}
import ai.chronon.online.KVStore.GetRequest
import ai.chronon.online.serde.{AvroCodec, AvroConversions}
import ai.chronon.online.stats.DriftMetrics
import org.apache.avro.generic
import org.apache.datasketches.kll.KllFloatsSketch
import org.slf4j.{Logger, LoggerFactory}

import java.nio.charset.Charset
import java.time.{LocalDate, ZoneOffset}
import java.util.concurrent.{CompletableFuture, TimeUnit}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

/** Java-friendly service for fetching enhanced statistics from KV Store.
  * This is a lightweight version that doesn't require Spark dependencies.
  *
  * @param api The API instance for accessing KV stores
  * @param tableBaseName The base table name for the BigTable enhanced stats table (default: "ENHANCED_STATS")
  * @param datasetName The dataset name within the KV store (default: Constants.EnhancedStatsDataset)
  */
class JavaStatsService(api: Api,
                       tableBaseName: String = "ENHANCED_STATS",
                       datasetName: String = Constants.EnhancedStatsDataset)(implicit ec: ExecutionContext) {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  private val charset = Charset.forName("UTF-8")

  // Use the specialized enhanced stats KV store for efficient BigTable time-series operations
  @transient lazy val kvStore: KVStore = api.genEnhancedStatsKvStore(tableBaseName)

  // Must be explicitly enabled via env var — prevents accidental use of sharding before jobs have
  // been re-run with CHRONON_SHARD_ENHANCED_STATS enabled on the write side.
  private val shardingEnabled: Boolean =
    sys.env.getOrElse("CHRONON_SHARD_ENHANCED_STATS", "false").equalsIgnoreCase("true")

  /** Helper to retrieve both key and value schemas from KV Store in a single batch call */
  private def getSchemasFromKVStore(keySchemaKey: String,
                                    valueSchemaKey: String,
                                    semanticHash: Option[String]): (Option[AvroCodec], Option[AvroCodec]) = {
    try {
      val requests = Seq(
        GetRequest(JavaStatsService.prefixWithHash(semanticHash, keySchemaKey.getBytes(charset)),
                   datasetName,
                   None,
                   None),
        GetRequest(JavaStatsService.prefixWithHash(semanticHash, valueSchemaKey.getBytes(charset)),
                   datasetName,
                   None,
                   None)
      )

      val responses = Await.result(
        kvStore.multiGet(requests),
        Duration(10L, TimeUnit.SECONDS)
      )

      val keyCodecOpt = responses(0).values match {
        case Success(values) if values.nonEmpty =>
          val freshest = values.zipWithIndex.maxBy { case (tv, i) => (tv.millis, i) }._1
          val schemaString = new String(freshest.bytes, charset)
          logger.info(s"Successfully found schema: $keySchemaKey (ts=${freshest.millis}, versions=${values.size})")
          Some(new AvroCodec(schemaString))
        case _ =>
          logger.warn(s"Schema not found for key: $keySchemaKey")
          None
      }

      val valueCodecOpt = responses(1).values match {
        case Success(values) if values.nonEmpty =>
          val freshest = values.zipWithIndex.maxBy { case (tv, i) => (tv.millis, i) }._1
          val schemaString = new String(freshest.bytes, charset)
          logger.info(s"Successfully found schema: $valueSchemaKey (ts=${freshest.millis}, versions=${values.size})")
          Some(new AvroCodec(schemaString))
        case _ =>
          logger.warn(s"Schema not found for key: $valueSchemaKey")
          None
      }

      (keyCodecOpt, valueCodecOpt)
    } catch {
      case e: Exception =>
        logger.error(s"Failed to retrieve schemas for $keySchemaKey and $valueSchemaKey: ${e.getMessage}")
        (None, None)
    }
  }

  /** Fetch and merge statistics for a given table and time range.
    *
    * @param tableName The name of the table (used as key in KV store)
    * @param startTimeMillis Start of time range (inclusive)
    * @param endTimeMillis End of time range (inclusive)
    * @param semanticHash If set, reads from the shard written by the job with this config hash.
    *                     Pass null (Java) or None (Scala) to read un-sharded legacy data.
    * @return CompletableFuture of JavaStatsResponse with statistics or error
    */
  def fetchStats(tableName: String,
                 startTimeMillis: Long,
                 endTimeMillis: Long,
                 semanticHash: String): CompletableFuture[JavaStatsResponse] = {

    val requestedHash = Option(semanticHash)
    // When the flag is off, drop any hash the caller passed and read un-sharded data.
    // Log a warning so the frontend knows the hash was ignored rather than silently failing.
    val maybeHash = if (shardingEnabled) {
      requestedHash
    } else {
      requestedHash.foreach { h =>
        logger.warn(
          s"[$tableName] semanticHash=$h was passed but CHRONON_SHARD_ENHANCED_STATS is not enabled — " +
            s"ignoring hash and reading un-sharded data. Enable the flag or remove semanticHash from the request.")
      }
      None
    }

    val scalaFuture: Future[JavaStatsResponse] = Future {
      Try {
        // Retrieve schemas from KV Store in a single batch call
        val keySchemaKey = s"$tableName${Constants.TimedKvRDDKeySchemaKey}"
        val valueSchemaKey = s"$tableName${Constants.TimedKvRDDValueSchemaKey}"

        val (keyCodecOpt, valueCodecOpt) = getSchemasFromKVStore(keySchemaKey, valueSchemaKey, maybeHash)

        if (keyCodecOpt.isEmpty || valueCodecOpt.isEmpty) {
          val hashHint = maybeHash match {
            case Some(h) =>
              s" with semanticHash=$h. The stats job may not have been run yet with CHRONON_SHARD_ENHANCED_STATS enabled"
            case None =>
              " (no semanticHash — reading un-sharded data)"
          }
          throw new RuntimeException(s"Failed to retrieve schemas for $tableName$hashHint. Has it been uploaded?")
        }

        val keyCodec = keyCodecOpt.get
        val valueCodec = valueCodecOpt.get

        // Encode key using the stored key codec
        val chrononRow = Array[Any](tableName)
        val record = AvroConversions
          .fromChrononRow(chrononRow, keyCodec.chrononSchema, keyCodec.schema)
          .asInstanceOf[generic.GenericData.Record]
        val keyBytes = JavaStatsService.prefixWithHash(maybeHash, keyCodec.encodeBinary(record))

        val getRequest = GetRequest(
          keyBytes,
          datasetName,
          Some(startTimeMillis),
          Some(endTimeMillis)
        )

        // Fetch all IRs in the time range
        val responseFuture = kvStore.get(getRequest)
        val response = Await.result(responseFuture, Duration(30L, TimeUnit.SECONDS))

        response.values match {
          case Failure(exception) =>
            throw new RuntimeException(s"Failed to fetch stats for $tableName: ${exception.getMessage}", exception)

          case Success(timedValues) =>
            if (timedValues == null || timedValues.isEmpty) {
              throw new RuntimeException(s"No data found for $tableName in range [$startTimeMillis, $endTimeMillis]")
            } else {
              logger.info(s"Fetched ${timedValues.size} tiles for $tableName")

              // Fetch schemas (these are static metadata)
              val selectedSchemaOpt = getMetadataValue(s"$tableName/selectedSchema", None, maybeHash)
              val noKeysSchemaOpt = getMetadataValue(s"$tableName/noKeysSchema", None, maybeHash)

              if (selectedSchemaOpt.isEmpty || noKeysSchemaOpt.isEmpty) {
                throw new RuntimeException(s"Failed to retrieve schemas for $tableName. Metadata may be missing.")
              }

              // Parse schemas
              val selectedSchemaCodec = new AvroCodec(selectedSchemaOpt.get)
              val selectedSchema = selectedSchemaCodec.chrononSchema.asInstanceOf[StructType]
              val noKeysSchemaCodec = new AvroCodec(noKeysSchemaOpt.get)
              val noKeysSchema = noKeysSchemaCodec.chrononSchema.asInstanceOf[StructType]

              // Fallback: try to get from metadata row (backward compatibility)
              val cardinalityMapOpt = getMetadataValue(s"$tableName/cardinalityMap", Some(endTimeMillis), maybeHash)
              val mergedCardinalityMap = if (cardinalityMapOpt.isDefined) {
                val cardinalityJson = cardinalityMapOpt.get
                cardinalityJson
                  .stripPrefix("{")
                  .stripSuffix("}")
                  .split(",")
                  .map { pair =>
                    val parts = pair.split(":")
                    val key = parts(0).stripPrefix("\"").stripSuffix("\"")
                    val value = parts(1).toDouble
                    key -> value
                  }
                  .toMap
              } else {
                Map.empty[String, Double]
              }

              logger.info(s"Merged cardinality map for $tableName with ${mergedCardinalityMap.size} columns")

              // Build aggregator with merged cardinality map
              val noKeysFields = noKeysSchema.fields.map(f => (f.name, f.fieldType)).toSeq
              val enhancedMetrics = StatsGenerator.buildEnhancedMetrics(
                noKeysFields,
                mergedCardinalityMap,
                cardinalityThreshold = 0.01
              )

              // Filter metrics to only those whose inputColumn exists in selectedSchema.
              // The stored selectedSchema is the source of truth — it reflects what was actually computed
              // and stored in the tiles. Metrics referencing columns not present there correspond to newer
              // code logic (e.g., __str variants added after the last stats job run) that we simply can't
              // serve yet. We warn and degrade gracefully rather than failing the request entirely.
              val selectedSchemaColumns = selectedSchema.fields.map(_.name).toSet
              val (compatibleMetrics, skippedMetrics) = enhancedMetrics.partition { m =>
                selectedSchemaColumns.contains(s"${m.name}${m.suffix}")
              }
              if (skippedMetrics.nonEmpty) {
                val skippedCols = skippedMetrics.map(m => s"${m.name}${m.suffix}").distinct
                logger.warn(
                  s"[$tableName] ${skippedCols.size} metric input columns not found in stored selectedSchema " +
                    s"(schema may predate recent code changes; re-run the stats job to get full metrics). " +
                    s"Skipped: [${skippedCols.mkString(", ")}]"
                )
              }

              val aggregator = StatsGenerator.buildAggregator(compatibleMetrics, selectedSchema)

              // Merge all IRs after denormalizing (converts bytes back to sketch objects).
              // Tiles that fail to decode (e.g. written with an older IR schema) are skipped with a warning.
              var successfulTiles = 0
              var skippedTiles = 0
              val totalTiles = timedValues.size
              val mergedIr = timedValues.foldLeft(aggregator.init) { (acc, timedValue) =>
                Try {
                  val normalizedIr = valueCodec.decodeRow(timedValue.bytes)
                  val denormalizedIr = aggregator.denormalize(normalizedIr)
                  aggregator.merge(acc, denormalizedIr)
                } match {
                  case Success(mergedResult) =>
                    successfulTiles += 1
                    mergedResult
                  case Failure(e) =>
                    skippedTiles += 1
                    if (skippedTiles <= 3) {
                      logger.warn(
                        s"[$tableName] Skipping tile at ts=${timedValue.millis} " +
                          s"(${e.getClass.getSimpleName}: ${e.getMessage})")
                    }
                    acc
                }
              }

              logger.info(
                s"[$tableName] Tile decode summary: $successfulTiles/$totalTiles decoded, $skippedTiles skipped")

              // Finalize to get final statistics
              val normalized = aggregator.finalize(mergedIr)

              // Convert to Map for easy access
              val fieldNames = aggregator.outputSchema.map(_._1)
              val statsMap = fieldNames.zip(normalized).toMap

              logger.info(s"Merged $successfulTiles tiles into final statistics for $tableName")

              // Add derived features
              val enhancedStatsMap = addDerivedFeatures(statsMap)

              JavaStatsResponse.success(tableName, enhancedStatsMap.asJava, successfulTiles, skippedTiles)
            }
        }
      } match {
        case Success(response) => response
        case Failure(exception) =>
          logger.error("Exception found during response construction", exception)
          JavaStatsResponse.failure(exception.getMessage)
      }
    }

    // Convert Scala Future to Java CompletableFuture
    val promise = new CompletableFuture[JavaStatsResponse]()
    scalaFuture.onComplete {
      case Success(response)  => promise.complete(response)
      case Failure(exception) => promise.completeExceptionally(exception)
    }
    promise
  }

  /** Computes drift between two explicit enhanced-stats time ranges.
    *
    * The requested metric can be a full metric output name or a base feature name. Metric resolution prefers
    * percentile sketches, then histograms, then boolean true/false/null distributions. The returned distances are
    * L-inf, L2, and L1 over normalized distributions.
    */
  def fetchDrift(tableName: String,
                 referenceStartTimeMillis: Long,
                 referenceEndTimeMillis: Long,
                 comparisonStartTimeMillis: Long,
                 comparisonEndTimeMillis: Long,
                 metricName: String,
                 semanticHash: String): CompletableFuture[JavaStatsDriftResponse] = {

    val scalaFuture: Future[JavaStatsDriftResponse] = Future {
      Try {
        val requestedHash = Option(semanticHash).filter(_.nonEmpty)
        val maybeHash = effectiveSemanticHash(tableName, requestedHash)
        val drift = computeDrift(tableName,
                                 referenceStartTimeMillis,
                                 referenceEndTimeMillis,
                                 comparisonStartTimeMillis,
                                 comparisonEndTimeMillis,
                                 metricName,
                                 maybeHash)

        logger.info(
          s"Computed drift for $tableName metric=${drift.metricName} " +
            s"referenceTiles=${drift.referenceTiles}, comparisonTiles=${drift.comparisonTiles}")

        JavaStatsDriftResponse.success(
          tableName,
          drift.metricName,
          drift.distances.asJavaMap,
          drift.referenceTiles,
          drift.comparisonTiles,
          drift.skippedTiles
        )
      } match {
        case Success(response) => response
        case Failure(exception) =>
          logger.error("Exception found during drift response construction", exception)
          JavaStatsDriftResponse.failure(exception.getMessage)
      }
    }

    val promise = new CompletableFuture[JavaStatsDriftResponse]()
    scalaFuture.onComplete {
      case Success(response)  => promise.complete(response)
      case Failure(exception) => promise.completeExceptionally(exception)
    }
    promise
  }

  /** Computes a daily trailing drift series using cached decoded IRs.
    *
    * For each anchor date ds, the reference window is [ds - windowDays, ds) and the comparison window is the
    * immediately preceding window of equal size. Bounds are converted to inclusive millisecond ranges internally
    * because KV reads and cached window merges use inclusive timestamps.
    */
  def fetchTrailingDrift(tableName: String,
                         startDate: String,
                         endDate: String,
                         windowDays: Int,
                         metricName: String,
                         distanceName: String,
                         semanticHash: String): CompletableFuture[JavaStatsTrailingDriftResponse] = {
    val scalaFuture: Future[JavaStatsTrailingDriftResponse] = Future {
      Try {
        if (windowDays <= 0) {
          throw new IllegalArgumentException("windowDays must be greater than 0")
        }

        val start = LocalDate.parse(startDate)
        val end = LocalDate.parse(endDate)
        if (start.isAfter(end)) {
          throw new IllegalArgumentException("startDate must be less than or equal to endDate")
        }

        val distanceSelector = normalizeDistanceName(distanceName)
        val requestedHash = Option(semanticHash).filter(_.nonEmpty)
        val maybeHash = effectiveSemanticHash(tableName, requestedHash)
        val points = new java.util.ArrayList[JavaStatsTrailingDriftPoint]()
        val windowMillis = TimeUnit.DAYS.toMillis(windowDays.toLong)
        val firstAnchorMillis = start.atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli
        val lastAnchorMillis = end.atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli
        val fetchStartMillis = firstAnchorMillis - (2L * windowMillis)
        val fetchEndMillis = lastAnchorMillis - 1L
        val cached = fetchDecodedIrRange(tableName, fetchStartMillis, fetchEndMillis, maybeHash, "trailing")

        var date = start
        while (!date.isAfter(end)) {
          val anchorMillis = date.atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli
          val referenceStart = anchorMillis - windowMillis
          val referenceEnd = anchorMillis - 1L
          val comparisonEnd = referenceStart - 1L
          val comparisonStart = comparisonEnd - windowMillis + 1L

          val point = Try {
            val reference = mergeCachedWindow(cached, referenceStart, referenceEnd, "reference")
            val comparison = mergeCachedWindow(cached, comparisonStart, comparisonEnd, "comparison")
            val drift = computeDriftFromMerged(tableName, reference, comparison, metricName)
            JavaStatsTrailingDriftPoint.success(date.toString, selectDistance(drift.distances, distanceSelector))
          }.recover { case e: Exception =>
            logger.warn(s"Failed trailing drift point for $tableName ds=$date: ${e.getMessage}")
            JavaStatsTrailingDriftPoint.failure(date.toString, e.getMessage)
          }.get

          points.add(point)
          date = date.plusDays(1)
        }

        JavaStatsTrailingDriftResponse.success(tableName, distanceSelector.responseKey, points)
      } match {
        case Success(response) => response
        case Failure(exception) =>
          logger.error("Exception found during trailing drift response construction", exception)
          JavaStatsTrailingDriftResponse.failure(exception.getMessage)
      }
    }

    val promise = new CompletableFuture[JavaStatsTrailingDriftResponse]()
    scalaFuture.onComplete {
      case Success(response)  => promise.complete(response)
      case Failure(exception) => promise.completeExceptionally(exception)
    }
    promise
  }

  private case class DriftComputation(metricName: String,
                                      distances: DriftMetrics.LpDistances,
                                      referenceTiles: Int,
                                      comparisonTiles: Int,
                                      skippedTiles: Int)

  /** Fetches and merges both windows, then delegates to distribution-specific drift computation. */
  private def computeDrift(tableName: String,
                           referenceStartTimeMillis: Long,
                           referenceEndTimeMillis: Long,
                           comparisonStartTimeMillis: Long,
                           comparisonEndTimeMillis: Long,
                           metricName: String,
                           semanticHash: Option[String]): DriftComputation = {
    val reference =
      fetchMergedIr(tableName, referenceStartTimeMillis, referenceEndTimeMillis, semanticHash, "reference")
    val comparison =
      fetchMergedIr(tableName, comparisonStartTimeMillis, comparisonEndTimeMillis, semanticHash, "comparison")
    computeDriftFromMerged(tableName, reference, comparison, metricName)
  }

  /** Computes drift from already-merged reference and comparison IRs.
    *
    * This method supports percentile sketches, low-cardinality histograms, and boolean features represented by
    * true/null/total counters. It is shared by the single-window and trailing-series endpoints.
    */
  private def computeDriftFromMerged(tableName: String,
                                     reference: MergedIrResult,
                                     comparison: MergedIrResult,
                                     metricName: String): DriftComputation = {
    val selected = resolveDriftMetric(tableName, reference.aggregator, comparison.aggregator, metricName)
    val referenceValue = reference.mergedIr(selected.referenceIdx)
    val comparisonValue = comparison.mergedIr(selected.comparisonIdx)
    val distances = selected.operation match {
      case Operation.APPROX_PERCENTILE =>
        DriftMetrics.kllSketchDistances(referenceValue.asInstanceOf[KllFloatsSketch],
                                        comparisonValue.asInstanceOf[KllFloatsSketch])
      case Operation.HISTOGRAM =>
        DriftMetrics.histogramLpDistances(
          referenceValue.asInstanceOf[java.util.Map[String, _ <: Number]],
          comparisonValue.asInstanceOf[java.util.Map[String, _ <: Number]]
        )
      case Operation.SUM if selected.booleanParts.isDefined =>
        val referenceHistogram = booleanHistogram(reference.mergedIr, selected.booleanParts.get)
        val comparisonHistogram = booleanHistogram(comparison.mergedIr, selected.comparisonBooleanParts.get)
        DriftMetrics.histogramLpDistances(referenceHistogram, comparisonHistogram)
      case other =>
        throw new RuntimeException(s"Unsupported drift metric operation: $other")
    }

    DriftComputation(
      selected.name,
      distances,
      reference.successfulTiles,
      comparison.successfulTiles,
      reference.skippedTiles + comparison.skippedTiles
    )
  }

  private case class BooleanMetricParts(trueIdx: Int, nullIdx: Int, totalIdx: Int)
  private case class DriftMetricSelection(name: String,
                                          operation: Operation,
                                          referenceIdx: Int,
                                          comparisonIdx: Int,
                                          booleanParts: Option[BooleanMetricParts] = None,
                                          comparisonBooleanParts: Option[BooleanMetricParts] = None)

  /** Resolves the user metric parameter to a drift-capable aggregation in both windows.
    *
    * Exact output names are accepted. Base feature names try `<name>_approx_percentile`, `<name>_histogram`,
    * `<name>__str_histogram`, then `<name>_true_sum` for boolean features.
    */
  private def resolveDriftMetric(tableName: String,
                                 referenceAggregator: RowAggregator,
                                 comparisonAggregator: RowAggregator,
                                 metricName: String): DriftMetricSelection = {
    val referenceMetrics = driftMetricIndices(referenceAggregator)
    val comparisonMetrics = driftMetricIndices(comparisonAggregator).map(metric => metric.name -> metric).toMap

    if (referenceMetrics.isEmpty) {
      throw new RuntimeException(s"No percentile, histogram, or boolean metrics found for $tableName")
    }

    val selected = Option(metricName).map(_.trim).filter(_.nonEmpty) match {
      case Some(name) =>
        val candidates =
          if (name.endsWith("_approx_percentile") || name.endsWith("_histogram") || name.endsWith("_true_sum"))
            Seq(name)
          else Seq(s"${name}_approx_percentile", s"${name}_histogram", s"${name}__str_histogram", s"${name}_true_sum")
        candidates.flatMap(candidate => referenceMetrics.find(_.name == candidate)).headOption.getOrElse {
          throw new RuntimeException(
            s"Drift metric '$name' not found. Available metrics: ${referenceMetrics.map(_.name).mkString(", ")}")
        }
      case None if referenceMetrics.size == 1 =>
        referenceMetrics.head
      case None =>
        throw new RuntimeException(
          s"Multiple drift metrics found. Pass metric=<name>. Available metrics: ${referenceMetrics.map(_.name).mkString(", ")}")
    }

    val comparisonMetric = comparisonMetrics.getOrElse(
      selected.name,
      throw new RuntimeException(s"Drift metric '${selected.name}' is not available in comparison window")
    )
    selected.copy(comparisonIdx = comparisonMetric.referenceIdx, comparisonBooleanParts = comparisonMetric.booleanParts)
  }

  /** Lists all aggregation outputs that can be compared as distributions. */
  private def driftMetricIndices(aggregator: RowAggregator): Seq[DriftMetricSelection] =
    aggregator.aggregationParts.zipWithIndex.collect {
      case (part, idx) if part.operation == Operation.APPROX_PERCENTILE || part.operation == Operation.HISTOGRAM =>
        DriftMetricSelection(part.outputColumnName, part.operation, idx, idx)
    } ++ booleanMetricIndices(aggregator)

  /** Finds boolean features that have true, null, and total counters available. */
  private def booleanMetricIndices(aggregator: RowAggregator): Seq[DriftMetricSelection] = {
    val outputNameToIndex = aggregator.aggregationParts.zipWithIndex.map { case (part, idx) =>
      part.outputColumnName -> idx
    }.toMap
    val totalIdxOpt = outputNameToIndex.get("total_count")
    aggregator.aggregationParts.zipWithIndex.flatMap { case (part, idx) =>
      val name = part.outputColumnName
      if (part.operation == Operation.SUM && name.endsWith("_true_sum")) {
        val base = name.stripSuffix("_true_sum")
        for {
          nullIdx <- outputNameToIndex.get(s"${base}__null_sum")
          totalIdx <- totalIdxOpt
        } yield DriftMetricSelection(name, Operation.SUM, idx, idx, Some(BooleanMetricParts(idx, nullIdx, totalIdx)))
      } else {
        None
      }
    }
  }

  /** Builds the true/false/null count histogram for a boolean feature from merged stats IR counters. */
  private def booleanHistogram(ir: Array[Any], parts: BooleanMetricParts): java.util.Map[String, java.lang.Double] = {
    val trueCount = numberToDouble(ir(parts.trueIdx))
    val nullCount = numberToDouble(ir(parts.nullIdx))
    val totalCount = numberToDouble(ir(parts.totalIdx))
    val falseCount = math.max(0.0, totalCount - trueCount - nullCount)
    val result = new java.util.HashMap[String, java.lang.Double]()
    result.put("true", trueCount)
    result.put("false", falseCount)
    result.put("null", nullCount)
    result
  }

  private def numberToDouble(value: Any): Double =
    value match {
      case null      => 0.0
      case n: Number => n.doubleValue()
      case other => throw new RuntimeException(s"Expected numeric boolean count but found ${other.getClass.getName}")
    }

  private case class DistanceSelector(responseKey: String)

  private def normalizeDistanceName(distanceName: String): DistanceSelector = {
    Option(distanceName).map(_.trim.toLowerCase).filter(_.nonEmpty).getOrElse("linf") match {
      case "linf" | "l-inf" | "linf-distance" => DistanceSelector("linf-distance")
      case "l2" | "l2-distance"               => DistanceSelector("l2-distance")
      case "l1" | "l1-distance"               => DistanceSelector("l1-distance")
      case other =>
        throw new IllegalArgumentException(s"Unsupported distance '$other'. Expected one of: linf, l2, l1")
    }
  }

  private def selectDistance(distances: DriftMetrics.LpDistances, selector: DistanceSelector): Double =
    selector.responseKey match {
      case "linf-distance" => distances.linf
      case "l2-distance"   => distances.l2
      case "l1-distance"   => distances.l1
    }

  private case class MergedIrResult(aggregator: RowAggregator,
                                    mergedIr: Array[Any],
                                    successfulTiles: Int,
                                    skippedTiles: Int)

  private case class DecodedIrRange(aggregator: RowAggregator, irs: Seq[(Long, Array[Any])], skippedTiles: Int)

  private def mergeCachedWindow(cached: DecodedIrRange,
                                startTimeMillis: Long,
                                endTimeMillis: Long,
                                label: String): MergedIrResult = {
    val windowIrs = cached.irs.filter { case (millis, _) =>
      millis >= startTimeMillis && millis <= endTimeMillis
    }
    if (windowIrs.isEmpty) {
      throw new RuntimeException(s"No $label data found in range [$startTimeMillis, $endTimeMillis]")
    }
    MergedIrResult(
      cached.aggregator,
      mergeDecodedIrs(cached.aggregator, windowIrs.map(_._2)),
      windowIrs.size,
      cached.skippedTiles
    )
  }

  private def mergeDecodedIrs(aggregator: RowAggregator, irs: Seq[Array[Any]]): Array[Any] = {
    irs.foldLeft(aggregator.init) { (acc, ir) =>
      aggregator.merge(acc, aggregator.clone(ir))
    }
  }

  private def effectiveSemanticHash(tableName: String, requestedHash: Option[String]): Option[String] = {
    if (shardingEnabled) {
      requestedHash
    } else {
      requestedHash.foreach { h =>
        logger.warn(
          s"[$tableName] semanticHash=$h was passed but CHRONON_SHARD_ENHANCED_STATS is not enabled — " +
            s"ignoring hash and reading un-sharded data. Enable the flag or remove semanticHash from the request.")
      }
      None
    }
  }

  private def fetchMergedIr(tableName: String,
                            startTimeMillis: Long,
                            endTimeMillis: Long,
                            semanticHash: Option[String],
                            label: String): MergedIrResult = {
    val decoded = fetchDecodedIrRange(tableName, startTimeMillis, endTimeMillis, semanticHash, label)
    MergedIrResult(
      decoded.aggregator,
      mergeDecodedIrs(decoded.aggregator, decoded.irs.map(_._2)),
      decoded.irs.size,
      decoded.skippedTiles
    )
  }

  private def fetchDecodedIrRange(tableName: String,
                                  startTimeMillis: Long,
                                  endTimeMillis: Long,
                                  semanticHash: Option[String],
                                  label: String): DecodedIrRange = {
    val keySchemaKey = s"$tableName${Constants.TimedKvRDDKeySchemaKey}"
    val valueSchemaKey = s"$tableName${Constants.TimedKvRDDValueSchemaKey}"
    val (keyCodecOpt, valueCodecOpt) = getSchemasFromKVStore(keySchemaKey, valueSchemaKey, semanticHash)

    if (keyCodecOpt.isEmpty || valueCodecOpt.isEmpty) {
      throw new RuntimeException(s"Failed to retrieve schemas for $tableName while reading $label window")
    }

    val keyCodec = keyCodecOpt.get
    val valueCodec = valueCodecOpt.get
    val chrononRow = Array[Any](tableName)
    val record = AvroConversions
      .fromChrononRow(chrononRow, keyCodec.chrononSchema, keyCodec.schema)
      .asInstanceOf[generic.GenericData.Record]
    val keyBytes = JavaStatsService.prefixWithHash(semanticHash, keyCodec.encodeBinary(record))

    val response = Await.result(
      kvStore.get(GetRequest(keyBytes, datasetName, Some(startTimeMillis), Some(endTimeMillis))),
      Duration(30L, TimeUnit.SECONDS)
    )

    val timedValues = response.values match {
      case Failure(exception) =>
        throw new RuntimeException(s"Failed to fetch $label stats for $tableName: ${exception.getMessage}", exception)
      case Success(values) if values == null || values.isEmpty =>
        throw new RuntimeException(s"No $label data found for $tableName in range [$startTimeMillis, $endTimeMillis]")
      case Success(values) => values
    }

    val selectedSchemaOpt = getMetadataValue(s"$tableName/selectedSchema", None, semanticHash)
    val noKeysSchemaOpt = getMetadataValue(s"$tableName/noKeysSchema", None, semanticHash)
    if (selectedSchemaOpt.isEmpty || noKeysSchemaOpt.isEmpty) {
      throw new RuntimeException(s"Failed to retrieve metadata schemas for $tableName while reading $label window")
    }

    val selectedSchemaCodec = new AvroCodec(selectedSchemaOpt.get)
    val selectedSchema = selectedSchemaCodec.chrononSchema.asInstanceOf[StructType]
    val noKeysSchemaCodec = new AvroCodec(noKeysSchemaOpt.get)
    val noKeysSchema = noKeysSchemaCodec.chrononSchema.asInstanceOf[StructType]
    val cardinalityMap = fetchCardinalityMap(tableName, Some(endTimeMillis), semanticHash)

    val noKeysFields = noKeysSchema.fields.map(f => (f.name, f.fieldType)).toSeq
    val enhancedMetrics = StatsGenerator.buildEnhancedMetrics(noKeysFields, cardinalityMap, cardinalityThreshold = 0.01)
    val selectedSchemaColumns = selectedSchema.fields.map(_.name).toSet
    val (compatibleMetrics, skippedMetrics) = enhancedMetrics.partition { m =>
      selectedSchemaColumns.contains(s"${m.name}${m.suffix}")
    }
    if (skippedMetrics.nonEmpty) {
      val skippedCols = skippedMetrics.map(m => s"${m.name}${m.suffix}").distinct
      logger.warn(
        s"[$tableName] ${skippedCols.size} metric input columns not found in stored selectedSchema while reading $label window. " +
          s"Skipped: [${skippedCols.mkString(", ")}]"
      )
    }

    val aggregator = StatsGenerator.buildAggregator(compatibleMetrics, selectedSchema)
    var skippedTiles = 0
    val decodedIrs = timedValues
      .flatMap { timedValue =>
        Try {
          val normalizedIr = valueCodec.decodeRow(timedValue.bytes)
          timedValue.millis -> aggregator.denormalize(normalizedIr)
        } match {
          case Success(decoded) =>
            Some(decoded)
          case Failure(e) =>
            skippedTiles += 1
            if (skippedTiles <= 3) {
              logger.warn(
                s"[$tableName] Skipping $label tile at ts=${timedValue.millis} " +
                  s"(${e.getClass.getSimpleName}: ${e.getMessage})")
            }
            None
        }
      }
      .sortBy(_._1)

    if (decodedIrs.isEmpty) {
      throw new RuntimeException(s"No $label tiles could be decoded for $tableName")
    }

    DecodedIrRange(aggregator, decodedIrs, skippedTiles)
  }

  /** Fetch metadata (cardinalityMap, selectedSchema, noKeysSchema) from KV Store
    *
    * @param tableName The table name
    * @param endTimeMillis Optional end time to get the latest cardinalityMap up to this time
    */
  private def fetchMetadata(
      tableName: String,
      endTimeMillis: Option[Long] = None): Option[(Map[String, Double], StructType, StructType)] = {
    val cardinalityMapKey = s"$tableName/cardinalityMap"
    val selectedSchemaKey = s"$tableName/selectedSchema"
    val noKeysSchemaKey = s"$tableName/noKeysSchema"

    try {
      // Fetch all three metadata values
      // cardinalityMap is time-dependent, fetch within time range to get the latest
      val cardinalityMapOpt = getMetadataValue(cardinalityMapKey, endTimeMillis, None)
      val selectedSchemaOpt = getMetadataValue(selectedSchemaKey, None, None)
      val noKeysSchemaOpt = getMetadataValue(noKeysSchemaKey, None, None)

      if (cardinalityMapOpt.isEmpty || selectedSchemaOpt.isEmpty || noKeysSchemaOpt.isEmpty) {
        logger.warn(s"Missing metadata for $tableName")
        return None
      }

      // Deserialize from JSON/Avro formats
      // Parse cardinality map from simple JSON format: {"col1":0.001,"col2":0.85}
      // Values are normalized ratios (distinct / total_rows).
      val cardinalityMapJson = cardinalityMapOpt.get
      val cardinalityMap = cardinalityMapJson
        .stripPrefix("{")
        .stripSuffix("}")
        .split(",")
        .map { pair =>
          val parts = pair.split(":")
          val key = parts(0).stripPrefix("\"").stripSuffix("\"")
          val value = parts(1).toDouble
          key -> value
        }
        .toMap

      // Parse schemas from Avro JSON format
      val selectedSchemaCodec = new AvroCodec(selectedSchemaOpt.get)
      val selectedSchema = selectedSchemaCodec.chrononSchema.asInstanceOf[StructType]

      val noKeysSchemaCodec = new AvroCodec(noKeysSchemaOpt.get)
      val noKeysSchema = noKeysSchemaCodec.chrononSchema.asInstanceOf[StructType]

      Some((cardinalityMap, selectedSchema, noKeysSchema))
    } catch {
      case e: Exception =>
        logger.error(s"Failed to fetch metadata for $tableName: ${e.getMessage}")
        None
    }
  }

  /** Helper to fetch a single metadata value from KV Store
    *
    * @param key The metadata key
    * @param endTimeMillis Optional end time for time-dependent metadata (gets latest value up to this time)
    * @param semanticHash If set, prefixes the key to target the correct shard
    */
  private def getMetadataValue(key: String,
                               endTimeMillis: Option[Long],
                               semanticHash: Option[String]): Option[String] = {
    try {
      val keyBytes = JavaStatsService.prefixWithHash(semanticHash, key.getBytes(charset))
      val response = Await.result(
        kvStore.get(GetRequest(keyBytes, datasetName, None, endTimeMillis)),
        Duration(10L, TimeUnit.SECONDS)
      )
      response.values match {
        case Success(values) if values.nonEmpty =>
          // BigTable returns cells in descending timestamp order (newest first), but we use maxBy
          // to be explicit and robust to any ordering differences across cell versions.
          val freshest = values.zipWithIndex.maxBy { case (tv, i) => (tv.millis, i) }._1
          logger.info(s"Using metadata for key: $key (ts=${freshest.millis}, versions=${values.size})")
          Some(new String(freshest.bytes, charset))
        case _ =>
          logger.warn(s"Metadata not found for key: $key")
          None
      }
    } catch {
      case e: Exception =>
        logger.error(s"Failed to retrieve metadata for $key: ${e.getMessage}")
        None
    }
  }

  private def fetchCardinalityMap(tableName: String,
                                  endTimeMillis: Option[Long],
                                  semanticHash: Option[String]): Map[String, Double] = {
    getMetadataValue(s"$tableName/cardinalityMap", endTimeMillis, semanticHash)
      .map(parseCardinalityMap)
      .getOrElse(Map.empty)
  }

  private def parseCardinalityMap(cardinalityJson: String): Map[String, Double] = {
    val trimmed = Option(cardinalityJson).map(_.trim).getOrElse("")
    if (trimmed.length <= 2) {
      Map.empty
    } else {
      trimmed
        .stripPrefix("{")
        .stripSuffix("}")
        .split(",")
        .flatMap { pair =>
          pair.split(":", 2) match {
            case Array(rawKey, rawValue) =>
              Try(rawValue.toDouble).toOption.map { value =>
                rawKey.trim.stripPrefix("\"").stripSuffix("\"") -> value
              }
            case _ => None
          }
        }
        .toMap
    }
  }

  /** Build a RowAggregator from stored metadata.
    * Reconstructs the original aggregator using cardinalityMap, selectedSchema, and noKeysSchema.
    *
    * @param tableName The table name
    * @param endTimeMillis Optional end time to get the latest cardinalityMap
    */
  private def buildAggregatorFromMetadata(tableName: String,
                                          endTimeMillis: Option[Long] = None): Option[RowAggregator] = {
    fetchMetadata(tableName, endTimeMillis).map { case (cardinalityMap, selectedSchema, noKeysSchema) =>
      logger.info(s"Reconstructing aggregator for $tableName with ${cardinalityMap.size} columns")

      // Rebuild the enhanced metrics using the stored metadata
      // buildEnhancedMetrics expects Seq[(String, DataType)]
      val noKeysFields = noKeysSchema.fields.map(f => (f.name, f.fieldType)).toSeq
      val enhancedMetrics = StatsGenerator.buildEnhancedMetrics(
        noKeysFields,
        cardinalityMap,
        cardinalityThreshold = 0.01
      )

      val selectedSchemaColumns = selectedSchema.fields.map(_.name).toSet
      val (compatibleMetrics, skippedMetrics) = enhancedMetrics.partition { m =>
        selectedSchemaColumns.contains(s"${m.name}${m.suffix}")
      }
      if (skippedMetrics.nonEmpty) {
        val skippedCols = skippedMetrics.map(m => s"${m.name}${m.suffix}").distinct
        logger.warn(
          s"[$tableName] ${skippedCols.size} metric input columns not found in stored selectedSchema " +
            s"(schema may predate recent code changes; re-run the stats job to get full metrics). " +
            s"Skipped: [${skippedCols.mkString(", ")}]"
        )
      }

      // Build the aggregator with the reconstructed metrics
      StatsGenerator.buildAggregator(compatibleMetrics, selectedSchema)
    }
  }

  /** Overload for callers that don't use semantic-hash sharding (reads un-sharded legacy data). */
  def fetchStats(tableName: String, startTimeMillis: Long, endTimeMillis: Long): CompletableFuture[JavaStatsResponse] =
    fetchStats(tableName, startTimeMillis, endTimeMillis, null)

  def fetchDrift(tableName: String,
                 referenceStartTimeMillis: Long,
                 referenceEndTimeMillis: Long,
                 comparisonStartTimeMillis: Long,
                 comparisonEndTimeMillis: Long,
                 metricName: String): CompletableFuture[JavaStatsDriftResponse] =
    fetchDrift(tableName,
               referenceStartTimeMillis,
               referenceEndTimeMillis,
               comparisonStartTimeMillis,
               comparisonEndTimeMillis,
               metricName,
               null)

  def fetchTrailingDrift(tableName: String,
                         startDate: String,
                         endDate: String,
                         windowDays: Int,
                         metricName: String,
                         distanceName: String): CompletableFuture[JavaStatsTrailingDriftResponse] =
    fetchTrailingDrift(tableName, startDate, endDate, windowDays, metricName, distanceName, null)

  /** Add derived features to the statistics map.
    * Computes additional metrics based on existing statistics:
    * - std_dev: Standard deviation from variance (sqrt of variance)
    * - false_sum: Count of false values (total_count - true_sum - null_sum)
    *
    * @param statsMap The original statistics map
    * @return Enhanced statistics map with derived features
    */
  private def addDerivedFeatures(statsMap: Map[String, Any]): Map[String, Any] = {
    var enhanced = statsMap

    // Add standard deviation for variance fields
    statsMap.foreach { case (key, value) =>
      if (key.endsWith("_variance")) {
        val baseKey = key.stripSuffix("_variance")
        val stdDevKey = s"${baseKey}_std_dev"
        value match {
          case v: Double => enhanced = enhanced + (stdDevKey -> Math.sqrt(v))
          case v: Float  => enhanced = enhanced + (stdDevKey -> Math.sqrt(v.toDouble))
          case _         => // Skip non-numeric variance
        }
      }
    }

    // Add false_sum for true_sum fields
    statsMap.foreach { case (key, value) =>
      if (key.endsWith("_true_sum")) {
        val baseKey = key.stripSuffix("_true_sum")
        val falseSumKey = s"${baseKey}_false_sum"
        val nullSumKey = s"${baseKey}__null_sum"

        (statsMap.get("total_count"), value, statsMap.get(nullSumKey)) match {
          case (Some(total: Long), trueSum: Long, Some(nullSum: Long)) =>
            enhanced = enhanced + (falseSumKey -> (total - trueSum - nullSum))
          case _ => // Skip if required fields are missing
        }
      }
    }

    enhanced
  }
}

object JavaStatsService {
  private val charset = java.nio.charset.Charset.forName("UTF-8")

  // Prepend "<hash>/" to key bytes when a semanticHash is provided, isolating data by config version.
  // None = no prefix (backward compatible with data written before sharding was introduced).
  def prefixWithHash(hash: Option[String], keyBytes: Array[Byte]): Array[Byte] =
    hash.fold(keyBytes)(h => s"$h/".getBytes(charset) ++ keyBytes)
}

/** Java-friendly response object for stats queries
  */
case class JavaStatsResponse(
    success: Boolean,
    tableName: String,
    statistics: java.util.Map[String, Any],
    tilesCount: Int,
    skippedTilesCount: Int,
    errorMessage: String
) {
  def isSuccess: Boolean = success
  def getStatistics: java.util.Map[String, Any] = statistics
  def getTilesCount: Int = tilesCount
  def getSkippedTilesCount: Int = skippedTilesCount
  def getErrorMessage: String = errorMessage
  def getTableName: String = tableName
}

object JavaStatsResponse {
  def success(tableName: String,
              stats: java.util.Map[String, Any],
              tilesCount: Int,
              skippedTilesCount: Int = 0): JavaStatsResponse =
    JavaStatsResponse(true, tableName, stats, tilesCount, skippedTilesCount, null)

  def failure(errorMessage: String): JavaStatsResponse =
    JavaStatsResponse(false, null, java.util.Collections.emptyMap(), 0, 0, errorMessage)
}

case class JavaStatsDriftResponse(
    success: Boolean,
    tableName: String,
    metricName: String,
    distances: java.util.Map[String, java.lang.Double],
    referenceTilesCount: Int,
    comparisonTilesCount: Int,
    skippedTilesCount: Int,
    errorMessage: String
) {
  def isSuccess: Boolean = success
  def getTableName: String = tableName
  def getMetricName: String = metricName
  def getDistances: java.util.Map[String, java.lang.Double] = distances
  def getReferenceTilesCount: Int = referenceTilesCount
  def getComparisonTilesCount: Int = comparisonTilesCount
  def getSkippedTilesCount: Int = skippedTilesCount
  def getErrorMessage: String = errorMessage
}

object JavaStatsDriftResponse {
  def success(tableName: String,
              metricName: String,
              distances: java.util.Map[String, java.lang.Double],
              referenceTilesCount: Int,
              comparisonTilesCount: Int,
              skippedTilesCount: Int): JavaStatsDriftResponse =
    JavaStatsDriftResponse(true,
                           tableName,
                           metricName,
                           distances,
                           referenceTilesCount,
                           comparisonTilesCount,
                           skippedTilesCount,
                           null)

  def failure(errorMessage: String): JavaStatsDriftResponse =
    JavaStatsDriftResponse(false, null, null, java.util.Collections.emptyMap(), 0, 0, 0, errorMessage)
}

case class JavaStatsTrailingDriftPoint(x: String, y: java.lang.Double, errorMessage: String) {
  def getX: String = x
  def getY: java.lang.Double = y
  def getErrorMessage: String = errorMessage
  def isSuccess: Boolean = errorMessage == null
}

object JavaStatsTrailingDriftPoint {
  def success(x: String, y: Double): JavaStatsTrailingDriftPoint =
    JavaStatsTrailingDriftPoint(x, java.lang.Double.valueOf(y), null)

  def failure(x: String, errorMessage: String): JavaStatsTrailingDriftPoint =
    JavaStatsTrailingDriftPoint(x, null, errorMessage)
}

case class JavaStatsTrailingDriftResponse(
    success: Boolean,
    tableName: String,
    distanceName: String,
    points: java.util.List[JavaStatsTrailingDriftPoint],
    errorMessage: String
) {
  def isSuccess: Boolean = success
  def getTableName: String = tableName
  def getDistanceName: String = distanceName
  def getPoints: java.util.List[JavaStatsTrailingDriftPoint] = points
  def getErrorMessage: String = errorMessage
}

object JavaStatsTrailingDriftResponse {
  def success(tableName: String,
              distanceName: String,
              points: java.util.List[JavaStatsTrailingDriftPoint]): JavaStatsTrailingDriftResponse =
    JavaStatsTrailingDriftResponse(true, tableName, distanceName, points, null)

  def failure(errorMessage: String): JavaStatsTrailingDriftResponse =
    JavaStatsTrailingDriftResponse(false, null, null, java.util.Collections.emptyList(), errorMessage)
}
