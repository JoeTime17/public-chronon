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

package ai.chronon.spark.other

import ai.chronon.aggregator.row.StatsGenerator
import ai.chronon.api.{Constants, Operation}
import ai.chronon.online.InMemoryKvStore
import ai.chronon.spark.AvroKvEncoder
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.stats.{EnhancedStatsCompute, EnhancedStatsStore}
import ai.chronon.spark.utils.{MockApi, SparkTestBase}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import org.slf4j.{Logger, LoggerFactory}

class EnhancedStatsComputeTest extends SparkTestBase {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  implicit val tableUtils: TableUtils = TableUtils(spark)

  override protected def sparkConfs: Map[String, String] = Map(
    "spark.driver.host" -> "localhost",
    "spark.driver.bindAddress" -> "127.0.0.1"
  )

  // Sample to ~100K rows (5%) to keep tests fast while preserving cardinality patterns
  // (13K users, 16K movies, 10 ratings → all remain distinguishable at any threshold).
  // Cached so the CSV is only read once across all tests.
  lazy val ratingsData: DataFrame = {
    val csvPath = getClass.getClassLoader.getResource("local_data_csv/rating.csv.gz").getPath
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(csvPath)
      .sample(0.05)
      .withColumn(tableUtils.partitionColumn,
          date_format(to_timestamp(col("timestamp")), "yyyy-MM-dd"))
      .withColumn(Constants.TimeColumn,
          unix_timestamp(to_timestamp(col("timestamp"))).cast(LongType) * 1000)
      .cache()
  }

  it should "detect cardinality correctly for ratings data" in {
    val df = ratingsData

    logger.info(s"Loaded ${df.count()} rating records")
    df.show(10, truncate = false)
    df.printSchema()

    // Check time range
    import org.apache.spark.sql.functions._
    val timeRange = df.select(
      min(col("timestamp")).as("min_ts"),
      max(col("timestamp")).as("max_ts"),
      approx_count_distinct(date_format(col("timestamp"), "yyyy-MM-dd")).as("unique_days"),
      approx_count_distinct(date_format(col("timestamp"), "yyyy-MM-dd HH")).as("unique_hours")
    ).collect().head

    logger.info(s"\nTime Range:")
    logger.info(s"  First record: ${timeRange.getString(0)}")
    logger.info(s"  Last record: ${timeRange.getString(1)}")
    logger.info(s"  Unique days: ${timeRange.getLong(2)}")
    logger.info(s"  Unique hours: ${timeRange.getLong(3)}")

    val enhancedStats = new EnhancedStatsCompute(
      inputDf = df,
      keys = Seq.empty, // No keys, we want stats on all columns
      name = "ratings_test",
      cardinalityThreshold = 0.01
    )

    // Check cardinality detection
    val cardinalityMap = enhancedStats.cardinalityMap
    logger.info("Cardinality Map (normalized distinct/total_rows):")
    cardinalityMap.foreach { case (col, card) =>
      logger.info(s"  $col: $card (${if (card <= 0.01) "LOW" else "HIGH"} cardinality)")
    }

    // Verify expected cardinalities
    assert(cardinalityMap.contains("userId"), "Should detect userId cardinality")
    assert(cardinalityMap.contains("movieId"), "Should detect movieId cardinality")
    assert(cardinalityMap.contains("rating"), "Should detect rating cardinality")

    // Rating should be low cardinality (0.5-5.0 scale, ~10 distinct values out of ~100K rows → ratio << 0.01)
    assert(cardinalityMap("rating") <= 0.01, s"Rating should be low cardinality, got ${cardinalityMap("rating")}")
  }

  it should "only compute null stats for date and timestamp columns" in {
    val data = Seq(
      ("2026-04-27 21:45:05.343", "2026-04-27", 10L),
      ("2026-04-27 21:45:06.343", "2026-04-27", 20L),
      (null, null, 30L)
    )
    val df = spark
      .createDataFrame(data)
      .toDF("created_at", "impressed_at", "score")
      .withColumn("created_at", to_timestamp(col("created_at")))
      .withColumn("impressed_at", to_date(col("impressed_at")))
      .withColumn(tableUtils.partitionColumn, lit("2026-04-27"))

    val enhancedStats = new EnhancedStatsCompute(
      inputDf = df,
      keys = Seq.empty,
      name = "datetime_stats_test",
      cardinalityThreshold = 0.01
    )

    val metricsByColumn = enhancedStats.enhancedMetrics.groupBy(_.name)
    assert(metricsByColumn("created_at").map(_.suffix) == Seq(StatsGenerator.nullSuffix))
    assert(metricsByColumn("impressed_at").map(_.suffix) == Seq(StatsGenerator.nullSuffix))

    val scoreOperations = enhancedStats.enhancedMetrics.filter(_.name == "score").map(_.operation).toSet
    assert(scoreOperations.contains(Operation.MAX), "Numeric columns should still receive numeric stats")
    assert(scoreOperations.contains(Operation.MIN), "Numeric columns should still receive numeric stats")

    val (resultDf, _) = enhancedStats.enhancedDailySummary(
      sample = 1.0,
      timeBucketMinutes = 0
    )
    assert(resultDf.count() == 1, "Should compute one daily stats tile")
  }

  it should "generate enhanced metrics based on cardinality" in {
    val df = ratingsData

    val enhancedStats = new EnhancedStatsCompute(
      inputDf = df,
      keys = Seq.empty,
      name = "ratings_enhanced_metrics",
      cardinalityThreshold = 0.01
    )

    // Check the enhanced metrics
    logger.info("\nEnhanced Metrics Generated:")
    enhancedStats.enhancedMetrics.groupBy(_.name).foreach { case (colName, metrics) =>
      logger.info(s"\n  Column: $colName")
      metrics.foreach { m =>
        logger.info(s"    - ${m.operation} (${m.expression}, suffix: ${m.suffix})")
      }
    }

    // Verify that numeric columns get appropriate metrics
    val ratingMetrics = enhancedStats.enhancedMetrics.filter(_.name == "rating")
    val ratingOperations = ratingMetrics.map(_.operation).toSet

    // Rating is numeric, should have: null, zero, max, min, avg, variance, approx_unique_count, approx_percentile
    assert(ratingOperations.contains(Operation.SUM), "Should have SUM for null/zero counting")
    assert(ratingOperations.contains(Operation.MAX), "Should have MAX for numeric column")
    assert(ratingOperations.contains(Operation.MIN), "Should have MIN for numeric column")
    assert(ratingOperations.contains(Operation.AVERAGE), "Should have AVERAGE for numeric column")
    assert(ratingOperations.contains(Operation.VARIANCE), "Should have VARIANCE for numeric column")
    assert(ratingOperations.contains(Operation.HISTOGRAM), "Should have HISTOGRAM since it's low cardinality")

    logger.info(s"\n✓ Rating column has ${ratingMetrics.size} metrics: ${ratingOperations.mkString(", ")}")
  }

  it should "compute daily enhanced summary statistics" in {
    val df = ratingsData

    val enhancedStats = new EnhancedStatsCompute(
      inputDf = df,
      keys = Seq.empty,
      name = "ratings_daily_stats",
      cardinalityThreshold = 0.01
    )

    // Generate daily tiles (timeBucketMinutes = 0 means daily aggregation)
    val (resultDf, _) = enhancedStats.enhancedDailySummary(
      sample = 1.0,
      timeBucketMinutes = 0 // Daily tiles
    )

    logger.info("\nDaily Enhanced Statistics:")
    logger.info(s"Number of daily tiles: ${resultDf.count()}")
    resultDf.show(10, truncate = false)
    resultDf.printSchema()

    // Verify the schema contains our enhanced metrics
    val columns = resultDf.columns.toSet

    logger.info(s"Columns in result: ${columns.mkString(", ")}")

    // Should have basic stats
    assert(columns.exists(_.contains("rating")), "Should have rating columns")

    // Check for enhanced metrics on rating column
    assert(columns.exists(_.toLowerCase.contains("null")), "Should have null count metrics")
    assert(columns.exists(_.toLowerCase.contains("zero")), "Should have zero count metrics")
    assert(columns.exists(_.toLowerCase.contains("total")), "Should have total count")

    logger.info(s"✓ Generated ${resultDf.count()} daily tiles with enhanced statistics")
    logger.info(s"✓ Schema has ${columns.size} columns including all enhanced metrics")
  }

  it should "verify statistics are correct for numeric columns" in {
    val df = ratingsData

    val enhancedStats = new EnhancedStatsCompute(
      inputDf = df,
      keys = Seq.empty,
      name = "ratings_verification",
      cardinalityThreshold = 0.01
    )

    // Generate statistics (daily tiles)
    val (resultDf, _) = enhancedStats.enhancedDailySummary(
      sample = 1.0,
      timeBucketMinutes = 0 // Daily tiles
    )

    logger.info("\nStatistics Verification:")
    resultDf.show(10, truncate = false)

    // Get the first row (daily aggregate) - use take(1) instead of collect() to avoid OOM
    val firstRow = resultDf.take(1).head

    // Extract some statistics
    val schema = resultDf.schema
    val columnMap = firstRow.getValuesMap[Any](schema.fieldNames)

    logger.info("\nKey Statistics:")
    columnMap.filter(_._1.contains("rating")).foreach { case (k, v) =>
      logger.info(s"  $k: $v")
    }

    // Verify we have meaningful data - find total count column (case insensitive)
    val totalCountKey = columnMap.keys.find(_.toLowerCase.contains("total"))
    assert(totalCountKey.isDefined, s"Should have total count column. Available columns: ${columnMap.keys.mkString(", ")}")

    val totalCount = columnMap(totalCountKey.get)
    assert(totalCount.asInstanceOf[Long] > 0, "Total count should be positive")

    logger.info(s"✓ Total count (${totalCountKey.get}): ${totalCount}")
  }

  it should "handle mixed cardinality columns appropriately" in {
    val df = ratingsData

    // Test with different ratio thresholds to see how it affects metric generation.
    // Thresholds are fractions of total rows: 0.001 = 0.1%, 0.01 = 1%, 0.1 = 10%.
    val thresholds = Seq(0.001, 0.01, 0.1)

    thresholds.foreach { threshold =>
      logger.info(s"\n=== Testing with cardinality ratio threshold: $threshold ===")

      val enhancedStats = new EnhancedStatsCompute(
        inputDf = df,
        keys = Seq.empty,
        name = s"ratings_threshold_${threshold.toString.replace(".", "_")}",
        cardinalityThreshold = threshold
      )

      val cardMap = enhancedStats.cardinalityMap
      cardMap.foreach { case (col, card) =>
        val classification = if (card <= threshold) "LOW (categorical)" else "HIGH (numeric)"
        logger.info(s"  $col: $card -> $classification")
      }

      // Count metrics per column
      enhancedStats.enhancedMetrics.groupBy(_.name).foreach { case (colName, metrics) =>
        logger.info(s"  $colName has ${metrics.size} metrics")
      }
    }
  }

  it should "generate IR bytes that can be uploaded to KV store" in {
    val df = ratingsData

    val enhancedStats = new EnhancedStatsCompute(
      inputDf = df,
      keys = Seq.empty,
      name = "ratings_kv_upload",
      cardinalityThreshold = 0.01
    )

    // Generate daily tiles
    val (flatDf, metadata) = enhancedStats.enhancedDailySummary(
      sample = 1.0,
      timeBucketMinutes = 0 // Daily tiles
    )

    // Convert to Avro format (ready for KV store)
    val keyColumns = Seq("JoinPath")
    val valueColumns = flatDf.columns.filterNot(c => c == "JoinPath" || c == Constants.TimeColumn).toSeq
    val avroDf = AvroKvEncoder.encodeTimed(
      flatDf, keyColumns, valueColumns,
      storeSchemasPrefix = Some("ratings_kv_upload"),
      metadata = Some(metadata)
    )

    logger.info("\nKV Store Ready Format:")
    val recordCount = avroDf.count()
    logger.info(s"Number of records: ${recordCount}")
    avroDf.show(5, truncate = false)
    avroDf.printSchema()

    // Verify structure
    val columns = avroDf.columns.toSet
    assert(columns.contains("key_bytes"), "Should have key_bytes for KV store")
    assert(columns.contains("value_bytes"), "Should have value_bytes (IR bytes) for KV store")
    assert(columns.contains(Constants.TimeColumn), "Should have timestamp for tiling")

    // Verify we have actual data - use take(1) instead of collect() to avoid OOM
    val firstRow = avroDf.take(1).head
    val keyBytes = firstRow.getAs[Array[Byte]]("key_bytes")
    val valueBytes = firstRow.getAs[Array[Byte]]("value_bytes")

    assert(keyBytes != null && keyBytes.length > 0, "Key bytes should not be empty")
    assert(valueBytes != null && valueBytes.length > 0, "Value bytes (IRs) should not be empty")

    logger.info(s"✓ Generated ${recordCount} KV records")
    logger.info(s"✓ Sample key size: ${keyBytes.length} bytes")
    logger.info(s"✓ Sample IR size: ${valueBytes.length} bytes")
  }

  it should "upload to InMemoryKVStore and fetch back successfully" in {
    import java.util.concurrent.Executors
      import scala.concurrent.ExecutionContext

    implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1))

    val df = ratingsData
    val tableName = "ratings_full_cycle_test"

    logger.info(s"\n=== Full Cycle Test: Compute → Upload → Fetch ===")

    // Step 1: Create InMemoryKVStore and MockApi using existing Spark session
    val inMemoryKvStore = new InMemoryKvStore(() => spark, hardFailureOnInvalidDataset = false)
    val kvStoreFunc = () => inMemoryKvStore
    val mockApi = new MockApi(kvStoreFunc, "enhanced_store_namespace")

    logger.info("✓ Created InMemoryKVStore and MockApi")

    // Step 2: Compute enhanced stats
    val enhancedStats = new EnhancedStatsCompute(
      inputDf = df,
      keys = Seq("userId"), // Exclude userId from stats (as it's a key)
      name = tableName,
      cardinalityThreshold = 0.01
    )

    logger.info(s"✓ Created EnhancedStatsCompute with ${enhancedStats.enhancedMetrics.length} metrics")

    // Log the metrics that will be computed
    val metricsByColumn = enhancedStats.enhancedMetrics.groupBy(_.name)
    logger.info(s"Metrics breakdown:")
    metricsByColumn.foreach { case (col, metrics) =>
      logger.info(s"  $col: ${metrics.map(_.operation).mkString(", ")}")
    }

    // Step 3: Generate daily tiles
    val (flatDf, statsMetadata) = enhancedStats.enhancedDailySummary(
      sample = 1.0,
      timeBucketMinutes = 0 // Daily tiles
    )

    val keyColumns = Seq("JoinPath")
    val valueColumns = flatDf.columns.filterNot(c => c == "JoinPath" || c == Constants.TimeColumn).toSeq
    val avroDf = AvroKvEncoder.encodeTimed(
      flatDf, keyColumns, valueColumns,
      storeSchemasPrefix = Some(tableName),
      metadata = Some(statsMetadata)
    )

    val tileCount = flatDf.count()
    logger.info(s"✓ Generated $tileCount daily tiles")

    // Step 4: Upload to KV store
    val statsStore = new EnhancedStatsStore(mockApi)
    statsStore.upload(avroDf, putsPerRequest = 100)

    logger.info(s"✓ Uploaded $tileCount tiles to InMemoryKVStore")

    // Step 5: Verify data was uploaded by checking KV store directly
    import ai.chronon.online.KVStore.GetRequest

      import java.nio.charset.StandardCharsets
      import scala.concurrent.Await
      import scala.concurrent.duration._

    // Check that schemas were stored
    val keySchemaKey = s"$tableName${Constants.TimedKvRDDKeySchemaKey}"
    val valueSchemaKey = s"$tableName${Constants.TimedKvRDDValueSchemaKey}"

    val keySchemaResponse = Await.result(
      inMemoryKvStore.get(GetRequest(keySchemaKey.getBytes(StandardCharsets.UTF_8), Constants.EnhancedStatsDataset, None, None)),
      10.seconds
    )

    val valueSchemaResponse = Await.result(
      inMemoryKvStore.get(GetRequest(valueSchemaKey.getBytes(StandardCharsets.UTF_8), Constants.EnhancedStatsDataset, None, None)),
      10.seconds
    )

    assert(keySchemaResponse.values.isSuccess, "Key schema should be stored")
    assert(valueSchemaResponse.values.isSuccess, "Value schema should be stored")

    val keySchemaBytes = keySchemaResponse.values.get.head.bytes
    val valueSchemaBytes = valueSchemaResponse.values.get.head.bytes
    logger.info(s"✓ Verified schemas are stored (key: ${keySchemaBytes.length} bytes, value: ${valueSchemaBytes.length} bytes)")

    // Check that metadata was stored
    val metadataKeys = Seq(
      s"$tableName/selectedSchema",
      s"$tableName/noKeysSchema",
      s"$tableName/cardinalityMap"
    )

    metadataKeys.foreach { key =>
      val response = Await.result(
        inMemoryKvStore.get(GetRequest(key.getBytes(StandardCharsets.UTF_8), Constants.EnhancedStatsDataset, None, None)),
        10.seconds
      )
      assert(response.values.isSuccess, s"Metadata key $key should be stored")
      logger.info(s"✓ Verified metadata key: $key")
    }

    // Step 6: Fetch stats back using JavaStatsService
    import ai.chronon.online.JavaStatsService

    val javaStatsService = new JavaStatsService(mockApi, "ENHANCED_STATS", Constants.EnhancedStatsDataset)

    // Get time range from data
    val timeRange = df.select(
      min(col(Constants.TimeColumn)).as("min_ts"),
      max(col(Constants.TimeColumn)).as("max_ts")
    ).collect().head

    val startTime = timeRange.getLong(0)
    val dataEndTime = timeRange.getLong(1)
    // Use current time as endTime so metadata (stored with System.currentTimeMillis) is included
    val endTime = System.currentTimeMillis()

    logger.info(s"Fetching stats for time range: [$startTime, $endTime] (data ends at $dataEndTime)")

    val statsFuture = javaStatsService.fetchStats(tableName, startTime, endTime)
    // Use CompletableFuture.get() with timeout
    val statsResponse = statsFuture.get(30, java.util.concurrent.TimeUnit.SECONDS)

    // Step 7: Verify the fetched stats
    assert(statsResponse.isSuccess, "Stats fetch should succeed")
    assert(statsResponse.getTilesCount > 0, "Should have processed at least one tile")

    logger.info(s"✓ Successfully fetched stats from ${statsResponse.getTilesCount} tiles")

    val stats = statsResponse.getStatistics
    logger.info(s"\nFetched Statistics (${stats.size()} metrics):")

    // Verify we have expected metrics
    import scala.jdk.CollectionConverters._
    val statKeys = stats.keySet().asScala

    // Should have total_count
    assert(statKeys.exists(_.contains("total")), "Should have total count")

    // Should have stats for non-key columns (movieId, rating, but NOT userId since it's a key)
    assert(statKeys.exists(_.startsWith("movieId")), "Should have movieId metrics")
    assert(statKeys.exists(_.startsWith("rating")), "Should have rating metrics")
    assert(!statKeys.exists(_.startsWith("userId")), "Should NOT have userId metrics (it's a key)")

    // Log some sample statistics
    statKeys.toSeq.sorted.take(10).foreach { key =>
      logger.info(s"  $key: ${stats.get(key)}")
    }

    // Verify specific metrics
    val totalCount = stats.asScala.find(_._1.toLowerCase.contains("total")).map(_._2.asInstanceOf[Long])
    assert(totalCount.isDefined, "Should have total count metric")
    assert(totalCount.get > 0, "Total count should be positive")

    logger.info(s"\n✓ Initial upload and fetch completed successfully")

    // Step 8: Schema Evolution - Add new column, remove existing column
    logger.info(s"\n=== Schema Evolution Test: Non-Compatible Changes ===")

    // Evolve schema: Add "year" column (extracted from timestamp), remove "movieId" column
    import org.apache.spark.sql.functions._
    val evolvedDf = df
      .withColumn("year", year(from_unixtime(col(Constants.TimeColumn) / 1000)))
      .drop("movieId")  // Remove movieId column

    logger.info(s"Original columns: ${df.columns.mkString(", ")}")
    logger.info(s"Evolved columns: ${evolvedDf.columns.mkString(", ")}")

    // Step 9: Compute enhanced stats with evolved schema
    val evolvedStats = new EnhancedStatsCompute(
      inputDf = evolvedDf,
      keys = Seq("userId"),
      name = tableName,  // Same table name - this is the evolution
      cardinalityThreshold = 0.01
    )

    logger.info(s"✓ Created EnhancedStatsCompute with evolved schema (${evolvedStats.enhancedMetrics.length} metrics)")

    val evolvedMetricsByColumn = evolvedStats.enhancedMetrics.groupBy(_.name)
    logger.info(s"Evolved metrics breakdown:")
    evolvedMetricsByColumn.foreach { case (col, metrics) =>
      logger.info(s"  $col: ${metrics.map(_.operation).mkString(", ")}")
    }

    // Step 10: Generate and upload evolved schema tiles
    val (evolvedFlatDf, evolvedMetadata) = evolvedStats.enhancedDailySummary(
      sample = 1.0,
      timeBucketMinutes = 0
    )

    val evolvedTileCount = evolvedFlatDf.count()
    logger.info(s"✓ Generated $evolvedTileCount tiles with evolved schema")

    // Upload evolved schema (this should update the metadata)
    val evolvedValueColumns = evolvedFlatDf.columns.filterNot(c => c == "JoinPath" || c == Constants.TimeColumn).toSeq
    val evolvedAvroDf = AvroKvEncoder.encodeTimed(
      evolvedFlatDf, keyColumns, evolvedValueColumns,
      storeSchemasPrefix = Some(tableName),
      metadata = Some(evolvedMetadata)
    )
    statsStore.upload(evolvedAvroDf, putsPerRequest = 100)
    logger.info(s"✓ Uploaded evolved schema to InMemoryKVStore")

    // Step 11: Fetch with evolved schema
    val evolvedStatsFuture = javaStatsService.fetchStats(tableName, startTime, endTime)
    val evolvedStatsResponse = evolvedStatsFuture.get(30, java.util.concurrent.TimeUnit.SECONDS)

    assert(evolvedStatsResponse.isSuccess, "Evolved schema stats fetch should succeed")
    assert(evolvedStatsResponse.getTilesCount > 0, "Should have tiles for evolved schema")

    val evolvedFetchedStats = evolvedStatsResponse.getStatistics
    logger.info(s"\n✓ Successfully fetched evolved schema stats from ${evolvedStatsResponse.getTilesCount} tiles")
    logger.info(s"Evolved schema statistics (${evolvedFetchedStats.size()} metrics):")

    val evolvedStatKeys = evolvedFetchedStats.keySet().asScala

    // Verify schema evolution: should NOT have movieId metrics anymore
    assert(!evolvedStatKeys.exists(_.startsWith("movieId")), "Should NOT have movieId metrics after evolution")

    // Should have year metrics (new column)
    assert(evolvedStatKeys.exists(_.startsWith("year")), "Should have year metrics after evolution")

    // Should still have rating metrics
    assert(evolvedStatKeys.exists(_.startsWith("rating")), "Should still have rating metrics")

    // Log sample evolved metrics
    logger.info("\nSample evolved metrics:")
    evolvedStatKeys.toSeq.filter(k => k.startsWith("year") || k.startsWith("rating")).sorted.take(10).foreach { key =>
      logger.info(s"  $key: ${evolvedFetchedStats.get(key)}")
    }

    logger.info(s"\n✓ Schema evolution test passed!")
    logger.info(s"  - Original schema: ${df.columns.length} columns")
    logger.info(s"  - Evolved schema: ${evolvedDf.columns.length} columns")
    logger.info(s"  - Removed: movieId")
    logger.info(s"  - Added: year")
    logger.info(s"  - Original metrics: ${stats.size()}")
    logger.info(s"  - Evolved metrics: ${evolvedFetchedStats.size()}")

    logger.info(s"\n✓ Full cycle test with schema evolution passed!")
    logger.info(s"  - Computed stats for ${df.count()} records")
    logger.info(s"  - Generated $tileCount tiles (initial)")
    logger.info(s"  - Generated $evolvedTileCount tiles (evolved)")
    logger.info(s"  - Uploaded to KV store twice (with schema evolution)")
    logger.info(s"  - Fetched back ${stats.size()} metrics (initial), ${evolvedFetchedStats.size()} metrics (evolved)")
    logger.info(s"  - Total records: ${totalCount.get}")
  }

}
