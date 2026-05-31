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
import ai.chronon.aggregator.test.Column
import ai.chronon.api._
import ai.chronon.online.serde.SparkConversions.toChrononSchema
import ai.chronon.spark.Extensions._
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.stats.{EnhancedStatsCompute, StatsCompute}
import ai.chronon.spark.submission.SparkSessionBuilder
import ai.chronon.spark.utils.{DataFrameGen, SparkTestBase}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class StatsComputeTest extends AnyFlatSpec {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  lazy val spark: SparkSession = SparkSessionBuilder.build("StatsComputeTest", local = true)
  implicit val tableUtils: TableUtils = TableUtils(spark)
  val namespace: String = "stats_compute_test"

  it should "summary" in {
    val data = Seq(
      ("1", Some(1L), Some(1.0), Some("a")),
      ("1", Some(1L), None, Some("b")),
      ("2", Some(2L), None, None),
      ("3", None, None, Some("d"))
    )
    val columns = Seq("keyId", "value", "double_value", "string_value")
    val df = spark.createDataFrame(data).toDF(columns: _*).withColumn(tableUtils.partitionColumn, lit("2022-04-09"))
    val stats = new StatsCompute(df, Seq("keyId"), "test")
    val aggregator =
      StatsGenerator.buildAggregator(stats.metrics, StructType.from("test", toChrononSchema(stats.selectedDf.schema)))
    val result = stats.dailySummary(aggregator)
    stats.addDerivedMetrics(result, aggregator).show()
  }

  it should "snapshot summary" in {
    SparkTestBase.createDatabase(spark, namespace)
    val data = Seq(
      ("1", Some(1L), Some(1.0), Some("a")),
      ("1", Some(1L), None, Some("b")),
      ("2", Some(2L), None, None),
      ("3", None, None, Some("d"))
    )
    val columns = Seq("keyId", "value", "double_value", "string_value")
    val df = spark
      .createDataFrame(data)
      .toDF(columns: _*)
      .withColumn(tableUtils.partitionColumn, lit("2022-04-09"))
      .drop(Constants.TimeColumn)
    val stats = new StatsCompute(df, Seq("keyId"), "snapshotTest")
    val aggregator =
      StatsGenerator.buildAggregator(stats.metrics, StructType.from("test", toChrononSchema(stats.selectedDf.schema)))
    val result = stats.dailySummary(aggregator)
    stats.addDerivedMetrics(result, aggregator).save(s"$namespace.testTablenameSnapshot")
  }

  it should "generated summary" in {
    val schema = List(
      Column("user", StringType, 10),
      Column("session_length", IntType, 10000)
    )
    val df = DataFrameGen.events(spark, schema, 100000, 10)
    val stats = new StatsCompute(df, Seq("user"), "generatedTest")
    val aggregator =
      StatsGenerator.buildAggregator(stats.metrics,
                                     StructType.from("generatedTest", toChrononSchema(stats.selectedDf.schema)))
    val daily = stats.dailySummary(aggregator, timeBucketMinutes = 0)

    logger.info("Daily Stats")
    daily.show()
    val bucketed = stats
      .dailySummary(aggregator)
      .replaceWithReadableTime(Seq(Constants.TimeColumn), false)

    logger.info("Bucketed Stats")
    bucketed.show()

    val denormalized = stats.addDerivedMetrics(bucketed, aggregator)
    logger.info("With Derived Data")
    denormalized.show(truncate = false)
  }

  it should "generated summary no ts" in {
    val schema = List(
      Column("user", StringType, 10),
      Column("session_length", IntType, 10000)
    )
    val df = DataFrameGen
      .events(spark, schema, 100000, 10)
      .drop(Constants.TimeColumn)
    val stats = new StatsCompute(df, Seq("user"), "noTsTest")
    val aggregator =
      StatsGenerator.buildAggregator(stats.metrics,
                                     StructType.from("noTsTest", toChrononSchema(stats.selectedDf.schema)))
    val daily = stats.dailySummary(aggregator, timeBucketMinutes = 0)

    logger.info("Daily Stats")
    daily.show()
    val bucketed = stats.dailySummary(aggregator)

    logger.info("Bucketed Stats")
    bucketed.show()

    val denormalized = stats.addDerivedMetrics(bucketed, aggregator)
    logger.info("With Derived Data")
    denormalized.show(truncate = false)
  }

  /** Reproduces the production crash where online buildAggregator fails with None.get when
    * the cardinality map stored in KV is stale relative to the selectedSchema.
    *
    * Scenario: a numeric column was high-cardinality when the offline stats job ran (no __str column
    * written to selectedSchema), but the online code receives an empty/stale cardinality map so it
    * defaults the column to cardinality=0 (low-cardinality), generating a __str metric whose
    * inputColumn doesn't exist in selectedSchema.
    */
  it should "fail with a clear error when cardinality map is stale causing schema mismatch" in {
    val schema = List(
      Column("user", StringType, 10),
      // 10000 distinct values → cardinality well above threshold=100 → high-cardinality path
      Column("high_card_value", LongType, 10000)
    )
    val df = DataFrameGen.events(spark, schema, 100000, 10)

    // Offline job: build selectedSchema from EnhancedStatsCompute with true cardinality
    val compute = new EnhancedStatsCompute(df, Seq("user"), "mismatchTest")
    val offlineSelectedSchema =
      StructType.from("mismatchTest", toChrononSchema(compute.enhancedSelectedDf.schema))

    // high_card_value should be in the high-cardinality branch → no __str column in selectedSchema
    val selectedCols = offlineSelectedSchema.fields.map(_.name).toSet
    assert(!selectedCols.contains("high_card_value__str"),
           s"high_card_value should be high-cardinality so __str must NOT be in selectedSchema, got: $selectedCols")

    // Online: stale cardinality map (column missing → defaults to 0 = low-cardinality)
    val staleCardinalityMap = Map.empty[String, Double]
    val noKeysFields = toChrononSchema(compute.enhancedSelectedDf.schema)
    val onlineMetrics =
      StatsGenerator.buildEnhancedMetrics(
        noKeysFields,
        staleCardinalityMap,
        cardinalityThreshold = 0.01
      )

    // Before the fix this was: NoSuchElementException: None.get (completely opaque)
    // After the fix it should be IllegalArgumentException naming the missing column
    val ex = intercept[IllegalArgumentException] {
      StatsGenerator.buildAggregator(onlineMetrics, offlineSelectedSchema)
    }
    assert(ex.getMessage.contains("high_card_value__str"),
           s"Error should name the missing column, got: ${ex.getMessage}")
  }

  /** Test to make sure aggregations are generated when it makes sense.
    * Example, percentiles are not currently supported for byte.
    */
  it should "generated summary byte" in {
    val schema = List(
      Column("user", StringType, 10),
      Column("session_length", IntType, 10000)
    )
    val byteSample = 1.toByte
    val df = DataFrameGen
      .events(spark, schema, 100000, 10)
      .withColumn("byte_column", lit(byteSample))
    val stats = new StatsCompute(df, Seq("user"), "byteTest")
    val aggregator =
      StatsGenerator.buildAggregator(stats.metrics,
                                     StructType.from("byteTest", toChrononSchema(stats.selectedDf.schema)))
    val daily = stats.dailySummary(aggregator, timeBucketMinutes = 0)

    logger.info("Daily Stats")
    daily.show()
    val bucketed = stats
      .dailySummary(aggregator)
      .replaceWithReadableTime(Seq(Constants.TimeColumn), false)

    logger.info("Bucketed Stats")
    bucketed.show()

    val denormalized = stats.addDerivedMetrics(bucketed, aggregator)
    logger.info("With Derived Data")
    denormalized.show(truncate = false)
  }
}
