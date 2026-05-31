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

package ai.chronon.spark.stats

import ai.chronon.api.TsUtils
import ai.chronon.online.fetcher.DataMetrics
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.submission.SparkSessionBuilder
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.{Logger, LoggerFactory}

class CompareTablesGenericTest extends AnyFlatSpec {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  lazy val spark: SparkSession = SparkSessionBuilder.build("CompareTablesGenericTest", local = true)
  private val tableUtils = TableUtils(spark)

  def toTs(arg: String): Long = TsUtils.datetimeToTs(arg)

  it should "basic comparison same schema" in {
    val leftData = Seq(
      (1, toTs("2021-04-10 09:00:00"), 1.0, "a"),
      (2, toTs("2021-04-10 11:00:00"), 2.0, "b"),
      (3, toTs("2021-04-10 15:00:00"), 3.0, "c")
    )
    val rightData = Seq(
      (1, toTs("2021-04-10 09:00:00"), 1.0, "a"),
      (2, toTs("2021-04-10 11:00:00"), 2.0, "b"),
      (3, toTs("2021-04-10 15:00:00"), 5.0, "c") // rating differs
    )

    val columns = Seq("id", "ts", "rating", "label")
    val leftDf = spark.createDataFrame(leftData).toDF(columns: _*)
    val rightDf = spark.createDataFrame(rightData).toDF(columns: _*)

    val keys = Seq("id", "ts")
    val (compareDf, metricsDf, metrics) =
      CompareBaseJob.compare(leftDf, rightDf, keys, tableUtils, requireTimeColumn = false)

    logger.info("Side-by-side table:")
    compareDf.show(truncate = false)
    logger.info("Metrics table:")
    metricsDf.show(truncate = false)

    assert(metrics.series.nonEmpty)
    // Check that identical rows have 0 mismatches and the differing row has 1
    val allMismatches = metrics.series.flatMap { case (_, m) =>
      m.filterKeys(_.endsWith("_mismatch_sum")).values.map(_.asInstanceOf[Long])
    }
    assert(allMismatches.sum > 0, "Should detect at least one mismatch")
  }

  it should "comparison without time column" in {
    val leftData = Seq(
      (1, 10.0, "x"),
      (2, 20.0, "y"),
      (3, 30.0, "z")
    )
    val rightData = Seq(
      (1, 10.0, "x"),
      (2, 25.0, "y"), // value differs
      (3, 30.0, "z")
    )

    val columns = Seq("id", "value", "name")
    val leftDf = spark.createDataFrame(leftData).toDF(columns: _*)
    val rightDf = spark.createDataFrame(rightData).toDF(columns: _*)

    // No time column in keys - synthetic ts should be injected
    val keys = Seq("id")
    val (compareDf, metricsDf, metrics) =
      CompareBaseJob.compare(leftDf, rightDf, keys, tableUtils, requireTimeColumn = false)

    logger.info("Side-by-side table:")
    compareDf.show(truncate = false)
    logger.info("Metrics table:")
    metricsDf.show(truncate = false)

    assert(metrics.series.nonEmpty)
    // All rows land in a single time bucket (ts=0)
    assert(metrics.series.length == 1, "All rows should be in one bucket with synthetic ts=0")
    val bucket = metrics.series.head._2
    val valueMismatch = bucket("value_mismatch_sum").asInstanceOf[Long]
    assert(valueMismatch == 1, s"Expected 1 value mismatch, got $valueMismatch")
  }

  it should "simple column rename mapping" in {
    val leftData = Seq(
      (1, toTs("2021-04-10 09:00:00"), 100.0),
      (2, toTs("2021-04-10 11:00:00"), 200.0)
    )
    val rightData = Seq(
      (1, toTs("2021-04-10 09:00:00"), 100.0),
      (2, toTs("2021-04-10 11:00:00"), 200.0)
    )

    val leftDf = spark.createDataFrame(leftData).toDF("id", "ts", "value_a")
    val rightDf = spark.createDataFrame(rightData).toDF("id", "ts", "value_b")

    val keys = Seq("id", "ts")
    val (compareDf, metricsDf, metrics) = CompareBaseJob.compare(
      leftDf,
      rightDf,
      keys,
      tableUtils,
      mapping = Map("value_a" -> "value_b"),
      requireTimeColumn = false
    )

    logger.info("Side-by-side table:")
    compareDf.show(truncate = false)
    logger.info("Metrics table:")
    metricsDf.show(truncate = false)

    // All values match, so no mismatches
    val allMismatches = metrics.series.flatMap { case (_, m) =>
      m.filterKeys(_.endsWith("_mismatch_sum")).values.map(_.asInstanceOf[Long])
    }
    assert(allMismatches.sum == 0, "All values should match with correct mapping")
  }

  it should "SQL expression mapping via selectExpr" in {
    // Simulates the CLI workflow: apply selectExpr on right DF before calling compare
    val leftData = Seq(
      (1, toTs("2021-04-10 09:00:00"), 1.5),
      (2, toTs("2021-04-10 11:00:00"), 2.0)
    )
    val rightData = Seq(
      (1, toTs("2021-04-10 09:00:00"), 150L),
      (2, toTs("2021-04-10 11:00:00"), 200L)
    )

    val leftDf = spark.createDataFrame(leftData).toDF("id", "ts", "price")
    val rawRightDf = spark.createDataFrame(rightData).toDF("id", "ts", "price_cents")

    // Apply SQL transform on right side (what the CLI run() does)
    val rightDf = rawRightDf.selectExpr("id", "ts", "CAST(price_cents AS DOUBLE) / 100.0 AS price")

    val keys = Seq("id", "ts")
    val (compareDf, metricsDf, metrics) = CompareBaseJob.compare(
      leftDf,
      rightDf,
      keys,
      tableUtils,
      requireTimeColumn = false
    )

    logger.info("Side-by-side table:")
    compareDf.show(truncate = false)
    logger.info("Metrics table:")
    metricsDf.show(truncate = false)

    val allMismatches = metrics.series.flatMap { case (_, m) =>
      m.filterKeys(_.endsWith("_mismatch_sum")).values.map(_.asInstanceOf[Long])
    }
    assert(allMismatches.sum == 0, "Values should match after SQL transform")
  }

  it should "key column rename" in {
    val leftData = Seq(
      (1, toTs("2021-04-10 09:00:00"), 10.0),
      (2, toTs("2021-04-10 11:00:00"), 20.0)
    )
    val rightData = Seq(
      (1, toTs("2021-04-10 09:00:00"), 10.0),
      (2, toTs("2021-04-10 11:00:00"), 20.0)
    )

    val leftDf = spark.createDataFrame(leftData).toDF("user_id", "ts", "score")
    val rawRightDf = spark.createDataFrame(rightData).toDF("uid", "ts", "score")

    // Rename key column on right side (what the CLI run() does)
    val rightDf = rawRightDf.selectExpr("uid AS user_id", "ts", "score")

    val keys = Seq("user_id", "ts")
    val (compareDf, metricsDf, metrics) = CompareBaseJob.compare(
      leftDf,
      rightDf,
      keys,
      tableUtils,
      requireTimeColumn = false
    )

    logger.info("Side-by-side table:")
    compareDf.show(truncate = false)
    logger.info("Metrics table:")
    metricsDf.show(truncate = false)

    val allMismatches = metrics.series.flatMap { case (_, m) =>
      m.filterKeys(_.endsWith("_mismatch_sum")).values.map(_.asInstanceOf[Long])
    }
    assert(allMismatches.sum == 0, "Join should work correctly with renamed key column")
  }

  it should "timestamp expression" in {
    // Simulate: left has Long millis, right has date string; CLI applies expressions before compare
    val leftData = Seq(
      (1, toTs("2021-04-10 09:00:00"), 100.0),
      (2, toTs("2021-04-10 11:00:00"), 200.0)
    )
    val rightData = Seq(
      (1, "2021-04-10 09:00:00", 100.0),
      (2, "2021-04-10 11:00:00", 200.0)
    )

    val leftDf = spark
      .createDataFrame(leftData)
      .toDF("id", "created_at", "value")
      .withColumn("ts", functions.col("created_at"))
      .drop("created_at")

    val rawRightDf = spark.createDataFrame(rightData).toDF("id", "event_date", "value")
    val rightDf = rawRightDf
      .withColumn("ts", functions.expr("UNIX_TIMESTAMP(event_date) * 1000"))
      .drop("event_date")

    val keys = Seq("id", "ts")
    val (compareDf, metricsDf, metrics) = CompareBaseJob.compare(
      leftDf,
      rightDf,
      keys,
      tableUtils,
      requireTimeColumn = false
    )

    logger.info("Side-by-side table:")
    compareDf.show(truncate = false)
    logger.info("Metrics table:")
    metricsDf.show(truncate = false)

    assert(metrics.series.nonEmpty, "Should have time-bucketed metrics")
    val allMismatches = metrics.series.flatMap { case (_, m) =>
      m.filterKeys(_.endsWith("_mismatch_sum")).values.map(_.asInstanceOf[Long])
    }
    assert(allMismatches.sum == 0, "Values should match with timestamp expressions applied")
  }

  it should "migration check" in {
    val leftData = Seq(
      (1, toTs("2021-04-10 09:00:00"), 10.0, "extra_col_val"),
      (2, toTs("2021-04-10 11:00:00"), 20.0, "extra_col_val2")
    )
    val rightData = Seq(
      (1, toTs("2021-04-10 09:00:00"), 10.0),
      (2, toTs("2021-04-10 11:00:00"), 20.0)
    )

    val leftDf = spark.createDataFrame(leftData).toDF("id", "ts", "value", "extra")
    val rightDf = spark.createDataFrame(rightData).toDF("id", "ts", "value")

    val keys = Seq("id", "ts")
    val (compareDf, metricsDf, metrics) = CompareBaseJob.compare(
      leftDf,
      rightDf,
      keys,
      tableUtils,
      migrationCheck = true,
      requireTimeColumn = false
    )

    logger.info("Side-by-side table:")
    compareDf.show(truncate = false)
    logger.info("Metrics table:")
    metricsDf.show(truncate = false)

    val allMismatches = metrics.series.flatMap { case (_, m) =>
      m.filterKeys(_.endsWith("_mismatch_sum")).values.map(_.asInstanceOf[Long])
    }
    assert(allMismatches.sum == 0, "Migration check should allow extra columns on left and match remaining")
  }

  it should "requireTimeColumn backward compat" in {
    val leftData = Seq((1, 10.0))
    val rightData = Seq((1, 10.0))

    val leftDf = spark.createDataFrame(leftData).toDF("id", "value")
    val rightDf = spark.createDataFrame(rightData).toDF("id", "value")

    // Default requireTimeColumn=true should fail when no time column present
    val caught = intercept[AssertionError] {
      CompareBaseJob.compare(leftDf, rightDf, Seq("id"), tableUtils)
    }
    assert(caught.getMessage.contains("time column"), "Should require time column by default")
  }
}
