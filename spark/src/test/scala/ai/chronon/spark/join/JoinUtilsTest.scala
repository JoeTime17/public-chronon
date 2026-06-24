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

package ai.chronon.spark.join

import ai.chronon.aggregator.test.Column
import ai.chronon.api
import ai.chronon.api.{Builders, PartitionRange, PartitionSpec}
import ai.chronon.spark.Extensions._
import ai.chronon.spark.JoinUtils
import ai.chronon.spark.JoinUtils.{contains_any, set_add}
import ai.chronon.spark.utils.{DataFrameGen, TestUtils}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.junit.Assert._

import java.nio.file.Files
import scala.collection.mutable
import scala.util.Try

class JoinUtilsTest extends BaseJoinTest {

  private implicit val partitionSpec: PartitionSpec = tableUtils.partitionSpec
  private val sparkDppEnabled = "spark.sql.optimizer.dynamicPartitionPruning.enabled"
  override protected val namespace = "join_util"
  it should "udf set add" in {
    val data = Seq(
      Row(Seq("a", "b", "c"), "a"),
      Row(Seq("a", "b", "c"), "d"),
      Row(Seq("a", "b", "c"), null),
      Row(null, "a"),
      Row(null, null)
    )

    val schema: StructType = new StructType()
      .add("set", ArrayType(StringType))
      .add("item", StringType)
    val df: DataFrame = spark.createDataFrame(java.util.Arrays.asList(data: _*), schema)

    val actual = df
      .select(set_add(col("set"), col("item")).as("new_set"))
      .collect()
      .map(_.getAs[mutable.WrappedArray[String]](0))

    val expected = Array(
      Seq("a", "b", "c"),
      Seq("a", "b", "c", "d"),
      Seq("a", "b", "c"),
      Seq("a"),
      null
    )

    expected.zip(actual).map { case (e, a) =>
      e == a
    }
  }

  it should "udf contains any" in {
    val data = Seq(
      Row(Seq("a", "b", "c"), Seq("a")),
      Row(Seq("a", "b", "c"), Seq("a", "b")),
      Row(Seq("a", "b", "c"), Seq("d")),
      Row(Seq("a", "b", "c"), null),
      Row(null, Seq("a")),
      Row(null, null)
    )

    val schema: StructType = new StructType()
      .add("array", ArrayType(StringType))
      .add("query", ArrayType(StringType))
    val df: DataFrame = spark.createDataFrame(java.util.Arrays.asList(data: _*), schema)

    val actual = df
      .select(contains_any(col("array"), col("query")).as("result"))
      .collect()
      .map(_.getAs[Any](0))

    val expected = Array(
      true, true, false, null, false, null
    )

    expected.zip(actual).map { case (e, a) =>
      e == a
    }
  }

  private def testJoinScenario(leftSchema: StructType,
                               rightSchema: StructType,
                               keys: Seq[String],
                               isFailure: Boolean): Try[DataFrame] = {
    val df = Try(
      JoinUtils.coalescedJoin(
        // using empty dataframe is sufficient to test spark query planning
        spark.createDataFrame(java.util.Arrays.asList[Row](), leftSchema),
        spark.createDataFrame(java.util.Arrays.asList[Row](), rightSchema),
        keys
      ))
    if (isFailure) {
      assertTrue(df.isFailure)
    } else {
      assertTrue(df.isSuccess)
    }
    df
  }

  it should "coalesced join mismatched key columns" in {
    // mismatch data type on join keys
    testJoinScenario(
      new StructType()
        .add("key", LongType)
        .add("col1", LongType),
      new StructType()
        .add("key", StringType)
        .add("col2", LongType),
      Seq("key"),
      isFailure = true
    )
  }

  it should "coalesced join mismatched shared columns" in {
    // mismatch data type on shared columns
    testJoinScenario(
      new StructType()
        .add("key", LongType)
        .add("col1", LongType),
      new StructType()
        .add("key", LongType)
        .add("col1", StringType),
      Seq("key"),
      isFailure = true
    )
  }

  it should "coalesced join missing keys" in {
    // missing some keys
    testJoinScenario(
      new StructType()
        .add("key1", LongType)
        .add("key2", LongType)
        .add("col1", LongType),
      new StructType()
        .add("key1", LongType)
        .add("col2", LongType),
      Seq("key1", "key2"),
      isFailure = true
    )
  }

  it should "coalesced join no shared columns" in {
    // test no shared columns
    val df = testJoinScenario(
      new StructType()
        .add("key", LongType)
        .add("col1", LongType),
      new StructType()
        .add("key", LongType)
        .add("col2", StringType),
      Seq("key"),
      isFailure = false
    )
    assertEquals(3, df.get.columns.length)
  }

  it should "coalesced join shared columns" in {
    // test shared columns
    val df = testJoinScenario(
      new StructType()
        .add("key", LongType)
        .add("col1", LongType)
        .add("col2", StringType),
      new StructType()
        .add("key", LongType)
        .add("col1", LongType)
        .add("col3", DoubleType),
      Seq("key"),
      isFailure = false
    )
    assertEquals(4, df.get.columns.length)
  }

  it should "coalesced join one sided left" in {
    // test when left side only has keys
    val df = testJoinScenario(
      new StructType()
        .add("key", LongType),
      new StructType()
        .add("key", LongType)
        .add("col1", LongType)
        .add("col2", DoubleType),
      Seq("key"),
      isFailure = false
    )
    assertEquals(3, df.get.columns.length)
  }

  it should "coalesced join one sided right" in {
    // test when right side only has keys
    val df = testJoinScenario(
      new StructType()
        .add("key", LongType)
        .add("col1", LongType)
        .add("col2", DoubleType),
      new StructType()
        .add("key", LongType),
      Seq("key"),
      isFailure = false
    )
    assertEquals(3, df.get.columns.length)
  }

  it should "filter columns" in {
    val testDf = createSampleTable()
    val filter = Array("listing", "ds", "feature_review")
    val filteredDf = JoinUtils.filterColumns(testDf, filter)
    assertTrue(filteredDf.schema.fieldNames.sorted sameElements filter.sorted)
  }

  it should "get ranges to fill" in {
    createDatabase(namespace)
    // left table
    val itemQueries = List(Column("item", api.StringType, 100))
    val itemQueriesTable = "joinUtil.item_queries_table"
    DataFrameGen
      .events(spark, itemQueries, 1000, partitions = 100)
      .save(itemQueriesTable)

    val startPartition = "2023-04-15"
    val endPartition = "2023-08-01"
    val leftSource = Builders.Source.events(Builders.Query(startPartition = startPartition), table = itemQueriesTable)
    val range = JoinUtils.getRangeToFill(leftSource, tableUtils, endPartition)
    assertEquals(range, PartitionRange(startPartition, endPartition))
  }

  it should "get ranges to fill with override" in {
    createDatabase(namespace)
    // left table
    val itemQueries = List(Column("item", api.StringType, 100))
    val itemQueriesTable = "joinUtil.queries_table"
    DataFrameGen
      .events(spark, itemQueries, 1000, partitions = 50)
      .save(itemQueriesTable)

    val configStartPartition = "2023-04-15"
    val overrideStartPartition = "2023-08-01"
    val endPartition = "2023-08-08"
    val leftSource = Builders.Source.events(Builders.Query(startPartition = configStartPartition), table = itemQueriesTable)
    val range = JoinUtils.getRangeToFill(leftSource, tableUtils, endPartition, Some(overrideStartPartition))
    assertEquals(range, PartitionRange(overrideStartPartition, endPartition))
  }

  it should "disable dynamic partition pruning only inside final join write optimization scope" in {
    spark.conf.set(sparkDppEnabled, "true")

    var valueInsideScope: String = null
    JoinUtils.withFinalJoinWriteOptimizations(tableUtils) {
      valueInsideScope = spark.conf.get(sparkDppEnabled)
    }

    assertEquals("false", valueInsideScope)
    assertEquals("true", spark.conf.get(sparkDppEnabled))
  }

  it should "keep repeated partitioned left joins linear while final join write optimizations are active" in {
    val paths = Map("left" -> createPartitionedParquet("left")) ++
      (1 to 5).map(i => s"right_$i" -> createPartitionedParquet(s"right-$i")).toMap

    spark.conf.set(sparkDppEnabled, "true")
    val baselineJoinCount = optimizedJoinCount(dppNestedJoin(parts = 5, paths))
    val scopedJoinCount = JoinUtils.withFinalJoinWriteOptimizations(tableUtils) {
      this.optimizedJoinCount(dppNestedJoin(parts = 5, paths))
    }

    assertTrue(s"Expected Spark DPP to expand the plan, but found only $baselineJoinCount joins.",
               baselineJoinCount > 5)
    assertEquals(5, scopedJoinCount)
    assertEquals("true", spark.conf.get(sparkDppEnabled))
  }

  private def createPartitionedParquet(name: String): String = {
    val path = Files.createTempDirectory(s"chronon-join-utils-dpp-$name").toString
    spark
      .range(2)
      .selectExpr("cast(id as int) as id", "date_format(date_add(date '2026-06-16', cast(id as int)), 'yyyy-MM-dd') as ds")
      .write
      .mode("overwrite")
      .partitionBy("ds")
      .parquet(path)
    path
  }

  private def dppNestedJoin(parts: Int, paths: Map[String, String]): DataFrame = {
    val left = spark.read.parquet(paths("left")).where("ds in ('2026-06-16', '2026-06-17')").as("a")
    (1 to parts).foldLeft(left) { case (partial, i) =>
      partial.join(
        spark.read.parquet(paths(s"right_$i")).as(s"b$i"),
        col("a.ds") === col(s"b$i.ds"),
        "left"
      )
    }
  }

  private def optimizedJoinCount(df: DataFrame): Int =
    "\\bJoin\\b".r.findAllMatchIn(df.queryExecution.optimizedPlan.toString()).length

  import ai.chronon.api.{LongType, StringType, StructField, StructType}

  def createSampleTable(tableName: String = "testSampleTable"): DataFrame = {
    val schema = StructType(
      tableName,
      Array(
        StructField("listing", LongType),
        StructField("feature_review", LongType),
        StructField("feature_locale", StringType),
        StructField("ds", StringType),
        StructField("ts", StringType)
      )
    )
    val rows = List(
      Row(1L, 20L, "US", "2022-10-01", "2022-10-01 10:00:00"),
      Row(2L, 38L, "US", "2022-10-02", "2022-10-02 11:00:00"),
      Row(3L, 19L, "CA", "2022-10-01", "2022-10-01 08:00:00")
    )
    TestUtils.makeDf(spark, schema, rows)
  }
}
