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

import ai.chronon.aggregator.test.Column
import ai.chronon.api.Extensions._
import ai.chronon.api._
import ai.chronon.spark.Comparison
import ai.chronon.spark.Extensions._
import ai.chronon.spark.batch.StagingQuery
import ai.chronon.spark.catalog.{FormatProvider, TableUtils}
import ai.chronon.spark.utils.{DataFrameGen, SparkTestBase}
import org.apache.spark.sql.SparkSession
import org.junit.Assert.assertEquals
import org.slf4j.{Logger, LoggerFactory}

class StagingQueryTest extends SparkTestBase {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  implicit private val tableUtils: TableUtils = TableUtils(spark)

  private val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
  private val ninetyDaysAgo = tableUtils.partitionSpec.minus(today, new Window(90, TimeUnit.DAYS))
  private val namespace = "staging_query_chronon_test"
  createDatabase(namespace)

  it should "staging query" in {
    val schema = List(
      Column("user", StringType, 10),
      Column("session_length", IntType, 1000)
    )

    val df = DataFrameGen
      .events(spark, schema, count = 100000, partitions = 100)
      .dropDuplicates("ts") // duplicates can create issues in comparisons
    logger.info("Generated staging query data:")
    df.show()
    val viewName = s"$namespace.test_staging_query_compare"
    df.save(viewName)

    val function = "temp_replace_a"
    val stagingQueryConf = Builders.StagingQuery(
      query = s"select * from $viewName WHERE ds BETWEEN {{ start_date }} AND {{ end_date }}",
      startPartition = ninetyDaysAgo,
      setups = Seq(s"create temporary function $function as 'org.apache.hadoop.hive.ql.udf.UDFRegExpReplace'"),
      metaData = Builders.MetaData(name = "test.user_session_features",
                                   namespace = namespace,
                                   tableProperties = Map("key" -> "val"),
                                   customJson = "{\"additional_partition_cols\": [\"user\"]}")
    )

    val stagingQuery = new StagingQuery(stagingQueryConf, today, tableUtils)
    stagingQuery.computeStagingQuery(stepDays = Option(30))
    val expected =
      tableUtils.sql(s"select * from $viewName where ds between '$ninetyDaysAgo' and '$today' AND user IS NOT NULL")

    val computed = tableUtils.sql(s"select * from ${stagingQueryConf.metaData.outputTable} WHERE user IS NOT NULL")
    val diff = Comparison.sideBySide(expected, computed, List("user", "ts", "ds"))
    if (diff.count() > 0) {
      logger.info(s"Actual count: ${expected.count()}")
      expected.show()
      logger.info(s"Computed count: ${computed.count()}")
      computed.show()
      logger.info(s"Diff count: ${diff.count()}")
      logger.info("diff result rows")
      diff.show()
    }
    assertEquals(0, diff.count())

    // reset for overrideStartPartition test
    spark.sql(s"DROP TABLE IF EXISTS ${stagingQueryConf.metaData.outputTable}")
    spark.sql(s"DROP TEMPORARY FUNCTION IF EXISTS $function")
    stagingQuery.computeStagingQuery(stepDays = Option(30), overrideStartPartition = Option(today))
    val expectedWithOverrideStartPartition =
      tableUtils.sql(s"select * from $viewName where ds = '$today' AND user IS NOT NULL")

    val computedWithOverrideStartPartition =
      tableUtils.sql(s"select * from ${stagingQueryConf.metaData.outputTable} WHERE user IS NOT NULL")
    val diffWithOverrideStartPartition = Comparison.sideBySide(expectedWithOverrideStartPartition,
                                                               computedWithOverrideStartPartition,
                                                               List("user", "ts", "ds"))
    if (diffWithOverrideStartPartition.count() > 0) {
      println(s"Actual count: ${expectedWithOverrideStartPartition.count()}")
      println(expectedWithOverrideStartPartition.show())
      println(s"Computed count: ${computedWithOverrideStartPartition.count()}")
      println(computedWithOverrideStartPartition.show())
      println(s"Diff count: ${diffWithOverrideStartPartition.count()}")
      println("diffWithOverrideStartPartition result rows")
      diffWithOverrideStartPartition.show()
    }
    assertEquals(0, diffWithOverrideStartPartition.count())
  }

  /** Test Staging Query update with new feature/column added to the query.
    */
  it should "staging query auto expand" in {
    val schema = List(
      Column("user", StringType, 10),
      Column("session_length", IntType, 50),
      Column("new_feature", StringType, 50)
    )

    val df = DataFrameGen
      .events(spark, schema, count = 30, partitions = 8)
      .dropDuplicates("ts") // duplicates can create issues in comparisons
    logger.info("Generated staging query data:")
    df.show()
    val viewName = s"$namespace.test_staging_query_view"
    df.save(viewName)

    val fiveDaysAgo = tableUtils.partitionSpec.minus(today, new Window(5, TimeUnit.DAYS))
    val stagingQueryConf = Builders.StagingQuery(
      query =
        s"select user, session_length, ds, ts from $viewName WHERE ds BETWEEN {{ start_date }} AND {{ end_date }}",
      startPartition = ninetyDaysAgo,
      setups = Seq("create temporary function temp_replace_b as 'org.apache.hadoop.hive.ql.udf.UDFRegExpReplace'"),
      metaData = Builders.MetaData(name = "test.user_auto_expand",
                                   namespace = namespace,
                                   tableProperties = Map("key" -> "val"),
                                   customJson = "{\"additional_partition_cols\": [\"user\"]}")
    )

    val stagingQuery = new StagingQuery(stagingQueryConf, fiveDaysAgo, tableUtils)
    stagingQuery.computeStagingQuery(stepDays = Option(30))
    val expected =
      tableUtils.sql(
        s"select user, session_length, ds, ts from $viewName where ds between '$ninetyDaysAgo' and '$fiveDaysAgo' AND user IS NOT NULL")

    val computed = tableUtils.sql(s"select * from ${stagingQueryConf.metaData.outputTable} WHERE user IS NOT NULL")
    val diff = Comparison.sideBySide(expected, computed, List("user", "ts", "ds"))
    assertEquals(0, diff.count())

    // Add new feature to the query
    val stagingQueryConfUpdated = Builders.StagingQuery(
      query =
        s"select user, session_length, new_feature, ds, ts from $viewName WHERE ds BETWEEN {{ start_date }} AND {{ end_date }}",
      startPartition = fiveDaysAgo,
      metaData = Builders.MetaData(name = "test.user_auto_expand",
                                   namespace = namespace,
                                   tableProperties = Map("key" -> "val"),
                                   customJson = "{\"additional_partition_cols\": [\"user\"]}")
    )
    val stagingQueryUpdated = new StagingQuery(stagingQueryConfUpdated, today, tableUtils)
    stagingQueryUpdated.computeStagingQuery(stepDays = Option(30))
    val fourDaysAgo = tableUtils.partitionSpec.minus(today, new Window(4, TimeUnit.DAYS))
    val expectedUpdated =
      tableUtils.sql(
        s"select user, session_length, new_feature, ds, ts from $viewName where ds between '$fourDaysAgo' and '$today' AND user IS NOT NULL")
    expectedUpdated.show()
    val computedUpdated = tableUtils.sql(
      s"select * from ${stagingQueryConfUpdated.metaData.outputTable} WHERE user IS NOT NULL and ds between '$fourDaysAgo' and '$today'")
    computedUpdated.show()

    val diffV2 = Comparison.sideBySide(expectedUpdated, computedUpdated, List("user", "ts", "ds"))
    if (diffV2.count() > 0) {
      logger.info(s"Actual count: ${expectedUpdated.count()}")
      expectedUpdated.show()
      logger.info(s"Computed count: ${computedUpdated.count()}")
      computedUpdated.show()
      logger.info(s"Diff count: ${diffV2.count()}")
      logger.info("diff result rows")
      diffV2.show()
    }
    assertEquals(0, diffV2.count())
  }

  /** Test that latest date is not changed between step ranges.
    * Compute in several step ranges a trivial query and for the first step range (first partition) the latest_date
    * value should be that of the latest partition (today).
    */
  it should "staging query latest date" in {
    val schema = List(
      Column("user", StringType, 10),
      Column("session_length", IntType, 1000)
    )

    val df = DataFrameGen
      .events(spark, schema, count = 1000, partitions = 100)
      .dropDuplicates("ts") // duplicates can create issues in comparisons
    val viewName = s"$namespace.test_staging_query_latest_date"
    df.save(viewName)

    val stagingQueryConf = Builders.StagingQuery(
      query = s"""
            |SELECT
            |  *
            |  , {{ latest_date }} AS latest_ds
            |FROM $viewName
            |WHERE ds BETWEEN {{ start_date }} AND {{ end_date }}""".stripMargin,
      startPartition = ninetyDaysAgo,
      metaData = Builders.MetaData(name = "test.staging_latest_date",
                                   namespace = namespace,
                                   tableProperties = Map("key" -> "val"))
    )
    val stagingQuery = new StagingQuery(stagingQueryConf, today, tableUtils)
    stagingQuery.computeStagingQuery(stepDays = Option(30))
    val expected =
      tableUtils.sql(s"""
                   |SELECT
                   |  *
                   |  , '$today' as latest_ds
                   |FROM $viewName
                   |WHERE ds = '$ninetyDaysAgo' AND user IS NOT NULL""".stripMargin)

    val computed = tableUtils.sql(s"""
      |SELECT * FROM ${stagingQueryConf.metaData.outputTable}
      |WHERE user IS NOT NULL AND ds = '$ninetyDaysAgo'
      |""".stripMargin)
    val diff = Comparison.sideBySide(expected, computed, List("user", "ts", "ds"))
    if (diff.count() > 0) {
      logger.info(s"Actual count: ${expected.count()}")
      expected.show()
      logger.info(s"Computed count: ${computed.count()}")
      computed.show()
      logger.info(s"Diff count: ${diff.count()}")
      logger.info("diff result rows")
      diff.show()
    }
    assertEquals(0, diff.count())
  }

  it should "staging query max date" in {
    val schema = List(
      Column("user", StringType, 10),
      Column("session_length", IntType, 1000)
    )

    val df = DataFrameGen
      .events(spark, schema, count = 1000, partitions = 100)
      .dropDuplicates("ts") // duplicates can create issues in comparisons
    val viewName = s"$namespace.test_staging_query_max_date"
    df.save(viewName)
    val maxDate = tableUtils.partitions(viewName).max

    val stagingQueryConf = Builders.StagingQuery(
      query = s"""
                 |SELECT
                 |  *
                 |  , {{ max_date(table=$viewName) }} AS latest_ds
                 |FROM $viewName
                 |WHERE ds BETWEEN {{ start_date }} AND {{ end_date }}""".stripMargin,
      startPartition = ninetyDaysAgo,
      metaData =
        Builders.MetaData(name = "test.staging_max_date", namespace = namespace, tableProperties = Map("key" -> "val"))
    )
    val stagingQuery = new StagingQuery(stagingQueryConf, today, tableUtils)
    stagingQuery.computeStagingQuery(stepDays = Option(30))
    val expected =
      tableUtils.sql(s"""
                        |SELECT
                        |  *
                        |  , '$maxDate' as latest_ds
                        |FROM $viewName
                        |WHERE ds = '$ninetyDaysAgo' AND user IS NOT NULL""".stripMargin)

    val computed = tableUtils.sql(s"""
                                     |SELECT * FROM ${stagingQueryConf.metaData.outputTable}
                                     |WHERE user IS NOT NULL AND ds = '$ninetyDaysAgo'
                                     |""".stripMargin)
    val diff = Comparison.sideBySide(expected, computed, List("user", "ts", "ds"))
    if (diff.count() > 0) {
      logger.info(s"Actual count: ${expected.count()}")
      expected.show()
      logger.info(s"Computed count: ${computed.count()}")
      computed.show()
      logger.info(s"Diff count: ${diff.count()}")
      logger.info("diff result rows")
      diff.show()
    }
    assertEquals(0, diff.count())
  }

  private def getPartitionColumnNames(tableName: String)(implicit spark: SparkSession): Seq[String] = {
    try {
      val format = FormatProvider.from(spark).readFormat(tableName).get
      val partitionsMap = format.partitions(tableName, "")
      if (partitionsMap.nonEmpty) {
        partitionsMap.head.keys.toSeq
      } else {
        Seq.empty
      }
    } catch {
      case e: Exception =>
        logger.warn(s"Could not get partition column names for $tableName: ${e.getMessage}")
        Seq.empty
    }
  }

  it should "handle additional output partition columns" in {
    val schema = List(
      Column("user", StringType, 10),
      Column("region", StringType, 5, nullRate = 0.0), // partition columns cannot have null
      Column("device", StringType, 3, nullRate = 0.0), // partition columns cannot have null
      Column("session_length", IntType, 1000)
    )

    // Generate test data with columns that can be used for additional partitioning
    val df = DataFrameGen
      .events(spark, schema, count = 10000, partitions = 20)
      .dropDuplicates("ts")
    logger.info("Generated test data for additional partition columns:")
    df.show()

    val tableName = s"$namespace.test_additional_partition_cols"
    df.save(tableName)

    // Define a staging query with multiple additional partition columns
    val stagingQueryConf = Builders.StagingQuery(
      query = s"select * from $tableName WHERE ds BETWEEN {{ start_date }} AND {{ end_date }}",
      startPartition = ninetyDaysAgo,
      metaData = Builders.MetaData(
        name = "test.additional_partitions",
        namespace = namespace,
        additionalOutputPartitionColumns = Seq("region", "device"), // Explicitly specify additional partition columns
        tableProperties = Map("key" -> "val")
      )
    )

    val stagingQuery = new StagingQuery(stagingQueryConf, today, tableUtils)
    stagingQuery.computeStagingQuery(stepDays = Option(30))

    // Verify the data was written correctly
    val expected = tableUtils.sql(
      s"select * from $tableName where ds between '$ninetyDaysAgo' and '$today'"
    )

    val computed = tableUtils.sql(s"select * from ${stagingQueryConf.metaData.outputTable}")
    val diff = Comparison.sideBySide(expected, computed, List("user", "ts", "ds"))

    val diffCount = diff.count()
    if (diffCount > 0) {
      logger.info("Different rows between expected and computed")

      logger.info("Expected rows")
      expected.show()

      logger.info("Computed rows")
      computed.show()

      logger.info("Diff rows (SxS)")
      diff.show()
    }

    assertEquals(0, diff.count())

    // Get the partition column names from the table metadata
    val partitionColumnNames = getPartitionColumnNames(stagingQueryConf.metaData.outputTable)(spark)

    // Verify all expected partition columns are present
    val expectedPartitionCols = Seq(tableUtils.partitionColumn, "region", "device")
    assertEquals(expectedPartitionCols.toSet, partitionColumnNames.toSet)
  }

  private val threeHourSpec = PartitionSpec("ds", "yyyy-MM-dd-HH-mm", 3 * 60 * 60 * 1000)

  it should "render inclusive sub-daily macro values with an exclusive end via offset" in {
    val subDailyTableUtils = TableUtils(spark, threeHourSpec)
    // the macro engine renders quoted values
    val rendered = StagingQuery.substitute(
      subDailyTableUtils,
      "SELECT {{ start_date }} AS s, {{ end_date }} AS e, {{ end_date(offset=1) }} AS x",
      start = "2024-01-01-06-00",
      end = "2024-01-01-06-00",
      latest = "2024-01-01-06-00"
    )

    // start_date and end_date are inclusive output-domain ds values; offset=1 renders the
    // exclusive interval end
    assertEquals(
      "SELECT '2024-01-01-06-00' AS s, '2024-01-01-06-00' AS e, '2024-01-01-09-00' AS x",
      rendered
    )
  }

  // Space/colon formats are NOT the default (ds values become object-store directory names,
  // where spaces and colons URL-escape — Hive percent-escapes colons), but they remain
  // expressible when declared explicitly. This test deliberately declares the legacy
  // space/colon format to lock the explicit-format capability.
  private val spaceColonSpec = PartitionSpec("ds", "yyyy-MM-dd HH:mm", 3 * 60 * 60 * 1000)

  it should "write and replay sub-daily partitions with an explicit space/colon format" in {
    val subDailyTableUtils = TableUtils(spark, spaceColonSpec)
    val inputTable = s"$namespace.subdaily_staging_input"
    spark.sql(s"DROP TABLE IF EXISTS $inputTable")
    spark.sql(s"""CREATE TABLE $inputTable (
                 |  user STRING,
                 |  session_length INT,
                 |  ds STRING
                 |)
                 |PARTITIONED BY (ds)""".stripMargin)
    spark.sql(s"""INSERT INTO $inputTable VALUES
                 |('a', 1, '2024-01-01 03:00'),
                 |('b', 2, '2024-01-01 03:00'),
                 |('c', 3, '2024-01-01 06:00')
                 |""".stripMargin)

    val stagingQueryConf = Builders.StagingQuery(
      query = s"select * from $inputTable WHERE ds BETWEEN {{ start_date }} AND {{ end_date }}",
      startPartition = "2024-01-01 00:00",
      metaData = Builders.MetaData(name = "test.subdaily_staging", namespace = namespace)
    )
    val outputTable = stagingQueryConf.metaData.outputTable

    val stagingQuery = new StagingQuery(stagingQueryConf, "2024-01-01 06:00", subDailyTableUtils)

    def fire(partition: String): Unit =
      stagingQuery.compute(PartitionRange(partition, partition)(spaceColonSpec), Seq.empty, Some(true))

    // distinct schedule fires write distinct formatted partitions
    fire("2024-01-01 03:00")
    // explicitly-declared space/colon partition values must round-trip through the catalog
    assertEquals(List("2024-01-01 03:00"), subDailyTableUtils.partitions(outputTable).sorted)

    fire("2024-01-01 06:00")
    assertEquals(List("2024-01-01 03:00", "2024-01-01 06:00"), subDailyTableUtils.partitions(outputTable).sorted)

    // replaying one fire is idempotent and only touches that partition
    spark.sql(s"INSERT INTO $inputTable VALUES ('d', 4, '2024-01-01 03:00')")
    fire("2024-01-01 03:00")

    val threeOClockUsers = spark
      .sql(s"SELECT user FROM $outputTable WHERE ds = '2024-01-01 03:00'")
      .collect()
      .map(_.getString(0))
      .sorted
    assertEquals(Seq("a", "b", "d"), threeOClockUsers.toSeq)

    val sixOClockUsers = spark
      .sql(s"SELECT user FROM $outputTable WHERE ds = '2024-01-01 06:00'")
      .collect()
      .map(_.getString(0))
      .sorted
    assertEquals(Seq("c"), sixOClockUsers.toSeq)
  }
}
