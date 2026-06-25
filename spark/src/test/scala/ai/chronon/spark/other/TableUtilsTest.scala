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

import ai.chronon.api._
import ai.chronon.spark._
import ai.chronon.spark.catalog.{Format, Hive, IncompatibleSchemaException, TableUtils}
import ai.chronon.spark.submission.SparkSessionBuilder
import ai.chronon.spark.utils.SparkTestBase
import ai.chronon.spark.utils.TestUtils.makeDf
import org.apache.hadoop.hive.ql.exec.UDF
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Row, _}
import org.junit.Assert.{assertEquals, assertFalse, assertNull, assertTrue}
import org.scalatest.flatspec.AnyFlatSpec

import scala.util.Try

case class TestRecord(ds: String, id: String)

class SimpleAddUDF extends UDF {
  def evaluate(value: Int): Int = {
    value + 20
  }
}

class TableUtilsTest extends AnyFlatSpec {
  val spark: SparkSession = SparkSessionBuilder.build("TableUtilsTest", local = true)

  private val tableUtils = TableUtils(spark)
  private implicit val partitionSpec: PartitionSpec = tableUtils.partitionSpec

  it should "handle special characters in column names with TableUtils.insertPartitions" in {
    val specialTableName = "db.special_chars_table"
    spark.sql("CREATE DATABASE IF NOT EXISTS db")

    // Create a struct type named "with" that contains a field "dots"
    val withStructType = StructType(
      "with",
      Array(StructField("dots", IntType), StructField("id", StringType))
    )

    // Create data for our test
    val row1 = Row("value1", 42, true, Row(123, "id1"), "2023-01-01")
    val row2 = Row("value2", 84, false, Row(456, "id2"), "2023-01-02")

    // Define schema with:
    // 1. "with.dots" - a column with dots in the name
    // 2. "with" - a struct that contains a field named "dots"
    val schema = StructType(
      specialTableName,
      Array(
        StructField("normal", StringType),
        StructField("with.dots", IntType), // Column with dots
        StructField("with#hash", BooleanType), // Column with hash
        StructField("with", withStructType), // Struct named "with" with field "dots"
        StructField("ds", StringType)
      )
    )

    // Create the DataFrame with our complex schema
    val specialCharsData = makeDf(spark, schema, List(row1, row2))

    try {
      // Use TableUtils.insertPartitions with our fixed column reference handling
      tableUtils.insertPartitions(
        specialCharsData,
        specialTableName,
        partitionColumns = List("ds")
      )

      // Verify that columns were preserved correctly
      val loadedData = tableUtils.loadTable(specialTableName)
      val expectedColumns = List("normal", "with.dots", "with#hash", "with", "ds")
      assertEquals(expectedColumns, loadedData.columns.toList)

      // Verify column values including both with.dots and with.dots
      val day1Data = loadedData.where(col("ds") === "2023-01-01").collect()
      assertEquals(1, day1Data.length)
      assertEquals("value1", day1Data(0).getAs[String]("normal"))
      assertEquals(42, day1Data(0).getAs[Int]("with.dots")) // Dot column
      assertEquals(true, day1Data(0).getAs[Boolean]("with#hash"))

      // Verify the struct field "with" that contains field "dots"
      val withStruct = day1Data(0).getAs[Row]("with")
      assertEquals(123, withStruct.getAs[Int]("dots")) // Same as with.dots in dot notation
      assertEquals("id1", withStruct.getAs[String]("id"))

      // Create a DataFrame with a backtick and a column with dots and hash
      val backticksData = makeDf(
        spark,
        StructType(
          specialTableName,
          Array(
            StructField("with`backtick", StringType),
            StructField("num", IntType),
            StructField("with.hash#mix", DoubleType), // Column with both dots and hash
            StructField("ds", StringType)
          )
        ),
        List(
          Row("tick", 100, 99.9, "2023-01-03")
        )
      )

      // Test with autoExpand=true which uses our other fixed code path
      tableUtils.insertPartitions(
        backticksData,
        specialTableName,
        partitionColumns = List("ds"),
        autoExpand = true
      )

      // Verify all columns are present after expansion
      val updatedData = tableUtils.loadTable(specialTableName)
      val allExpectedCols =
        expectedColumns.reverse.tail.reverse ++ List("with`backtick", "num", "with.hash#mix") :+ "ds"
      assertEquals(allExpectedCols, updatedData.columns.toList)

      // Verify the new row data
      val day3Data = updatedData.where(col("ds") === "2023-01-03").collect()
      assertEquals(1, day3Data.length)
      assertEquals("tick", day3Data(0).getAs[String]("with`backtick"))
      assertEquals(100, day3Data(0).getAs[Int]("num"))
      assertEquals(99.9, day3Data(0).getAs[Double]("with.hash#mix"), 0.0)

      // Null for fields not in this row
      assertNull(day3Data(0).getAs[Row]("with"))
    } finally {
      // Clean up
      spark.sql(s"DROP TABLE IF EXISTS $specialTableName")
    }
  }

  it should "handle schema expansion with TableUtils.insertPartitions" in {
    val expandTableName = "db.expand_table"
    spark.sql("CREATE DATABASE IF NOT EXISTS db")

    // Create initial DataFrame with base columns
    val initialData = spark
      .createDataFrame(
        Seq(
          (1L, "A", "2023-01-01")
        ))
      .toDF("id", "name", "ds")

    try {
      import org.junit.Assert.assertNull
      // Insert initial data
      tableUtils.insertPartitions(
        initialData,
        expandTableName,
        partitionColumns = List("ds")
      )

      // Create DataFrame with additional columns
      val expandedData = spark
        .createDataFrame(
          Seq(
            (2L, "B", Some(25), Some("user@example.com"), "2023-01-02")
          ))
        .toDF("id", "name", "age", "email", "ds")

      // Use autoExpand=true to test the column expansion logic that we fixed
      tableUtils.insertPartitions(
        expandedData,
        expandTableName,
        partitionColumns = List("ds"),
        autoExpand = true
      )

      // Verify the expanded schema
      val loadedData = tableUtils.loadTable(expandTableName)
      val expectedColumns = List("id", "name", "age", "email", "ds")
      assertEquals(expectedColumns, loadedData.columns.toList)

      // Original row should have nulls for new columns
      val day1Data = loadedData.where(col("ds") === "2023-01-01").collect()
      assertEquals(1, day1Data.length)
      assertEquals(1L, day1Data(0).getAs[Long]("id"))
      assertEquals("A", day1Data(0).getAs[String]("name"))
      assertNull(day1Data(0).getAs[Integer]("age"))
      assertNull(day1Data(0).getAs[String]("email"))

      // New row should have all columns populated
      val day2Data = loadedData.where(col("ds") === "2023-01-02").collect()
      assertEquals(1, day2Data.length)
      assertEquals(2L, day2Data(0).getAs[Long]("id"))
      assertEquals("B", day2Data(0).getAs[String]("name"))
      assertEquals(25, day2Data(0).getAs[Int]("age"))
      assertEquals("user@example.com", day2Data(0).getAs[String]("email"))
    } finally {
      // Clean up
      spark.sql(s"DROP TABLE IF EXISTS $expandTableName")
    }
  }

  it should "column from sql" in {
    val sampleSql =
      """
        |SELECT
        |  CASE WHEN column_a IS NULL THEN 1 ELSE NULL END,
        |  column_b,
        |  column_c AS not_this_one,
        |  COALESCE(IF(column_d, column_e, NULL), column_e) AS not_this_one_either,
        |  column_nested.first.second AS not_this_one_as_well
        |FROM fake_table
        |WHERE column_f IS NOT NULL AND column_g != 'Something' AND column_d > 0
        |""".stripMargin

    val columns = tableUtils.getColumnsFromQuery(sampleSql)
    val expected = Seq("column_a",
                       "column_b",
                       "column_c",
                       "column_d",
                       "column_e",
                       "column_f",
                       "column_g",
                       "column_nested.first.second").sorted
    assertEquals(expected, columns.sorted)
  }

  private def testInsertPartitions(tableName: String,
                                   df1: DataFrame,
                                   df2: DataFrame,
                                   ds1: String,
                                   ds2: String): Unit = {
    tableUtils.insertPartitions(df1, tableName, autoExpand = true)
    val addedColumns = df2.schema.fieldNames.filterNot(df1.schema.fieldNames.contains)
    val removedColumns = df1.schema.fieldNames.filterNot(df2.schema.fieldNames.contains)
    val inconsistentColumns = (
      for (
        (name1, dtype1) <- df1.schema.fields.map(structField => (structField.name, structField.dataType));
        (name2, dtype2) <- df2.schema.fields.map(structField => (structField.name, structField.dataType))
      ) yield {
        name1 == name2 && dtype1 != dtype2
      }
    ).filter(identity)

    if (inconsistentColumns.nonEmpty) {
      val insertTry = Try(tableUtils.insertPartitions(df2, tableName, autoExpand = true))
      val e = insertTry.failed.get.asInstanceOf[IncompatibleSchemaException]
      assertEquals(inconsistentColumns.length, e.inconsistencies.length)
      return
    }

    if (df2.schema != df1.schema) {
      val insertTry = Try(tableUtils.insertPartitions(df2, tableName))
      assertTrue(insertTry.failed.get.isInstanceOf[AnalysisException])
    }

    tableUtils.insertPartitions(df2, tableName, autoExpand = true)

    val dataRead1 = tableUtils.loadTable(tableName).where(col("ds") === ds1)
    val dataRead2 = tableUtils.loadTable(tableName).where(col("ds") === ds2)
    assertTrue(dataRead1.columns.length == dataRead2.columns.length)

    val totalColumnsCount = (df1.schema.fieldNames.toSet ++ df2.schema.fieldNames.toSet).size
    assertEquals(totalColumnsCount, dataRead1.columns.length)
    assertEquals(totalColumnsCount, dataRead2.columns.length)

    addedColumns.foreach(col => {
      dataRead1.foreach(row => assertTrue(Option(row.getAs[Any](col)).isEmpty))
    })
    removedColumns.foreach(col => {
      dataRead2.foreach(row => assertTrue(Option(row.getAs[Any](col)).isEmpty))
    })
  }

  it should "insert partitions add columns" in {
    val tableName = "db.test_table_1"
    spark.sql("CREATE DATABASE IF NOT EXISTS db")
    val columns1 = Array(
      StructField("long_field", LongType),
      StructField("int_field", IntType),
      StructField("string_field", StringType)
    )
    val df1 = makeDf(
      spark,
      StructType(
        tableName,
        columns1 :+ StructField("ds", StringType)
      ),
      List(
        Row(1L, 2, "3", "2022-10-01")
      )
    )

    val df2 = makeDf(
      spark,
      StructType(
        tableName,
        columns1
          :+ StructField("double_field", DoubleType)
          :+ StructField("ds", StringType)
      ),
      List(
        Row(4L, 5, "6", 7.0, "2022-10-02")
      )
    )

    testInsertPartitions(tableName, df1, df2, ds1 = "2022-10-01", ds2 = "2022-10-02")
  }

  it should "insert partitions remove columns" in {
    val tableName = "db.test_table_2"
    spark.sql("CREATE DATABASE IF NOT EXISTS db")
    val columns1 = Array(
      StructField("long_field", LongType),
      StructField("int_field", IntType),
      StructField("string_field", StringType)
    )
    val df1 = makeDf(
      spark,
      StructType(
        tableName,
        columns1
          :+ StructField("double_field", DoubleType)
          :+ StructField("ds", StringType)
      ),
      List(
        Row(1L, 2, "3", 4.0, "2022-10-01")
      )
    )

    val df2 = makeDf(
      spark,
      StructType(
        tableName,
        columns1 :+ StructField("ds", StringType)
      ),
      List(
        Row(5L, 6, "7", "2022-10-02")
      )
    )
    testInsertPartitions(tableName, df1, df2, ds1 = "2022-10-01", ds2 = "2022-10-02")
  }

  it should "insert partitions modified columns" in {
    val tableName = "db.test_table_3"
    spark.sql("CREATE DATABASE IF NOT EXISTS db")
    val columns1 = Array(
      StructField("long_field", LongType),
      StructField("int_field", IntType)
    )
    val df1 = makeDf(
      spark,
      StructType(
        tableName,
        columns1
          :+ StructField("string_field", StringType)
          :+ StructField("ds", StringType)
      ),
      List(
        Row(1L, 2, "3", "2022-10-01")
      )
    )

    val df2 = makeDf(
      spark,
      StructType(
        tableName,
        columns1
          :+ StructField("string_field", DoubleType) // modified column data type
          :+ StructField("ds", StringType)
      ),
      List(
        Row(1L, 2, 3.0, "2022-10-02")
      )
    )

    testInsertPartitions(tableName, df1, df2, ds1 = "2022-10-01", ds2 = "2022-10-02")
  }

  it should "chunk" in {
    val actual = tableUtils.chunk(Set("2021-01-01", "2021-01-02", "2021-01-05", "2021-01-07"))
    val expected = Seq(
      PartitionRange("2021-01-01", "2021-01-02"),
      PartitionRange("2021-01-05", "2021-01-05"),
      PartitionRange("2021-01-07", "2021-01-07")
    )
    assertEquals(expected, actual)
  }


  it should "double udf registration" in {
    tableUtils.sql("CREATE TEMPORARY FUNCTION test AS 'ai.chronon.spark.other.SimpleAddUDF'")
    tableUtils.sql("CREATE TEMPORARY FUNCTION test AS 'ai.chronon.spark.other.SimpleAddUDF'")
  }

  it should "insert partitions table reachable already" in {
    val tableName = "db.test_table_exists_already"

    spark.sql("CREATE DATABASE IF NOT EXISTS db")
    val columns = Array(
      StructField("long_field", LongType),
      StructField("int_field", IntType),
      StructField("string_field", StringType),
      StructField("ds", StringType)
    )

    // Create the table beforehand
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName (long_field LONG, int_field INT, string_field STRING, ds STRING)")

    val df1 = makeDf(
      spark,
      StructType(
        tableName,
        columns
      ),
      List(
        Row(1L, 2, "3", "2022-10-01")
      )
    )
    val df2 = makeDf(
      spark,
      StructType(
        tableName,
        columns
      ),
      List(
        Row(1L, 2, "3", "2022-10-02")
      )
    )

    // check if insertion still works
    testInsertPartitions(tableName, df1, df2, ds1 = "2022-10-01", ds2 = "2022-10-02")
  }

  it should "create table already exists" in {
    val tableName = "db.test_create_table_already_exists"
    spark.sql("CREATE DATABASE IF NOT EXISTS db")

    val columns = Array(
      StructField("long_field", LongType),
      StructField("int_field", IntType),
      StructField("string_field", StringType)
    )

    spark.sql(
      "CREATE TABLE IF NOT EXISTS db.test_create_table_already_exists (long_field LONG, int_field INT, string_field STRING)")

    try {
      val df = makeDf(
        spark,
        StructType(
          tableName,
          columns
        ),
        List(
          Row(1L, 2, "3")
        )
      )
      tableUtils.insertPartitions(df, tableName, partitionColumns = List.empty)
      assertTrue(spark.catalog.tableExists(tableName))
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
  }

  it should "repartitioning an empty dataframe should work" in {
    import spark.implicits._
    val tableName = "db.test_empty_table"
    SparkTestBase.createDatabase(spark, "db")

    tableUtils.insertPartitions(spark.emptyDataset[TestRecord].toDF(), tableName)
    val res = tableUtils.loadTable(tableName)
    assertEquals(0, res.count)

    tableUtils.insertPartitions(spark.createDataFrame(List(TestRecord("2025-01-01", "a"))), tableName)
    val newRes = tableUtils.loadTable(tableName)

    assertEquals(1, newRes.count)
  }

  it should "create table" in {
    val tableName = "db.test_create_table"
    spark.sql("CREATE DATABASE IF NOT EXISTS db")
    try {
      val columns = Array(
        StructField("long_field", LongType),
        StructField("int_field", IntType),
        StructField("string_field", StringType)
      )
      val df = makeDf(
        spark,
        StructType(
          tableName,
          columns
        ),
        List(
          Row(1L, 2, "3")
        )
      )
      tableUtils.insertPartitions(df, tableName, partitionColumns = List.empty)
      assertTrue(spark.catalog.tableExists(tableName))
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
  }

  it should "create table with semantic hash renames to final name" in {
    val dbName = s"db_${System.currentTimeMillis()}"
    val tableName = s"$dbName.test_semantic_hash"
    val hash = "abc123"
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $dbName")
    try {
      val schema = new org.apache.spark.sql.types.StructType()
        .add("id", org.apache.spark.sql.types.IntegerType)
        .add("value", org.apache.spark.sql.types.StringType)
      Hive.createTable(tableName, schema, List.empty, null, Some(hash))(spark)
      assertTrue(s"Final table $tableName should exist", spark.catalog.tableExists(tableName))
      assertFalse(s"Intermediate table should not exist",
        spark.catalog.tableExists(s"$dbName.test_semantic_hash_$hash"))
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
      spark.sql(s"DROP TABLE IF EXISTS $dbName.test_semantic_hash_$hash")
    }
  }

  it should "semantic hash appends only to table name component, not database" in {
    val dbName = s"db_${System.currentTimeMillis()}"
    val tableName = s"$dbName.test_hash_component"
    val hash = "def456"
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $dbName")
    try {
      val schema = new org.apache.spark.sql.types.StructType()
        .add("id", org.apache.spark.sql.types.IntegerType)
        .add("value", org.apache.spark.sql.types.StringType)
      Hive.createTable(tableName, schema, List.empty, null, Some(hash))(spark)
      assertTrue(s"Final table $tableName should exist", spark.catalog.tableExists(tableName))
      // The intermediate table should have the hash appended to the table part only
      assertFalse(s"Intermediate table should not exist after rename",
        spark.catalog.tableExists(s"$dbName.test_hash_component_$hash"))
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
      spark.sql(s"DROP TABLE IF EXISTS $dbName.test_hash_component_$hash")
    }
  }

  it should "semantic hash handles backtick-quoted identifiers" in {
    val dbName = s"db_${System.currentTimeMillis()}"
    val tableName = s"`$dbName`.`test_quoted_hash`"
    val hash = "xyz789"
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $dbName")
    try {
      val schema = new org.apache.spark.sql.types.StructType()
        .add("id", org.apache.spark.sql.types.IntegerType)
        .add("value", org.apache.spark.sql.types.StringType)
      Hive.createTable(tableName, schema, List.empty, null, Some(hash))(spark)
      assertTrue(s"Final table should exist", spark.catalog.tableExists(s"$dbName.test_quoted_hash"))
      assertFalse(s"Intermediate hashed table should not exist",
        spark.catalog.tableExists(s"$dbName.test_quoted_hash_$hash"))
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $dbName.test_quoted_hash")
      spark.sql(s"DROP TABLE IF EXISTS $dbName.test_quoted_hash_$hash")
    }
  }

  it should "create table without semantic hash uses name directly" in {
    val dbName = s"db_${System.currentTimeMillis()}"
    val tableName = s"$dbName.test_no_hash"
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $dbName")
    try {
      val schema = new org.apache.spark.sql.types.StructType()
        .add("id", org.apache.spark.sql.types.IntegerType)
        .add("value", org.apache.spark.sql.types.StringType)
      Hive.createTable(tableName, schema, List.empty, null, None)(spark)
      assertTrue(s"Table $tableName should exist", spark.catalog.tableExists(tableName))
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
  }

  it should "rename tables" in {
    val dbName = "db"
    val srcTable = s"$dbName.test_rename_src"
    val destTable = s"$dbName.test_rename_dest"

    spark.sql(s"CREATE DATABASE IF NOT EXISTS $dbName")
    try {
      spark.sql(s"CREATE TABLE $srcTable (id INT, ds STRING)")
      spark.sql(s"INSERT INTO $srcTable VALUES (1, '2024-01-01'), (2, '2024-01-02')")

      tableUtils.renameTable(srcTable, destTable)

      assertFalse(s"Original table should not exist after rename", spark.catalog.tableExists(srcTable))
      assertTrue(s"Destination table should exist after rename", spark.catalog.tableExists(destTable))
      assertEquals(2L, spark.table(destTable).count())
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $srcTable")
      spark.sql(s"DROP TABLE IF EXISTS $destTable")
    }
  }

  it should "test catalog detection" in {
    implicit val localSparkRef: SparkSession = spark
    assertEquals("catalogA", Format.getCatalog("catalogA.foo.bar"))
    assertEquals("catalogA", Format.getCatalog("`catalogA`.foo.bar"))
    assertEquals("spark_catalog", Format.getCatalog("`catalogA.foo`.bar"))
    assertEquals("spark_catalog", Format.getCatalog("`catalogA.foo.bar`"))
    assertEquals("spark_catalog", Format.getCatalog("foo.bar"))
    assertEquals("spark_catalog", Format.getCatalog("bar"))
    assertThrows[ParseException](Format.getCatalog(""))
  }

  it should "generate whereClauses with default partition column" in {
    val range = PartitionRange("2024-01-01", "2024-01-10")
    // Uses exclusive end (< next(end)) which works for both partitioned and time-partitioned tables
    val clauses = tableUtils.whereClauses(range)
    assertEquals(Seq("ds >= '2024-01-01'", "ds < '2024-01-11'"), clauses)
  }

  it should "generate whereClauses with custom partition column" in {
    val range = PartitionRange("2024-01-01", "2024-01-10")
    val clauses = tableUtils.whereClauses(range, partitionColumn = Some("date"))
    assertEquals(Seq("date >= '2024-01-01'", "date < '2024-01-11'"), clauses)
  }

  it should "generate whereClauses with null start" in {
    val range = PartitionRange(null, "2024-01-10")
    val clauses = tableUtils.whereClauses(range)
    assertEquals(Seq("ds < '2024-01-11'"), clauses)
  }

  it should "generate whereClauses with null end" in {
    val range = PartitionRange("2024-01-01", null)
    val clauses = tableUtils.whereClauses(range)
    assertEquals(Seq("ds >= '2024-01-01'"), clauses)
  }

  it should "return virtual partitions for time-partitioned table" in {
    val dbName = s"db_${System.nanoTime()}"
    val tableName = s"$dbName.time_partitioned_test"
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $dbName")

    import spark.implicits._
    // Create an unpartitioned table with a timestamp column spanning 5 calendar dates.
    // Use midday times to avoid timezone boundary issues (Spark uses UTC for date cast)
    val data = Seq(
      ("user1", java.sql.Timestamp.valueOf("2024-01-01 12:00:00")),
      ("user2", java.sql.Timestamp.valueOf("2024-01-02 12:00:00")),
      ("user3", java.sql.Timestamp.valueOf("2024-01-03 12:00:00")),
      ("user1", java.sql.Timestamp.valueOf("2024-01-05 12:00:00"))
    ).toDF("user_id", "created_at")

    data.write.saveAsTable(tableName)

    val partitions = tableUtils.partitions(tableName, timePartitioned = true,
      tablePartitionSpec = Some(PartitionSpec("created_at", "yyyy-MM-dd", 24 * 60 * 60 * 1000)))

    // Timestamp-derived virtual partitions expose the last complete interval.
    // With max(created_at) inside 2024-01-05, 2024-01-04 is the last complete daily interval.
    assertEquals(List("2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04"), partitions)

    spark.sql(s"DROP TABLE IF EXISTS $tableName")
    spark.sql(s"DROP DATABASE IF EXISTS $dbName")
  }

  it should "floor off-grid timestamps to the grid for sub-daily virtual partitions" in {
    val dbName = s"db_${System.nanoTime()}"
    val tableName = s"$dbName.subdaily_time_partitioned"
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $dbName")

    // min/max are deliberately off-boundary: ds values must land on the declared 3h grid.
    // SQL timestamp literals parse in the session timezone (UTC), unlike Timestamp.valueOf.
    spark.sql(s"CREATE TABLE $tableName (user_id STRING, created_at TIMESTAMP)")
    spark.sql(s"""
      INSERT INTO $tableName VALUES
        ('user1', TIMESTAMP '2024-01-01 09:17:00'),
        ('user2', TIMESTAMP '2024-01-01 14:05:00')
    """)

    val threeHourSpec =
      PartitionSpec("created_at", "yyyy-MM-dd-HH-mm", 3 * 60 * 60 * 1000)
    val partitions = tableUtils.partitions(tableName, timePartitioned = true, tablePartitionSpec = Some(threeHourSpec))

    // floor(09:17) = 09:00; last complete interval before floor(14:05) = 12:00 is 09:00
    assertEquals(List("2024-01-01-09-00"), partitions)

    val offsetSpec =
      PartitionSpec("created_at", "yyyy-MM-dd-HH-mm", 3 * 60 * 60 * 1000, offsetMillis = 60 * 60 * 1000)
    val offsetPartitions =
      tableUtils.partitions(tableName, timePartitioned = true, tablePartitionSpec = Some(offsetSpec))

    // grid is 01:00, 04:00, 07:00, 10:00, 13:00, ...: floor(09:17) = 07:00 and the last
    // complete interval before floor(14:05) = 13:00 is 10:00
    assertEquals(List("2024-01-01-07-00", "2024-01-01-10-00"), offsetPartitions)

    spark.sql(s"DROP TABLE IF EXISTS $tableName")
    spark.sql(s"DROP DATABASE IF EXISTS $dbName")
  }

  it should "preserve string sub-daily partition values through scanDf" in {
    val dbName = s"db_${System.nanoTime()}"
    val tableName = s"$dbName.scan_subdaily_string_ds"
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $dbName")

    val threeHourSpec = PartitionSpec("ds", "yyyy-MM-dd-HH-mm", 3 * 60 * 60 * 1000)
    val subDailyTableUtils = TableUtils(spark, threeHourSpec)

    spark.sql(s"CREATE TABLE $tableName (user_id STRING, ds STRING) PARTITIONED BY (ds)")
    spark.sql(s"INSERT INTO $tableName VALUES ('user1', '2024-01-01-09-00'), ('user2', '2024-01-01-12-00')")

    val range = PartitionRange("2024-01-01-09-00", "2024-01-01-12-00")(threeHourSpec)
    val result = subDailyTableUtils.scanDf(null, tableName, range = Some(range))

    // regression: scanDfBase used to date_format string ds values through an implicit
    // string->timestamp cast, which nulled any value Spark can't natively cast (the dash
    // sub-daily formats; the legacy space/colon format survived only by coincidence)
    val dsValues = result.select("ds").collect().map(_.getString(0)).sorted.toList
    assertEquals(List("2024-01-01-09-00", "2024-01-01-12-00"), dsValues)

    spark.sql(s"DROP TABLE IF EXISTS $tableName")
    spark.sql(s"DROP DATABASE IF EXISTS $dbName")
  }

  it should "scan timestamp partition columns with epoch-typed bounds and grid ds values" in {
    val dbName = s"db_${System.nanoTime()}"
    val tableName = s"$dbName.scan_subdaily_ts_ds"
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $dbName")

    val threeHourSpec = PartitionSpec("ds", "yyyy-MM-dd-HH-mm", 3 * 60 * 60 * 1000)
    val subDailyTableUtils = TableUtils(spark, threeHourSpec)

    // a time_partitioned table: ds is a real timestamp, no physical grid
    spark.sql(s"CREATE TABLE $tableName (user_id STRING, ds TIMESTAMP)")
    spark.sql(s"""
      INSERT INTO $tableName VALUES
        ('below', TIMESTAMP '2024-01-01 08:59:00'),
        ('in1',   TIMESTAMP '2024-01-01 09:17:00'),
        ('in2',   TIMESTAMP '2024-01-01 12:05:00'),
        ('above', TIMESTAMP '2024-01-01 15:01:00')
    """)

    // ds [09:00, 12:00] on a 3h grid = time interval [09:00, 15:00). Regression: ds
    // literals like '2024-01-01-09-00' cast to NULL against a timestamp column, so the scan
    // was silently empty; epoch-typed bounds make it correct and rerun-deterministic.
    val range = PartitionRange("2024-01-01-09-00", "2024-01-01-12-00")(threeHourSpec)
    val result = subDailyTableUtils.scanDf(null, tableName, range = Some(range))

    val rows = result.select("user_id", "ds").collect().map(r => (r.getString(0), r.getString(1))).sortBy(_._1)
    // off-boundary timestamps render as grid ds values, not per-minute ones
    assertEquals(List(("in1", "2024-01-01-09-00"), ("in2", "2024-01-01-12-00")), rows.toList)

    spark.sql(s"DROP TABLE IF EXISTS $tableName")
    spark.sql(s"DROP DATABASE IF EXISTS $dbName")
  }

  it should "return empty list for virtual partitions on empty table" in {
    val dbName = s"db_${System.nanoTime()}"
    val tableName = s"$dbName.empty_time_partitioned"
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $dbName")
    spark.sql(s"CREATE TABLE $tableName (user_id STRING, created_at TIMESTAMP)")

    val partitions = tableUtils.partitions(tableName, timePartitioned = true,
      tablePartitionSpec = Some(PartitionSpec("created_at", "yyyy-MM-dd", 24 * 60 * 60 * 1000)))

    assertEquals(List.empty, partitions)

    spark.sql(s"DROP TABLE IF EXISTS $tableName")
    spark.sql(s"DROP DATABASE IF EXISTS $dbName")
  }

  it should "scanDf with timePartitioned query derives virtual ds column" in {
    val dbName = s"db_${System.nanoTime()}"
    val tableName = s"$dbName.scan_time_partitioned"
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $dbName")

    import spark.implicits._
    val data = Seq(
      ("user1", 100, java.sql.Timestamp.valueOf("2024-01-01 12:00:00")),
      ("user2", 200, java.sql.Timestamp.valueOf("2024-01-01 12:30:00")),
      ("user3", 300, java.sql.Timestamp.valueOf("2024-01-02 12:00:00")),
      ("user1", 400, java.sql.Timestamp.valueOf("2024-01-03 12:00:00"))
    ).toDF("user_id", "value", "created_at")

    data.write.saveAsTable(tableName)

    val query = Builders.Query(
      partitionColumn = "created_at",
      timeColumn = "UNIX_TIMESTAMP(created_at) * 1000"
    )
    query.setTimePartitioned(true)

    val range = PartitionRange("2024-01-01", "2024-01-02")
    val result = tableUtils.scanDf(query, tableName, range = Some(range))

    // Should have a "ds" column with formatted date strings
    assertTrue(result.schema.fieldNames.contains("ds"))

    val dsValues = result.select("ds").distinct().collect().map(_.getString(0)).sorted.toList
    assertEquals(List("2024-01-01", "2024-01-02"), dsValues)

    // Should only contain rows within the range (2024-01-01 and 2024-01-02)
    assertEquals(3, result.count())

    spark.sql(s"DROP TABLE IF EXISTS $tableName")
    spark.sql(s"DROP DATABASE IF EXISTS $dbName")
  }

  it should "derive existing partitions from a value scan for unpartitioned (clustered-style) tables" in {
    // an unpartitioned table with a string ds column: no catalog partitions exist, so the data
    // must come from the MAX(ds) scan fallback - this is the join-part reuse path for
    // clustered warehouse tables
    import spark.implicits._
    val tableName = "db.unpartitioned_scan_table"
    spark.sql("CREATE DATABASE IF NOT EXISTS db")
    spark.sql(s"DROP TABLE IF EXISTS $tableName")
    Seq(("2024-01-01", "a"), ("2024-01-02", "b"), ("2024-01-03", "c"))
      .toDF("ds", "id")
      .write
      .saveAsTable(tableName)

    // logical partitions come from the distinct values of the ds column, so compute
    // planning (unfilledRanges, step runners) sees existing data instead of recomputing
    assertEquals(List("2024-01-01", "2024-01-02", "2024-01-03"), tableUtils.partitions(tableName).sorted)

    val watermark = tableUtils.dataWatermarkMillis(tableName)
    assertTrue(watermark.isDefined)
    // last partition 2024-01-03 covers through 2024-01-04 00:00
    assertEquals(PartitionSpec.daily.epochMillis("2024-01-04"), watermark.get)

    assertTrue(tableUtils.tableCoversRange(tableName, PartitionRange("2024-01-02", "2024-01-03")))
    assertFalse(tableUtils.tableCoversRange(tableName, PartitionRange("2024-01-02", "2024-01-04")))

    spark.sql(s"DROP TABLE IF EXISTS $tableName")
  }

  it should "compute watermarks in epoch millis for sub-daily partition specs" in {
    // dash-separated format: the Hive catalog percent-escapes colons in partition values
    // ('04:00' lists as '04%3A00'), which is exactly the format-hygiene warning on
    // PartitionSpec construction
    val subDailySpec = PartitionSpec("ds", "yyyy-MM-dd-HH-mm", 3 * 60 * 60 * 1000L, 60 * 60 * 1000L)
    import spark.implicits._
    val tableName = "db.sub_daily_watermark_table"
    spark.sql("CREATE DATABASE IF NOT EXISTS db")
    spark.sql(s"DROP TABLE IF EXISTS $tableName")
    Seq(("2024-01-05-01-00", "a"), ("2024-01-05-04-00", "b"))
      .toDF("ds", "id")
      .write
      .partitionBy("ds")
      .saveAsTable(tableName)

    val watermark = tableUtils.dataWatermarkMillis(tableName, Some(subDailySpec))
    // last partition 04:00 covers through 07:00
    assertEquals(subDailySpec.epochMillis("2024-01-05-07-00"), watermark.get)

    assertTrue(
      tableUtils.tableCoversRange(tableName,
                                  PartitionRange("2024-01-05-04-00", "2024-01-05-04-00")(subDailySpec),
                                  Some(subDailySpec)))
    assertFalse(
      tableUtils.tableCoversRange(tableName,
                                  PartitionRange("2024-01-05-07-00", "2024-01-05-07-00")(subDailySpec),
                                  Some(subDailySpec)))

    spark.sql(s"DROP TABLE IF EXISTS $tableName")
  }

}
