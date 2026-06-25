package ai.chronon.spark.catalog

import ai.chronon.api.PartitionSpec
import ai.chronon.spark.utils.SparkTestBase
import org.apache.spark.sql.SparkSession
import org.junit.Assert.assertEquals
import org.scalatest.matchers.should.Matchers._

class FormatTest extends SparkTestBase {

  override def sparkConfs: Map[String, String] = Map(
    "spark.sql.catalog.spark_non_default_catalog" -> "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.spark_non_default_catalog.type" -> "hadoop",
    "spark.sql.catalog.spark_non_default_catalog.warehouse" -> icebergWarehouse
  )

  // --- resolvePartitionColumn hook ---

  // Regression: formats like Snowflake discover the actual partition column from table metadata
  // (e.g. a clustering key named ITEM_RECEIVED_DATE) and key their partitions() result maps by
  // that column. The base primaryPartitions used the caller-provided column (e.g. "ds" from a
  // dependency's TableInfo) for the map lookup, so the two would diverge and results would be empty.
  it should "return empty when partitions() keys by a different column than the caller provides and no hook is overridden" in {
    val fmt = new Format {
      override def supportSubPartitionsFilter = false
      // simulates a format that keys by the storage column name, ignoring the caller's partitionColumn
      override def partitions(tableName: String, partitionFilters: String)(implicit ss: SparkSession) =
        List(Map("ITEM_RECEIVED_DATE" -> "2023-01-01"), Map("ITEM_RECEIVED_DATE" -> "2023-01-02"))
    }
    fmt.primaryPartitions("db.table", "ds", "")(spark) shouldBe empty
  }

  it should "return results when resolvePartitionColumn remaps the caller column to the stored key" in {
    val fmt = new Format {
      override def supportSubPartitionsFilter = false
      override def partitions(tableName: String, partitionFilters: String)(implicit ss: SparkSession) =
        List(Map("ITEM_RECEIVED_DATE" -> "2023-01-01"), Map("ITEM_RECEIVED_DATE" -> "2023-01-02"))
      // simulates resolving "ds" → "ITEM_RECEIVED_DATE" via table metadata lookup
      override protected def resolvePartitionColumn(tableName: String, partitionColumn: String)(implicit
          ss: SparkSession) =
        "ITEM_RECEIVED_DATE"
    }
    fmt.primaryPartitions("db.table", "ds", "")(spark) shouldBe List("2023-01-01", "2023-01-02")
  }

  it should "apply subPartitionsFilter correctly when using a resolved partition column" in {
    val fmt = new Format {
      override def supportSubPartitionsFilter = true
      override def partitions(tableName: String, partitionFilters: String)(implicit ss: SparkSession) =
        List(
          Map("ITEM_RECEIVED_DATE" -> "2023-01-01", "hr" -> "12"),
          Map("ITEM_RECEIVED_DATE" -> "2023-01-01", "hr" -> "13"),
          Map("ITEM_RECEIVED_DATE" -> "2023-01-02", "hr" -> "12")
        )
      override protected def resolvePartitionColumn(tableName: String, partitionColumn: String)(implicit
          ss: SparkSession) =
        "ITEM_RECEIVED_DATE"
    }
    fmt.primaryPartitions("db.table", "ds", "", subPartitionsFilter = Map("hr" -> "12"))(spark) shouldBe
      List("2023-01-01", "2023-01-02")
  }

  it should "fall back to a table scan when metadata partitions are null" in {
    val tableName = "format_null_metadata_fallback_test"
    spark.sql(s"""
      CREATE OR REPLACE TEMP VIEW $tableName AS
      SELECT * FROM VALUES
        (1, '2024-04-01'),
        (2, '2024-04-03'),
        (3, '2024-04-02')
      AS t(id, ds)
    """)

    val fmt = new Format {
      override def supportSubPartitionsFilter = false
      override def partitions(tableName: String, partitionFilters: String)(implicit ss: SparkSession) = Nil
      override def primaryPartitions(tableName: String,
                                     partitionColumn: String,
                                     partitionFilters: String,
                                     subPartitionsFilter: Map[String, String])(implicit
          ss: SparkSession) = List(null)
    }

    fmt.firstAvailablePartition(tableName, "ds", PartitionSpec.daily)(spark) shouldBe Some("2024-04-01")
    fmt.lastAvailablePartition(tableName, "ds", PartitionSpec.daily)(spark) shouldBe Some("2024-04-03")
  }

  it should "compute metadata boundaries by ignoring null partition entries" in {
    val tableName = "format_null_metadata_boundary_test"
    spark.sql(s"""
      CREATE OR REPLACE TEMP VIEW $tableName AS
      SELECT * FROM VALUES
        (1, '2024-04-01'),
        (2, '2024-04-03'),
        (3, '2024-04-02')
      AS t(id, ds)
    """)

    val fmt = new Format {
      override def supportSubPartitionsFilter = false
      override def partitions(tableName: String, partitionFilters: String)(implicit ss: SparkSession) = Nil
      override def primaryPartitions(tableName: String,
                                     partitionColumn: String,
                                     partitionFilters: String,
                                     subPartitionsFilter: Map[String, String])(implicit
          ss: SparkSession) = List("2024-04-02", null, "2024-04-01")
    }

    Format.sanitizePartitionValues(List("2024-04-02", null, "2024-04-01")) shouldBe List(
      "2024-04-02",
      "2024-04-01")
    Format.pickMinPartition(List("2024-04-02", null, "2024-04-01")) shouldBe Some("2024-04-01")
    Format.pickMaxPartition(List("2024-04-02", null, "2024-04-01")) shouldBe Some("2024-04-02")
    fmt.firstAvailablePartition(tableName, "ds", PartitionSpec.daily)(spark) shouldBe Some("2024-04-01")
    fmt.lastAvailablePartition(tableName, "ds", PartitionSpec.daily)(spark) shouldBe Some("2024-04-02")
  }

  it should "deduplicate sanitized metadata partitions while preserving discovery order" in {
    val fmt = new Format {
      override def supportSubPartitionsFilter = false
      override def partitions(tableName: String, partitionFilters: String)(implicit ss: SparkSession) = Nil
      override def primaryPartitions(tableName: String,
                                     partitionColumn: String,
                                     partitionFilters: String,
                                     subPartitionsFilter: Map[String, String])(implicit
          ss: SparkSession) = List("2024-04-02", null, "2024-04-02", "2024-04-01")

      def discoveredPartitions(tableName: String, partitionColumn: String)(implicit
          ss: SparkSession): Option[List[String]] =
        metadataPartitions(tableName, partitionColumn)(ss)
    }

    fmt.discoveredPartitions("db.table", "ds")(spark) shouldBe Some(List("2024-04-02", "2024-04-01"))
  }

  it should "return the last complete partition when scanning a timestamp column" in {
    val tableName = "format_timestamp_scan_last_available_test"
    spark.sql(s"""
      CREATE OR REPLACE TEMP VIEW $tableName AS
      SELECT * FROM VALUES
        (1, TIMESTAMP '2024-04-01 12:00:00'),
        (2, TIMESTAMP '2024-04-03 12:00:00')
      AS t(id, created_at)
    """)

    val fmt = new Format {
      override def supportSubPartitionsFilter = false
      override def partitions(tableName: String, partitionFilters: String)(implicit ss: SparkSession) = Nil
      override def primaryPartitions(tableName: String,
                                     partitionColumn: String,
                                     partitionFilters: String,
                                     subPartitionsFilter: Map[String, String])(implicit
          ss: SparkSession) = Nil
    }

    fmt.lastAvailablePartition(tableName, "created_at", PartitionSpec.daily)(spark) shouldBe Some("2024-04-02")
  }

  it should "return the max date when scanning a date column" in {
    val tableName = "format_date_scan_last_available_test"
    spark.sql(s"""
      CREATE OR REPLACE TEMP VIEW $tableName AS
      SELECT * FROM VALUES
        (1, DATE '2024-04-01'),
        (2, DATE '2024-04-03')
      AS t(id, ds)
    """)

    val fmt = new Format {
      override def supportSubPartitionsFilter = false
      override def partitions(tableName: String, partitionFilters: String)(implicit ss: SparkSession) = Nil
      override def primaryPartitions(tableName: String,
                                     partitionColumn: String,
                                     partitionFilters: String,
                                     subPartitionsFilter: Map[String, String])(implicit
          ss: SparkSession) = Nil
    }

    fmt.lastAvailablePartition(tableName, "ds", PartitionSpec.daily)(spark) shouldBe Some("2024-04-03")
    fmt.virtualPartitions(tableName, "ds", PartitionSpec.daily)(spark) shouldBe
      List("2024-04-01", "2024-04-02", "2024-04-03")
  }

  it should "resolve table names consistently with Spark SQL" in {
    spark.sql("CREATE DATABASE IF NOT EXISTS spark_non_default_catalog.custom_test_db")
    spark.sql(
      "CREATE TABLE spark_non_default_catalog.custom_test_db.resolve_test_table (id INT, ds STRING) USING iceberg PARTITIONED BY (ds)")
    try {
      val tableName = "spark_non_default_catalog.custom_test_db.resolve_test_table"
      val resolved = Format.resolveTableName(tableName)
      val sparkTable = spark.catalog.getTable(tableName)
      assertEquals(sparkTable.catalog, resolved.catalog)
      assertEquals(sparkTable.database, resolved.namespace)
      assertEquals(sparkTable.name, resolved.table)
    } finally {
      spark.sql("DROP TABLE IF EXISTS spark_non_default_catalog.custom_test_db.resolve_test_table")
      spark.sql("DROP DATABASE IF EXISTS spark_non_default_catalog.custom_test_db")
    }
  }

  it should "resolve fully qualified table names" in {
    assertEquals(ResolvedTableName("catalogA", "ns", "tbl"), Format.resolveTableName("catalogA.ns.tbl"))
    assertEquals(ResolvedTableName("catalogA", "ns", "tbl"), Format.resolveTableName("`catalogA`.ns.tbl"))
  }

  it should "resolve two-part table names with default catalog" in {
    assertEquals(ResolvedTableName("spark_catalog", "mydb", "tbl"), Format.resolveTableName("mydb.tbl"))
  }

  it should "resolve single-part table names with default catalog and current database" in {
    val resolved = Format.resolveTableName("tbl")
    assertEquals("spark_catalog", resolved.catalog)
    assertEquals(spark.catalog.currentDatabase, resolved.namespace)
    assertEquals("tbl", resolved.table)
  }

  it should "quote resolved table names" in {
    Format.resolveTableName("`my-catalog`.`my-ns`.`my-table`").quoted shouldBe "`my-catalog`.`my-ns`.`my-table`"
    Format.resolveTableName("`a``b`.`c``d`").quoted shouldBe "`spark_catalog`.`a``b`.`c``d`"
  }

  it should "strip destination catalog in rename SQL when source and destination share a catalog" in {
    val sql = Format.renameTableSql(
      "spark_non_default_catalog.custom_test_db.src_table",
      "spark_non_default_catalog.custom_test_db.dest_table"
    )
    assertEquals(
      "ALTER TABLE spark_non_default_catalog.custom_test_db.src_table RENAME TO `custom_test_db`.`dest_table`",
      sql)
  }

  it should "keep destination catalog in rename SQL when source and destination catalogs differ" in {
    val sql = Format.renameTableSql(
      "spark_non_default_catalog.custom_test_db.src_table",
      "spark_catalog.default.dest_table"
    )
    assertEquals(
      "ALTER TABLE spark_non_default_catalog.custom_test_db.src_table RENAME TO `spark_catalog`.`default`.`dest_table`",
      sql)
  }

  // --- parseIdentifier ---

  it should "parse a 3-part unquoted identifier" in {
    Format.parseIdentifier("catalog.schema.table") shouldBe Seq("catalog", "schema", "table")
  }

  it should "parse a 2-part identifier" in {
    Format.parseIdentifier("schema.table") shouldBe Seq("schema", "table")
  }

  it should "parse a single-segment identifier" in {
    Format.parseIdentifier("table") shouldBe Seq("table")
  }

  it should "strip surrounding backticks from quoted segments" in {
    Format.parseIdentifier("workspace.`schema-with-dashes`.events") shouldBe
      Seq("workspace", "schema-with-dashes", "events")
  }

  it should "preserve a literal dot inside a backticked segment" in {
    Format.parseIdentifier("catalog.`schema.with.dots`.table") shouldBe
      Seq("catalog", "schema.with.dots", "table")
  }

  it should "handle every segment quoted" in {
    Format.parseIdentifier("`c`.`s`.`t`") shouldBe Seq("c", "s", "t")
  }

  // --- zero-parsed-partitions canary ---

  it should "warn when a nonempty listing parses zero partitions under the spec" in {
    val threeHourly = PartitionSpec("ds", "yyyy-MM-dd-HH-mm", 3 * 60 * 60 * 1000)
    // compact ds values, or values missing their minute level: nothing parses, the table reads as
    // permanently empty
    Format.zeroParsedPartitionsWarning("db.t", List("20260603-04", "20260603-05"), threeHourly) shouldBe defined
    Format.zeroParsedPartitionsWarning("db.t", List("2026-06-03-03"), threeHourly) shouldBe defined
  }

  it should "stay quiet for empty listings or when any partition parses" in {
    val threeHourly = PartitionSpec("ds", "yyyy-MM-dd-HH-mm", 3 * 60 * 60 * 1000)
    Format.zeroParsedPartitionsWarning("db.t", List.empty, threeHourly) shouldBe empty
    Format.zeroParsedPartitionsWarning("db.t", List("2026-06-03-03-00"), threeHourly) shouldBe empty
    // one parseable ds among garbage means the table is usable; per-value failures
    // surface later as loud ParseExceptions instead
    Format.zeroParsedPartitionsWarning("db.t", List("garbage", "2026-06-03-03-00"), threeHourly) shouldBe empty
  }

}
