package ai.chronon.spark.catalog

import ai.chronon.api.PartitionSpec
import ai.chronon.spark.utils.SparkTestBase
import org.scalatest.matchers.should.Matchers

class IcebergTest extends SparkTestBase with Matchers {

  override def sparkConfs: Map[String, String] = Map(
    "spark.serializer" -> "org.apache.spark.serializer.JavaSerializer",
    "spark.sql.extensions" -> (
      "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions," +
      "ai.chronon.spark.extensions.ChrononMergeExtension"
    )
  )

  "Iceberg.tableProperties" should "contain expected commit retry and format properties" in {
    val props = Iceberg.tableProperties
    props("commit.retry.num-retries") shouldBe "20"
    props("commit.retry.min-wait-ms") shouldBe "10000"
    props("commit.retry.max-wait-ms") shouldBe "600000"
    props("commit.status-check.num-retries") shouldBe "20"
    props("commit.status-check.min-wait-ms") shouldBe "10000"
    props("commit.status-check.max-wait-ms") shouldBe "600000"
    props("write.merge.isolation-level") shouldBe "snapshot"
    props("format-version") shouldBe "2"
  }

  "Iceberg.supportSubPartitionsFilter" should "return false" in {
    Iceberg.supportSubPartitionsFilter shouldBe false
  }

  "Iceberg.partitions" should "return partition values for an Iceberg table" in {
    val tableName = "default.iceberg_partitions_test"

    spark.sql(s"""
      CREATE TABLE IF NOT EXISTS $tableName (
        id INT,
        name STRING,
        ds STRING
      ) USING iceberg
      PARTITIONED BY (ds)
    """)

    spark.sql(s"""
      INSERT INTO $tableName VALUES
      (1, 'a', '2024-01-01'),
      (2, 'b', '2024-01-02'),
      (3, 'c', '2024-01-01')
    """)

    val parts = Iceberg.partitions(tableName, "")
    parts should contain theSameElementsAs List(
      Map("ds" -> "2024-01-01"),
      Map("ds" -> "2024-01-02")
    )

    Iceberg.partitions("`default`.`iceberg_partitions_test`", "") should contain theSameElementsAs parts
    Iceberg.partitions("`spark_catalog`.`default`.`iceberg_partitions_test`", "") should contain theSameElementsAs parts
  }

  "Iceberg.primaryPartitions" should "return primary partition values" in {
    val tableName = "default.iceberg_primary_partitions_test"

    spark.sql(s"""
      CREATE TABLE IF NOT EXISTS $tableName (
        id INT,
        value DOUBLE,
        ds STRING
      ) USING iceberg
      PARTITIONED BY (ds)
    """)

    spark.sql(s"""
      INSERT INTO $tableName VALUES
      (1, 1.0, '2024-02-01'),
      (2, 2.0, '2024-02-02'),
      (3, 3.0, '2024-02-03')
    """)

    val parts = Iceberg.primaryPartitions(tableName, "ds", "")
    parts should contain theSameElementsAs List("2024-02-01", "2024-02-02", "2024-02-03")
  }

  it should "throw NotImplementedError when subPartitionsFilter is non-empty" in {
    a[NotImplementedError] should be thrownBy {
      Iceberg.primaryPartitions("any_table", "ds", "", subPartitionsFilter = Map("hr" -> "12"))
    }
  }

  it should "cast primary partition values before filtering null hour partitions" in {
    val tableName = "default.iceberg_date_with_hr_partition_test"
    spark.sql(s"DROP TABLE IF EXISTS $tableName")

    spark.sql(s"""
      CREATE TABLE $tableName (
        id INT,
        ds DATE,
        hr STRING
      ) USING iceberg
      PARTITIONED BY (ds, hr)
    """)

    spark.sql(s"""
      INSERT INTO $tableName VALUES
      (1, DATE '2024-03-01', NULL),
      (2, DATE '2024-03-02', '12')
    """)

    Iceberg.primaryPartitions(tableName, "ds", "") shouldBe List("2024-03-01")
  }

  it should "work with a single-part table name" in {
    val tableName = "iceberg_single_part_name_test"

    spark.sql(s"""
      CREATE TABLE IF NOT EXISTS $tableName (
        id INT,
        ds STRING
      ) USING iceberg
      PARTITIONED BY (ds)
    """)

    spark.sql(s"""INSERT INTO $tableName VALUES (1, '2024-03-01')""")

    val parts = Iceberg.primaryPartitions(tableName, "ds", "")
    parts shouldBe List("2024-03-01")
  }

  it should "derive virtual partitions from Iceberg file stats for a clustered timestamp column" in {
    val tableName = "default.iceberg_time_stats_test"
    spark.sql(s"DROP TABLE IF EXISTS $tableName")

    spark.sql(s"""
      CREATE TABLE $tableName (
        id INT,
        created_at TIMESTAMP
      ) USING iceberg
      TBLPROPERTIES (
        'write.metadata.metrics.default' = 'full',
        'write.metadata.metrics.column.created_at' = 'full'
      )
    """)
    spark.sql(s"ALTER TABLE $tableName WRITE ORDERED BY created_at")

    spark.sql(s"""
      INSERT INTO $tableName VALUES
      (1, TIMESTAMP '2024-04-01 12:00:00'),
      (2, TIMESTAMP '2024-04-03 12:00:00')
    """)

    Iceberg.statsDateRange(tableName, "created_at", PartitionSpec.daily) shouldBe
      Some(StatsDateRange(start = "2024-04-01", end = "2024-04-03"))
    Iceberg.virtualPartitions(tableName, "created_at", PartitionSpec.daily) shouldBe
      List("2024-04-01", "2024-04-02")
    Iceberg.firstAvailablePartition(tableName, "created_at", PartitionSpec.daily) shouldBe Some("2024-04-01")
    Iceberg.lastAvailablePartition(tableName, "created_at", PartitionSpec.daily) shouldBe Some("2024-04-02")
  }

  it should "return the inclusive last partition from Iceberg file stats for a single-day timestamp range" in {
    val range = StatsDateRange(start = "2024-04-01", end = "2024-04-01")

    range.virtualPartitions(PartitionSpec.daily) shouldBe List("2024-04-01")
    range.firstAvailablePartition shouldBe "2024-04-01"
    range.lastAvailablePartition shouldBe "2024-04-01"
  }

  it should "prefer actual partition metadata over Iceberg file stats" in {
    val tableName = "default.iceberg_partition_metadata_test"
    spark.sql(s"DROP TABLE IF EXISTS $tableName")

    try {
      spark.sql(s"""
        CREATE TABLE $tableName (
          id INT,
          created_at TIMESTAMP,
          ds STRING
        ) USING iceberg
        PARTITIONED BY (ds)
        TBLPROPERTIES (
          'write.metadata.metrics.default' = 'full',
          'write.metadata.metrics.column.ds' = 'full'
        )
      """)

      spark.sql(s"""
        INSERT INTO $tableName VALUES
        (1, TIMESTAMP '2024-06-01 12:00:00', '2024-06-01'),
        (2, TIMESTAMP '2024-06-03 12:00:00', '2024-06-03')
      """)

      Iceberg.virtualPartitions(tableName, "ds", PartitionSpec.daily) should contain theSameElementsAs
        List("2024-06-01", "2024-06-03")
      Iceberg.firstAvailablePartition(tableName, "ds", PartitionSpec.daily) shouldBe Some("2024-06-01")
      Iceberg.lastAvailablePartition(tableName, "ds", PartitionSpec.daily) shouldBe Some("2024-06-03")
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
  }

  it should "fall back to scanning when Iceberg file stats do not cover the timestamp column" in {
    val tableName = "default.iceberg_time_missing_stats_test"
    spark.sql(s"DROP TABLE IF EXISTS $tableName")

    spark.sql(s"""
      CREATE TABLE $tableName (
        id INT,
        created_at TIMESTAMP
      ) USING iceberg
      TBLPROPERTIES (
        'write.metadata.metrics.default' = 'none'
      )
    """)

    spark.sql(s"""
      INSERT INTO $tableName VALUES
      (1, TIMESTAMP '2024-05-01 12:00:00'),
      (2, TIMESTAMP '2024-05-03 12:00:00')
    """)

    Iceberg.statsDateRange(tableName, "created_at", PartitionSpec.daily) shouldBe None
    Iceberg.virtualPartitions(tableName, "created_at", PartitionSpec.daily) shouldBe
      List("2024-05-01", "2024-05-02")
    Iceberg.firstAvailablePartition(tableName, "created_at", PartitionSpec.daily) shouldBe Some("2024-05-01")
    Iceberg.lastAvailablePartition(tableName, "created_at", PartitionSpec.daily) shouldBe Some("2024-05-02")
  }

  "insertPartitions on unpartitioned Iceberg table" should "overwrite only the subrange without duplicates" in {
    val tableName = "default.iceberg_unpartitioned_overwrite_test"
    val tableUtils = TableUtils(spark)

    // Create an unpartitioned Iceberg table (ds is a regular column, not a partition key)
    // format-version 2 + merge-on-read enables row-level deletes within data files
    spark.sql(s"""
      CREATE TABLE IF NOT EXISTS $tableName (
        id INT,
        value STRING,
        ds STRING
      ) USING iceberg
      TBLPROPERTIES (
        'format-version' = '2',
        'write.delete.mode' = 'merge-on-read',
        'write.update.mode' = 'merge-on-read'
      )
    """)

    // First write: ds range 2026-04-01 to 2026-04-08
    import spark.implicits._
    val firstWrite = (1 to 8).map { day =>
      val ds = f"2026-04-${day}%02d"
      (day, s"first_$day", ds)
    }.toDF("id", "value", "ds")

    tableUtils.insertPartitions(firstWrite, tableName)

    // Verify first write
    val afterFirst = spark.table(tableName).collect()
    afterFirst.length shouldBe 8

    // Second write: overwrite only subrange 2026-04-03 to 2026-04-05 with new data
    val secondWrite = (3 to 5).map { day =>
      val ds = f"2026-04-${day}%02d"
      (day + 100, s"second_$day", ds)
    }.toDF("id", "value", "ds")

    tableUtils.insertPartitions(secondWrite, tableName)

    // Verify: subrange should have ONLY second write data (no duplicates)
    val result = spark.table(tableName).orderBy("ds", "id").collect()
    result.length shouldBe 8 // 5 untouched from first write + 3 replaced from second write

    // Days 01, 02 should have first write data
    val day01 = result.filter(_.getAs[String]("ds") == "2026-04-01")
    day01.length shouldBe 1
    day01.head.getAs[String]("value") shouldBe "first_1"

    val day02 = result.filter(_.getAs[String]("ds") == "2026-04-02")
    day02.length shouldBe 1
    day02.head.getAs[String]("value") shouldBe "first_2"

    // Days 03-05 should have ONLY second write data (replaced, not duplicated)
    for (day <- 3 to 5) {
      val ds = f"2026-04-${day}%02d"
      val rows = result.filter(_.getAs[String]("ds") == ds)
      rows.length shouldBe 1
      rows.head.getAs[Int]("id") shouldBe day + 100
      rows.head.getAs[String]("value") shouldBe s"second_$day"
    }

    // Days 06-08 should have first write data (untouched)
    for (day <- 6 to 8) {
      val ds = f"2026-04-${day}%02d"
      val rows = result.filter(_.getAs[String]("ds") == ds)
      rows.length shouldBe 1
      rows.head.getAs[String]("value") shouldBe s"first_$day"
    }
  }

  it should "handle partial overlap where incoming range extends beyond existing" in {
    val tableName = "default.iceberg_unpartitioned_partial_overlap_test"
    val tableUtils = TableUtils(spark)

    spark.sql(s"""
      CREATE TABLE IF NOT EXISTS $tableName (
        id INT,
        value STRING,
        ds STRING
      ) USING iceberg
      TBLPROPERTIES (
        'format-version' = '2',
        'write.delete.mode' = 'merge-on-read',
        'write.update.mode' = 'merge-on-read'
      )
    """)

    import spark.implicits._

    // First write: ds 2026-04-01 to 2026-04-05
    val firstWrite = (1 to 5).map { day =>
      val ds = f"2026-04-${day}%02d"
      (day, s"first_$day", ds)
    }.toDF("id", "value", "ds")
    tableUtils.insertPartitions(firstWrite, tableName)

    // Second write: ds 2026-04-03 to 2026-04-08 (overlaps 03-05, extends with 06-08)
    val secondWrite = (3 to 8).map { day =>
      val ds = f"2026-04-${day}%02d"
      (day + 100, s"second_$day", ds)
    }.toDF("id", "value", "ds")
    tableUtils.insertPartitions(secondWrite, tableName)

    val result = spark.table(tableName).orderBy("ds").collect()
    result.length shouldBe 8 // 2 untouched + 3 replaced + 3 new

    // Days 01-02: untouched first write
    for (day <- 1 to 2) {
      val ds = f"2026-04-${day}%02d"
      val rows = result.filter(_.getAs[String]("ds") == ds)
      rows.length shouldBe 1
      rows.head.getAs[String]("value") shouldBe s"first_$day"
    }

    // Days 03-05: replaced by second write (no duplicates)
    for (day <- 3 to 5) {
      val ds = f"2026-04-${day}%02d"
      val rows = result.filter(_.getAs[String]("ds") == ds)
      rows.length shouldBe 1
      rows.head.getAs[Int]("id") shouldBe day + 100
      rows.head.getAs[String]("value") shouldBe s"second_$day"
    }

    // Days 06-08: new from second write
    for (day <- 6 to 8) {
      val ds = f"2026-04-${day}%02d"
      val rows = result.filter(_.getAs[String]("ds") == ds)
      rows.length shouldBe 1
      rows.head.getAs[Int]("id") shouldBe day + 100
      rows.head.getAs[String]("value") shouldBe s"second_$day"
    }
  }

  it should "handle writing an entirely new non-overlapping range" in {
    val tableName = "default.iceberg_unpartitioned_no_overlap_test"
    val tableUtils = TableUtils(spark)

    spark.sql(s"""
      CREATE TABLE IF NOT EXISTS $tableName (
        id INT,
        value STRING,
        ds STRING
      ) USING iceberg
      TBLPROPERTIES (
        'format-version' = '2',
        'write.delete.mode' = 'merge-on-read',
        'write.update.mode' = 'merge-on-read'
      )
    """)

    import spark.implicits._

    // First write: ds 2026-04-01 to 2026-04-03
    val firstWrite = (1 to 3).map { day =>
      val ds = f"2026-04-${day}%02d"
      (day, s"first_$day", ds)
    }.toDF("id", "value", "ds")
    tableUtils.insertPartitions(firstWrite, tableName)

    // Second write: ds 2026-04-06 to 2026-04-08 (no overlap at all)
    val secondWrite = (6 to 8).map { day =>
      val ds = f"2026-04-${day}%02d"
      (day + 100, s"second_$day", ds)
    }.toDF("id", "value", "ds")
    tableUtils.insertPartitions(secondWrite, tableName)

    val result = spark.table(tableName).orderBy("ds").collect()
    result.length shouldBe 6 // 3 from first + 3 from second, no overlap

    // First write untouched
    for (day <- 1 to 3) {
      val ds = f"2026-04-${day}%02d"
      val rows = result.filter(_.getAs[String]("ds") == ds)
      rows.length shouldBe 1
      rows.head.getAs[String]("value") shouldBe s"first_$day"
    }

    // Second write appended
    for (day <- 6 to 8) {
      val ds = f"2026-04-${day}%02d"
      val rows = result.filter(_.getAs[String]("ds") == ds)
      rows.length shouldBe 1
      rows.head.getAs[String]("value") shouldBe s"second_$day"
    }
  }

  it should "handle full overlap rewrite of exact same range" in {
    val tableName = "default.iceberg_unpartitioned_full_overlap_test"
    val tableUtils = TableUtils(spark)

    spark.sql(s"""
      CREATE TABLE IF NOT EXISTS $tableName (
        id INT,
        value STRING,
        ds STRING
      ) USING iceberg
      TBLPROPERTIES (
        'format-version' = '2',
        'write.delete.mode' = 'merge-on-read',
        'write.update.mode' = 'merge-on-read'
      )
    """)

    import spark.implicits._

    // First write: ds 2026-04-01 to 2026-04-05
    val firstWrite = (1 to 5).map { day =>
      val ds = f"2026-04-${day}%02d"
      (day, s"first_$day", ds)
    }.toDF("id", "value", "ds")
    tableUtils.insertPartitions(firstWrite, tableName)

    // Second write: exact same range with new data
    val secondWrite = (1 to 5).map { day =>
      val ds = f"2026-04-${day}%02d"
      (day + 200, s"replaced_$day", ds)
    }.toDF("id", "value", "ds")
    tableUtils.insertPartitions(secondWrite, tableName)

    val result = spark.table(tableName).orderBy("ds").collect()
    result.length shouldBe 5

    for (day <- 1 to 5) {
      val ds = f"2026-04-${day}%02d"
      val rows = result.filter(_.getAs[String]("ds") == ds)
      rows.length shouldBe 1
      rows.head.getAs[Int]("id") shouldBe day + 200
      rows.head.getAs[String]("value") shouldBe s"replaced_$day"
    }
  }

  it should "not destroy data from a prior write when recompute overlaps into a shared file" in {
    val tableName = "default.iceberg_unpartitioned_shared_file_test"
    val tableUtils = TableUtils(spark)

    spark.sql(s"""
      CREATE TABLE IF NOT EXISTS $tableName (
        id INT,
        value STRING,
        ds STRING
      ) USING iceberg
      TBLPROPERTIES (
        'format-version' = '2',
        'write.delete.mode' = 'merge-on-read',
        'write.update.mode' = 'merge-on-read'
      )
    """)

    import spark.implicits._

    // Job 1 writes days 0-5 — all land in a single data file (unpartitioned)
    val job1 = (0 to 5).map { day =>
      val ds = f"2026-05-${day + 1}%02d"
      (day, s"job1_$day", ds)
    }.toDF("id", "value", "ds")
    tableUtils.insertPartitions(job1, tableName)

    // Verify job 1 wrote one file with all 6 rows
    val afterJob1 = spark.table(tableName).collect()
    afterJob1.length shouldBe 6

    // Job 2 (recompute) writes days 3-6 — overlaps days 3-5, adds day 6
    val job2 = (3 to 6).map { day =>
      val ds = f"2026-05-${day + 1}%02d"
      (day + 100, s"job2_$day", ds)
    }.toDF("id", "value", "ds")
    tableUtils.insertPartitions(job2, tableName)

    val result = spark.table(tableName).orderBy("ds").collect()
    // Should have 7 rows: days 0-2 from job 1 + days 3-6 from job 2
    result.length shouldBe 7

    // Days 0-2 (2026-05-01 to 2026-05-03): preserved from job 1, NOT destroyed
    for (day <- 0 to 2) {
      val ds = f"2026-05-${day + 1}%02d"
      val rows = result.filter(_.getAs[String]("ds") == ds)
      withClue(s"Day $ds should have exactly 1 row from job 1: ") {
        rows.length shouldBe 1
        rows.head.getAs[Int]("id") shouldBe day
        rows.head.getAs[String]("value") shouldBe s"job1_$day"
      }
    }

    // Days 3-5 (2026-05-04 to 2026-05-06): replaced by job 2, no duplicates
    for (day <- 3 to 5) {
      val ds = f"2026-05-${day + 1}%02d"
      val rows = result.filter(_.getAs[String]("ds") == ds)
      withClue(s"Day $ds should have exactly 1 row from job 2 (no duplicate from job 1): ") {
        rows.length shouldBe 1
        rows.head.getAs[Int]("id") shouldBe day + 100
        rows.head.getAs[String]("value") shouldBe s"job2_$day"
      }
    }

    // Day 6 (2026-05-07): new from job 2
    val day6 = result.filter(_.getAs[String]("ds") == "2026-05-07")
    day6.length shouldBe 1
    day6.head.getAs[Int]("id") shouldBe 106
    day6.head.getAs[String]("value") shouldBe "job2_6"
  }

  it should "drop stale rows within the recompute range when recompute produces fewer rows per day" in {
    val tableName = "default.iceberg_unpartitioned_stale_rows_test"
    val tableUtils = TableUtils(spark)

    spark.sql(s"""
      CREATE TABLE IF NOT EXISTS $tableName (
        id INT,
        event_time STRING,
        value STRING,
        ds STRING
      ) USING iceberg
      TBLPROPERTIES (
        'format-version' = '2',
        'write.delete.mode' = 'merge-on-read',
        'write.update.mode' = 'merge-on-read'
      )
    """)

    import spark.implicits._

    // First write: day 0 has two events (time1 and time2), days 1-2 have one event each
    val firstWrite = Seq(
      (1, "time1", "old_event_1", "2026-06-01"),
      (2, "time2", "old_event_2", "2026-06-01"),
      (3, "time1", "old_event_3", "2026-06-02"),
      (4, "time1", "old_event_4", "2026-06-03")
    ).toDF("id", "event_time", "value", "ds")
    tableUtils.insertPartitions(firstWrite, tableName)

    spark.table(tableName).collect().length shouldBe 4

    // Recompute for day 0 only — produces only time2 (time1 is gone, e.g. data correction).
    // time1 from the first write should NOT survive in the final table.
    val recompute = Seq(
      (10, "time2", "new_event_2", "2026-06-01")
    ).toDF("id", "event_time", "value", "ds")
    tableUtils.insertPartitions(recompute, tableName)

    val result = spark.table(tableName).orderBy("ds", "id").collect()

    // Day 01: only the recomputed row (time1 from first write must be gone)
    val day01 = result.filter(_.getAs[String]("ds") == "2026-06-01")
    withClue("Day 2026-06-01 should have only the recomputed row, stale time1 must be deleted: ") {
      day01.length shouldBe 1
      day01.head.getAs[Int]("id") shouldBe 10
      day01.head.getAs[String]("value") shouldBe "new_event_2"
    }

    // Days 02-03: untouched from first write
    val day02 = result.filter(_.getAs[String]("ds") == "2026-06-02")
    day02.length shouldBe 1
    day02.head.getAs[String]("value") shouldBe "old_event_3"

    val day03 = result.filter(_.getAs[String]("ds") == "2026-06-03")
    day03.length shouldBe 1
    day03.head.getAs[String]("value") shouldBe "old_event_4"

    result.length shouldBe 3 // 1 recomputed + 2 untouched
  }

  it should "correctly overwrite when partition column is DATE type" in {
    val tableName = "default.iceberg_unpartitioned_date_col_test"
    val tableUtils = TableUtils(spark)

    spark.sql(s"""
      CREATE TABLE IF NOT EXISTS $tableName (
        id INT,
        value STRING,
        ds DATE
      ) USING iceberg
      TBLPROPERTIES (
        'format-version' = '2',
        'write.delete.mode' = 'merge-on-read',
        'write.update.mode' = 'merge-on-read'
      )
    """)

    // Insert via SQL so DATE literals are correct
    spark.sql(s"""INSERT INTO $tableName VALUES
      (1, 'old_1', DATE '2026-04-01'),
      (2, 'old_2', DATE '2026-04-02'),
      (3, 'old_3', DATE '2026-04-03')
    """)
    spark.table(tableName).count() shouldBe 3

    // Overwrite day 2 with new data using insertPartitions (DATE column)
    import spark.implicits._
    val overwrite = Seq((10, "new_2", java.sql.Date.valueOf("2026-04-02"))).toDF("id", "value", "ds")
    tableUtils.insertPartitions(overwrite, tableName)

    val result = spark.table(tableName).orderBy("ds").collect()
    result.length shouldBe 3

    // Day 01: untouched
    result(0).getAs[Int]("id") shouldBe 1

    // Day 02: replaced (not duplicated)
    result(1).getAs[Int]("id") shouldBe 10
    result(1).getAs[String]("value") shouldBe "new_2"

    // Day 03: untouched
    result(2).getAs[Int]("id") shouldBe 3
  }

  it should "correctly overwrite when partition column is TIMESTAMP type" in {
    val tableName = "default.iceberg_unpartitioned_timestamp_col_test"
    val tableUtils = TableUtils(spark)

    spark.sql(s"""
      CREATE TABLE IF NOT EXISTS $tableName (
        id INT,
        value STRING,
        ts TIMESTAMP
      ) USING iceberg
      TBLPROPERTIES (
        'format-version' = '2',
        'write.delete.mode' = 'merge-on-read',
        'write.update.mode' = 'merge-on-read'
      )
    """)

    // Use SQL TIMESTAMP literals (interpreted in session timezone = UTC)
    spark.sql(s"""INSERT INTO $tableName VALUES
      (1, 'old_1', TIMESTAMP '2026-04-01 10:00:00'),
      (2, 'old_2', TIMESTAMP '2026-04-01 14:00:00'),
      (3, 'old_3', TIMESTAMP '2026-04-02 10:00:00')
    """)
    spark.table(tableName).count() shouldBe 3

    // Build overwrite DF from SQL too so timestamps are consistent with the session timezone
    val overwrite = spark.sql("SELECT 10 as id, 'new_1' as value, TIMESTAMP '2026-04-01 10:00:00' as ts")
    tableUtils.insertPartitions(overwrite, tableName, partitionColumns = List("ts"))

    val result = spark.table(tableName).orderBy("ts").collect()

    // Source has min=max=2026-04-01 10:00:00 UTC, so only that exact timestamp is in the
    // delete range. Row at 14:00 and row on day 2 should be untouched.
    result.length shouldBe 3

    // ts=10:00 day 1: replaced
    result(0).getAs[Int]("id") shouldBe 10
    result(0).getAs[String]("value") shouldBe "new_1"

    // ts=14:00 day 1: untouched
    result(1).getAs[Int]("id") shouldBe 2
    result(1).getAs[String]("value") shouldBe "old_2"

    // ts=10:00 day 2: untouched
    result(2).getAs[Int]("id") shouldBe 3
  }

  it should "correctly overwrite when partition column is INT type" in {
    val tableName = "default.iceberg_unpartitioned_int_col_test"
    val tableUtils = TableUtils(spark)

    spark.sql(s"""
      CREATE TABLE IF NOT EXISTS $tableName (
        id INT,
        value STRING,
        batch_id INT
      ) USING iceberg
      TBLPROPERTIES (
        'format-version' = '2',
        'write.delete.mode' = 'merge-on-read',
        'write.update.mode' = 'merge-on-read'
      )
    """)

    spark.sql(s"""INSERT INTO $tableName VALUES
      (1, 'old_1', 100), (2, 'old_2', 200), (3, 'old_3', 300)
    """)

    // Overwrite batch_id=200 with new data; verifies typed literals for INT partition cols.
    import spark.implicits._
    val overwrite = Seq((10, "new_2", 200)).toDF("id", "value", "batch_id")
    tableUtils.insertPartitions(overwrite, tableName, partitionColumns = List("batch_id"))

    val result = spark.table(tableName).orderBy("batch_id").collect()
    result.length shouldBe 3

    result(0).getAs[Int]("id") shouldBe 1 // batch 100 untouched
    result(1).getAs[Int]("id") shouldBe 10 // batch 200 replaced
    result(1).getAs[String]("value") shouldBe "new_2"
    result(2).getAs[Int]("id") shouldBe 3 // batch 300 untouched
  }

  it should "correctly overwrite when partition column is categorical (additionalPartitions case)" in {
    // Regression: a min/max range filter over a categorical column would have included
    // alphabetically in-between values that are not in the source — wrongly deleting them.
    // Example below: source has {'click', 'view'}. Range 'click' <= action <= 'view' spans
    // 'delete' as well. The IN-list delete condition must leave 'delete' rows untouched.
    val tableName = "default.iceberg_unpartitioned_categorical_col_test"
    val tableUtils = TableUtils(spark)

    spark.sql(s"""
      CREATE TABLE IF NOT EXISTS $tableName (
        id INT,
        value STRING,
        action STRING
      ) USING iceberg
      TBLPROPERTIES (
        'format-version' = '2',
        'write.delete.mode' = 'merge-on-read',
        'write.update.mode' = 'merge-on-read'
      )
    """)

    spark.sql(s"""INSERT INTO $tableName VALUES
      (1, 'old_click',  'click'),
      (2, 'old_delete', 'delete'),
      (3, 'old_view',   'view')
    """)

    import spark.implicits._
    val overwrite = Seq(
      (10, "new_click", "click"),
      (30, "new_view", "view")
    ).toDF("id", "value", "action")
    tableUtils.insertPartitions(overwrite, tableName, partitionColumns = List("action"))

    val result = spark.table(tableName).orderBy("action").collect()
    result.length shouldBe 3

    result(0).getAs[String]("action") shouldBe "click"
    result(0).getAs[Int]("id") shouldBe 10 // click replaced

    result(1).getAs[String]("action") shouldBe "delete"
    result(1).getAs[Int]("id") shouldBe 2 // delete untouched — would've been wiped by min/max range
    result(1).getAs[String]("value") shouldBe "old_delete"

    result(2).getAs[String]("action") shouldBe "view"
    result(2).getAs[Int]("id") shouldBe 30 // view replaced
  }
}
