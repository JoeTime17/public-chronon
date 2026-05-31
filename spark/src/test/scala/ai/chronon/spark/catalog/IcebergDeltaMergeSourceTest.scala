package ai.chronon.spark.catalog

import ai.chronon.spark.utils.SparkTestBase
import io.delta.tables.DeltaTable
import org.apache.commons.io.FileUtils
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files

class IcebergDeltaMergeSourceTest extends SparkTestBase with Matchers {

  override def sparkConfs: Map[String, String] = Map(
    "spark.serializer" -> "org.apache.spark.serializer.JavaSerializer",
    "spark.sql.extensions" -> (
      "io.delta.sql.DeltaSparkSessionExtension," +
      "ai.chronon.spark.extensions.ChrononDeltaFixExtension," +
      "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions," +
      "ai.chronon.spark.extensions.ChrononMergeExtension"
    )
  )

  "insertPartitions on an unpartitioned Iceberg table" should
    "prepare Delta source scans before MERGE" in {
      val tableName = "default.iceberg_delta_merge_source_test"
      val deltaDir = Files.createTempDirectory("chronon-delta-dv-source")
      val deltaPath = deltaDir.toString
      val tableUtils = TableUtils(spark)
      import spark.implicits._

      try {
        spark.sql(s"DROP TABLE IF EXISTS $tableName")
        Seq(
          (1, "first_1", "2026-04-01"),
          (2, "deleted", "2026-04-01"),
          (3, "first_2", "2026-04-02")
        ).toDF("id", "value", "ds")
          .write
          .format("delta")
          .option("delta.enableDeletionVectors", "true")
          .mode("overwrite")
          .save(deltaPath)
        DeltaTable.forPath(spark, deltaPath).delete("id = 2")

        spark.sql(s"""
          CREATE TABLE $tableName (
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
        spark.sql(s"""
          INSERT INTO $tableName VALUES
          (99, 'stale', '2026-04-01'),
          (100, 'keep', '2026-04-03')
        """)

        val sourceDf = spark.read.format("delta").load(deltaPath)
        tableUtils.insertPartitions(sourceDf, tableName)

        val result = spark.table(tableName).orderBy("ds", "id").collect()
        result.map(_.getAs[Int]("id")) should contain theSameElementsInOrderAs Seq(1, 3, 100)
        result.map(_.getAs[String]("value")) should contain theSameElementsInOrderAs Seq(
          "first_1",
          "first_2",
          "keep")
      } finally {
        spark.sql(s"DROP TABLE IF EXISTS $tableName")
        FileUtils.deleteDirectory(deltaDir.toFile)
      }
    }

  "insertPartitions on a V1 table" should
    "prepare Delta source scans before insertInto" in {
      val tableName = "default.v1_delta_dv_insert_source_test"
      val deltaDir = Files.createTempDirectory("chronon-delta-dv-v1-source")
      val deltaPath = deltaDir.toString
      val tableUtils = TableUtils(spark)
      import spark.implicits._

      try {
        spark.sql(s"DROP TABLE IF EXISTS $tableName")
        Seq(
          (1, "first_1", "2026-04-01"),
          (2, "deleted", "2026-04-01"),
          (3, "first_2", "2026-04-02")
        ).toDF("id", "value", "ds")
          .write
          .format("delta")
          .option("delta.enableDeletionVectors", "true")
          .mode("overwrite")
          .save(deltaPath)
        DeltaTable.forPath(spark, deltaPath).delete("id = 2")

        spark.sql(s"""
          CREATE TABLE $tableName (
            id INT,
            value STRING
          ) USING parquet
          PARTITIONED BY (ds STRING)
        """)
        spark.sql(s"""
          INSERT INTO $tableName VALUES
          (99, 'stale', '2026-04-01'),
          (100, 'keep', '2026-04-03')
        """)

        val sourceDf = spark.read.format("delta").load(deltaPath)
        tableUtils.insertPartitions(sourceDf, tableName)

        val result = spark.table(tableName).orderBy("ds", "id").collect()
        result.map(_.getAs[Int]("id")) should contain theSameElementsInOrderAs Seq(1, 3, 100)
        result.map(_.getAs[String]("value")) should contain theSameElementsInOrderAs Seq(
          "first_1",
          "first_2",
          "keep")
      } finally {
        spark.sql(s"DROP TABLE IF EXISTS $tableName")
        FileUtils.deleteDirectory(deltaDir.toFile)
      }
    }
}
