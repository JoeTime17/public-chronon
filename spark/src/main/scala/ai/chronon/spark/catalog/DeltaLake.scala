package ai.chronon.spark.catalog

import ai.chronon.api.PartitionSpec
import org.apache.spark.sql.Column
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.functions.{
  coalesce,
  col,
  count,
  date_format,
  from_json,
  lit,
  min,
  max,
  to_date,
  to_timestamp,
  when
}
import org.apache.spark.sql.types.{DataType, DateType, MapType, StringType, StructField, StructType, TimestampType}

import scala.util.{Failure, Success, Try}

// Compiled against delta-spark 3.3.2 to match EMR 7.12.0. DeltaLog.update() signature changes
// across Delta versions (e.g. 2 params in 3.2, 3 params in 3.3), so compiling against an older
// version will cause NoSuchMethodError at runtime if the EMR-bundled Delta jar has a newer signature.
case object DeltaLake extends Format {

  private val DayMillis = 24L * 60L * 60L * 1000L

  override def tableTypeString: String = "delta"

  override def primaryPartitions(tableName: String,
                                 partitionColumn: String,
                                 partitionFilters: String,
                                 subPartitionsFilter: Map[String, String])(implicit
      sparkSession: SparkSession): List[String] =
    super.primaryPartitions(tableName, partitionColumn, partitionFilters, subPartitionsFilter)

  override def partitions(tableName: String, partitionFilters: String)(implicit
      sparkSession: SparkSession): List[Map[String, String]] = {

    // delta lake doesn't support the `SHOW PARTITIONS <tableName>` syntax - https://github.com/delta-io/delta/issues/996
    // there's alternative ways to retrieve partitions using the DeltaLog abstraction which is what we have to lean into
    // below first pull table location as that is what we need to pass to the delta log
    val describeResult = sparkSession.sql(s"DESCRIBE DETAIL $tableName")
    val tablePath = describeResult.select("location").head().getString(0)

    val snapshot = DeltaLog.forTable(sparkSession, tablePath).update()
    val snapshotPartitionsDf = snapshot.allFiles.toDF().select("partitionValues")

    val partitions = snapshotPartitionsDf.collect().map(r => r.getAs[Map[String, String]](0))
    partitions.toList.distinct

  }

  override def virtualPartitions(tableName: String, timestampColumn: String, partitionSpec: PartitionSpec)(implicit
      sparkSession: SparkSession): List[String] = {
    metadataPartitions(tableName, timestampColumn)
      .filter(_.nonEmpty)
      .orElse(statsDateRange(tableName, timestampColumn, partitionSpec)
        .map(_.virtualPartitions(partitionSpec)))
      .getOrElse(super.virtualPartitions(tableName, timestampColumn, partitionSpec))
  }

  override def firstAvailablePartition(tableName: String, partitionColumn: String, partitionSpec: PartitionSpec)(
      implicit sparkSession: SparkSession): Option[String] =
    metadataFirstAvailablePartition(tableName, partitionColumn)
      .orElse(statsDateRange(tableName, partitionColumn, partitionSpec).map(_.firstAvailablePartition))
      .orElse(scanFirstAvailablePartition(tableName, partitionColumn, partitionSpec))

  override def lastAvailablePartition(tableName: String, partitionColumn: String, partitionSpec: PartitionSpec)(implicit
      sparkSession: SparkSession): Option[String] =
    metadataLastAvailablePartition(tableName, partitionColumn)
      .orElse(statsLastAvailablePartition(tableName, partitionColumn, partitionSpec))
      .orElse(scanLastAvailablePartition(tableName, partitionColumn, partitionSpec))

  private def statsLastAvailablePartition(tableName: String, columnName: String, partitionSpec: PartitionSpec)(implicit
      sparkSession: SparkSession): Option[String] =
    statsDateRange(tableName, columnName, partitionSpec).map { range =>
      sparkSession.read.table(tableName).schema(columnName).dataType match {
        case TimestampType => partitionSpec.before(range.lastAvailablePartition)
        case _             => range.lastAvailablePartition
      }
    }

  private[catalog] def statsDateRange(tableName: String, columnName: String, partitionSpec: PartitionSpec)(implicit
      sparkSession: SparkSession): Option[StatsDateRange] = {
    import sparkSession.implicits._

    Try {
      val describeResult = sparkSession.sql(s"DESCRIBE DETAIL $tableName")
      val tablePath = describeResult.select("location").head().getString(0)
      val activeFiles = DeltaLog.forTable(sparkSession, tablePath).update().allFiles.toDF()
      val columnType = sparkSession.read.table(tableName).schema(columnName).dataType

      val statsSchema = StructType(
        Seq(
          StructField("minValues", MapType(StringType, StringType), nullable = true),
          StructField("maxValues", MapType(StringType, StringType), nullable = true)
        ))

      val stats = activeFiles
        .select(from_json(col("stats"), statsSchema).as("stats"))
        .select(
          col("stats.minValues").getItem(columnName).as("min_value"),
          col("stats.maxValues").getItem(columnName).as("max_value")
        )

      val boundaries = stats
        .agg(
          count(lit(1)).as("fileCount"),
          count(when(col("min_value").isNull || col("max_value").isNull, lit(1))).as("missingCount"),
          date_format(min(statsBoundary("min_value", columnType, partitionSpec)), partitionSpec.format).as("start"),
          date_format(max(statsBoundary("max_value", columnType, partitionSpec)), partitionSpec.format).as("end")
        )
        .collect()
        .headOption

      boundaries.flatMap { row =>
        val fileCount = row.getAs[Long]("fileCount")
        val missingCount = row.getAs[Long]("missingCount")
        val start = row.getAs[String]("start")
        val end = row.getAs[String]("end")

        if (fileCount > 0 && missingCount == 0 && start != null && end != null) {
          Some(StatsDateRange(start = start, end = end))
        } else {
          None
        }
      }
    } match {
      case Success(result) =>
        if (result.isDefined) {
          logger.info(s"Resolved Delta log stats boundaries for $tableName.$columnName: ${result.get}")
        } else {
          logger.info(s"Delta log stats were incomplete for $tableName.$columnName; falling back to table scan")
        }
        result
      case Failure(e) =>
        logger.warn(
          s"Failed to resolve Delta log stats boundaries for $tableName.$columnName: ${Option(e.getMessage).getOrElse("(no message)")}")
        None
    }
  }

  private def statsBoundary(boundaryColumn: String, columnType: DataType, partitionSpec: PartitionSpec): Column =
    columnType match {
      case DateType =>
        col(boundaryColumn).cast(DateType)
      case StringType if partitionSpec.spanMillis >= DayMillis =>
        coalesce(to_date(col(boundaryColumn), partitionSpec.format), col(boundaryColumn).cast(DateType))
      case StringType =>
        coalesce(to_timestamp(col(boundaryColumn), partitionSpec.format), col(boundaryColumn).cast("timestamp"))
      case _ =>
        col(boundaryColumn).cast("timestamp")
    }

  override def supportSubPartitionsFilter: Boolean = true
}
