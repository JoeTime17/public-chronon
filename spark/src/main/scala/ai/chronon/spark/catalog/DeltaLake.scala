package ai.chronon.spark.catalog

import ai.chronon.api.PartitionSpec
import org.apache.spark.sql.Column
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.functions.{
  coalesce,
  col,
  count,
  from_json,
  lit,
  min,
  max,
  timestamp_millis,
  to_timestamp,
  when
}
import org.apache.spark.sql.types.{
  DataType,
  DateType,
  LongType,
  MapType,
  NumericType,
  StringType,
  StructField,
  StructType,
  TimestampType
}

import scala.util.{Failure, Success, Try}

// Compiled against delta-spark 3.3.2 to match EMR 7.12.0. DeltaLog.update() signature changes
// across Delta versions (e.g. 2 params in 3.2, 3 params in 3.3), so compiling against an older
// version will cause NoSuchMethodError at runtime if the EMR-bundled Delta jar has a newer signature.
case object DeltaLake extends Format {

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

  // the spark catalog's listColumns doesn't expose partitioning for delta tables; the delta
  // log's own metadata is the ordered source of truth
  override def partitionColumnNames(tableName: String)(implicit sparkSession: SparkSession): Seq[String] =
    Try {
      sparkSession.sql(s"DESCRIBE DETAIL $tableName").select("partitionColumns").head().getSeq[String](0).toList
    }.getOrElse(Seq.empty)

  override def virtualPartitions(tableName: String, timestampColumn: String, partitionSpec: PartitionSpec)(implicit
      sparkSession: SparkSession): List[String] = {
    metadataPartitions(tableName, timestampColumn)
      .filter(_.nonEmpty)
      .orElse(statsVirtualPartitions(tableName, timestampColumn, partitionSpec))
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

  private def statsVirtualPartitions(tableName: String, columnName: String, partitionSpec: PartitionSpec)(implicit
      sparkSession: SparkSession): Option[List[String]] =
    statsDateRange(tableName, columnName, partitionSpec).map { range =>
      sparkSession.read.table(tableName).schema(columnName).dataType match {
        case TimestampType =>
          partitionSpec.expandRange(range.firstAvailablePartition, partitionSpec.before(range.lastAvailablePartition))
        case _ => range.virtualPartitions(partitionSpec)
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

      // raw epoch millis, grid-floored via spec.at in Scala: a format-truncation here would
      // emit off-boundary ds values for sub-daily or offset specs (e.g. '...09:17' from
      // MIN(ts)=09:17 on a 3h grid)
      val boundaries = stats
        .agg(
          count(lit(1)).as("fileCount"),
          count(when(col("min_value").isNull || col("max_value").isNull, lit(1))).as("missingCount"),
          (min(statsBoundary("min_value", columnType, partitionSpec)).cast("long") * 1000).as("startMillis"),
          (max(statsBoundary("max_value", columnType, partitionSpec)).cast("long") * 1000).as("endMillis")
        )
        .collect()
        .headOption

      boundaries.flatMap { row =>
        val fileCount = row.getAs[Long]("fileCount")
        val missingCount = row.getAs[Long]("missingCount")

        if (fileCount > 0 && missingCount == 0 && !row.isNullAt(2) && !row.isNullAt(3)) {
          Some(
            StatsDateRange(start = partitionSpec.at(row.getAs[Long]("startMillis")),
                           end = partitionSpec.at(row.getAs[Long]("endMillis"))))
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

  // always a TIMESTAMP-typed column so the boundary survives as epoch time; the session
  // timezone is UTC by convention, making the casts grid-safe. Numeric columns are epoch
  // MILLIS per chronon convention - a direct TimestampType cast would read them as seconds.
  private def statsBoundary(boundaryColumn: String, columnType: DataType, partitionSpec: PartitionSpec): Column =
    columnType match {
      case DateType =>
        col(boundaryColumn).cast(DateType).cast(TimestampType)
      case StringType =>
        coalesce(to_timestamp(col(boundaryColumn), partitionSpec.format), col(boundaryColumn).cast(TimestampType))
      case _: NumericType =>
        timestamp_millis(col(boundaryColumn).cast(LongType))
      case _ =>
        col(boundaryColumn).cast(TimestampType)
    }

  override def supportSubPartitionsFilter: Boolean = true
}
