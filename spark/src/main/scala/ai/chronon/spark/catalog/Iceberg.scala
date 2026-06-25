package ai.chronon.spark.catalog

import ai.chronon.api.PartitionSpec
import ai.chronon.spark.batch.iceberg.IcebergPartitionStatsExtractor
import org.apache.iceberg.DataFile
import org.apache.iceberg.spark.source.SparkTable
import org.apache.iceberg.types.Type
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StructType, TimestampType}

import java.time.{LocalDate, ZoneOffset}
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

case object Iceberg extends Format {

  override def tableTypeString: String = "iceberg"

  override def tableProperties: Map[String, String] = {
    Map(
      "commit.retry.num-retries" -> "20", // default = 4
      "commit.retry.min-wait-ms" -> (10 * 1000).toString,
      "commit.retry.max-wait-ms" -> (600 * 1000).toString,
      "commit.status-check.num-retries" -> "20", // default = 3
      "commit.status-check.min-wait-ms" -> (10 * 1000).toString, // default = 1000
      "commit.status-check.max-wait-ms" -> (600 * 1000).toString,
      "write.merge.isolation-level" -> "snapshot",
      "format-version" -> "2"
    )
  }

  def qualifyWithCatalog(tableName: String)(implicit sparkSession: SparkSession): String =
    Format.resolveTableName(tableName).quoted

  override def primaryPartitions(tableName: String,
                                 partitionColumn: String,
                                 partitionFilters: String,
                                 subPartitionsFilter: Map[String, String])(implicit
      sparkSession: SparkSession): List[String] = {

    if (!supportSubPartitionsFilter && subPartitionsFilter.nonEmpty) {
      throw new NotImplementedError("subPartitionsFilter is not supported on this format")
    }

    Try(getIcebergPartitions(tableName, partitionColumn)) match {
      case Success(p) => p
      case Failure(e) if Option(e.getMessage).exists(_.contains("TABLE_OR_VIEW_NOT_FOUND")) =>
        logger.warn(s"Failed to get partitions for $tableName: ${e.getMessage}")
        List.empty
      case Failure(e) =>
        logger.warn(
          s"Failed to get partitions for $tableName: ${e.getClass.getSimpleName}: ${Option(e.getMessage).getOrElse("(no message)")}")
        List.empty
    }
  }

  override def partitions(tableName: String, partitionFilters: String)(implicit
      sparkSession: SparkSession): List[Map[String, String]] = {
    val partitionsDf = sparkSession.table(s"${Format.resolveTableName(tableName).quoted}.partitions")

    val index = partitionsDf.schema.fieldIndex("partition")
    val partitionColumnNames = partitionsDf.schema(index).dataType.asInstanceOf[StructType].fieldNames

    partitionsDf
      .select(col("partition"))
      .collect()
      .map { row =>
        val partitionData = row.getStruct(0)
        partitionColumnNames.flatMap { colName =>
          Option(partitionData.getAs[Any](colName)).map(colName -> _.toString)
        }.toMap
      }
      .toList
      .distinct
  }

  /** Partition column names from the Iceberg partition spec (via the .partitions metadata
    * table, which the spark catalog's listColumns can't see). Empty if unpartitioned.
    */
  override def partitionColumnNames(tableName: String)(implicit sparkSession: SparkSession): Seq[String] =
    Try {
      val partitionsDf = sparkSession.table(s"${Format.resolveTableName(tableName).quoted}.partitions")
      val index = partitionsDf.schema.fieldIndex("partition")
      partitionsDf.schema(index).dataType.asInstanceOf[StructType].fieldNames.toSeq
    }.getOrElse(Seq.empty)

  override def virtualPartitions(tableName: String, timestampColumn: String, partitionSpec: PartitionSpec)(implicit
      sparkSession: SparkSession): List[String] =
    metadataPartitions(tableName, timestampColumn)
      .filter(_.nonEmpty)
      .orElse(statsVirtualPartitions(tableName, timestampColumn, partitionSpec))
      .getOrElse(super.virtualPartitions(tableName, timestampColumn, partitionSpec))

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
      sparkSession: SparkSession): Option[StatsDateRange] =
    Try {
      val table = loadIcebergTable(tableName).getOrElse {
        throw new IllegalStateException(s"Could not load Iceberg table: $tableName")
      }
      val field = Option(table.schema().findField(columnName)).getOrElse {
        throw new IllegalArgumentException(s"Column $columnName not found in Iceberg schema for $tableName")
      }
      val fieldId = field.fieldId().asInstanceOf[java.lang.Integer]
      val fieldType = field.`type`()
      val extractor = new IcebergPartitionStatsExtractor(sparkSession)

      currentDataFilesDateRange(table, fieldId, fieldType, partitionSpec, extractor)
    } match {
      case Success(result) =>
        if (result.isDefined) {
          logger.info(s"Resolved Iceberg file stats boundaries for $tableName.$columnName: ${result.get}")
        } else {
          logger.info(s"Iceberg file stats were incomplete for $tableName.$columnName; falling back to table scan")
        }
        result
      case Failure(e) =>
        logger.warn(
          s"Failed to resolve Iceberg file stats boundaries for $tableName.$columnName: ${Option(e.getMessage).getOrElse("(no message)")}")
        None
    }

  private def fileDateRange(file: DataFile,
                            fieldId: java.lang.Integer,
                            fieldType: org.apache.iceberg.types.Type,
                            partitionSpec: PartitionSpec,
                            extractor: IcebergPartitionStatsExtractor): Option[(Long, Long)] = {
    val lower = Option(file.lowerBounds()).flatMap(bounds => Option(bounds.get(fieldId)))
    val upper = Option(file.upperBounds()).flatMap(bounds => Option(bounds.get(fieldId)))

    for {
      lowerBound <- lower
      upperBound <- upper
    } yield {
      val lowerMillis = boundMillis(extractor.convertBoundValue(lowerBound, fieldType), fieldType, partitionSpec)
      val upperMillis = boundMillis(extractor.convertBoundValue(upperBound, fieldType), fieldType, partitionSpec)
      lowerMillis -> upperMillis
    }
  }

  private def boundMillis(value: Any, fieldType: Type, partitionSpec: PartitionSpec): Long =
    fieldType.typeId() match {
      case Type.TypeID.TIMESTAMP =>
        Math.floorDiv(value.asInstanceOf[java.lang.Long].longValue(), 1000L)
      case Type.TypeID.DATE =>
        LocalDate
          .ofEpochDay(value.asInstanceOf[java.lang.Integer].longValue())
          .atStartOfDay()
          .toInstant(ZoneOffset.UTC)
          .toEpochMilli
      case Type.TypeID.STRING =>
        partitionSpec.epochMillis(value.toString)
      case other =>
        throw new IllegalArgumentException(s"Unsupported Iceberg bound type $other for value $value")
    }

  private def currentDataFilesDateRange(table: org.apache.iceberg.Table,
                                        fieldId: java.lang.Integer,
                                        fieldType: org.apache.iceberg.types.Type,
                                        partitionSpec: PartitionSpec,
                                        extractor: IcebergPartitionStatsExtractor): Option[StatsDateRange] =
    Option(table.currentSnapshot()).flatMap { _ =>
      val tasks = table.newScan().includeColumnStats().planFiles()
      try {
        val range = tasks.iterator().asScala.foldLeft(Some(None): Option[Option[(Long, Long)]]) {
          case (None, _) => None
          case (Some(acc), task) =>
            fileDateRange(task.file(), fieldId, fieldType, partitionSpec, extractor).map {
              case (lowerMillis, upperMillis) =>
                Some(acc.fold(lowerMillis -> upperMillis) { case (minMillis, maxMillis) =>
                  Math.min(minMillis, lowerMillis) -> Math.max(maxMillis, upperMillis)
                })
            }
        }

        range.flatten.map { case (minMillis, maxMillis) =>
          StatsDateRange(
            start = partitionSpec.at(minMillis),
            end = partitionSpec.at(maxMillis)
          )
        }
      } finally {
        tasks.close()
      }
    }

  private def loadIcebergTable(tableName: String)(implicit
      sparkSession: SparkSession): Option[org.apache.iceberg.Table] =
    Try {
      val resolved = Format.resolveTableName(tableName)
      val catalog = sparkSession.sessionState.catalogManager
        .catalog(resolved.catalog)
        .asInstanceOf[TableCatalog]

      catalog.loadTable(resolved.toIdentifier) match {
        case sparkTable: SparkTable => sparkTable.table()
        case other => throw new IllegalStateException(s"Not an Iceberg SparkTable: ${other.getClass.getName}")
      }
    }.toOption

  private def getIcebergPartitions(tableName: String, partitionColumn: String)(implicit
      sparkSession: SparkSession): List[String] = {

    val partitionsDf = sparkSession.table(s"${Format.resolveTableName(tableName).quoted}.partitions")

    val index = partitionsDf.schema.fieldIndex("partition")
    if (partitionsDf.schema(index).dataType.asInstanceOf[StructType].fieldNames.contains("hr")) {
      // Hour filter is currently buggy in iceberg. https://github.com/apache/iceberg/issues/4718
      // so we collect and then filter.
      partitionsDf
        .select(col(s"partition.$partitionColumn").cast("string"), col("partition.hr"))
        .collect()
        .filter(_.get(1) == null)
        .flatMap(row => Option(row.getString(0)))
        .toList
    } else {
      partitionsDf
        .select(col(s"partition.$partitionColumn").cast("string"))
        .collect()
        .flatMap(row => Option(row.getString(0)))
        .toList
    }
  }

  override def supportSubPartitionsFilter: Boolean = false
}
