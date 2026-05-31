package ai.chronon.spark.batch.iceberg

import ai.chronon.api.PartitionSpec
import ai.chronon.observability.{TileSummary, TileSummaryKey}
import org.apache.iceberg.DataFile
import org.apache.iceberg.types.Type
import org.slf4j.LoggerFactory

import java.time.{LocalDate, ZoneOffset}
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Try

object IcebergClusteredStatsExtractor {
  import IcebergPartitionStatsExtractor.PartitionKey

  @transient private lazy val logger = LoggerFactory.getLogger(getClass)

  def extract(fullTableName: String, table: org.apache.iceberg.Table, confName: String)(implicit
      partitionSpec: PartitionSpec): Option[Map[TileSummaryKey, TileSummary]] = {
    Option(table.schema()) match {
      case None =>
        logger.info(
          s"Cannot extract synthetic partition stats for $fullTableName because the Iceberg schema is missing")
        None
      case Some(schema) =>
        Option(schema.findField(partitionSpec.column)) match {
          case None =>
            logger.info(
              s"Cannot extract synthetic partition stats for $fullTableName because column ${partitionSpec.column} is missing")
            None
          case Some(partitionField) =>
            extractForPartitionField(fullTableName,
                                     table,
                                     confName,
                                     schema,
                                     partitionField.fieldId(),
                                     partitionField.`type`())
        }
    }
  }

  private def extractForPartitionField(fullTableName: String,
                                       table: org.apache.iceberg.Table,
                                       confName: String,
                                       schema: org.apache.iceberg.Schema,
                                       partitionFieldId: Int,
                                       partitionFieldType: Type)(implicit
      partitionSpec: PartitionSpec): Option[Map[TileSummaryKey, TileSummary]] = {
    Option(table.currentSnapshot()) match {
      case None => Some(Map.empty)
      case Some(_) =>
        val tasks = table.newScan().includeColumnStats().planFiles()
        val partitionAccumulators = mutable.Map[PartitionKey, PartitionAccumulator]()
        var complete = true

        try {
          val iterator = tasks.iterator().asScala
          while (iterator.hasNext && complete) {
            val file = iterator.next().file()
            val partitionKey = syntheticPartitionKey(file, partitionFieldId, partitionFieldType)
            val columnStats = extractStrictColumnStats(file, schema, Set(partitionFieldId))

            (partitionKey, columnStats) match {
              case (Some(key), Some(stats)) =>
                val accumulator = partitionAccumulators.getOrElseUpdate(
                  key,
                  new PartitionAccumulator(key, confName, schema)
                )
                accumulator.addFileStats(file.recordCount(), stats)
              case _ =>
                complete = false
            }
          }
        } finally {
          tasks.close()
        }

        if (complete) Some(partitionAccumulators.values.flatMap(_.toTileSummaries).toMap)
        else None
    }
  }

  private def syntheticPartitionKey(file: DataFile, partitionFieldId: Int, partitionFieldType: Type)(implicit
      partitionSpec: PartitionSpec): Option[PartitionKey] = {
    for {
      lower <- partitionBoundValue(file.lowerBounds(), partitionFieldId, partitionFieldType)
      upper <- partitionBoundValue(file.upperBounds(), partitionFieldId, partitionFieldType)
      if lower == upper
    } yield List(partitionSpec.column -> lower)
  }

  private def partitionBoundValue(bounds: java.util.Map[Integer, java.nio.ByteBuffer],
                                  partitionFieldId: Int,
                                  partitionFieldType: Type)(implicit partitionSpec: PartitionSpec): Option[String] =
    Option(bounds).flatMap { values =>
      Option(values.get(partitionFieldId)).flatMap { bound =>
        Try(partitionValue(convertBoundValue(bound, partitionFieldType), partitionFieldType)).toOption
      }
    }

  private def partitionValue(value: Any, fieldType: Type)(implicit partitionSpec: PartitionSpec): String =
    fieldType.typeId() match {
      case Type.TypeID.TIMESTAMP =>
        partitionSpec.at(Math.floorDiv(value.asInstanceOf[java.lang.Long].longValue(), 1000L))
      case Type.TypeID.DATE =>
        val millis = LocalDate
          .ofEpochDay(value.asInstanceOf[java.lang.Integer].longValue())
          .atStartOfDay()
          .toInstant(ZoneOffset.UTC)
          .toEpochMilli
        partitionSpec.at(millis)
      case Type.TypeID.STRING =>
        partitionSpec.at(partitionSpec.epochMillis(value.toString))
      case other =>
        throw new IllegalArgumentException(
          s"Unsupported Iceberg synthetic partition bound type $other for value $value")
    }

  private[iceberg] def extractStrictColumnStats(file: DataFile,
                                                schema: org.apache.iceberg.Schema,
                                                excludedFieldIds: Set[Int]): Option[Map[Int, ColumnStats]] = {
    val fieldIds = schema.columns().asScala.map(_.fieldId()).filterNot(excludedFieldIds.contains)
    val nullCounts = Option(file.nullValueCounts()).map(
      _.asScala
        .map { case (fieldId, nullCount) =>
          fieldId.toInt -> nullCount.toLong
        }
        .toMap)
    val lowerBounds = extractBounds(file.lowerBounds(), schema, excludedFieldIds)
    val upperBounds = extractBounds(file.upperBounds(), schema, excludedFieldIds)

    nullCounts.flatMap { counts =>
      if (fieldIds.forall(counts.contains)) {
        Some(fieldIds.map { fieldId =>
          fieldId -> ColumnStats(
            nullCount = counts(fieldId),
            minValue = lowerBounds.get(fieldId),
            maxValue = upperBounds.get(fieldId)
          )
        }.toMap)
      } else {
        None
      }
    }
  }

  private def extractBounds(bounds: java.util.Map[Integer, java.nio.ByteBuffer],
                            schema: org.apache.iceberg.Schema,
                            excludedFieldIds: Set[Int]): Map[Int, Any] =
    Option(bounds)
      .map(
        _.asScala
          .filterNot { case (fieldId, _) => excludedFieldIds.contains(fieldId) }
          .flatMap { case (fieldId, bound) =>
            Option(schema.findField(fieldId)).flatMap { field =>
              Option(bound).map { validBound =>
                fieldId.toInt -> convertBoundValue(validBound, field.`type`())
              }
            }
          }
          .toMap)
      .getOrElse(Map.empty[Int, Any])

  private def convertBoundValue(bound: java.nio.ByteBuffer, fieldType: Type): Any = {
    require(bound != null, "bound cannot be null")
    require(fieldType != null, "fieldType cannot be null")
    org.apache.iceberg.types.Conversions.fromByteBuffer(fieldType, bound)
  }
}
