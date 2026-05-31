package ai.chronon.spark.batch.iceberg

import ai.chronon.api.ScalaJavaConversions.JMapOps
import ai.chronon.api.{PartitionSpec, ThriftJsonCodec}
import ai.chronon.observability._
import ai.chronon.online.KVStore.PutRequest
import ai.chronon.spark.catalog.Format
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.iceberg.spark.source.SparkTable
import org.apache.iceberg.{DataFile, ManifestFiles}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.TableCatalog

import java.nio.charset.StandardCharsets
import scala.collection.JavaConverters._
import scala.collection.mutable

object IcebergPartitionStatsExtractor {
  type PartitionKey = List[(String, String)]

  def extractPartitionMillisFromSlice(slice: String, partitionSpec: PartitionSpec): Long = {
    // Parse hive-style partition string (e.g., "day=2024-01-15/hour=00") to extract partition value
    val partitionPairs = slice
      .split("/")
      .map { pair =>
        val parts = pair.split("=", 2)
        if (parts.length == 2) {
          parts(0).trim -> parts(1).trim
        } else {
          throw new IllegalArgumentException(s"Invalid partition format in slice: $pair")
        }
      }
      .toMap

    // Extract the value for the partition column used by PartitionSpec
    val partitionValue = partitionPairs.getOrElse(
      partitionSpec.column,
      throw new IllegalArgumentException(s"Partition column '${partitionSpec.column}' not found in slice: $slice"))

    // Convert partition value to millis using PartitionSpec
    partitionSpec.epochMillis(partitionValue)
  }

  def createPartitionStatsPutRequest(
      outputTable: String,
      partitionStats: TileStats,
      dayPartitionMillis: Long,
      statsType: TileStatsType
  ): PutRequest = {
    val key = createMetricsRowKey(outputTable, statsType)
    val value = ThriftJsonCodec.toCompactBase64(partitionStats).getBytes
    val datasetName = s"${outputTable}_BATCH"
    PutRequest(key, value, datasetName, Some(dayPartitionMillis))
  }

  def createNullCountsStats(
      columnTileSummaries: Iterable[(TileSummaryKey, TileSummary)]
  ): NullCounts = {
    val nullCounts = columnTileSummaries.map { case (tileKey, tileSummary) =>
      val fieldId = tileKey.getColumn.toInt
      fieldId -> (if (tileSummary.isSetNullCount) tileSummary.getNullCount else 0L)
    }.toMap

    val rowCount = columnTileSummaries.headOption.map(_._2.getCount).getOrElse(0L)

    new NullCounts()
      .setRowCount(rowCount)
      .setNullCounts(nullCounts.map { case (k, v) =>
        k.asInstanceOf[java.lang.Integer] -> v.asInstanceOf[java.lang.Long]
      }.toJava)
  }

  def createMetricsRowKey(outputTable: String, statsType: TileStatsType): Array[Byte] = {
    s"${outputTable}#${statsType.name()}".getBytes(StandardCharsets.UTF_8)
  }

  def createSchemaMappingPutRequest(outputTable: String, fieldIdToNameMap: Map[Int, String]): PutRequest = {

    // Serialize the mapping to JSON
    val objectMapper = new ObjectMapper()
    objectMapper.registerModule(DefaultScalaModule)
    val jsonValue = objectMapper.writeValueAsString(fieldIdToNameMap)

    // Create the row key following the same pattern as createMetricsRowKey
    val key = s"${outputTable}#schema".getBytes(StandardCharsets.UTF_8)
    val value = jsonValue.getBytes(StandardCharsets.UTF_8)
    val datasetName = s"${outputTable}_BATCH"

    PutRequest(key, value, datasetName)
  }

}

case class ColumnStats(
    nullCount: Long,
    distinctCount: Option[Long] = None,
    minValue: Option[Any] = None,
    maxValue: Option[Any] = None
) {
  def aggregate(other: ColumnStats): ColumnStats = {
    ColumnStats(
      nullCount = this.nullCount + other.nullCount,
      distinctCount = (this.distinctCount, other.distinctCount) match {
        case (Some(a), Some(b)) => Some(math.max(a, b)) // Conservative estimate - take max
        case (Some(a), None)    => Some(a)
        case (None, Some(b))    => Some(b)
        case (None, None)       => None
      },
      minValue = (this.minValue, other.minValue) match {
        case (Some(a), Some(b)) => Some(compareValues(a, b, takeMin = true))
        case (Some(a), None)    => Some(a)
        case (None, Some(b))    => Some(b)
        case (None, None)       => None
      },
      maxValue = (this.maxValue, other.maxValue) match {
        case (Some(a), Some(b)) => Some(compareValues(a, b, takeMin = false))
        case (Some(a), None)    => Some(a)
        case (None, Some(b))    => Some(b)
        case (None, None)       => None
      }
    )
  }

  private def compareValues(a: Any, b: Any, takeMin: Boolean): Any = {
    try {
      (a, b) match {
        case (null, _) => if (takeMin) a else b
        case (_, null) => if (takeMin) a else b
        case (a1: java.lang.Comparable[_], b1) if a1.getClass == b1.getClass =>
          val comparison = a1.asInstanceOf[java.lang.Comparable[Any]].compareTo(b1)
          if ((comparison < 0) == takeMin) a1 else b1
        case (a1: Comparable[_], b1) if a1.getClass == b1.getClass =>
          val comparison = a1.asInstanceOf[Comparable[Any]].compareTo(b1)
          if ((comparison < 0) == takeMin) a1 else b1
        case _ => if (takeMin) a else b
      }
    } catch {
      case _: Exception => if (takeMin) a else b // Fallback to first value on comparison error
    }
  }
}

class PartitionAccumulator(
    val partitionKey: IcebergPartitionStatsExtractor.PartitionKey,
    val confName: String,
    val schema: org.apache.iceberg.Schema
)(implicit val partitionSpec: PartitionSpec) {
  var totalRowCount: Long = 0L
  val columnStats = mutable.Map[Int, ColumnStats]()

  def addFileStats(rowCount: Long, fileColumnStats: Map[Int, ColumnStats]): Unit = {
    totalRowCount += rowCount

    fileColumnStats.foreach { case (fieldId, stats) =>
      val currentStats = columnStats.getOrElse(fieldId, ColumnStats(0L))
      columnStats(fieldId) = currentStats.aggregate(stats)
    }
  }

  def toTileSummaries: Map[TileSummaryKey, TileSummary] = {
    columnStats.map { case (fieldId, stats) =>
      // Create partition key string from partition values
      val partitionKeyStr = partitionKey.map { case (col, value) => s"$col=$value" }.mkString("/")

      val tileKey = new TileSummaryKey()
        .setColumn(fieldId.toString)
        .setName(confName)
        .setSlice(partitionKeyStr) // Use slice to store partition key
        .setSizeMillis(partitionSpec.spanMillis)

      val tileSummary = new TileSummary()
        .setCount(totalRowCount)
        .setNullCount(stats.nullCount)

      tileKey -> tileSummary
    }.toMap
  }
}

class IcebergPartitionStatsExtractor(spark: SparkSession) {
  import IcebergPartitionStatsExtractor.PartitionKey

  private def loadIcebergTable(fullTableName: String): Option[org.apache.iceberg.Table] = {
    try {
      implicit val sparkSession: SparkSession = spark
      val resolved = Format.resolveTableName(fullTableName)
      val catalog = spark.sessionState.catalogManager
        .catalog(resolved.catalog)
        .asInstanceOf[TableCatalog]

      Option(catalog.loadTable(resolved.toIdentifier)) match {
        case Some(sparkTable: SparkTable) => Some(sparkTable.table())
        case _                            => None
      }
    } catch {
      case _: Exception => None
    }
  }

  def extractSchemaMapping(fullTableName: String): Option[Map[Int, String]] = {
    loadIcebergTable(fullTableName).flatMap { table =>
      Option(table.schema()).map { schema =>
        schema.columns().asScala.map(field => field.fieldId() -> field.name()).toMap
      }
    }
  }

  def extractPartitionedStats(fullTableName: String, confName: String)(implicit
      partitionSpec: PartitionSpec): Option[Map[TileSummaryKey, TileSummary]] = {
    loadIcebergTable(fullTableName).flatMap { table =>
      val tableSpec = Option(table.spec())

      if (tableSpec.isEmpty) {
        return None
      }

      if (!tableSpec.get.isPartitioned) {
        return IcebergClusteredStatsExtractor.extract(fullTableName, table, confName)
      }

      val currentSnapshot = Option(table.currentSnapshot())

      val partitionAccumulators = mutable.Map[PartitionKey, PartitionAccumulator]()

      currentSnapshot.foreach { snapshot =>
        val manifestFiles = snapshot.allManifests(table.io()).asScala
        manifestFiles.foreach { manifestFile =>
          val manifestReader = ManifestFiles.read(manifestFile, table.io())

          try {
            manifestReader.forEach((file: DataFile) => {

              val rowCount: Long = file.recordCount()
              val schema = Option(table.schema())
                .getOrElse(throw new IllegalStateException("Table schema is null"))
              val specs = Option(table.specs())
                .getOrElse(throw new IllegalStateException("Table specs is null"))
              val icebergPartitionSpec: org.apache.iceberg.PartitionSpec = Option(specs.get(file.specId()))
                .getOrElse(throw new IllegalStateException(s"Partition spec not found for specId: ${file.specId()}"))
              val partitionFieldIds = Option(icebergPartitionSpec.fields())
                .map(_.asScala.map(_.sourceId()).toSet)
                .getOrElse(Set.empty[Int])

              // Extract partition key using Iceberg's partitionToPath which properly formats all types
              val partition = Option(file.partition())
                .getOrElse(throw new IllegalStateException("File partition data is null"))
              val partitionPath = icebergPartitionSpec.partitionToPath(partition)

              val partitionColToValue: PartitionKey = partitionPath
                .split("/")
                .map { pair =>
                  val parts = pair.split("=", 2)
                  if (parts.length == 2) {
                    parts(0) -> parts(1)
                  } else {
                    throw new IllegalStateException(s"Invalid partition format: $pair in path $partitionPath")
                  }
                }
                .toList

              // Extract column statistics for this file
              val columnStats = extractColumnStats(file, schema, partitionFieldIds)

              // Get or create partition accumulator and add file stats
              val accumulator = partitionAccumulators.getOrElseUpdate(
                partitionColToValue,
                new PartitionAccumulator(partitionColToValue, confName, schema)
              )
              accumulator.addFileStats(rowCount, columnStats)
            })
          } finally {
            manifestReader.close()
          }
        }
      }

      // Convert accumulators to TileKey -> TileSummary mapping
      val result = mutable.Map[TileSummaryKey, TileSummary]()

      partitionAccumulators.values.foreach { accumulator =>
        result ++= accumulator.toTileSummaries
      }

      Some(result.toMap)
    }
  }

  private def extractColumnStats(
      file: DataFile,
      schema: org.apache.iceberg.Schema,
      partitionFieldIds: Set[Int]
  ): Map[Int, ColumnStats] = {

    val columnStatsMap = mutable.Map[Int, ColumnStats]()

    // Extract null counts
    val nullCounts = Option(file.nullValueCounts())
      .map(
        _.asScala
          .filterNot { case (fieldId, _) => partitionFieldIds.contains(fieldId) }
          .map { case (fieldId, nullCount) => fieldId.toInt -> nullCount.toLong }
          .toMap)
      .getOrElse(Map.empty[Int, Long])

    // Extract lower bounds (min values)
    val lowerBounds = Option(file.lowerBounds())
      .map(
        _.asScala
          .filterNot { case (fieldId, _) => partitionFieldIds.contains(fieldId) }
          .flatMap { case (fieldId, bound) =>
            Option(schema.findField(fieldId)).flatMap { field =>
              Option(bound).filter(_ != null).map { validBound =>
                fieldId.toInt -> convertBoundValue(validBound, field.`type`())
              }
            }
          }
          .toMap)
      .getOrElse(Map.empty[Int, Any])

    // Extract upper bounds (max values)
    val upperBounds = Option(file.upperBounds())
      .map(
        _.asScala
          .filterNot { case (fieldId, _) => partitionFieldIds.contains(fieldId) }
          .flatMap { case (fieldId, bound) =>
            Option(schema.findField(fieldId)).flatMap { field =>
              Option(bound).filter(_ != null).map { validBound =>
                fieldId.toInt -> convertBoundValue(validBound, field.`type`())
              }
            }
          }
          .toMap)
      .getOrElse(Map.empty[Int, Any])

    // Combine all statistics using fieldIds as keys
    // This ensures deterministic ordering since we use fieldId as the key
    val allFieldIds =
      (nullCounts.keySet ++ lowerBounds.keySet ++ upperBounds.keySet)

    allFieldIds.foreach { fieldId =>
      val nullCount = nullCounts.getOrElse(fieldId, 0L)
      val minValue = lowerBounds.get(fieldId)
      val maxValue = upperBounds.get(fieldId)

      val distinctCount: Option[Long] = None

      columnStatsMap(fieldId) = ColumnStats(
        nullCount = nullCount,
        distinctCount = distinctCount,
        minValue = minValue,
        maxValue = maxValue
      )
    }

    columnStatsMap.toMap
  }

  private[spark] def convertBoundValue(bound: java.nio.ByteBuffer, fieldType: org.apache.iceberg.types.Type): Any = {
    require(bound != null, "bound cannot be null")
    require(fieldType != null, "fieldType cannot be null")
    org.apache.iceberg.types.Conversions.fromByteBuffer(fieldType, bound)
  }
}
