package ai.chronon.spark.catalog

import ai.chronon.api.PartitionSpec
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.catalyst.util.QuotingUtils
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.functions.{col, date_format, date_sub, min, max}
import org.apache.spark.sql.types.{DateType, StringType, StructType}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

trait Format {

  @transient protected lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  def tableProperties: Map[String, String] = Map.empty[String, String]

  def tableTypeString: String = ""

  def createTable(tableName: String,
                  schema: StructType,
                  partitionColumns: List[String],
                  providedProperties: Map[String, String],
                  semanticHash: Option[String] = None)(implicit sparkSession: SparkSession): Unit = {
    val (creationName, quotedOriginal) = semanticHash match {
      case Some(hash) =>
        val parts = Format.parseIdentifier(tableName).toList
        val hashedParts = parts.init :+ s"${parts.last}_$hash"
        (hashedParts.map(QuotingUtils.quoteIdentifier).mkString("."), tableName)
      case None => (tableName, tableName)
    }
    sparkSession.sql(
      CreationUtils
        .createTableSql(creationName, schema, partitionColumns, providedProperties, tableTypeString))
    if (semanticHash.isDefined) {
      try {
        sparkSession.sql(Format.renameTableSql(creationName, tableName))
      } catch {
        case _: TableAlreadyExistsException =>
          // Another writer already created the target table — safe to clean up our intermediate table
          logger.info(s"Table $quotedOriginal already exists, dropping intermediate table $creationName")
          sparkSession.sql(s"DROP TABLE IF EXISTS $creationName")
        case e: Exception =>
          logger.error(
            s"Failed to rename $creationName to $quotedOriginal. Orphan table $creationName may need manual cleanup.",
            e)
          throw e
      }
    }
  }

  def table(tableName: String, partitionFilters: String)(implicit sparkSession: SparkSession): DataFrame = {

    val df = sparkSession.read.table(tableName)

    if (partitionFilters.isEmpty) {
      df
    } else {
      df.where(partitionFilters)
    }

  }

  // Allow formats to remap the caller-provided column to the name actually used in storage
  // (e.g. a format that stores partitions under an uppercase key, or discovers the real column from metadata)
  protected def resolvePartitionColumn(tableName: String, partitionColumn: String)(implicit
      sparkSession: SparkSession): String = partitionColumn

  // Return the primary partitions (based on the 'partitionColumn') filtered down by sub-partition filters if provided
  // If subpartition filters are supplied and the format doesn't support it, we throw an error
  def primaryPartitions(tableName: String,
                        partitionColumn: String,
                        partitionFilters: String,
                        subPartitionsFilter: Map[String, String] = Map.empty)(implicit
      sparkSession: SparkSession): List[String] = {

    if (!supportSubPartitionsFilter && subPartitionsFilter.nonEmpty) {
      throw new NotImplementedError("subPartitionsFilter is not supported on this format")
    }

    // Allow formats to remap the caller-provided column to the name actually used in storage
    val effectiveColumn = resolvePartitionColumn(tableName, partitionColumn)

    val partitionSeq = Try(partitions(tableName, partitionFilters)(sparkSession)) match {
      case Success(p) => p
      case Failure(e) if Option(e.getMessage).exists(_.contains("TABLE_OR_VIEW_NOT_FOUND")) =>
        logger.warn(s"Failed to get partitions for $tableName: ${e.getMessage}")
        List.empty
      case Failure(e) =>
        logger.warn(
          s"Failed to get partitions for $tableName: ${e.getClass.getSimpleName}: ${Option(e.getMessage).getOrElse("(no message)")}")
        List.empty
    }

    partitionSeq.flatMap { partitionMap =>
      if (
        subPartitionsFilter.forall { case (k, v) =>
          partitionMap.get(k).contains(v)
        }
      ) {
        // partitionMap values come from a Java-interop catalog, so .get can
        // yield Some(null). Rehydrate via Option(_) to collapse that to None.
        partitionMap.get(effectiveColumn).flatMap(Option(_))
      } else {
        None
      }
    }
  }

  // Return a sequence for partitions where each partition entry consists of a map of partition keys to values
  // e.g. Seq(
  //         Map("ds" -> "2023-04-01", "hr" -> "12"),
  //         Map("ds" -> "2023-04-01", "hr" -> "13")
  //         Map("ds" -> "2023-04-02", "hr" -> "00")
  //      )
  def partitions(tableName: String, partitionFilters: String)(implicit
      sparkSession: SparkSession): List[Map[String, String]]

  // Does this format support sub partitions filters
  def supportSubPartitionsFilter: Boolean

  protected def metadataPartitions(tableName: String, partitionColumn: String)(implicit
      sparkSession: SparkSession): Option[List[String]] =
    Try(primaryPartitions(tableName, partitionColumn, "")(sparkSession)) match {
      case Success(metadata) =>
        Some(Format.sanitizePartitionValues(metadata).distinct)
      case Failure(ex) =>
        logger.warn(
          s"[NonFatal] Failed to check primary partitions for ${tableName}, falling back to another boundary lookup: ${ex.getMessage}")
        None
    }

  protected def metadataFirstAvailablePartition(tableName: String, partitionColumn: String)(implicit
      sparkSession: SparkSession): Option[String] =
    metadataPartitions(tableName, partitionColumn).flatMap(Format.pickMinPartition)

  protected def metadataLastAvailablePartition(tableName: String, partitionColumn: String)(implicit
      sparkSession: SparkSession): Option[String] =
    metadataPartitions(tableName, partitionColumn).flatMap(Format.pickMaxPartition)

  protected def scanLastAvailablePartition(tableName: String, partitionColumn: String, partitionSpec: PartitionSpec)(
      implicit sparkSession: SparkSession): Option[String] = {
    import sparkSession.implicits._
    Try {
      val df = sparkSession.read.table(tableName)
      val colType = df.schema(partitionColumn).dataType
      colType match {
        case StringType =>
          df.select(max(col(partitionColumn)).as("last_partition"))
            .as[String]
            .collect()
            .headOption
            .flatMap(v => Option(v))
        case _ =>
          df.select(date_format(date_sub(max(col(partitionColumn)).cast(DateType), 1), partitionSpec.format)
            .as("last_partition"))
            .as[String]
            .collect()
            .headOption
            .flatMap(v => Option(v))
      }
    } match {
      case Success(result) => result
      case Failure(e) if Option(e.getMessage).exists(_.contains("TABLE_OR_VIEW_NOT_FOUND")) =>
        logger.warn(s"Failed to get last available partition for $tableName: ${e.getMessage}")
        None
      case Failure(e) =>
        logger.warn(
          s"Failed to get last available partition for $tableName: ${e.getClass.getSimpleName}: ${Option(e.getMessage).getOrElse("(no message)")}")
        None
    }
  }

  protected def scanFirstAvailablePartition(tableName: String, partitionColumn: String, partitionSpec: PartitionSpec)(
      implicit sparkSession: SparkSession): Option[String] = {
    import sparkSession.implicits._
    Try {
      val df = sparkSession.read.table(tableName)
      val colType = df.schema(partitionColumn).dataType
      colType match {
        case StringType =>
          df.select(min(col(partitionColumn)).as("first_partition"))
            .as[String]
            .collect()
            .headOption
            .flatMap(v => Option(v))
        case _ =>
          df.select(date_format(min(col(partitionColumn)).cast(DateType), partitionSpec.format).as("first_partition"))
            .as[String]
            .collect()
            .headOption
            .flatMap(v => Option(v))
      }
    } match {
      case Success(result) => result
      case Failure(e) if Option(e.getMessage).exists(_.contains("TABLE_OR_VIEW_NOT_FOUND")) =>
        logger.warn(s"Failed to get first available partition for $tableName: ${e.getMessage}")
        None
      case Failure(e) =>
        logger.warn(
          s"Failed to get first available partition for $tableName: ${e.getClass.getSimpleName}: ${Option(e.getMessage).getOrElse("(no message)")}")
        None
    }
  }

  // Unified last available partition: handles both string partition columns and timestamp/date columns.
  // For string columns that are catalog partitions (Hive/Iceberg/Delta), uses metadata-only lookup.
  // For non-string columns, falls back to the historical scan behavior: DATE(MAX(col)) - 1 day.
  def lastAvailablePartition(tableName: String, partitionColumn: String, partitionSpec: PartitionSpec)(implicit
      sparkSession: SparkSession): Option[String] =
    metadataLastAvailablePartition(tableName, partitionColumn)
      .orElse(scanLastAvailablePartition(tableName, partitionColumn, partitionSpec))

  // Unified first available partition: handles both string partition columns and timestamp/date columns.
  // For string columns that are catalog partitions, uses metadata-only lookup.
  // For timestamp/date columns, falls back to a scan.
  def firstAvailablePartition(tableName: String, partitionColumn: String, partitionSpec: PartitionSpec)(implicit
      sparkSession: SparkSession): Option[String] =
    metadataFirstAvailablePartition(tableName, partitionColumn)
      .orElse(scanFirstAvailablePartition(tableName, partitionColumn, partitionSpec))

  @deprecated("Use lastAvailablePartition instead", "0.1.0")
  def maxTimestampDate(tableName: String, timestampColumn: String, partitionSpec: PartitionSpec)(implicit
      sparkSession: SparkSession): Option[String] = {
    import sparkSession.implicits._
    Try {
      val df = sparkSession.read.table(tableName)
      df.select(date_format(max(col(timestampColumn)).cast(DateType), partitionSpec.format).as("max_date"))
        .as[String]
        .collect()
        .headOption
        .flatMap(v => Option(v))
    } match {
      case Success(result) => result
      case Failure(e) =>
        logger.warn(s"Failed to get max timestamp date for $tableName: ${e.getMessage}")
        None
    }
  }

  @deprecated("Use lastAvailablePartition/firstAvailablePartition instead", "0.1.0")
  def virtualPartitions(tableName: String, timestampColumn: String, partitionSpec: PartitionSpec)(implicit
      sparkSession: SparkSession): List[String] = {
    import sparkSession.implicits._
    Try {
      val df = sparkSession.read.table(tableName)
      val result = df
        .select(
          date_format(min(col(timestampColumn)).cast(DateType), partitionSpec.format).as("min_date"),
          date_format(max(col(timestampColumn)).cast(DateType), partitionSpec.format).as("max_date")
        )
        .as[(String, String)]
        .collect()
        .headOption

      result
        .flatMap { case (minDate, maxDate) =>
          if (minDate == null || maxDate == null) None
          else Some(partitionSpec.expandRange(minDate, maxDate))
        }
        .getOrElse(List.empty)
    } match {
      case Success(partitions) => partitions
      case Failure(e) =>
        logger.warn(s"Failed to get virtual partitions for $tableName: ${e.getMessage}")
        List.empty
    }
  }

}

private[catalog] case class StatsDateRange(start: String, end: String) {
  def virtualPartitions(partitionSpec: PartitionSpec): List[String] =
    partitionSpec.expandRange(start, end)

  def firstAvailablePartition: String = start

  def lastAvailablePartition: String = end
}

case class ResolvedTableName(catalog: String, namespace: String, table: String) {
  def toIdentifier: Identifier = Identifier.of(Array(namespace), table)
}

object Format {

  private val stringOrdering: Ordering[String] = Ordering.String

  def sanitizePartitionValues(partitions: Iterable[String]): List[String] = partitions.iterator
    .flatMap(Option(_))
    .toList

  def pickMinPartition(partitions: Iterable[String]): Option[String] = {
    sanitizePartitionValues(partitions).reduceOption((x, y) => stringOrdering.min(x, y))
  }

  def pickMaxPartition(partitions: Iterable[String]): Option[String] = {
    sanitizePartitionValues(partitions).reduceOption((x, y) => stringOrdering.max(x, y))
  }

  /** Parse a (possibly multipart, possibly backticked) table identifier into its dotted segments,
    * using the session parser so any session-level parser extensions apply. Does NOT fill in a
    * default catalog: callers (e.g. [[resolveTableName]]) own that policy since different contexts
    * have different defaults (read vs. write catalog, ephemeral eval catalog, etc.).
    *
    * Prefer this over `split("\\.")` wherever you need to break up a table identifier — the naive
    * split drops backticks and corrupts segments containing escaped dots.
    */
  def parseIdentifier(identifier: String)(implicit sparkSession: SparkSession): Seq[String] =
    sparkSession.sessionState.sqlParser.parseMultipartIdentifier(identifier)

  def parseHiveStylePartition(pstring: String): List[(String, String)] = {
    pstring
      .split("/")
      .map { part =>
        val p = part.split("=", 2)
        p(0) -> p(1)
      }
      .toList
  }

  def resolveTableName(tableName: String)(implicit sparkSession: SparkSession): ResolvedTableName = {
    val parsed = parseIdentifier(tableName)
    def defaultCatalog: String = sparkSession.conf.get("spark.sql.defaultCatalog", "spark_catalog")
    parsed.toList match {
      case catalog :: namespace :: table :: Nil => ResolvedTableName(catalog, namespace, table)
      case namespace :: table :: Nil            => ResolvedTableName(defaultCatalog, namespace, table)
      case table :: Nil =>
        ResolvedTableName(defaultCatalog, sparkSession.catalog.currentDatabase, table)
      case _ => throw new IllegalStateException(s"Invalid table naming convention specified: ${tableName}")
    }
  }

  // Lightweight version that avoids triggering catalog initialization
  def getCatalog(inputTableName: String)(implicit sparkSession: SparkSession): String = {
    val parsed = parseIdentifier(inputTableName)
    def defaultCatalog: String = sparkSession.conf.get("spark.sql.defaultCatalog", "spark_catalog")
    parsed.toList match {
      case catalog :: _ :: _ :: Nil => catalog
      case _ :: _ :: Nil            => defaultCatalog
      case _ :: Nil                 => defaultCatalog
      case _ => throw new IllegalStateException(s"Invalid table naming convention specified: ${inputTableName}")
    }
  }

  def renameTableSql(srcTable: String, destTable: String)(implicit sparkSession: SparkSession): String = {
    val srcResolved = resolveTableName(srcTable)
    val destResolved = resolveTableName(destTable)
    val normalizedDest = if (srcResolved.catalog == destResolved.catalog) {
      s"${QuotingUtils.quoteIdentifier(destResolved.namespace)}.${QuotingUtils.quoteIdentifier(destResolved.table)}"
    } else {
      s"${QuotingUtils.quoteIdentifier(destResolved.catalog)}.${QuotingUtils.quoteIdentifier(destResolved.namespace)}.${QuotingUtils.quoteIdentifier(destResolved.table)}"
    }
    s"ALTER TABLE $srcTable RENAME TO $normalizedDest"
  }

}
