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

package ai.chronon.spark.catalog

import ai.chronon.api.{Constants, PartitionRange, PartitionSpec, Query, QueryUtils, TsUtils}
import ai.chronon.api.ColorPrinter.ColorString
import ai.chronon.api.Extensions._
import ai.chronon.api.ScalaJavaConversions._
import org.apache.spark.sql.{AnalysisException, Column, DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Project}
import org.apache.spark.sql.catalyst.util.QuotingUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.slf4j.{Logger, LoggerFactory}

import java.io.{PrintWriter, StringWriter}
import java.time.{Instant, ZoneId}
import java.time.format.DateTimeFormatter
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

/** Trait to track the table format in use by a Chronon dataset and some utility methods to help
  * retrieve metadata / configure it appropriately at creation time
  */

class TableUtils(@transient val sparkSession: SparkSession, partitionSpecOverride: Option[PartitionSpec] = None)
    extends Serializable {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  private val ARCHIVE_TIMESTAMP_FORMAT = "yyyyMMddHHmmss"
  @transient private lazy val archiveTimestampFormatter = DateTimeFormatter
    .ofPattern(ARCHIVE_TIMESTAMP_FORMAT)
    .withZone(ZoneId.systemDefault())
  val partitionSpec: PartitionSpec = partitionSpecOverride.getOrElse {
    val partitionColumn = sparkSession.conf.get("spark.chronon.partition.column", "ds")
    val partitionFormat = sparkSession.conf.get("spark.chronon.partition.format", "yyyy-MM-dd")
    PartitionSpec(partitionColumn, partitionFormat, WindowUtils.Day.millis)
  }
  val partitionColumn: String = partitionSpec.column
  val partitionFormat: String = partitionSpec.format

  val smallModelEnabled: Boolean =
    sparkSession.conf.get("spark.chronon.backfill.small_mode.enabled", "true").toBoolean
  val smallModeNumRowsCutoff: Int =
    sparkSession.conf.get("spark.chronon.backfill.small_mode.cutoff", "5000").toInt
  val backfillValidationEnforced: Boolean =
    sparkSession.conf.get("spark.chronon.backfill.validation.enabled", "true").toBoolean
  // Threshold to control whether to use bloomfilter on join backfill. If the backfill row approximate count is under this threshold, we will use bloomfilter.
  // default threshold is 100K rows
  val bloomFilterThreshold: Long =
    sparkSession.conf.get("spark.chronon.backfill.bloomfilter.threshold", "1000000").toLong
  val checkLeftTimeRange: Boolean =
    sparkSession.conf.get("spark.chronon.join.backfill.check.left_time_range", "false").toBoolean
  // TODO: This should be at the level of groupBy in theory
  val skewFreeMode: Boolean = sparkSession.conf
    .get("spark.chronon.join.backfill.mode.skewFree", "true")
    .toLowerCase()
    .toBoolean

  // transient because the format provider is not always serializable.
  // for example, BigQueryImpl during reflecting with bq flavor
  @transient private lazy val tableFormatProvider: FormatProvider = FormatProvider.from(sparkSession)

  sparkSession.sparkContext.setLogLevel("ERROR")

  def withJobDescription[T](desc: String)(block: => T): T = {
    val sc = sparkSession.sparkContext
    val prev = sc.getLocalProperty("spark.job.description")
    sc.setJobDescription(s"[chronon] $desc")
    try block
    finally sc.setJobDescription(prev)
  }

  def tableReachable(tableName: String, ignoreFailure: Boolean = false): Boolean = {
    Try { sparkSession.table(tableName) } match {
      case Success(_) => true
      case Failure(ex: AnalysisException) if ex.getMessage.contains("TABLE_OR_VIEW_NOT_FOUND") =>
        if (!ignoreFailure) {
          logger.info(s"Cannot find table or view $tableName.")
        }
        false
      case Failure(ex) =>
        if (!ignoreFailure) {
          logger.info(s"""Couldn't reach $tableName. Error: ${ex.traceString.red}
             |Call path:
             |${cleanStackTrace(ex).yellow}
             |""".stripMargin)
        }
        false
    }
  }

  def loadTable(tableName: String, rangeWheres: Seq[String] = List.empty[String]): DataFrame = {
    tableFormatProvider
      .readFormat(tableName)
      .map(_.table(tableName, andPredicates(rangeWheres))(sparkSession))
      .getOrElse(
        throw new RuntimeException(s"Could not load table: ${tableName} with partition filter: ${rangeWheres}"))
  }

  def partitions(tableName: String,
                 subPartitionsFilter: Map[String, String] = Map.empty,
                 partitionRange: Option[PartitionRange] = None,
                 tablePartitionSpec: Option[PartitionSpec] = None,
                 timePartitioned: Boolean = false): List[String] = {
    val rangeWheres = andPredicates(partitionRange.map(_.whereClauses).getOrElse(Seq.empty))

    val effectivePartColumn = tablePartitionSpec.map(_.column).getOrElse(partitionSpec.column)
    val effectiveSpec = tablePartitionSpec.getOrElse(partitionSpec)

    val partitions = tableFormatProvider
      .readFormat(tableName)
      .map((format) => {
        if (timePartitioned) {
          logger.info(
            s"Getting virtual partitions for time-partitioned table ${tableName} using column ${effectivePartColumn}")
          val allPartitions = format.virtualPartitions(tableName, effectivePartColumn, effectiveSpec)(sparkSession)
          // Filter by range if provided
          partitionRange match {
            case Some(range) =>
              allPartitions.filter(p =>
                (Option(range.start).isEmpty || p >= range.start) &&
                  (Option(range.end).isEmpty || p <= range.end))
            case None => allPartitions
          }
        } else {
          logger.info(
            s"Getting partitions for ${tableName} with partitionColumnName ${effectivePartColumn} and subpartitions: ${subPartitionsFilter}")
          val catalogPartitions =
            format.primaryPartitions(tableName, effectivePartColumn, rangeWheres, subPartitionsFilter)(sparkSession)
          // clustered tables have no catalog partitions but real logical ones: fall back to
          // the distinct values of the partition column so compute planning sees the data that
          // exists instead of recomputing fully-populated tables (sub-partition filters can't
          // be honored on this path, so it only fires without them)
          if (catalogPartitions.nonEmpty || subPartitionsFilter.nonEmpty) catalogPartitions
          else format.scanDistinctPartitions(tableName, effectivePartColumn, rangeWheres)(sparkSession)
        }
      })
      .map { partitions =>
        val nonNullPartitions = Format.sanitizePartitionValues(partitions)
        if (nonNullPartitions.isEmpty) {
          logger.info(s"No partitions found for table: $tableName with subpartition filters ${subPartitionsFilter}")
        } else {
          logger.info(
            s"Found ${nonNullPartitions.size}, between (${nonNullPartitions.min}, ${nonNullPartitions.max}) partitions for table: $tableName")
        }
        Format.zeroParsedPartitionsWarning(tableName, nonNullPartitions, effectiveSpec).foreach(logger.error)
        nonNullPartitions
      }
      .getOrElse(List.empty)

    if (!timePartitioned) {
      tablePartitionSpec
        .map(ps => partitions.map(toGlobalFormat(_, ps)))
        .getOrElse(partitions)
    } else {
      partitions
    }
  }

  /** Listed ds values are normalized to the global spec's format only when the grids match (the
    * legacy yyyyMMdd-table-under-yyyy-MM-dd-global case, which long-standing callers expect);
    * values on a different grid stay in their own spec - translating them into a coarser
    * global spec would floor them and collapse distinct partitions into one.
    */
  private def toGlobalFormat(value: String, tableSpec: PartitionSpec): String =
    if (tableSpec.hasSameGrid(partitionSpec)) tableSpec.translate(value, partitionSpec) else value

  def maxTimestampDate(tableName: String,
                       timestampColumn: String,
                       tablePartitionSpec: Option[PartitionSpec] = None): Option[String] = {
    val effectiveSpec = tablePartitionSpec.getOrElse(partitionSpec)
    tableFormatProvider
      .readFormat(tableName)
      .flatMap(_.maxTimestampDate(tableName, timestampColumn, effectiveSpec)(sparkSession))
  }

  /** The table's last partition ds (in its own spec - no translation shim) and the
    * exclusive epoch upper bound of the time its data holds. Completeness questions are
    * answered by comparing epoch millis - spec-free - rather than by comparing ds values
    * across specs.
    *
    * Cost contract: readiness checks must stay metadata-fast. This is ONE catalog-metadata
    * read; the value scan inside Format.lastAvailablePartition only fires for tables with no
    * catalog partitions at all (clustered / timestamp-backed), where a query is the only
    * possible signal. The millis arithmetic is local.
    */
  def dataWatermark(tableName: String, tableSpec: Option[PartitionSpec] = None): Option[(String, Long)] = {
    val spec = tableSpec.getOrElse(partitionSpec)
    tableFormatProvider
      .readFormat(tableName)
      .flatMap(_.lastAvailablePartition(tableName, spec.column, spec)(sparkSession))
      .map(value => (value, spec.partitionEndMillis(value)))
  }

  def dataWatermarkMillis(tableName: String, tableSpec: Option[PartitionSpec] = None): Option[Long] =
    dataWatermark(tableName, tableSpec).map(_._2)

  def tableCoversRange(table: String, range: PartitionRange, tableSpec: Option[PartitionSpec] = None): Boolean = {
    try {
      dataWatermarkMillis(table, tableSpec) match {
        case Some(watermark) =>
          // maxMillis is inclusive-millis; the watermark is exclusive
          val covers = watermark > range.maxMillis
          if (!covers) {
            logger.info(
              s"Table $table does not cover range: data watermark ${TsUtils.toStr(watermark)} <= " +
                s"required end ${TsUtils.toStr(range.maxMillis)}")
          }
          covers
        case None =>
          logger.info(s"Table $table has no available partitions")
          false
      }
    } catch {
      case e: Exception =>
        logger.warn(s"Error checking table completeness: ${e.getMessage}")
        false
    }
  }

  // Given a table and a query extract the schema of the columns involved as input.
  def getColumnsFromQuery(query: String): Seq[String] = {
    val parser = sparkSession.sessionState.sqlParser
    val logicalPlan = parser.parsePlan(query)
    logicalPlan
      .collect {
        case p: Project =>
          p.projectList.flatMap(p => parser.parseExpression(p.sql).references.map(attr => attr.name))
        case f: Filter => f.condition.references.map(attr => attr.name)
      }
      .flatten
      .map(_.replace("`", ""))
      .distinct
      .sorted
  }

  def getSchemaFromTable(tableName: String): StructType = {
    loadTable(tableName).schema
  }

  def lastAvailablePartition(tableName: String,
                             partitionRange: Option[PartitionRange] = None,
                             subPartitionFilters: Map[String, String] = Map.empty,
                             tablePartitionSpec: Option[PartitionSpec] = None): Option[String] = {
    val effectiveSpec = tablePartitionSpec.getOrElse(partitionSpec)
    val effectivePartColumn = effectiveSpec.column
    if (subPartitionFilters.nonEmpty) {
      // Fall back to enumeration when sub-partition filters are needed
      Format.pickMaxPartition(
        partitions(tableName, subPartitionFilters, partitionRange, tablePartitionSpec = tablePartitionSpec))
    } else {
      tableFormatProvider
        .readFormat(tableName)
        .flatMap(_.lastAvailablePartition(tableName, effectivePartColumn, effectiveSpec)(sparkSession))
        .map(toGlobalFormat(_, effectiveSpec))
    }
  }

  def firstAvailablePartition(tableName: String,
                              partitionSpec: PartitionSpec = partitionSpec,
                              partitionRange: Option[PartitionRange] = None,
                              subPartitionFilters: Map[String, String] = Map.empty): Option[String] = {
    if (subPartitionFilters.nonEmpty) {
      // Fall back to enumeration when sub-partition filters are needed
      Format.pickMinPartition(
        partitions(
          tableName,
          subPartitionFilters,
          partitionRange.map(_.intersectingRange(partitionSpec)),
          tablePartitionSpec = Some(partitionSpec)
        ))
    } else {
      val effectivePartColumn = partitionSpec.column
      tableFormatProvider
        .readFormat(tableName)
        .flatMap(_.firstAvailablePartition(tableName, effectivePartColumn, partitionSpec)(sparkSession))
        .map(toGlobalFormat(_, partitionSpec))
    }
  }

  def insertPartitions(df: DataFrame,
                       tableName: String,
                       tableProperties: Map[String, String] = null,
                       partitionColumns: List[String] = List(partitionColumn),
                       autoExpand: Boolean = false,
                       semanticHash: Option[String] = None): Unit = {

    // partitions to the last
    val colOrder = df.columns.diff(partitionColumns) ++ partitionColumns

    val dfRearranged = df.select(colOrder.map(colName => df.col(QuotingUtils.quoteIdentifier(colName))): _*)

    if (!tableReachable(tableName, ignoreFailure = true)) {
      try {
        tableFormatProvider.writeFormat.createTable(tableName,
                                                    dfRearranged.schema,
                                                    partitionColumns,
                                                    tableProperties,
                                                    semanticHash)(sparkSession)
      } catch {
        case _: TableAlreadyExistsException =>
          logger.info(s"Table $tableName already exists, skipping creation")
        case e: Exception =>
          logger.error(s"Failed to create table $tableName", e)
          throw e
      }
    }

    if (autoExpand) {
      expandTable(tableName, dfRearranged.schema)
    }

    val defaultTableProperties = tableFormatProvider.writeFormat.tableProperties
    val userTableProperties = Option(tableProperties).getOrElse(Map.empty)
    val desiredTableProperties = defaultTableProperties ++ userTableProperties
    val existingTableProperties = getTableProperties(tableName).getOrElse(Map.empty)
    val propertyDiff = desiredTableProperties.filter { case (key, value) =>
      !existingTableProperties.get(key).contains(value)
    }
    if (propertyDiff.nonEmpty) {
      sql(CreationUtils.alterTablePropertiesSql(tableName, propertyDiff))
    }

    val finalizedDf = if (autoExpand) {
      // reselect the columns so that a deprecated columns will be selected as NULL before write
      val tableSchema = getSchemaFromTable(tableName)
      val finalColumns = tableSchema.fieldNames.map(fieldName => {
        val escapedName = QuotingUtils.quoteIdentifier(fieldName)
        if (dfRearranged.schema.fieldNames.contains(fieldName)) {
          df(escapedName)
        } else {
          lit(null).as(escapedName)
        }
      })
      dfRearranged.select(finalColumns: _*)
    } else {
      // if autoExpand is set to false, and an inconsistent df is passed, we want to pass in the df as in
      // so that an exception will be thrown below
      dfRearranged
    }

    logger.info(s"Writing to $tableName ...")
    val isIceberg = tableFormatProvider.readFormat(tableName).contains(Iceberg)
    val hasPartitionSpec =
      isIceberg && Try(Iceberg.partitionColumnNames(tableName)(sparkSession).nonEmpty).getOrElse(false)
    if (isIceberg && partitionColumns.nonEmpty && !hasPartitionSpec) {
      // Unpartitioned / UC liquid clustering: insertInto() with DYNAMIC mode appends instead of
      // replacing. Use MERGE INTO with ON FALSE for atomic delete+insert in a single snapshot.
      // ON FALSE means: no target row matches any source row, so all target rows matching the
      // delete condition are "not matched by source" (deleted) and all source rows are
      // "not matched by target" (inserted).
      //
      // Delete condition uses per-column IN lists AND'ed together rather than a min/max range.
      // additionalPartitions can include categorical columns (e.g. `action`) where range
      // semantics are meaningless; IN-lists stay correct for those. It is slightly over-broad
      // for multi-column keys (matches cartesian product of distinct values) but safe here —
      // we only delete rows the upstream job intends to rewrite.
      val tempView = s"__chronon_insert_${tableName.replace('.', '_')}_${System.nanoTime()}"
      finalizedDf.createOrReplaceTempView(tempView)
      val deleteCondition = partitionColumns
        .map { pc =>
          val values = finalizedDf.select(col(pc)).distinct().collect().map(row => lit(row.get(0)).expr.sql)
          s"target.`$pc` IN (${values.mkString(", ")})"
        }
        .mkString(" AND ")
      val mergeSQL =
        s"""MERGE INTO $tableName AS target
           |USING $tempView AS source
           |ON FALSE
           |WHEN NOT MATCHED BY SOURCE AND $deleteCondition THEN DELETE
           |WHEN NOT MATCHED THEN INSERT *""".stripMargin
      sparkSession.sql(mergeSQL)
      sparkSession.catalog.dropTempView(tempView)
    } else {
      finalizedDf.write
        .mode(SaveMode.Overwrite)
        .insertInto(tableName)
    }
    logger.info(s"Finished writing to $tableName")
  }

  // retains only the invocations from chronon code.
  private def cleanStackTrace(throwable: Throwable): String = {
    val sw = new StringWriter()
    val pw = new PrintWriter(sw)
    throwable.printStackTrace(pw)
    val stackTraceString = sw.toString
    "    " + stackTraceString
      .split("\n")
      .filter(_.contains("chronon"))
      .map(_.replace("at ai.chronon.spark.test.", "").replace("at ai.chronon.spark.", "").stripLeading())
      .mkString("\n    ")
  }

  def sql(query: String): DataFrame = {
    val parallelism = sparkSession.sparkContext.getConf.getInt("spark.default.parallelism", 1000)
    val coalesceFactor = sparkSession.sparkContext.getConf.getInt("spark.chronon.coalesce.factor", 10)
    val stackTraceString = cleanStackTrace(new Throwable())

    logger.info(s"""
         |  ${"---- running query ----".highlight}
         |
         |${("    " + query.trim.replace("\n", "\n    ")).yellow}
         |
         |  ---- call path ----
         |
         |$stackTraceString
         |
         |  ---- end ----
         |""".stripMargin)
    try {
      // Run the query
      val df = sparkSession.sql(query).coalesce(coalesceFactor * parallelism)
      df
    } catch {
      case e: AnalysisException if e.getMessage.contains(" already exists") =>
        logger.warn(s"Non-Fatal: ${e.getMessage}. Query may result in redefinition.")
        sparkSession.sql("SHOW USER FUNCTIONS")
      case e: Exception =>
        logger.error("Error running query:", e)
        throw e
    }
  }

  def chunk(partitions: Set[String], spec: PartitionSpec = partitionSpec): Seq[PartitionRange] = {
    val sortedDates = partitions.toSeq.sorted
    sortedDates.foldLeft(Seq[PartitionRange]()) { (ranges, nextDate) =>
      if (ranges.isEmpty || spec.after(ranges.last.end) != nextDate) {
        ranges :+ PartitionRange(nextDate, nextDate)(spec)
      } else {
        val newRange = PartitionRange(ranges.last.start, nextDate)(spec)
        ranges.dropRight(1) :+ newRange
      }
    }
  }

  def unfilledRanges(outputTable: String,
                     outputPartitionRange: PartitionRange,
                     inputTables: Option[Seq[String]] = None,
                     // ------- TODO: CLEANUP --------
                     inputTableToSubPartitionFiltersMap: Map[String, Map[String, String]] = Map.empty,
                     inputToOutputShift: Int = 0,
                     skipFirstHole: Boolean = true,
                     inputPartitionSpecs: Seq[PartitionSpec] = Seq(partitionSpec)

                     // ------- TODO: CLEANUP --------
  ): Option[Seq[PartitionRange]] = {

    val validPartitionRange = if (outputPartitionRange.start == null) { // determine partition range automatically
      val inputStart = inputTables.flatMap(
        _.map(table =>
          firstAvailablePartition(table,
                                  outputPartitionRange.partitionSpec,
                                  Option(outputPartitionRange),
                                  inputTableToSubPartitionFiltersMap.getOrElse(table, Map.empty))).min)
      require(
        inputStart.isDefined,
        s"""Either partition range needs to have a valid start or
           |an input table with valid data needs to be present
           |inputTables: $inputTables, partitionRange: $outputPartitionRange
           |""".stripMargin
      )

      // firstAvailablePartition normalizes grid-matching ds values to the global format, so
      // the shift must use the spec the value actually arrives in
      val autoSpec =
        if (outputPartitionRange.partitionSpec.hasSameGrid(partitionSpec)) partitionSpec
        else outputPartitionRange.partitionSpec
      PartitionRange(autoSpec.shiftPartitions(inputStart.get, inputToOutputShift),
                     autoSpec.normalizeStart(outputPartitionRange.end, outputPartitionRange.partitionSpec))(autoSpec)
    } else {

      outputPartitionRange
    }

    // Heterogeneous-partition joins arrive with validPartitionRange in the left source's
    // spec (e.g. yyyyMMdd) while outputExisting is read in the default spec (yyyy-MM-dd).
    // Without canonicalizing first, the set-diffs below see zero overlap and silently
    // collapse the join's compute range to nothing.
    // Ranges on a different grid are NOT canonicalized: translating into a coarser global
    // spec would floor their ds values and collapse distinct partitions - they stay in their
    // own spec.
    val canonicalRange =
      if (validPartitionRange.partitionSpec == partitionSpec) validPartitionRange
      else if (validPartitionRange.partitionSpec.hasSameGrid(partitionSpec))
        validPartitionRange.intersectingRange(partitionSpec)
      else validPartitionRange

    // all partition arithmetic below happens in this spec
    val workingSpec = canonicalRange.partitionSpec

    val outputExisting =
      if (workingSpec.hasSameGrid(partitionSpec)) partitions(outputTable)
      else partitions(outputTable, tablePartitionSpec = Some(workingSpec))
    // To avoid recomputing partitions removed by retention mechanisms we will not fill holes in the very beginning of the range
    // If a user fills a new partition in the newer end of the range, then we will never fill any partitions before that range.
    // We instead log a message saying why we won't fill the earliest hole.
    val cutoffPartition = if (outputExisting.nonEmpty) {
      Format.pickMaxPartition(Seq(outputExisting.min, canonicalRange.start)).getOrElse(canonicalRange.start)
    } else {
      canonicalRange.start
    }

    val fillablePartitions =
      if (skipFirstHole) {
        canonicalRange.partitions.toSet.filter(_ >= cutoffPartition)
      } else {
        canonicalRange.partitions.toSet
      }

    val outputMissing = fillablePartitions -- outputExisting

    val existingInputPartitions =
      for (
        inputTables <- inputTables.toSeq;
        inputPartitionSpec <- inputPartitionSpecs;
        table <- inputTables;
        subPartitionFilters = inputTableToSubPartitionFiltersMap.getOrElse(table, Map.empty);
        // List in the input table's spec; output ds filters can exclude coarser input partitions.
        listed = partitions(table,
                            subPartitionFilters,
                            Option(outputPartitionRange.intersectingRange(inputPartitionSpec)),
                            tablePartitionSpec = Some(inputPartitionSpec));
        listedSpec = if (inputPartitionSpec.hasSameGrid(partitionSpec)) partitionSpec else inputPartitionSpec;
        contained <- PartitionRange.fullyContainedPartitions(listed, listedSpec, workingSpec)
      ) yield {
        workingSpec.shiftPartitions(contained, inputToOutputShift)
      }

    val inputMissing = inputTables
      .map(_ => fillablePartitions -- existingInputPartitions)
      .getOrElse(Set.empty)

    val missingPartitions = outputMissing -- inputMissing
    val missingChunks = chunk(missingPartitions, workingSpec)

    logger.info(s"""
               |Unfilled range computation:
               |   Output table: $outputTable
               |   Missing output partitions: ${outputMissing.toSeq.sorted.prettyInline}
               |   Input tables: ${inputTables.getOrElse(Seq("None")).mkString(", ")}
               |   Missing input partitions: ${inputMissing.toSeq.sorted.prettyInline}
               |   Unfilled Partitions: ${missingPartitions.toSeq.sorted.prettyInline}
               |   Unfilled ranges: ${missingChunks.sorted.mkString("")}
               |""".stripMargin)

    if (missingPartitions.isEmpty) return None
    Some(missingChunks)
  }

  // Needs provider
  def getTableProperties(tableName: String): Option[Map[String, String]] = {
    if (!tableReachable(tableName)) {
      return None
    }
    try {
      val propertiesDf = sparkSession.sql(s"SHOW TBLPROPERTIES $tableName")
      val properties = propertiesDf.collect().map(row => row.getString(0) -> row.getString(1)).toMap
      Some(properties)
    } catch {
      case _: Exception => None
    }
  }

  // Needs provider
  private def dropTableIfExists(tableName: String): Unit = {
    val command = s"DROP TABLE IF EXISTS $tableName"
    logger.info(s"Dropping table with command: $command")
    sql(command)
  }

  def renameTable(srcTable: String, destTable: String): Unit = {
    val command = Format.renameTableSql(srcTable, destTable)(sparkSession)
    logger.info(s"Renaming table with command: $command")
    sql(command)
  }

  def archiveOrDropTableIfExists(tableName: String, timestamp: Option[Instant]): Option[String] = {
    val archiveTry = Try(archiveTableIfExists(tableName, timestamp))
    archiveTry match {
      case Success(archivedTableOpt) => archivedTableOpt
      case Failure(e) =>
        logger.info(s"""Fail to archive table $tableName
           |${e.getMessage}
           |Proceed to dropping the table instead.
           |""".stripMargin)
        dropTableIfExists(tableName)
        None
    }
  }

  // Needs provider
  private def archiveTableIfExists(tableName: String, timestamp: Option[Instant]): Option[String] = {
    if (tableReachable(tableName)) {
      val humanReadableTimestamp = archiveTimestampFormatter.format(timestamp.getOrElse(Instant.now()))
      val finalArchiveTableName = s"${tableName}_$humanReadableTimestamp"
      renameTable(tableName, finalArchiveTableName)
      Some(finalArchiveTableName)
    } else {
      None
    }
  }

  def dropTableOnSchemaChange(tableName: String, incomingDf: DataFrame): Unit = {
    if (!tableReachable(tableName)) return

    val existingSchema = loadTable(tableName).schema
    val existingSchemaMap = existingSchema
      .map(f => f.name -> f.dataType)
      .toMap

    val incomingSchemaMap = incomingDf.schema
      .map(f => f.name -> f.dataType)
      .toMap

    val existingCols = existingSchemaMap.keySet
    val incomingCols = incomingDf.columns.toSet

    val addedCols = incomingCols -- existingCols
    val removedCols = existingCols -- incomingCols

    val updatedCols = incomingCols
      .intersect(existingCols)
      .flatMap { col =>
        val existingType = existingSchemaMap(col)
        val incomingType = incomingSchemaMap(col)

        if (existingType != incomingType) {
          Some(s"$col: ${existingType.catalogString}  -->  ${incomingType.catalogString}")
        } else {
          None
        }

      }
      .toSeq

    logger.info(s"""
         |--- Table Archival Check ----
         |
         |incoming schema:
         |  ${incomingDf.schema.catalogString}
         |existing schema:
         |  ${existingSchema.catalogString}
         |
         |added columns:
         |  $addedCols
         |removed columns:
         |  $removedCols
         |updated columns:
         |  ${updatedCols.mkString("\n  ")}
         |
         |""".stripMargin)

    if (addedCols.nonEmpty || removedCols.nonEmpty || updatedCols.nonEmpty) {
      dropTableIfExists(tableName)
    }

  }

  /*
   * This method detects new columns that appear in newSchema but not in current table,
   * and append those new columns at the end of the existing table. This allows continuous evolution
   * of a Hive table without dropping or archiving data.
   *
   * Warning: ALTER TABLE behavior also depends on underlying storage solution.
   * To read using Hive, which differentiates Table-level schema and Partition-level schema, it is required to
   * take an extra step to sync Table-level schema into Partition-level schema in order to read updated data
   * in Hive. To read from Spark, this is not required since it always uses the Table-level schema.
   */
  private def expandTable(tableName: String, newSchema: StructType): Unit = {

    val existingSchema = getSchemaFromTable(tableName)
    val existingFieldsMap = existingSchema.fields.map(field => (field.name, field)).toMap

    val inconsistentFields = mutable.ListBuffer[(String, DataType, DataType)]()
    val newFields = mutable.ListBuffer[StructField]()

    newSchema.fields.foreach(field => {
      val fieldName = field.name
      if (existingFieldsMap.contains(fieldName)) {
        val existingDataType = existingFieldsMap(fieldName).dataType

        // compare on catalogString so that we don't check nullability which is not relevant for hive tables
        if (existingDataType.catalogString != field.dataType.catalogString) {
          inconsistentFields += ((fieldName, existingDataType, field.dataType))
        }
      } else {
        newFields += field
      }
    })

    if (inconsistentFields.nonEmpty) {
      throw IncompatibleSchemaException(inconsistentFields.toSeq)
    }

    val newFieldDefinitions = newFields.map(newField => newField.toDDL)
    val expandTableQueryOpt = if (newFieldDefinitions.nonEmpty) {
      val tableLevelAlterSql =
        s"""ALTER TABLE $tableName
           |ADD COLUMNS (
           |    ${newFieldDefinitions.mkString(",\n    ")}
           |)
           |""".stripMargin

      Some(tableLevelAlterSql)
    } else {
      None
    }

    /* check if any old columns are skipped in new field and send warning */
    val updatedFieldsMap = newSchema.fields.map(field => (field.name, field)).toMap
    val excludedFields = existingFieldsMap.filter { case (name, _) =>
      !updatedFieldsMap.contains(name)
    }.toSeq

    if (excludedFields.nonEmpty) {
      val excludedFieldsStr =
        excludedFields.map(tuple => s"columnName: ${tuple._1} dataType: ${tuple._2.dataType.catalogString}")
      logger.info(
        s"""Warning. Detected columns that exist in Hive table but not in updated schema. These are ignored in DDL.
           |${excludedFieldsStr.mkString("\n")}
           |""".stripMargin)
    }

    if (expandTableQueryOpt.nonEmpty) {
      sql(expandTableQueryOpt.get)

      // set a flag in table props to indicate that this is a dynamic table
      sql(CreationUtils.alterTablePropertiesSql(tableName, Map(Constants.ChrononDynamicTable -> true.toString)))
    }
  }

  private def andPredicates(predicates: Seq[String]): String = {
    val whereStr = predicates.map(p => s"($p)").mkString(" AND ")
    logger.info(s"""Where str: $whereStr""")
    whereStr
  }

  def scanDfBase(selectMap: Map[String, String],
                 table: String,
                 wheres: Seq[String],
                 rangeWheres: Seq[String],
                 fallbackSelects: Option[Map[String, String]] = None): DataFrame = {

    val selects = QueryUtils.buildSelects(selectMap, fallbackSelects)

    logger.info(s""" Scanning data:
                   |  table: ${table.green}
                   |  selects:
                   |    ${selects.mkString("\n    ").green}
                   |  wheres:
                   |    ${wheres.mkString(",\n    ").green}
                   |  partition filters:
                   |    ${rangeWheres.mkString(",\n    ").green}
                   |""".stripMargin)

    var df = loadTable(table, rangeWheres)

    if (selects.nonEmpty) df = df.selectExpr(selects: _*)

    if (wheres.nonEmpty) {
      val whereStr = andPredicates(wheres)
      df = df.where(whereStr)
    }

    val parallelism = sparkSession.sparkContext.getConf.getInt("spark.default.parallelism", 1000)
    val coalesceFactor = sparkSession.sparkContext.getConf.getInt("spark.chronon.coalesce.factor", 10)

    // TODO: this is a temporary fix to handle the case where the partition column is not a string.
    //  This is the case for partitioned BigQuery native tables.
    // String ds values must pass through untouched: date_format would round-trip them through
    // an implicit string->timestamp cast, which nulls any value Spark can't natively cast (e.g.
    // the dash-separated sub-daily formats; the legacy space/colon format only survived the
    // round trip because it coincides with Spark's native timestamp literal shape).
    (if (df.schema.fieldNames.contains(partitionColumn) && df.schema(partitionColumn).dataType != StringType) {
       df.withColumn(partitionColumn,
                     gridPartitionColumn(df.col(partitionColumn), df.schema(partitionColumn).dataType, partitionSpec))
     } else {
       df
     }).coalesce(coalesceFactor * parallelism)
  }

  /** The ds of the grid partition containing each value of a timestamp-bearing column: floored
    * to the spec's partitionInterval+partitionOffset before formatting. Plain date_format only
    * truncates to the format's granularity, so a 3h grid with a minute-bearing format would emit
    * per-minute values (…-13-47) instead of grid boundaries (…-12-00). Grid intervals are whole
    * minutes, so seconds-domain arithmetic is exact. Numeric columns hold epoch MILLIS by
    * convention - casting them through TimestampType would reinterpret millis as seconds.
    */
  private def gridPartitionColumn(c: Column, dataType: DataType, spec: PartitionSpec): Column = {
    val seconds: Column = dataType match {
      case _: NumericType => (c.cast(LongType) / 1000).cast(LongType)
      case _              => c.cast(TimestampType).cast(LongType)
    }
    val spanSeconds = spec.spanMillis / 1000
    val offsetSeconds = Math.floorMod(spec.offsetMillis, spec.spanMillis) / 1000
    val floored = seconds - pmod(seconds - lit(offsetSeconds), lit(spanSeconds))
    from_unixtime(floored, spec.format)
  }

  def whereClauses(
      range: PartitionRange,
      partitionColumn: Option[String] = None
  ): Seq[String] = {
    val col = partitionColumn.getOrElse(this.partitionColumn)
    // Use >= start and < next(end) which works for both partitioned and time-partitioned tables.
    // For partitioned tables with daily partitions (e.g., ds='2024-01-10'), this is equivalent
    // to <= end since there are no values between discrete partition dates.
    val startClause = Option(range.start).map(s => s"$col >= '$s'")
    val endClause = Option(range.end).map(e => s"$col < '${range.partitionSpec.after(e)}'")
    (startClause ++ endClause).toSeq
  }

  /** Range predicates typed to the column. ds literals only compare correctly against
    * STRING columns: against TIMESTAMP/DATE columns Spark casts the literal, and sub-daily
    * dash values (yyyy-MM-dd-HH-mm) cast to NULL - the filter then drops every row and the
    * scan is silently empty. Timestamp-bearing columns get epoch bounds computed Scala-side
    * instead; the end bound is the exclusive end of the range's last interval
    * (rerun-deterministic: a job for [13:00, 16:00) never reads rows that landed after 16:00).
    * Numeric columns hold epoch MILLIS by convention - never cast them through TimestampType
    * (seconds, 1000x off).
    */
  def typedWhereClauses(range: PartitionRange, columnName: String, columnType: DataType): Seq[String] = {
    def bounds(render: Long => String): Seq[String] = {
      val startClause = Option(range.start).map { s =>
        s"$columnName >= ${render(range.partitionSpec.partitionStartMillis(s))}"
      }
      val endClause = Option(range.end).map { e =>
        s"$columnName < ${render(range.partitionSpec.partitionEndMillis(e))}"
      }
      (startClause ++ endClause).toSeq
    }

    columnType match {
      case StringType     => whereClauses(range, Some(columnName))
      case _: NumericType => bounds(millis => s"${millis}L")
      case _              => bounds(millis => s"timestamp_millis(${millis}L)")
    }
  }

  /** Column type of `columnName` in `table` (StringType when absent); metadata-only, no scan. */
  def partitionColumnType(table: String, columnName: String): DataType =
    Try(loadTable(table).schema).toOption
      .flatMap(schema => schema.fields.find(_.name == columnName))
      .map(_.dataType)
      .getOrElse(StringType)

  /** Like whereClauses but resolves the column's actual type from the table schema first. */
  def rangeWheresFor(range: PartitionRange, table: String, columnName: String): Seq[String] =
    typedWhereClauses(range, columnName, partitionColumnType(table, columnName))

  def scanDf(query: Query,
             table: String,
             fallbackSelects: Option[Map[String, String]] = None,
             range: Option[PartitionRange] = None): DataFrame = {

    val maybeQuery = Option(query)

    val queryPartitionColumn = maybeQuery.flatMap(q => Option(q.partitionColumn)).getOrElse(partitionColumn)

    val rangeWheres = range
      .map(r => rangeWheresFor(r, table, queryPartitionColumn))
      .getOrElse(Seq.empty)

    val queryWheres = maybeQuery.flatMap(q => Option(q.wheres)).map(_.toScala).getOrElse(Seq.empty)
    val wheres: Seq[String] = rangeWheres ++ queryWheres
    val selects = maybeQuery.flatMap(q => Option(q.selects)).map(_.toScala).getOrElse(Map.empty)

    val scanDf = scanDfBase(selects, table, wheres, rangeWheres, fallbackSelects)

    if (queryPartitionColumn != partitionColumn) {
      val renamed = scanDf.withColumnRenamed(queryPartitionColumn, partitionColumn)
      // If the partition column is not a string (e.g. timestamp/date), convert to grid ds values
      val colType = renamed.schema(partitionColumn).dataType
      if (colType != StringType) {
        renamed.withColumn(partitionColumn, gridPartitionColumn(col(partitionColumn), colType, partitionSpec))
      } else {
        renamed
      }
    } else {
      scanDf
    }
  }
}

object TableUtils {
  def apply(sparkSession: SparkSession) = new TableUtils(sparkSession)
  def apply(sparkSession: SparkSession, partitionSpec: PartitionSpec) =
    new TableUtils(sparkSession, Some(partitionSpec))
}

sealed case class IncompatibleSchemaException(inconsistencies: Seq[(String, DataType, DataType)]) extends Exception {
  override def getMessage: String = {
    val inconsistenciesStr =
      inconsistencies.map(tuple => s"columnName: ${tuple._1} existingType: ${tuple._2} newType: ${tuple._3}")
    s"""Existing columns cannot be modified:
       |${inconsistenciesStr.mkString("\n")}
       |""".stripMargin
  }
}
