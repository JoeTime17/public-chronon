package ai.chronon.spark.batch

import ai.chronon.api.DataModel.ENTITIES
import ai.chronon.api.Extensions.{DateRangeOps, GroupByOps, JoinPartOps, MetadataOps, SourceOps}
import ai.chronon.api.ScalaJavaConversions.{IterableOps, MapOps}
import ai.chronon.api.planner.RelevantLeftForJoinPart
import ai.chronon.api.{
  Accuracy,
  Constants,
  DataModel,
  DateRange,
  JoinPart,
  MetaData,
  PartitionRange,
  PartitionSpec,
  QueryUtils
}
import ai.chronon.planner.JoinMergeNode
import ai.chronon.spark.Extensions._
import ai.chronon.spark.JoinUtils.{coalescedJoin, leftDf, withFinalJoinWriteOptimizations}
import ai.chronon.spark.JoinUtils
import ai.chronon.spark.catalog.TableUtils
import com.google.gson.Gson
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, date_format, from_unixtime, left, log, unix_timestamp}
import org.apache.spark.sql.types.LongType
import org.slf4j.{Logger, LoggerFactory}

import java.time.Instant

import scala.util.{Failure, Success}

/** Result of analyzing join parts for reuse from production table.
  *
  * @param reuseTable Optional table name to reuse columns from (production or archive table)
  * @param columnsToReuse List of column names that can be reused from the reuse table
  * @param joinPartsToCompute List of join parts that need to be computed (not reusable)
  */
case class JoinPartReuseAnalysis(
    reuseTable: Option[String],
    columnsToReuse: Seq[String],
    joinPartsToCompute: Seq[JoinPart]
)

/*
leftInputTable is either the output of the SourceJob or the output of the BootstrapJob depending on if there are bootstraps or external parts.

joinPartsToTables is a map of JoinPart to the table name of the output of that joinPart job. JoinParts that are being skipped for this range
due to bootstrap can be omitted from this map.
 */

class MergeJob(node: JoinMergeNode, metaData: MetaData, range: DateRange, joinParts: Seq[JoinPart])(implicit
    tableUtils: TableUtils) {

  implicit val partitionSpec: PartitionSpec = tableUtils.partitionSpec

  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  // Processing metadata columns that get dropped in final output
  private val processingColumns = Set(Constants.MatchedHashes, Constants.TimePartitionColumn)
  private val hashExclusionColumn: Set[String] = processingColumns ++ Set(tableUtils.partitionColumn)

  private val colHashTablePropsKey = "column_hashes"

  private val join = node.join
  private val leftInputTable = if (join.bootstrapParts != null || join.onlineExternalParts != null) {
    join.metaData.bootstrapTable
  } else {
    JoinUtils.computeFullLeftSourceTableName(join)
  }
  // Use the node's Join's metadata for output table
  private val outputTable = metaData.outputTable
  private val dateRange = range.toPartitionRange
  private val productionJoin = node.productionJoin
  // If we want, we can modify to just pass the table into the node instead of the whole join
  // We only use productionJoinTableOpt below currently, but additional metadata might be useful eventually
  private val productionJoinTableOpt = Option(productionJoin).map(_.metaData.outputTable)

  val archiveReuseTable = outputTable + Constants.archiveReuseTableSuffix

  def run(): Unit = tableUtils.withJobDescription(s"MergeJob($outputTable) $dateRange") {

    // Always check to see if we need to archive the current output table
    // Occurs when columns are changed/removed/added
    // Computed based on column level semantic hashing that occurs at compile time
    archiveOutputTableIfRequired()

    // This job benefits from a step day of 1 to avoid needing to shuffle on writing output (single partition)
    dateRange.stepsByDays(1).foreach { dayStep =>
      // Scan left input table once to get schema and potentially reuse
      val leftInputDf = tableUtils.scanDf(query = null, table = leftInputTable, range = Some(dayStep))

      // Check if we can reuse columns from production table
      val reuseAnalysis = analyzeJoinPartsForReuse(dayStep, leftInputDf)

      // Get left DataFrame with potentially reused columns from production
      val leftDf = if (reuseAnalysis.reuseTable.isDefined) {
        logger.info(s"Reusing ${reuseAnalysis.columnsToReuse.length} columns (${reuseAnalysis.columnsToReuse
            .mkString(", ")}) from table: ${reuseAnalysis.reuseTable.get}")

        // Select left columns + reused columns from production table
        val leftColumns = leftInputDf.schema.fieldNames.filterNot(processingColumns.contains)
        val columnsToSelect = leftColumns ++ reuseAnalysis.columnsToReuse
        val productionDf = tableUtils.scanDf(query = null, table = reuseAnalysis.reuseTable.get, range = Some(dayStep))

        val selectedDf = productionDf.select(columnsToSelect.map(col): _*)

        // Add back ts_ds column if this is an EVENTS source and the column is missing
        if (join.left.dataModel == DataModel.EVENTS && !selectedDf.columns.contains(Constants.TimePartitionColumn)) {
          selectedDf.withTimeBasedColumn(Constants.TimePartitionColumn, spec = tableUtils.partitionSpec)
        } else {
          selectedDf
        }
      } else {
        leftInputDf
      }

      // Get right parts data only for join parts that need to be computed
      val rightPartsData = getRightPartsData(dayStep, reuseAnalysis.joinPartsToCompute)

      val joinedDfTry =
        try {
          Success(
            rightPartsData
              .foldLeft(leftDf) { case (partialDf, (rightPart, rightDf)) =>
                joinWithLeft(partialDf, rightDf, rightPart)
              }
              // drop all processing metadata columns
              .drop(Constants.MatchedHashes, Constants.TimePartitionColumn))
        } catch {
          case e: Exception =>
            e.printStackTrace()
            Failure(e)
        }

      val tableProps = createTableProperties

      withFinalJoinWriteOptimizations(tableUtils) {
        joinedDfTry.get.save(outputTable, tableProps, autoExpand = true)
      }
    }
  }

  private def createTableProperties: Map[String, String] = {
    val gson = new Gson()
    val existingTableProps = Option(metaData.tableProps).getOrElse(Map.empty[String, String])

    val columnHashesMap = Option(join.metaData.columnHashes)
      .map(hashes => Map(colHashTablePropsKey -> gson.toJson(hashes)))
      .getOrElse {
        logger.warn(s"columnHashes not set for join: ${join.metaData.name}")
        Map.empty[String, String]
      }

    existingTableProps ++ columnHashesMap
  }

  private def getRightPartsData(dayStep: PartitionRange,
                                joinPartsToProcess: Seq[JoinPart] = joinParts): Seq[(JoinPart, DataFrame)] = {
    joinPartsToProcess.map { joinPart =>
      // Use the RelevantLeftForJoinPart utility to get the part table name
      val partTable = RelevantLeftForJoinPart.fullPartTableName(join, joinPart)
      val effectiveRange =
        if (join.left.dataModel == DataModel.EVENTS && joinPart.groupBy.inferredAccuracy == Accuracy.SNAPSHOT) {
          val partSpec = JoinUtils.partSnapshotSpec(joinPart)
          if (partSpec.hasSameGrid(tableUtils.partitionSpec)) {
            // Same-grid snapshot parts keep the historical behavior: read one partition back.
            JoinUtils.snapshotLookbackRange(dayStep, partSpec)
          } else {
            // Cross-grid snapshot part tables are partitioned like the join output and
            // carry the RHS as-of boundary in `ts`.
            dayStep
          }
        } else {
          dayStep
        }
      val wheres = effectiveRange.whereClauses
      val sql = QueryUtils.build(null, partTable, wheres)
      logger.info(s"Pulling data from joinPart table with: $sql")
      (joinPart, tableUtils.scanDfBase(null, partTable, List.empty, wheres, None))
    }.toSeq
  }

  def joinWithLeft(leftDf: DataFrame, rightDf: DataFrame, joinPart: JoinPart): DataFrame = {
    val partLeftKeys = joinPart.rightToLeft.values.toArray
    lazy val partSpec = JoinUtils.partSnapshotSpec(joinPart)
    val leftEventsSnapshot =
      join.left.dataModel == DataModel.EVENTS && joinPart.groupBy.inferredAccuracy == Accuracy.SNAPSHOT
    val crossGridSnapshot = leftEventsSnapshot && !partSpec.hasSameGrid(tableUtils.partitionSpec)

    // compute join keys, besides the groupBy keys -  like ds, ts etc.,
    val additionalKeys: Seq[String] = {
      if (join.left.dataModel == ENTITIES) {
        Seq(tableUtils.partitionColumn)
      } else if (joinPart.groupBy.inferredAccuracy == Accuracy.TEMPORAL) {
        Seq(Constants.TimeColumn, tableUtils.partitionColumn)
      } else { // left-events + snapshot => join-key = ds_of_left_ts
        Seq(Constants.TimePartitionColumn)
      }
    }
    val keys = partLeftKeys ++ additionalKeys

    val prefixedRightDf = MergeJob.prefixJoinPartValueColumns(rightDf, joinPart)

    // apply key-renaming to key columns
    val newColumns = prefixedRightDf.columns.map { column =>
      if (joinPart.rightToLeft.contains(column)) {
        col(column).as(joinPart.rightToLeft(column))
      } else {
        col(column)
      }
    }

    val keyRenamedRightDf = prefixedRightDf.select(newColumns: _*)

    // adjust join keys: the snapshot pick is per row ON THE RHS GROUPBY'S DECLARED GRID - a row
    // at time T reads the latest RHS snapshot whose time is <= T, independent of the
    // join's own grid. Daily RHS under a daily join degenerates to the historical behavior.
    val joinableRightDf = if (crossGridSnapshot) {
      keyRenamedRightDf
        .withColumn(Constants.TimePartitionColumn, col(Constants.TimeColumn).cast(LongType))
        .drop(tableUtils.partitionColumn, Constants.TimeColumn)
        .dropDuplicates(keys)
    } else if (additionalKeys.contains(Constants.TimePartitionColumn)) {
      // snapshot partition p holds the aggregate as of epoch(p) + one RHS partitionInterval;
      // rename to
      // the as-of boundary so it matches the left rows' RHS-grid floor
      keyRenamedRightDf
        .withColumn(
          Constants.TimePartitionColumn,
          date_format(
            from_unixtime(
              unix_timestamp(col(tableUtils.partitionColumn), partSpec.format) +
                partSpec.spanMillis / 1000),
            partSpec.format
          )
        )
        .drop(tableUtils.partitionColumn)
    } else {
      keyRenamedRightDf
    }

    // the left match key is per-joinPart (different parts may live on different RHS grids):
    // re-stamp TimePartitionColumn = floor(left.ts, RHS grid) instead of trusting the shared
    // join-grid column stamped by SourceJob
    val joinableLeftDf = if (crossGridSnapshot) {
      leftDf.withColumn(Constants.TimePartitionColumn,
                        JoinUtils.snapshotGridFloorMillis(col(Constants.TimeColumn), partSpec))
    } else if (additionalKeys.contains(Constants.TimePartitionColumn)) {
      leftDf.withTimeBasedColumn(Constants.TimePartitionColumn, spec = partSpec)
    } else {
      leftDf
    }

    logger.info(s"""
                   |Join keys for ${joinPart.groupBy.metaData.name}: ${keys.mkString(", ")}
                   |Left Schema:
                   |${joinableLeftDf.schema.pretty}
                   |Right Schema:
                   |${joinableRightDf.schema.pretty}""".stripMargin)
    val joinedDf = coalescedJoin(joinableLeftDf, joinableRightDf, keys)
    logger.info(s"""Final Schema:
                   |${joinedDf.schema.pretty}
                   |""".stripMargin)

    joinedDf
  }

  /** Check for columns that have mismatched semantic hashes between two hash maps
    *
    * @param columns The columns to check
    * @param reuseTableColHashes Hash map from the reuse table
    * @param currentColumnHashes Hash map from the current configuration
    * @return Set of columns that have mismatched hashes
    */
  private def findMismatchedHashes(columns: Iterable[String],
                                   reuseTableColHashes: Map[String, String],
                                   currentColumnHashes: Map[String, String]): Set[String] = {
    columns.filter { col =>
      !reuseTableColHashes.contains(col) ||
      !currentColumnHashes.contains(col) ||
      reuseTableColHashes(col) != currentColumnHashes(col)
    }.toSet
  }

  private def archiveOutputTable(): Unit = {
    // If there is an existing _archive_reuse table then rename that to archive_currtime
    // Then rename the current output table to _archive_reuse
    tableUtils.archiveOrDropTableIfExists(archiveReuseTable, Option(Instant.now()))
    logger.info(s"Archiving current output table $outputTable to $archiveReuseTable")
    tableUtils.renameTable(outputTable, archiveReuseTable)
  }

  private def getExistingColHashes(table: String): Map[String, String] = {
    val gson = new Gson()

    val tablePropsColHashValue = tableUtils
      .getTableProperties(table)
      .get(colHashTablePropsKey)

    gson.fromJson(tablePropsColHashValue, classOf[java.util.Map[String, String]]).toScala
  }

  def archiveOutputTableIfRequired(): Unit = {
    // Compares the saved column hashes on the current output table to the config that is being run
    // If a mismatch is found, then archives the current output table to the archive_reuse table
    // archive_reuse is the most recent version of this table. We check it for column reuse.
    val currentColHashes = join.metaData.columnHashes.toScala
    val existingColHashes: Map[String, String] =
      try {
        getExistingColHashes(outputTable)
      } catch {
        case e: Exception =>
          logger.warn(s"Could not get existing column hashes for table $outputTable: ${e.getMessage}")
          return
      }

    logger.info(s"""Checking if table needs to be archived. Comparing
                 |Existing column hashes: $existingColHashes
                 |With current column hashes: $currentColHashes""".stripMargin)

    val existingKeys = existingColHashes.keySet
    val currentKeys = currentColHashes.keySet

    val added = currentKeys.diff(existingKeys)
    val removed = existingKeys.diff(currentKeys)
    val common = existingKeys.intersect(currentKeys)
    val changed = common.filter(k => existingColHashes(k) != currentColHashes(k))

    if (added.nonEmpty || removed.nonEmpty || changed.nonEmpty) {
      logger.info(s"""
           |Archiving table $outputTable due to column changes:
           | Added: ${added.mkString(", ")}
           | Removed: ${removed.mkString(", ")}
           | Changed: ${changed.mkString(", ")}
           | Unchanged: ${common.diff(changed).mkString(", ")}
           |""".stripMargin)
      archiveOutputTable()
    } else {
      logger.info("No differences found in column hashes, not archiving table.")
    }
  }

  /** Analyze join parts to determine which can be reused from production table
    * Returns JoinPartReuseAnalysis containing reuse table, columns to reuse, and join parts to compute
    */
  def analyzeJoinPartsForReuse(dayStep: PartitionRange, currentLeftDf: DataFrame): JoinPartReuseAnalysis = {
    val archiveReuseTable = join.metaData.outputTable + Constants.archiveReuseTableSuffix

    // First we check if archiveReuseTable covers the range
    // Else we check if the production table covers the range
    // And we use the tables in that cascading order
    val reuseTable = if (tableUtils.tableCoversRange(archiveReuseTable, dayStep)) {
      logger.info(s"Archive reuse table $archiveReuseTable covers range, analyzing join parts for reuse")
      archiveReuseTable
    } else if (productionJoinTableOpt.isDefined && tableUtils.tableCoversRange(productionJoinTableOpt.get, dayStep)) {
      // TODO - modify this to be a table instead of the full join
      logger.info(s"Production table $productionJoinTableOpt.get covers range, analyzing join parts for reuse")
      productionJoinTableOpt.get
    } else {
      logger.info("No production or archive reuse table covers the range, proceeding with normal join")
      return JoinPartReuseAnalysis(None, Seq.empty, joinParts)
    }

    // Get the existing column hashes from the production table
    val reuseTableColHashes: Map[String, String] = getExistingColHashes(reuseTable)
    logger.info(s"Found existing column hashes from $reuseTable: $reuseTableColHashes")

    val currentColumnHashes = Option(join.metaData.columnHashes).map(_.toScala) match {
      case Some(hashes) =>
        logger.info(s"Current column hashes: $hashes")
        hashes
      case None =>
        logger.warn(s"No column hashes found in join metadata, proceeding without reuse")
        return JoinPartReuseAnalysis(None, Seq.empty, joinParts)
    }

    // Get reuse table cols once
    val reuseTableColumns =
      try {
        val schema = tableUtils.scanDf(query = null, table = reuseTable, range = Some(dayStep)).schema
        logger.info(s"Reuse table schema: ${schema.pretty}")
        schema.fieldNames.toSet
      } catch {
        case e: Exception =>
          logger.warn(s"Could not get production table schema: ${e.getMessage}")
          return JoinPartReuseAnalysis(None, Seq.empty, joinParts)
      }

    // Check if left schemas are compatible using the production columns we just got
    // Also check semantic hashes to ensure left columns have matching semantics
    val currentLeftColumns = currentLeftDf.schema.fieldNames.toSet -- hashExclusionColumn
    val leftSchemaMismatches = findMismatchedHashes(currentLeftColumns, reuseTableColHashes, currentColumnHashes)
    if (leftSchemaMismatches.nonEmpty) {
      logger.info(
        s"Left columns have mismatched semantic hashes, cannot reuse from production table. Mismatched columns: ${leftSchemaMismatches
            .mkString(", ")}")
      return JoinPartReuseAnalysis(None, Seq.empty, joinParts)
    }

    // Start with empty list - we'll only add base left columns and reusable join part columns
    val columnsToReuse = scala.collection.mutable.ListBuffer[String]()
    val joinPartsToRejoin = scala.collection.mutable.ListBuffer[JoinPart]()

    joinParts.foreach { joinPart =>
      val joinPartGroupByName = joinPart.groupBy.metaData.cleanNameWithoutVersion

      // Get expected output columns for this join part
      val partTable = RelevantLeftForJoinPart.fullPartTableName(join, joinPart)

      // Get join part table schema to determine its value columns
      val partSchema = tableUtils.scanDf(query = null, table = partTable, range = Some(dayStep)).schema
      logger.info(s"Join part $joinPartGroupByName schema: ${partSchema.pretty}")
      val partKeyColumns = joinPart.rightToLeft.keys.toSet ++ Set(Constants.TimeColumn,
                                                                  tableUtils.partitionColumn,
                                                                  Constants.TimePartitionColumn)
      val partValueColumns =
        partSchema.fieldNames.filterNot(partKeyColumns.contains).map(joinPart.columnPrefix + _)

      // Check if all value columns from this join part are present in production table
      // And that the semantic hashes match
      val missingColumns = partValueColumns.filterNot(reuseTableColumns.contains)
      val mismatchedHashes = findMismatchedHashes(partValueColumns, reuseTableColHashes, currentColumnHashes)

      if (missingColumns.isEmpty && mismatchedHashes.isEmpty) {
        logger.info(s"Join part $joinPartGroupByName can be reused from production table")
        columnsToReuse ++= partValueColumns
      } else {
        if (missingColumns.nonEmpty) {
          logger.info(
            s"Join part $joinPartGroupByName cannot be fully reused from production table. Missing columns: ${missingColumns
                .mkString(", ")}"
          )
        }
        if (mismatchedHashes.nonEmpty) {
          logger.info(
            s"Join part $joinPartGroupByName has semantic hash mismatches on columns: ${mismatchedHashes.mkString(", ")}"
          )
        }
        joinPartsToRejoin += joinPart
      }
    }
    if (columnsToReuse.isEmpty) {
      logger.info("No columns can be reused from production table, proceeding with normal join")
      JoinPartReuseAnalysis(None, Seq.empty, joinParts)
    } else {
      logger.info(s"Reusing columns from production table: ${columnsToReuse.mkString(", ")}")
      JoinPartReuseAnalysis(Option(reuseTable), columnsToReuse.toSeq, joinPartsToRejoin.toSeq)
    }
  }
}

object MergeJob {

  def prefixJoinPartValueColumns(rightDf: DataFrame, joinPart: JoinPart)(implicit tableUtils: TableUtils): DataFrame = {
    val nonValueColumns = joinPart.rightToLeft.keys.toArray ++ Array(Constants.TimeColumn,
                                                                     tableUtils.partitionColumn,
                                                                     Constants.TimePartitionColumn)
    val valueColumns = rightDf.schema.names.filterNot(nonValueColumns.contains)
    val prefixedRightDf = rightDf.prefixColumnNames(joinPart.columnPrefix, valueColumns)
    prefixedRightDf
  }

}
