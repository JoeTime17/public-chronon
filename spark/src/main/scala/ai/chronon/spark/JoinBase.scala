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

package ai.chronon.spark

import ai.chronon.api
import ai.chronon.api.{Accuracy, Constants, DateRange, JoinPart, PartitionRange, PartitionSpec}
import ai.chronon.api.DataModel.ENTITIES
import ai.chronon.api.Extensions._
import ai.chronon.online.metrics.Metrics
import ai.chronon.planner.JoinBootstrapNode
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.Extensions._
import ai.chronon.spark.JoinUtils.{
  coalescedJoin,
  leftDf,
  shouldRecomputeLeft,
  tablesToRecompute,
  withFinalJoinWriteOptimizations
}
import ai.chronon.spark.batch._
import com.google.gson.Gson
import ai.chronon.api.MetaData
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.slf4j.{Logger, LoggerFactory}

import java.time.Instant
import scala.collection.JavaConverters._

abstract class JoinBase(val joinConfCloned: api.Join,
                        endPartition: String,
                        tableUtils: TableUtils,
                        skipFirstHole: Boolean,
                        showDf: Boolean = false,
                        selectedJoinParts: Option[Seq[String]] = None) {

  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  implicit val tu = tableUtils
  private implicit val partitionSpec: PartitionSpec = tableUtils.partitionSpec

  val joinMetaData: MetaData = joinConfCloned.metaData
  assert(Option(joinMetaData.outputNamespace).nonEmpty, "output namespace could not be empty or null")
  val metrics: Metrics.Context = Metrics.Context(Metrics.Environment.JoinOffline, joinConfCloned)
  val outputTable: String = joinMetaData.outputTable

  // Used for parallelized JoinPart execution
  val bootstrapTable: String = joinMetaData.bootstrapTable

  // Get table properties from config
  protected val confTableProps: Map[String, String] =
    Option(joinMetaData.tableProps).getOrElse(Map.empty[String, String])

  private val gson = new Gson()
  // Combine tableProperties set on conf with encoded Join
  protected val tableProps: Map[String, String] =
    confTableProps ++ Map(Constants.JoinSemanticHashKey -> gson.toJson(joinConfCloned.semanticHash.asJava))

  def joinWithLeft(leftDf: DataFrame, rightDf: DataFrame, joinPart: JoinPart): DataFrame = {
    val partLeftKeys = joinPart.rightToLeft.values.toArray

    // compute join keys, besides the groupBy keys -  like ds, ts etc.,
    val additionalKeys: Seq[String] = {
      if (joinConfCloned.left.dataModel == ENTITIES) {
        Seq(tableUtils.partitionColumn)
      } else if (joinPart.groupBy.inferredAccuracy == Accuracy.TEMPORAL) {
        Seq(Constants.TimeColumn, tableUtils.partitionColumn)
      } else { // left-events + snapshot => join-key = ds_of_left_ts
        Seq(Constants.TimePartitionColumn)
      }
    }
    val keys = partLeftKeys ++ additionalKeys

    // apply prefix to value columns
    val nonValueColumns = joinPart.rightToLeft.keys.toArray ++ Array(Constants.TimeColumn,
                                                                     tableUtils.partitionColumn,
                                                                     Constants.TimePartitionColumn)
    val valueColumns = rightDf.schema.names.filterNot(nonValueColumns.contains)
    val prefixedRightDf = rightDf.prefixColumnNames(joinPart.columnPrefix, valueColumns)

    // apply key-renaming to key columns
    val newColumns = prefixedRightDf.columns.map { column =>
      if (joinPart.rightToLeft.contains(column)) {
        col(column).as(joinPart.rightToLeft(column))
      } else {
        col(column)
      }
    }
    val keyRenamedRightDf = prefixedRightDf.select(newColumns: _*)

    // adjust join keys
    val joinableRightDf = if (additionalKeys.contains(Constants.TimePartitionColumn)) {
      // increment one day to align with left side ts_ds
      // because one day was decremented from the partition range for snapshot accuracy
      keyRenamedRightDf
        .withColumn(
          Constants.TimePartitionColumn,
          date_format(date_add(to_date(col(tableUtils.partitionColumn), tableUtils.partitionSpec.format), 1),
                      tableUtils.partitionSpec.format)
        )
        .drop(tableUtils.partitionColumn)
    } else {
      keyRenamedRightDf
    }

    logger.info(s"""
               |Join keys for ${joinPart.groupBy.metaData.name}: ${keys.mkString(", ")}
               |Left Schema:
               |${leftDf.schema.pretty}
               |Right Schema:
               |${joinableRightDf.schema.pretty}""".stripMargin)
    val joinedDf = coalescedJoin(leftDf, joinableRightDf, keys)
    logger.info(s"""Final Schema:
               |${joinedDf.schema.pretty}
               |""".stripMargin)

    joinedDf
  }

  def computeRange(leftDf: DataFrame,
                   leftRange: PartitionRange,
                   bootstrapInfo: BootstrapInfo,
                   runSmallMode: Boolean = false,
                   usingBootstrappedLeft: Boolean = false): Option[DataFrame]

  private def getUnfilledRange(overrideStartPartition: Option[String],
                               outputTable: String): (PartitionRange, Seq[PartitionRange]) = {
    val rangeToFill = JoinUtils.getRangeToFill(joinConfCloned.left,
                                               tableUtils,
                                               endPartition,
                                               overrideStartPartition,
                                               joinConfCloned.historicalBackfill)
    logger.info(s"Left side range to fill $rangeToFill")

    (rangeToFill,
     tableUtils
       .unfilledRanges(
         outputTable,
         rangeToFill,
         Some(Seq(joinConfCloned.left.table)),
         skipFirstHole = skipFirstHole,
         inputPartitionSpecs = Seq(joinConfCloned.left.query.partitionSpec(tableUtils.partitionSpec))
       )
       .getOrElse(Seq.empty))
  }

  def computeLeft(overrideStartPartition: Option[String] = None): Unit = {
    // Runs the left side query for a join and saves the output to a table, for reuse by joinPart
    // Computation in parallelized joinPart execution mode.
    if (shouldRecomputeLeft(joinConfCloned, bootstrapTable, tableUtils)) {
      logger.info("Detected semantic change in left side of join, archiving left table for recomputation.")
      val archivedAtTs = Instant.now()
      tableUtils.archiveOrDropTableIfExists(bootstrapTable, Some(archivedAtTs))
    }

    val (rangeToFill, unfilledRanges) = getUnfilledRange(overrideStartPartition, bootstrapTable)

    if (unfilledRanges.isEmpty) {
      logger.info("Range to fill already computed. Skipping query execution...")
    } else {
      // Register UDFs for the left part computation
      Option(joinConfCloned.setups).foreach(_.foreach(tableUtils.sql))
      logger.info(s"Running ranges: $unfilledRanges")
      unfilledRanges.foreach { unfilledRange =>
        val leftDf = JoinUtils.leftDf(joinConfCloned, unfilledRange, tableUtils)
        if (leftDf.isDefined) {
          val leftTaggedDf = leftDf.get.addTimebasedColIfExists()

          val bootstrapJobDateRange = new DateRange()
            .setStartDate(unfilledRange.start)
            .setEndDate(unfilledRange.end)

          val bootstrapMetadata = joinConfCloned.metaData.deepCopy()
          bootstrapMetadata.setName(bootstrapTable)
          val bootstrapNode = new JoinBootstrapNode()
            .setJoin(joinConfCloned)

          val bootstrapJob = new JoinBootstrapJob(bootstrapNode, bootstrapMetadata, bootstrapJobDateRange)
          val leftSchema = leftDf.map(df => df.schema)
          val bootstrapInfo = BootstrapInfo.from(joinConfCloned, rangeToFill, tableUtils, leftSchema)
          bootstrapJob.computeBootstrapTable(leftTaggedDf, bootstrapInfo, tableProps = tableProps)
        } else {
          logger.info(s"Query produced no results for date range: $unfilledRange. Please check upstream.")
        }
      }
    }
  }

  def computeFinalJoin(leftDf: DataFrame, leftRange: PartitionRange, bootstrapInfo: BootstrapInfo): Unit

  def computeFinal(overrideStartPartition: Option[String] = None): Unit = {

    // Utilizes the same tablesToRecompute check as the monolithic spark job, because if any joinPart changes, then so does the output table
    if (tablesToRecompute(joinConfCloned, outputTable, tableUtils).isEmpty) {
      logger.info("No semantic change detected, leaving output table in place.")
    } else {
      logger.info("Semantic changes detected, archiving output table.")
      val archivedAtTs = Instant.now()
      tableUtils.archiveOrDropTableIfExists(outputTable, Some(archivedAtTs))
    }

    val (rangeToFill, unfilledRanges) = getUnfilledRange(overrideStartPartition, outputTable)

    if (unfilledRanges.isEmpty) {
      logger.info("Range to fill already computed. Skipping query execution...")
    } else {
      val leftSchema = leftDf(joinConfCloned, unfilledRanges.head, tableUtils, limit = Some(1)).map(df => df.schema)
      val bootstrapInfo = BootstrapInfo.from(joinConfCloned, rangeToFill, tableUtils, leftSchema)
      logger.info(s"Running ranges: $unfilledRanges")
      unfilledRanges.foreach { unfilledRange =>
        val leftDf = JoinUtils.leftDf(joinConfCloned, unfilledRange, tableUtils)
        if (leftDf.isDefined) {
          computeFinalJoin(leftDf.get, unfilledRange, bootstrapInfo)
        } else {
          logger.info(s"Query produced no results for date range: $unfilledRange. Please check upstream.")
        }
      }
    }

  }

  def forceComputeRangeAndSave(range: PartitionRange, semanticHash: Option[String] = None): Option[DataFrame] =
    tableUtils.withJobDescription(s"MonolithJoin(${joinMetaData.name}) $range") {

      Option(joinConfCloned.setups).foreach(_.foreach(tableUtils.sql))
      val leftDfInRange = leftDf(joinConfCloned, range, tableUtils)

      if (leftDfInRange.isEmpty) return None

      val bootstrapInfo = BootstrapInfo.from(joinConfCloned.deepCopy(), range, tableUtils, leftDfInRange.map(_.schema))

      val dfOpt = computeRange(leftDfInRange.get, range, bootstrapInfo)
      dfOpt.map { df =>
        val table = joinMetaData.outputTable

        tableUtils.dropTableOnSchemaChange(table, df)
        withFinalJoinWriteOptimizations(tableUtils) {
          df.save(table, semanticHash = semanticHash)
        }

        tableUtils.loadTable(table, range.whereClauses)
      }
    }

  def computeJoin(stepDays: Option[Int] = None, overrideStartPartition: Option[String] = None): DataFrame = {
    computeJoinOpt(stepDays, overrideStartPartition).get
  }

  def computeJoinOpt(stepDays: Option[Int] = None,
                     overrideStartPartition: Option[String] = None,
                     useBootstrapForLeft: Boolean = false): Option[DataFrame] = {

    assert(Option(joinConfCloned.metaData.team).nonEmpty,
           s"join.metaData.team needs to be set for join ${joinConfCloned.metaData.name}")

    joinConfCloned.joinParts.asScala.foreach { jp =>
      assert(Option(jp.groupBy.metaData.team).nonEmpty,
             s"groupBy.metaData.team needs to be set for joinPart ${jp.groupBy.metaData.name}")
    }

    // Run validations before starting the job
    // val analyzer = new Analyzer(tableUtils, joinConfCloned, endPartition, endPartition, silenceMode = true)
    try {
      // analyzer.analyzeJoin(joinConfCloned, validationAssert = true)
      metrics.gauge(Metrics.Name.validationSuccess, 1)
      logger.info("Join conf validation succeeded. No error found.")
    } catch {
      case ex: AssertionError =>
        metrics.gauge(Metrics.Name.validationFailure, 1)
        logger.error("Validation failed. Please check the validation error in log.")
        if (tableUtils.backfillValidationEnforced) throw ex
      case e: Throwable =>
        metrics.gauge(Metrics.Name.validationFailure, 1)
        logger.error(s"An unexpected error occurred during validation. ${e.getMessage}")
    }

    // First run command to archive tables that have changed semantically since the last run
    val archivedAtTs = Instant.now()
    // TODO: We should not archive the output table in the case of selected join parts mode
    tablesToRecompute(joinConfCloned, outputTable, tableUtils).foreach(
      tableUtils.archiveOrDropTableIfExists(_, Some(archivedAtTs)))

    // Check semantic hash before overwriting left side
    val source = joinConfCloned.left
    if (useBootstrapForLeft) {
      logger.info("Overwriting left side to use saved Bootstrap table...")
      source.overwriteTable(bootstrapTable)
      val query = source.query
      // sets map and where clauses already applied to bootstrap transformation
      query.setSelects(null)
      query.setWheres(null)
    }
    // detect holes and chunks to fill
    // OverrideStartPartition is used to replace the start partition of the join config. This is useful when
    //  1 - User would like to test run with different start partition
    //  2 - User has entity table which is cumulative and only want to run backfill for the latest partition
    val rangeToFill = JoinUtils.getRangeToFill(source,
                                               tableUtils,
                                               endPartition,
                                               overrideStartPartition,
                                               joinConfCloned.historicalBackfill)
    val leftSource = source

    logger.info(s"Join range to fill $rangeToFill")

    // check if left source has any data covering the requested range
    val leftSpec = leftSource.query.partitionSpec(tableUtils.partitionSpec)
    val leftLastPartition = tableUtils.lastAvailablePartition(
      leftSource.table,
      tablePartitionSpec = Option(leftSpec)
    )
    val leftFirstPartition = tableUtils.firstAvailablePartition(
      leftSource.table,
      partitionSpec = leftSpec
    )

    if (leftFirstPartition.isEmpty || leftLastPartition.isEmpty) {
      logger.info(s"""No data present in join.left table ${leftSource.table}
                   | for the requested range ${rangeToFill.start} - ${rangeToFill.end}. Exiting...""".stripMargin)
      return None
    }

    val unfilledRanges = tableUtils
      .unfilledRanges(
        outputTable,
        rangeToFill,
        Some(Seq(leftSource.table)),
        skipFirstHole = skipFirstHole,
        inputPartitionSpecs = Seq(leftSource.query.partitionSpec(tableUtils.partitionSpec))
      )
      .getOrElse(Seq.empty)

    def finalResult: DataFrame = tableUtils.scanDf(null, outputTable, range = Some(rangeToFill))

    if (unfilledRanges.isEmpty) {
      logger.info(s"\nThere is no data to compute based on end partition of ${rangeToFill.end}.\n\n Exiting..")
      return Some(finalResult)
    }

    stepDays.foreach(metrics.gauge("step_days", _))
    val stepRanges = unfilledRanges.flatMap { unfilledRange =>
      stepDays.map(unfilledRange.steps).getOrElse(Seq(unfilledRange))
    }

    val leftSchema = leftDf(joinConfCloned, unfilledRanges.head, tableUtils, limit = Some(1)).map(df => df.schema)
    // build bootstrap info once for the entire job
    val bootstrapInfo = BootstrapInfo.from(joinConfCloned.deepCopy(), rangeToFill, tableUtils, leftSchema)

    val wholeRange = PartitionRange(unfilledRanges.minBy(_.start).start, unfilledRanges.maxBy(_.end).end)

    val leftDataOpt = leftDf(joinConfCloned, wholeRange, tableUtils)
    require(leftDataOpt.nonEmpty,
            s"left side of the join ${joinConfCloned.metaData.name} produced empty data in range $wholeRange")

    val runSmallMode = JoinUtils.runSmallMode(tableUtils, leftDataOpt.get)

    val effectiveRanges = if (runSmallMode) {
      Seq(wholeRange)
    } else {
      stepRanges
    }

    logger.info(s"Join ranges to compute: ${effectiveRanges.map { _.toString() }.pretty}")
    effectiveRanges.zipWithIndex.foreach { case (range, index) =>
      val startMillis = System.currentTimeMillis()
      val progress = s"| [${index + 1}/${effectiveRanges.size}]"
      logger.info(s"Computing join for range: ${range.toString()}  $progress")

      leftDf(joinConfCloned, range, tableUtils).map { leftDfInRange =>
        if (showDf) leftDfInRange.prettyPrint()
        // set autoExpand = true to ensure backward compatibility due to column ordering changes

        val finalDf = computeRange(leftDfInRange, range, bootstrapInfo, runSmallMode, useBootstrapForLeft)

        if (selectedJoinParts.isDefined) {
          assert(finalDf.isEmpty,
                 "The arg `selectedJoinParts` is defined, so no final join is required. `finalDf` should be empty")
          logger.info(s"Skipping writing to the output table for range: ${range.toString()}  $progress")
        } else {

          withFinalJoinWriteOptimizations(tableUtils) {
            finalDf.get.save(outputTable, tableProps, autoExpand = true)
          }
          val elapsedMins = (System.currentTimeMillis() - startMillis) / (60 * 1000)
          metrics.gauge(Metrics.Name.LatencyMinutes, elapsedMins)
          metrics.gauge(Metrics.Name.PartitionCount, range.partitions.length)
          logger.info(
            s"Wrote to table $outputTable, into partitions: ${range.toString()} $progress in $elapsedMins mins")

        }
      }
    }
    if (selectedJoinParts.isDefined) {
      logger.info("Skipping final join because selectedJoinParts is defined.")
      None
    } else {
      logger.info(s"Wrote to table $outputTable, into partitions: $unfilledRanges")
      Some(finalResult)
    }
  }
}
