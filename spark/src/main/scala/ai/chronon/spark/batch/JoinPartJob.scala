package ai.chronon.spark.batch

import ai.chronon.api.DataModel.{ENTITIES, EVENTS}
import ai.chronon.api.Extensions.{DateRangeOps, DerivationOps, GroupByOps, JoinPartOps, MetadataOps, TableInfoOps}
import ai.chronon.api.PartitionRange.toTimeRange
import ai.chronon.api._
import ai.chronon.online.metrics.Metrics
import ai.chronon.planner.JoinPartNode
import ai.chronon.spark.Extensions._
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.join.UnionJoin
import ai.chronon.spark.{GroupBy, JoinUtils}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, date_format, explode, lit, udf, unix_timestamp}
import org.apache.spark.sql.types.{LongType, StringType}
import org.apache.spark.util.sketch.BloomFilter
import org.slf4j.{Logger, LoggerFactory}

import java.util
import scala.collection.Map
import scala.jdk.CollectionConverters._

case class JoinPartJobContext(leftDf: Option[DfWithStats],
                              joinLevelBloomMapOpt: Option[util.Map[String, BloomFilter]],
                              tableProps: Map[String, String],
                              runSmallMode: Boolean)

// alignOutput forces the job to produce the partitions specified by range.
// legacy behavior was to not align, but that prevents from partition aware orchestration
class JoinPartJob(node: JoinPartNode,
                  metaData: MetaData,
                  range: DateRange,
                  showDf: Boolean = false,
                  alignOutput: Boolean = false)(implicit tableUtils: TableUtils) {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  implicit val partitionSpec: PartitionSpec = tableUtils.partitionSpec

  private val leftTable = node.leftSourceTable
  private val joinPart = node.joinPart
  private val dateRange = range.toPartitionRange

  // Snapshot-accuracy parts compute at the RHS groupBy's declared grid (the join grid when
  // nothing is declared). Physical part-table partitions still use tableUtils.partitionSpec.
  private val partSnapshotSpec: PartitionSpec = {
    val joinSpec = tableUtils.partitionSpec
    val planned = Option(joinPart.groupBy.metaData)
      .filter(md => Option(md.executionInfo).flatMap(ei => Option(ei.outputTableInfo)).exists(_.isSetPartitionInterval))
      .map(_ => joinPart.groupBy.partitionSpec(joinSpec))

    val resolved = planned.getOrElse(JoinUtils.partSnapshotSpec(joinPart))
    if (resolved.hasSameGrid(joinSpec)) joinSpec else resolved.copy(column = joinSpec.column)
  }
  private lazy val snapshotTableUtils: TableUtils =
    if (partSnapshotSpec == tableUtils.partitionSpec) tableUtils
    else TableUtils(tableUtils.sparkSession, partSnapshotSpec)
  private val skewKeys: Option[Map[String, Seq[String]]] = Option(node.skewKeys).map { skewKeys =>
    skewKeys.asScala.map { case (k, v) => k -> v.asScala.toSeq }.toMap
  }

  def run(context: Option[JoinPartJobContext] = None): Option[DataFrame] =
    tableUtils.withJobDescription(s"JoinPartJob(${joinPart.groupBy.metaData.name}) $dateRange") {

      logger.info(s"Running join part job for ${joinPart.groupBy.metaData.name} on range $dateRange")

      val jobContext = context.getOrElse {
        // LeftTable is already computed by SourceJob, no need to apply query/filters/etc
        val relevantLeftCols =
          joinPart.rightToLeft.values.toArray ++ Seq(tableUtils.partitionColumn) ++ (node.leftDataModel match {
            case ENTITIES => None
            case EVENTS   => Some(Constants.TimeColumn)
          })

        val query = Builders.Query(selects = relevantLeftCols.map(t => t -> t).toMap)
        val cachedLeftDf = tableUtils.scanDf(query = query, leftTable, range = Some(dateRange))

        // If the left dataframe is empty, skip this range entirely
        if (cachedLeftDf.isEmpty) {
          logger.info(s"Left dataframe is empty for range $dateRange, skipping join part computation")
          return None
        }

        val runSmallMode = JoinUtils.runSmallMode(tableUtils, cachedLeftDf)

        val leftWithStats = DfWithStats(cachedLeftDf)(partitionSpec)

        val joinLevelBloomMapOpt =
          JoinUtils.genBloomFilterIfNeeded(joinPart, node.leftDataModel, dateRange, None)

        JoinPartJobContext(Option(leftWithStats),
                           joinLevelBloomMapOpt,
                           Option(metaData.tableProps).getOrElse(Map.empty[String, String]),
                           runSmallMode)
      }

      // TODO: fix left df and left time range, bloom filter, small mode args
      computeRightTable(
        jobContext,
        joinPart,
        dateRange,
        metaData.outputTable
      )
    }

  private def computeRightTable(jobContext: JoinPartJobContext,
                                joinPart: JoinPart,
                                leftRange: PartitionRange, // missing left partitions
                                partTable: String): Option[DataFrame] = {

    // val partMetrics = Metrics.Context(metrics, joinPart) -- TODO is this metrics context sufficient, or should we pass thru for monolith join?
    val partMetrics = Metrics.Context(Metrics.Environment.JoinOffline, joinPart.groupBy)

    val crossGridSnapshot = node.leftDataModel == EVENTS &&
      joinPart.groupBy.inferredAccuracy == Accuracy.SNAPSHOT &&
      !partSnapshotSpec.hasSameGrid(tableUtils.partitionSpec)
    val rightRange = JoinUtils.snapshotScanRange(node.leftDataModel, joinPart, leftRange, partSnapshotSpec)
    val outputRange = if (crossGridSnapshot) leftRange else rightRange

    // Can kill the option after we deprecate monolith join job
    jobContext.leftDf.foreach { leftDf =>
      try {

        val start = System.currentTimeMillis()
        val prunedLeft = leftDf.prunePartitions(leftRange) // We can kill this after we deprecate monolith join job
        val filledDf =
          computeJoinPart(prunedLeft, joinPart, jobContext.joinLevelBloomMapOpt, skipBloom = jobContext.runSmallMode)

        // Cache join part data into intermediate table
        if (filledDf.isDefined) {
          logger.info(s"Writing to join part table: $partTable for partition range $outputRange")
          filledDf.get.save(partTable, jobContext.tableProps.toMap)
        } else {
          logger.info(s"Skipping $partTable because no data in computed joinPart.")
        }

        val elapsedMins = (System.currentTimeMillis() - start) / 60000
        partMetrics.gauge(Metrics.Name.LatencyMinutes, elapsedMins)
        logger.info(s"Wrote to join part table: $partTable in $elapsedMins minutes")
      } catch {
        case e: Exception =>
          logger.error(s"Error while processing groupBy: ${joinPart.groupBy.getMetaData.getName}")
          throw e
      }
    }

    if (tableUtils.tableReachable(partTable)) {
      Some(tableUtils.scanDf(query = null, partTable, range = Some(outputRange)))
    } else {
      // Happens when everything is handled by bootstrap
      None
    }
  }

  private def computeJoinPart(leftDfWithStats: Option[DfWithStats],
                              joinPart: JoinPart,
                              joinLevelBloomMapOpt: Option[util.Map[String, BloomFilter]],
                              skipBloom: Boolean): Option[DataFrame] = {

    if (leftDfWithStats.isEmpty) {
      // happens when all rows are already filled by bootstrap tables
      logger.info(s"\nBackfill is NOT required for ${joinPart.groupBy.metaData.name} since all rows are bootstrapped.")
      return None
    }

    val statsDf = leftDfWithStats.get

    logger.info(s"\nBackfill is required for ${joinPart.groupBy.metaData.name}")
    val rightBloomMap = if (skipBloom) {
      None
    } else {
      JoinUtils.genBloomFilterIfNeeded(joinPart, node.leftDataModel, dateRange, joinLevelBloomMapOpt)
    }

    val rightSkewFilter = JoinUtils.partSkewFilter(joinPart, skewKeys)

    def genGroupBy(partitionRange: PartitionRange, effectiveTableUtils: TableUtils = tableUtils) =
      GroupBy.from(joinPart.groupBy,
                   partitionRange,
                   effectiveTableUtils,
                   computeDependency = true,
                   rightBloomMap,
                   rightSkewFilter,
                   showDf = showDf)

    // all lazy vals - so evaluated only when needed by each case.
    lazy val partitionRangeGroupBy = genGroupBy(dateRange)

    lazy val unfilledPartitionRange = if (tableUtils.checkLeftTimeRange) {
      val timeRange = statsDf.timeRange
      logger.info(s"left unfilled time range checked to be: $timeRange")
      timeRange.toPartitionRange
    } else {
      logger.info(s"Not checking time range, but inferring it from partition range: $dateRange")
      dateRange
    }

    val leftSkewFilter =
      JoinUtils.skewFilter(Option(joinPart.rightToLeft.values.toSeq), skewKeys, joinPart.rightToLeft.values.toSeq)
    // this is the second time we apply skew filter - but this filters only on the keys
    // relevant for this join part.
    lazy val skewFilteredLeft = leftSkewFilter
      .map { sf =>
        val filtered = statsDf.df.filter(sf)
        logger.info(s"""Skew filtering left-df for
                       |GroupBy: ${joinPart.groupBy.metaData.name}
                       |filterClause: $sf
                       |""".stripMargin)
        filtered
      }
      .getOrElse(statsDf.df)

    /*
      For the corner case when the values of the key mapping also exist in the keys, for example:
      Map(user -> user_name, user_name -> user)
      the below logic will first rename the conflicted column with some random suffix and update the rename map
     */
    lazy val renamedLeftRawDf = {
      val columns = skewFilteredLeft.columns.flatMap { column =>
        if (joinPart.leftToRight.contains(column)) {
          Some(col(column).as(joinPart.leftToRight(column)))
        } else if (joinPart.rightToLeft.contains(column)) {
          None
        } else {
          Some(col(column))
        }
      }
      skewFilteredLeft.select(columns: _*)
    }

    // RHS-grid lookback: snapshot part partitions are named one RHS partitionInterval before
    // their snapshot time. alignOutput only makes sense when the RHS grid matches the join grid.
    lazy val shiftedPartitionRange =
      if (alignOutput && partSnapshotSpec.hasSameGrid(tableUtils.partitionSpec)) unfilledPartitionRange
      else JoinUtils.snapshotLookbackRange(unfilledPartitionRange, partSnapshotSpec)

    // normalize non-string (date/timestamp-typed) partition columns into formatted ds values.
    // String values pass through untouched: date_format's implicit string->timestamp cast
    // nulls values Spark can't natively cast (e.g. dash-separated sub-daily formats).
    val renamedLeftDf = renamedLeftRawDf.select(renamedLeftRawDf.columns.map {
      case c if c == tableUtils.partitionColumn && renamedLeftRawDf.schema(c).dataType != StringType =>
        date_format(renamedLeftRawDf.col(c), tableUtils.partitionFormat).as(c)
      case c => renamedLeftRawDf.col(c)
    }.toList: _*)

    val rightDf = (node.leftDataModel, joinPart.groupBy.dataModel, joinPart.groupBy.inferredAccuracy) match {
      case (ENTITIES, EVENTS, _)   => partitionRangeGroupBy.snapshotEvents(dateRange)
      case (ENTITIES, ENTITIES, _) => partitionRangeGroupBy.snapshotEntities
      case (EVENTS, EVENTS, Accuracy.SNAPSHOT) =>
        genGroupBy(shiftedPartitionRange, snapshotTableUtils).snapshotEvents(shiftedPartitionRange)
      case (EVENTS, EVENTS, Accuracy.TEMPORAL) =>
        if (tableUtils.skewFreeMode) {

          // Use UnionJoin for skewFree mode - it will handle column selection internally
          logger.info(s"Using UnionJoin for TEMPORAL events join part: ${joinPart.groupBy.metaData.name}")

          // key renaming is already done, so we should remove the keyMapping from joinPart
          val joinPartWithoutMapping = joinPart.deepCopy()
          joinPartWithoutMapping.unsetKeyMapping()

          UnionJoin.computeJoinPart(renamedLeftDf,
                                    joinPartWithoutMapping,
                                    unfilledPartitionRange,
                                    produceFinalJoinOutput = false)

        } else {
          // Use traditional temporalEvents approach
          genGroupBy(unfilledPartitionRange).temporalEvents(renamedLeftDf, Some(toTimeRange(unfilledPartitionRange)))
        }

      case (EVENTS, ENTITIES, Accuracy.SNAPSHOT) =>
        genGroupBy(shiftedPartitionRange, snapshotTableUtils).snapshotEntities

      case (EVENTS, ENTITIES, Accuracy.TEMPORAL) =>
        // Snapshots and mutations live on the groupBy's declared (typically daily) grid: a
        // partition holds data between its interval start and end. Run the computation in
        // that grid's universe so the ds_of_ts/mutation-day arithmetic lands on it; left
        // partition values pass through unchanged.
        genGroupBy(JoinUtils.snapshotLookbackRange(unfilledPartitionRange, partSnapshotSpec), snapshotTableUtils)
          .temporalEntities(renamedLeftDf)
    }

    val rightDfWithDerivations = if (joinPart.groupBy.hasDerivations) {

      val finalOutputColumns = joinPart.groupBy.derivationsScala.finalOutputColumn(
        rightDf.columns,
        ensureKeys = joinPart.groupBy.keys(tableUtils.partitionColumn)
      )

      val result = rightDf.select(finalOutputColumns: _*)
      result

    } else {
      rightDf
    }

    val outputDf =
      if (
        node.leftDataModel == EVENTS &&
        joinPart.groupBy.inferredAccuracy == Accuracy.SNAPSHOT &&
        !partSnapshotSpec.hasSameGrid(tableUtils.partitionSpec)
      ) {
        val snapshotStartMillis =
          (unix_timestamp(col(tableUtils.partitionColumn), partSnapshotSpec.format) * lit(1000L)).cast(LongType)
        val physicalSpec = tableUtils.partitionSpec
        val physicalColumn = tableUtils.partitionColumn
        val snapshotSpanMillis = partSnapshotSpec.spanMillis
        val physicalPartitions = udf { asOfMillis: Long =>
          physicalSpec
            .rangeIntersecting(PartitionInterval(asOfMillis, asOfMillis + snapshotSpanMillis))
            .map(_.partitions)
            .getOrElse(Seq.empty)
        }
        rightDfWithDerivations
          // Cross-grid part tables are physically placed on the join output grid; `ts` carries
          // the RHS snapshot as-of boundary used by merge for per-row matching.
          .withColumn(Constants.TimeColumn, snapshotStartMillis + lit(partSnapshotSpec.spanMillis))
          .withColumn(physicalColumn, explode(physicalPartitions(col(Constants.TimeColumn))))
          .where(col(physicalColumn).isin(unfilledPartitionRange.partitions: _*))
      } else {
        rightDfWithDerivations
      }

    if (showDf) {
      logger.info(s"printing results for joinPart: ${joinPart.groupBy.metaData.name}")
      outputDf.prettyPrint()
    }

    Some(outputDf)
  }
}
