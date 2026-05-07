package ai.chronon.spark.batch

import ai.chronon.api.Extensions._
import ai.chronon.api._
import ai.chronon.api.planner.{DependencyResolver, NodeRunner}
import ai.chronon.api.secrets.SecretResolver
import ai.chronon.observability.{TileStats, TileStatsType}
import ai.chronon.online.{Api, KVStore}
import ai.chronon.planner._
import ai.chronon.spark.Extensions._
import ai.chronon.spark.batch.iceberg.IcebergPartitionStatsExtractor
import ai.chronon.spark.batch.{StagingQuery => StagingQueryUtil}
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.join.UnionJoin
import ai.chronon.spark.submission.SparkSessionBuilder
import ai.chronon.spark.utils.SemanticUtils
import ai.chronon.spark.{GroupBy, GroupByUpload, Join, ModelTransformsJob}
import org.rogach.scallop.{ScallopConf, ScallopOption}
import org.slf4j.{Logger, LoggerFactory}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.Await
import scala.util.{Failure, Success, Try}

class BatchNodeRunnerArgs(args: Array[String]) extends ScallopConf(args) {

  this: ScallopConf =>

  val confPath: ScallopOption[String] = opt[String](required = true, descr = "Path to node configuration file")
  val startDs: ScallopOption[String] = opt[String](
    required = false,
    descr = "Start date string in format yyyy-MM-dd, used for partitioning"
  )
  val endDs: ScallopOption[String] = opt[String](
    required = true,
    descr = "End date string in format yyyy-MM-dd, used for partitioning"
  )

  val onlineClass: ScallopOption[String] = opt[String](
    required = true,
    descr = "Fully qualified Online.Api based class. We expect the jar to be on the class path")

  val apiProps: Map[String, String] = props[String]('Z', descr = "Props to configure API Store")

  val tableStatsDataset: ScallopOption[String] = opt[String](
    required = false,
    descr = "Name of table in kv store to use to store partition statistics",
    default = None
  )

  verify()
}

class BatchNodeRunner(node: Node, tableUtils: TableUtils, api: Api) extends NodeRunner {
  @transient private lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  // in ad-hoc flows, the jobs downstream of external tables will simply fail (albeit, with retries)
  // in scheduled flow, the jobs downstream of external sensors will be stalled by the sensor
  def checkPartitions(conf: ExternalSourceSensorNode, range: PartitionRange): Try[Unit] = {
    val tableName = Option(conf.sourceTableDependency)
      .map(_.tableInfo)
      .map(_.table)
      .getOrElse(
        throw new IllegalArgumentException("ExternalSourceSensorNode must have a sourceTableDependency defined")
      )
    val retryCount = if (conf.isSetRetryCount) conf.retryCount else 3L
    val retryIntervalMin = if (conf.isSetRetryIntervalMin) conf.retryIntervalMin else 3L

    val tableInfo = conf.sourceTableDependency.tableInfo
    val hasPartitionColumn = Option(tableInfo.partitionColumn).isDefined
    val hasTriggerExpr = Option(tableInfo.triggerExpr).isDefined

    // Case 1: triggerExpr overrides — user-defined readiness check
    if (hasTriggerExpr) {
      val triggerExpr = tableInfo.triggerExpr
      @tailrec
      def retryTriggerExpr(attempt: Long): Try[Unit] = {
        Try {
          val sql = s"SELECT ${triggerExpr} FROM ${tableName}"
          logger.info(s"Executing trigger expression query: ${sql} on engine: ${conf.engineType.name()}")
          val result = conf.engineType match {
            case EngineType.SPARK => tableUtils.sql(sql)
            case _                => throw new RuntimeException("Not implemented.")
          }
          val triggerValue = result
            .collect()
            .headOption
            .getOrElse(throw new RuntimeException(s"Trigger expression query returned no results"))
            .get(0)

          val maxPartition = range.end
          val triggerValueStr = triggerValue.toString
          logger.info(s"Trigger value: ${triggerValueStr}, Max partition: ${maxPartition}")

          if (triggerValueStr > maxPartition) {
            logger.info(
              s"Trigger expression ${triggerExpr} value ${triggerValueStr} > ${maxPartition}. Sensor succeeded.")
            ()
          } else {
            throw new RuntimeException(
              s"Trigger expression ${triggerExpr} value ${triggerValueStr} is not greater than ${maxPartition}")
          }
        } match {
          case Success(_) => Success(())
          case Failure(e) if attempt < retryCount =>
            logger.warn(
              s"Attempt ${attempt + 1} failed: Trigger expression check failed with error: ${e.getMessage}. " +
                s"Retrying in ${retryIntervalMin} minutes")
            Thread.sleep(retryIntervalMin * 60 * 1000)
            retryTriggerExpr(attempt + 1)
          case Failure(e) =>
            Failure(
              new RuntimeException(s"Sensor timed out after ${retryIntervalMin * attempt} minutes. " +
                                     s"Trigger expression check failed: ${e.getMessage}",
                                   e))
        }
      }
      return retryTriggerExpr(0)
    }

    // Case 2: Has partition column — unified check: lastAvailablePartition >= range.end
    // Works for dense Hive, sparse Hive, Iceberg hidden partitions, timestamp columns — all the same.
    if (hasPartitionColumn) {
      val spec = tableInfo.partitionSpec(tableUtils.partitionSpec)
      @tailrec
      def retry(attempt: Long): Try[Unit] = {
        Try {
          logger.info(s"Checking last available partition for ${tableName} column ${tableInfo.partitionColumn}")
          val lastPartition = tableUtils
            .lastAvailablePartition(tableName, tablePartitionSpec = Some(spec))
            .getOrElse(throw new RuntimeException(s"Could not determine last available partition for ${tableName}"))

          val requiredEnd = range.end
          logger.info(s"Last available partition: ${lastPartition}, required end: ${requiredEnd}")

          if (lastPartition >= requiredEnd) {
            logger.info(s"Sensor succeeded: ${lastPartition} >= ${requiredEnd}")
            ()
          } else {
            throw new RuntimeException(
              s"Sensor check failed: last available partition ${lastPartition} < required end ${requiredEnd}")
          }
        } match {
          case Success(_) => Success(())
          case Failure(e) if attempt < retryCount =>
            logger.warn(s"Attempt ${attempt + 1} failed: ${e.getMessage}. Retrying in ${retryIntervalMin} minutes")
            Thread.sleep(retryIntervalMin * 60 * 1000)
            retry(attempt + 1)
          case Failure(e) =>
            Failure(
              new RuntimeException(s"Sensor timed out after ${retryIntervalMin * attempt} minutes. ${e.getMessage}", e))
        }
      }
      return retry(0)
    }

    // Case 3: No partition column, no trigger — just check table existence
    logger.info(s"Input table ${tableName} has no partitions or trigger expression defined. Checking table existence.")
    if (tableUtils.tableReachable(tableName)) Success(())
    else Failure(new RuntimeException(s"Table ${tableName} was not found."))
  }

  private def runStagingQuery(metaData: MetaData, stagingQuery: StagingQueryNode, range: PartitionRange): Unit = {
    require(stagingQuery.isSetStagingQuery, "StagingQueryNode must have a stagingQuery set")
    logger.info(s"Running staging query for '${metaData.name}'")
    val stagingQueryConf = stagingQuery.stagingQuery
    val sq = StagingQueryUtil.from(stagingQueryConf, range.end, tableUtils)
    sq.compute(
      range,
      Option(stagingQuery.stagingQuery.setups).map(_.asScala.toSeq).getOrElse(Seq.empty),
      Option(true)
    )

    logger.info(s"Successfully completed staging query for '${metaData.name}'")
  }

  private def runGroupByUpload(metadata: MetaData, groupByUpload: GroupByUploadNode, range: PartitionRange): Unit = {
    require(groupByUpload.isSetGroupBy, "GroupByUploadNode must have a groupBy set")
    val groupBy = groupByUpload.groupBy
    logger.info(s"Running groupBy upload for '${metadata.name}' for day: ${range.end}")

    GroupByUpload.run(groupBy, range.end, Option(tableUtils))
    logger.info(s"Successfully completed groupBy upload for '${metadata.name}' for day: ${range.end}")
  }

  private def runMonolithJoin(metadata: MetaData, monolithJoin: MonolithJoinNode, range: PartitionRange): Unit = {
    require(monolithJoin.isSetJoin, "MonolithJoinNode must have a join set")

    val joinConf = monolithJoin.join
    val joinName = metadata.name

    val standaloneUnionJoinEligible = UnionJoin.isEligibleForStandaloneRun(joinConf)

    logger.info(
      s"Running join backfill for '$joinName' with skewFreeMode: ${tableUtils.skewFreeMode}, standalone union-join eligible: $standaloneUnionJoinEligible")
    logger.info(s"Processing range: [${range.start}, ${range.end}]")

    val semanticHash = Option(node.semanticHash).filter(_.nonEmpty)

    if (standaloneUnionJoinEligible && tableUtils.skewFreeMode) {

      logger.info(s"Using standalone-union-join. Will skip writing join-part table & source table.")

      UnionJoin.computeJoinAndSave(joinConf, range, semanticHash)(tableUtils)

      logger.info(s"Successfully wrote range: $range")

    } else {
      val join = new Join(joinConf, range.end, tableUtils)
      val result = join.forceComputeRangeAndSave(range, semanticHash)

      result match {
        case Some(df) =>
          logger.info(s"\nShowing three rows of output above.\nQuery table '${metadata.outputTable}' for more.\n")
          df.show(numRows = 3, truncate = 0, vertical = true)

        case None =>
          throw new IllegalArgumentException(
            s"Join produced no results for range $range. Ensure that the input data is all present"
          )
      }
    }
  }

  private def runJoinStatsCompute(metadata: MetaData,
                                  joinStatsCompute: JoinStatsComputeNode,
                                  range: PartitionRange): Unit = {
    require(joinStatsCompute.isSetJoin, "JoinStatsComputeNode must have a join set")
    val joinConf = joinStatsCompute.join
    val joinName = metadata.name

    // step-days slicing is handled by the orchestrator via DependencyResolver.getMissingSteps;
    // each invocation receives exactly one step-sized range.
    logger.info(s"Running stats compute for join '$joinName' for range: [${range.start}, ${range.end}]")

    // Import the necessary stats classes
    import ai.chronon.spark.stats.EnhancedStatsCompute

    // Get the join output table from dependencies - the first table dependency should be the join output
    val joinOutputTable = Option(metadata.executionInfo)
      .flatMap(ei => Option(ei.getTableDependencies))
      .map(_.asScala.head.getTableInfo.table)
      .getOrElse(
        throw new IllegalStateException(s"Could not determine join output table for stats compute node: $joinName"))

    logger.info(s"Reading join output from table: $joinOutputTable")

    // Read the join output for the specified partition range
    val joinOutputDf = tableUtils.sql(
      s"SELECT * FROM $joinOutputTable WHERE ${tableUtils.partitionColumn} >= '${range.start}' AND ${tableUtils.partitionColumn} <= '${range.end}'"
    )

    if (joinOutputDf.isEmpty) {
      logger.info(s"No rows found for join '$joinName' in range [${range.start}, ${range.end}], skipping stats compute")
      return
    }

    // Extract key columns from the join configuration
    // Use Try to handle potential NPE from keyColumns method when join parts are null
    val keys = scala.util.Try(joinConf.keyColumns).toOption.map(_.toSeq).getOrElse(Seq.empty)
    if (keys.nonEmpty) {
      logger.info(s"Computing enhanced statistics with keys: ${keys.mkString(", ")}")
    } else {
      logger.info(s"Computing enhanced statistics without key exclusions (all columns will be analyzed)")
    }

    // Create EnhancedStatsCompute instance
    val enhancedStats = new EnhancedStatsCompute(
      inputDf = joinOutputDf,
      keys = keys,
      name = joinConf.metaData.name
    )

    // Compute daily summary statistics
    val (flatDf, statsMetadata) = enhancedStats.enhancedDailySummary(
      sample = 1.0,
      timeBucketMinutes = 0 // Daily tiles
    )

    // Convert to Avro DataFrame format (with key_bytes, value_bytes, ts columns)
    implicit val sparkSession = tableUtils.sparkSession
    val keyColumns = Seq("JoinPath")
    val valueColumns = flatDf.columns.filterNot(c => c == "JoinPath" || c == Constants.TimeColumn).toSeq
    val avroDf = ai.chronon.spark.AvroKvEncoder.encodeTimed(
      flatDf,
      keyColumns,
      valueColumns,
      storeSchemasPrefix = Some(joinConf.metaData.name),
      metadata = Some(statsMetadata)
    )
    val outputTable = metadata.outputTable
    avroDf.show()

    // Add partition column (ds) derived from timestamp for partitioning
    val avroDfWithPartition = avroDf.withTimeBasedColumn(tableUtils.partitionColumn).drop("key_json").drop("value_json")

    val recordCount = avroDfWithPartition.count()
    logger.info(s"Saving $recordCount stats records to table: $outputTable")

    // Write the stats in Avro format to the output table, partitioned by day
    tableUtils.insertPartitions(
      df = avroDfWithPartition,
      tableName = outputTable,
      semanticHash = Option(node.semanticHash).filter(_.nonEmpty)
    )

    // Upload to KV store using the proper EnhancedStatsStore method.
    // Sharding is gated by CHRONON_SHARD_ENHANCED_STATS — must be enabled on both write and read sides
    // before activating, otherwise the service won't find the sharded data.
    logger.info(s"Uploading $recordCount stats records to KV store")
    import ai.chronon.spark.stats.EnhancedStatsStore
    val shardingEnabled = sys.env.getOrElse("CHRONON_SHARD_ENHANCED_STATS", "false").equalsIgnoreCase("true")
    val statsSemanticHash = if (shardingEnabled) Option(node.semanticHash).filter(_.nonEmpty) else None
    logger.info(
      s"Stats sharding: ${if (shardingEnabled) s"enabled, semanticHash=${statsSemanticHash.getOrElse("(empty)")}"
        else "disabled (CHRONON_SHARD_ENHANCED_STATS not set)"}")
    val statsStore = new EnhancedStatsStore(api, Constants.EnhancedStatsDataset, semanticHash = statsSemanticHash)(
      tableUtils)
    statsStore.upload(avroDf, putsPerRequest = 100)

    logger.info(s"Successfully computed and saved stats for join '$joinName'")
  }

  private[batch] def extractAndPersistPartitionStats(metricsKvStore: KVStore, outputTable: String, confName: String)(
      implicit partitionSpec: PartitionSpec): Unit = {
    try {
      logger.info(s"Extracting partition statistics for table: $outputTable")
      val statsExtractor = new IcebergPartitionStatsExtractor(tableUtils.sparkSession)

      statsExtractor.extractPartitionedStats(outputTable, confName) match {
        case Some(tileSummaries) if tileSummaries.nonEmpty =>
          val groupedTileSummaries = tileSummaries.groupBy { case (observabilityTileKey, _) =>
            val dayPartitionMillis =
              IcebergPartitionStatsExtractor.extractPartitionMillisFromSlice(observabilityTileKey.getSlice,
                                                                             partitionSpec)
            (dayPartitionMillis)
          }

          val statsPutRequests = groupedTileSummaries.map { case ((dayPartitionMillis), columnTileSummaries) =>
            val nullCountsStats = IcebergPartitionStatsExtractor.createNullCountsStats(columnTileSummaries)
            val partitionStats = TileStats.nullCounts(nullCountsStats)
            IcebergPartitionStatsExtractor.createPartitionStatsPutRequest(outputTable,
                                                                          partitionStats,
                                                                          dayPartitionMillis,
                                                                          TileStatsType.NULL_COUNTS)
          }.toSeq

          statsExtractor.extractSchemaMapping(outputTable) match {
            case Some(schemaMapping) =>
              val schemaPutRequest =
                IcebergPartitionStatsExtractor.createSchemaMappingPutRequest(outputTable, schemaMapping)
              val allPutRequests = statsPutRequests :+ schemaPutRequest

              try {
                val kvStoreUpdates = metricsKvStore.multiPut(allPutRequests)
                Await.result(kvStoreUpdates, 30.seconds)

                logger.info(
                  s"Successfully persisted data quality metrics and schema mapping for table: $outputTable (${tileSummaries.size} tile summaries)")
              } catch {
                case e: Exception =>
                  logger.info(
                    s"Failed to persist data quality metrics to KV store for table: $outputTable. This may be expected if the KV store table does not exist. Error: ${e.traceString}")
              }
            case None =>
              logger.info(
                s"Could not extract schema mapping for table: $outputTable, skipping column stats persistence")
          }
        case Some(tileSummaries) if tileSummaries.isEmpty =>
          logger.info(s"No tile summaries found for table: $outputTable")
        case None =>
          logger.info(
            s"Table $outputTable is not an Iceberg table or is not partitioned, skipping column stats extraction")
      }
    } catch {
      case e: Exception =>
        logger.error(s"Failed to extract/persist data quality metrics for table: $outputTable", e)
      // Don't fail the job if stats extraction fails
    }
  }

  override def run(metadata: MetaData, conf: NodeContent, maybeRange: Option[PartitionRange]): Unit = {
    require(maybeRange.isDefined, "Partition range must be defined for batch node runner")
    val range = maybeRange.get
    val dateRange = new DateRange().setStartDate(range.start).setEndDate(range.end)

    conf.getSetField match {
      case NodeContent._Fields.MONOLITH_JOIN =>
        runMonolithJoin(metadata, conf.getMonolithJoin, range)

      case NodeContent._Fields.UNION_JOIN =>
        logger.info(s"Running union join for '${metadata.name}' for range: [${range.start}, ${range.end}]")
        require(conf.getUnionJoin.isSetJoin, "UnionJoinNode must have a join set")
        UnionJoin.computeJoinAndSave(conf.getUnionJoin.join, range)(tableUtils)
        logger.info(s"Successfully completed union join for '${metadata.name}'")

      case NodeContent._Fields.SOURCE_WITH_FILTER =>
        logger.info(s"Running source with filter job for '${metadata.name}' for range: [${range.start}, ${range.end}]")
        new SourceJob(conf.getSourceWithFilter, metadata, dateRange)(tableUtils).run()
        logger.info(s"Successfully completed source with filter job for '${metadata.name}'")

      case NodeContent._Fields.JOIN_BOOTSTRAP =>
        logger.info(s"Running join bootstrap job for '${metadata.name}' for range: [${range.start}, ${range.end}]")
        new JoinBootstrapJob(conf.getJoinBootstrap, metadata, dateRange)(tableUtils).run()
        logger.info(s"Successfully completed join bootstrap job for '${metadata.name}'")

      case NodeContent._Fields.JOIN_PART =>
        logger.info(s"Running join part job for '${metadata.name}' for range: [${range.start}, ${range.end}]")
        new JoinPartJob(conf.getJoinPart, metadata, dateRange, alignOutput = true)(tableUtils).run()
        logger.info(s"Successfully completed join part job for '${metadata.name}'")

      case NodeContent._Fields.JOIN_MERGE =>
        logger.info(s"Running join merge job for '${metadata.name}' for range: [${range.start}, ${range.end}]")
        val joinParts = Option(conf.getJoinMerge.join.joinParts).map(_.asScala.toSeq).getOrElse(Seq.empty)
        new MergeJob(conf.getJoinMerge, metadata, dateRange, joinParts)(tableUtils).run()
        logger.info(s"Successfully completed join merge job for '${metadata.name}'")

      case NodeContent._Fields.JOIN_DERIVATION =>
        logger.info(s"Running join derivation job for '${metadata.name}' for range: [${range.start}, ${range.end}]")
        new JoinDerivationJob(conf.getJoinDerivation, metadata, dateRange)(tableUtils).run()
        logger.info(s"Successfully completed join derivation job for '${metadata.name}'")

      case NodeContent._Fields.GROUP_BY_UPLOAD =>
        runGroupByUpload(metadata, conf.getGroupByUpload, range)

      case NodeContent._Fields.GROUP_BY_BACKFILL =>
        logger.info(s"Running groupBy backfill for '${metadata.name}' for range: [${range.start}, ${range.end}]")
        GroupBy.computeBackfill(
          conf.getGroupByBackfill.groupBy,
          range.start,
          range.end,
          tableUtils
        )
        logger.info(s"Successfully completed groupBy backfill for '${metadata.name}'")

      case NodeContent._Fields.STAGING_QUERY =>
        runStagingQuery(metadata, conf.getStagingQuery, range)

      case NodeContent._Fields.EXTERNAL_SOURCE_SENSOR =>
        checkPartitions(conf.getExternalSourceSensor, range) match {
          case Success(_) =>
          case Failure(exception) =>
            logger.error(s"ExternalSourceSensor check failed.", exception)
            throw exception
        }

      case NodeContent._Fields.MODEL_TRANSFORMS_BACKFILL =>
        logger.info(
          s"Running model transforms backfill for '${metadata.name}' for range: [${range.start}, ${range.end}]")
        require(conf.getModelTransformsBackfill.isSetModelTransforms,
                "ModelTransformsBackfillNode must have modelTransforms set")
        val modelTransforms = conf.getModelTransformsBackfill.modelTransforms

        val modelPlatformProvider = Option(api.generateModelPlatformProvider)
          .getOrElse(
            throw new IllegalStateException("Api with ModelPlatformProvider must be set for ModelTransforms backfill"))

        ModelTransformsJob.computeBackfill(
          modelTransforms,
          range,
          tableUtils,
          modelPlatformProvider
        )
        logger.info(s"Successfully completed model transforms backfill for '${metadata.name}'")

      case NodeContent._Fields.JOIN_STATS_COMPUTE =>
        logger.info(s"Running join stats compute for '${metadata.name}' for range: [${range.start}, ${range.end}]")
        require(conf.getJoinStatsCompute.isSetJoin, "JoinStatsComputeNode must have a join set")
        runJoinStatsCompute(metadata, conf.getJoinStatsCompute, range)
        logger.info(s"Successfully completed join stats compute for '${metadata.name}'")

      case _ =>
        throw new UnsupportedOperationException(s"Unsupported NodeContent type: ${conf.getSetField}")
    }
  }

  private def postJobActions(metadata: MetaData, range: PartitionRange, tableStatsDataset: Option[String]): Unit = {
    val outputTablePartitionSpec = (for {
      meta <- Option(metadata)
      executionInfo <- Option(meta.executionInfo)
      outputTableInfo <- Option(executionInfo.outputTableInfo)
      definedSpec = outputTableInfo.partitionSpec(tableUtils.partitionSpec)
    } yield definedSpec).getOrElse(tableUtils.partitionSpec)
    val outputTable = metadata.executionInfo.outputTableInfo.table

    val firstOutputPartition =
      tableUtils.firstAvailablePartition(outputTable, partitionSpec = outputTablePartitionSpec)
    val lastOutputPartition =
      tableUtils.lastAvailablePartition(outputTable, tablePartitionSpec = Option(outputTablePartitionSpec))
    val translatedRange = range.translate(tableUtils.partitionSpec)

    logger.info(
      s"Output table last available partition for '${metadata.name}': ${lastOutputPartition.getOrElse("none")}")

    // Validate output covers the requested range
    lastOutputPartition match {
      case Some(lastPartition) if lastPartition >= translatedRange.end =>
        logger.info(
          s"Output table $outputTable covers requested range (last: $lastPartition >= end: ${translatedRange.end})")
      case Some(lastPartition) =>
        logger.error(
          s"After job completion, output table $outputTable last partition $lastPartition < required end ${translatedRange.end}")
      case None =>
        logger.error(s"After job completion, output table $outputTable has no partitions")
    }

    logger.info(s"Successfully completed batch node runner for '${metadata.name}'")

    // Extract and persist partition statistics to KV store - done at the very end
    // Skip data quality metrics persistence for EXTERNAL_SOURCE_SENSOR nodes
    if (node.content.getSetField != NodeContent._Fields.EXTERNAL_SOURCE_SENSOR) {
      tableStatsDataset.foreach { tableStats =>
        val metricsKvStore = api.genMetricsKvStore(tableStats)
        Option(metadata.outputTable) match {
          case Some(outputTable) =>
            extractAndPersistPartitionStats(metricsKvStore, outputTable, metadata.name)(outputTablePartitionSpec)
          case None =>
            logger.warn(s"Skipping partition stats extraction for '${metadata.name}' - outputTable is null")
        }
      }
    } else {
      logger.info(s"Skipping data quality metrics persistence for EXTERNAL_SOURCE_SENSOR node '${metadata.name}'")
    }
  }

  case class TablePartitionStatus(name: String,
                                  firstAvailablePartition: Option[String],
                                  lastAvailablePartition: Option[String],
                                  ready: Boolean,
                                  requiredEnd: String,
                                  semanticHash: Option[String])

  /** Computes partition statuses for input tables using lastAvailablePartition >= required end.
    * Works uniformly for dense, sparse, Hive, Iceberg, timestamp columns.
    */
  private[batch] def computeInputTablePartitionStatuses(
      metadata: MetaData,
      range: PartitionRange,
      tableUtils: TableUtils
  ): Iterable[TablePartitionStatus] = {
    val inputTableDependencies: Map[String, Array[TableDependency]] =
      Option(metadata.executionInfo.getTableDependencies)
        .map(_.asScala.toArray)
        .getOrElse(Array.empty)
        .map(td => td.tableInfo.table -> td)
        .groupBy(_._1)
        .mapValues(_.map(_._2))
        .toMap

    inputTableDependencies
      .filterNot(_._2.forall(td => td.isSetIsSoftNodeDependency && td.isSoftNodeDependency))
      .map { case (table, deps) =>
        val inputPartitionSpec = deps.head.tableInfo.partitionSpec(tableUtils.partitionSpec)

        val firstPartition =
          tableUtils.firstAvailablePartition(table, partitionSpec = inputPartitionSpec)
        val lastPartition = tableUtils.lastAvailablePartition(table, tablePartitionSpec = Some(inputPartitionSpec))

        // Compute the maximum required end across all dependencies for this table
        val requiredEnd = deps
          .flatMap { td =>
            DependencyResolver
              .computeInputRange(range, td)
              .map(_.translate(tableUtils.partitionSpec))
              .map(_.end)
          }
          .toSeq
          .sorted
          .lastOption
          .getOrElse(range.end)

        val ready = lastPartition.exists(_ >= requiredEnd)

        // Collect semanticHash values from all dependencies for this table
        val semanticHashes = deps.flatMap { td =>
          if (td.isSetSemanticHash && td.semanticHash.nonEmpty) {
            Some(td.semanticHash)
          } else {
            None
          }
        }.toSet

        val semanticHash = if (semanticHashes.size > 1) {
          logger.error(s"Table $table has inconsistent semanticHash values across dependencies: $semanticHashes")
          None
        } else {
          semanticHashes.headOption
        }

        TablePartitionStatus(table, firstPartition, lastPartition, ready, requiredEnd, semanticHash)
      }
  }

  private[batch] def isSensorNode: Boolean = {
    node.metaData.name.toLowerCase.contains("sensor")
  }

  def runFromArgs(
      startDs: String,
      endDs: String,
      tableStatsDataset: Option[String]
  ): Int = {
    Try {
      val metadata = node.metaData
      val range = PartitionRange(startDs, endDs)(PartitionSpec.daily)

      val inputTablePartitionStatuses = computeInputTablePartitionStatuses(metadata, range, tableUtils)

      logger.info(s"Starting batch node runner for '${metadata.name}'")

      // drop table if semantic hash doesn't match
      val outputTable = node.metaData.outputTable
      val incomingSemanticHash = node.semanticHash

      val su = new SemanticUtils(tableUtils)

      val archivedTableOpt = if (!isSensorNode) {
        su.checkSemanticHashAndArchive(outputTable, incomingSemanticHash)
      } else None

      val notReadyTables = inputTablePartitionStatuses.filterNot(_.ready)

      if (notReadyTables.nonEmpty) {
        throw new RuntimeException(
          "The following input tables are not ready for the requested range " +
            "(if the table exists, check logs above for credential or connectivity errors):\n" +
            notReadyTables
              .map { tps =>
                s"Table: ${tps.name}, last available: ${tps.lastAvailablePartition.getOrElse("none")}, required end: ${tps.requiredEnd}"
              }
              .mkString("\n")
        )
      } else {

        try {
          run(metadata, node.content, Option(range))
        } catch {
          case e: Exception =>
            archivedTableOpt.foreach { archivedTable =>
              try {
                // Another writer may have already succeeded with the new semantic hash.
                // Only rollback if the output table is missing or still has the old hash.
                val currentHash = if (tableUtils.tableReachable(outputTable)) {
                  tableUtils.getTableProperties(outputTable).flatMap(_.get(Constants.SemanticHashKey))
                } else None

                if (currentHash.contains(incomingSemanticHash)) {
                  logger.info(
                    s"Skipping rollback for $outputTable: another writer already produced it with hash $incomingSemanticHash")
                } else {
                  if (tableUtils.tableReachable(outputTable)) {
                    tableUtils.sql(s"DROP TABLE IF EXISTS $outputTable")
                  }
                  tableUtils.renameTable(archivedTable, outputTable)
                  logger.info(s"Rolled back archival: restored $archivedTable to $outputTable")
                }
              } catch {
                case rollbackEx: Exception =>
                  logger.error(s"Failed to rollback archival for $outputTable from $archivedTable", rollbackEx)
              }
            }
            throw e
        }

        try {

          postJobActions(metadata = metadata, range = range, tableStatsDataset = tableStatsDataset)

          if (!isSensorNode) {
            su.setSemanticHash(outputTable, incomingSemanticHash)
          }
        } catch {
          case e: Exception =>
            // Don't fail the job if post-job actions fail
            logger.error(s"Post-job actions failed for '${metadata.name}'", e)
        }
      }
    } match {
      case Success(_) => {
        logger.info("Batch node runner completed successfully")
        0
      }
      case Failure(e) => {
        logger.error(s"Batch node runner failed for '${node.metaData.name}'", e)
        1
      }
    }
  }
}

object BatchNodeRunner {

  def main(args: Array[String]): Unit = {
    val batchArgs = new BatchNodeRunnerArgs(args)
    val resolvedEnv = SecretResolver.resolveVaultUris(sys.env.toMap)
    val driverSecrets = resolvedEnv -- sys.env.keySet
    val node = ThriftJsonCodec.fromJsonFile[Node](batchArgs.confPath(), check = false)
    val tableUtils = TableUtils(SparkSessionBuilder.build(s"batch-node-runner-${node.metaData.name}"))
    val api = instantiateApi(batchArgs.onlineClass(), batchArgs.apiProps ++ driverSecrets)
    val runner = new BatchNodeRunner(node, tableUtils, api)
    val exitCode =
      runner.runFromArgs(batchArgs.startDs(), batchArgs.endDs(), batchArgs.tableStatsDataset.toOption)
    tableUtils.sparkSession.stop()
    System.exit(exitCode)
  }

  def instantiateApi(onlineClass: String, props: Map[String, String]): Api = {
    val cl = Thread.currentThread().getContextClassLoader
    val cls = cl.loadClass(onlineClass)
    val constructor = cls.getConstructors.apply(0)
    // Convert to regular Map to ensure serializability (Scallop's LazyMap is not serializable)
    // Force a copy by converting to HashMap and back to ensure it's not a LazyMap
    val serializableProps: Map[String, String] = scala.collection.immutable.HashMap(props.toSeq: _*)
    val onlineImpl = constructor.newInstance(serializableProps)
    onlineImpl.asInstanceOf[Api]
  }
}
