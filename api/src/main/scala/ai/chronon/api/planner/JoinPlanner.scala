package ai.chronon.api.planner

import ai.chronon.api.Extensions.{GroupByOps, JoinOps, MetadataOps, SourceOps, StringOps, TableInfoOps, WindowUtils}
import ai.chronon.api.ScalaJavaConversions.{IterableOps, IteratorOps}
import ai.chronon.api._
import ai.chronon.planner
import ai.chronon.planner._

import scala.collection.JavaConverters._
import scala.language.{implicitConversions, reflectiveCalls}

class JoinPlanner(join: Join)(implicit outputPartitionSpec: PartitionSpec)
    extends ConfPlanner[Join](join)(outputPartitionSpec) {

  private val joinPartitionSpec: PartitionSpec =
    join.partitionSpec(outputPartitionSpec)

  private def validatePartitionGrids(): Unit = {
    // join output partitions are computed from left rows in the same time range, so the join's
    // boundaries must sit on the left source's boundaries; a coarser/undeclared left
    // partitionInterval under a sub-daily join is the silent-staleness trap. Right parts read
    // per-row by ts and are validated (loosely, by design) in validateJoinPartGrids.
    for {
      left <- Option(join.left)
      query <- Option(left.query)
    } {
      PartitionSpecResolver.validateQueryGrid(
        join.metaData.name,
        joinPartitionSpec,
        query,
        s"left source ${left.rawTable}",
        left.dataModel,
        PartitionSpecResolver.sourceSpec(left, joinPartitionSpec)
      )
    }
    validateBootstrapGrids()
    JoinPlanner.validateJoinPartGrids(join, joinPartitionSpec)
    PartitionSpecResolver.validateEmbeddedJoinSourceTree(join.left, joinPartitionSpec)
    Option(join.joinParts).foreach { joinParts =>
      joinParts.asScala.foreach { joinPart =>
        val groupBy = joinPart.groupBy
        val groupBySpec = groupBy.partitionSpec(joinPartitionSpec)
        PartitionSpecResolver.validateEmbeddedJoinSourceTrees(
          Option(groupBy.sources).map(_.asScala.toSeq).getOrElse(Seq.empty),
          groupBySpec
        )
      }
    }
  }

  private def validateBootstrapGrids(): Unit =
    Option(join.bootstrapParts).foreach { bootstrapParts =>
      val deps = bootstrapParts.asScala.map(bp => TableDependencies.fromTable(bp.table, bp.query)).toSeq
      PartitionSpecResolver.validateAndResolveDependencies(
        join.metaData.name,
        joinPartitionSpec,
        deps,
        dep => s"bootstrap table ${dep.tableInfo.table}",
        DataModel.EVENTS
      )
    }

  // will mutate the join in place - use on deepCopy-ied objects only
  private def joinWithoutMetadata(join: Join): Unit = {
    join.unsetMetaData()
    Option(join.joinParts).foreach(_.iterator().toScala.foreach(_.groupBy.unsetMetaData()))
  }

  private def joinWithoutExecutionInfo: Join = {
    val copied = join.deepCopy()
    copied.metaData.unsetExecutionInfo()
    Option(copied.joinParts).foreach(_.iterator().toScala.foreach(_.groupBy.metaData.unsetExecutionInfo()))
    copied
  }

  val leftSourceNode: Node = {

    val left = join.left
    val result = new SourceWithFilterNode()
      .setSource(left)
      .setExcludeKeys(join.skewKeys)

    val leftSourceHash = ThriftJsonCodec.hexDigest(result)
    val leftSourceTable = left.table.replace(".", "__").sanitize // source_namespace.table -> source_namespace__table
    val outputTableName =
      leftSourceTable + "__" + leftSourceHash + "__source" // source__<source_namespace>__<table>__<hash>

    // at this point metaData.outputTable = join_namespace.source__<source_namespace>__<table>__<hash>
    val metaData = MetaDataUtils.layer(
      join.metaData,
      "source",
      outputTableName,
      TableDependencies.fromSource(join.left, maxWindowOpt = Some(WindowUtils.zero())).toSeq
    )(joinPartitionSpec)

    toNode(metaData, _.setSourceWithFilter(result), result)
  }

  private val bootstrapNodeOpt: Option[Node] = Option(join.bootstrapParts).map { bootstrapParts =>
    val result = new JoinBootstrapNode()
      .setJoin(joinWithoutExecutionInfo)

    // bootstrap tables follow the standard naming convention: outputTable + "_bootstrap"
    val bootstrapNodeName = join.metaData.name + "_bootstrap"

    val bootstrapDeps = bootstrapParts.toScala.map { bp =>
      TableDependencies.fromTable(bp.table, bp.query)
    }.toSeq
    val tableDeps =
      TableDependencies.fromTableInfo(leftSourceNode.metaData.executionInfo.outputTableInfo) +: bootstrapDeps

    val metaData = MetaDataUtils.layer(
      join.metaData,
      "bootstrap",
      bootstrapNodeName,
      tableDeps
    )(joinPartitionSpec)

    val content = new NodeContent()
    content.setJoinBootstrap(result)

    val copy = result.deepCopy()
    joinWithoutMetadata(copy.join)

    toNode(metaData, _.setJoinBootstrap(result), copy)
  }

  private def copyForExecutableJoinPart(joinPart: JoinPart): JoinPart = {
    val copy = joinPart.deepCopy()
    // Keep groupBy executionInfo on executable JOIN_PART nodes: snapshot parts need the
    // declared groupBy grid as their logical as-of cadence even when the physical part table
    // stays on the join grid for partitioned joins.
    copy
  }

  private def buildJoinPartNode(joinPart: JoinPart): Node = {

    val result = new JoinPartNode()
      .setJoinPart(copyForExecutableJoinPart(joinPart))
      .setLeftDataModel(join.left.dataModel)
      .setLeftSourceTable(leftSourceNode.metaData.outputTable)
    PartitionSpecResolver.applyJoinPartSnapshotSpec(result.joinPart, joinPartitionSpec)

    val partTable = RelevantLeftForJoinPart.partTableName(join, joinPart)

    val snapshotShift =
      PartitionSpecResolver.snapshotSourceShift(result.joinPart, Option(join.left.dataModel), joinPartitionSpec)

    val deps = TableDependencies.fromGroupBy(result.joinPart.groupBy, Option(join.left.dataModel), snapshotShift) :+
      TableDependencies.fromTableInfo(leftSourceNode.metaData.executionInfo.outputTableInfo)

    // use step days from group_by if set, otherwise default to 15d for events and 1 for entities
    val stepDays = Option(joinPart.groupBy.metaData.executionInfo)
      .filter(_.isSetStepDays)
      .map(_.stepDays)
      .getOrElse(joinPart.groupBy.dataModel match {
        case DataModel.ENTITIES => 1
        case DataModel.EVENTS   => 15
      })

    val metaData = MetaDataUtils
      .layer(
        joinPart.groupBy.metaData,
        "join_part",
        partTable,
        deps,
        stepDays = Some(stepDays)
      )(joinPartitionSpec)
      .setOutputNamespace(join.metaData.outputNamespace)

    // Join part tables are physical join intermediates, so their partitions stay on the
    // join/output grid. Snapshot parts carry their logical as-of cadence in `ts`.
    metaData.executionInfo.outputTableInfo.withSpec(joinPartitionSpec)

    val copy = result.deepCopy()
    copy.joinPart.groupBy.unsetMetaData()

    toNode(metaData, _.setJoinPart(result), copy)
  }

  private val joinPartNodes: Seq[Node] = join.joinParts.toScala.map { buildJoinPartNode }.toSeq

  val mergeNode: Node = {
    val result = new JoinMergeNode()
      .setJoin(join)

    val joinPartDeps = joinPartNodes.map { jpNode =>
      val joinPart = jpNode.content.getJoinPart.joinPart
      val snapshotSpec = PartitionSpecResolver.snapshotSpec(joinPart, joinPartitionSpec)
      val shouldShift = join.left.dataModel == DataModel.EVENTS &&
        joinPart.groupBy.inferredAccuracy == Accuracy.SNAPSHOT &&
        snapshotSpec.hasSameGrid(joinPartitionSpec)

      // If the part's snapshot partitionInterval matches the join's, keep the historical
      // behavior: read the part table one partition back. If they differ, the part table is
      // partitioned exactly like the join output - one ds per join partition, each row's
      // snapshot time in ts - so the merge reads the same ds range it writes; no shift.
      val shiftAmount =
        if (shouldShift) Some(joinPartitionSpec.intervalWindow) else None
      TableDependencies.fromTableInfo(jpNode.metaData.executionInfo.outputTableInfo, shift = shiftAmount)
    }
    val leftDep = TableDependencies.fromTableInfo(
      bootstrapNodeOpt
        .map(_.metaData.executionInfo.outputTableInfo)
        .getOrElse(leftSourceNode.metaData.executionInfo.outputTableInfo)
    )
    val deps = leftDep +: joinPartDeps

    val mergeNodeName = join.metaData.name + "__merged"

    val metaData = MetaDataUtils
      .layer(
        join.metaData,
        "merge",
        mergeNodeName,
        deps,
        outputTableOverride = Some(join.metaData.outputTable)
      )(joinPartitionSpec)

    val copy = result.deepCopy()
    joinWithoutMetadata(copy.join)
    copy.join.unsetDerivations()

    toNode(metaData, _.setJoinMerge(result), copy)
  }

  private val derivationNodeOpt: Option[Node] = Option(join.derivations).map { _ =>
    val result = new JoinDerivationNode()
      .setJoin(join)

    val derivationNodeName = join.metaData.name + "__derived"
    val derivationOutputTable = join.metaData.outputTable + "__derived"

    val metaData = MetaDataUtils
      .layer(
        join.metaData,
        "derive",
        derivationNodeName,
        Seq(TableDependencies.fromTableInfo(mergeNode.metaData.executionInfo.outputTableInfo)),
        outputTableOverride = Some(derivationOutputTable)
      )(joinPartitionSpec)

    val copy = result.deepCopy()
    joinWithoutMetadata(copy.join)

    toNode(metaData, _.setJoinDerivation(result), copy)
  }

  private val statsComputeNodeOpt: Option[Node] = {
    val enableStatsCompute = Option(join.metaData.executionInfo)
      .flatMap(ei => Option(ei.enableStatsCompute))
      .exists(_.booleanValue())

    if (enableStatsCompute) {
      Some {
        val result = new JoinStatsComputeNode()
          .setJoin(join)

        val statsComputeNodeName = join.metaData.name + "__stats_compute"

        // Stats compute depends on the final output (derivation if present, otherwise merge)
        val inputTableInfo = derivationNodeOpt
          .map(_.metaData.executionInfo.outputTableInfo)
          .getOrElse(mergeNode.metaData.executionInfo.outputTableInfo)

        val stepDays = 1 // Stats computed daily

        val tableDep = TableDependencies.fromTableInfo(inputTableInfo)

        val metaData = MetaDataUtils
          .layer(
            join.metaData,
            "stats_compute",
            statsComputeNodeName,
            Seq(tableDep),
            Some(stepDays)
          )(joinPartitionSpec)

        val copy = result.deepCopy()
        joinWithoutMetadata(copy.join)

        toNode(metaData, _.setJoinStatsCompute(result), copy)
      }
    } else {
      None
    }
  }

  def offlineNodes: Seq[Node] = {

    Seq(leftSourceNode) ++
      bootstrapNodeOpt ++
      joinPartNodes ++
      Seq(mergeNode) ++
      derivationNodeOpt ++
      statsComputeNodeOpt
  }

  def metadataUploadNode: Node = {
    val stepDays = 1 // Default step days for metadata upload

    // Create table dependencies for all GroupBy parts (both direct GroupBy deps and upstream join deps)
    val allDeps = Option(join.joinParts).map(_.toScala).getOrElse(Seq.empty).flatMap { joinPart =>
      val groupBy = joinPart.groupBy
      val hasStreamingSource = groupBy.streamingSource.isDefined

      // Add dependency on the GroupBy node (either uploadToKV or streaming)
      val groupByTableName = if (hasStreamingSource) {
        groupBy.metaData.outputTable + s"__${GroupByPlanner.Streaming}"
      } else {
        groupBy.metaData.outputTable + s"__${GroupByPlanner.UploadToKV}"
      }
      val groupByOutputSpec = groupBy.partitionSpec(outputPartitionSpec)

      val groupByDep = new TableDependency()
        .setTableInfo(
          new TableInfo().setTable(groupByTableName).withSpec(groupByOutputSpec)
        )
        .setStartOffset(WindowUtils.zero())
        .setEndOffset(WindowUtils.zero())

      // Add dependencies on upstream join metadata uploads if GroupBy has JoinSource
      val upstreamJoinDeps = if (hasStreamingSource) {
        // Skip this for streaming GroupBys since the streaming node will handle this dependency
        Seq.empty
      } else {
        TableDependencies.fromJoinSources(groupBy.sources)
      }

      // Return both the GroupBy dependency and any upstream join dependencies
      Seq(groupByDep) ++ upstreamJoinDeps
    }
    val metadataUploadDeps = allDeps

    val metaData =
      MetaDataUtils.layer(join.metaData,
                          "metadata_upload",
                          join.metaData.name + "__metadata_upload",
                          metadataUploadDeps.toSeq,
                          Some(stepDays))(joinPartitionSpec)
    val node = new JoinMetadataUpload().setJoin(joinWithoutExecutionInfo)

    val copy = joinWithoutExecutionInfo.deepCopy()
    joinWithoutMetadata(copy)

    toNode(metaData, _.setJoinMetadataUpload(node), copy)
  }

  def unionJoinNode: Node = {
    val result = new planner.UnionJoinNode()
      .setJoin(joinWithoutExecutionInfo)

    val metaData = MetaDataUtils.layer(
      join.metaData,
      "union_join",
      join.metaData.name,
      TableDependencies.fromJoin(join, Some(joinPartitionSpec)).toSeq,
      outputTableOverride = Some(join.metaData.outputTable)
    )(joinPartitionSpec)

    val copy = result.deepCopy()
    joinWithoutMetadata(copy.join)

    toNode(metaData, _.setUnionJoin(result), copy)
  }

  override def buildPlan: ConfPlan = {
    validatePartitionGrids()
    // Check if this join is eligible for UnionJoin
    // Conditions: left is events, 1 join part, TEMPORAL accuracy, no bootstrap parts
    val isUnionJoinEligible = join.left.isSetEvents &&
      join.getJoinParts.size() == 1 &&
      join.getJoinParts.get(0).groupBy.inferredAccuracy == Accuracy.TEMPORAL &&
      !join.isSetBootstrapParts

    if (isUnionJoinEligible) {
      // Use UnionJoin path
      val unionNode = unionJoinNode
      val sensorNodes = ExternalSourceSensorUtil
        .sensorNodes(unionNode.metaData)(joinPartitionSpec)
        .map((es) =>
          toNode(es.metaData, _.setExternalSourceSensor(es), ExternalSourceSensorUtil.semanticExternalSourceSensor(es)))

      val metadataUpload = metadataUploadNode

      val terminalNodeNames = Map(
        planner.Mode.BACKFILL -> unionNode.metaData.name,
        planner.Mode.DEPLOY -> metadataUpload.metaData.name
      )

      new ConfPlan()
        .setNodes((Seq(unionNode, metadataUpload) ++ sensorNodes).asJava)
        .setTerminalNodeNames(terminalNodeNames.asJava)
    } else {
      // Use standard modular path
      val allOfflineNodes = offlineNodes

      // The final offline node is the backfill terminal
      val backfillTerminalNode = allOfflineNodes.last

      // Get sensor nodes for the backfill terminal node
      val sensorNodes = ExternalSourceSensorUtil
        .sensorNodes(backfillTerminalNode.metaData)(joinPartitionSpec)
        .map((es) =>
          toNode(es.metaData, _.setExternalSourceSensor(es), ExternalSourceSensorUtil.semanticExternalSourceSensor(es)))

      val metadataUpload = metadataUploadNode

      val terminalNodeNames = Map(
        planner.Mode.BACKFILL -> backfillTerminalNode.metaData.name,
        planner.Mode.DEPLOY -> metadataUpload.metaData.name
      )

      new ConfPlan()
        .setNodes((allOfflineNodes ++ Seq(metadataUpload) ++ sensorNodes).asJava)
        .setTerminalNodeNames(terminalNodeNames.asJava)
    }
  }
}

object JoinPlanner {

  // Join parts are matched by left row time, not by whole-partition reads:
  //   - snapshot parts: for each left row, MergeJob picks the latest RHS snapshot whose time
  //     is <= the row's ts, ON THE RHS GROUPBY'S DECLARED GRID - a row sitting exactly on a
  //     boundary picks the snapshot ending at its ts. (The inclusive "endMillis - 1" used
  //     elsewhere converts a partition to the last millisecond it holds; it is unrelated to
  //     this comparison.) That per-row pick works on any RHS grid - a finer or coarser RHS
  //     yields bounded staleness rather than missing data - so no grid relationship is
  //     required between the RHS and the join (deliberate relaxation; only the storage-cost
  //     warning applies),
  //   - temporal parts: the engine recomputes from raw events with temporal accuracy, so the
  //     groupBy output grid does not constrain the join grid at all. The part's raw event
  //     sources only need to be finer exact divisors of the join grid; if their offsets do
  //     not line up, DependencyResolver expands to every intersecting source partition.
  def validateJoinPartGrids(join: Join, joinSpec: PartitionSpec): Unit = {
    Option(join.joinParts).foreach { joinParts =>
      joinParts.asScala.foreach { joinPart =>
        if (joinPart.groupBy.inferredAccuracy == Accuracy.SNAPSHOT) {
          val groupBySpec = PartitionSpecResolver.snapshotSpec(joinPart, joinSpec)
          if (joinPart.groupBy.dataModel == DataModel.ENTITIES) {
            PartitionSpecResolver.warnSubDailyEntitySnapshot(
              s"join ${join.metaData.name}, part ${joinPart.groupBy.metaData.name}",
              groupBySpec)
          }
        } else if (joinPart.groupBy.inferredAccuracy == Accuracy.TEMPORAL) {
          Option(joinPart.groupBy.sources).foreach { sources =>
            sources.asScala
              .filter(_.dataModel == DataModel.EVENTS)
              .foreach { source =>
                PartitionSpecResolver.validateTemporalJoinEventSourceGrid(
                  join.metaData.name,
                  joinSpec,
                  source.query,
                  s"temporal join part ${joinPart.groupBy.metaData.name} source ${source.rawTable}"
                )
              }
          }
        }
      }
    }
  }

  // will mutate the join in place - use on deepCopy-ied objects only
  private def unsetNestedMetadata(join: Join): Unit = {
    join.unsetMetaData()
    Option(join.joinParts).foreach(_.iterator().toScala.foreach(_.groupBy.unsetMetaData()))
    // Keep onlineExternalParts as they affect output schema and are needed for bootstrap/merge/derivation
    // join.unsetOnlineExternalParts()
  }

}
