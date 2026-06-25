package ai.chronon.api.planner

import ai.chronon.api.Extensions.{GroupByOps, WindowUtils}
import ai.chronon.api.Extensions._
import ai.chronon.api.ScalaJavaConversions.IterableOps
import ai.chronon.api.{DataModel, Join, PartitionSpec, TableDependency, TableInfo}
import ai.chronon.planner
import ai.chronon.planner.Node

import scala.collection.JavaConverters._

case class MonolithJoinPlanner(join: Join)(implicit outputPartitionSpec: PartitionSpec)
    extends ConfPlanner[Join](join)(outputPartitionSpec) {

  private val confOutputPartitionSpec: PartitionSpec =
    join.partitionSpec(outputPartitionSpec)

  private def validatePartitionIntervals(): Unit = {
    for {
      left <- Option(join.left)
      query <- Option(left.query)
    } {
      PartitionSpecResolver.validateQueryGrid(
        join.metaData.name,
        confOutputPartitionSpec,
        query,
        s"left source ${left.rawTable}",
        left.dataModel,
        PartitionSpecResolver.sourceSpec(left, confOutputPartitionSpec)
      )
    }
    validateBootstrapGrids()
    JoinPlanner.validateJoinPartGrids(join, confOutputPartitionSpec)
    PartitionSpecResolver.validateEmbeddedJoinSourceTree(join.left, confOutputPartitionSpec)
    Option(join.joinParts).foreach { joinParts =>
      joinParts.asScala.foreach { joinPart =>
        val groupBy = joinPart.groupBy
        val groupBySpec = groupBy.partitionSpec(confOutputPartitionSpec)
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
        confOutputPartitionSpec,
        deps,
        dep => s"bootstrap table ${dep.tableInfo.table}",
        DataModel.EVENTS
      )
    }

  private def semanticMonolithJoin(join: Join): Join = {
    val semanticJoin = join.deepCopy()
    semanticJoin.unsetMetaData()
    Option(semanticJoin.joinParts).map(_.asScala).foreach { parts =>
      parts.foreach(joinPart => joinPart.groupBy.unsetMetaData())
    }
    Option(semanticJoin.bootstrapParts).map(_.asScala).foreach { bootstrapParts =>
      bootstrapParts.foreach(bootstrapPart => bootstrapPart.unsetMetaData())
    }
    semanticJoin.unsetOnlineExternalParts()
    semanticJoin

  }

  def monolithJoinNode: Node = {
    val tableDeps = TableDependencies.fromJoin(join, Some(confOutputPartitionSpec))

    val metaData =
      MetaDataUtils.layer(join.metaData,
                          "monolith_join",
                          join.metaData.name + "__monolith_join",
                          tableDeps,
                          outputTableOverride = Some(join.metaData.outputTable))(confOutputPartitionSpec)
    val node = new planner.MonolithJoinNode().setJoin(join)
    toNode(metaData, _.setMonolithJoin(node), semanticMonolithJoin(join))
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
                          Some(stepDays))(confOutputPartitionSpec)
    val node = new planner.JoinMetadataUpload().setJoin(join)
    toNode(metaData, _.setJoinMetadataUpload(node), semanticMonolithJoin(join))
  }

  def statsComputeNode: Node = {
    val defaultStepDays = 1
    val effectiveStepDays = Option(join.metaData.executionInfo)
      .filter(_.isSetStepDays)
      .map(_.stepDays)
      .getOrElse(defaultStepDays)

    // Stats compute depends on the monolith join output
    val tableDep = new TableDependency()
      .setTableInfo(
        new TableInfo().setTable(monolithJoinNode.metaData.outputTable).withSpec(confOutputPartitionSpec)
      )
      .setStartOffset(WindowUtils.zero())
      .setEndOffset(WindowUtils.zero())

    val metaData =
      MetaDataUtils.layer(join.metaData,
                          "stats_compute",
                          join.metaData.name + "__stats_compute",
                          Seq(tableDep),
                          Some(effectiveStepDays))(confOutputPartitionSpec)

    val node = new planner.JoinStatsComputeNode().setJoin(join)
    toNode(metaData, _.setJoinStatsCompute(node), semanticMonolithJoin(join))
  }

  override def buildPlan: planner.ConfPlan = {
    validatePartitionIntervals()
    val confPlan = new planner.ConfPlan()

    val backfill = monolithJoinNode
    val metadataUpload = metadataUploadNode

    // Check if stats compute is enabled via ExecutionInfo feature flag
    val enableStatsCompute = Option(join.metaData.executionInfo)
      .flatMap(ei => Option(ei.enableStatsCompute))
      .exists(_.booleanValue())

    val sensorNodes = ExternalSourceSensorUtil
      .sensorNodes(backfill.metaData)(confOutputPartitionSpec)
      .map((es) =>
        toNode(es.metaData, _.setExternalSourceSensor(es), ExternalSourceSensorUtil.semanticExternalSourceSensor(es)))

    val (allNodes, terminalNodeNames) = if (enableStatsCompute) {
      val statsCompute = statsComputeNode

      // When stats are enabled, stats upload becomes the terminal node for BACKFILL
      // This ensures the dependency chain: backfill -> statsCompute
      val terminals = Map(
        planner.Mode.BACKFILL -> statsCompute.metaData.name,
        planner.Mode.DEPLOY -> metadataUpload.metaData.name
      )

      (List(backfill, statsCompute, metadataUpload) ++ sensorNodes, terminals)
    } else {
      val terminals = Map(
        planner.Mode.BACKFILL -> backfill.metaData.name,
        planner.Mode.DEPLOY -> metadataUpload.metaData.name
      )
      (List(backfill, metadataUpload) ++ sensorNodes, terminals)
    }

    confPlan
      .setNodes(allNodes.asJava)
      .setTerminalNodeNames(terminalNodeNames.asJava)
  }
}
