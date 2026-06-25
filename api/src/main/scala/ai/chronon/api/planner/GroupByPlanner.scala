package ai.chronon.api.planner

import ai.chronon.api.{DataModel, GroupBy, PartitionSpec, TableDependency, TableInfo}
import ai.chronon.api.Extensions._
import ai.chronon.planner.{
  ConfPlan,
  GroupByBackfillNode,
  GroupByUploadNode,
  GroupByUploadToKVNode,
  GroupByStreamingNode,
  Node
}
import scala.collection.JavaConverters._

case class GroupByPlanner(groupBy: GroupBy)(implicit outputPartitionSpec: PartitionSpec)
    extends ConfPlanner[GroupBy](groupBy)(outputPartitionSpec) {

  private val confOutputPartitionSpec: PartitionSpec =
    groupBy.partitionSpec(outputPartitionSpec)

  private def validatePartitionIntervals(): Unit = {
    if (groupBy.dataModel == DataModel.ENTITIES && groupBy.inferredAccuracy == ai.chronon.api.Accuracy.SNAPSHOT) {
      PartitionSpecResolver.warnSubDailyEntitySnapshot(s"groupBy ${groupBy.metaData.name}", confOutputPartitionSpec)
    }
    for {
      sources <- Option(groupBy.sources).toSeq
      source <- sources.asScala
      query <- Option(source.query)
    } {
      PartitionSpecResolver.validateQueryGrid(
        groupBy.metaData.name,
        confOutputPartitionSpec,
        query,
        s"source ${source.rawTable}",
        source.dataModel,
        PartitionSpecResolver.sourceSpec(source, confOutputPartitionSpec)
      )
    }
    PartitionSpecResolver.validateEmbeddedJoinSourceTrees(
      Option(groupBy.sources).map(_.asScala.toSeq).getOrElse(Seq.empty),
      confOutputPartitionSpec
    )
  }

  // execInfo can be heavy - and we don't want to duplicate it
  private def eraseExecutionInfo: GroupBy = {
    val result = groupBy.deepCopy()
    result.metaData.unsetExecutionInfo()
    result
  }

  private val groupByTableDeps: Seq[TableDependency] = TableDependencies.fromGroupBy(groupBy)

  def backfillNode: Node = {
    val defaultStepDays = if (groupBy.dataModel == DataModel.EVENTS) 15 else 1
    val effectiveStepDays =
      Option(groupBy.metaData.executionInfo).filter(_.isSetStepDays).map(_.stepDays).getOrElse(defaultStepDays)

    val metaData = MetaDataUtils.layer(groupBy.metaData,
                                       "group_by",
                                       groupBy.metaData.name + "__group_by",
                                       groupByTableDeps,
                                       Option(effectiveStepDays),
                                       Some(groupBy.metaData.outputTable))(confOutputPartitionSpec)

    val node = new GroupByBackfillNode().setGroupBy(eraseExecutionInfo)

    toNode(metaData, _.setGroupByBackfill(node), semanticGroupBy(groupBy))
  }

  private def semanticGroupBy(groupBy: GroupBy): GroupBy = {
    val semanticGroupBy = groupBy.deepCopy()
    semanticGroupBy.unsetMetaData()
    // Topics themselves don't affect the sem hash, we yank them out to allow users to change / fix misconfigured topics
    // without it requiring them to run the full chain of jobs
    Option(semanticGroupBy.sources).foreach { sources =>
      sources.forEach { source =>
        if (source.isSetEvents) source.getEvents.unsetTopic()
        else if (source.isSetEntities) source.getEntities.unsetMutationTopic()
      }
    }
    semanticGroupBy
  }

  def uploadNode: Node = {
    val stepDays = 1 // GBUs write out data per day
    val metaData =
      MetaDataUtils.layer(groupBy.metaData,
                          "upload",
                          groupBy.metaData.name + "__upload",
                          groupByTableDeps,
                          Some(stepDays))(confOutputPartitionSpec)

    val node = new GroupByUploadNode().setGroupBy(eraseExecutionInfo)
    toNode(metaData, _.setGroupByUpload(node), semanticGroupBy(groupBy))
  }

  def uploadToKVNode: Node = {
    val tableDep = new TableDependency()
      .setTableInfo(
        new TableInfo().setTable(uploadNode.metaData.outputTable).withSpec(confOutputPartitionSpec)
      )
      .setStartOffset(WindowUtils.zero())
      .setEndOffset(WindowUtils.zero())
    val uploadToKVTableDeps = Seq(tableDep)

    val metaData =
      MetaDataUtils.layer(
        groupBy.metaData,
        GroupByPlanner.UploadToKV,
        groupBy.metaData.name + s"__${GroupByPlanner.UploadToKV}",
        uploadToKVTableDeps,
        None
      )(confOutputPartitionSpec)

    val node = new GroupByUploadToKVNode().setGroupBy(eraseExecutionInfo)
    toNode(metaData, _.setGroupByUploadToKV(node), semanticGroupBy(groupBy))
  }

  def streamingNode: Option[Node] = {
    groupBy.streamingSource.map { _ =>
      // Streaming node has table dependency on the upload to KV
      val uploadToKVDep = new TableDependency()
        .setTableInfo(
          new TableInfo().setTable(uploadToKVNode.metaData.outputTable).withSpec(confOutputPartitionSpec)
        )
        .setStartOffset(WindowUtils.zero())
        .setEndOffset(WindowUtils.zero())

      // If this GroupBy has a JoinSource, add dependency on upstream join's metadata upload
      val joinSourceDeps = TableDependencies.fromJoinSources(groupBy.sources)

      val streamingTableDeps = Seq(uploadToKVDep) ++ joinSourceDeps

      val metaData =
        MetaDataUtils.layer(
          groupBy.metaData,
          GroupByPlanner.Streaming,
          groupBy.metaData.name + s"__${GroupByPlanner.Streaming}",
          streamingTableDeps,
          None
        )(confOutputPartitionSpec)

      val node = new GroupByStreamingNode().setGroupBy(eraseExecutionInfo)
      toNode(metaData, _.setGroupByStreaming(node), semanticGroupBy(groupBy))
    }
  }

  override def buildPlan: ConfPlan = {
    validatePartitionIntervals()
    val backfill = backfillNode
    val sensorNodes = ExternalSourceSensorUtil
      .sensorNodes(backfill.metaData)(confOutputPartitionSpec)
      .map { es =>
        toNode(es.metaData, _.setExternalSourceSensor(es), ExternalSourceSensorUtil.semanticExternalSourceSensor(es))
      }
    val allNodes = Seq(backfill, uploadNode, uploadToKVNode) ++ sensorNodes ++ streamingNode.toSeq

    val deployTerminalNode = streamingNode.map(_.metaData.name).getOrElse(uploadToKVNode.metaData.name)

    val terminalNodeNames = Map(
      ai.chronon.planner.Mode.BACKFILL -> backfill.metaData.name,
      ai.chronon.planner.Mode.DEPLOY -> deployTerminalNode
    )

    val confPlan = new ConfPlan()
      .setNodes(allNodes.asJava)
      .setTerminalNodeNames(terminalNodeNames.asJava)
    confPlan
  }
}

object GroupByPlanner {
  val Streaming = "streaming"
  val UploadToKV = "uploadToKV"
}
