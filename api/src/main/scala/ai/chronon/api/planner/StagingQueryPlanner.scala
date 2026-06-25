package ai.chronon.api.planner

import ai.chronon.api.Extensions._
import ai.chronon.api.{DataModel, PartitionSpec, StagingQuery}
import ai.chronon.planner.{ConfPlan, StagingQueryNode}

import scala.collection.JavaConverters._

case class StagingQueryPlanner(stagingQuery: StagingQuery)(implicit outputPartitionSpec: PartitionSpec)
    extends ConfPlanner[StagingQuery](stagingQuery)(outputPartitionSpec) {

  private val confOutputPartitionSpec: PartitionSpec =
    stagingQuery.partitionSpec(outputPartitionSpec)

  private def semanticStagingQuery(stagingQuery: StagingQuery): StagingQuery = {
    val semanticStagingQuery = stagingQuery.deepCopy()
    semanticStagingQuery.unsetMetaData()
    semanticStagingQuery
  }

  override def buildPlan: ConfPlan = {
    val tableDependencies = PartitionSpecResolver.validateAndResolveDependencies(
      stagingQuery.metaData.name,
      confOutputPartitionSpec,
      TableDependencies.fromStagingQuery(stagingQuery),
      dep => s"table dependency ${dep.tableInfo.table}",
      DataModel.EVENTS
    )

    val metaData = MetaDataUtils.layer(
      stagingQuery.metaData,
      "staging",
      stagingQuery.metaData.name + "__staging",
      tableDependencies,
      outputTableOverride = Some(stagingQuery.metaData.outputTable)
    )(confOutputPartitionSpec)

    val node = new StagingQueryNode().setStagingQuery(stagingQuery)
    val finalNode = toNode(metaData, _.setStagingQuery(node), semanticStagingQuery(stagingQuery))
    val externalSensorNodes = ExternalSourceSensorUtil
      .sensorNodes(finalNode.metaData)(confOutputPartitionSpec)
      .map((es) =>
        toNode(es.metaData, _.setExternalSourceSensor(es), ExternalSourceSensorUtil.semanticExternalSourceSensor(es)))

    val terminalNodeNames = Map(
      ai.chronon.planner.Mode.BACKFILL -> finalNode.metaData.name
    )

    new ConfPlan()
      .setNodes((Seq(finalNode) ++ externalSensorNodes).asJava)
      .setTerminalNodeNames(terminalNodeNames.asJava)
  }
}
