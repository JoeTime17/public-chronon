package ai.chronon.api.planner

import ai.chronon.api.Extensions._
import ai.chronon.api.{Model, PartitionSpec, TableDependency}
import ai.chronon.planner
import ai.chronon.planner.{ConfPlan, CreateModelEndpointNode, DeployModelNode, Node, TrainModelNode}

import scala.collection.JavaConverters._

class ModelPlanner(model: Model)(implicit outputPartitionSpec: PartitionSpec)
    extends ConfPlanner[Model](model)(outputPartitionSpec) {

  private val confOutputPartitionSpec: PartitionSpec =
    model.partitionSpec(outputPartitionSpec)

  private def validatePartitionIntervals(): Unit = {
    for {
      trainingConf <- Option(model.trainingConf)
      trainingDataSource <- Option(trainingConf.trainingDataSource)
      query <- Option(trainingDataSource.query)
    } {
      PartitionSpecResolver.validateQueryGrid(
        model.metaData.name,
        confOutputPartitionSpec,
        query,
        s"training source ${trainingDataSource.rawTable}",
        trainingDataSource.dataModel
      )
    }
  }

  private def eraseExecutionInfo: Model = {
    val result = model.deepCopy()
    result.metaData.unsetExecutionInfo()
    result
  }

  private def semanticModel(model: Model): Model = {
    val semantic = model.deepCopy()
    semantic.unsetMetaData()
    semantic
  }

  private def createTrainNode: Option[Node] = {
    val result = for {
      trainingConf <- Option(model.trainingConf)
      trainingDataSource <- Option(trainingConf.trainingDataSource)
      trainingDataWindow <- Option(trainingConf.trainingDataWindow)
      tableDeps <- TableDependencies.fromSource(trainingDataSource, maxWindowOpt = Option(trainingDataWindow))
    } yield {
      val metaData =
        MetaDataUtils.layer(
          model.metaData,
          "model_training",
          model.metaData.name + "__model_training",
          Seq(tableDeps),
          None
        )(confOutputPartitionSpec)
      val node = new TrainModelNode().setModel(eraseExecutionInfo)
      val copy = semanticModel(model)
      toNode(metaData, _.setTrainModel(node), copy)
    }
    result
  }

  def createEndpointNode: Node = {
    val tableDeps = if (model.isSetTrainingConf) {
      val trainingNode = createTrainNode.get
      val tableDep = new TableDependency()
        .setTableInfo(
          trainingNode.metaData.executionInfo.outputTableInfo.deepCopy()
        )
        .setStartOffset(WindowUtils.zero())
        .setEndOffset(WindowUtils.zero())
      Seq(tableDep)
    } else {
      Seq.empty
    }

    val metaData =
      MetaDataUtils.layer(
        model.metaData,
        "model_create_endpoint",
        model.metaData.name + "__model_create_endpoint",
        tableDeps,
        None
      )(confOutputPartitionSpec)

    val node = new CreateModelEndpointNode().setModel(eraseExecutionInfo)

    val copy = semanticModel(model)

    toNode(metaData, _.setCreateModelEndpoint(node), copy)
  }

  def deployModelNode: Node = {
    val stepDays = 1 // Default step days for model deployment

    // Deploy depends on the endpoint being created
    val createEndpoint = createEndpointNode
    val tableDep = new TableDependency()
      .setTableInfo(
        createEndpoint.metaData.executionInfo.outputTableInfo.deepCopy()
      )
      .setStartOffset(WindowUtils.zero())
      .setEndOffset(WindowUtils.zero())
    val tableDeps = Seq(tableDep)

    val metaData =
      MetaDataUtils.layer(
        model.metaData,
        "model_deploy",
        model.metaData.name + "__model_deploy",
        tableDeps,
        Some(stepDays)
      )(confOutputPartitionSpec)

    val node = new DeployModelNode().setModel(eraseExecutionInfo)

    val copy = semanticModel(model)

    toNode(metaData, _.setDeployModel(node), copy)
  }

  override def buildPlan: ConfPlan = {
    validatePartitionIntervals()
    val createEndpoint = createEndpointNode
    val deploy = deployModelNode

    val terminalNodeNames = Map(
      planner.Mode.DEPLOY -> deploy.metaData.name
    )

    val training = createTrainNode
    if (training.isDefined) {
      new ConfPlan()
        .setNodes(Seq(createEndpoint, deploy, training.get).asJava)
        .setTerminalNodeNames((terminalNodeNames + (planner.Mode.BACKFILL -> training.get.metaData.name)).asJava)
    } else {
      new ConfPlan()
        .setNodes(Seq(createEndpoint, deploy).asJava)
        .setTerminalNodeNames(terminalNodeNames.asJava)
    }
  }
}

object ModelPlanner {
  def apply(model: Model)(implicit outputPartitionSpec: PartitionSpec): ModelPlanner =
    new ModelPlanner(model)(outputPartitionSpec)
}
