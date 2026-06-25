package ai.chronon.api.test.planner

import ai.chronon.api.Builders.Query
import ai.chronon.api.{Builders => B, _}
import ai.chronon.api.Extensions.WindowUtils
import ai.chronon.api.planner.ModelPlanner
import ai.chronon.planner.Mode
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

class ModelPlannerTest extends AnyFlatSpec with Matchers {

  private implicit val testPartitionSpec: PartitionSpec = PartitionSpec.daily
  private val threeHourSpec = PartitionSpec("ds", "yyyy-MM-dd-HH-mm", 3 * 60 * 60 * 1000)

  private def outputTableInfo(table: String, spec: PartitionSpec): TableInfo =
    new TableInfo()
      .setTable(table)
      .setPartitionColumn(spec.column)
      .setPartitionFormat(spec.format)
      .setPartitionInterval(WindowUtils.fromMillis(spec.spanMillis))

  private def executionInfoFor(table: String, spec: PartitionSpec): ExecutionInfo =
    new ExecutionInfo().setOutputTableInfo(outputTableInfo(table, spec))

  private def sourceWithPartitionSpec(table: String, spec: PartitionSpec): Source = {
    val query = Query(partitionColumn = spec.column)
    query.setPartitionFormat(spec.format)
    query.setPartitionInterval(WindowUtils.fromMillis(spec.spanMillis))
    B.Source.events(table = table, query = query)
  }

  private def buildModelWithTrainingSpec(name: String, trainingDataSource: Source, trainingWindow: Window): Model = {
    B.Model(
      metaData = B.MetaData(
        name = name,
        namespace = "test_namespace"
      ),
      trainingSpec = B.TrainingSpec(
        trainingDataSource = trainingDataSource,
        trainingDataWindow = trainingWindow
      ),
      inferenceSpec = B.InferenceSpec(
        modelBackend = ModelBackend.VertexAI,
        modelBackendParams = Map("project" -> "test-project", "region" -> "us-central1")
      )
    )
  }

  private def buildModel(name: String): Model = {
    B.Model(
      metaData = B.MetaData(
        name = name,
        namespace = "test_namespace"
      ),
      inferenceSpec = B.InferenceSpec(
        modelBackend = ModelBackend.VertexAI,
        modelBackendParams = Map("project" -> "test-project", "region" -> "us-central1")
      )
    )
  }

  "ModelPlanner" should "create trainModel, createEndpoint and deployModel nodes when trainingSpec is present" in {
    val trainingDataTable = "training_data_table"
    val model = buildModelWithTrainingSpec("test_model_with_training", B.Source.events(table = trainingDataTable, query = Query()), new Window().setTimeUnit(TimeUnit.DAYS).setLength(5))
    val planner = new ModelPlanner(model)
    val plan = planner.buildPlan

    // Should create plan with all three nodes
    plan.nodes.asScala should have size 3

    // Find all nodes
    val trainModelNode = plan.nodes.asScala.find(_.content.isSetTrainModel)
    val createEndpointNode = plan.nodes.asScala.find(_.content.isSetCreateModelEndpoint)
    val deployModelNode = plan.nodes.asScala.find(_.content.isSetDeployModel)

    trainModelNode should be(defined)

    // Verify trainModel node's table deps
    val trainTableDeps = trainModelNode.get.metaData.executionInfo.tableDependencies.asScala
    trainTableDeps should have size 1
    trainTableDeps.head.tableInfo.table shouldBe trainingDataTable
    trainTableDeps.head.startOffset shouldBe new Window().setTimeUnit(TimeUnit.DAYS).setLength(5)

    createEndpointNode should be(defined)
    deployModelNode should be(defined)

    // Verify dependencies
    val createEndpointDeps = createEndpointNode.get.metaData.executionInfo.tableDependencies.asScala
    createEndpointDeps should have size 1
    createEndpointDeps.head.tableInfo.table shouldBe trainModelNode.get.metaData.executionInfo.outputTableInfo.table

    val deployDeps = deployModelNode.get.metaData.executionInfo.tableDependencies.asScala
    deployDeps should have size 1
    deployDeps.head.tableInfo.table shouldBe createEndpointNode.get.metaData.executionInfo.outputTableInfo.table
  }

  "ModelPlanner" should "create both createEndpoint and deployModel nodes" in {
    val model = buildModel("test_model")
    val planner = new ModelPlanner(model)
    val plan = planner.buildPlan

    // Should create plan with both nodes
    plan.nodes.asScala should have size 2

    // Find both nodes
    val createEndpointNode = plan.nodes.asScala.find(_.content.isSetCreateModelEndpoint)
    val deployModelNode = plan.nodes.asScala.find(_.content.isSetDeployModel)

    createEndpointNode should be(defined)
    deployModelNode should be(defined)

    // Verify createEndpoint node
    createEndpointNode.get.content.getCreateModelEndpoint.model should not be null
    createEndpointNode.get.metaData.name should equal(s"${model.metaData.name}__model_create_endpoint")

    // Verify no table dependencies for endpoint creation
    val createEndpointDeps = createEndpointNode.get.metaData.executionInfo.tableDependencies.asScala
    createEndpointDeps should be(empty)

    // Verify deployModel node
    deployModelNode.get.content.getDeployModel.model should not be null
    deployModelNode.get.metaData.name should equal(s"${model.metaData.name}__model_deploy")

    // Verify table dependencies - should depend on createEndpoint
    val deployDeps = deployModelNode.get.metaData.executionInfo.tableDependencies.asScala
    deployDeps should have size 1
    deployDeps.head.tableInfo.table shouldBe createEndpointNode.get.metaData.executionInfo.outputTableInfo.table

    // Verify step days for deployment
    deployModelNode.get.metaData.executionInfo.isSetStepDays shouldBe true
    deployModelNode.get.metaData.executionInfo.stepDays shouldBe 1
  }

  it should "support sub-daily model retraining when the training source matches the interval" in {
    val model = B.Model(
      metaData = B.MetaData(
        name = "three_hour_model",
        namespace = "test_namespace",
        executionInfo = executionInfoFor("test_namespace.three_hour_model", threeHourSpec)
      ),
      trainingSpec = B.TrainingSpec(
        trainingDataSource = sourceWithPartitionSpec("training_data_table", threeHourSpec),
        trainingDataWindow = new Window().setTimeUnit(TimeUnit.HOURS).setLength(3)
      ),
      inferenceSpec = B.InferenceSpec(
        modelBackend = ModelBackend.VertexAI,
        modelBackendParams = Map("project" -> "test-project", "region" -> "us-central1")
      )
    )

    val plan = new ModelPlanner(model).buildPlan

    val trainNode = plan.nodes.asScala.find(_.content.isSetTrainModel).get
    trainNode.metaData.executionInfo.outputTableInfo.partitionFormat should equal(threeHourSpec.format)
    trainNode.metaData.executionInfo.outputTableInfo.partitionInterval should equal(WindowUtils.fromMillis(threeHourSpec.spanMillis))
  }

  it should "reject sub-daily models over training sources with no declared partition interval" in {
    // undeclared = implicitly daily: the intraday-staleness trap, caught at plan time
    val model = B.Model(
      metaData = B.MetaData(
        name = "undeclared_source_model",
        namespace = "test_namespace",
        executionInfo = executionInfoFor("test_namespace.undeclared_source_model", threeHourSpec)
      ),
      trainingSpec = B.TrainingSpec(
        trainingDataSource = B.Source.events(table = "undeclared_training_table", query = Query()),
        trainingDataWindow = new Window().setTimeUnit(TimeUnit.DAYS).setLength(5)
      ),
      inferenceSpec = B.InferenceSpec(modelBackend = ModelBackend.VertexAI)
    )

    val error = the[IllegalArgumentException] thrownBy new ModelPlanner(model).buildPlan
    error.getMessage should include("time_partitioned")
  }

  it should "allow sub-daily models over undeclared training sources marked time_partitioned" in {
    val source = B.Source.events(table = "tp_training_table", query = Query())
    source.getEvents.query.setTimePartitioned(true)
    val model = B.Model(
      metaData = B.MetaData(
        name = "tp_source_model",
        namespace = "test_namespace",
        executionInfo = executionInfoFor("test_namespace.tp_source_model", threeHourSpec)
      ),
      trainingSpec = B.TrainingSpec(
        trainingDataSource = source,
        trainingDataWindow = new Window().setTimeUnit(TimeUnit.DAYS).setLength(5)
      ),
      inferenceSpec = B.InferenceSpec(modelBackend = ModelBackend.VertexAI)
    )

    noException should be thrownBy new ModelPlanner(model).buildPlan
  }

  it should "reject sub-daily model intervals that are finer than the training source" in {
    val model = B.Model(
      metaData = B.MetaData(
        name = "invalid_hourly_model",
        namespace = "test_namespace",
        executionInfo = executionInfoFor("test_namespace.invalid_hourly_model", threeHourSpec)
      ),
      trainingSpec = B.TrainingSpec(
        trainingDataSource = sourceWithPartitionSpec("daily_training_data_table", PartitionSpec.daily),
        trainingDataWindow = new Window().setTimeUnit(TimeUnit.DAYS).setLength(1)
      ),
      inferenceSpec = B.InferenceSpec(modelBackend = ModelBackend.VertexAI)
    )

    an[IllegalArgumentException] should be thrownBy new ModelPlanner(model).buildPlan
  }

  it should "only create DEPLOY terminal node (not BACKFILL)" in {
    val model = buildModel("test_model")
    val planner = new ModelPlanner(model)
    val plan = planner.buildPlan

    // Verify terminal nodes - should only have DEPLOY
    plan.terminalNodeNames.asScala.size shouldBe 1
    plan.terminalNodeNames.containsKey(Mode.DEPLOY) shouldBe true
    plan.terminalNodeNames.containsKey(Mode.BACKFILL) shouldBe false

    // Verify DEPLOY points to deployModel node
    val deployModelNode = plan.nodes.asScala.find(_.content.isSetDeployModel)
    deployModelNode should be(defined)
    plan.terminalNodeNames.get(Mode.DEPLOY) shouldBe deployModelNode.get.metaData.name
  }

  it should "handle models with different backend types" in {
    val vertexModel = B.Model(
      metaData = B.MetaData(name = "vertex_model", namespace = "test_namespace"),
      inferenceSpec = B.InferenceSpec(
        modelBackend = ModelBackend.VertexAI,
        modelBackendParams = Map("project" -> "test-project")
      )
    )

    val sageMakerModel = B.Model(
      metaData = B.MetaData(name = "sagemaker_model", namespace = "test_namespace"),
      inferenceSpec = B.InferenceSpec(
        modelBackend = ModelBackend.SageMaker,
        modelBackendParams = Map("region" -> "us-west-2")
      )
    )

    val vertexPlanner = new ModelPlanner(vertexModel)
    val sageMakerPlanner = new ModelPlanner(sageMakerModel)

    noException should be thrownBy {
      vertexPlanner.buildPlan
      sageMakerPlanner.buildPlan
    }

    // Verify both plans are created successfully with 2 nodes each
    vertexPlanner.buildPlan.nodes.asScala should have size 2
    sageMakerPlanner.buildPlan.nodes.asScala should have size 2
  }
}
