package ai.chronon.api.test.planner

import ai.chronon.api.{Builders => B, _}
import ai.chronon.api.Extensions.{MetadataOps, TableInfoOps, WindowUtils}
import ai.chronon.api.planner.ModelTransformsPlanner
import ai.chronon.planner.Mode
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

class ModelTransformsPlannerTest extends AnyFlatSpec with Matchers {

  private implicit val testPartitionSpec: PartitionSpec = PartitionSpec.daily
  private val threeHourOffsetSpec =
    PartitionSpec("ds", "yyyy-MM-dd-HH-mm", 3 * 60 * 60 * 1000L, 60 * 60 * 1000L)

  private def modularExecutionInfo: ExecutionInfo =
    new ExecutionInfo().setConf(
      new ConfigProperties().setCommon(Map("modular_execution" -> "true").asJava)
    )

  private def buildModelTransforms(name: String, source: Source): ModelTransforms = {
    val testModel = B.Model(
      metaData = B.MetaData(name = s"${name}_model"),
      inferenceSpec = B.InferenceSpec(
        modelBackend = ModelBackend.VertexAI,
        modelBackendParams = Map("project" -> "test-project")
      )
    )

    B.ModelTransforms(
      metaData = B.MetaData(
        name = name,
        namespace = "test_namespace"
      ),
      sources = Seq(source),
      models = Seq(testModel)
    )
  }

  "ModelTransformsPlanner" should "create an upload node" in {
    val source = B.Source.events(
      query = B.Query(),
      table = "test_namespace.test_table"
    )

    val modelTransforms = buildModelTransforms("test_model_transforms", source)
    val planner = new ModelTransformsPlanner(modelTransforms)
    val plan = planner.buildPlan

    // Should create plan successfully with both upload and backfill nodes
    plan.nodes.asScala should have size 2

    // Find the upload node
    val uploadNode = plan.nodes.asScala.find(_.content.isSetModelTransformsUpload)

    uploadNode should be(defined)

    // Upload node should have content
    uploadNode.get.content should not be null
    uploadNode.get.content.getModelTransformsUpload should not be null
    uploadNode.get.content.getModelTransformsUpload.modelTransforms should not be null

    // Verify metadata
    uploadNode.get.metaData.name should equal(s"${modelTransforms.metaData.name}__model_transforms_upload")

    // Verify no table dependencies for upload node with non-JoinSource sources
    val deps = uploadNode.get.metaData.executionInfo.tableDependencies.asScala
    deps should be(empty)

    // Verify terminal nodes include DEPLOY
    plan.terminalNodeNames.containsKey(Mode.DEPLOY) shouldBe true
    plan.terminalNodeNames.get(Mode.DEPLOY) shouldBe uploadNode.get.metaData.name
  }

  "ModelTransformsPlanner" should "handle model transforms with join source and no derivations" in {
    val join = B.Join(
      left = B.Source.events(
        query = B.Query(),
        table = "test_namespace.events_table"
      ),
      joinParts = Seq.empty,
      metaData = B.MetaData(
        name = "test_join",
        namespace = "test_namespace"
      )
    )

    val joinSource = B.Source.joinSource(
      join = join,
      query = B.Query()
    )

    val modelTransforms = buildModelTransforms("test_model_transforms_with_join", joinSource)
    val planner = new ModelTransformsPlanner(modelTransforms)
    val plan = planner.buildPlan

    // Should create plan successfully with both upload and backfill nodes
    plan.nodes.asScala should have size 2

    val uploadNode = plan.nodes.asScala.find(_.content.isSetModelTransformsUpload)
    uploadNode should be(defined)

    // Verify the node has dependencies (should include the upstream join metadata upload)
    val deps = uploadNode.get.metaData.executionInfo.tableDependencies.asScala
    deps should not be empty

    val backfillNode = plan.nodes.asScala.find(_.content.isSetModelTransformsBackfill)
    backfillNode should be(defined)
    val backfillDeps = backfillNode.get.metaData.executionInfo.tableDependencies.asScala
    backfillDeps should not be empty
    backfillDeps.length shouldBe 1
    backfillDeps.head.tableInfo.table shouldBe "test_namespace.test_join"
  }

  it should "preserve upstream join output grid on join source backfill dependencies" in {
    val leftQuery = B.Query(partitionColumn = "ds")
    leftQuery.setPartitionInterval(WindowUtils.fromMillis(threeHourOffsetSpec.spanMillis))
    leftQuery.setPartitionOffset(WindowUtils.fromMillis(threeHourOffsetSpec.offsetMillis))

    val join = B.Join(
      left = B.Source.events(
        query = leftQuery,
        table = "test_namespace.events_table"
      ),
      joinParts = Seq.empty,
      metaData = B.MetaData(
        name = "test_join",
        namespace = "test_namespace"
      )
    )
    join.metaData.executionInfo.setOutputTableInfo(
      new TableInfo().setTable(join.metaData.outputTable).withSpec(threeHourOffsetSpec)
    )

    val joinSource = B.Source.joinSource(
      join = join,
      query = B.Query()
    )

    val modelTransforms = buildModelTransforms("test_model_transforms_with_subdaily_join", joinSource)
    modelTransforms.metaData.executionInfo.setOutputTableInfo(
      new TableInfo().setTable(modelTransforms.metaData.outputTable).withSpec(threeHourOffsetSpec)
    )
    val planner = new ModelTransformsPlanner(modelTransforms)
    val plan = planner.buildPlan

    val backfillNode = plan.nodes.asScala.find(_.content.isSetModelTransformsBackfill)
    backfillNode should be(defined)

    val backfillDeps = backfillNode.get.metaData.executionInfo.tableDependencies.asScala
    backfillDeps should have size 1

    val tableInfo = backfillDeps.head.tableInfo
    tableInfo.table shouldBe "test_namespace.test_join"
    tableInfo.partitionFormat shouldBe threeHourOffsetSpec.format
    tableInfo.partitionInterval shouldBe WindowUtils.fromMillis(threeHourOffsetSpec.spanMillis)
    tableInfo.partitionOffset shouldBe WindowUtils.fromMillis(threeHourOffsetSpec.offsetMillis)
  }

  it should "reject invalid grids inside embedded JoinSource trees" in {
    val join = B.Join(
      left = B.Source.events(
        query = B.Query(partitionColumn = "ds"),
        table = "test_namespace.daily_events_table"
      ),
      joinParts = Seq.empty,
      metaData = B.MetaData(
        name = "test_join",
        namespace = "test_namespace"
      )
    )
    join.metaData.executionInfo.setOutputTableInfo(
      new TableInfo().setTable(join.metaData.outputTable).withSpec(threeHourOffsetSpec)
    )

    val joinSource = B.Source.joinSource(
      join = join,
      query = B.Query()
    )

    val modelTransforms = buildModelTransforms("test_model_transforms_with_invalid_join_source", joinSource)
    modelTransforms.metaData.executionInfo.setOutputTableInfo(
      new TableInfo().setTable(modelTransforms.metaData.outputTable).withSpec(threeHourOffsetSpec)
    )

    val error = the[IllegalArgumentException] thrownBy new ModelTransformsPlanner(modelTransforms).buildPlan
    error.getMessage should include("test_join")
    error.getMessage should include("no declared partition_interval")
  }

  "ModelTransformsPlanner" should "use the base join output for join sources with derivations in non-modular mode" in {
    val join = B.Join(
      left = B.Source.events(
        query = B.Query(),
        table = "test_namespace.events_table"
      ),
      joinParts = Seq.empty,
      metaData = B.MetaData(
        name = "test_join",
        namespace = "test_namespace"
      ),
      derivations = Seq(
        B.Derivation(
          name = "derived_col",
          expression = "some-expression")
      )
    )

    val joinSource = B.Source.joinSource(
      join = join,
      query = B.Query()
    )

    val modelTransforms = buildModelTransforms("test_model_transforms_with_join", joinSource)
    val planner = new ModelTransformsPlanner(modelTransforms)
    val plan = planner.buildPlan

    // Should create plan successfully with both upload and backfill nodes
    plan.nodes.asScala should have size 2

    val uploadNode = plan.nodes.asScala.find(_.content.isSetModelTransformsUpload)
    uploadNode should be(defined)

    // Verify the node has dependencies (should include the upstream join metadata upload)
    val deps = uploadNode.get.metaData.executionInfo.tableDependencies.asScala
    deps should not be empty

    val backfillNode = plan.nodes.asScala.find(_.content.isSetModelTransformsBackfill)
    backfillNode should be(defined)
    val backfillDeps = backfillNode.get.metaData.executionInfo.tableDependencies.asScala
    backfillDeps should not be empty
    backfillDeps.length shouldBe 1
    backfillDeps.head.tableInfo.table shouldBe "test_namespace.test_join"
  }

  "ModelTransformsPlanner" should "use the derived join output for join sources with derivations in modular mode when a separate derivation node is planned" in {
    val join = B.Join(
      left = B.Source.events(
        query = B.Query(),
        table = "test_namespace.events_table"
      ),
      joinParts = Seq.empty,
      metaData = B.MetaData(
        name = "test_join",
        namespace = "test_namespace",
        executionInfo = modularExecutionInfo
      ),
      derivations = Seq(
        B.Derivation(
          name = "derived_col",
          expression = "some-expression")
      )
    )

    val joinSource = B.Source.joinSource(
      join = join,
      query = B.Query()
    )

    val modelTransforms = buildModelTransforms("test_model_transforms_with_join", joinSource)
    val planner = new ModelTransformsPlanner(modelTransforms)
    val plan = planner.buildPlan

    plan.nodes.asScala should have size 2

    val backfillNode = plan.nodes.asScala.find(_.content.isSetModelTransformsBackfill)
    backfillNode should be(defined)
    val backfillDeps = backfillNode.get.metaData.executionInfo.tableDependencies.asScala
    backfillDeps should not be empty
    backfillDeps.length shouldBe 1
    backfillDeps.head.tableInfo.table shouldBe "test_namespace.test_join__derived"
  }

  "ModelTransformsPlanner" should "use the base join output for union-join-eligible modular join sources with derivations" in {
    val joinPartGroupBy = B.GroupBy(
      sources = Seq(B.Source.events(
        query = B.Query(startPartition = "2023-06-01", partitionColumn = "ds"),
        table = "test_namespace.feature_events"
      )),
      keyColumns = Seq("user_id"),
      aggregations = Seq(B.Aggregation(Operation.LAST, "listing_id")),
      accuracy = Accuracy.TEMPORAL,
      metaData = B.MetaData(name = "test_group_by", namespace = "test_namespace")
    )

    val join = B.Join(
      left = B.Source.events(
        query = B.Query(
          selects = B.Selects("user_id", "ts"),
          startPartition = "2023-06-01",
          partitionColumn = "ds"
        ),
        table = "test_namespace.events_table"
      ),
      joinParts = Seq(B.JoinPart(groupBy = joinPartGroupBy)),
      metaData = B.MetaData(
        name = "test_join",
        namespace = "test_namespace",
        executionInfo = modularExecutionInfo
      ),
      derivations = Seq(
        B.Derivation(
          name = "derived_col",
          expression = "test_group_by_listing_id_last")
      )
    )

    val joinSource = B.Source.joinSource(
      join = join,
      query = B.Query()
    )

    val modelTransforms = buildModelTransforms("test_model_transforms_with_union_join_source", joinSource)
    val planner = new ModelTransformsPlanner(modelTransforms)
    val plan = planner.buildPlan

    plan.nodes.asScala should have size 2

    val backfillNode = plan.nodes.asScala.find(_.content.isSetModelTransformsBackfill)
    backfillNode should be(defined)
    val backfillDeps = backfillNode.get.metaData.executionInfo.tableDependencies.asScala
    backfillDeps should not be empty
    backfillDeps.length shouldBe 1
    backfillDeps.head.tableInfo.table shouldBe "test_namespace.test_join"
  }



  it should "create both backfill and upload nodes" in {
    val source = B.Source.events(
      query = B.Query(),
      table = "test_namespace.test_table"
    )

    val modelTransforms = buildModelTransforms("test_model_transforms", source)
    val planner = new ModelTransformsPlanner(modelTransforms)
    val plan = planner.buildPlan

    // Should create plan with both nodes
    plan.nodes.asScala should have size 2

    // Find both nodes
    val uploadNode = plan.nodes.asScala.find(_.content.isSetModelTransformsUpload)
    val backfillNode = plan.nodes.asScala.find(_.content.isSetModelTransformsBackfill)

    uploadNode should be(defined)
    backfillNode should be(defined)

    // Verify metadata
    uploadNode.get.metaData.name should equal(s"${modelTransforms.metaData.name}__model_transforms_upload")
    backfillNode.get.metaData.name should equal(s"${modelTransforms.metaData.name}__model_transforms_backfill")

    // Verify terminal nodes
    plan.terminalNodeNames.asScala.size shouldBe 2
    plan.terminalNodeNames.containsKey(Mode.DEPLOY) shouldBe true
    plan.terminalNodeNames.containsKey(Mode.BACKFILL) shouldBe true
    plan.terminalNodeNames.get(Mode.DEPLOY) shouldBe uploadNode.get.metaData.name
    plan.terminalNodeNames.get(Mode.BACKFILL) shouldBe backfillNode.get.metaData.name
  }

  it should "create backfill node with table dependencies for events source" in {
    val source = B.Source.events(
      query = B.Query(),
      table = "test_namespace.test_table"
    )

    val modelTransforms = buildModelTransforms("test_model_transforms", source)
    val planner = new ModelTransformsPlanner(modelTransforms)
    val backfillNode = planner.backfillNode

    // Verify backfill node content
    backfillNode.content.isSetModelTransformsBackfill shouldBe true
    backfillNode.content.getModelTransformsBackfill.modelTransforms should not be null

    // Verify table dependencies for events source
    val deps = backfillNode.metaData.executionInfo.tableDependencies.asScala
    deps should have size 1
    deps.head.tableInfo.table shouldBe "test_namespace.test_table"
  }

  it should "create backfill node with table dependencies for join source" in {
    val joinOutputTable = "test_namespace.test_join_output"
    val joinMetadata = B.MetaData(
      name = "test_join",
      namespace = "test_namespace"
    )
    val outputTableInfo = new TableInfo().setTable(joinOutputTable)
    val executionInfo = new ExecutionInfo().setOutputTableInfo(outputTableInfo)
    joinMetadata.setExecutionInfo(executionInfo)

    val join = B.Join(
      left = B.Source.events(
        query = B.Query(),
        table = "test_namespace.events_table"
      ),
      joinParts = Seq.empty,
      metaData = joinMetadata
    )

    val joinSource = B.Source.joinSource(
      join = join,
      query = B.Query()
    )

    val modelTransforms = buildModelTransforms("test_model_transforms_with_join", joinSource)
    val planner = new ModelTransformsPlanner(modelTransforms)
    val backfillNode = planner.backfillNode

    // Verify backfill node content
    backfillNode.content.isSetModelTransformsBackfill shouldBe true

    // Verify table dependencies point to join output table
    val deps = backfillNode.metaData.executionInfo.tableDependencies.asScala
    deps should have size 1
    deps.head.tableInfo.table shouldBe joinOutputTable
  }

  it should "preserve join source query in table dependencies" in {
    val joinOutputTable = "test_namespace.test_join_output"
    val joinMetadata = B.MetaData(
      name = "test_join",
      namespace = "test_namespace"
    )
    val outputTableInfo = new TableInfo().setTable(joinOutputTable)
    val executionInfo = new ExecutionInfo().setOutputTableInfo(outputTableInfo)
    joinMetadata.setExecutionInfo(executionInfo)

    val join = B.Join(
      left = B.Source.events(
        query = B.Query(),
        table = "test_namespace.events_table"
      ),
      joinParts = Seq.empty,
      metaData = joinMetadata
    )

    val joinSourceQuery = B.Query(
      selects = Map(
        "user_id" -> "user_id",
        "renamed_col" -> "original_col"
      )
    )

    val joinSource = B.Source.joinSource(
      join = join,
      query = joinSourceQuery
    )

    val modelTransforms = buildModelTransforms("test_model_transforms_with_query", joinSource)
    val planner = new ModelTransformsPlanner(modelTransforms)
    val backfillNode = planner.backfillNode

    // Verify table dependencies include the query information
    val deps = backfillNode.metaData.executionInfo.tableDependencies.asScala
    deps should have size 1

    val tableDep = deps.head
    tableDep.tableInfo.table shouldBe joinOutputTable
  }

  it should "handle multiple sources in backfill node" in {
    val source1 = B.Source.events(
      query = B.Query(),
      table = "test_namespace.test_table_1"
    )

    val joinMetadata = B.MetaData(
      name = "test_join",
      namespace = "test_namespace"
    )
    val outputTableInfo = new TableInfo().setTable("test_namespace.test_join_output")
    val executionInfo = new ExecutionInfo().setOutputTableInfo(outputTableInfo)
    joinMetadata.setExecutionInfo(executionInfo)

    val join = B.Join(
      left = B.Source.events(
        query = B.Query(),
        table = "test_namespace.events_table"
      ),
      joinParts = Seq.empty,
      metaData = joinMetadata
    )

    val source2 = B.Source.joinSource(
      join = join,
      query = B.Query()
    )

    val testModel = B.Model(
      metaData = B.MetaData(name = "test_model"),
      inferenceSpec = B.InferenceSpec(
        modelBackend = ModelBackend.VertexAI,
        modelBackendParams = Map("project" -> "test-project")
      )
    )

    val modelTransforms = B.ModelTransforms(
      metaData = B.MetaData(
        name = "test_model_transforms_multi_source",
        namespace = "test_namespace"
      ),
      sources = Seq(source1, source2),
      models = Seq(testModel)
    )

    val planner = new ModelTransformsPlanner(modelTransforms)
    val backfillNode = planner.backfillNode

    // Verify table dependencies include both sources
    val deps = backfillNode.metaData.executionInfo.tableDependencies.asScala
    deps should have size 2

    val tableNames = deps.map(_.tableInfo.table).toSet
    tableNames should contain("test_namespace.test_table_1")
    tableNames should contain("test_namespace.test_join_output")
  }

  it should "create backfill node with model dependencies when models have training configs" in {
    val source = B.Source.events(
      query = B.Query(),
      table = "test_namespace.test_table"
    )

    // Create a model with training config
    val modelWithTraining = B.Model(
      metaData = B.MetaData(
        name = "test_model_with_training",
        namespace = "test_namespace"
      ),
      trainingSpec = B.TrainingSpec(
        trainingDataSource = B.Source.events(table = "training_data_table", query = B.Query())
      ),
      inferenceSpec = B.InferenceSpec(
        modelBackend = ModelBackend.VertexAI,
        modelBackendParams = Map("project" -> "test-project")
      )
    )

    // Create a model without training config
    val modelWithoutTraining = B.Model(
      metaData = B.MetaData(
        name = "test_model_without_training",
        namespace = "test_namespace"
      ),
      inferenceSpec = B.InferenceSpec(
        modelBackend = ModelBackend.VertexAI,
        modelBackendParams = Map("project" -> "test-project")
      )
    )

    val modelTransforms = B.ModelTransforms(
      metaData = B.MetaData(
        name = "test_model_transforms_with_model_deps",
        namespace = "test_namespace"
      ),
      sources = Seq(source),
      models = Seq(modelWithTraining, modelWithoutTraining)
    )

    val planner = new ModelTransformsPlanner(modelTransforms)
    val backfillNode = planner.backfillNode

    // Verify table dependencies include both the source and the deployed model node
    val deps = backfillNode.metaData.executionInfo.tableDependencies.asScala
    deps should have size 2

    val tableNames = deps.map(_.tableInfo.table).toSet
    // Should include the source table
    tableNames should contain("test_namespace.test_table")
    // Should include the deployed model node for the model with training config
    tableNames should contain("test_namespace.test_model_with_training__model_deploy")
  }
}
