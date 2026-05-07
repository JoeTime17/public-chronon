package ai.chronon.api.test.planner

import ai.chronon.api
import ai.chronon.api.Builders.{Join, MetaData}
import ai.chronon.api.planner.{GroupByPlanner, LocalRunner, MonolithJoinPlanner}
import ai.chronon.api.{Builders, ExecutionInfo, PartitionSpec}
import ai.chronon.planner.{ConfPlan, Mode}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import ai.chronon.api.Extensions._

import java.nio.file.Paths
import scala.jdk.CollectionConverters._

class MonolithJoinPlannerTest extends AnyFlatSpec with Matchers {

  private implicit val testPartitionSpec: PartitionSpec = PartitionSpec.daily

  private def validateJoinPlan(plan: ConfPlan): Unit = {
    // Should create plan successfully with both backfill and metadata upload nodes

    // Find the backfill node and metadata upload node
    val backfillNode = plan.nodes.asScala.find(_.content.isSetMonolithJoin)
    val metadataUploadNode = plan.nodes.asScala.find(_.content.isSetJoinMetadataUpload)

    backfillNode should be(defined)
    metadataUploadNode should be(defined)

    // Backfill node should have content
    backfillNode.get.content should not be null
    backfillNode.get.content.getMonolithJoin should not be null
    backfillNode.get.content.getMonolithJoin.join should not be null

    // Metadata upload node should have content
    metadataUploadNode.get.content should not be null
    metadataUploadNode.get.content.getJoinMetadataUpload should not be null
    metadataUploadNode.get.content.getJoinMetadataUpload.join should not be null

    plan.terminalNodeNames.asScala.size shouldBe 2
    plan.terminalNodeNames.containsKey(Mode.DEPLOY) shouldBe true
    plan.terminalNodeNames.containsKey(Mode.BACKFILL) shouldBe true

    // Validate that no node names contain forward slashes
    plan.nodes.asScala.foreach { node =>
      val nodeName = node.metaData.name
      withClue(s"Node name '$nodeName' contains forward slash") {
        nodeName should not contain "/"
      }
    }
  }

  it should "monolith join planner plans valid confs without exceptions" in {

    val rootDir = Paths.get(getClass.getClassLoader.getResource("canary/compiled/joins").getPath)

    val joinConfs = LocalRunner.parseConfs[api.Join](rootDir.toString)

    val joinPlanners = joinConfs.map(MonolithJoinPlanner(_))

    joinPlanners
      .foreach { planner =>
        noException should be thrownBy {
          val plan = planner.buildPlan
          validateJoinPlan(plan)
        }
      }
  }

  it should "monolith join should avoid metadata when computing semantic hash" in {
    val firstJoin = Join(
      metaData = MetaData(name = "firstJoin", executionInfo = new ExecutionInfo().setStepDays(2)),
      left = Builders.Source.events(Builders.Query(), table = "test_namespace.join_table"),
      joinParts = Seq.empty,
      bootstrapParts = Seq.empty
    )

    val secondJoin = Join(
      metaData = MetaData(name = "secondJoin", executionInfo = new ExecutionInfo().setStepDays(1)),
      left = Builders.Source.events(Builders.Query(), table = "test_namespace.join_table"),
      joinParts = Seq.empty,
      bootstrapParts = Seq.empty
    )
    val firstPlan = MonolithJoinPlanner(firstJoin).buildPlan
    val secondPlan = MonolithJoinPlanner(secondJoin).buildPlan

    // Semantic hashes should be identical since metadata is excluded
    val firstSemanticHashes = firstPlan.nodes.asScala.map(_.semanticHash)
    val secondSemanticHashes = secondPlan.nodes.asScala.map(_.semanticHash)
    firstSemanticHashes should equal(secondSemanticHashes)
  }

  it should "monolith join planner should create valid terminal node names" in {
    val join = Join(
      metaData = MetaData(name = "testJoin"),
      left = Builders.Source.events(Builders.Query(), table = "test_namespace.test_join_table"),
      joinParts = Seq.empty,
      bootstrapParts = Seq.empty
    )

    val planner = MonolithJoinPlanner(join)
    val plan = planner.buildPlan

    plan.terminalNodeNames.asScala should contain key ai.chronon.planner.Mode.BACKFILL
    plan.terminalNodeNames.asScala should contain key ai.chronon.planner.Mode.DEPLOY
    plan.terminalNodeNames.asScala(ai.chronon.planner.Mode.BACKFILL) should equal("testJoin__monolith_join")
    plan.terminalNodeNames.asScala(ai.chronon.planner.Mode.DEPLOY) should equal("testJoin__metadata_upload")
  }

  it should "monolith join planner should respect step days from execution info" in {
    val joinWithStepDays = Join(
      metaData = MetaData(name = "testJoin", executionInfo = new ExecutionInfo().setStepDays(5)),
      left = Builders.Source.events(Builders.Query(), table = "test_namespace.test_join_with_step_days_table"),
      joinParts = Seq.empty,
      bootstrapParts = Seq.empty
    )

    val joinWithoutStepDays = Join(
      metaData = MetaData(name = "testJoin2"),
      left = Builders.Source.events(Builders.Query(), table = "test_namespace.test_join_without_step_days_table"),
      joinParts = Seq.empty,
      bootstrapParts = Seq.empty
    )

    val plannerWithStepDays = MonolithJoinPlanner(joinWithStepDays)
    val plannerWithoutStepDays = MonolithJoinPlanner(joinWithoutStepDays)

    noException should be thrownBy {
      plannerWithStepDays.buildPlan
      plannerWithoutStepDays.buildPlan
    }
  }

  it should "set nonzero step days" in {
    val joinWithNonZeroStepDays = Join(
      metaData = MetaData(name = "testJoin", executionInfo = new ExecutionInfo()),
      left = Builders.Source.events(Builders.Query(), table = "test_namespace.test_join_non_zero_step_days_table"),
      joinParts = Seq.empty,
      bootstrapParts = Seq.empty
    )

    val plannerWithNonZeroStepDays = MonolithJoinPlanner(joinWithNonZeroStepDays)
    val plan = plannerWithNonZeroStepDays.buildPlan
    plan.nodes.asScala.foreach((node) => node.metaData.executionInfo.stepDays should equal(1))
  }

  it should "monolith join planner should produce same semantic hash with different executionInfo" in {
    val joinWithExecutionInfo1 = Join(
      metaData = MetaData(
        name = "testJoin1",
        executionInfo = new ExecutionInfo().setStepDays(3)
      ),
      left = Builders.Source.events(Builders.Query(), table = "test_namespace.test_join_execution_info_table"),
      joinParts = Seq.empty,
      bootstrapParts = Seq.empty
    )

    val joinWithExecutionInfo2 = Join(
      metaData = MetaData(
        name = "testJoin2",
        executionInfo = new ExecutionInfo().setStepDays(7)
      ),
      left = Builders.Source.events(Builders.Query(), table = "test_namespace.test_join_execution_info_table"),
      joinParts = Seq.empty,
      bootstrapParts = Seq.empty
    )

    val firstPlan = MonolithJoinPlanner(joinWithExecutionInfo1).buildPlan
    val secondPlan = MonolithJoinPlanner(joinWithExecutionInfo2).buildPlan

    // Semantic hashes should be identical since executionInfo is part of metadata and excluded
    val firstSemanticHashes = firstPlan.nodes.asScala.map(_.semanticHash)
    val secondSemanticHashes = secondPlan.nodes.asScala.map(_.semanticHash)
    firstSemanticHashes should equal(secondSemanticHashes)
  }

  it should "stats compute node should inherit stepDays from executionInfo" in {
    val joinWithStepDays = Join(
      metaData = MetaData(
        name = "testStatsJoin",
        executionInfo = new ExecutionInfo().setStepDays(7).setEnableStatsCompute(true)
      ),
      left = Builders.Source.events(Builders.Query(), table = "test_namespace.stats_step_days_table"),
      joinParts = Seq.empty,
      bootstrapParts = Seq.empty
    )

    val plan = MonolithJoinPlanner(joinWithStepDays).buildPlan

    val statsNode = plan.nodes.asScala.find(_.content.isSetJoinStatsCompute)
    statsNode should be(defined)
    statsNode.get.metaData.executionInfo.stepDays should equal(7)

    // Backfill terminal should be the stats node when stats are enabled
    plan.terminalNodeNames.asScala(ai.chronon.planner.Mode.BACKFILL) should equal("testStatsJoin__stats_compute")
  }

  it should "stats compute node should default to stepDays=1 when executionInfo has no stepDays" in {
    val joinNoStepDays = Join(
      metaData = MetaData(
        name = "testStatsJoinDefault",
        executionInfo = new ExecutionInfo().setEnableStatsCompute(true)
      ),
      left = Builders.Source.events(Builders.Query(), table = "test_namespace.stats_default_step_days_table"),
      joinParts = Seq.empty,
      bootstrapParts = Seq.empty
    )

    val plan = MonolithJoinPlanner(joinNoStepDays).buildPlan

    val statsNode = plan.nodes.asScala.find(_.content.isSetJoinStatsCompute)
    statsNode should be(defined)
    statsNode.get.metaData.executionInfo.stepDays should equal(1)
  }

  it should "monolith join planner should produce exactly two nodes (backfill and metadata upload) for canary confs" in {
    val rootDir = Paths.get(getClass.getClassLoader.getResource("canary/compiled/joins").getPath)

    val joinConfs = LocalRunner.parseConfs[api.Join](rootDir.toString)

    joinConfs.foreach { joinConf =>
      val planner = MonolithJoinPlanner(joinConf)
      val plan = planner.buildPlan

      validateJoinPlan(plan)
    }
  }

  it should "monolith join planner should create metadata upload node with correct properties" in {
    val join = Join(
      metaData = MetaData(name = "testJoin"),
      left = Builders.Source.events(Builders.Query(), table = s"test_namespace.test_table"),
      joinParts = Seq.empty,
      bootstrapParts = Seq.empty
    )

    val planner = MonolithJoinPlanner(join)
    val plan = planner.buildPlan

    validateJoinPlan(plan)

    val metadataUploadNode = plan.nodes.asScala.find(_.content.isSetJoinMetadataUpload).get

    // Verify metadata upload node name
    metadataUploadNode.metaData.name should equal("testJoin__metadata_upload")

    // Verify the wrapped join is correct
    metadataUploadNode.content.getJoinMetadataUpload.join should equal(join)
  }

  it should "include mutation table dependencies and sensors for temporal entity join parts when the left side is events" in {
    import ai.chronon.api.Builders._

    val entityQuery = Query(
      startPartition = "2025-01-01",
      partitionColumn = "ds"
    )
    entityQuery.setPartitionInterval(WindowUtils.Day)

    val temporalEntityGroupBy = GroupBy(
      sources = Seq(Source.entities(
        query = entityQuery,
        snapshotTable = "test.dim_snapshot",
        mutationTable = "test.dim_mutations"
      )),
      keyColumns = Seq("listing_id"),
      aggregations = Seq(Aggregation(ai.chronon.api.Operation.LAST, "headline", Seq(WindowUtils.Unbounded))),
      accuracy = ai.chronon.api.Accuracy.TEMPORAL,
      metaData = MetaData(namespace = "test_namespace", name = "temporal_entity_gb")
    )

    val join = Join(
      metaData = MetaData(name = "temporal_entity_join"),
      left = Source.events(Query(partitionColumn = "ds"), table = "test.left_events"),
      joinParts = Seq(new ai.chronon.api.JoinPart().setGroupBy(temporalEntityGroupBy)),
      bootstrapParts = Seq.empty
    )

    val plan = MonolithJoinPlanner(join).buildPlan

    val backfillNode = plan.nodes.asScala.find(_.content.isSetMonolithJoin).get
    val tableDeps = backfillNode.metaData.executionInfo.tableDependencies.asScala

    tableDeps.map(_.tableInfo.table) should equal(
      Seq("test.left_events", "test.dim_snapshot", "test.dim_mutations")
    )
    tableDeps(1).startOffset should equal(WindowUtils.Day)
    tableDeps(1).endOffset should equal(WindowUtils.Day)
    tableDeps(2).startOffset should equal(WindowUtils.zero())
    tableDeps(2).endOffset should equal(WindowUtils.zero())

    val sensorOutputTables = plan.nodes.asScala
      .filter(_.content.isSetExternalSourceSensor)
      .map(_.content.getExternalSourceSensor.metaData.executionInfo.outputTableInfo.table)
      .toSet

    sensorOutputTables should equal(
      Set("test.left_events", "test.dim_snapshot", "test.dim_mutations")
    )
  }

  it should "monolith join planner should skip metadata in semantic hash for both nodes" in {
    val firstJoin = Join(
      metaData = MetaData(name = "firstJoin", executionInfo = new ExecutionInfo().setStepDays(2)),
      left = Builders.Source.events(Builders.Query(), table = "test_namespace.join_semantic_hash_table"),
      joinParts = Seq.empty,
      bootstrapParts = Seq.empty
    )

    val secondJoin = Join(
      metaData = MetaData(name = "secondJoin", executionInfo = new ExecutionInfo().setStepDays(1)),
      left = Builders.Source.events(Builders.Query(), table = "test_namespace.join_semantic_hash_table"),
      joinParts = Seq.empty,
      bootstrapParts = Seq.empty
    )

    val firstPlan = MonolithJoinPlanner(firstJoin).buildPlan
    val secondPlan = MonolithJoinPlanner(secondJoin).buildPlan

    // Semantic hashes should be identical for both backfill and metadata upload nodes
    val firstBackfillHash = firstPlan.nodes.asScala.find(_.content.isSetMonolithJoin).get.semanticHash
    val secondBackfillHash = secondPlan.nodes.asScala.find(_.content.isSetMonolithJoin).get.semanticHash
    firstBackfillHash should equal(secondBackfillHash)

    val firstMetadataUploadHash = firstPlan.nodes.asScala.find(_.content.isSetJoinMetadataUpload).get.semanticHash
    val secondMetadataUploadHash = secondPlan.nodes.asScala.find(_.content.isSetJoinMetadataUpload).get.semanticHash
    firstMetadataUploadHash should equal(secondMetadataUploadHash)
  }

  it should "metadata upload node should depend on streaming GroupBy nodes when join parts have streaming sources" in {
    import ai.chronon.api.Builders._

    // Create a streaming GroupBy (has topic)
    val streamingGroupBy = GroupBy(
      sources = Seq(Source.events(Query(), table = "test_namespace.events_table", topic = "events_topic")),
      keyColumns = Seq("user_id"),
      aggregations = Seq(Aggregation(ai.chronon.api.Operation.COUNT, "event_count")),
      metaData = MetaData(namespace = "test_namespace", name = "streaming_gb")
    )

    val joinPart = new ai.chronon.api.JoinPart()
      .setGroupBy(streamingGroupBy)

    val join = Join(
      metaData = MetaData(name = "testJoinWithStreaming"),
      left = Source.events(Query(), table = "test_namespace.left_table"),
      joinParts = Seq(joinPart),
      bootstrapParts = Seq.empty
    )

    val planner = MonolithJoinPlanner(join)
    val plan = planner.buildPlan

    validateJoinPlan(plan)

    val metadataUploadNode = plan.nodes.asScala.find(_.content.isSetJoinMetadataUpload).get

    // Should have dependencies on streaming GroupBy node
    val tableDeps = metadataUploadNode.metaData.executionInfo.tableDependencies.asScala
    tableDeps should not be empty
    val streamingDep = tableDeps.head
    streamingDep.tableInfo.table should equal(streamingGroupBy.metaData.outputTable + "__streaming")
  }

  it should "metadata upload node should depend on uploadToKV GroupBy nodes when join parts have non-streaming sources" in {
    import ai.chronon.api.Builders._

    // Create a non-streaming GroupBy (no topic)
    val nonStreamingGroupBy = GroupBy(
      sources = Seq(Source.events(Query(), table = "test_namespace.events_table")),
      keyColumns = Seq("user_id"),
      aggregations = Seq(Aggregation(ai.chronon.api.Operation.COUNT, "event_count")),
      metaData = MetaData(namespace = "test_namespace", name = "non_streaming_gb")
    )

    val joinPart = new ai.chronon.api.JoinPart()
      .setGroupBy(nonStreamingGroupBy)

    val join = Join(
      metaData = MetaData(name = "testJoinWithNonStreaming"),
      left = Source.events(Query(), table = "test_namespace.left_table"),
      joinParts = Seq(joinPart),
      bootstrapParts = Seq.empty
    )

    val planner = MonolithJoinPlanner(join)
    val plan = planner.buildPlan

    validateJoinPlan(plan)

    val metadataUploadNode = plan.nodes.asScala.find(_.content.isSetJoinMetadataUpload).get

    // Should have dependencies on uploadToKV GroupBy node
    val tableDeps = metadataUploadNode.metaData.executionInfo.tableDependencies.asScala
    tableDeps should not be empty
    val uploadToKVDep = tableDeps.head
    uploadToKVDep.tableInfo.table should equal(nonStreamingGroupBy.metaData.outputTable + "__uploadToKV")
  }

  it should "metadata upload node should handle mixed streaming and non-streaming GroupBy dependencies" in {
    import ai.chronon.api.Builders._

    // Create a streaming GroupBy
    val streamingGroupBy = GroupBy(
      sources = Seq(Source.events(Query(), table = "test_namespace.streaming_events", topic = "streaming_topic")),
      keyColumns = Seq("user_id"),
      aggregations = Seq(Aggregation(ai.chronon.api.Operation.COUNT, "stream_count")),
      metaData = MetaData(namespace = "test_namespace", name = "mixed_streaming_gb")
    )

    // Create a non-streaming GroupBy
    val nonStreamingGroupBy = GroupBy(
      sources = Seq(Source.events(Query(), table = "test_namespace.batch_events")),
      keyColumns = Seq("user_id"),
      aggregations = Seq(Aggregation(ai.chronon.api.Operation.SUM, "batch_sum")),
      metaData = MetaData(namespace = "test_namespace", name = "mixed_batch_gb")
    )

    val streamingJoinPart = new ai.chronon.api.JoinPart()
      .setGroupBy(streamingGroupBy)

    val nonStreamingJoinPart = new ai.chronon.api.JoinPart()
      .setGroupBy(nonStreamingGroupBy)

    val join = Join(
      metaData = MetaData(name = "testJoinMixed"),
      left = Source.events(Query(), table = "test_namespace.left_table"),
      joinParts = Seq(streamingJoinPart, nonStreamingJoinPart),
      bootstrapParts = Seq.empty
    )

    val planner = MonolithJoinPlanner(join)
    val plan = planner.buildPlan

    validateJoinPlan(plan)

    val metadataUploadNode = plan.nodes.asScala.find(_.content.isSetJoinMetadataUpload).get

    // Should have dependencies on both streaming and uploadToKV nodes
    val tableDeps = metadataUploadNode.metaData.executionInfo.tableDependencies.asScala
    tableDeps should not be empty
    tableDeps should have size 2

    val depTables = tableDeps.map(_.tableInfo.table).toSet
    depTables should contain(streamingGroupBy.metaData.outputTable + "__streaming")
    depTables should contain(nonStreamingGroupBy.metaData.outputTable + "__uploadToKV")
  }

  it should "metadata upload node should have no GroupBy dependencies when join has no join parts" in {
    val join = Join(
      metaData = MetaData(name = "testJoinNoJoinParts"),
      left = Builders.Source.events(Builders.Query(), table = "test_namespace.left_only_table"),
      joinParts = Seq.empty,
      bootstrapParts = Seq.empty
    )

    val planner = MonolithJoinPlanner(join)
    val plan = planner.buildPlan

    validateJoinPlan(plan)

    val metadataUploadNode = plan.nodes.asScala.find(_.content.isSetJoinMetadataUpload).get

    // Should have no GroupBy dependencies
    val tableDeps = metadataUploadNode.metaData.executionInfo.tableDependencies.asScala
    tableDeps.size should be(0)
  }

  // Join Chaining Tests

  it should "include upstream join dependency when GroupBy has JoinSource" in {
    import ai.chronon.api.Builders._

    // Create upstream GroupBy
    val upstreamGroupBy = GroupBy(
      sources = Seq(Source.events(Query(), table = "test_namespace.raw_events")),
      keyColumns = Seq("user_id"),
      aggregations = Seq(Aggregation(ai.chronon.api.Operation.COUNT, "event_count")),
      metaData = MetaData(namespace = "test_namespace", name = "upstream_gb")
    )

    // Create upstream Join
    val upstreamJoinPart = new ai.chronon.api.JoinPart()
      .setGroupBy(upstreamGroupBy)

    val upstreamJoin = Join(
      metaData = MetaData(namespace = "test_namespace", name = "upstream_join"),
      left = Source.events(Query(), table = "test_namespace.left_events"),
      joinParts = Seq(upstreamJoinPart),
      bootstrapParts = Seq.empty
    )

    // Create downstream GroupBy that uses JoinSource
    val downstreamGroupBy = GroupBy(
      sources = Seq(Source.joinSource(upstreamJoin, Query())),
      keyColumns = Seq("user_id"),
      aggregations = Seq(Aggregation(ai.chronon.api.Operation.SUM, "total_count")),
      metaData = MetaData(namespace = "test_namespace", name = "downstream_gb")
    )

    // Create downstream Join
    val downstreamJoinPart = new ai.chronon.api.JoinPart()
      .setGroupBy(downstreamGroupBy)

    val downstreamJoin = Join(
      metaData = MetaData(name = "downstream_join"),
      left = Source.events(Query(), table = "test_namespace.downstream_left"),
      joinParts = Seq(downstreamJoinPart),
      bootstrapParts = Seq.empty
    )

    val planner = MonolithJoinPlanner(downstreamJoin)
    val plan = planner.buildPlan

    validateJoinPlan(plan)

    // Verify dependencies in metadata upload node
    val metadataUploadNode = plan.nodes.asScala.find(_.content.isSetJoinMetadataUpload).get
    val tableDeps = metadataUploadNode.metaData.executionInfo.tableDependencies.asScala

    // Should have dependencies on both downstream GroupBy and upstream join
    tableDeps.size should be >= 2

    // Should have dependency on downstream GroupBy (uploadToKV table)
    val groupByDep = tableDeps.find(_.tableInfo.table.contains("downstream_gb__uploadToKV"))
    groupByDep should be(defined)
    
    // Should have dependency on upstream join's metadata upload
    val upstreamJoinDep = tableDeps.find(_.tableInfo.table.contains("upstream_join__metadata_upload"))
    upstreamJoinDep should be(defined)
  }

  it should "handle multiple upstream join dependencies correctly" in {
    import ai.chronon.api.Builders._

    // Create first upstream join chain
    val upstreamGroupBy1 = GroupBy(
      sources = Seq(Source.events(Query(), table = "test_namespace.events1")),
      keyColumns = Seq("user_id"),
      aggregations = Seq(Aggregation(ai.chronon.api.Operation.COUNT, "count1")),
      metaData = MetaData(namespace = "test_namespace", name = "upstream_gb1")
    )

    val upstreamJoin1 = Join(
      metaData = MetaData(namespace = "test_namespace", name = "upstream_join1"),
      left = Source.events(Query(), table = "test_namespace.left1"),
      joinParts = Seq(new ai.chronon.api.JoinPart().setGroupBy(upstreamGroupBy1)),
      bootstrapParts = Seq.empty
    )

    // Create second upstream join chain
    val upstreamGroupBy2 = GroupBy(
      sources = Seq(Source.events(Query(), table = "test_namespace.events2")),
      keyColumns = Seq("user_id"),
      aggregations = Seq(Aggregation(ai.chronon.api.Operation.SUM, "sum2")),
      metaData = MetaData(namespace = "test_namespace", name = "upstream_gb2")
    )

    val upstreamJoin2 = Join(
      metaData = MetaData(namespace = "test_namespace", name = "upstream_join2"),
      left = Source.events(Query(), table = "test_namespace.left2"),
      joinParts = Seq(new ai.chronon.api.JoinPart().setGroupBy(upstreamGroupBy2)),
      bootstrapParts = Seq.empty
    )

    // Create downstream GroupBys that use both JoinSources
    val downstreamGroupBy1 = GroupBy(
      sources = Seq(Source.joinSource(upstreamJoin1, Query())),
      keyColumns = Seq("user_id"),
      aggregations = Seq(Aggregation(ai.chronon.api.Operation.AVERAGE, "avg1")),
      metaData = MetaData(namespace = "test_namespace", name = "downstream_gb1")
    )

    val downstreamGroupBy2 = GroupBy(
      sources = Seq(Source.joinSource(upstreamJoin2, Query())),
      keyColumns = Seq("user_id"),
      aggregations = Seq(Aggregation(ai.chronon.api.Operation.MAX, "max2")),
      metaData = MetaData(namespace = "test_namespace", name = "downstream_gb2")
    )

    // Create final Join with both chained GroupBys
    val finalJoin = Join(
      metaData = MetaData(name = "final_join"),
      left = Source.events(Query(), table = "test_namespace.final_left"),
      joinParts = Seq(
        new ai.chronon.api.JoinPart().setGroupBy(downstreamGroupBy1),
        new ai.chronon.api.JoinPart().setGroupBy(downstreamGroupBy2)
      ),
      bootstrapParts = Seq.empty
    )

    val planner = MonolithJoinPlanner(finalJoin)
    val plan = planner.buildPlan

    validateJoinPlan(plan)

    // Verify dependencies include both upstream joins
    val metadataUploadNode = plan.nodes.asScala.find(_.content.isSetJoinMetadataUpload).get
    val tableDeps = metadataUploadNode.metaData.executionInfo.tableDependencies.asScala

    // Should have dependencies on both downstream GroupBys and both upstream joins
    tableDeps.size should be >= 4

    // Verify all expected dependencies exist
    val gb1Dep = tableDeps.find(_.tableInfo.table.contains("downstream_gb1__uploadToKV"))
    gb1Dep should be(defined)
    
    val gb2Dep = tableDeps.find(_.tableInfo.table.contains("downstream_gb2__uploadToKV"))
    gb2Dep should be(defined)
    
    val join1Dep = tableDeps.find(_.tableInfo.table.contains("upstream_join1__metadata_upload"))
    join1Dep should be(defined)
    
    val join2Dep = tableDeps.find(_.tableInfo.table.contains("upstream_join2__metadata_upload"))
    join2Dep should be(defined)
  }

  it should "skip upstream join dependencies for streaming GroupBys with JoinSource" in {
    import ai.chronon.api.Builders._

    // Create upstream Join
    val upstreamJoin = Join(
      metaData = MetaData(namespace = "test_namespace", name = "upstream_join"),
      left = Source.events(Query(), table = "test_namespace.left_events"),
      joinParts = Seq.empty,
      bootstrapParts = Seq.empty
    )

    // Create streaming GroupBy that uses JoinSource
    // Note: A GroupBy needs both a JoinSource and a regular source with topic for streaming
    val streamingGroupBy = GroupBy(
      sources = Seq(
        Source.joinSource(upstreamJoin, Query()),
        Source.events(Query(), table = "test_namespace.streaming_events", topic = "streaming_topic")
      ),
      keyColumns = Seq("user_id"),
      aggregations = Seq(Aggregation(ai.chronon.api.Operation.COUNT, "stream_count")),
      metaData = MetaData(namespace = "test_namespace", name = "streaming_chained_gb")
    )

    // Create Join with streaming GroupBy
    val joinWithStreamingGroupBy = Join(
      metaData = MetaData(name = "join_with_streaming_chained"),
      left = Source.events(Query(), table = "test_namespace.final_left"),
      joinParts = Seq(new ai.chronon.api.JoinPart().setGroupBy(streamingGroupBy)),
      bootstrapParts = Seq.empty
    )

    val planner = MonolithJoinPlanner(joinWithStreamingGroupBy)
    val plan = planner.buildPlan

    validateJoinPlan(plan)

    // Verify dependencies - should NOT include upstream join metadata upload dependency
    // because the streaming GroupBy node will handle it
    val metadataUploadNode = plan.nodes.asScala.find(_.content.isSetJoinMetadataUpload).get
    val tableDeps = metadataUploadNode.metaData.executionInfo.tableDependencies.asScala

    // Should only have dependency on the streaming GroupBy (streaming table)
    tableDeps.size should be(1)
    
    val streamingDep = tableDeps.find(_.tableInfo.table.contains("streaming_chained_gb__streaming"))
    streamingDep should be(defined)
    
    // Should NOT have upstream join metadata upload dependency (streaming node handles it)
    val upstreamJoinDeps = tableDeps.filter(_.tableInfo.table.contains("upstream_join__metadata_upload"))
    upstreamJoinDeps should be(empty)
  }

  it should "include upstream join dependencies for non-streaming GroupBys with JoinSource" in {
    import ai.chronon.api.Builders._

    // Create upstream Join
    val upstreamJoin = Join(
      metaData = MetaData(namespace = "test_namespace", name = "upstream_join"),
      left = Source.events(Query(), table = "test_namespace.left_events"),
      joinParts = Seq.empty,
      bootstrapParts = Seq.empty
    )

    // Create non-streaming GroupBy that uses JoinSource
    val nonStreamingGroupBy = GroupBy(
      sources = Seq(Source.joinSource(upstreamJoin, Query())),
      keyColumns = Seq("user_id"),
      aggregations = Seq(Aggregation(ai.chronon.api.Operation.COUNT, "batch_count")),
      metaData = MetaData(namespace = "test_namespace", name = "batch_chained_gb")
    )
    // NO streaming source - this is a batch GroupBy

    // Create Join with non-streaming GroupBy
    val joinWithBatchGroupBy = Join(
      metaData = MetaData(name = "join_with_batch_chained"),
      left = Source.events(Query(), table = "test_namespace.final_left"),
      joinParts = Seq(new ai.chronon.api.JoinPart().setGroupBy(nonStreamingGroupBy)),
      bootstrapParts = Seq.empty
    )

    val planner = MonolithJoinPlanner(joinWithBatchGroupBy)
    val plan = planner.buildPlan

    validateJoinPlan(plan)

    // Verify dependencies - should include BOTH the GroupBy dependency AND upstream join dependency
    val metadataUploadNode = plan.nodes.asScala.find(_.content.isSetJoinMetadataUpload).get
    val tableDeps = metadataUploadNode.metaData.executionInfo.tableDependencies.asScala

    // Should have dependencies on both the batch GroupBy and upstream join
    tableDeps.size should be(2)
    
    val groupByDep = tableDeps.find(_.tableInfo.table.contains("batch_chained_gb__uploadToKV"))
    groupByDep should be(defined)
    
    // Should have upstream join metadata upload dependency since it's not streaming
    val upstreamJoinDep = tableDeps.find(_.tableInfo.table.contains("upstream_join__metadata_upload"))
    upstreamJoinDep should be(defined)
  }

  it should "not add upstream join dependencies for regular (non-chained) GroupBys" in {
    import ai.chronon.api.Builders._

    // Create regular GroupBy (no JoinSource)
    val regularGroupBy = GroupBy(
      sources = Seq(Source.events(Query(), table = "test_namespace.regular_events")),
      keyColumns = Seq("user_id"),
      aggregations = Seq(Aggregation(ai.chronon.api.Operation.COUNT, "regular_count")),
      metaData = MetaData(namespace = "test_namespace", name = "regular_gb")
    )

    // Create Join with regular GroupBy
    val regularJoin = Join(
      metaData = MetaData(name = "regular_join"),
      left = Source.events(Query(), table = "test_namespace.regular_left"),
      joinParts = Seq(new ai.chronon.api.JoinPart().setGroupBy(regularGroupBy)),
      bootstrapParts = Seq.empty
    )

    val planner = MonolithJoinPlanner(regularJoin)
    val plan = planner.buildPlan

    validateJoinPlan(plan)

    // Verify only regular dependencies (no upstream join dependencies)
    val metadataUploadNode = plan.nodes.asScala.find(_.content.isSetJoinMetadataUpload).get
    val tableDeps = metadataUploadNode.metaData.executionInfo.tableDependencies.asScala

    // Should only have dependency on the regular GroupBy
    tableDeps.size should be(1)
    
    val regularGbDep = tableDeps.find(_.tableInfo.table.contains("regular_gb__uploadToKV"))
    regularGbDep should be(defined)
    
    // Should not have any upstream join dependencies
    val upstreamJoinDeps = tableDeps.filter(_.tableInfo.table.contains("__metadata_upload"))
    upstreamJoinDeps should be(empty)
  }
}
