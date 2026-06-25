package ai.chronon.api.test.planner

import ai.chronon.api.{Accuracy, Aggregation, Builders, EnvironmentVariables, GroupBy, Operation, PartitionSpec, TableInfo}
import ai.chronon.api.Extensions.{GroupByOps, MetadataOps, WindowUtils}
import ai.chronon.api.planner.{GroupByPlanner, LocalRunner}
import ai.chronon.api.test.planner.GroupByPlannerTest.buildGroupBy
import ai.chronon.planner.{ConfPlan, Mode}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Paths
import scala.jdk.CollectionConverters._

class GroupByPlannerTest extends AnyFlatSpec with Matchers {

  private implicit val testPartitionSpec: PartitionSpec = PartitionSpec.daily
  private val oneHourSpec = PartitionSpec("ds", "yyyy-MM-dd-HH-mm", 60 * 60 * 1000)
  private val threeHourSpec = PartitionSpec("ds", "yyyy-MM-dd-HH-mm", 3 * 60 * 60 * 1000)
  // divides the day (unlike 5h, which PartitionSpec now rejects at construction) but is not a
  // multiple of the 3h source grid, so it still exercises the exact-multiple rejection
  private val fourHourSpec = PartitionSpec("ds", "yyyy-MM-dd-HH-mm", 4 * 60 * 60 * 1000)
  private val sixHourSpec = PartitionSpec("ds", "yyyy-MM-dd-HH-mm", 6 * 60 * 60 * 1000)

  private def outputTableInfo(table: String, spec: PartitionSpec): TableInfo = {
    val tableInfo = new TableInfo()
      .setTable(table)
      .setPartitionColumn(spec.column)
      .setPartitionFormat(spec.format)
      .setPartitionInterval(WindowUtils.fromMillis(spec.spanMillis))
    if (spec.offsetMillis != 0) tableInfo.setPartitionOffset(WindowUtils.fromMillis(spec.offsetMillis))
    tableInfo
  }

  private def withOutputSpec(groupBy: GroupBy, spec: PartitionSpec): GroupBy = {
    groupBy.metaData.executionInfo.setOutputTableInfo(outputTableInfo(groupBy.metaData.outputTable, spec))
    groupBy
  }

  private def withEventSourceSpec(groupBy: GroupBy, spec: PartitionSpec): GroupBy = {
    val query = Builders.Query(partitionColumn = spec.column)
    query.setPartitionFormat(spec.format)
    query.setPartitionInterval(WindowUtils.fromMillis(spec.spanMillis))
    if (spec.offsetMillis != 0) query.setPartitionOffset(WindowUtils.fromMillis(spec.offsetMillis))
    groupBy.setSources(Seq(Builders.Source.events(query, table = "my_user_events")).asJava)
    groupBy
  }

  private def validateGBPlan(groupBy: GroupBy, plan: ConfPlan): Unit = {
    // Should create plan successfully with expected number of nodes
    val hasStreaming = groupBy.streamingSource.isDefined
    val expectedNodeCount = if (hasStreaming) 5 else 4
    plan.nodes.asScala should have size expectedNodeCount

    // Find the nodes
    val uploadNode = plan.nodes.asScala.find(_.content.isSetGroupByUpload)
    val backfillNode = plan.nodes.asScala.find(_.content.isSetGroupByBackfill)
    val uploadToKVNode = plan.nodes.asScala.find(_.content.isSetGroupByUploadToKV)
    val streamingNode = plan.nodes.asScala.find(_.content.isSetGroupByStreaming)

    uploadNode should be(defined)
    backfillNode should be(defined)
    uploadToKVNode should be(defined)

    if (hasStreaming) {
      streamingNode should be(defined)
    } else {
      streamingNode should not be defined
    }

    // Upload node should have content
    uploadNode.get.content should not be null
    uploadNode.get.content.getGroupByUpload should not be null
    uploadNode.get.content.getGroupByUpload.groupBy should not be null
    // upload node should use the daily partition spec
    uploadNode.get.metaData.executionInfo.outputTableInfo.partitionColumn should equal(PartitionSpec.daily.column)
    uploadNode.get.metaData.executionInfo.outputTableInfo.partitionFormat should equal(PartitionSpec.daily.format)

    // Backfill node should have content
    backfillNode.get.content should not be null
    backfillNode.get.content.getGroupByBackfill should not be null
    backfillNode.get.content.getGroupByBackfill.groupBy should not be null

    // UploadToKV node should have content
    uploadToKVNode.get.content should not be null
    uploadToKVNode.get.content.getGroupByUploadToKV should not be null
    uploadToKVNode.get.content.getGroupByUploadToKV.groupBy should not be null

    // Streaming node should have content if present
    if (hasStreaming) {
      streamingNode.get.content should not be null
      streamingNode.get.content.getGroupByStreaming should not be null
      streamingNode.get.content.getGroupByStreaming.groupBy should not be null
    }

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

  it should "always plan nonzero step days" in {
    val groupBy = buildGroupBy()
    val plannerWithNonZeroStepDays = GroupByPlanner(groupBy)
    val plan = plannerWithNonZeroStepDays.buildPlan
    plan.nodes.asScala.foreach((node) => node.metaData.executionInfo.stepDays shouldNot be(0))
  }

  it should "GB planner handles valid confs" in {

    val gbRootDir = Paths.get(getClass.getClassLoader.getResource("canary/compiled/group_bys").getPath)

    val gbConfs = LocalRunner.parseConfs[ai.chronon.api.GroupBy](gbRootDir.toString)

    val planners = gbConfs.map(new GroupByPlanner(_))

    planners
      .foreach { planner =>
        noException should be thrownBy {
          val plan = planner.buildPlan
          validateGBPlan(planner.groupBy, plan)
        }
      }
  }

  it should "GB planner should create valid plans" in {
    val gb = buildGroupBy()

    val planner = new GroupByPlanner(gb)

    noException should be thrownBy {
      val plan = planner.buildPlan
      validateGBPlan(planner.groupBy, plan)
      plan.terminalNodeNames.asScala(Mode.DEPLOY) should equal("user_charges__uploadToKV")
      plan.terminalNodeNames.asScala(Mode.BACKFILL) should equal("user_charges__group_by")
    }
  }

  it should "allow groupBy output intervals that coarsen or match the source partition interval" in {
    Seq(threeHourSpec, sixHourSpec, PartitionSpec.daily).foreach { groupBySpec =>
      val gb = withOutputSpec(withEventSourceSpec(buildGroupBy(), threeHourSpec), groupBySpec)
      noException should be thrownBy GroupByPlanner(gb).buildPlan
    }
  }

  it should "reject groupBy output intervals that are finer or not multiples of the source partition interval" in {
    Seq(oneHourSpec, fourHourSpec).foreach { groupBySpec =>
      val gb = withOutputSpec(withEventSourceSpec(buildGroupBy(), threeHourSpec), groupBySpec)
      an[IllegalArgumentException] should be thrownBy GroupByPlanner(gb).buildPlan
    }
  }

  it should "reject a sub-daily groupBy over a source with no declared partition interval" in {
    // an undeclared source is implicitly daily: every intraday run would block on the full
    // day's partition and the whole pipeline lands a day late - permanently, silently
    val gb = withOutputSpec(buildGroupBy(), threeHourSpec)
    val error = the[IllegalArgumentException] thrownBy GroupByPlanner(gb).buildPlan
    error.getMessage should include("time_partitioned")
  }

  it should "allow a sub-daily groupBy over an undeclared source marked time_partitioned" in {
    // time_partitioned sources land continuously; intraday readiness is sensed from timestamps
    val gb = withOutputSpec(buildGroupBy(), threeHourSpec)
    gb.sources.asScala.foreach(_.getEvents.query.setTimePartitioned(true))
    noException should be thrownBy GroupByPlanner(gb).buildPlan
  }

  it should "use upstream JoinSource output grid when validating sub-daily groupBys" in {
    val upstreamSourceQuery = Builders.Query(partitionColumn = "ds")
    upstreamSourceQuery.setPartitionInterval(WindowUtils.fromMillis(threeHourSpec.spanMillis))
    val upstreamGroupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.events(upstreamSourceQuery, table = "test_namespace.upstream_events")),
      keyColumns = Seq("user_id"),
      aggregations = Seq(Builders.Aggregation(Operation.COUNT, "upstream_count")),
      metaData = Builders.MetaData(namespace = "test_namespace", name = "upstream_gb")
    )

    val upstreamLeftQuery = Builders.Query(partitionColumn = "ds")
    upstreamLeftQuery.setPartitionInterval(WindowUtils.fromMillis(threeHourSpec.spanMillis))
    val upstreamJoin = Builders.Join(
      metaData = Builders.MetaData(namespace = "test_namespace", name = "upstream_join"),
      left = Builders.Source.events(upstreamLeftQuery, table = "test_namespace.upstream_left"),
      joinParts = Seq(new ai.chronon.api.JoinPart().setGroupBy(upstreamGroupBy)),
      bootstrapParts = Seq.empty
    )
    upstreamJoin.metaData.executionInfo.setOutputTableInfo(outputTableInfo(upstreamJoin.metaData.outputTable, threeHourSpec))

    val downstreamGroupBy = withOutputSpec(
      Builders.GroupBy(
        sources = Seq(Builders.Source.joinSource(upstreamJoin, Builders.Query())),
        keyColumns = Seq("user_id"),
        aggregations = Seq(Builders.Aggregation(Operation.SUM, "downstream_sum")),
        metaData = Builders.MetaData(namespace = "test_namespace", name = "downstream_gb")
      ),
      threeHourSpec
    )

    val plan = GroupByPlanner(downstreamGroupBy).buildPlan
    val backfill = plan.nodes.asScala.find(_.content.isSetGroupByBackfill).get
    val joinSourceDep = backfill.metaData.executionInfo.tableDependencies.asScala.head

    joinSourceDep.tableInfo.table should equal(upstreamJoin.metaData.outputTable)
    joinSourceDep.tableInfo.partitionInterval should equal(WindowUtils.fromMillis(threeHourSpec.spanMillis))
    joinSourceDep.tableInfo.partitionFormat should equal(threeHourSpec.format)
  }

  it should "keep allowing daily groupBys over undeclared sources" in {
    noException should be thrownBy GroupByPlanner(buildGroupBy()).buildPlan
  }

  it should "stamp the downstream grid onto time_partitioned dependencies" in {
    // a time-partitioned source has no physical grid (its column is a real timestamp), so the
    // dependency must carry the downstream node's grid: sensing and range math then quantize
    // intraday requirements on the node's partitionInterval instead of stalling on a daily boundary
    val gb = withOutputSpec(buildGroupBy(), threeHourSpec)
    gb.sources.asScala.foreach(_.getEvents.query.setTimePartitioned(true))

    val plan = GroupByPlanner(gb).buildPlan
    val backfill = plan.nodes.asScala.find(_.content.isSetGroupByBackfill).get
    val dep = backfill.metaData.executionInfo.tableDependencies.asScala.head

    dep.tableInfo.timePartitioned shouldBe true
    dep.tableInfo.partitionInterval should equal(WindowUtils.fromMillis(threeHourSpec.spanMillis))
    dep.tableInfo.partitionFormat should equal(threeHourSpec.format)
  }

  it should "reject groupBy output boundaries that don't line up on the source grid" in {
    val offsetSourceSpec = PartitionSpec("ds", "yyyy-MM-dd-HH-mm", 3 * 60 * 60 * 1000, offsetMillis = 60 * 60 * 1000)
    val gb = withOutputSpec(withEventSourceSpec(buildGroupBy(), offsetSourceSpec), sixHourSpec)

    val error = the[IllegalArgumentException] thrownBy GroupByPlanner(gb).buildPlan
    // the error must name both grids so the mismatch is actionable
    error.getMessage should include("don't line up")
    error.getMessage should include("6h starting 00:00")
    error.getMessage should include("3h starting 01:00")
  }

  it should "allow groupBy output boundaries that line up on an offset source grid" in {
    val offsetSourceSpec = PartitionSpec("ds", "yyyy-MM-dd-HH-mm", 3 * 60 * 60 * 1000, offsetMillis = 60 * 60 * 1000)
    // offsets differing by a whole number of upstream intervals line up: 1h and 4h offsets
    // over a 3h upstream interval both sit on the 1h-offset source grid
    val oneHourOffsetSpec = PartitionSpec("ds", "yyyy-MM-dd-HH-mm", 6 * 60 * 60 * 1000, offsetMillis = 60 * 60 * 1000)
    val fourHourOffsetSpec =
      PartitionSpec("ds", "yyyy-MM-dd-HH-mm", 6 * 60 * 60 * 1000, offsetMillis = 4 * 60 * 60 * 1000)

    Seq(oneHourOffsetSpec, fourHourOffsetSpec).foreach { groupBySpec =>
      val gb = withOutputSpec(withEventSourceSpec(buildGroupBy(), offsetSourceSpec), groupBySpec)
      noException should be thrownBy GroupByPlanner(gb).buildPlan
    }
  }

  it should "GB planner should skip metadata in sem hash" in {
    val firstGb = buildGroupBy()

    // Create a second GroupBy with different metadata
    val secondGb = buildGroupBy()
    secondGb.metaData = firstGb.metaData.deepCopy()
    secondGb.metaData.name = "secondGroupBy"
    secondGb.metaData.samplePercent = 0.5 // Different sample percent

    val firstPlanner = new GroupByPlanner(firstGb)
    val secondPlanner = new GroupByPlanner(secondGb)

    val firstPlan = firstPlanner.buildPlan
    val secondPlan = secondPlanner.buildPlan

    // Semantic hashes should be identical since metadata is excluded (for upload nodes)
    val firstUploadHash = firstPlan.nodes.asScala.find(_.content.isSetGroupByUpload).get.semanticHash
    val secondUploadHash = secondPlan.nodes.asScala.find(_.content.isSetGroupByUpload).get.semanticHash
    firstUploadHash should equal(secondUploadHash)
  }

  it should "GB planner should produce same semantic hash with different executionInfo" in {
    val firstGB = buildGroupBy()

    val secondGB = buildGroupBy()
    secondGB.metaData = firstGB.metaData.deepCopy()
    secondGB.metaData.executionInfo = firstGB.metaData.executionInfo.deepCopy()
    secondGB.metaData.executionInfo.env =
      new EnvironmentVariables().setCommon(Map("foo" -> "bar", "baz" -> "qux").asJava)

    val firstPlanner = new GroupByPlanner(firstGB)
    val secondPlanner = new GroupByPlanner(secondGB)

    val firstPlan = firstPlanner.buildPlan
    val secondPlan = secondPlanner.buildPlan

    // Semantic hashes should be identical since metadata (including executionInfo) is excluded
    val firstSemanticHashes = firstPlan.nodes.asScala.map(_.semanticHash)
    val secondSemanticHashes = secondPlan.nodes.asScala.map(_.semanticHash)
    firstSemanticHashes should equal(secondSemanticHashes)
  }

  it should "GB planner uploadToKV node should have correct table dependencies" in {
    val gb = buildGroupBy()
    val planner = new GroupByPlanner(gb)
    val plan = planner.buildPlan
    validateGBPlan(gb, plan)

    val uploadToKVNode = plan.nodes.asScala.find(_.content.isSetGroupByUploadToKV).get
    val executionInfo = uploadToKVNode.metaData.executionInfo
    val tableDeps = executionInfo.tableDependencies.asScala

    // Should have exactly one table dependency
    tableDeps should have size 1

    val tableDep = tableDeps.head
    // Validate table dependency references the upload table
    tableDep.tableInfo.table should equal(gb.metaData.uploadTable)

    // Validate partition specifications match the test partition spec
    tableDep.tableInfo.partitionColumn should equal(testPartitionSpec.column)
    tableDep.tableInfo.partitionFormat should equal(testPartitionSpec.format)

    // Validate offsets are set to zero for upload scenarios
    tableDep.startOffset should not be null
    tableDep.endOffset should not be null

    // Validate output table info is properly set
    val outputTableInfo = uploadToKVNode.metaData.executionInfo.outputTableInfo
    outputTableInfo should not be null
    outputTableInfo.table should equal(gb.metaData.outputTable + "__uploadToKV")
    outputTableInfo.partitionColumn should equal(testPartitionSpec.column)
    outputTableInfo.partitionFormat should equal(testPartitionSpec.format)
    outputTableInfo.partitionInterval should not be null
  }

  it should "GB planner should create streaming node when streamingSource is present" in {
    val gb = buildGroupBy(includeTopic = true)
    val planner = GroupByPlanner(gb)
    val plan = planner.buildPlan

    validateGBPlan(gb, plan)

    // DEPLOY mode should now point to streaming node
    plan.terminalNodeNames.asScala(Mode.DEPLOY) should equal("user_charges__streaming")
    plan.terminalNodeNames.asScala(Mode.BACKFILL) should equal("user_charges__group_by")

    // Verify streaming node has correct table dependencies (same as uploadToKV)
    val streamingNode = plan.nodes.asScala.find(_.content.isSetGroupByStreaming).get
    val executionInfo = streamingNode.metaData.executionInfo
    val tableDeps = executionInfo.tableDependencies.asScala

    tableDeps should have size 1
    val tableDep = tableDeps.head
    tableDep.tableInfo.table should equal(gb.metaData.outputTable + "__uploadToKV")

    // Verify streaming node has correct output table info
    val streamingOutputTableInfo = streamingNode.metaData.executionInfo.outputTableInfo
    streamingOutputTableInfo should not be null
    streamingOutputTableInfo.table should equal(gb.metaData.outputTable + "__streaming")
    streamingOutputTableInfo.partitionColumn should equal(testPartitionSpec.column)
    streamingOutputTableInfo.partitionFormat should equal(testPartitionSpec.format)
    streamingOutputTableInfo.partitionInterval should not be null
  }

  // GroupBy Chaining Tests

  it should "streaming node should depend on upstream join when GroupBy has JoinSource" in {
    import ai.chronon.api.Builders._

    // Create upstream GroupBy
    val upstreamGroupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.events(Builders.Query(), table = "test_namespace.upstream_events")),
      keyColumns = Seq("user_id"),
      aggregations = Seq(Builders.Aggregation(ai.chronon.api.Operation.COUNT, "upstream_count")),
      metaData = Builders.MetaData(namespace = "test_namespace", name = "upstream_gb")
    )

    // Create upstream Join
    val upstreamJoinPart = new ai.chronon.api.JoinPart()
      .setGroupBy(upstreamGroupBy)

    val upstreamJoin = Builders.Join(
      metaData = Builders.MetaData(namespace = "test_namespace", name = "upstream_join"),
      left = Builders.Source.events(Builders.Query(), table = "test_namespace.upstream_left"),
      joinParts = Seq(upstreamJoinPart),
      bootstrapParts = Seq.empty
    )

    // Create downstream GroupBy with JoinSource and streaming source
    // The GroupBy needs both a JoinSource and a regular source with topic for streaming
    val downstreamGroupBy = Builders.GroupBy(
      sources = Seq(
        Builders.Source.joinSource(upstreamJoin, Builders.Query()),
        Builders.Source.events(Builders.Query(), table = "test_namespace.downstream_events", topic = "downstream_topic")
      ),
      keyColumns = Seq("user_id"),
      aggregations = Seq(Builders.Aggregation(ai.chronon.api.Operation.SUM, "downstream_sum")),
      metaData = Builders.MetaData(namespace = "test_namespace", name = "downstream_gb")
    )

    val planner = GroupByPlanner(downstreamGroupBy)
    val plan = planner.buildPlan

    // Should have streaming node since streamingSource is defined
    val streamingNode = plan.nodes.asScala.find(_.content.isSetGroupByStreaming)
    streamingNode should be(defined)

    // Verify streaming node dependencies
    val streamingTableDeps = streamingNode.get.metaData.executionInfo.tableDependencies.asScala

    // Should depend on both uploadToKV and upstream join's metadata upload
    streamingTableDeps.size should be >= 2

    // Should have dependency on own uploadToKV
    val uploadToKVDep = streamingTableDeps.find(_.tableInfo.table.contains("downstream_gb__uploadToKV"))
    uploadToKVDep should be(defined)

    // Should have dependency on upstream join's metadata upload
    val upstreamJoinDep = streamingTableDeps.find(_.tableInfo.table.contains("upstream_join__metadata_upload"))
    upstreamJoinDep should be(defined)
  }

  it should "streaming node should not add upstream join dependencies for regular sources" in {
    // Use existing buildGroupBy method which creates a regular GroupBy with streaming source
    val regularGroupBy = GroupByPlannerTest.buildGroupBy(includeTopic = true)

    val planner = GroupByPlanner(regularGroupBy)
    val plan = planner.buildPlan

    // Should have streaming node since topic is included
    val streamingNode = plan.nodes.asScala.find(_.content.isSetGroupByStreaming)
    streamingNode should be(defined)

    // Verify streaming node dependencies
    val streamingTableDeps = streamingNode.get.metaData.executionInfo.tableDependencies.asScala

    // Should only depend on own uploadToKV (no upstream join dependencies)
    streamingTableDeps.size should be(1)

    val uploadToKVDep = streamingTableDeps.find(_.tableInfo.table.contains("user_charges__uploadToKV"))
    uploadToKVDep should be(defined)

    // Should not have any upstream join dependencies
    val upstreamJoinDeps = streamingTableDeps.filter(_.tableInfo.table.contains("__metadata_upload"))
    upstreamJoinDeps should be(empty)
  }

  it should "semantic hash should be the same regardless of EventSource topic" in {
    val gbTopicA = Builders.GroupBy(
      sources = Seq(Builders.Source.events(Builders.Query(), table = "my_user_events", topic = "topic_a")),
      keyColumns = Seq("user"),
      aggregations = Seq(Builders.Aggregation(Operation.COUNT, "charges", Seq(WindowUtils.Unbounded))),
      metaData = Builders.MetaData(namespace = "test_namespace", name = "user_charges")
    )
    val gbTopicB = Builders.GroupBy(
      sources = Seq(Builders.Source.events(Builders.Query(), table = "my_user_events", topic = "topic_b")),
      keyColumns = Seq("user"),
      aggregations = Seq(Builders.Aggregation(Operation.COUNT, "charges", Seq(WindowUtils.Unbounded))),
      metaData = Builders.MetaData(namespace = "test_namespace", name = "user_charges")
    )

    val planA = GroupByPlanner(gbTopicA).buildPlan
    val planB = GroupByPlanner(gbTopicB).buildPlan

    // Topics are routing concerns; they should not affect the semantic hash
    planA.nodes.asScala.zip(planB.nodes.asScala).foreach { case (nodeA, nodeB) =>
      nodeA.semanticHash should equal(nodeB.semanticHash)
    }
  }

  it should "semantic hash should be the same regardless of EntitySource mutationTopic" in {
    val gbTopicA = Builders.GroupBy(
      sources = Seq(
        Builders.Source.entities(Builders.Query(), snapshotTable = "my_entity_snapshot", mutationTopic = "topic_a")),
      keyColumns = Seq("entity_id"),
      aggregations = Seq(Builders.Aggregation(Operation.LAST, "value", Seq(WindowUtils.Unbounded))),
      metaData = Builders.MetaData(namespace = "test_namespace", name = "entity_gb")
    )
    val gbTopicB = Builders.GroupBy(
      sources = Seq(
        Builders.Source.entities(Builders.Query(), snapshotTable = "my_entity_snapshot", mutationTopic = "topic_b")),
      keyColumns = Seq("entity_id"),
      aggregations = Seq(Builders.Aggregation(Operation.LAST, "value", Seq(WindowUtils.Unbounded))),
      metaData = Builders.MetaData(namespace = "test_namespace", name = "entity_gb")
    )

    val planA = GroupByPlanner(gbTopicA).buildPlan
    val planB = GroupByPlanner(gbTopicB).buildPlan

    // mutationTopic is a routing concern; it should not affect the semantic hash
    planA.nodes.asScala.zip(planB.nodes.asScala).foreach { case (nodeA, nodeB) =>
      nodeA.semanticHash should equal(nodeB.semanticHash)
    }
  }

  it should "GroupBy without streaming source should not be affected by JoinSource" in {
    import ai.chronon.api.Builders._

    // Create upstream Join
    val upstreamGroupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.events(Builders.Query(), table = "test_namespace.upstream_events")),
      keyColumns = Seq("user_id"),
      aggregations = Seq(Builders.Aggregation(ai.chronon.api.Operation.COUNT, "upstream_count")),
      metaData = Builders.MetaData(namespace = "test_namespace", name = "upstream_gb")
    )

    val upstreamJoin = Builders.Join(
      metaData = Builders.MetaData(namespace = "test_namespace", name = "upstream_join"),
      left = Builders.Source.events(Builders.Query(), table = "test_namespace.upstream_left"),
      joinParts = Seq(new ai.chronon.api.JoinPart().setGroupBy(upstreamGroupBy)),
      bootstrapParts = Seq.empty
    )

    // Create downstream GroupBy with JoinSource but NO streaming source
    val downstreamGroupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.joinSource(upstreamJoin, Builders.Query())),
      keyColumns = Seq("user_id"),
      aggregations = Seq(Builders.Aggregation(ai.chronon.api.Operation.SUM, "downstream_sum")),
      metaData = Builders.MetaData(namespace = "test_namespace", name = "downstream_gb")
      // No streamingSource defined
    )

    val planner = GroupByPlanner(downstreamGroupBy)
    val plan = planner.buildPlan

    // Should NOT have streaming node since streamingSource is not defined
    val streamingNode = plan.nodes.asScala.find(_.content.isSetGroupByStreaming)
    streamingNode should be(empty)

    // Verify the plan has the expected nodes (backfill, upload, uploadToKV)
    val allNodes = plan.nodes.asScala.toList
    val backfillNodes = allNodes.count(_.content.isSetGroupByBackfill)
    val uploadNodes = allNodes.count(_.content.isSetGroupByUpload)
    val uploadToKVNodes = allNodes.count(_.content.isSetGroupByUploadToKV)

    backfillNodes should be(1)
    uploadNodes should be(1)
    uploadToKVNodes should be(1)

    // Terminal node should be uploadToKV (not streaming)
    val deployTerminalNodeName = plan.terminalNodeNames.asScala(ai.chronon.planner.Mode.DEPLOY)
    deployTerminalNodeName should include("uploadToKV")
  }
}

object GroupByPlannerTest {
  def buildGroupBy(includeTopic: Boolean = false): GroupBy = {
    val eventsTable = "my_user_events"
    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(Operation.COUNT, "charges", Seq(WindowUtils.Unbounded))
    )
    val source = Builders.Source.events(Builders.Query(), table = eventsTable)
    val sourceWithTopic = if (includeTopic) {
      Builders.Source.events(Builders.Query(), table = eventsTable, topic = "my_user_events_topic")
    } else {
      source
    }

    Builders.GroupBy(
      sources = Seq(sourceWithTopic),
      keyColumns = Seq("user"),
      aggregations = aggregations,
      metaData = Builders.MetaData(namespace = "test_namespace", name = "user_charges"),
      accuracy = Accuracy.TEMPORAL
    )
  }
}
