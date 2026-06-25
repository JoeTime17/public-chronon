package ai.chronon.api.test.planner

import ai.chronon.api.Extensions.WindowUtils
import ai.chronon.api.{Builders, ExecutionInfo, PartitionSpec, TableInfo}
import ai.chronon.api.planner.ConfPlanner
import ai.chronon.planner.{ConfPlan, GroupByBackfillNode, GroupByStreamingNode}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ConfPlannerTest extends AnyFlatSpec with Matchers {

  private implicit val testPartitionSpec: PartitionSpec = PartitionSpec.daily

  // Mock concrete implementation of ConfPlanner for testing
  private class TestConfPlanner extends ConfPlanner[String]("test-conf") {
    override def buildPlan: ConfPlan = new ConfPlan()
  }

  private val testPlanner = new TestConfPlanner()
  private val testMetaData = Builders.MetaData(namespace = "test", name = "test_node")
  private val threeHourSpec = PartitionSpec("ds", "yyyy-MM-dd-HH", 3 * 60 * 60 * 1000)

  private def outputGridMetaData(spec: PartitionSpec): ai.chronon.api.MetaData =
    Builders.MetaData(
      namespace = "test",
      name = "test_node",
      executionInfo = new ExecutionInfo().setOutputTableInfo(
        new TableInfo()
          .setTable("test.test_node")
          .setPartitionColumn(spec.column)
          .setPartitionFormat(spec.format)
          .setPartitionInterval(WindowUtils.fromMillis(spec.spanMillis))
      )
    )

  it should "create Node with correct fields for non-streaming content" in {
    val groupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.events(Builders.Query(), table = "test_table")),
      keyColumns = Seq("key"),
      aggregations = Seq(),
      metaData = testMetaData
    )

    val node = testPlanner.toNode(
      testMetaData,
      content => content.setGroupByBackfill(new GroupByBackfillNode().setGroupBy(groupBy)),
      groupBy
    )

    node should not be null
    node.metaData should equal(testMetaData)
    node.content should not be null
    node.content.isSetGroupByBackfill shouldBe true
    node.content.getGroupByBackfill.getGroupBy should equal(groupBy)
    node.semanticHash should not be null
    node.semanticHash should not be empty
    node.isSetIsLongRunning shouldBe true
    node.isLongRunning shouldBe false

    // Validate that node name does not contain forward slashes
    val nodeName = node.metaData.name
    withClue(s"Node name '$nodeName' contains forward slash") {
      nodeName should not contain "/"
    }
  }

  it should "set isLongRunning to true for GROUP_BY_STREAMING content" in {
    val groupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.events(Builders.Query(), table = "test_table")),
      keyColumns = Seq("key"),
      aggregations = Seq(),
      metaData = testMetaData
    )

    val node = testPlanner.toNode(
      testMetaData,
      content => content.setGroupByStreaming(new GroupByStreamingNode().setGroupBy(groupBy)),
      groupBy
    )

    node.isLongRunning shouldBe true
    node.content.isSetGroupByStreaming shouldBe true
  }

  it should "set isLongRunning to false for GROUP_BY_BACKFILL content" in {
    val groupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.events(Builders.Query(), table = "test_table")),
      keyColumns = Seq("key"),
      aggregations = Seq(),
      metaData = testMetaData
    )

    val node = testPlanner.toNode(
      testMetaData,
      content => content.setGroupByBackfill(new GroupByBackfillNode().setGroupBy(groupBy)),
      groupBy
    )

    node.isSetIsLongRunning shouldBe true
    node.isLongRunning shouldBe false
    node.content.isSetGroupByBackfill shouldBe true
  }

  it should "generate consistent semantic hash for identical hashable objects" in {
    val groupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.events(Builders.Query(), table = "test_table")),
      keyColumns = Seq("key"),
      aggregations = Seq(),
      metaData = testMetaData
    )

    val node1 = testPlanner.toNode(
      testMetaData,
      content => content.setGroupByBackfill(new GroupByBackfillNode().setGroupBy(groupBy)),
      groupBy
    )

    val node2 = testPlanner.toNode(
      testMetaData,
      content => content.setGroupByBackfill(new GroupByBackfillNode().setGroupBy(groupBy)),
      groupBy
    )

    node1.semanticHash should equal(node2.semanticHash)
  }

  it should "mix the output grid into node semantic hash" in {
    val groupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.events(Builders.Query(), table = "test_table")),
      keyColumns = Seq("key"),
      aggregations = Seq(),
      metaData = testMetaData
    )
    val hashableGroupBy = groupBy.deepCopy()
    hashableGroupBy.unsetMetaData()

    val unsetGridNode = testPlanner.toNode(
      testMetaData,
      content => content.setGroupByBackfill(new GroupByBackfillNode().setGroupBy(groupBy)),
      hashableGroupBy
    )

    val dailyGridNode = testPlanner.toNode(
      outputGridMetaData(PartitionSpec.daily),
      content => content.setGroupByBackfill(new GroupByBackfillNode().setGroupBy(groupBy)),
      hashableGroupBy
    )

    val threeHourGridNode = testPlanner.toNode(
      outputGridMetaData(threeHourSpec),
      content => content.setGroupByBackfill(new GroupByBackfillNode().setGroupBy(groupBy)),
      hashableGroupBy
    )

    dailyGridNode.semanticHash should equal(unsetGridNode.semanticHash)
    threeHourGridNode.semanticHash should not equal unsetGridNode.semanticHash
  }

  it should "generate different semantic hash for different hashable objects" in {
    val groupBy1 = Builders.GroupBy(
      sources = Seq(Builders.Source.events(Builders.Query(), table = "test_table_1")),
      keyColumns = Seq("key"),
      aggregations = Seq(),
      metaData = testMetaData
    )

    val groupBy2 = Builders.GroupBy(
      sources = Seq(Builders.Source.events(Builders.Query(), table = "test_table_2")),
      keyColumns = Seq("key"),
      aggregations = Seq(),
      metaData = testMetaData
    )

    val node1 = testPlanner.toNode(
      testMetaData,
      content => content.setGroupByBackfill(new GroupByBackfillNode().setGroupBy(groupBy1)),
      groupBy1
    )

    val node2 = testPlanner.toNode(
      testMetaData,
      content => content.setGroupByBackfill(new GroupByBackfillNode().setGroupBy(groupBy2)),
      groupBy2
    )

    node1.semanticHash should not equal node2.semanticHash
  }

  it should "handle null values gracefully" in {
    val groupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.events(Builders.Query(), table = "test_table")),
      keyColumns = Seq("key"),
      aggregations = Seq(),
      metaData = testMetaData
    )

    noException should be thrownBy {
      testPlanner.toNode(
        testMetaData,
        content => content.setGroupByBackfill(new GroupByBackfillNode().setGroupBy(groupBy)),
        groupBy
      )
    }
  }
}
