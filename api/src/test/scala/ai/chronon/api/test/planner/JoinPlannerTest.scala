package ai.chronon.api.test.planner

import ai.chronon.api.Builders.{Join, MetaData}
import ai.chronon.api.Extensions.WindowUtils
import ai.chronon.api.Extensions._
import ai.chronon.api.planner.JoinPlanner
import ai.chronon.api.{Accuracy, Builders, ConfigProperties, ExecutionInfo, Operation, PartitionSpec}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

class JoinPlannerTest extends AnyFlatSpec with Matchers {

  private implicit val testPartitionSpec: PartitionSpec = PartitionSpec.daily

  private def modularExecutionInfo: ExecutionInfo =
    new ExecutionInfo().setConf(
      new ConfigProperties().setCommon(Map("modular_execution" -> "true").asJava)
    )

  private def temporalEntityGroupBy(name: String): ai.chronon.api.GroupBy = {
    val entityQuery = Builders.Query(
      startPartition = "2025-01-01",
      partitionColumn = "ds"
    )
    entityQuery.setPartitionInterval(WindowUtils.Day)

    Builders.GroupBy(
      sources = Seq(Builders.Source.entities(
        query = entityQuery,
        snapshotTable = "test.dim_snapshot",
        mutationTable = "test.dim_mutations"
      )),
      keyColumns = Seq("listing_id"),
      aggregations = Seq(Builders.Aggregation(Operation.LAST, "headline", Seq(WindowUtils.Unbounded))),
      accuracy = Accuracy.TEMPORAL,
      metaData = Builders.MetaData(namespace = "test_namespace", name = name)
    )
  }

  it should "include mutation table dependencies and sensors on the UnionJoin path" in {
    val join = Join(
      metaData = MetaData(
        name = "modular_union_join",
        namespace = "test_namespace",
        executionInfo = modularExecutionInfo
      ),
      left = Builders.Source.events(Builders.Query(partitionColumn = "ds"), table = "test.left_events"),
      joinParts = Seq(Builders.JoinPart(groupBy = temporalEntityGroupBy("union_temporal_entity_gb")))
    )

    val plan = new JoinPlanner(join).buildPlan

    val unionNode = plan.nodes.asScala.find(_.content.isSetUnionJoin).get
    val tableDeps = unionNode.metaData.executionInfo.tableDependencies.asScala

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

  it should "include mutation table dependencies on the join part for the standard modular path" in {
    val standardGroupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.events(Builders.Query(partitionColumn = "ds"), table = "test.other_events")),
      keyColumns = Seq("listing_id"),
      aggregations = Seq(Builders.Aggregation(Operation.COUNT, "event_count", Seq(WindowUtils.Unbounded))),
      accuracy = Accuracy.TEMPORAL,
      metaData = Builders.MetaData(namespace = "test_namespace", name = "standard_events_gb")
    )

    val join = Join(
      metaData = MetaData(
        name = "modular_standard_join",
        namespace = "test_namespace",
        executionInfo = modularExecutionInfo
      ),
      left = Builders.Source.events(Builders.Query(partitionColumn = "ds"), table = "test.left_events"),
      joinParts = Seq(
        Builders.JoinPart(groupBy = temporalEntityGroupBy("standard_temporal_entity_gb")),
        Builders.JoinPart(groupBy = standardGroupBy)
      ),
      bootstrapParts = Seq.empty
    )

    val plan = new JoinPlanner(join).buildPlan

    val temporalJoinPartNode = plan.nodes.asScala
      .find(node =>
        node.content.isSetJoinPart &&
          node.content.getJoinPart.joinPart.groupBy.metaData.name == "standard_temporal_entity_gb")
      .get

    val tableDeps = temporalJoinPartNode.metaData.executionInfo.tableDependencies.asScala
    val depTables = tableDeps.map(_.tableInfo.table)

    depTables should contain("test.dim_snapshot")
    depTables should contain("test.dim_mutations")
    depTables should contain(temporalJoinPartNode.content.getJoinPart.leftSourceTable)

    val snapshotDep = tableDeps.find(_.tableInfo.table == "test.dim_snapshot").get
    snapshotDep.startOffset should equal(WindowUtils.Day)
    snapshotDep.endOffset should equal(WindowUtils.Day)

    val mutationDep = tableDeps.find(_.tableInfo.table == "test.dim_mutations").get
    mutationDep.startOffset should equal(WindowUtils.zero())
    mutationDep.endOffset should equal(WindowUtils.zero())
  }

  it should "depend on upstream join output when the left source is a join source" in {
    val upstreamListingLookup = Builders.GroupBy(
      sources = Seq(Builders.Source.events(Builders.Query(partitionColumn = "ds"), table = "test.user_listings")),
      keyColumns = Seq("user_id"),
      aggregations = Seq(Builders.Aggregation(Operation.LAST, "listing_id", Seq(WindowUtils.Unbounded))),
      accuracy = Accuracy.TEMPORAL,
      metaData = Builders.MetaData(namespace = "test_namespace", name = "upstream_listing_lookup")
    )

    val upstreamJoin = Join(
      metaData = MetaData(
        name = "upstream_join",
        namespace = "test_namespace",
        executionInfo = modularExecutionInfo
      ),
      left = Builders.Source.events(Builders.Query(partitionColumn = "ds"), table = "test.left_events"),
      joinParts = Seq(Builders.JoinPart(groupBy = upstreamListingLookup))
    )

    val downstreamListingFeatures = Builders.GroupBy(
      sources = Seq(Builders.Source.events(Builders.Query(partitionColumn = "ds"), table = "test.listing_features")),
      keyColumns = Seq("listing_id"),
      aggregations = Seq(Builders.Aggregation(Operation.LAST, "price", Seq(WindowUtils.Unbounded))),
      accuracy = Accuracy.TEMPORAL,
      metaData = Builders.MetaData(namespace = "test_namespace", name = "downstream_listing_features")
    )

    val listingIdColumn = upstreamListingLookup.valueColumns.head
    val leftJoinSourceQuery = Builders.Query(
      selects = Builders.Selects.exprs(
        "user_id" -> "user_id",
        "listing_id" -> listingIdColumn,
        "ts" -> "ts"
      ),
      partitionColumn = "ds"
    )

    val downstreamJoin = Join(
      metaData = MetaData(
        name = "downstream_join",
        namespace = "test_namespace",
        executionInfo = modularExecutionInfo
      ),
      left = Builders.Source.joinSource(upstreamJoin, leftJoinSourceQuery),
      joinParts = Seq(Builders.JoinPart(groupBy = downstreamListingFeatures))
    )

    val plan = new JoinPlanner(downstreamJoin).buildPlan

    val sourceNode = plan.nodes.asScala.find(_.content.isSetSourceWithFilter).get
    val sourceDeps = sourceNode.metaData.executionInfo.tableDependencies.asScala.map(_.tableInfo.table)
    sourceDeps should contain(upstreamJoin.metaData.outputTable)

    val metadataUploadNode = plan.nodes.asScala.find(_.content.isSetJoinMetadataUpload).get
    val metadataDeps = metadataUploadNode.metaData.executionInfo.tableDependencies.asScala.map(_.tableInfo.table)
    metadataDeps should contain(downstreamListingFeatures.metaData.outputTable + "__uploadToKV")
    metadataDeps should not contain (upstreamJoin.metaData.outputTable + "__metadata_upload")
  }
}
