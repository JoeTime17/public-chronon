package ai.chronon.api.test.planner

import ai.chronon.api.Builders.{Join, MetaData}
import ai.chronon.api.Extensions.WindowUtils
import ai.chronon.api.Extensions._
import ai.chronon.api.planner.{JoinPlanner, MonolithJoinPlanner}
import ai.chronon.api.{Accuracy, Builders, ConfigProperties, ExecutionInfo, Operation, PartitionSpec, TableInfo}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

class JoinPlannerTest extends AnyFlatSpec with Matchers {

  private implicit val testPartitionSpec: PartitionSpec = PartitionSpec.daily
  private val threeHourSpec = PartitionSpec("ds", "yyyy-MM-dd-HH-mm", 3 * 60 * 60 * 1000)
  private val offsetThreeHourSpec =
    PartitionSpec("ds", "yyyy-MM-dd-HH-mm", 3 * 60 * 60 * 1000, 60 * 60 * 1000)

  private def outputTableInfo(table: String, spec: PartitionSpec): TableInfo = {
    val result = new TableInfo()
      .setTable(table)
      .setPartitionColumn(spec.column)
      .setPartitionFormat(spec.format)
      .setPartitionInterval(WindowUtils.fromMillis(spec.spanMillis))
    if (spec.offsetMillis != 0) result.setPartitionOffset(WindowUtils.fromMillis(spec.offsetMillis))
    result
  }

  private def executionInfoFor(table: String, spec: PartitionSpec): ExecutionInfo =
    new ExecutionInfo().setOutputTableInfo(outputTableInfo(table, spec))

  // join boundaries must sit on the left source's boundaries: sub-daily joins need a left
  // whose declared grid the join's boundaries line up on, so sub-daily fixtures declare the
  // left's partition interval explicitly
  private def leftEventsWithSpec(spec: PartitionSpec): ai.chronon.api.Source = {
    val query = Builders.Query(partitionColumn = spec.column)
    query.setPartitionFormat(spec.format)
    query.setPartitionInterval(WindowUtils.fromMillis(spec.spanMillis))
    Builders.Source.events(query, table = "test.left_events")
  }

  private def sourceEventsWithSpec(table: String, spec: PartitionSpec): ai.chronon.api.Source = {
    val query = Builders.Query(partitionColumn = spec.column)
    query.setPartitionFormat(spec.format)
    query.setPartitionInterval(WindowUtils.fromMillis(spec.spanMillis))
    if (spec.offsetMillis != 0) query.setPartitionOffset(WindowUtils.fromMillis(spec.offsetMillis))
    Builders.Source.events(query, table = table)
  }

  private def groupByWithOutputSpec(name: String,
                                    spec: PartitionSpec,
                                    accuracy: Accuracy = Accuracy.TEMPORAL,
                                    sourceSpec: Option[PartitionSpec] = None): ai.chronon.api.GroupBy =
    Builders.GroupBy(
      sources = Seq(sourceSpec
        .map(sourceEventsWithSpec(s"test.$name", _))
        .getOrElse(Builders.Source.events(Builders.Query(partitionColumn = "ds"), table = s"test.$name"))),
      keyColumns = Seq("listing_id"),
      aggregations = Seq(Builders.Aggregation(Operation.COUNT, "event_count", Seq(WindowUtils.Unbounded))),
      accuracy = accuracy,
      metaData = Builders.MetaData(
        namespace = "test_namespace",
        name = name,
        executionInfo = executionInfoFor(s"test_namespace.$name", spec)
      )
    )

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

  it should "allow daily joins over sub-daily groupBy outputs and preserve dependency partition specs" in {
    val hourlyGroupBy = groupByWithOutputSpec("three_hour_gb", threeHourSpec, sourceSpec = Some(threeHourSpec))
    val join = Join(
      metaData = MetaData(name = "daily_join", namespace = "test_namespace"),
      left = Builders.Source.events(Builders.Query(partitionColumn = "ds"), table = "test.left_events"),
      joinParts = Seq(Builders.JoinPart(groupBy = hourlyGroupBy))
    )

    val plan = new JoinPlanner(join).buildPlan

    val metadataUploadNode = plan.nodes.asScala.find(_.content.isSetJoinMetadataUpload).get
    val groupByDep = metadataUploadNode.metaData.executionInfo.tableDependencies.asScala
      .find(_.tableInfo.table == hourlyGroupBy.metaData.outputTable + "__uploadToKV")
      .get
    groupByDep.tableInfo.partitionFormat should equal(threeHourSpec.format)
    groupByDep.tableInfo.partitionInterval should equal(WindowUtils.fromMillis(threeHourSpec.spanMillis))

    val backfillNode = plan.nodes.asScala.find(_.content.isSetUnionJoin).get
    backfillNode.metaData.executionInfo.outputTableInfo.partitionFormat should equal(PartitionSpec.daily.format)
  }

  it should "allow daily temporal join parts over offset sub-daily event sources" in {
    val offsetGroupBy =
      groupByWithOutputSpec("offset_temporal_gb", offsetThreeHourSpec, sourceSpec = Some(offsetThreeHourSpec))
    val join = Join(
      metaData = MetaData(
        name = "daily_join_offset_temporal_source",
        namespace = "test_namespace",
        executionInfo = modularExecutionInfo
      ),
      left = Builders.Source.events(Builders.Query(partitionColumn = "ds"), table = "test.left_events"),
      joinParts = Seq(Builders.JoinPart(groupBy = offsetGroupBy)),
      bootstrapParts = Seq.empty
    )

    val plan = new JoinPlanner(join).buildPlan
    val joinPartNode = plan.nodes.asScala.find(_.content.isSetJoinPart).get
    val sourceDep = joinPartNode.metaData.executionInfo.tableDependencies.asScala
      .find(_.tableInfo.table == "test.offset_temporal_gb")
      .get

    sourceDep.tableInfo.partitionFormat should equal(offsetThreeHourSpec.format)
    sourceDep.tableInfo.partitionInterval should equal(WindowUtils.fromMillis(offsetThreeHourSpec.spanMillis))
    sourceDep.tableInfo.partitionOffset should equal(WindowUtils.fromMillis(offsetThreeHourSpec.offsetMillis))
  }

  it should "resolve metadata upload dependencies using the upstream output grid, not the join grid" in {
    val defaultDailyGroupBy = Builders.GroupBy(
      sources = Seq(sourceEventsWithSpec("test.daily_source", threeHourSpec)),
      keyColumns = Seq("listing_id"),
      aggregations = Seq(Builders.Aggregation(Operation.COUNT, "event_count", Seq(WindowUtils.Unbounded))),
      accuracy = Accuracy.TEMPORAL,
      metaData = Builders.MetaData(namespace = "test_namespace", name = "default_daily_gb")
    )

    val subDailyJoin = Join(
      metaData = MetaData(
        name = "subdaily_join_with_daily_upload_dep",
        namespace = "test_namespace",
        executionInfo = executionInfoFor("test_namespace.subdaily_join_with_daily_upload_dep", threeHourSpec)
      ),
      left = leftEventsWithSpec(threeHourSpec),
      joinParts = Seq(Builders.JoinPart(groupBy = defaultDailyGroupBy))
    )

    val plan = new JoinPlanner(subDailyJoin).buildPlan
    val metadataUploadNode = plan.nodes.asScala.find(_.content.isSetJoinMetadataUpload).get
    val groupByDep = metadataUploadNode.metaData.executionInfo.tableDependencies.asScala
      .find(_.tableInfo.table == defaultDailyGroupBy.metaData.outputTable + "__uploadToKV")
      .get

    groupByDep.tableInfo.partitionFormat should equal(PartitionSpec.daily.format)
    groupByDep.tableInfo.partitionInterval should equal(WindowUtils.Day)
  }

  it should "allow sub-daily joins over coarser groupBy outputs - parts read by left row time" in {
    // snapshot parts pick the latest snapshot at or before the left row time and
    // temporal parts recompute from raw events, so a finer join over a coarser groupBy output
    // grid is staleness, not missing data
    val dailySnapshotGroupBy = groupByWithOutputSpec("daily_snapshot_gb", PartitionSpec.daily, Accuracy.SNAPSHOT)
    val dailyTemporalGroupBy =
      groupByWithOutputSpec("daily_temporal_gb", PartitionSpec.daily, sourceSpec = Some(threeHourSpec))
    val subDailyJoin = Join(
      metaData = MetaData(
        name = "three_hour_join",
        namespace = "test_namespace",
        executionInfo = executionInfoFor("test_namespace.three_hour_join", threeHourSpec)
      ),
      left = leftEventsWithSpec(threeHourSpec),
      joinParts =
        Seq(Builders.JoinPart(groupBy = dailySnapshotGroupBy),
            Builders.JoinPart(groupBy = dailyTemporalGroupBy))
    )

    noException should be thrownBy new JoinPlanner(subDailyJoin).buildPlan
  }

  it should "reject sub-daily temporal event join parts with undeclared source intervals" in {
    val temporalGroupBy =
      groupByWithOutputSpec("undeclared_temporal_event_gb", PartitionSpec.daily, Accuracy.TEMPORAL)
    val subDailyJoin = Join(
      metaData = MetaData(
        name = "undeclared_temporal_event_join",
        namespace = "test_namespace",
        executionInfo = executionInfoFor("test_namespace.undeclared_temporal_event_join", threeHourSpec)
      ),
      left = leftEventsWithSpec(threeHourSpec),
      joinParts = Seq(Builders.JoinPart(groupBy = temporalGroupBy))
    )

    val error = the[IllegalArgumentException] thrownBy new JoinPlanner(subDailyJoin).buildPlan
    error.getMessage should include("temporal join part undeclared_temporal_event_gb source test.undeclared_temporal_event_gb")
    error.getMessage should include("time_partitioned")
  }

  it should "reject a sub-daily join over a left source with no declared partition interval" in {
    // unlike right parts, the left feeds the join's own partitions: an undeclared (implicitly
    // daily) left under a sub-daily join is the silent intraday-staleness trap
    val subDailyJoin = Join(
      metaData = MetaData(
        name = "undeclared_left_join",
        namespace = "test_namespace",
        executionInfo = executionInfoFor("test_namespace.undeclared_left_join", threeHourSpec)
      ),
      left = Builders.Source.events(Builders.Query(partitionColumn = "ds"), table = "test.left_events"),
      joinParts = Seq(Builders.JoinPart(groupBy = groupByWithOutputSpec("any_gb", threeHourSpec)))
    )

    val error = the[IllegalArgumentException] thrownBy new JoinPlanner(subDailyJoin).buildPlan
    error.getMessage should include("time_partitioned")
  }

  it should "allow a sub-daily join over an undeclared left marked time_partitioned" in {
    val left = Builders.Source.events(Builders.Query(partitionColumn = "ds"), table = "test.left_events")
    left.getEvents.query.setTimePartitioned(true)
    val subDailyJoin = Join(
      metaData = MetaData(
        name = "tp_left_join",
        namespace = "test_namespace",
        executionInfo = executionInfoFor("test_namespace.tp_left_join", threeHourSpec)
      ),
      left = left,
      joinParts =
        Seq(Builders.JoinPart(groupBy = groupByWithOutputSpec("tp_any_gb",
                                                              threeHourSpec,
                                                              sourceSpec = Some(threeHourSpec))))
    )

    noException should be thrownBy new JoinPlanner(subDailyJoin).buildPlan
  }

  it should "reject a sub-daily join over a left that declares a coarser grid" in {
    val subDailyJoin = Join(
      metaData = MetaData(
        name = "coarse_left_join",
        namespace = "test_namespace",
        executionInfo = executionInfoFor("test_namespace.coarse_left_join", threeHourSpec)
      ),
      left = leftEventsWithSpec(PartitionSpec.daily),
      joinParts = Seq(Builders.JoinPart(groupBy = groupByWithOutputSpec("coarse_any_gb", threeHourSpec)))
    )

    an[IllegalArgumentException] should be thrownBy new JoinPlanner(subDailyJoin).buildPlan
  }

  it should "reject invalid grids inside embedded left JoinSource trees" in {
    val upstreamJoin = Builders.Join(
      metaData = Builders.MetaData(
        name = "upstream_join",
        namespace = "test_namespace",
        executionInfo = executionInfoFor("test_namespace.upstream_join", threeHourSpec)
      ),
      left = Builders.Source.events(Builders.Query(partitionColumn = "ds"), table = "test.daily_left"),
      joinParts = Seq.empty,
      bootstrapParts = Seq.empty
    )

    val downstreamJoin = Join(
      metaData = MetaData(
        name = "downstream_join",
        namespace = "test_namespace",
        executionInfo = executionInfoFor("test_namespace.downstream_join", threeHourSpec)
      ),
      left = Builders.Source.joinSource(upstreamJoin, Builders.Query(partitionColumn = "ds")),
      joinParts = Seq.empty
    )

    val error = the[IllegalArgumentException] thrownBy new JoinPlanner(downstreamJoin).buildPlan
    error.getMessage should include("upstream_join")
    error.getMessage should include("no declared partition_interval")
  }

  it should "shift same-grid snapshot part merge dependencies only" in {
    def mergeSnapshotDepOffset(joinSpec: PartitionSpec, groupBySpec: PartitionSpec): ai.chronon.api.Window = {
      val snapshotGroupBy =
        groupByWithOutputSpec(s"snapshot_gb_${joinSpec.spanMillis}_${groupBySpec.spanMillis}",
                              groupBySpec,
                              Accuracy.SNAPSHOT)
      val join = Join(
        metaData = MetaData(
          name = s"merge_shift_join_${joinSpec.spanMillis}_${groupBySpec.spanMillis}",
          namespace = "test_namespace",
          executionInfo = executionInfoFor(s"test_namespace.merge_shift_join_${joinSpec.spanMillis}", joinSpec)
        ),
        left = leftEventsWithSpec(joinSpec),
        joinParts = Seq(Builders.JoinPart(groupBy = snapshotGroupBy))
      )
      val plan = new JoinPlanner(join).buildPlan
      val mergeNode = plan.nodes.asScala.find(_.content.isSetJoinMerge).get
      val partDep = mergeNode.metaData.executionInfo.tableDependencies.asScala
        .find(_.tableInfo.table.contains("snapshot_gb"))
        .get
      partDep.startOffset
    }

    // Same-grid snapshot parts keep the historical behavior: read one partition back.
    mergeSnapshotDepOffset(PartitionSpec.daily, PartitionSpec.daily).millis should equal(WindowUtils.Day.millis)
    mergeSnapshotDepOffset(threeHourSpec, threeHourSpec).millis should equal(threeHourSpec.spanMillis)

    // Cross-grid snapshot part tables are partitioned exactly like the join output and carry
    // each row's snapshot time in `ts`, so readiness stays on the requested ds range; no shift.
    mergeSnapshotDepOffset(PartitionSpec.daily, threeHourSpec).millis shouldBe 0L
    mergeSnapshotDepOffset(threeHourSpec, PartitionSpec.daily).millis shouldBe 0L
  }

  it should "shift join part source dependencies only when snapshot and join grids differ" in {
    def sourceDeclaredSnapshotGroupBy(name: String,
                                      table: String,
                                      spec: PartitionSpec): ai.chronon.api.GroupBy = {
      val sourceQuery = Builders.Query(partitionColumn = spec.column)
      sourceQuery.setPartitionFormat(spec.format)
      sourceQuery.setPartitionInterval(WindowUtils.fromMillis(spec.spanMillis))
      Builders.GroupBy(
        sources = Seq(Builders.Source.events(sourceQuery, table = table)),
        keyColumns = Seq("listing_id"),
        aggregations = Seq(Builders.Aggregation(Operation.COUNT, "event_count", Seq(WindowUtils.Unbounded))),
        accuracy = Accuracy.SNAPSHOT,
        metaData = Builders.MetaData(namespace = "test_namespace", name = name)
      )
    }

    def sourceDepFor(groupBy: ai.chronon.api.GroupBy, sourceTable: String) = {
      val join = Join(
        metaData = MetaData(
          name = s"${groupBy.metaData.name}_join",
          namespace = "test_namespace",
          executionInfo = modularExecutionInfo
        ),
        left = Builders.Source.events(Builders.Query(partitionColumn = "ds"), table = "test.left_events"),
        joinParts = Seq(Builders.JoinPart(groupBy = groupBy))
      )
      val plan = new JoinPlanner(join).buildPlan
      val joinPartNode = plan.nodes.asScala.find(_.content.isSetJoinPart).get
      val resolvedSnapshotInfo = joinPartNode.content.getJoinPart.joinPart.groupBy.metaData.executionInfo.outputTableInfo
      val sourceDep = joinPartNode.metaData.executionInfo.tableDependencies.asScala
        .find(_.tableInfo.table == sourceTable)
        .get
      (resolvedSnapshotInfo, sourceDep)
    }

    val (sameGridSnapshotInfo, sameGridDep) =
      sourceDepFor(sourceDeclaredSnapshotGroupBy("daily_source_declared_snapshot_gb",
                                                 "test.daily_snapshot_source",
                                                 PartitionSpec.daily),
                   "test.daily_snapshot_source")
    sameGridSnapshotInfo.partitionFormat should equal(PartitionSpec.daily.format)
    sameGridSnapshotInfo.partitionInterval should equal(WindowUtils.Day)
    sameGridDep.startOffset should be(null)
    sameGridDep.endOffset should equal(WindowUtils.zero())

    val (crossGridSnapshotInfo, crossGridDep) =
      sourceDepFor(sourceDeclaredSnapshotGroupBy("three_hour_source_declared_snapshot_gb",
                                                 "test.three_hour_snapshot_source",
                                                 threeHourSpec),
                   "test.three_hour_snapshot_source")
    crossGridSnapshotInfo.partitionFormat should equal(threeHourSpec.format)
    crossGridSnapshotInfo.partitionInterval should equal(WindowUtils.fromMillis(threeHourSpec.spanMillis))
    crossGridDep.startOffset should be(null)
    crossGridDep.endOffset should equal(WindowUtils.fromMillis(threeHourSpec.spanMillis))
  }

  it should "shift monolith snapshot source dependencies when snapshot and join grids differ" in {
    val sourceQuery = Builders.Query(partitionColumn = threeHourSpec.column)
    sourceQuery.setPartitionFormat(threeHourSpec.format)
    sourceQuery.setPartitionInterval(WindowUtils.fromMillis(threeHourSpec.spanMillis))
    val snapshotGroupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.events(sourceQuery, table = "test.three_hour_snapshot_source")),
      keyColumns = Seq("listing_id"),
      aggregations = Seq(Builders.Aggregation(Operation.COUNT, "event_count", Seq(WindowUtils.Unbounded))),
      accuracy = Accuracy.SNAPSHOT,
      metaData = Builders.MetaData(namespace = "test_namespace", name = "monolith_three_hour_snapshot_gb")
    )
    val join = Join(
      metaData = MetaData(name = "monolith_daily_join", namespace = "test_namespace"),
      left = Builders.Source.events(Builders.Query(partitionColumn = "ds"), table = "test.left_events"),
      joinParts = Seq(Builders.JoinPart(groupBy = snapshotGroupBy))
    )

    val plan = MonolithJoinPlanner(join).buildPlan
    val monolithNode = plan.nodes.asScala.find(_.content.isSetMonolithJoin).get
    val sourceDep = monolithNode.metaData.executionInfo.tableDependencies.asScala
      .find(_.tableInfo.table == "test.three_hour_snapshot_source")
      .get

    sourceDep.startOffset should be(null)
    sourceDep.endOffset should equal(WindowUtils.fromMillis(threeHourSpec.spanMillis))
  }

  it should "partition modular join part intermediates in the join output domain" in {
    val hourlyGroupBy = groupByWithOutputSpec("modular_three_hour_gb", threeHourSpec, sourceSpec = Some(threeHourSpec))
    val dailyGroupBy = groupByWithOutputSpec("modular_daily_gb", PartitionSpec.daily)
    val join = Join(
      metaData = MetaData(
        name = "daily_modular_join",
        namespace = "test_namespace",
        executionInfo = modularExecutionInfo
      ),
      left = Builders.Source.events(Builders.Query(partitionColumn = "ds"), table = "test.left_events"),
      joinParts = Seq(Builders.JoinPart(groupBy = hourlyGroupBy), Builders.JoinPart(groupBy = dailyGroupBy))
    )

    val plan = new JoinPlanner(join).buildPlan

    val hourlyJoinPartNode = plan.nodes.asScala
      .find(node =>
        node.content.isSetJoinPart &&
          node.content.getJoinPart.joinPart.groupBy.metaData.name == hourlyGroupBy.metaData.name)
      .get
    val joinPartOutputTableInfo = hourlyJoinPartNode.metaData.executionInfo.outputTableInfo

    joinPartOutputTableInfo.partitionFormat should equal(PartitionSpec.daily.format)
    joinPartOutputTableInfo.partitionInterval should equal(WindowUtils.Day)
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

  it should "put the left table first in merge node dependencies" in {
    val standardGroupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.events(Builders.Query(partitionColumn = "ds"), table = "test.other_events")),
      keyColumns = Seq("listing_id"),
      aggregations = Seq(Builders.Aggregation(Operation.COUNT, "event_count", Seq(WindowUtils.Unbounded))),
      accuracy = Accuracy.TEMPORAL,
      metaData = Builders.MetaData(namespace = "test_namespace", name = "merge_order_events_gb")
    )

    val join = Join(
      metaData = MetaData(
        name = "modular_merge_order_join",
        namespace = "test_namespace",
        executionInfo = modularExecutionInfo
      ),
      left = Builders.Source.events(Builders.Query(partitionColumn = "ds"), table = "test.left_events"),
      joinParts = Seq(
        Builders.JoinPart(groupBy = temporalEntityGroupBy("merge_order_temporal_entity_gb")),
        Builders.JoinPart(groupBy = standardGroupBy)
      )
    )

    val plan = new JoinPlanner(join).buildPlan

    val sourceNode = plan.nodes.asScala.find(_.content.isSetSourceWithFilter).get
    val joinPartNodes = plan.nodes.asScala.filter(_.content.isSetJoinPart)
    val mergeNode = plan.nodes.asScala.find(_.content.isSetJoinMerge).get

    val depTables = mergeNode.metaData.executionInfo.tableDependencies.asScala.map(_.tableInfo.table)

    depTables.head should equal(sourceNode.metaData.outputTable)
    depTables.tail should equal(joinPartNodes.map(_.metaData.outputTable))
  }

  it should "put the bootstrap left table first in merge node dependencies" in {
    val standardGroupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.events(Builders.Query(partitionColumn = "ds"), table = "test.other_events")),
      keyColumns = Seq("listing_id"),
      aggregations = Seq(Builders.Aggregation(Operation.COUNT, "event_count", Seq(WindowUtils.Unbounded))),
      accuracy = Accuracy.TEMPORAL,
      metaData = Builders.MetaData(namespace = "test_namespace", name = "merge_bootstrap_order_events_gb")
    )

    val join = Join(
      metaData = MetaData(
        name = "modular_merge_bootstrap_order_join",
        namespace = "test_namespace",
        executionInfo = modularExecutionInfo
      ),
      left = Builders.Source.events(Builders.Query(partitionColumn = "ds"), table = "test.left_events"),
      joinParts = Seq(
        Builders.JoinPart(groupBy = temporalEntityGroupBy("merge_bootstrap_order_temporal_entity_gb")),
        Builders.JoinPart(groupBy = standardGroupBy)
      ),
      bootstrapParts = Seq(
        Builders.BootstrapPart(
          table = "test.bootstrap_features",
          query = Builders.Query(partitionColumn = "ds"),
          keyColumns = Seq("listing_id")
        )
      )
    )

    val plan = new JoinPlanner(join).buildPlan

    val bootstrapNode = plan.nodes.asScala.find(_.content.isSetJoinBootstrap).get
    val joinPartNodes = plan.nodes.asScala.filter(_.content.isSetJoinPart)
    val mergeNode = plan.nodes.asScala.find(_.content.isSetJoinMerge).get

    val depTables = mergeNode.metaData.executionInfo.tableDependencies.asScala.map(_.tableInfo.table)

    depTables.head should equal(bootstrapNode.metaData.outputTable)
    depTables.tail should equal(joinPartNodes.map(_.metaData.outputTable))
  }

  it should "put the source table first in bootstrap node dependencies" in {
    val standardGroupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.events(Builders.Query(partitionColumn = "ds"), table = "test.other_events")),
      keyColumns = Seq("listing_id"),
      aggregations = Seq(Builders.Aggregation(Operation.COUNT, "event_count", Seq(WindowUtils.Unbounded))),
      accuracy = Accuracy.TEMPORAL,
      metaData = Builders.MetaData(namespace = "test_namespace", name = "bootstrap_order_events_gb")
    )

    val join = Join(
      metaData = MetaData(
        name = "modular_bootstrap_order_join",
        namespace = "test_namespace",
        executionInfo = modularExecutionInfo
      ),
      left = Builders.Source.events(Builders.Query(partitionColumn = "ds"), table = "test.left_events"),
      joinParts = Seq(Builders.JoinPart(groupBy = standardGroupBy)),
      bootstrapParts = Seq(
        Builders.BootstrapPart(
          table = "test.bootstrap_features",
          query = Builders.Query(partitionColumn = "ds"),
          keyColumns = Seq("listing_id")
        )
      )
    )

    val plan = new JoinPlanner(join).buildPlan

    val sourceNode = plan.nodes.asScala.find(_.content.isSetSourceWithFilter).get
    val bootstrapNode = plan.nodes.asScala.find(_.content.isSetJoinBootstrap).get

    val depTables = bootstrapNode.metaData.executionInfo.tableDependencies.asScala.map(_.tableInfo.table)

    depTables.head should equal(sourceNode.metaData.outputTable)
    depTables.tail should equal(Seq("test.bootstrap_features"))
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
