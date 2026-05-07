package ai.chronon.api.test.planner

import ai.chronon.api.Extensions.{WindowUtils, MetadataOps}
import ai.chronon.api.planner.{DependencyResolver, TableDependencies}
import ai.chronon.api.{
  Accuracy,
  Builders,
  DataModel,
  Operation,
  PartitionRange,
  PartitionSpec,
  TableDependency,
  TableInfo,
  TimeUnit,
  Window
}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TableDependenciesTest extends AnyFlatSpec with Matchers {

  private implicit val testPartitionSpec: PartitionSpec = PartitionSpec.daily

  "TableDependencies.fromTable" should "handle left Source with null query (default behavior)" in {
    val table = "test.events_table"
    val shift = Some(WindowUtils.Day)

    val result = TableDependencies.fromTable(table, query = null, shift)

    result should not be null
    result.getTableInfo should not be null
    result.getTableInfo.getTable should equal(table)

    // When query is null, partition info should not be set
    result.getTableInfo.getPartitionColumn should be(null)
    result.getTableInfo.getPartitionFormat should be(null)
    result.getTableInfo.getPartitionInterval should be(null)

    // Offsets should use the shift parameter (WindowUtils.Day)
    result.getStartOffset should equal(shift.get)
    result.getEndOffset should equal(shift.get)

    // Cut-offs should be null
    result.getStartCutOff should be(null)
    result.getEndCutOff should be(null)
  }

  it should "handle left Source with null query and no shift (default behavior)" in {
    val table = "test.events_table"

    val result = TableDependencies.fromTable(table, query = null, shift = None)

    result should not be null
    result.getTableInfo should not be null
    result.getTableInfo.getTable should equal(table)

    // When query is null, partition info should not be set
    result.getTableInfo.getPartitionColumn should be(null)
    result.getTableInfo.getPartitionFormat should be(null)
    result.getTableInfo.getPartitionInterval should be(null)

    // Offsets should use zero when no shift is provided
    result.getStartOffset should equal(WindowUtils.zero())
    result.getEndOffset should equal(WindowUtils.zero())

    // Cut-offs should be null
    result.getStartCutOff should be(null)
    result.getEndCutOff should be(null)
  }

  it should "handle left Source with non-null query (validate partitionLag, start/end partitions)" in {
    val table = "test.events_table"
    val partitionLag = new Window(2, TimeUnit.DAYS)
    val startPartition = "2023-01-01"
    val endPartition = "2023-12-31"
    val partitionColumn = "ds"
    val partitionFormat = "yyyy-MM-dd"
    val partitionInterval = WindowUtils.Day

    val query = Builders.Query(
      startPartition = startPartition,
      endPartition = endPartition,
      partitionColumn = partitionColumn
    )
    query.setPartitionFormat(partitionFormat)
    query.setPartitionInterval(partitionInterval)
    query.setPartitionLag(partitionLag)

    val result = TableDependencies.fromTable(table, query)

    result should not be null
    result.getTableInfo should not be null
    result.getTableInfo.getTable should equal(table)

    // Partition information should be preserved from query
    result.getTableInfo.getPartitionColumn should equal(partitionColumn)
    result.getTableInfo.getPartitionFormat should equal(partitionFormat)
    result.getTableInfo.getPartitionInterval should equal(partitionInterval)

    // Offsets should use partitionLag from query
    result.getStartOffset should equal(partitionLag)
    result.getEndOffset should equal(partitionLag)

    // Cut-offs should use start/end partitions from query
    result.getStartCutOff should equal(startPartition)
    result.getEndCutOff should equal(endPartition)
  }

  it should "handle left Source with non-null query but no partitionLag" in {
    val table = "test.events_table"
    val startPartition = "2023-01-01"
    val endPartition = "2023-12-31"
    val partitionColumn = "ds"

    val query = Builders.Query(
      startPartition = startPartition,
      endPartition = endPartition,
      partitionColumn = partitionColumn
    )

    val result = TableDependencies.fromTable(table, query)

    result should not be null

    // When no partitionLag, should default to zero
    result.getStartOffset should equal(WindowUtils.zero())
    result.getEndOffset should equal(WindowUtils.zero())

    // Cut-offs should still be set
    result.getStartCutOff should equal(startPartition)
    result.getEndCutOff should equal(endPartition)
  }

  it should "handle left Source with non-null query and shift parameter" in {
    val table = "test.events_table"
    val partitionLag = new Window(1, TimeUnit.DAYS)
    val shift = Some(new Window(3, TimeUnit.DAYS))
    val startPartition = "2023-01-01"
    val endPartition = "2023-12-31"

    val query = Builders.Query(
      startPartition = startPartition,
      endPartition = endPartition,
      partitionColumn = "ds"
    )
    query.setPartitionLag(partitionLag)

    val result = TableDependencies.fromTable(table, query, shift)

    result should not be null

    result.getStartOffset should equal(partitionLag)
    result.getEndOffset should equal(partitionLag)

    // Cut-offs should still use query values
    result.getStartCutOff should equal(startPartition)
    result.getEndCutOff should equal(endPartition)
  }

  it should "prioritize partitionLag over shiftwhen both are provided" in {
    val table = "test.events_table"
    val partitionLag = new Window(1, TimeUnit.DAYS)
    val shift = Some(new Window(5, TimeUnit.HOURS))

    val query = Builders.Query()
    query.setPartitionLag(partitionLag)

    val result = TableDependencies.fromTable(table, query, shift)

    // shift should take precedence over partitionLag
    result.getStartOffset should equal(partitionLag)
    result.getEndOffset should equal(partitionLag)
  }

  it should "handle complete query with all partition settings" in {
    val table = "test.detailed_events"
    val partitionLag = new Window(1, TimeUnit.HOURS)
    val startPartition = "2023-06-01"
    val endPartition = "2023-06-30"
    val partitionColumn = "partition_date"
    val partitionFormat = "yyyy-MM-dd"
    val partitionInterval = new Window(1, TimeUnit.DAYS)

    val query = Builders.Query(
      startPartition = startPartition,
      endPartition = endPartition,
      partitionColumn = partitionColumn
    )
    query.setPartitionFormat(partitionFormat)
    query.setPartitionInterval(partitionInterval)
    query.setPartitionLag(partitionLag)

    val result = TableDependencies.fromTable(table, query)

    result should not be null
    val tableInfo = result.getTableInfo

    // Validate all table info fields
    tableInfo.getTable should equal(table)
    tableInfo.getPartitionColumn should equal(partitionColumn)
    tableInfo.getPartitionFormat should equal(partitionFormat)
    tableInfo.getPartitionInterval should equal(partitionInterval)

    // Validate dependency timing
    result.getStartOffset should equal(partitionLag)
    result.getEndOffset should equal(partitionLag)
    result.getStartCutOff should equal(startPartition)
    result.getEndCutOff should equal(endPartition)
  }

  it should "handle minimal query with only required fields" in {
    val table = "test.minimal_table"
    val partitionColumn = "dt"

    val query = Builders.Query(partitionColumn = partitionColumn)

    val result = TableDependencies.fromTable(table, query)

    result should not be null
    val tableInfo = result.getTableInfo

    tableInfo.getTable should equal(table)
    tableInfo.getPartitionColumn should equal(partitionColumn)

    // Optional fields should be null when not provided
    tableInfo.getPartitionFormat should be(null)
    tableInfo.getPartitionInterval should be(null)

    // Should default to zero offsets when no lag specified
    result.getStartOffset should equal(WindowUtils.zero())
    result.getEndOffset should equal(WindowUtils.zero())
    result.getStartCutOff should be(null)
    result.getEndCutOff should be(null)
  }

  "TableDependencies.fromJoinSources" should "create dependencies for JoinSources" in {
    import ai.chronon.api.Builders._

    // Create upstream join
    val upstreamJoin = Join(
      metaData = MetaData(namespace = "test_namespace", name = "upstream_join"),
      left = Source.events(Query(), table = "test_namespace.upstream_events"),
      joinParts = Seq.empty,
      bootstrapParts = Seq.empty
    )

    // Create sources with JoinSource (as Java List to match actual usage)
    import scala.jdk.CollectionConverters._
    val sources = Seq(
      Source.joinSource(upstreamJoin, Query()),
      Source.events(Query(), table = "test_namespace.regular_events"), // Regular source (should be filtered out)
      Source.joinSource(upstreamJoin, Query()) // Another JoinSource
    ).asJava

    val result = TableDependencies.fromJoinSources(sources)

    // Should only return dependencies for the 2 JoinSources, not the regular source
    result should have size 2

    result.foreach { dep =>
      dep.getTableInfo should not be null
      dep.getTableInfo.getTable should equal(upstreamJoin.metaData.outputTable + "__metadata_upload")
      dep.getStartOffset should equal(WindowUtils.zero())
      dep.getEndOffset should equal(WindowUtils.zero())
    }
  }

  it should "return empty sequence when no JoinSources are present" in {
    import ai.chronon.api.Builders._
    import scala.jdk.CollectionConverters._

    val sources = Seq(
      Source.events(Query(), table = "test_namespace.events1"),
      Source.events(Query(), table = "test_namespace.events2")
    ).asJava

    val result = TableDependencies.fromJoinSources(sources)

    result should be(empty)
  }

  it should "handle empty source list" in {
    import scala.jdk.CollectionConverters._
    val result = TableDependencies.fromJoinSources(Seq.empty.asJava)

    result should be(empty)
  }

  it should "handle null source list" in {
    val result = TableDependencies.fromJoinSources(null)

    result should be(empty)
  }

  // timePartitioned flag is deprecated — column type is detected automatically at runtime.
  // These tests verify that setting partitionColumn to a timestamp column name works without the flag.
  "TableDependencies.fromSource with timestamp partition column" should "set partitionColumn on TableInfo" in {
    val query = Builders.Query(
      partitionColumn = "created_at",
      timeColumn = "UNIX_TIMESTAMP(created_at) * 1000"
    )

    val source = Builders.Source.events(query, table = "test_db.events")
    val result = TableDependencies.fromSource(source)

    result should be(defined)
    val tableInfo = result.get.getTableInfo
    tableInfo.getTable should equal("test_db.events")
    tableInfo.getPartitionColumn should equal("created_at")
  }

  "TableDependencies.fromGroupBy with timestamp partition column" should "propagate partitionColumn through sources" in {
    val query = Builders.Query(
      partitionColumn = "created_at",
      timeColumn = "UNIX_TIMESTAMP(created_at) * 1000"
    )

    val source = Builders.Source.events(query, table = "test_db.events")
    val groupBy = Builders.GroupBy(
      sources = Seq(source),
      keyColumns = Seq("user_id"),
      aggregations = Seq(
        Builders.Aggregation(
          inputColumn = "value",
          operation = Operation.SUM,
          windows = Seq(new Window(7, TimeUnit.DAYS))
        )
      )
    )

    val deps = TableDependencies.fromGroupBy(groupBy)
    deps should not be empty
    deps.head.getTableInfo.getPartitionColumn should equal("created_at")
  }

  "TableDependencies.fromGroupBy" should "include snapshot and mutation dependencies for temporal entity sources when the left side is events" in {
    val entityQuery = Builders.Query(
      startPartition = "2025-01-01",
      partitionColumn = "ds"
    )
    entityQuery.setPartitionInterval(WindowUtils.Day)

    val groupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.entities(
        query = entityQuery,
        snapshotTable = "test.dim_snapshot",
        mutationTable = "test.dim_mutations"
      )),
      keyColumns = Seq("listing_id"),
      aggregations = Seq(Builders.Aggregation(Operation.LAST, "headline", Seq(WindowUtils.Unbounded))),
      accuracy = Accuracy.TEMPORAL
    )

    val deps = TableDependencies.fromGroupBy(groupBy, leftDataModel = Some(DataModel.EVENTS))

    deps.map(_.getTableInfo.getTable) should equal(Seq("test.dim_snapshot", "test.dim_mutations"))
    deps.head.getStartOffset should equal(WindowUtils.Day)
    deps.head.getEndOffset should equal(WindowUtils.Day)
    deps.head.getStartCutOff should equal("2025-01-01")
    deps(1).getStartOffset should equal(WindowUtils.zero())
    deps(1).getEndOffset should equal(WindowUtils.zero())
    deps(1).getStartCutOff should equal("2025-01-01")
  }

  "TableDependencies.fromJoin" should "include mutation dependencies for temporal entity join parts when the left side is events" in {
    val leftQuery = Builders.Query(partitionColumn = "ds")
    val entityQuery = Builders.Query(
      startPartition = "2025-01-01",
      partitionColumn = "ds"
    )
    entityQuery.setPartitionInterval(WindowUtils.Day)

    val join = Builders.Join(
      left = Builders.Source.events(leftQuery, table = "test.left_events"),
      joinParts = Seq(Builders.JoinPart(groupBy = Builders.GroupBy(
        sources = Seq(Builders.Source.entities(
          query = entityQuery,
          snapshotTable = "test.dim_snapshot",
          mutationTable = "test.dim_mutations"
        )),
        keyColumns = Seq("listing_id"),
        aggregations = Seq(Builders.Aggregation(Operation.LAST, "headline", Seq(WindowUtils.Unbounded))),
        accuracy = Accuracy.TEMPORAL
      ))),
      bootstrapParts = Seq.empty
    )

    val deps = TableDependencies.fromJoin(join)

    deps.map(_.getTableInfo.getTable) should equal(
      Seq("test.left_events", "test.dim_snapshot", "test.dim_mutations")
    )
    deps(1).getStartOffset should equal(WindowUtils.Day)
    deps(1).getEndOffset should equal(WindowUtils.Day)
    deps(2).getStartOffset should equal(WindowUtils.zero())
    deps(2).getEndOffset should equal(WindowUtils.zero())
  }

  // Mirrors the three shapes the Python TableDependency dataclass can produce after
  // exposing start_cutoff / end_cutoff. These pin down what each shape resolves to
  // in the planner — critical because the platform orchestrator expands the full
  // resolved range and requires every date in it to be Filled on the upstream.
  private def tableDepOf(startOffset: Window,
                         endOffset: Window,
                         startCutOff: String = null,
                         endCutOff: String = null): TableDependency = {
    new TableDependency()
      .setTableInfo(new TableInfo().setTable("ns.upstream"))
      .setStartOffset(startOffset)
      .setEndOffset(endOffset)
      .setStartCutOff(startCutOff)
      .setEndCutOff(endCutOff)
  }

  "DependencyResolver.computeInputRange" should "return [Q.start, Q.end] for symmetric offset=0" in {
    val queryRange = PartitionRange("2026-01-10", "2026-01-10")
    val dep = tableDepOf(WindowUtils.zero(), WindowUtils.zero())

    DependencyResolver.computeInputRange(queryRange, dep) should equal(
      Some(PartitionRange("2026-01-10", "2026-01-10"))
    )
  }

  it should "pin the start at startCutOff when startOffset is null and endOffset is zero" in {
    val queryRange = PartitionRange("2026-01-10", "2026-01-10")
    val dep = tableDepOf(
      startOffset = null,
      endOffset = WindowUtils.zero(),
      startCutOff = "2024-01-01"
    )

    DependencyResolver.computeInputRange(queryRange, dep) should equal(
      Some(PartitionRange("2024-01-01", "2026-01-10"))
    )
  }

  it should "honor both startCutOff and offset — cutoff wins when query.start - offset goes past it" in {
    val queryRange = PartitionRange("2024-01-03", "2024-01-10")
    val sevenDays = new Window(7, TimeUnit.DAYS)
    val dep = tableDepOf(
      startOffset = sevenDays,
      endOffset = sevenDays,
      startCutOff = "2024-01-01"
    )

    // query.start - 7d = 2023-12-27 which is < startCutOff, so start clamps to the cutoff.
    DependencyResolver.computeInputRange(queryRange, dep) should equal(
      Some(PartitionRange("2024-01-01", "2024-01-03"))
    )
  }
}
