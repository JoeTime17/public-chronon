package ai.chronon.spark.join

import ai.chronon.api
import ai.chronon.api.Extensions.WindowUtils
import ai.chronon.api.{Accuracy, Builders, Constants, DateRange, ExecutionInfo, Operation, PartitionSpec, TableInfo, TsUtils}
import ai.chronon.planner.JoinMergeNode
import ai.chronon.spark.Extensions._
import ai.chronon.spark.Join
import ai.chronon.spark.batch.MergeJob
import ai.chronon.spark.catalog.TableUtils
import org.junit.Assert._

/** Snapshot-accuracy joins must stay as-of correct ACROSS snapshot changes: a row at
  * time T reads the latest snapshot whose time is <= T, on the RHS table's
  * declared grid, regardless of which left partition the row sits in. The interesting case
  * for sub-daily outputs is a left partition straddling midnight - its rows must SPLIT
  * across two snapshot versions.
  */
class SnapshotAsOfJoinTest extends BaseJoinTest {

  private def buildMergeJob(suffix: String): (MergeJob, api.JoinPart) = {
    val viewsGroupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.events(query = Builders.Query(), table = s"$namespace.snapshot_asof_views")),
      keyColumns = Seq("item"),
      aggregations = Seq(Builders.Aggregation(operation = Operation.AVERAGE, inputColumn = "time_spent_ms")),
      metaData = Builders.MetaData(name = s"unit_test.snapshot_asof_gb_$suffix", namespace = namespace),
      accuracy = Accuracy.SNAPSHOT
    )

    val joinPart = Builders.JoinPart(groupBy = viewsGroupBy)
    val join = Builders.Join(
      left = Builders.Source.events(Builders.Query(), table = s"$namespace.snapshot_asof_left"),
      joinParts = Seq(joinPart),
      metaData = Builders.MetaData(name = s"unit_test.snapshot_asof_join_$suffix", namespace = namespace)
    )

    val mergeJob = new MergeJob(
      new JoinMergeNode().setJoin(join),
      join.metaData,
      new DateRange().setStartDate("2026-06-03").setEndDate("2026-06-04"),
      Seq(joinPart)
    )(tableUtils)
    (mergeJob, joinPart)
  }

  // daily snapshots with a value CHANGE between versions: ds=06-02 holds state as of midnight
  // 06-03, ds=06-03 as of midnight 06-04
  private def snapshotDf = {
    import spark.implicits._
    Seq(
      ("a", 100.0, "2026-06-02"),
      ("a", 200.0, "2026-06-03")
    ).toDF("item", "time_spent_ms_average", "ds")
  }

  it should "match rows of one sub-daily left partition to different snapshot versions per row time" in {
    import spark.implicits._

    val (mergeJob, joinPart) = buildMergeJob("straddle")

    // two rows of the SAME 3h@01:00 left partition [22:00, 01:00) straddling midnight
    val leftDf = Seq(
      ("a", TsUtils.datetimeToTs("2026-06-03 23:30:00"), "2026-06-03-22-00"),
      ("a", TsUtils.datetimeToTs("2026-06-04 00:30:00"), "2026-06-03-22-00")
    ).toDF("item", "ts", "ds")
      .withTimeBasedColumn(Constants.TimePartitionColumn)

    val joined = mergeJob.joinWithLeft(leftDf, snapshotDf, joinPart)
    val valueCol = joined.columns.find(_.endsWith("time_spent_ms_average")).get

    val byTs = joined
      .collect()
      .map(row => row.getAs[Long]("ts") -> row.getAs[Double](valueCol))
      .toMap

    // pre-midnight row sees the old snapshot version, post-midnight row the new one
    assertEquals(100.0, byTs(TsUtils.datetimeToTs("2026-06-03 23:30:00")), 0.0)
    assertEquals(200.0, byTs(TsUtils.datetimeToTs("2026-06-04 00:30:00")), 0.0)
  }

  it should "match exact-boundary rows and rows 1ms either side of the RHS snapshot boundary" in {
    import spark.implicits._

    val (mergeJob, joinPart) = buildMergeJob("boundary")

    val boundary = TsUtils.datetimeToTs("2026-06-04 00:00:00") // snapshot time of ds=06-03
    val leftDf = Seq(
      ("a", boundary - 1, "2026-06-03-22-00"), // 1ms before: still the old snapshot
      ("a", boundary, "2026-06-03-22-00"), // exactly at the boundary: the new snapshot (snapshot time <= ts)
      ("a", boundary + 1, "2026-06-03-22-00") // 1ms after: the new snapshot
    ).toDF("item", "ts", "ds")
      .withTimeBasedColumn(Constants.TimePartitionColumn)

    val joined = mergeJob.joinWithLeft(leftDf, snapshotDf, joinPart)
    val valueCol = joined.columns.find(_.endsWith("time_spent_ms_average")).get

    val byTs = joined
      .collect()
      .map(row => row.getAs[Long]("ts") -> row.getAs[Double](valueCol))
      .toMap

    assertEquals(100.0, byTs(boundary - 1), 0.0)
    assertEquals(200.0, byTs(boundary), 0.0)
    assertEquals(200.0, byTs(boundary + 1), 0.0)
  }

  it should "dedupe monolith cross-grid snapshot RHS fan-out without dropping duplicate left rows" in {
    import spark.implicits._

    val hourMillis = 60L * 60 * 1000
    val subDailySpec =
      PartitionSpec("ds", "yyyy-MM-dd-HH-mm", 3 * hourMillis, hourMillis)
    val subDailyTableUtils = TableUtils(spark, subDailySpec)

    val dailySnapshotInfo = new TableInfo()
      .setPartitionColumn(PartitionSpec.daily.column)
      .setPartitionFormat(PartitionSpec.daily.format)
      .setPartitionInterval(WindowUtils.Day)

    val viewsGroupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.events(query = Builders.Query(), table = s"$namespace.snapshot_asof_views")),
      keyColumns = Seq("item"),
      aggregations = Seq(Builders.Aggregation(operation = Operation.AVERAGE, inputColumn = "time_spent_ms")),
      metaData = Builders
        .MetaData(name = "unit_test.snapshot_asof_gb_monolith_dedupe", namespace = namespace)
        .setExecutionInfo(new ExecutionInfo().setOutputTableInfo(dailySnapshotInfo)),
      accuracy = Accuracy.SNAPSHOT
    )
    val joinPart = Builders.JoinPart(groupBy = viewsGroupBy)
    val join = Builders.Join(
      left = Builders.Source.events(Builders.Query(), table = s"$namespace.snapshot_asof_left"),
      joinParts = Seq(joinPart),
      metaData = Builders.MetaData(name = "unit_test.snapshot_asof_join_monolith_dedupe", namespace = namespace)
    )
    val monolithJoin = new Join(join, "2026-06-03-22-00", subDailyTableUtils)

    val leftTs = TsUtils.datetimeToTs("2026-06-03 23:30:00")
    val leftDf = Seq(
      ("a", leftTs, "2026-06-03-22-00"),
      ("a", leftTs, "2026-06-03-22-00")
    ).toDF("item", "ts", "ds")

    val snapshotAsOf = TsUtils.datetimeToTs("2026-06-03 00:00:00")
    val fanoutRightDf = Seq(
      ("a", 100.0, snapshotAsOf, "2026-06-03-19-00"),
      ("a", 100.0, snapshotAsOf, "2026-06-03-22-00")
    ).toDF("item", "time_spent_ms_average", "ts", "ds")

    val joined = monolithJoin.joinWithLeft(leftDf, fanoutRightDf, joinPart)
    val valueCol = joined.columns.find(_.endsWith("time_spent_ms_average")).get
    val rows = joined.collect()

    assertEquals(2, rows.length)
    rows.foreach(row => assertEquals(100.0, row.getAs[Double](valueCol), 0.0))
  }
}
