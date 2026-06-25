package ai.chronon.api.test

import ai.chronon.api.Extensions._
import ai.chronon.api._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.Collections

/** The output partition grid (interval + offset) is a data-layout property: changing it renames every ds in
  * the output table, so it must participate in semantic hashing and force a version bump. The
  * schedule (and hence the derived processing delay) is execution-only and must not. The grid
  * participates via a conditional token so every existing (daily) conf keeps its exact hash.
  */
class SemanticHashGridTest extends AnyFlatSpec with Matchers {

  private def grid(interval: Window, offset: Option[Window] = None): TableInfo = {
    val tableInfo = new TableInfo()
    tableInfo.setPartitionInterval(interval)
    offset.foreach(tableInfo.setPartitionOffset)
    tableInfo
  }

  private def buildGroupBy(tableInfo: Option[TableInfo], schedule: String = "@daily"): GroupBy = {
    val executionInfo = new ExecutionInfo().setOfflineSchedule(schedule)
    tableInfo.foreach(executionInfo.setOutputTableInfo)
    val metaData = new MetaData().setName("test_team.test_gb").setExecutionInfo(executionInfo)
    new GroupBy()
      .setMetaData(metaData)
      .setKeyColumns(Collections.singletonList("k"))
  }

  private def buildJoin(tableInfo: Option[TableInfo]): Join = {
    val executionInfo = new ExecutionInfo()
    tableInfo.foreach(executionInfo.setOutputTableInfo)
    val metaData = new MetaData().setName("test_team.test_join").setExecutionInfo(executionInfo)
    val left = new Source()
    left.setEvents(new EventSource().setTable("ns.events").setQuery(new Query()))
    new Join()
      .setMetaData(metaData)
      .setLeft(left)
      .setJoinParts(Collections.emptyList[JoinPart]())
  }

  "GroupBy.semanticHash" should "stay byte-stable for daily and unset grids" in {
    val unset = buildGroupBy(None)
    val dayUnit = buildGroupBy(Some(grid(new Window(1, TimeUnit.DAYS))))
    val hourUnit = buildGroupBy(Some(grid(new Window(24, TimeUnit.HOURS))))

    // Window(1, DAYS), Window(24, HOURS) and no declaration all mean the same daily grid:
    // they must hash identically or existing confs churn on recompile
    dayUnit.semanticHash should be(unset.semanticHash)
    hourUnit.semanticHash should be(unset.semanticHash)
  }

  it should "change when the grid interval or offset changes" in {
    val daily = buildGroupBy(None)
    val threeHour = buildGroupBy(Some(grid(new Window(3, TimeUnit.HOURS))))
    val offsetThreeHour =
      buildGroupBy(Some(grid(new Window(3, TimeUnit.HOURS), Some(new Window(1, TimeUnit.HOURS)))))

    threeHour.semanticHash should not be daily.semanticHash
    offsetThreeHour.semanticHash should not be threeHour.semanticHash
  }

  it should "treat equal-millis interval units as the same grid" in {
    val hoursForm = buildGroupBy(Some(grid(new Window(3, TimeUnit.HOURS))))
    val minutesForm = buildGroupBy(Some(grid(new Window(180, TimeUnit.MINUTES))))
    hoursForm.semanticHash should be(minutesForm.semanticHash)
  }

  it should "not change when only the schedule changes" in {
    val onTheHour = buildGroupBy(Some(grid(new Window(3, TimeUnit.HOURS))), schedule = "0 */3 * * *")
    val delayed = buildGroupBy(Some(grid(new Window(3, TimeUnit.HOURS))), schedule = "20 */3 * * *")
    onTheHour.semanticHash should be(delayed.semanticHash)
  }

  "Join.semanticHash" should "mix the join output grid into the left hash" in {
    val daily = buildJoin(None)
    val dailyExplicit = buildJoin(Some(grid(new Window(1, TimeUnit.DAYS))))
    val threeHour = buildJoin(Some(grid(new Window(3, TimeUnit.HOURS))))

    val leftSourceKey = "left_source"
    dailyExplicit.semanticHash(leftSourceKey) should be(daily.semanticHash(leftSourceKey))
    threeHour.semanticHash(leftSourceKey) should not be daily.semanticHash(leftSourceKey)
  }
}
