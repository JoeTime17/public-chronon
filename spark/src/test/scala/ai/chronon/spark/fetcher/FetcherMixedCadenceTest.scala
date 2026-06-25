package ai.chronon.spark.fetcher

import ai.chronon.api._
import ai.chronon.api.Extensions._
import ai.chronon.spark.Extensions.DataframeOps
import ai.chronon.spark.utils.SparkTestBase
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.matchers.should.Matchers

import java.util.TimeZone

class FetcherMixedCadenceTest extends SparkTestBase with Matchers {

  private val hourlyPartitionSpec = PartitionSpec("ds", "yyyy-MM-dd-HH-mm", WindowUtils.Hour.millis)
  private val threeHourPartitionSpec = PartitionSpec("ds", "yyyy-MM-dd-HH-mm", 3 * WindowUtils.Hour.millis)
  private val dailyPartitionSpec = PartitionSpec.daily
  private val sixHourWindow = new Window(6, TimeUnit.HOURS)
  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

  it should "match offline and online values for realtime hourly and daily join parts" in {
    val namespace = "mixed_cadence_fetcher_test"
    val joinConf = generateMixedCadenceData(namespace, spark)

    FetcherTestUtil.compareTemporalFetch(joinConf,
                                         "2023-08-14-12-00",
                                         namespace,
                                         consistencyCheck = false,
                                         dropDsOnWrite = true,
                                         partitionSpec = hourlyPartitionSpec,
                                         offlineAssertion = assertExpectedOfflineRows)(spark)
  }

  it should "match offline and online values for temporal daily snapshot and three hourly snapshot join parts" in {
    val namespace = "mixed_snapshot_cadence_fetcher_test"
    val joinConf = generateMixedSnapshotCadenceData(namespace, spark)

    FetcherTestUtil.compareTemporalFetch(joinConf,
                                         "2023-08-14-12-00",
                                         namespace,
                                         consistencyCheck = false,
                                         dropDsOnWrite = true,
                                         partitionSpec = hourlyPartitionSpec,
                                         offlineAssertion = assertExpectedSnapshotOfflineRows)(spark)
  }

  private def assertExpectedOfflineRows(df: DataFrame): Unit = {
    val actual = df
      .select(
        "user_id",
        "ts",
        "ds",
        "rt_user_id_realtime_value_sum",
        "hourly_user_id_hourly_value_sum",
        "daily_user_id_daily_value_sum"
      )
      .collect()
      .map { row =>
        (
          row.getAs[String]("user_id"),
          row.getAs[Long]("ts"),
          row.getAs[String]("ds"),
          row.getAs[Long]("rt_user_id_realtime_value_sum"),
          row.getAs[Long]("hourly_user_id_hourly_value_sum"),
          row.getAs[Long]("daily_user_id_daily_value_sum")
        )
      }
      .toSet

    actual shouldEqual Set(
      ("user1", ts("2023-08-14 12:00"), "2023-08-14-12-00", 7L, 30L, 100L),
      ("user2", ts("2023-08-14 12:00"), "2023-08-14-12-00", 4L, 7L, 200L)
    )
  }

  private def assertExpectedSnapshotOfflineRows(df: DataFrame): Unit = {
    val actual = df
      .select(
        "user_id",
        "ts",
        "ds",
        "temporal_user_id_temporal_value_sum",
        "daily_snapshot_user_id_daily_snapshot_value_sum",
        "three_hour_snapshot_user_id_three_hour_snapshot_value_sum_6h"
      )
      .collect()
      .map { row =>
        (
          row.getAs[String]("user_id"),
          row.getAs[Long]("ts"),
          row.getAs[String]("ds"),
          row.getAs[Long]("temporal_user_id_temporal_value_sum"),
          row.getAs[Long]("daily_snapshot_user_id_daily_snapshot_value_sum"),
          row.getAs[Long]("three_hour_snapshot_user_id_three_hour_snapshot_value_sum_6h")
        )
      }
      .toSet

    actual shouldEqual Set(
      ("user1", ts("2023-08-14 12:00"), "2023-08-14-12-00", 30L, 100L, 30L),
      ("user1", ts("2023-08-14 12:07"), "2023-08-14-12-00", 30L, 100L, 30L),
      ("user2", ts("2023-08-14 12:00"), "2023-08-14-12-00", 7L, 200L, 7L)
    )
  }

  private def generateMixedCadenceData(namespace: String, spark: SparkSession): Join = {
    SparkTestBase.createDatabase(spark, namespace)

    val leftTable = s"$namespace.left_events"
    spark
      .createDataFrame(
        Seq(
          ("user1", ts("2023-08-14 12:00"), "2023-08-14-12-00"),
          ("user2", ts("2023-08-14 12:00"), "2023-08-14-12-00")
        ))
      .toDF("user_id", "ts", "ds")
      .save(leftTable)

    val realtimeTable = s"$namespace.realtime_events"
    spark
      .createDataFrame(
        Seq(
          ("user1", 7L, ts("2023-08-14 11:40"), "2023-08-14-11-00"),
          ("user1", 5L, ts("2023-08-14 12:10"), "2023-08-14-12-00"),
          ("user2", 4L, ts("2023-08-14 11:50"), "2023-08-14-11-00")
        ))
      .toDF("user_id", "realtime_value", "ts", "ds")
      .save(realtimeTable)

    val hourlyTable = s"$namespace.hourly_events"
    spark
      .createDataFrame(
        Seq(
          ("user1", 10L, ts("2023-08-14 09:15"), "2023-08-14-09-00"),
          ("user1", 20L, ts("2023-08-14 11:30"), "2023-08-14-11-00"),
          ("user2", 3L, ts("2023-08-14 10:00"), "2023-08-14-10-00"),
          ("user2", 4L, ts("2023-08-14 11:15"), "2023-08-14-11-00")
        ))
      .toDF("user_id", "hourly_value", "ts", "ds")
      .save(hourlyTable)

    val dailyTable = s"$namespace.daily_events"
    spark
      .createDataFrame(
        Seq(
          ("user1", 100L, ts("2023-08-13 20:00"), "2023-08-13"),
          ("user1", 999L, ts("2023-08-14 13:00"), "2023-08-14"),
          ("user2", 200L, ts("2023-08-13 21:00"), "2023-08-13")
        ))
      .toDF("user_id", "daily_value", "ts", "ds")
      .save(dailyTable)

    val realtimeGroupBy = Builders.GroupBy(
      metaData = Builders.MetaData(namespace = namespace, name = "realtime_value", executionInfo = hourlyExecutionInfo),
      sources = Seq(
        Builders.Source.events(
          query = withPartition(
            Builders.Query(selects = Builders.Selects("user_id", "realtime_value", "ts")),
            hourlyPartitionSpec
          ),
          table = realtimeTable,
          topic = "realtime_topic"
        )),
      keyColumns = Seq("user_id"),
      aggregations = Seq(sum("realtime_value")),
      accuracy = Accuracy.TEMPORAL
    )

    val hourlyGroupBy = Builders.GroupBy(
      metaData = Builders.MetaData(namespace = namespace, name = "hourly_value", executionInfo = hourlyExecutionInfo),
      sources = Seq(
        Builders.Source.events(
          query = withPartition(
            Builders.Query(selects = Builders.Selects("user_id", "hourly_value", "ts")),
            hourlyPartitionSpec
          ),
          table = hourlyTable
        )),
      keyColumns = Seq("user_id"),
      aggregations = Seq(sum("hourly_value")),
      accuracy = Accuracy.TEMPORAL
    )

    val dailyGroupBy = Builders.GroupBy(
      metaData = Builders.MetaData(namespace = namespace, name = "daily_value", executionInfo = dailyExecutionInfo),
      sources = Seq(
        Builders.Source.events(
          query = Builders.Query(selects = Builders.Selects("user_id", "daily_value", "ts")),
          table = dailyTable
        )),
      keyColumns = Seq("user_id"),
      aggregations = Seq(sum("daily_value")),
      accuracy = Accuracy.TEMPORAL
    )

    Builders.Join(
      left = Builders.Source.events(
        query = withPartition(
          Builders.Query(selects = Builders.Selects("user_id", "ts"), startPartition = "2023-08-14-12-00"),
          hourlyPartitionSpec
        ),
        table = leftTable
      ),
      joinParts = Seq(
        Builders.JoinPart(groupBy = realtimeGroupBy, prefix = "rt").setUseLongNames(false),
        Builders.JoinPart(groupBy = hourlyGroupBy, prefix = "hourly").setUseLongNames(false),
        Builders.JoinPart(groupBy = dailyGroupBy, prefix = "daily").setUseLongNames(false)
      ),
      metaData =
        Builders.MetaData(namespace = namespace,
                          name = "mixed_cadence_join",
                          team = "chronon",
                          executionInfo = hourlyExecutionInfo)
    )
  }

  private def generateMixedSnapshotCadenceData(namespace: String, spark: SparkSession): Join = {
    SparkTestBase.createDatabase(spark, namespace)

    val leftTable = s"$namespace.snapshot_left_events"
    spark
      .createDataFrame(
        Seq(
          ("user1", ts("2023-08-14 12:00"), "2023-08-14-12-00"),
          // off-grid event time: ds_of_ts must floor to the hourly grid (12:00), otherwise
          // the snapshot equality join silently produces nulls for this row
          ("user1", ts("2023-08-14 12:07"), "2023-08-14-12-00"),
          ("user2", ts("2023-08-14 12:00"), "2023-08-14-12-00")
        ))
      .toDF("user_id", "ts", "ds")
      .save(leftTable)

    val temporalTable = s"$namespace.temporal_events"
    spark
      .createDataFrame(
        Seq(
          ("user1", 10L, ts("2023-08-14 09:15"), "2023-08-14-09-00"),
          ("user1", 20L, ts("2023-08-14 11:30"), "2023-08-14-11-00"),
          ("user2", 3L, ts("2023-08-14 10:00"), "2023-08-14-10-00"),
          ("user2", 4L, ts("2023-08-14 11:15"), "2023-08-14-11-00")
        ))
      .toDF("user_id", "temporal_value", "ts", "ds")
      .save(temporalTable)

    val dailySnapshotTable = s"$namespace.daily_snapshot_events"
    spark
      .createDataFrame(
        Seq(
          ("user1", 100L, ts("2023-08-13 20:00"), "2023-08-13"),
          ("user1", 999L, ts("2023-08-14 13:00"), "2023-08-14"),
          ("user2", 200L, ts("2023-08-13 21:00"), "2023-08-13")
        ))
      .toDF("user_id", "daily_snapshot_value", "ts", "ds")
      .save(dailySnapshotTable)

    val threeHourSnapshotTable = s"$namespace.three_hour_snapshot_events"
    spark
      .createDataFrame(
        Seq(
          ("user1", 10L, ts("2023-08-14 07:15"), "2023-08-14-06-00"),
          ("user1", 20L, ts("2023-08-14 10:30"), "2023-08-14-09-00"),
          ("user1", 999L, ts("2023-08-14 12:30"), "2023-08-14-12-00"),
          ("user2", 3L, ts("2023-08-14 08:00"), "2023-08-14-06-00"),
          ("user2", 4L, ts("2023-08-14 11:15"), "2023-08-14-09-00")
        ))
      .toDF("user_id", "three_hour_snapshot_value", "ts", "ds")
      .save(threeHourSnapshotTable)

    val temporalGroupBy = Builders.GroupBy(
      metaData = Builders.MetaData(namespace = namespace, name = "temporal_value", executionInfo = hourlyExecutionInfo),
      sources = Seq(
        Builders.Source.events(
          query = withPartition(
            Builders.Query(selects = Builders.Selects("user_id", "temporal_value"), timeColumn = "ts"),
            hourlyPartitionSpec
          ),
          table = temporalTable
        )),
      keyColumns = Seq("user_id"),
      aggregations = Seq(sum("temporal_value")),
      accuracy = Accuracy.TEMPORAL
    )

    val dailySnapshotGroupBy = Builders.GroupBy(
      metaData =
        Builders.MetaData(namespace = namespace, name = "daily_snapshot_value", executionInfo = dailyExecutionInfo),
      sources = Seq(
        Builders.Source.events(
          query = Builders.Query(selects = Builders.Selects("user_id", "daily_snapshot_value"), timeColumn = "ts"),
          table = dailySnapshotTable
        )),
      keyColumns = Seq("user_id"),
      aggregations = Seq(sum("daily_snapshot_value")),
      accuracy = Accuracy.SNAPSHOT
    )

    val threeHourSnapshotGroupBy = Builders.GroupBy(
      metaData = Builders.MetaData(namespace = namespace,
                                   name = "three_hour_snapshot_value",
                                   executionInfo = threeHourExecutionInfo),
      sources = Seq(
        Builders.Source.events(
          query = withPartition(
            Builders.Query(selects = Builders.Selects("user_id", "three_hour_snapshot_value"), timeColumn = "ts"),
            threeHourPartitionSpec
          ),
          table = threeHourSnapshotTable
        )),
      keyColumns = Seq("user_id"),
      aggregations = Seq(sum("three_hour_snapshot_value", windows = Seq(sixHourWindow))),
      accuracy = Accuracy.SNAPSHOT
    )

    Builders.Join(
      left = Builders.Source.events(
        query = withPartition(
          Builders.Query(selects = Builders.Selects("user_id", "ts"), startPartition = "2023-08-14-12-00"),
          hourlyPartitionSpec
        ),
        table = leftTable
      ),
      joinParts = Seq(
        Builders.JoinPart(groupBy = temporalGroupBy, prefix = "temporal").setUseLongNames(false),
        Builders.JoinPart(groupBy = dailySnapshotGroupBy, prefix = "daily_snapshot").setUseLongNames(false),
        Builders.JoinPart(groupBy = threeHourSnapshotGroupBy, prefix = "three_hour_snapshot").setUseLongNames(false)
      ),
      metaData =
        Builders.MetaData(namespace = namespace,
                          name = "mixed_snapshot_cadence_join",
                          team = "chronon",
                          executionInfo = hourlyExecutionInfo)
    )
  }

  private def sum(inputColumn: String, windows: Seq[Window] = Seq(WindowUtils.Unbounded)): Aggregation =
    Builders.Aggregation(operation = Operation.SUM, inputColumn = inputColumn, windows = windows)

  private def withPartition(query: Query, partitionSpec: PartitionSpec): Query =
    query
      .setPartitionColumn(partitionSpec.column)
      .setPartitionFormat(partitionSpec.format)
      .setPartitionInterval(partitionSpec.intervalWindow)

  private def hourlyExecutionInfo: ExecutionInfo = executionInfo(hourlyPartitionSpec)

  private def threeHourExecutionInfo: ExecutionInfo = executionInfo(threeHourPartitionSpec)

  private def dailyExecutionInfo: ExecutionInfo = executionInfo(dailyPartitionSpec)

  private def executionInfo(partitionSpec: PartitionSpec): ExecutionInfo =
    new ExecutionInfo()
      .setOutputTableInfo(
        new TableInfo()
          .setPartitionColumn(partitionSpec.column)
          .setPartitionFormat(partitionSpec.format)
          .setPartitionInterval(partitionSpec.intervalWindow)
      )

  private def ts(arg: String): Long = TsUtils.datetimeToTs(s"$arg:00")
}
