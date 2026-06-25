package ai.chronon.spark.fetcher

import ai.chronon.api._
import ai.chronon.api.Constants.MetadataDataset
import ai.chronon.api.Extensions._
import ai.chronon.api.planner.RelevantLeftForJoinPart
import ai.chronon.api.ScalaJavaConversions.IterableOps
import ai.chronon.online.fetcher.{FetchContext, MetadataStore}
import ai.chronon.online.fetcher.Fetcher.Request
import ai.chronon.spark.Extensions.DataframeOps
import ai.chronon.spark.batch.ModularMonolith
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.utils.{MockApi, OnlineUtils, SparkTestBase}
import org.apache.spark.sql.SparkSession
import org.scalatest.matchers.should.Matchers

import java.util.TimeZone
import java.util.concurrent.Executors

import scala.concurrent.duration.{Duration, SECONDS}
import scala.concurrent.{Await, ExecutionContext}

/** Online/offline join-parity matrix with hand-computed golden values.
  *
  * The matrix is accuracy/cadence x data model, hosted by a single join whose left/output partition
  * grid is 3h with a 1h offset (partitions ..., 22:00, 01:00, 04:00, ..., ds = interval start).
  * It covers the six core cells plus a second TEMPORAL/ENTITIES cell on the sub-daily grid:
  *
  * {{{
  *                          EVENTS                            ENTITIES
  *   TEMPORAL               sawtooth windows over events      daily and sub-daily mutations
  *   SNAPSHOT daily         per-row daily snapshot pick       per-row daily snapshot pick
  *   SNAPSHOT 3h offset 1h  snapshot pick by left row time    snapshot pick by left row time
  * }}}
  *
  * ==Engine snapshot convention (read from MergeJob / JoinPartJob / JoinUtils)==
  *
  * For an EVENTS left and a SNAPSHOT-accuracy join part, the snapshot pick is per row ON THE
  * RHS GROUPBY'S DECLARED GRID (its partition_interval/partition_offset; the join's grid when
  * nothing is declared):
  *  - JoinPartJob computes snapshots on the RHS grid. Same-grid parts keep the historical
  *    lookback partition naming; cross-grid part tables are partitioned like the join output
  *    and carry the RHS snapshot time in `ts`.
  *  - MergeJob stamps each left row with TimePartitionColumn = floor(left.ts, RHS grid). Same-grid
  *    right rows are renamed from ds + one RHS partitionInterval; cross-grid right rows use their
  *    persisted ts.
  *
  * Net effect: a left row at time T reads the latest RHS snapshot whose time is
  * <= T — never a future snapshot, staleness bounded by one RHS partitionInterval — INDEPENDENT
  * of the join's own grid. The daily cells under this 3h+1h join pick per row on the DAILY grid:
  * pre-midnight rows of a midnight-straddling left partition see the old daily snapshot and
  * post-midnight rows the new one (see the u3 rows below). For RHS sources that are coarser
  * than the join grid (the daily events cell), as-of correctness additionally relies on the
  * source declaring no timeColumn: the engine synthesizes ts = partition end - 1ms, so the
  * pick becomes "latest source partition COMPLETE at the row's RHS-grid floor" — exactly
  * what online whole-partition batch uploads reproduce.
  *
  * Online semantics being mirrored: SNAPSHOT parts serve the batch-upload value as-of the
  * upload's batch-end (epoch of the groupBy-grid floor of endDs); TEMPORAL parts serve
  * batch IR as-of batch-end merged with streaming rows in [batchEnd, queryTs).
  *
  * The 14d temporal window additionally locks the SawtoothMutationAggregator tail fix: its
  * daily tail hops must retain events between the tail-hop boundary (floor(batchEnd - 14d to
  * the day grid)) and the raw batchEnd - 14d instant for the NON-midnight batch ends used
  * here (10:00/13:00). The 10000 txn @ 2023-07-31 04:00 sits exactly in that gap — a
  * pre-fix upload would drop it from the batch IR while the offline sawtooth keeps it.
  */
class JoinParityMatrixTest extends SparkTestBase with Matchers {

  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

  // 3h interval, 1h offset: grid points 01:00, 04:00, 07:00, ..., 22:00 (UTC)
  private val subDailySpec =
    PartitionSpec("ds", "yyyy-MM-dd-HH-mm", 3 * WindowUtils.Hour.millis, WindowUtils.Hour.millis)
  private val dailySpec = PartitionSpec.daily

  private val sixHours = new Window(6, TimeUnit.HOURS)
  private val oneDay = new Window(1, TimeUnit.DAYS)
  private val fourteenDays = new Window(14, TimeUnit.DAYS)

  // All timestamps UTC. ts("2023-08-14 12:07") => epoch millis.
  private def ts(arg: String): Long = TsUtils.datetimeToTs(s"$arg:00")

  // ---------------------------------------------------------------------------------------------
  // Query timestamps (left rows). Left partitions are on the 3h+1h grid.
  // ---------------------------------------------------------------------------------------------
  private val T_U1_OFF_GRID = ts("2023-08-14 12:07") // off-grid: floors to 10:00
  private val T_U2_POST_STREAM = ts("2023-08-14 12:30") // after the 12:20 streaming event
  private val T_U1_ON_GRID = ts("2023-08-14 13:00") // exactly on a 3h+1h grid boundary
  private val T_U3_POST_MIDNIGHT = ts("2023-08-14 00:30") // floors across midnight to 2023-08-13 22:00
  private val T_U3_PRE_MIDNIGHT = ts("2023-08-13 23:30") // SAME left partition as above, pre-midnight
  private val T_U2_PRE_MUTATION = ts("2023-08-14 09:00") // before u2's 10:00 mutation; phase-0 online

  case class Golden(user: String, tsMillis: Long, leftDs: String, features: Map[String, Any])

  // ---------------------------------------------------------------------------------------------
  // Goldens. Derivations reference the fixture tables created in generateJoin below.
  // Engine convention: snapshot cells pick per row as of floor(left.ts, RHS grid) — the
  // 3h+1h grid for the snap3/ent3/mut3 cells, the DAILY grid for the snapd/entd/mut cells.
  // The temporal events cell is sawtooth-accurate at left.ts.
  // ---------------------------------------------------------------------------------------------
  private val goldens = Seq(
    // u1 @ 12:07 (off-grid) -> 3h+1h grid floor = 10:00, daily grid floor = 2023-08-14.
    Golden(
      "u1",
      T_U1_OFF_GRID,
      "2023-08-14-10-00",
      Map(
        // temporal events, SUM(txn_amount) over a 1d sawtooth window ending at 12:07:
        //   5 @ 08:15 + 11 @ 11:30 = 16. The 1000 @ 2023-08-13 11:00 fell out of the 1d window
        //   (~1h before the window tail), and 13 @ 12:20 belongs to u2.
        "tmp_user_id_txn_amount_sum_1d" -> 16L,
        // 14d window with DAILY hops: tail = floor(12:07 - 14d, 1d) = 2023-07-31 00:00, so the
        // 10000 @ 2023-07-31 04:00 hop-gap event IS in the window (the aggregator-fix lock):
        //   10000 + 1000 @ 08-13 11:00 + 5 @ 08:15 + 11 @ 11:30 = 11016.
        "tmp_user_id_txn_amount_sum_14d" -> 11016L,
        // daily-events snapshot, PER-ROW DAILY pick (no timeColumn => engine ts = partition
        // end - 1ms): daily floor(12:07) = 2023-08-14 -> reads snapshot as of 08-14 00:00 ->
        // partitions complete by then are ds <= 2023-08-13 -> SUM = 4. The 50 in ds=2023-08-14
        // (completes at 08-15 00:00) is excluded.
        "snapd_user_id_amount_d_sum" -> 4L,
        // 3h+1h events snapshot as-of 10:00 (real ts): 15 @ 08-13 23:40 + 20 @ 02:30 +
        // 35 @ 05:30 + 100 @ 08:30 = 170; 1000 @ 11:30 is after the 10:00 bound.
        "snap3_user_id_amount_3h_sum" -> 170L,
        // 6h window as-of 10:00 => events with ts in [04:00, 10:00): 35 @ 05:30 + 100 @ 08:30 = 135.
        "snap3_user_id_amount_3h_sum_6h" -> 135L,
        // 3h+1h entity snapshot: reads entity partition 07:00 = state as of 10:00 -> u1 balance 15.
        "ent3_user_id_balance_3h" -> 15L,
        // temporal entities (daily mutations): batch state as-of 08-14 00:00 = snapshot ds 08-13
        // (u1: 4 + 3 = 7), then 08-14 mutations with mutation_ts <= 12:07: insert 2 @ 06:00,
        // update 2 -> 9 @ 11:00 => 7 + 2 - 2 + 9 = 16.
        "mut_user_id_rating_sum" -> 16L,
        // temporal entities (3h+1h mutations): snapshot state as-of 10:00 = 9, then the
        // 10:00 bucket update 2 -> 9 at 11:00 => 9 - 2 + 9 = 16.
        "mut3_user_id_rating_sum" -> 16L,
        // daily entity snapshot, PER-ROW DAILY pick: daily floor(12:07) = 08-14 -> reads
        // entity partition 2023-08-13 -> u1 balance 102 (101 of 08-12 / 103 of 08-14 NOT bound).
        "entd_user_id_balance_d" -> 102L
      )
    ),
    // u2 @ 12:30 -> same 10:00 grid floor; temporal cells additionally see post-batch-end
    // changes that only the streaming path can deliver online.
    Golden(
      "u2",
      T_U2_POST_STREAM,
      "2023-08-14-10-00",
      Map(
        // 7 @ 09:40 (batch side) + 13 @ 12:20 (post-batch-end, streamed) = 20
        "tmp_user_id_txn_amount_sum_1d" -> 20L,
        // u2's only events in 14d: 7 @ 09:40 + 13 @ 12:20 = 20
        "tmp_user_id_txn_amount_sum_14d" -> 20L,
        // daily floor(12:30) = 08-14 -> as-of 08-14 00:00 -> ds <= 2023-08-13 -> 9
        "snapd_user_id_amount_d_sum" -> 9L,
        // as-of 10:00: 7 @ 06:10 + 70 @ 09:00 = 77
        "snap3_user_id_amount_3h_sum" -> 77L,
        // 6h window [04:00, 10:00): both events inside -> 77
        "snap3_user_id_amount_3h_sum_6h" -> 77L,
        // entity partition 07:00 -> u2 balance 25
        "ent3_user_id_balance_3h" -> 25L,
        // daily batch state 5 + insert(1) @ 10:00 <= 12:30 => 6
        "mut_user_id_rating_sum" -> 6L,
        // sub-daily snapshot state as-of 10:00 = 5, plus the 10:00 bucket insert(1) => 6
        "mut3_user_id_rating_sum" -> 6L,
        // daily pick -> entity partition 08-13 -> 202
        "entd_user_id_balance_d" -> 202L
      )
    ),
    // u1 @ exactly 13:00 (on-grid) -> floor(13:00, 3h+1h) = 13:00; daily floor = 08-14.
    Golden(
      "u1",
      T_U1_ON_GRID,
      "2023-08-14-13-00",
      Map(
        // 1d window ending 13:00: 5 @ 08:15 + 11 @ 11:30 = 16 (nothing for u1 in (12:07, 13:00])
        "tmp_user_id_txn_amount_sum_1d" -> 16L,
        // same 14d events as the 12:07 row (daily-hop tail still 2023-07-31 00:00) -> 11016
        "tmp_user_id_txn_amount_sum_14d" -> 11016L,
        // daily pick is unchanged at 13:00: as of 08-14 00:00 -> 4 (proves the daily cell
        // reads a different (coarser) partition than the 3h cell for the same left row)
        "snapd_user_id_amount_d_sum" -> 4L,
        // as-of 13:00: 170 + 1000 @ 11:30 = 1170 (the 11:30 event flips in vs. the 12:07 row)
        "snap3_user_id_amount_3h_sum" -> 1170L,
        // 6h window [07:00, 13:00): 100 @ 08:30 + 1000 @ 11:30 = 1100
        "snap3_user_id_amount_3h_sum_6h" -> 1100L,
        // entity partition 10:00 -> u1 balance 16 (differs from the 12:07 row's 15: different pick)
        "ent3_user_id_balance_3h" -> 16L,
        // daily mutations <= 13:00 are the same as <= 12:07 => 16
        "mut_user_id_rating_sum" -> 16L,
        // exact 13:00 grid boundary picks the 13:00 bucket, whose snapshot already includes
        // the 10:00 bucket mutations => 16
        "mut3_user_id_rating_sum" -> 16L,
        "entd_user_id_balance_d" -> 102L
      )
    ),
    // u3 @ 00:30 just after midnight -> 3h+1h grid floor crosses the day boundary to
    // 2023-08-13 22:00, but the DAILY grid floor is 2023-08-14: the daily cells read the
    // NEW day's snapshots while the 3h cells stay as-of 22:00 of 08-13.
    Golden(
      "u3",
      T_U3_POST_MIDNIGHT,
      "2023-08-13-22-00",
      Map(
        // 1d window ending 00:30: 9 @ 08-13 21:00 + 17 @ 08-14 00:10 = 26 (the 00:10 event lives
        // in source partition "2023-08-13-22-00", which straddles midnight)
        "tmp_user_id_txn_amount_sum_1d" -> 26L,
        // u3's only events in 14d -> 26
        "tmp_user_id_txn_amount_sum_14d" -> 26L,
        // PER-ROW DAILY pick: daily floor(08-14 00:30) = 2023-08-14 -> as of 08-14 00:00 ->
        // ds <= 2023-08-13 -> 6 + 60 = 66. (Under the old join-grid flooring this row bound
        // as-of 22:00 and saw only 6 — the daily partition 08-13 was not complete yet. The
        // per-row daily pick is what online serving with a daily upload reproduces.)
        "snapd_user_id_amount_d_sum" -> 66L,
        // as-of 2023-08-13 22:00: 3 @ 19:30; 40 @ 23:00 is after the bound
        "snap3_user_id_amount_3h_sum" -> 3L,
        // 6h window [16:00, 22:00) on 08-13: 3 @ 19:30
        "snap3_user_id_amount_3h_sum_6h" -> 3L,
        // entity partition 2023-08-13 19:00 -> u3 balance 31
        "ent3_user_id_balance_3h" -> 31L,
        // daily batch state as-of 08-14 00:00 = snapshot ds 08-13 -> u3 rating 8; no u3 mutations
        "mut_user_id_rating_sum" -> 8L,
        // sub-daily mutation grid floor 22:00 -> snapshot as-of 22:00 -> u3 rating 8
        "mut3_user_id_rating_sum" -> 8L,
        // daily pick -> entity partition 08-13 -> u3 balance 302 (the NEW daily snapshot)
        "entd_user_id_balance_d" -> 302L
      )
    ),
    // u3 @ 23:30, the PRE-midnight row of the SAME left partition "2023-08-13-22-00": the
    // daily cells must read the OLD day's snapshots — one left partition, two daily picks.
    // Offline-golden-only: a pinned batch upload cannot represent a query before its batch-end.
    Golden(
      "u3",
      T_U3_PRE_MIDNIGHT,
      "2023-08-13-22-00",
      Map(
        // 1d window ending 08-13 23:30: only 9 @ 21:00 (the 17 @ 08-14 00:10 is in the future)
        "tmp_user_id_txn_amount_sum_1d" -> 9L,
        "tmp_user_id_txn_amount_sum_14d" -> 9L,
        // daily floor(08-13 23:30) = 2023-08-13 -> as-of 08-13 00:00 -> ds <= 2023-08-12 -> 6.
        // The post-midnight row of this same left partition sees 66: per-row split.
        "snapd_user_id_amount_d_sum" -> 6L,
        // floor(23:30, 3h+1h) = 22:00 -> same as-of bound as the 00:30 row -> 3
        "snap3_user_id_amount_3h_sum" -> 3L,
        "snap3_user_id_amount_3h_sum_6h" -> 3L,
        "ent3_user_id_balance_3h" -> 31L,
        // ds_of_ts = 08-13 needs the 08-12 daily snapshot partition, which does not exist -> null
        "mut_user_id_rating_sum" -> null,
        // same sub-daily 22:00 bucket as the post-midnight row -> snapshot as-of 22:00 -> 8
        "mut3_user_id_rating_sum" -> 8L,
        // daily pick -> entity partition 08-12 -> u3 balance 301 (the OLD daily snapshot;
        // the 00:30 row of the same left partition reads 302)
        "entd_user_id_balance_d" -> 301L
      )
    ),
    // u2 @ 09:00, before u2's 10:00 mutation: pre-mutation state online via the phase-0 serve
    // (batch end 07:00).
    Golden(
      "u2",
      T_U2_PRE_MUTATION,
      "2023-08-14-07-00",
      Map(
        // no u2 txn events before 09:00 (7 @ 09:40 is later) -> null
        "tmp_user_id_txn_amount_sum_1d" -> null,
        "tmp_user_id_txn_amount_sum_14d" -> null,
        // daily floor(09:00) = 08-14 -> as-of 00:00 -> 9
        "snapd_user_id_amount_d_sum" -> 9L,
        // as-of floor(09:00, 3h+1h) = 07:00: 7 @ 06:10 (70 @ 09:00 is at the bound, excluded)
        "snap3_user_id_amount_3h_sum" -> 7L,
        // 6h window [01:00, 07:00): 7 @ 06:10
        "snap3_user_id_amount_3h_sum_6h" -> 7L,
        // entity partition 04:00 -> u2 balance 24
        "ent3_user_id_balance_3h" -> 24L,
        // u2's insert happens at 10:00 > 09:00 -> still daily batch state 5
        "mut_user_id_rating_sum" -> 5L,
        // u2's insert happens at the 10:00 boundary, after the 07:00 bucket queried by 09:00
        "mut3_user_id_rating_sum" -> 5L,
        "entd_user_id_balance_d" -> 202L
      )
    )
  )

  it should "match goldens offline and online for all matrix cells on one sub-daily join" in {
    val namespace = "join_parity_matrix"
    val joinConf = generateJoin(namespace, spark)
    implicit val tableUtils: TableUtils = TableUtils(spark, subDailySpec)

    // Offline backfill over the full left partition range (22:00 of 08-13 through 13:00 of 08-14;
    // the in-between grid partitions have no left rows and are skipped by the source job).
    val dateRange = new DateRange().setStartDate("2023-08-13-22-00").setEndDate("2023-08-14-13-00")
    ModularMonolith.run(joinConf, dateRange)
    assertDailySnapshotPartHasOneDsPerJoinPartition(joinConf)

    val offlineRows = assertOfflineMatchesGoldens(joinConf, goldens)

    // Online phase 0: batch-end pinned at the 07:00 boundary; the only servable golden row is
    // u2 @ 09:00 (inside [07:00, 10:00)) - the pre-mutation assertion.
    val phase0Rows = goldens.filter(g => g.leftDs == "2023-08-14-07-00")
    serveAndAssertOnline(joinConf, "2023-08-14-07-00", subDailySpec, namespace, "p0", phase0Rows, offlineRows)

    // Online phase 1: pin batch-end at the 10:00 grid boundary (endDs partition
    // "2023-08-14-10-00"; each groupBy uploads its own grid floor of that boundary: the 3h+1h
    // groupBys batch-end at 10:00, the daily groupBys at 2023-08-14 00:00). Events at/after
    // each groupBy's batch-end reach the temporal cells only through the streaming path. Only
    // left rows inside [10:00, 13:00) are servable from this upload.
    val phase1Rows = goldens.filter(g => g.leftDs == "2023-08-14-10-00")
    serveAndAssertOnline(joinConf, "2023-08-14-10-00", subDailySpec, namespace, "p1", phase1Rows, offlineRows)

    // Online phase 2: re-serve with batch-end pinned at 13:00 (fresh KV store and re-uploaded
    // batch data) and fetch the row that sits exactly on the 13:00 grid boundary. A query at
    // exactly batch-end is legal (batchEndTs > queryTs is the rejection condition) and must see
    // the [10:00, 13:00) snapshot partition.
    // The u3 rows are offline-golden-only: a single pinned batch upload cannot represent a
    // query time before its batch-end (the temporal path rejects queryTs < batchEnd, and
    // snapshot serving cannot time-travel), so there is no meaningful online assertion there.
    val phase2Rows = goldens.filter(g => g.leftDs == "2023-08-14-13-00")
    serveAndAssertOnline(joinConf, "2023-08-14-13-00", subDailySpec, namespace, "p2", phase2Rows, offlineRows)
  }

  it should "keep cross-grid snapshot fan-out scoped across incremental straddling-partition runs" in {
    val namespace = "join_parity_incremental"
    val joinConf = generateJoin(namespace, spark)
    implicit val tableUtils: TableUtils = TableUtils(spark, subDailySpec)

    spark
      .createDataFrame(Seq(("u3", ts("2023-08-13 21:30"), "2023-08-13-19-00")))
      .toDF("user_id", "ts", "ds")
      .save(s"$namespace.left_events")

    def runOne(partition: String): Unit =
      ModularMonolith.run(joinConf, new DateRange().setStartDate(partition).setEndDate(partition))

    val straddlingPartition = "2023-08-13-22-00"
    val dailySnapshotPartTable = joinPartTable(joinConf, "parity_daily_amount")
    val dailyEntityPartTable = joinPartTable(joinConf, "parity_daily_balance")

    runOne("2023-08-13-19-00")

    withClue("prior partition run must not prefill the midnight-straddling daily events part: ") {
      tableUtils.partitions(dailySnapshotPartTable).toSet should not contain straddlingPartition
    }
    withClue("prior partition run must not prefill the midnight-straddling daily entities part: ") {
      tableUtils.partitions(dailyEntityPartTable).toSet should not contain straddlingPartition
    }

    runOne(straddlingPartition)

    val expectedAsOfs = Set(ts("2023-08-13 00:00"), ts("2023-08-14 00:00"))
    dailyPartAsOfs(dailySnapshotPartTable, straddlingPartition) shouldEqual expectedAsOfs
    dailyPartAsOfs(dailyEntityPartTable, straddlingPartition) shouldEqual expectedAsOfs

    val rowsByTs = tableUtils
      .sql(s"""
              |SELECT user_id, ts, snapd_user_id_amount_d_sum, entd_user_id_balance_d, ds
              |FROM ${joinConf.metaData.outputTable}
              |WHERE ds = '$straddlingPartition' AND user_id = 'u3'
              |""".stripMargin)
      .collect()
      .map { row =>
        row.getAs[Long]("ts") ->
          (row.getAs[Any]("snapd_user_id_amount_d_sum"), row.getAs[Any]("entd_user_id_balance_d"))
      }
      .toMap

    rowsByTs shouldEqual Map(
      T_U3_PRE_MIDNIGHT -> (6L, 301L),
      T_U3_POST_MIDNIGHT -> (66L, 302L)
    )

    runOne("2023-08-13-19-00")

    withClue("targeted rerun of the prior partition must not overwrite the straddling partition: ") {
      dailyPartAsOfs(dailySnapshotPartTable, straddlingPartition) shouldEqual expectedAsOfs
      dailyPartAsOfs(dailyEntityPartTable, straddlingPartition) shouldEqual expectedAsOfs
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Assertion harness
  // ---------------------------------------------------------------------------------------------

  private def joinPartTable(joinConf: Join, groupByName: String): String = {
    val joinPart = joinConf.joinParts.toScala
      .find(_.groupBy.metaData.name == groupByName)
      .get
    RelevantLeftForJoinPart.fullPartTableName(joinConf, joinPart)
  }

  private def dailyPartAsOfs(partTable: String, partition: String)(implicit tableUtils: TableUtils): Set[Long] =
    tableUtils
      .sql(s"SELECT ts FROM $partTable WHERE ds = '$partition' AND user_id = 'u3'")
      .collect()
      .map(_.getAs[Long]("ts"))
      .toSet

  private def assertDailySnapshotPartHasOneDsPerJoinPartition(joinConf: Join)(implicit
      tableUtils: TableUtils): Unit = {
    val partTable = joinPartTable(joinConf, "parity_daily_amount")
    val partitions = tableUtils.partitions(partTable).toSet

    Seq(
      "2023-08-13-22-00",
      "2023-08-14-01-00",
      "2023-08-14-04-00",
      "2023-08-14-07-00",
      "2023-08-14-10-00",
      "2023-08-14-13-00"
    ).foreach { expectedPartition =>
      withClue(s"daily snapshot join-part should have one ds per join partition for $expectedPartition: ") {
        partitions should contain(expectedPartition)
      }
    }
  }

  /** Asserts the offline join output equals the goldens row by row (exact values, exact left
    * partition ds values, no extra rows) and returns the offline rows keyed by (user, ts).
    */
  private def assertOfflineMatchesGoldens(joinConf: Join, goldens: Seq[Golden])(implicit
      tableUtils: TableUtils): Map[(String, Long), Map[String, Any]] = {
    val outputDf = tableUtils.sql(s"SELECT * FROM ${joinConf.metaData.outputTable}")
    outputDf.show(truncate = false)
    val featureColumns = goldens.flatMap(_.features.keys).distinct

    val collectedRows = outputDf.collect()
    val rowKeys = collectedRows.map(row => (row.getAs[String]("user_id"), row.getAs[Long]("ts")))
    withClue(s"offline output of ${joinConf.metaData.outputTable} should not duplicate golden rows: ") {
      collectedRows.length shouldEqual goldens.length
      rowKeys.groupBy(identity).map { case (key, rows) => key -> rows.length } shouldEqual rowKeys.map(_ -> 1).toMap
    }

    val offlineRows: Map[(String, Long), Map[String, Any]] = collectedRows
      .map { row =>
        val key = (row.getAs[String]("user_id"), row.getAs[Long]("ts"))
        val values: Map[String, Any] =
          (featureColumns.map(c => c -> row.getAs[Any](c)) :+ ("ds" -> row.getAs[String]("ds"))).toMap
        key -> values
      }
      .toMap

    withClue(s"offline output of ${joinConf.metaData.outputTable} should have exactly one row per golden row: ") {
      offlineRows.keySet shouldEqual goldens.map(g => (g.user, g.tsMillis)).toSet
    }

    goldens.foreach { golden =>
      val actual = offlineRows((golden.user, golden.tsMillis))
      withClue(s"offline left partition ds for ${golden.user} @ ${TsUtils.toStr(golden.tsMillis)}: ") {
        actual("ds") shouldEqual golden.leftDs
      }
      golden.features.foreach { case (feature, expected) =>
        withClue(s"offline $feature for ${golden.user} @ ${TsUtils.toStr(golden.tsMillis)}: ") {
          actual(feature) shouldEqual expected
        }
      }
    }
    offlineRows
  }

  /** Uploads every joinPart groupBy (batch + streaming where applicable) into a fresh in-memory
    * KV store for the given endDs, fetches the join at the goldens' query timestamps and asserts
    * the fetched values equal both the goldens and the offline rows.
    */
  private def serveAndAssertOnline(joinConf: Join,
                                   endDs: String,
                                   joinSpec: PartitionSpec,
                                   namespace: String,
                                   phaseId: String,
                                   goldens: Seq[Golden],
                                   offlineRows: Map[(String, Long), Map[String, Any]]): Unit = {
    implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1))
    val kvStoreFunc = () => OnlineUtils.buildInMemoryKVStore(s"JoinParityMatrixTest_$phaseId")
    val inMemoryKvStore = kvStoreFunc()
    val mockApi = new MockApi(kvStoreFunc, namespace)

    joinConf.joinParts.toScala.foreach { jp =>
      val groupBySpec = jp.groupBy.partitionSpec(joinSpec)
      val groupByEndDs = joinSpec.translate(endDs, groupBySpec)
      // A previous serve phase leaves its batch partition in the upload table; bulkPut loads
      // every partition of that table, so stale uploads must be dropped before re-serving.
      spark.sql(s"DROP TABLE IF EXISTS ${jp.groupBy.metaData.uploadTable}")
      val groupByTableUtils = TableUtils(spark, groupBySpec)
      OnlineUtils.serve(groupByTableUtils,
                        inMemoryKvStore,
                        kvStoreFunc,
                        namespace,
                        groupByEndDs,
                        jp.groupBy,
                        dropDsOnWrite = true)
    }

    inMemoryKvStore.create(MetadataDataset)
    new MetadataStore(FetchContext(inMemoryKvStore)).putJoinConf(joinConf)

    val fetcher = mockApi.buildFetcher(debug = true)
    val requests = goldens.map { g =>
      Request(joinConf.metaData.name, Map("user_id" -> (g.user: AnyRef)), Some(g.tsMillis))
    }
    val responses = Await.result(fetcher.fetchJoin(requests), Duration(100, SECONDS))

    responses.zip(goldens).foreach { case (response, golden) =>
      val fetched = response.values.get
      golden.features.foreach { case (feature, expected) =>
        withClue(s"online[$phaseId] $feature for ${golden.user} @ ${TsUtils.toStr(golden.tsMillis)} " +
          s"(batch served at endDs=$endDs), full response: $fetched : ") {
          fetched.get(feature).orNull shouldEqual expected
          // ... and equal to the offline backfill row for the same (key, ts)
          fetched.get(feature).orNull shouldEqual offlineRows((golden.user, golden.tsMillis))(feature)
        }
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Join fixture: one sub-daily join hosting all matrix cells
  // ---------------------------------------------------------------------------------------------
  private def generateJoin(namespace: String, spark: SparkSession): Join = {
    SparkTestBase.createDatabase(spark, namespace)

    // Left events: deliberately awkward timestamps (see goldens above for the pick each
    // row exercises). ds values are the 3h+1h grid floor of ts.
    val leftTable = s"$namespace.left_events"
    spark
      .createDataFrame(
        Seq(
          ("u1", T_U1_OFF_GRID, "2023-08-14-10-00"),
          ("u2", T_U2_POST_STREAM, "2023-08-14-10-00"),
          ("u1", T_U1_ON_GRID, "2023-08-14-13-00"),
          ("u3", T_U3_POST_MIDNIGHT, "2023-08-13-22-00"),
          ("u3", T_U3_PRE_MIDNIGHT, "2023-08-13-22-00"), // same partition, pre-midnight
          ("u2", T_U2_PRE_MUTATION, "2023-08-14-07-00")
        ))
      .toDF("user_id", "ts", "ds")
      .save(leftTable)

    // TEMPORAL/EVENTS cell source, partitioned on the 3h+1h grid. The 11:30/12:20/13:40 events
    // sit at/after the phase-1 batch-end (10:00) and reach the fetcher via streaming injection.
    // The 10000 @ 2023-07-31 04:00 event is the aggregator-fix lock: for a 10:00/13:00 batch
    // end its 14d daily-hop tail is 2023-07-31 00:00 while the raw batchEnd - 14d tail is
    // 10:00/13:00 of 07-31 - a pre-fix tailTs dropped events in that gap from the batch IR.
    val txnTable = s"$namespace.txn_events"
    spark
      .createDataFrame(
        Seq(
          ("u1", 10000L, ts("2023-07-31 04:00"), "2023-07-31-04-00"), // hop-gap event (see above)
          ("u1", 1000L, ts("2023-08-13 11:00"), "2023-08-13-10-00"), // outside every 1d query window
          ("u3", 9L, ts("2023-08-13 21:00"), "2023-08-13-19-00"),
          ("u3", 17L, ts("2023-08-14 00:10"), "2023-08-13-22-00"), // partition ds straddles midnight
          ("u1", 5L, ts("2023-08-14 08:15"), "2023-08-14-07-00"),
          ("u2", 7L, ts("2023-08-14 09:40"), "2023-08-14-07-00"),
          ("u1", 11L, ts("2023-08-14 11:30"), "2023-08-14-10-00"), // post-batch-end: streaming only
          ("u2", 13L, ts("2023-08-14 12:20"), "2023-08-14-10-00"), // post-batch-end: streaming only
          // u9 is never queried; it keeps the phase-2 streaming injection (ds >= 13:00) non-empty
          ("u9", 999L, ts("2023-08-14 13:40"), "2023-08-14-13-00")
        ))
      .toDF("user_id", "txn_amount", "ts", "ds")
      .save(txnTable)

    // SNAPSHOT-daily/EVENTS cell source: a classic daily event table WITHOUT a ts column. The
    // engine synthesizes ts = partition end - 1ms, which makes the offline pick "latest daily
    // partition complete at the left row's DAILY floor" — identical to online whole-partition
    // batch serving (see class doc).
    val dailyEventsTable = s"$namespace.daily_events"
    spark
      .createDataFrame(
        Seq(
          ("u3", 6L, "2023-08-12"),
          ("u1", 4L, "2023-08-13"),
          ("u2", 9L, "2023-08-13"),
          ("u3", 60L, "2023-08-13"), // included for u3 @ 00:30 (daily floor 08-14), not @ 23:30
          ("u1", 50L, "2023-08-14"), // never bound: would require a row at/after 08-15 00:00
          ("u2", 90L, "2023-08-14")
        ))
      .toDF("user_id", "amount_d", "ds")
      .save(dailyEventsTable)

    // SNAPSHOT-3h+1h/EVENTS cell source: events partitioned on the offset grid with real
    // timestamps (timeColumn = ts), so snapshot values are exact as-of the 3h+1h boundaries.
    val offsetEventsTable = s"$namespace.offset_grid_events"
    spark
      .createDataFrame(
        Seq(
          ("u3", 3L, ts("2023-08-13 19:30"), "2023-08-13-19-00"),
          ("u1", 15L, ts("2023-08-13 23:40"), "2023-08-13-22-00"),
          ("u3", 40L, ts("2023-08-13 23:00"), "2023-08-13-22-00"), // after u3's 22:00 bound
          ("u1", 20L, ts("2023-08-14 02:30"), "2023-08-14-01-00"),
          ("u1", 35L, ts("2023-08-14 05:30"), "2023-08-14-04-00"),
          ("u2", 7L, ts("2023-08-14 06:10"), "2023-08-14-04-00"),
          ("u1", 100L, ts("2023-08-14 08:30"), "2023-08-14-07-00"),
          ("u2", 70L, ts("2023-08-14 09:00"), "2023-08-14-07-00"),
          ("u1", 1000L, ts("2023-08-14 11:30"), "2023-08-14-10-00") // visible only to the 13:00 row
        ))
      .toDF("user_id", "amount_3h", "ts", "ds")
      .save(offsetEventsTable)

    // SNAPSHOT-3h+1h/ENTITIES cell source: full entity state per 3h+1h partition; partition P
    // holds the state for [P, P+3h), i.e. the state as-of P+3h once complete. Balances encode the
    // partition (u1: 1x, u2: 2x, u3: 3x) so a wrong pick is unambiguous.
    val offsetBalanceTable = s"$namespace.offset_grid_balance"
    spark
      .createDataFrame(
        Seq(
          ("u1", 11L, "2023-08-13-19-00"),
          ("u2", 21L, "2023-08-13-19-00"),
          ("u3", 31L, "2023-08-13-19-00"),
          ("u1", 12L, "2023-08-13-22-00"),
          ("u2", 22L, "2023-08-13-22-00"),
          ("u3", 32L, "2023-08-13-22-00"),
          ("u1", 13L, "2023-08-14-01-00"),
          ("u2", 23L, "2023-08-14-01-00"),
          ("u3", 33L, "2023-08-14-01-00"),
          ("u1", 14L, "2023-08-14-04-00"),
          ("u2", 24L, "2023-08-14-04-00"),
          ("u3", 34L, "2023-08-14-04-00"),
          ("u1", 15L, "2023-08-14-07-00"),
          ("u2", 25L, "2023-08-14-07-00"),
          ("u3", 35L, "2023-08-14-07-00"),
          ("u1", 16L, "2023-08-14-10-00"),
          ("u2", 26L, "2023-08-14-10-00"),
          ("u3", 36L, "2023-08-14-10-00")
        ))
      .toDF("user_id", "balance_3h", "ds")
      .save(offsetBalanceTable)

    // TEMPORAL/ENTITIES daily cell: snapshot partition ds holds the state as of end-of-ds; the
    // 08-13 partition is the batch state at 08-14 00:00 (u1: 4 + 3 = 7, u2: 5, u3: 8).
    val ratingsSnapshotTable = s"$namespace.ratings_snapshot"
    spark
      .createDataFrame(
        Seq(
          ("u1", ts("2023-08-12 10:00"), 4L, "2023-08-13"),
          ("u1", ts("2023-08-13 09:00"), 3L, "2023-08-13"),
          ("u2", ts("2023-08-13 11:00"), 5L, "2023-08-13"),
          ("u3", ts("2023-08-13 08:00"), 8L, "2023-08-13")
        ))
      .toDF("user_id", "ts", "rating", "ds")
      .save(ratingsSnapshotTable)

    // Daily mutations of 2023-08-14:
    //  - u1 inserts rating 2 at 06:00 (single is_before=false row),
    //  - u1 updates that rating 2 -> 9 at 11:00 (is_before=true reversal + is_before=false new),
    //  - u2 inserts rating 1 at 10:00.
    val ratingsMutationsTable = s"$namespace.ratings_mutations"
    spark
      .createDataFrame(
        Seq(
          ("u1", ts("2023-08-14 06:00"), 2L, "2023-08-14", ts("2023-08-14 06:00"), false),
          ("u1", ts("2023-08-14 06:00"), 2L, "2023-08-14", ts("2023-08-14 11:00"), true),
          ("u1", ts("2023-08-14 06:00"), 9L, "2023-08-14", ts("2023-08-14 11:00"), false),
          ("u2", ts("2023-08-14 10:00"), 1L, "2023-08-14", ts("2023-08-14 10:00"), false)
        ))
      .toDF("user_id", "ts", "rating", "ds", "mutation_ts", "is_before")
      .save(ratingsMutationsTable)

    // TEMPORAL/ENTITIES sub-daily cell: same mutations, but snapshots and mutation partitions
    // live on the 3h+1h grid.
    val offsetRatingsSnapshotTable = s"$namespace.ratings_snapshot_3h"
    spark
      .createDataFrame(
        Seq(
          ("u3", ts("2023-08-13 08:00"), 8L, "2023-08-13-19-00"),
          ("u2", ts("2023-08-13 11:00"), 5L, "2023-08-14-04-00"),
          ("u1", ts("2023-08-12 10:00"), 4L, "2023-08-14-07-00"),
          ("u1", ts("2023-08-13 09:00"), 3L, "2023-08-14-07-00"),
          ("u1", ts("2023-08-14 06:00"), 2L, "2023-08-14-07-00"),
          ("u2", ts("2023-08-13 11:00"), 5L, "2023-08-14-07-00"),
          ("u1", ts("2023-08-12 10:00"), 4L, "2023-08-14-10-00"),
          ("u1", ts("2023-08-13 09:00"), 3L, "2023-08-14-10-00"),
          ("u1", ts("2023-08-14 06:00"), 9L, "2023-08-14-10-00")
        ))
      .toDF("user_id", "ts", "rating", "ds")
      .save(offsetRatingsSnapshotTable)

    val offsetRatingsMutationsTable = s"$namespace.ratings_mutations_3h"
    spark
      .createDataFrame(
        Seq(
          ("u1", ts("2023-08-14 06:00"), 2L, "2023-08-14-04-00", ts("2023-08-14 06:00"), false),
          ("u1", ts("2023-08-14 06:00"), 2L, "2023-08-14-10-00", ts("2023-08-14 11:00"), true),
          ("u1", ts("2023-08-14 06:00"), 9L, "2023-08-14-10-00", ts("2023-08-14 11:00"), false),
          ("u2", ts("2023-08-14 10:00"), 1L, "2023-08-14-10-00", ts("2023-08-14 10:00"), false)
        ))
      .toDF("user_id", "ts", "rating", "ds", "mutation_ts", "is_before")
      .save(offsetRatingsMutationsTable)

    // SNAPSHOT-daily/ENTITIES cell: balances encode the partition (u1: 10x, u2: 20x, u3: 30x).
    // Per-row daily pick: a row at time T reads the partition of day(T) - 1.
    val dailyBalanceTable = s"$namespace.daily_balance"
    spark
      .createDataFrame(
        Seq(
          ("u1", 101L, "2023-08-12"),
          ("u2", 201L, "2023-08-12"),
          ("u3", 301L, "2023-08-12"), // bound by u3 @ 08-13 23:30 (daily floor 08-13)
          ("u1", 102L, "2023-08-13"),
          ("u2", 202L, "2023-08-13"),
          ("u3", 302L, "2023-08-13"), // bound by u3 @ 08-14 00:30 (daily floor 08-14)
          ("u1", 103L, "2023-08-14"),
          ("u2", 203L, "2023-08-14"),
          ("u3", 303L, "2023-08-14")
        ))
      .toDF("user_id", "balance_d", "ds")
      .save(dailyBalanceTable)

    val temporalEventsGroupBy = Builders.GroupBy(
      metaData =
        Builders.MetaData(namespace = namespace, name = "parity_txn_sum", executionInfo = executionInfo(subDailySpec)),
      sources = Seq(
        Builders.Source.events(
          query = withPartition(
            Builders.Query(selects = Builders.Selects("user_id", "txn_amount"),
                           timeColumn = "ts",
                           startPartition = "2023-07-31-04-00"),
            subDailySpec
          ),
          table = txnTable,
          topic = "parity_txn_topic"
        )),
      keyColumns = Seq("user_id"),
      aggregations = Seq(
        Builders
          .Aggregation(operation = Operation.SUM, inputColumn = "txn_amount", windows = Seq(oneDay, fourteenDays))),
      accuracy = Accuracy.TEMPORAL
    )

    val dailySnapshotEventsGroupBy = Builders.GroupBy(
      metaData = Builders.MetaData(namespace = namespace,
                                   name = "parity_daily_amount",
                                   executionInfo = executionInfo(dailySpec)),
      sources = Seq(
        Builders.Source.events(
          // no timeColumn: classic snapshot-style daily event table (see class doc)
          query = withPartition(
            Builders.Query(selects = Builders.Selects("user_id", "amount_d"), startPartition = "2023-08-12"),
            dailySpec
          ),
          table = dailyEventsTable
        )),
      keyColumns = Seq("user_id"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.SUM,
                             inputColumn = "amount_d",
                             windows = Seq(WindowUtils.Unbounded))),
      accuracy = Accuracy.SNAPSHOT
    )

    val offsetSnapshotEventsGroupBy = Builders.GroupBy(
      metaData = Builders.MetaData(namespace = namespace,
                                   name = "parity_offset_amount",
                                   executionInfo = executionInfo(subDailySpec)),
      sources = Seq(
        Builders.Source.events(
          query = withPartition(
            Builders.Query(selects = Builders.Selects("user_id", "amount_3h"),
                           timeColumn = "ts",
                           startPartition = "2023-08-13-19-00"),
            subDailySpec
          ),
          table = offsetEventsTable
        )),
      keyColumns = Seq("user_id"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.SUM,
                             inputColumn = "amount_3h",
                             windows = Seq(WindowUtils.Unbounded, sixHours))),
      accuracy = Accuracy.SNAPSHOT
    )

    val offsetSnapshotEntitiesGroupBy = Builders.GroupBy(
      metaData = Builders.MetaData(namespace = namespace,
                                   name = "parity_offset_balance",
                                   executionInfo = executionInfo(subDailySpec)),
      sources = Seq(
        Builders.Source.entities(
          query = withPartition(
            Builders.Query(selects = Builders.Selects("user_id", "balance_3h"), startPartition = "2023-08-13-19-00"),
            subDailySpec
          ),
          snapshotTable = offsetBalanceTable
        )),
      keyColumns = Seq("user_id")
      // no aggregations: pass-through latest state per partition (inferred SNAPSHOT accuracy)
    )

    val mutationsGroupBy = Builders.GroupBy(
      metaData = Builders.MetaData(namespace = namespace,
                                   name = "parity_ratings_sum",
                                   executionInfo = executionInfo(dailySpec)),
      sources = Seq(
        Builders.Source.entities(
          query = withPartition(
            Builders.Query(
              selects = Map("user_id" -> "user_id", "ts" -> "ts", "rating" -> "rating"),
              startPartition = "2023-08-12",
              mutationTimeColumn = "mutation_ts",
              reversalColumn = "is_before"
            ),
            dailySpec
          ),
          snapshotTable = ratingsSnapshotTable,
          mutationTable = ratingsMutationsTable,
          mutationTopic = "parity_mutations_topic"
        )),
      keyColumns = Seq("user_id"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.SUM, inputColumn = "rating", windows = Seq(WindowUtils.Unbounded))),
      accuracy = Accuracy.TEMPORAL
    )

    val offsetMutationsGroupBy = Builders.GroupBy(
      metaData = Builders.MetaData(namespace = namespace,
                                   name = "parity_offset_ratings_sum",
                                   executionInfo = executionInfo(subDailySpec)),
      sources = Seq(
        Builders.Source.entities(
          query = withPartition(
            Builders.Query(
              selects = Map("user_id" -> "user_id", "ts" -> "ts", "rating" -> "rating"),
              startPartition = "2023-08-13-19-00",
              mutationTimeColumn = "mutation_ts",
              reversalColumn = "is_before"
            ),
            subDailySpec
          ),
          snapshotTable = offsetRatingsSnapshotTable,
          mutationTable = offsetRatingsMutationsTable,
          mutationTopic = "parity_offset_mutations_topic"
        )),
      keyColumns = Seq("user_id"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.SUM, inputColumn = "rating", windows = Seq(WindowUtils.Unbounded))),
      accuracy = Accuracy.TEMPORAL
    )

    val dailySnapshotEntitiesGroupBy = Builders.GroupBy(
      metaData = Builders.MetaData(namespace = namespace,
                                   name = "parity_daily_balance",
                                   executionInfo = executionInfo(dailySpec)),
      sources = Seq(
        Builders.Source.entities(
          query = withPartition(
            Builders.Query(selects = Builders.Selects("user_id", "balance_d"), startPartition = "2023-08-12"),
            dailySpec
          ),
          snapshotTable = dailyBalanceTable
        )),
      keyColumns = Seq("user_id")
      // no aggregations: pass-through latest state per partition (inferred SNAPSHOT accuracy)
    )

    Builders.Join(
      left = Builders.Source.events(
        query = withPartition(
          Builders.Query(selects = Builders.Selects("user_id", "ts"), startPartition = "2023-08-13-22-00"),
          subDailySpec
        ),
        table = leftTable
      ),
      joinParts = Seq(
        Builders.JoinPart(groupBy = temporalEventsGroupBy, prefix = "tmp").setUseLongNames(false),
        Builders.JoinPart(groupBy = dailySnapshotEventsGroupBy, prefix = "snapd").setUseLongNames(false),
        Builders.JoinPart(groupBy = offsetSnapshotEventsGroupBy, prefix = "snap3").setUseLongNames(false),
        Builders.JoinPart(groupBy = offsetSnapshotEntitiesGroupBy, prefix = "ent3").setUseLongNames(false),
        Builders.JoinPart(groupBy = mutationsGroupBy, prefix = "mut").setUseLongNames(false),
        Builders.JoinPart(groupBy = offsetMutationsGroupBy, prefix = "mut3").setUseLongNames(false),
        Builders.JoinPart(groupBy = dailySnapshotEntitiesGroupBy, prefix = "entd").setUseLongNames(false)
      ),
      metaData = Builders.MetaData(namespace = namespace,
                                   name = "parity_matrix_join",
                                   team = "chronon",
                                   executionInfo = executionInfo(subDailySpec))
    )
  }

  // ---------------------------------------------------------------------------------------------
  // Builders helpers
  // ---------------------------------------------------------------------------------------------

  // Declares the partition grid (column/format/interval/offset) on a source query. The offset is
  // always set explicitly: an unset query offset falls back to the JOIN's offset, which would
  // silently put e.g. a daily source onto the 1h-offset grid.
  private def withPartition(query: Query, partitionSpec: PartitionSpec): Query =
    query
      .setPartitionColumn(partitionSpec.column)
      .setPartitionFormat(partitionSpec.format)
      .setPartitionInterval(partitionSpec.intervalWindow)
      .setPartitionOffset(offsetWindow(partitionSpec))

  private def executionInfo(partitionSpec: PartitionSpec): ExecutionInfo =
    new ExecutionInfo()
      .setOutputTableInfo(
        new TableInfo()
          .setPartitionColumn(partitionSpec.column)
          .setPartitionFormat(partitionSpec.format)
          .setPartitionInterval(partitionSpec.intervalWindow)
          .setPartitionOffset(offsetWindow(partitionSpec))
      )

  private def offsetWindow(partitionSpec: PartitionSpec): Window =
    new Window((partitionSpec.offsetMillis / WindowUtils.Hour.millis).toInt, TimeUnit.HOURS)
}
