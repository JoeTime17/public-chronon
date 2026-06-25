package ai.chronon.spark.stats.drift

import ai.chronon
import ai.chronon.api.ColorPrinter.ColorString
import ai.chronon.api.Extensions.{MetadataOps, WindowOps}
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.api.{Constants, PartitionSpec, Window}
import ai.chronon.observability.{DriftMetric, TileSummary, TileSummarySeries}
import ai.chronon.online.KVStore
import ai.chronon.online.stats.DriftStore
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.stats.drift.scripts.PrepareData
import ai.chronon.online.InMemoryKvStore
import ai.chronon.spark.utils.{MockApi, SparkTestBase}
import org.scalatest.matchers.should.Matchers
import org.slf4j.LoggerFactory

import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.util.Success

class DriftTest extends SparkTestBase with Matchers {

  val namespace = "drift_test"
  implicit val tableUtils: TableUtils = TableUtils(spark)
  createDatabase(namespace)

  @transient private lazy val logger = LoggerFactory.getLogger(getClass.getName)

  def showTable(name: String)(implicit tableUtils: TableUtils): Unit = {
    logger.info(s"Showing table $name".yellow)
    val df = tableUtils.loadTable(name)
    val maxColNameLength = df.schema.fieldNames.map(_.length).max
    def pad(s: String): String = s.padTo(maxColNameLength, ' ')
    df.schema.fields.foreach { f =>
      logger.info(s"    ${pad(f.name)} : ${f.dataType.typeName}".yellow)
    }

    df.show(10)
  }

  "end_to_end" should "fetch prepare anomalous data, summarize, upload and fetch without failures" in {

    // generate anomalous data (join output)
    val prepareData = PrepareData(namespace)
    val join = prepareData.generateAnomalousFraudJoin
    val df = prepareData.generateFraudSampleData(200000, "2023-01-01", "2023-02-28", join.metaData.loggedTable)
    df.show(10)

    // mock api impl for online fetching and uploading
    val kvStoreFunc: () => KVStore = () => {
      // cannot reuse the variable - or serialization error
      val result = InMemoryKvStore.build("drift_test")
      result
    }
    val api = new MockApi(kvStoreFunc, namespace)

    // compute summary table and packed table (for uploading)
    Summarizer.compute(api, join.metaData, ds = "2023-02-28", useLogs = true)
    val summaryTable = join.metaData.summaryTable
    val packedTable = join.metaData.packedSummaryTable
    showTable(summaryTable)
    showTable(packedTable)

    // create necessary tables in kvstore
    val kvStore = api.genKvStore
    kvStore.create(Constants.MetadataDataset)
    kvStore.create(Constants.TiledSummaryDataset)

    // upload join conf
    api.buildFetcher().metadataStore.putJoinConf(join)

    // upload summaries
    val uploader = new SummaryUploader(tableUtils.loadTable(packedTable), api)
    uploader.run()

    // test drift store methods
    val driftStore = new DriftStore(api.genKvStore)

    // fetch keys
    driftStore.tileKeysForJoin(join)

    // fetch summaries
    val startMs = PartitionSpec.daily.epochMillis("2023-01-01")
    val endMs = PartitionSpec.daily.epochMillis("2023-02-28")
    val summariesFuture = driftStore.getSummaries(join, Some(startMs), Some(endMs), None)
    val summaries = Await.result(summariesFuture, Duration.create(10, TimeUnit.SECONDS))
    logger.info(s"${summaries.length} summaries fetched successfully")

    // fetch drift series
    val driftSeriesFuture = driftStore.getDriftSeries(
      join.metaData.name,
      DriftMetric.JENSEN_SHANNON,
      lookBack = new Window(7, chronon.api.TimeUnit.DAYS),
      startMs,
      endMs
    )
    val driftSeries = Await.result(driftSeriesFuture.get, Duration.create(10, TimeUnit.SECONDS))

    val (nulls, totals) = driftSeries.iterator.foldLeft(0 -> 0) { case ((nulls, total), s) =>
      val currentNulls = Option(s.getPercentileDriftSeries).map(_.iterator().toScala.count(_ == null)).getOrElse(0)
      val currentCount = Option(s.getPercentileDriftSeries).map(_.size()).getOrElse(0)
      (nulls + currentNulls, total + currentCount)
    }

    logger.info(s"""drift totals: $totals
         |drift nulls: $nulls
         |""".stripMargin.red)

    logger.info("Drift series fetched successfully".green)

    totals should be > 0
    nulls.toDouble / totals.toDouble should be < 0.6

    val summarySeriesFuture = driftStore.getSummarySeries(
      join.metaData.name,
      startMs,
      endMs
    )
    val summarySeries = Await.result(summarySeriesFuture.get, Duration.create(100, TimeUnit.SECONDS))
    val (summaryNulls, summaryTotals) = summarySeries.iterator.foldLeft(0 -> 0) { case ((nulls, total), s) =>
      if (s.getPercentiles == null) {
        (nulls + 1) -> (total + 1)
      } else {
        val currentNulls = s.getPercentiles.iterator().toScala.count(_ == null)
        val currentCount = s.getPercentiles.size()
        (nulls + currentNulls, total + currentCount)
      }
    }
    logger.info(s"""summary ptile totals: $summaryTotals
         |summary ptile nulls: $summaryNulls
         |""".stripMargin)

    summaryTotals should be > 0
    // TODO - see why this is acting up
    summaryNulls.toDouble / summaryTotals.toDouble should be < 0.3
    logger.info("Summary series fetched successfully".green)

    val startTs = 1673308800000L
    val endTs = 1674172800000L
    val joinName = "risk.user_transactions.txn_join"
    val name = "dim_user_account_type"
    val window = new Window(10, ai.chronon.api.TimeUnit.HOURS)

    implicit val execContext = scala.concurrent.ExecutionContext.global
    val metric = ValuesMetric
    val maybeCurrentSummarySeries = driftStore.getSummarySeries(joinName, startTs, endTs, Some(name))
    val maybeBaselineSummarySeries =
      driftStore.getSummarySeries(joinName, startTs - window.millis, endTs - window.millis, Some(name))
    val result = (maybeCurrentSummarySeries, maybeBaselineSummarySeries) match {
      case (Success(currentSummarySeriesFuture), Success(baselineSummarySeriesFuture)) =>
        Future.sequence(Seq(currentSummarySeriesFuture, baselineSummarySeriesFuture)).map { merged =>
          val currentSummarySeries = merged.head
          val baselineSummarySeries = merged.last
          val isCurrentNumeric = currentSummarySeries.headOption.forall(checkIfNumeric)
          val isBaselineNumeric = baselineSummarySeries.headOption.forall(checkIfNumeric)

          val currentFeatureTs = {
            if (currentSummarySeries.isEmpty) Seq.empty
            else convertTileSummarySeriesToTimeSeries(currentSummarySeries.head, isCurrentNumeric, metric)
          }
          val baselineFeatureTs = {
            if (baselineSummarySeries.isEmpty) Seq.empty
            else convertTileSummarySeriesToTimeSeries(baselineSummarySeries.head, isBaselineNumeric, metric)
          }

          ComparedFeatureTimeSeries(name, isCurrentNumeric, baselineFeatureTs, currentFeatureTs)
        }
    }
    val comparedTimeSeries = Await.result(result, Duration.create(10, TimeUnit.SECONDS))
    logger.info(
      s"lengths - current/baseline: ${comparedTimeSeries.current.length} / ${comparedTimeSeries.baseline.length}")
  }

  // this is clunky copy of code, but was necessary to run the logic end-to-end without mocking drift store
  // TODO move this into TimeSeriesControllerSpec and refactor that test to be more end-to-end.
  case class ComparedFeatureTimeSeries(feature: String,
                                       isNumeric: Boolean,
                                       baseline: Seq[TimeSeriesPoint],
                                       current: Seq[TimeSeriesPoint])

  sealed trait Metric

  /** Roll up over null counts */
  case object NullMetric extends Metric

  /** Roll up over raw values */
  case object ValuesMetric extends Metric

  case class TimeSeriesPoint(value: Double, ts: Long, label: Option[String] = None, nullValue: Option[Int] = None)

  def checkIfNumeric(summarySeries: TileSummarySeries): Boolean = {
    val ptiles = summarySeries.percentiles.toScala
    ptiles != null && ptiles.exists(_ != null)
  }

  private def convertTileSummarySeriesToTimeSeries(summarySeries: TileSummarySeries,
                                                   isNumeric: Boolean,
                                                   metric: Metric): Seq[TimeSeriesPoint] = {
    if (metric == NullMetric) {
      summarySeries.nullCount.toScala.zip(summarySeries.timestamps.toScala).map { case (nullCount, ts) =>
        TimeSeriesPoint(0, ts, nullValue = Some(nullCount.intValue()))
      }
    } else {
      if (isNumeric) {
        val percentileSeriesPerBreak = summarySeries.percentiles.toScala
        val timeStamps = summarySeries.timestamps.toScala
        val breaks = Seq("p5", "p50", "p95")
        percentileSeriesPerBreak.zip(breaks).flatMap { case (percentileSeries, break) =>
          percentileSeries.toScala.zip(timeStamps).map { case (value, ts) => TimeSeriesPoint(value, ts, Some(break)) }
        }
      } else {
        val histogramOfSeries = summarySeries.histogram.toScala
        val timeStamps = summarySeries.timestamps.toScala
        histogramOfSeries.flatMap { case (label, values) =>
          values.toScala.zip(timeStamps).map { case (value, ts) => TimeSeriesPoint(value.toDouble, ts, Some(label)) }
        }.toSeq
      }
    }
  }

  "percentileToIndex" should "correctly convert percentile strings to indices" in {
    val info = new DriftStore(null).TileSummaryInfo(null, null)

    info.percentileToIndex("p0") shouldBe 0
    info.percentileToIndex("p5") shouldBe 1
    info.percentileToIndex("p50") shouldBe 10
    info.percentileToIndex("p95") shouldBe 19
    info.percentileToIndex("p100") shouldBe 20
  }

  it should "throw NumberFormatException for invalid input" in {
    val info = new DriftStore(null).TileSummaryInfo(null, null)

    an[NumberFormatException] should be thrownBy info.percentileToIndex("invalid")
    an[NumberFormatException] should be thrownBy info.percentileToIndex("p")
    an[NumberFormatException] should be thrownBy info.percentileToIndex("px5")
  }

  "filterPercentiles" should "correctly filter default percentiles" in {
    val info = new DriftStore(null).TileSummaryInfo(null, null)

    val summary = new TileSummary()
    summary.setPercentiles((0 to 100 by 5).map(_.toDouble).map(Double.box).asJava)

    val filtered = info.filterPercentiles(summary)
    filtered.getPercentiles.asScala should contain theSameElementsInOrderAs Seq(5.0, 50.0, 95.0).map(Double.box)
  }

  "filterPercentiles" should "correctly filter specified percentiles" in {
    val info = new DriftStore(null).TileSummaryInfo(null, null)

    val summary = new TileSummary()
    summary.setPercentiles((0 to 100 by 5).map(_.toDouble).map(Double.box).asJava)

    val filtered = info.filterPercentiles(summary, Seq("p10", "p55", "p75"))
    filtered.getPercentiles.asScala should contain theSameElementsInOrderAs Seq(10.0, 55.0, 75.0).map(Double.box)
  }

  it should "handle null percentiles" in {
    val info = new DriftStore(null).TileSummaryInfo(null, null)

    val summary = new TileSummary()
    summary.setPercentiles(null)

    val filtered = info.filterPercentiles(summary)
    filtered.getPercentiles should be(null)
  }
}
