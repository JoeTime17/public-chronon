package ai.chronon.spark.scripts

import ai.chronon
import ai.chronon.api.ColorPrinter.ColorString
import ai.chronon.api.Constants
import ai.chronon.api.Extensions.MetadataOps
import ai.chronon.api.Extensions.WindowOps
import ai.chronon.api.PartitionSpec
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.api.Window
import ai.chronon.observability.DriftMetric
import ai.chronon.observability.TileDriftSeries
import ai.chronon.observability.TileSummarySeries
import ai.chronon.online.KVStore
import ai.chronon.online.stats.DriftStore
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.stats.drift.Summarizer
import ai.chronon.spark.stats.drift.SummaryUploader
import ai.chronon.spark.stats.drift.scripts.PrepareData
import ai.chronon.spark.submission.SparkSessionBuilder
import ai.chronon.online.InMemoryKvStore
import ai.chronon.spark.utils.MockApi
import org.rogach.scallop.ScallopConf
import org.rogach.scallop.ScallopOption
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.util.concurrent.TimeUnit
import scala.concurrent.Await
import scala.concurrent.duration.Duration

object ObservabilityDemo {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {

    val config = new Conf(args)
    val startDs = config.startDs()
    val endDs = config.endDs()
    val rowCount = config.rowCount()
    val namespace = config.namespace()

    val spark = SparkSessionBuilder.build(namespace, local = true)
    implicit val tableUtils: TableUtils = TableUtils(spark)
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $namespace")

    // generate anomalous data (join output)
    val prepareData = PrepareData(namespace)
    val join = prepareData.generateAnomalousFraudJoin

    time("Preparing data") {
      val df = prepareData.generateFraudSampleData(rowCount, startDs, endDs, join.metaData.loggedTable)
      df.show(10, truncate = false)
    }

    // mock api impl for online fetching and uploading
    val kvStoreFunc: () => KVStore = () => {
      // cannot reuse the variable - or serialization error
      val result = InMemoryKvStore.build(namespace)
      result
    }
    val api = new MockApi(kvStoreFunc, namespace)

    time("Summarizing data") {
      // compute summary table and packed table (for uploading)
      Summarizer.compute(api, join.metaData, ds = endDs, useLogs = true)
    }

    val packedTable = join.metaData.packedSummaryTable

    // create necessary tables in kvstore
    val kvStore = api.genKvStore
    kvStore.create(Constants.MetadataDataset)
    kvStore.create(Constants.TiledSummaryDataset)

    // upload join conf
    api.buildFetcher().metadataStore.putJoinConf(join)

    time("Uploading summaries") {
      val uploader = new SummaryUploader(tableUtils.loadTable(packedTable), api)
      uploader.run()
    }

    // test drift store methods
    val driftStore = new DriftStore(api.genKvStore)

    // TODO: Wire up drift store into hub and create an endpoint

    // fetch keys
    val tileKeys = driftStore.tileKeysForJoin(join)
    val tileKeysSimple = tileKeys.mapValues(_.map(_.column).toSeq)
    tileKeysSimple.foreach { case (k, v) => logger.info(s"$k -> [${v.mkString(", ")}]") }

    // fetch summaries
    val startMs = PartitionSpec.daily.epochMillis(startDs)
    val endMs = PartitionSpec.daily.epochMillis(endDs)
    val summariesFuture = driftStore.getSummaries(join, Some(startMs), Some(endMs), None)
    val summaries = Await.result(summariesFuture, Duration.create(10, TimeUnit.SECONDS))
    logger.info(summaries.toString())

    var driftSeries: Seq[TileDriftSeries] = null
    // fetch drift series
    time("Fetching drift series") {
      val driftSeriesFuture = driftStore.getDriftSeries(
        join.metaData.name,
        DriftMetric.JENSEN_SHANNON,
        lookBack = new Window(7, chronon.api.TimeUnit.DAYS),
        startMs,
        endMs
      )
      driftSeries = Await.result(driftSeriesFuture.get, Duration.create(10, TimeUnit.SECONDS))
    }

    val (nulls, totals) = driftSeries.iterator.foldLeft(0 -> 0) { case ((nulls, total), s) =>
      val currentNulls = s.getPercentileDriftSeries.iterator().toScala.count(_ == null)
      val currentCount = s.getPercentileDriftSeries.size()
      (nulls + currentNulls, total + currentCount)
    }

    logger.info(s"""
           |drift totals: $totals
           |drift nulls: $nulls
           |""".stripMargin.red)

    logger.info("Drift series fetched successfully".green)

    var summarySeries: Seq[TileSummarySeries] = null

    time("Fetching summary series") {
      val summarySeriesFuture = driftStore.getSummarySeries(
        join.metaData.name,
        startMs,
        endMs
      )
      summarySeries = Await.result(summarySeriesFuture.get, Duration.create(10, TimeUnit.SECONDS))
    }

    val (summaryNulls, summaryTotals) = summarySeries.iterator.foldLeft(0 -> 0) { case ((nulls, total), s) =>
      if (s.getPercentiles == null) {
        (nulls + 1) -> (total + 1)
      } else {
        val currentNulls = s.getPercentiles.iterator().toScala.count(_ == null)
        val currentCount = s.getPercentiles.size()
        (nulls + currentNulls, total + currentCount)
      }
    }

    val server = new DataServer(driftSeries.toSeq, summarySeries.toSeq)
    server.start()

    val startTs = 1673308800000L
    val endTs = 1674172800000L
    val joinName = "risk.user_transactions.txn_join"
    val name = "dim_user_account_type"
    val window = new Window(10, ai.chronon.api.TimeUnit.HOURS)

    logger.info("Looking up current summary series")
    val maybeCurrentSummarySeries = driftStore.getSummarySeries(joinName, startTs, endTs, Some(name)).get
    val currentSummarySeries = Await.result(maybeCurrentSummarySeries, Duration.create(10, TimeUnit.SECONDS))
    logger.info("Now looking up baseline summary series")
    val maybeBaselineSummarySeries =
      driftStore.getSummarySeries(joinName, startTs - window.millis, endTs - window.millis, Some(name))
    val baselineSummarySeries = Await.result(maybeBaselineSummarySeries.get, Duration.create(10, TimeUnit.SECONDS))

    logger.info(s"Current summary series: $currentSummarySeries")
    logger.info(s"Baseline summary series: $baselineSummarySeries")

    logger.info(s"""
           |summary ptile totals: $summaryTotals
           |summary ptile nulls: $summaryNulls
           |""".stripMargin)

    logger.info("Summary series fetched successfully".green)

    spark.stop()
    System.exit(0)
  }

  def time(message: String)(block: => Unit): Unit = {
    logger.info(s"$message..".yellow)
    val start = System.currentTimeMillis()
    block
    val end = System.currentTimeMillis()
    logger.info(s"$message took ${end - start} ms".green)
  }

  class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
    val startDs: ScallopOption[String] = opt[String](
      name = "start-ds",
      default = Some("2023-01-08"),
      descr = "Start date in YYYY-MM-DD format"
    )

    val endDs: ScallopOption[String] = opt[String](
      name = "end-ds",
      default = Some("2023-02-28"),
      descr = "End date in YYYY-MM-DD format"
    )

    val rowCount: ScallopOption[Int] = opt[Int](
      name = "row-count",
      default = Some(700000),
      descr = "Number of rows to generate"
    )

    val namespace: ScallopOption[String] = opt[String](
      name = "namespace",
      default = Some("observability_demo"),
      descr = "Namespace for the demo"
    )

    verify()
  }
}
