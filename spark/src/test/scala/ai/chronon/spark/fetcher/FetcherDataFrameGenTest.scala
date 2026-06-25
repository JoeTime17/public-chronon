package ai.chronon.spark.fetcher

import ai.chronon.aggregator.test.Column
import ai.chronon.api
import ai.chronon.api.Builders.Derivation
import ai.chronon.api._
import ai.chronon.spark.Extensions._
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.utils.{DataFrameGen, SparkTestBase}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import java.util.TimeZone

/** Generates data using DataFrameGen with the same parameters and join structure as FetcherGeneratedTest,
  * but shifts all timestamps to a fixed past reference date so the test is stable and inspectable.
  *
  * This makes it easier to reason about the offline/online comparison without the data changing
  * every run due to System.currentTimeMillis() drift.
  */
class FetcherDataFrameGenTest extends SparkTestBase {

  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  private val tableUtils = TableUtils(spark)
  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

  // All generated data is shifted so that "today" aligns to this fixed date
  private val fixedToday = "2021-04-10"
  private val fixedYesterday = "2021-04-09"

  // Millisecond shift: move generated timestamps (relative to now) back to fixedEndMs
  private val fixedEndMs = TsUtils.datetimeToTs(s"$fixedToday 23:59:59")
  private val tsShift = fixedEndMs - System.currentTimeMillis()
  // Days shift for entity partition strings (negative → past)
  private val daysShift = (tsShift / (24L * 3600 * 1000)).toInt

  /** Shifts all timestamp-like Long columns by tsShift and recomputes ds.
    * Handles events (which have a ts column) and mutations (which also have mutation_ts).
    * Falls back to date_add on the ds string when no ts column is present (entities).
    */
  private def shiftTimestamps(df: DataFrame): DataFrame = {
    val tsShiftLit = tsShift
    val daysShiftLit = daysShift
    var result = df

    for (c <- Seq("ts", "mutation_ts", "created_at", "updated_at")) {
      if (result.schema.fieldNames.contains(c)) {
        result = result.withColumn(c, result.col(c) + tsShiftLit)
      }
    }

    // Recompute ds from shifted ts when available; otherwise shift the string partition column
    if (result.schema.fieldNames.contains("ts")) {
      result = result.withColumn("ds", from_unixtime(result.col("ts") / 1000, tableUtils.partitionFormat))
    } else if (result.schema.fieldNames.contains("ds")) {
      result = result.withColumn(
        "ds",
        date_format(date_add(to_date(result.col("ds"), tableUtils.partitionFormat), daysShiftLit),
                    tableUtils.partitionFormat)
      )
    }

    result
  }

  it should "test temporal fetch join with DataFrameGen data at fixed dates" in {
    val namespace = "dfgen_fetch"
    val joinConf = generateData(namespace, spark)
    FetcherTestUtil.compareTemporalFetch(
      joinConf,
      endDs = fixedToday,
      namespace = namespace,
      consistencyCheck = false,
      dropDsOnWrite = false
    )(spark)
  }

  // Same structure as FetcherTestUtil.generateRandomData but with timestamp shifting.
  private def generateData(namespace: String, spark: SparkSession): api.Join = {
    SparkTestBase.createDatabase(spark, namespace)

    val keyCount = 4
    val cardinality = 100
    val rowCount = cardinality * keyCount * 50
    val userCol = Column("user", StringType, keyCount)
    val vendorCol = Column("vendor", StringType, keyCount)
    val topic = "test_topic_dfgen"

    // --- temporal events: payments ---
    val paymentCols = Seq(userCol, vendorCol, Column("payment", LongType, 100), Column("notes", StringType, 20))
    val paymentsTable = s"$namespace.payments_table"
    val tsColString = "ts_string"
    shiftTimestamps(DataFrameGen.events(spark, paymentCols, rowCount, 60))
      .withTimeFormattedColumn(tsColString, Constants.TimeColumn, "yyyy-MM-dd HH:mm:ss")
      .save(paymentsTable)

    val userPaymentsGroupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.events(query = Builders.Query(), table = paymentsTable, topic = topic)),
      keyColumns = Seq("user"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.COUNT,
                             inputColumn = "payment",
                             windows = Seq(new Window(6, TimeUnit.HOURS), new Window(14, TimeUnit.DAYS))),
        Builders.Aggregation(operation = Operation.COUNT, inputColumn = "payment"),
        Builders.Aggregation(operation = Operation.LAST, inputColumn = "payment"),
        Builders.Aggregation(operation = Operation.LAST_K, argMap = Map("k" -> "5"), inputColumn = "notes"),
        Builders.Aggregation(operation = Operation.VARIANCE, inputColumn = "payment"),
        Builders.Aggregation(operation = Operation.FIRST, inputColumn = "notes"),
        Builders.Aggregation(operation = Operation.FIRST, inputColumn = tsColString),
        Builders.Aggregation(operation = Operation.LAST, inputColumn = tsColString)
      ),
      metaData = Builders.MetaData(name = "unit_test.user_payments", namespace = namespace)
    )

    // --- snapshot events: ratings ---
    val ratingCols = Seq(
      userCol,
      vendorCol,
      Column("rating", IntType, 5),
      Column("bucket", StringType, 5),
      Column("sub_rating", ListType(DoubleType), 5),
      Column("txn_types", ListType(StringType), 5)
    )
    val ratingsTable = s"$namespace.ratings_table"
    shiftTimestamps(DataFrameGen.events(spark, ratingCols, rowCount, 180)).save(ratingsTable)

    val vendorRatingsGroupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.events(query = Builders.Query(), table = ratingsTable)),
      keyColumns = Seq("vendor"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.AVERAGE,
                             inputColumn = "rating",
                             windows = Seq(new Window(2, TimeUnit.DAYS), new Window(30, TimeUnit.DAYS)),
                             buckets = Seq("bucket")),
        Builders.Aggregation(operation = Operation.SKEW,
                             inputColumn = "rating",
                             windows = Seq(new Window(2, TimeUnit.DAYS), new Window(30, TimeUnit.DAYS)),
                             buckets = Seq("bucket")),
        Builders.Aggregation(operation = Operation.HISTOGRAM,
                             inputColumn = "txn_types",
                             windows = Seq(new Window(3, TimeUnit.DAYS))),
        Builders.Aggregation(operation = Operation.APPROX_FREQUENT_K,
                             inputColumn = "txn_types",
                             windows = Seq(new Window(3, TimeUnit.DAYS))),
        Builders.Aggregation(operation = Operation.LAST_K,
                             argMap = Map("k" -> "300"),
                             inputColumn = "user",
                             windows = Seq(new Window(2, TimeUnit.DAYS), new Window(30, TimeUnit.DAYS)))
      ),
      metaData = Builders.MetaData(name = "unit_test.vendor_ratings", namespace = namespace),
      accuracy = Accuracy.SNAPSHOT
    )

    // --- no-agg entities: balance ---
    val userBalanceCols = Seq(userCol, Column("balance", IntType, 5000))
    val balanceTable = s"$namespace.balance_table"
    shiftTimestamps(DataFrameGen.entities(spark, userBalanceCols, rowCount, 180))
      .groupBy("user", "ds")
      .agg(avg("balance") as "avg_balance")
      .save(balanceTable)

    val userBalanceGroupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.entities(query = Builders.Query(), snapshotTable = balanceTable)),
      keyColumns = Seq("user"),
      metaData = Builders.MetaData(name = "unit_test.user_balance", namespace = namespace)
    )

    // --- snapshot entities: credit ---
    val userVendorCreditCols = Seq(
      Column("account", StringType, 100),
      vendorCol,
      Column("credit", IntType, 500),
      Column("ts", LongType, 100)
    )
    val creditTable = s"$namespace.credit_table"
    shiftTimestamps(DataFrameGen.entities(spark, userVendorCreditCols, rowCount, 100))
      .withColumnRenamed("vendor", "vendor_id")
      .save(creditTable)

    val creditGroupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.entities(query = Builders.Query(), snapshotTable = creditTable)),
      keyColumns = Seq("vendor_id"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.SUM,
                             inputColumn = "credit",
                             windows = Seq(new Window(2, TimeUnit.DAYS), new Window(30, TimeUnit.DAYS)))),
      metaData = Builders.MetaData(name = "unit_test.vendor_credit", namespace = namespace)
    )

    val creditDerivationGroupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.entities(query = Builders.Query(), snapshotTable = creditTable)),
      keyColumns = Seq("vendor_id"),
      aggregations = Seq(
        Builders
          .Aggregation(operation = Operation.SUM, inputColumn = "credit", windows = Seq(new Window(3, TimeUnit.DAYS)))),
      metaData = Builders.MetaData(name = "unit_test/vendor_credit_derivation", namespace = namespace),
      derivations = Seq(
        Builders.Derivation("credit_sum_3d_test_rename", "credit_sum_3d"),
        Builders.Derivation("*", "*")
      )
    )

    // --- temporal entities: reviews ---
    val vendorReviewCols = Seq(Column("vendor", StringType, 10), Column("review", LongType, 10))
    val snapshotTable = s"$namespace.reviews_table_snapshot"
    val mutationTable = s"$namespace.reviews_table_mutations"
    val mutationTopic = "reviews_mutation_topic"
    val (snapshotDf, mutationsDf) =
      DataFrameGen.mutations(spark, vendorReviewCols, rowCount, 35, 0.2, 1, keyColumnName = "vendor")
    shiftTimestamps(snapshotDf).withColumnRenamed("vendor", "vendor_id").save(snapshotTable)
    shiftTimestamps(mutationsDf).withColumnRenamed("vendor", "vendor_id").save(mutationTable)

    val reviewGroupBy = Builders.GroupBy(
      sources = Seq(
        Builders.Source.entities(
          query = Builders.Query(startPartition = tableUtils.partitionSpec.before(fixedYesterday)),
          snapshotTable = snapshotTable,
          mutationTable = mutationTable,
          mutationTopic = mutationTopic
        )),
      keyColumns = Seq("vendor_id"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.SUM,
                             inputColumn = "review",
                             windows = Seq(new Window(2, TimeUnit.DAYS), new Window(30, TimeUnit.DAYS)))),
      metaData = Builders.MetaData(name = "unit_test/vendor_review", namespace = namespace),
      accuracy = Accuracy.TEMPORAL
    )

    // --- query table ---
    val queryCols = Seq(userCol, vendorCol)
    val queriesTable = s"$namespace.queries_table"
    shiftTimestamps(DataFrameGen.events(spark, queryCols, rowCount, 4))
      .withColumnRenamed("user", "user_id")
      .withColumnRenamed("vendor", "vendor_id")
      .save(queriesTable)

    Builders
      .Join(
        left = Builders.Source.events(Builders.Query(startPartition = fixedToday), table = queriesTable),
        joinParts = Seq(
          Builders
            .JoinPart(groupBy = vendorRatingsGroupBy, keyMapping = Map("vendor_id" -> "vendor"))
            .setUseLongNames(false),
          Builders
            .JoinPart(groupBy = userPaymentsGroupBy, keyMapping = Map("user_id" -> "user"))
            .setUseLongNames(false),
          Builders.JoinPart(groupBy = userBalanceGroupBy, keyMapping = Map("user_id" -> "user")).setUseLongNames(false),
          Builders.JoinPart(groupBy = reviewGroupBy).setUseLongNames(false),
          Builders.JoinPart(groupBy = creditGroupBy, prefix = "b").setUseLongNames(false),
          Builders.JoinPart(groupBy = creditGroupBy, prefix = "a").setUseLongNames(false),
          Builders.JoinPart(groupBy = creditDerivationGroupBy, prefix = "c").setUseLongNames(false)
        ),
        metaData = Builders.MetaData(name = "test.payments_join_dfgen",
                                     namespace = namespace,
                                     team = "chronon",
                                     consistencySamplePercent = 30),
        derivations = Seq(
          Builders.Derivation("*", "*"),
          Builders.Derivation("hist_3d", "vendor_txn_types_histogram_3d"),
          Builders.Derivation("payment_variance", "user_payment_variance/2"),
          Builders.Derivation("derived_ds", "from_unixtime(ts/1000, 'yyyy-MM-dd')"),
          Builders.Derivation("direct_ds", "ds")
        )
      )
      .setUseLongNames(false)
  }
}
