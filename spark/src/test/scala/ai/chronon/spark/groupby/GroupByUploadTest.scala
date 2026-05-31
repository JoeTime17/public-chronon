/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.spark.groupby

import ai.chronon.aggregator.test.Column
import ai.chronon.api.Extensions._
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.api.{Window, _}
import ai.chronon.online.fetcher.Fetcher
import ai.chronon.aggregator.windowing.{FiveMinuteResolution, SawtoothOnlineAggregator}
import ai.chronon.online.serde.{AvroCodec, AvroConversions, SparkConversions}
import ai.chronon.spark.Extensions.DataframeOps
import ai.chronon.spark.GroupByUpload
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.submission.SparkSessionBuilder
import ai.chronon.spark.utils.{DataFrameGen, MockApi, OnlineUtils, SparkTestBase}
import com.google.gson.Gson
import org.apache.spark.sql.{Row, SparkSession}
import org.junit.Assert.assertEquals
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import java.util
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

class GroupByUploadTest extends SparkTestBase with Matchers {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  private val tableUtils = TableUtils(spark)
  private val createdDatabases = scala.collection.mutable.Set[String]()

  // Generate unique namespace per test to avoid interference
  private def testNamespace(suffix: String = ""): String = {
    val uuid = java.util.UUID.randomUUID().toString.replace("-", "").take(8)
    val ns = if (suffix.isEmpty) {
      s"group_by_upload_test_${uuid}"
    } else {
      s"group_by_upload_test_${suffix}_${uuid}"
    }
    createdDatabases.add(ns)
    ns
  }

  override def afterAll(): Unit = {
    // Cleanup: drop all created databases
    createdDatabases.foreach { db =>
      try {
        spark.sql(s"DROP DATABASE IF EXISTS $db CASCADE")
      } catch {
        case e: Exception => logger.warn(s"Failed to drop database $db", e)
      }
    }
    super.afterAll()
  }

  it should "temporal events last k" in {
    val namespace = testNamespace("temporal_events_last_k")
    val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
    val yesterday = tableUtils.partitionSpec.before(today)
    createDatabase(namespace)
    tableUtils.sql(s"USE $namespace")
    val eventsTable = "events_last_k_dup" // occurs in groupByTest
    val eventSchema = List(
      Column("user", StringType, 10),
      Column("list_event", StringType, 100)
    )
    val eventDf = DataFrameGen.events(spark, eventSchema, count = 1000, partitions = 18)
    eventDf.save(s"$namespace.$eventsTable")

    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(Operation.LAST_K, "list_event", Seq(WindowUtils.Unbounded), argMap = Map("k" -> "30"))
    )
    val keys = Seq("user").toArray
    val groupByConf =
      Builders.GroupBy(
        sources = Seq(Builders.Source.events(Builders.Query(), table = eventsTable)),
        keyColumns = keys,
        aggregations = aggregations,
        metaData = Builders.MetaData(namespace = namespace, name = "test_last_k_upload"),
        accuracy = Accuracy.TEMPORAL
      )
    GroupByUpload.run(groupByConf, endDs = yesterday)
  }

  it should "struct support" in {
    val namespace = testNamespace("struct_support")
    val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
    val yesterday = tableUtils.partitionSpec.before(today)
    createDatabase(namespace)
    tableUtils.sql(s"USE $namespace")
    val eventsBase = "events_source"
    val eventsTable = "events_table_struct"
    val eventSchema = List(
      Column("user", StringType, 10),
      Column("event1", IntType, 100),
      Column("event2", DoubleType, 100),
      Column("event3", LongType, 100),
      Column("event4", StringType, 100)
    )
    val eventBaseDf = DataFrameGen.events(spark, eventSchema, count = 1000, partitions = 18)
    eventBaseDf.save(s"$namespace.$eventsBase")

    val eventDf = spark.sql(s"""
      SELECT
        user
        , ts
        , ds
        , NAMED_STRUCT('event1', event1, 'event2', event2, 'nested', NAMED_STRUCT('event3', event3, 'event4', event4)) as event_struct
      FROM $namespace.$eventsBase
      """)
    eventDf.save(s"$namespace.$eventsTable")
    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(Operation.LAST_K, "event_struct", Seq(WindowUtils.Unbounded), argMap = Map("k" -> "30"))
    )
    val keys = Seq("user").toArray
    val groupByConf =
      Builders.GroupBy(
        sources = Seq(Builders.Source.events(Builders.Query(), table = eventsTable)),
        keyColumns = keys,
        aggregations = aggregations,
        metaData = Builders.MetaData(namespace = namespace, name = "test_last_k_upload"),
        accuracy = Accuracy.TEMPORAL
      )
    GroupByUpload.run(groupByConf, endDs = yesterday)
  }

  it should "multiple avg counters" in {
    val namespace = testNamespace("multiple_avg_counters")
    val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
    val yesterday = tableUtils.partitionSpec.before(today)
    createDatabase(namespace)
    tableUtils.sql(s"USE $namespace")
    val eventsTable = "my_events"
    val eventSchema = List(
      Column("user", StringType, 10),
      Column("list_event", StringType, 100),
      Column("views", IntType, 10),
      Column("rating", IntType, 10)
    )
    val eventDf = DataFrameGen.events(spark, eventSchema, count = 1000, partitions = 18)
    eventDf.save(s"$namespace.$eventsTable")

    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(Operation.LAST_K, "list_event", Seq(WindowUtils.Unbounded), argMap = Map("k" -> "30")),
      Builders.Aggregation(Operation.AVERAGE, "views", Seq(WindowUtils.Unbounded, new Window(1, TimeUnit.DAYS)))
    )
    val keys = Seq("user").toArray
    val groupByConf =
      Builders.GroupBy(
        sources = Seq(Builders.Source.events(Builders.Query(), table = eventsTable)),
        keyColumns = keys,
        aggregations = aggregations,
        metaData = Builders.MetaData(namespace = namespace, name = "test_multiple_avg_upload"),
        accuracy = Accuracy.TEMPORAL
      )
    GroupByUpload.run(groupByConf, endDs = yesterday)
  }

  it should "produce a valid nullCountMap where both collapsedIr and tailHops are null for temporal events case" in {
    val namespace = testNamespace("nullcount_all_null")
    val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
    val yesterday = tableUtils.partitionSpec.before(today)
    createDatabase(namespace)
    tableUtils.sql(s"USE $namespace")
    val eventsTable = "my_events_always_null"
    val eventSchema = List(
      Column("user", StringType, 10),
      Column("list_event", StringType, 100, nullRate = 1.0), // always null
      Column("views", IntType, 10,  nullRate = 1.0), // always null
    )
    val eventDf = DataFrameGen.events(spark, eventSchema, count = 1000, partitions = 18)

    // Check the input data is as expected
    eventDf.show(10, truncate = false)
    val listEventNotNullCount = eventDf.filter("list_event IS NOT NULL").count()
    val viewsNotNullCount = eventDf.filter("views IS NOT NULL").count()
    listEventNotNullCount shouldBe 0
    viewsNotNullCount shouldBe 0

    eventDf.save(s"$namespace.$eventsTable")

    // Count how many "user" keys there are in the eventDf
    val numUsers = spark.sql(s"SELECT COUNT(DISTINCT user) as user_count FROM $namespace.$eventsTable").collect().head.getAs[Long]("user_count")

    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(Operation.LAST_K, "list_event", Seq(new Window(18, TimeUnit.DAYS)), argMap = Map("k" -> "30")),
      Builders.Aggregation(Operation.AVERAGE, "views", Seq(new Window(18, TimeUnit.DAYS), new Window(1, TimeUnit.DAYS)))
    )
    val keys = Seq("user").toArray
    val groupByConf =
      Builders.GroupBy(
        sources = Seq(Builders.Source.events(Builders.Query(), table = eventsTable)),
        keyColumns = keys,
        aggregations = aggregations,
        metaData = Builders.MetaData(namespace = namespace, name = "test_multiple_avg_upload"),
        accuracy = Accuracy.TEMPORAL
      )
    val result = GroupByUpload.generateDf(groupByConf, endDs = yesterday, tableUtils = tableUtils).nullCounts
    print(result)

    result.keys.size shouldBe 3 // 3 output columns. 1 agg with 1 window, 1 agg with 2 windows = 3 output columns
    result.values.foreach { count =>
      count shouldBe numUsers
    }
  }

  it should "produce a valid nullCountMap with both collapsedIr and tailHops are non null for temporal events case" in {
    val namespace = testNamespace("nullcount_non_null")
    val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
    val yesterday = tableUtils.partitionSpec.before(today)
    createDatabase(namespace)
    tableUtils.sql(s"USE $namespace")
    val eventsTable = "my_events_non_null"
    val eventSchema = List(
      Column("user", StringType, 10),
      Column("list_event", StringType, 100, nullRate = 0.0), // never null
      Column("views", IntType, 10,  nullRate = 0.0), // never null
    )
    val eventDf = DataFrameGen.events(spark, eventSchema, count = 1000, partitions = 18)
    import org.apache.spark.sql.functions.{col, lit}
    val guaranteedRecentDf = eventDf
      .select("user")
      .distinct()
      .withColumn("list_event", lit("recent-list-event"))
      .withColumn("views", lit(1))
      .withColumn("ts", lit(tableUtils.partitionSpec.epochMillis(today) - 1L))
      .withColumn(tableUtils.partitionColumn, lit(yesterday))
      .select(eventDf.columns.map(col): _*)
    val eventDfWithRecentRows = eventDf.unionByName(guaranteedRecentDf)

    // Check the input data is as expected
    eventDfWithRecentRows.show(10, truncate = false)
    val listEventNullCount = eventDfWithRecentRows.filter("list_event IS NULL").count()
    val viewsNullCount = eventDfWithRecentRows.filter("views IS NULL").count()
    listEventNullCount shouldBe 0
    viewsNullCount shouldBe 0

    eventDfWithRecentRows.save(s"$namespace.$eventsTable")

    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(Operation.LAST_K, "list_event", Seq(new Window(18, TimeUnit.DAYS)), argMap = Map("k" -> "30")),
      Builders.Aggregation(Operation.AVERAGE, "views", Seq(new Window(18, TimeUnit.DAYS), new Window(1, TimeUnit.DAYS)))
    )
    val keys = Seq("user").toArray
    val groupByConf =
      Builders.GroupBy(
        sources = Seq(Builders.Source.events(Builders.Query(), table = eventsTable)),
        keyColumns = keys,
        aggregations = aggregations,
        metaData = Builders.MetaData(namespace = namespace, name = "test_multiple_avg_upload"),
        accuracy = Accuracy.TEMPORAL
      )
    val result = GroupByUpload.generateDf(groupByConf, endDs = yesterday, tableUtils = tableUtils).nullCounts
    print(result)

    result.isEmpty shouldBe true // empty null count map
  }

  it should "produce a valid empty null nullCountMap for snapshot events case" in {
    val namespace = testNamespace("snapshot_empty_null")
    val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
    val yesterday = tableUtils.partitionSpec.before(today)
    createDatabase(namespace)
    tableUtils.sql(s"USE $namespace")
    val eventsTable = "my_snapshot_events_empty_null"
    val eventSchema = List(
      Column("user", StringType, 10),
      Column("list_event", StringType, 100, nullRate = 0.0), // never null
      Column("views", IntType, 10,  nullRate = 0.0), // never null
    )

    val eventDf = DataFrameGen.events(spark, eventSchema, count = 1000, partitions = 18)

    // Check the input data is as expected
    eventDf.show(10, truncate = false)
    val listEventNullCount = eventDf.filter("list_event IS NULL").count()
    val viewsNullCount = eventDf.filter("views IS NULL").count()
    listEventNullCount shouldBe 0
    viewsNullCount shouldBe 0

    eventDf.save(s"$namespace.$eventsTable")

    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(Operation.LAST_K, "list_event", Seq(WindowUtils.Unbounded), argMap = Map("k" -> "30")),
      Builders.Aggregation(Operation.AVERAGE, "views", Seq(WindowUtils.Unbounded))
    )
    val keys = Seq("user").toArray
    val groupByConf =
      Builders.GroupBy(
        sources = Seq(Builders.Source.events(Builders.Query(), table = eventsTable)),
        keyColumns = keys,
        aggregations = aggregations,
        metaData = Builders.MetaData(namespace = namespace, name = "test_multiple_avg_upload"),
        accuracy = Accuracy.SNAPSHOT
      )
    val result = GroupByUpload.generateDf(groupByConf, endDs = yesterday, tableUtils = tableUtils).nullCounts
    print(result)

    result shouldBe empty
  }

  it should "produce a valid non-empty nullCountMap for snapshot events case" in {
    val namespace = testNamespace("snapshot_non_empty_null")
    val batchEndDs = "2024-08-01"
    createDatabase(namespace)
    tableUtils.sql(s"USE $namespace")
    val eventsTable = "my_snapshot_events_non_empty_null"

    def ts(arg: String) = TsUtils.datetimeToTs(s"2023-$arg:00")
    val viewColumns = Seq("user", "list_event", "views", "ts", "ds")
    val viewsData = Seq(
      ("user1", "some-list-event", null.asInstanceOf[Integer], ts("08-13 11:00"), "2023-08-13"),
    )
    val viewsDf = spark.createDataFrame(viewsData).toDF(viewColumns: _*)
    viewsDf.save(eventsTable)
    viewsDf.show()

    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(Operation.LAST_K, "list_event", Seq(WindowUtils.Unbounded, new Window(5, TimeUnit.DAYS)), argMap = Map("k" -> "30")),
      Builders.Aggregation(Operation.AVERAGE, "views", Seq(WindowUtils.Unbounded))
    )
    val keys = Seq("user").toArray
    val groupByConf =
      Builders.GroupBy(
        sources = Seq(Builders.Source.events(Builders.Query(), table = eventsTable)),
        keyColumns = keys,
        aggregations = aggregations,
        metaData = Builders.MetaData(namespace = namespace, name = "test_multiple_avg_upload"),
        accuracy = Accuracy.SNAPSHOT
      )
    val result = GroupByUpload.generateDf(groupByConf, endDs = batchEndDs, tableUtils = tableUtils).nullCounts
    print(result)

    result.isEmpty shouldBe false
    result.keys.size shouldBe 2 // only the list_event unbounded was non-null. the other two should be null
    result.values.foreach { count =>
      count shouldBe 1L
    }
  }

  it should "produce a valid empty nullCountMap for snapshot entities case" in {
    val namespace = testNamespace("entities_empty_null")
    createDatabase(namespace)
    tableUtils.sql(s"USE $namespace")
    val reviewsTable = s"${namespace}.reviews_entity_non_empty_null"
    setupReviewsTable(reviewsTable)

    // empty out aggregations
    val reviewGroupBy = sampleEntitiesGroupBy(reviewsTable, namespace)
    reviewGroupBy.aggregations = null
    reviewGroupBy.accuracy = Accuracy.SNAPSHOT

    val result = GroupByUpload.generateDf(reviewGroupBy, endDs = "2023-08-15", tableUtils = tableUtils).nullCounts
    print(result)

    result shouldBe empty
  }

  it should "produce a valid non-empty nullCountMap for snapshot entities case" in {
    val namespace = testNamespace("entities_non_empty_null")
    createDatabase(namespace)
    tableUtils.sql(s"USE $namespace")
    val reviewsTable = s"${namespace}.reviews_entity_empty_null"

    def ts(arg: String) = TsUtils.datetimeToTs(s"2023-$arg:00")
    setupReviewsTable(reviewsTable, Option(Seq(
      ("review3", null.asInstanceOf[String], ts("08-15 08:00"), "2023-08-15") // insert
    )
    ))

    // empty out aggregations
    val reviewGroupBy = sampleEntitiesGroupBy(reviewsTable, namespace)
    reviewGroupBy.aggregations = null
    reviewGroupBy.accuracy = Accuracy.SNAPSHOT

    val result = GroupByUpload.generateDf(reviewGroupBy, endDs = "2023-08-15", tableUtils = tableUtils).nullCounts
    print(result)

    result.isEmpty shouldBe false
    result.values .foreach { count =>
      count shouldBe 1L
    }
  }


  def setupReviewsTable(reviewsTable: String, maybeReviewsData: Option[Seq[(String, String, Long, String)]] = None) = {
    def ts(arg: String) = TsUtils.datetimeToTs(s"2023-$arg:00")

    val reviewsColumns = Seq("review", "listing", "ts", "ds")
    val reviewsData: Seq[(String, String, Long, String)] = maybeReviewsData.getOrElse(
    Seq(
      ("review1", "listing1", ts("07-13 10:00"), "2023-08-14"),
      ("review2", "listing1", ts("07-13 11:00"), "2023-08-14"), // delete (next day)
      ("review3", "listing2", ts("08-15 08:00"), "2023-08-15") // insert
    ))

    val reviewsDf = spark.createDataFrame(reviewsData).toDF(reviewsColumns: _*)
    reviewsDf.save(reviewsTable)
    reviewsDf.show()

    val reviewsMutationsColumns = Seq("is_before", "mutation_ts", "review", "listing", "ts", "ds")
    val reviewsMutations = Seq(
      (true, ts("08-15 06:00"), "review2", "listing1", ts("07-13 11:00"), "2023-08-15"), // delete
      (false, ts("08-15 08:00"), "review3", "listing2", ts("08-15 08:00"), "2023-08-15") // insert
    )
    val reviewsMutationsDf = spark.createDataFrame(reviewsMutations).toDF(reviewsMutationsColumns: _*)
    reviewsMutationsDf.save(s"${reviewsTable}_mutations")
    reviewsMutationsDf.show()
  }
  def sampleEntitiesGroupBy(reviewsTable: String, namespace: String) = {
    Builders.GroupBy(
      metaData = Builders.MetaData(namespace = namespace, name = "review_attrs"),
      sources = Seq(
        Builders.Source.entities(
          Builders.Query(selects = Builders.Selects("review", "listing", "ts")),
          snapshotTable = reviewsTable,
          mutationTopic = s"${reviewsTable}_mutations",
          mutationTable = s"${reviewsTable}_mutations"
        )),
      keyColumns = scala.Seq("review"),
      aggregations = Seq(
        Builders.Aggregation(
          operation = Operation.LAST,
          inputColumn = "listing"
        ))
    )
  }
  //  joinLeft = (review, category, rating)  [ratings]
  //  joinPart = (review, user, listing)     [reviews]
  // groupBy = keys:[listing, category], aggs:[avg(rating)]
  it should "listing rating category join source" in {
    val namespace = testNamespace("listing_rating")
    createDatabase(namespace)
    tableUtils.sql(s"USE $namespace")

    val ratingsTable = s"${namespace}.ratings"
    def ts(arg: String) = TsUtils.datetimeToTs(s"2023-$arg:00")

    val ratingsColumns = Seq("review", "rating", "category_ratings", "ts", "ds")
    val ratingsData = Seq(
      ("review1", 4, Map("location" -> 4, "cleanliness" -> 4), ts("07-13 11:00"), "2023-08-14"),
      ("review2", 5, Map("location" -> 5, "cleanliness" -> 4), ts("07-13 12:00"), "2023-08-14"), // to delete
      ("review3", 3, Map("location" -> 4, "cleanliness" -> 2), ts("08-15 09:00"), "2023-08-15"), // insert
      ("review1", 2, Map("location" -> 1, "cleanliness" -> 3), ts("08-15 10:00"), "2023-08-15") // update
    )
    val ratingsDf = spark.createDataFrame(ratingsData).toDF(ratingsColumns: _*)
    ratingsDf.save(ratingsTable)
    ratingsDf.show()

    val ratingsMutationsColumns = Seq("is_before", "mutation_ts", "review", "rating", "category_ratings", "ts", "ds")

    val ds = "2023-08-15"
    val ratingsMutations = Seq(
      // delete
      (true, ts("08-15 06:00"), "review2", 5, Map("location" -> 5, "cleanliness" -> 4), ts("07-13 12:00"), ds),
      // insert
      (false, ts("08-15 09:00"), "review3", 3, Map("location" -> 4, "cleanliness" -> 2), ts("08-15 09:00"), ds),
      // update - before
      (true, ts("08-15 10:00"), "review1", 4, Map("location" -> 4, "cleanliness" -> 4), ts("07-13 11:00"), ds),
      // update - after
      (false, ts("08-15 10:00"), "review1", 2, Map("location" -> 1, "cleanliness" -> 3), ts("08-15 10:00"), ds)
    )
    val ratingsMutationsDf = spark.createDataFrame(ratingsMutations).toDF(ratingsMutationsColumns: _*)
    ratingsMutationsDf.save(s"${ratingsTable}_mutations")
    ratingsMutationsDf.show()

    val reviewsTable = s"${namespace}.reviews"
    setupReviewsTable(reviewsTable)

    val leftRatings =
      Builders.Source.entities(
        Builders.Query(selects = Builders.Selects("review", "rating", "category_ratings", "ts")),
        snapshotTable = ratingsTable,
        mutationTopic = s"${ratingsTable}_mutations",
        mutationTable = s"${ratingsTable}_mutations"
      )
    val reviewGroupBy = sampleEntitiesGroupBy(reviewsTable, namespace)

    val joinConf = Builders.Join(
      metaData = Builders.MetaData(namespace = namespace, name = "review_enrichment"),
      left = leftRatings,
      joinParts = Seq(Builders.JoinPart(groupBy = reviewGroupBy))
    )

    val listingRatingGroupBy = Builders.GroupBy(
      metaData = Builders.MetaData(namespace = namespace, name = "listing_ratings"),
      sources = Seq(
        Builders.Source.joinSource(
          join = joinConf,
          query = Builders.Query(selects =
            Builders.Selects("review", "review_attrs_listing_last", "rating", "category_ratings", "ts"))
        )),
      keyColumns = scala.Seq("review_attrs_listing_last"),
      aggregations = Seq(
        Builders.Aggregation(
          operation = Operation.AVERAGE,
          inputColumn = "rating"
        ),
        Builders.Aggregation(
          operation = Operation.AVERAGE,
          inputColumn = "category_ratings"
        )
      )
    )

    val kvStore = OnlineUtils.buildInMemoryKVStore("chaining_test")
    val endDs = "2023-08-15"
    val kvStoreFunc = () => OnlineUtils.buildInMemoryKVStore("chaining_test")

    // DO-NOT-SET debug=true here since the streaming job won't put data into kv store
    joinConf.joinParts.toScala.foreach(jp =>
      OnlineUtils.serve(tableUtils, kvStore, kvStoreFunc, "chaining_test", endDs, jp.groupBy, dropDsOnWrite = true))

    OnlineUtils.serve(tableUtils, kvStore, kvStoreFunc, "chaining_test", endDs, listingRatingGroupBy, debug = false)

    kvStoreFunc().show()

    // visualizing values by time to help reason about the tests
    //
    // listing1    08-15    hr = 00      hr = 06       hr = 10
    //   review 1           4, (4, 4)                  2, (1, 3)
    //   review 2           5, (5, 4)     absent       absent
    //                      4.5 (4.5, 4)  4, (4, 4)    2, (1, 3)
    //
    //                      location
    //
    // listing2    08-15    hr = 00    hr = 09
    //   review 3            absent    3, (4, 2)
    //                   null, null     3, (4, 2)
    //
    val api = new MockApi(kvStoreFunc, "chaining_test")
    val fetcher = api.buildFetcher(debug = true)
    val requestResponse = Seq(
      Fetcher.Request("listing_ratings",
                      Map("review_attrs_listing_last" -> "listing1"),
                      Some(ts("08-15 05:00"))) -> 4.5,
      Fetcher.Request("listing_ratings", Map("review_attrs_listing_last" -> "listing1"), Some(ts("08-15 08:00"))) -> 4,
      Fetcher.Request("listing_ratings", Map("review_attrs_listing_last" -> "listing1"), Some(ts("08-15 11:00"))) -> 2,
      Fetcher.Request("listing_ratings",
                      Map("review_attrs_listing_last" -> "listing2"),
                      Some(ts("08-15 07:00"))) -> null,
      Fetcher.Request("listing_ratings", Map("review_attrs_listing_last" -> "listing2"), Some(ts("08-15 10:00"))) -> 3
    )
    val responseF = fetcher.fetchGroupBys(requestResponse.map(_._1))

    val responses = Await.result(responseF, 10.seconds)
    val results = responses.map(r => r.values.get("rating_average"))
    val categoryRatingResults = responses.map(r => r.values.get("category_ratings_average")).toArray
    def cRating(location: Double, cleanliness: Double): java.util.Map[String, Double] =
      Map("location" -> location, "cleanliness" -> cleanliness).toJava
    val gson = new Gson()
    assertEquals(results, requestResponse.map(_._2))

    val expectedCategoryRatings = Array(
      cRating(4.5, 4.0),
      cRating(4.0, 4.0),
      cRating(1.0, 3.0),
      null,
      cRating(4.0, 2.0)
    )
    logger.info(gson.toJson(categoryRatingResults))
    logger.info(gson.toJson(expectedCategoryRatings))
    categoryRatingResults.zip(expectedCategoryRatings).foreach { case (actual, expected) =>
      assertEquals(actual, expected)
    }
  }

  // This test is to ensure that the GroupByUpload can handle a GroupBy with lastK struct and derivations
  it should "upload groupBy with lastK struct + derivations" in {
    val namespace = testNamespace("lastk_struct_derivations")
    val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
    val yesterday = tableUtils.partitionSpec.before(today)
    createDatabase(namespace)
    tableUtils.sql(s"USE $namespace")

    val eventsTable = "test_gb_with_derivations"

    // Create test data with the columns needed for the derivations GroupBy
    import org.apache.spark.sql.functions._
      import spark.implicits._

    val testData = Seq(
      ("test_user_123", 100, 42.5, System.currentTimeMillis() - 86400000L), // 1 day ago
      ("test_user_123", 200, 33.3, System.currentTimeMillis() - 172800000L), // 2 days ago
      ("test_user_456", 150, 25.0, System.currentTimeMillis() - 86400000L)
    ).toDF("id", "int_val", "double_val", "ts")
      .withColumn(tableUtils.partitionColumn, from_unixtime(col("ts") / 1000, tableUtils.partitionFormat))

    testData.save(s"$namespace.$eventsTable")

    val groupByConf = makeDerivationsGroupBy(namespace, eventsTable)
    GroupByUpload.run(groupByConf, endDs = yesterday)
  }

  private def makeDerivationsGroupBy(namespace: String, eventsTable: String): GroupBy =
    Builders.GroupBy(
      sources = Seq(
        Builders.Source.events(
          table = s"$namespace.$eventsTable",
          topic = "events.my_stream",
          query = Builders.Query(
            selects = Map(
              "id" -> "id",
              "int_val" -> "int_val",
              "double_val" -> "double_val",
              "named_struct" -> "IF(id IS NOT NULL, NAMED_STRUCT('id', id, 'int_val', int_val), NULL)",
              "another_named_struct" -> "IF(id IS NOT NULL, NAMED_STRUCT('id', id, 'double_val', double_val), NULL)"
            ),
            wheres = Seq.empty,
            timeColumn = "ts",
            startPartition = "20231106"
          )
        )
      ),
      keyColumns = Seq("id"),
      aggregations = Seq(
        Builders.Aggregation(
          operation = Operation.SUM,
          inputColumn = "double_val",
          windows = Seq(
            new Window(1, TimeUnit.DAYS)
          )
        ),
        Builders.Aggregation(
          operation = Operation.LAST_K,
          inputColumn = "named_struct",
          windows = Seq(
            new Window(1, TimeUnit.DAYS),
            new Window(2, TimeUnit.DAYS)
          ),
          argMap = Map("k" -> "2")
        ),
        Builders.Aggregation(
          operation = Operation.LAST_K,
          inputColumn = "another_named_struct",
          windows = Seq(
            new Window(1, TimeUnit.DAYS),
            new Window(2, TimeUnit.DAYS)
          ),
          argMap = Map("k" -> "2")
        ),
        Builders.Aggregation(
          operation = Operation.LAST,
          inputColumn = "int_val",
          windows = Seq(
            new Window(1, TimeUnit.DAYS)
          )
        )
      ),
      metaData = Builders.MetaData(
        namespace = namespace,
        name = "derivations_test_group_by"
      ),
      accuracy = Accuracy.TEMPORAL,
      derivations = Seq(
        Builders.Derivation(
          name = "int_val",
          expression = "int_val_last_1d"
        ),
        Builders.Derivation(
          name = "id_last2_1d",
          expression = "transform(named_struct_last2_1d, x -> x.id)"
        ),
        Builders.Derivation(
          name = "id2_last2_1d",
          expression = "transform(another_named_struct_last2_1d, x -> x.id)"
        )
      )
    )

  it should "produce valid batch data for temporal events case" in {
    val namespace = testNamespace("temporal_batch_data")
    createDatabase(namespace)
    val eventsTable = "my_events_check_temporal"
    GroupByUploadTest.runAndValidateActualTemporalBatchData(namespace=namespace, sparkSession = spark, tableUtils = tableUtils, eventsTable = eventsTable)
  }

  it should "handle decimal types with aggregations and derivations" in {
    val namespace = testNamespace("decimal_comprehensive")
    createDatabase(namespace)
    tableUtils.sql(s"USE $namespace")

    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.types.{DecimalType => SparkDecimalType}

    val transactionsTable = s"${namespace}.transactions"
    def ts(arg: String) = TsUtils.datetimeToTs(s"2023-$arg:00")

    val transactionsColumns = Seq("user_id", "product_id", "price", "discount", "quantity", "ts", "ds")
    val transactionsData = Seq(
      ("user1", "prod_a", new java.math.BigDecimal("100.50"), new java.math.BigDecimal("10.05"), new java.math.BigDecimal("2.5"), ts("08-13 10:00"), "2023-08-13"),
      ("user1", "prod_b", new java.math.BigDecimal("50.25"), new java.math.BigDecimal("5.00"), new java.math.BigDecimal("1.0"), ts("08-13 11:00"), "2023-08-13"),
      ("user2", "prod_c", new java.math.BigDecimal("200.75"), new java.math.BigDecimal("20.00"), new java.math.BigDecimal("3.0"), ts("08-13 09:00"), "2023-08-13")
    )
    val transactionsRdd = spark.sparkContext.parallelize(transactionsData)
    val transactionsDf = spark.createDataFrame(transactionsRdd).toDF(transactionsColumns: _*)
      .withColumn("price", col("price").cast(SparkDecimalType(10, 2)))
      .withColumn("discount", col("discount").cast(SparkDecimalType(8, 2)))
      .withColumn("quantity", col("quantity").cast(SparkDecimalType(8, 2)))
    transactionsDf.save(transactionsTable)
    transactionsDf.show()

    val userTransactionsGroupBy = Builders.GroupBy(
      metaData = Builders.MetaData(namespace = namespace, name = "user_transactions"),
      sources = Seq(
        Builders.Source.events(
          Builders.Query(selects = Builders.Selects("user_id", "price", "discount", "quantity", "ts")),
          table = transactionsTable
        )
      ),
      keyColumns = Seq("user_id"),
      aggregations = Seq(
        Builders.Aggregation(
          operation = Operation.SUM,
          inputColumn = "price",
          windows = Seq(WindowUtils.Unbounded)
        ),
        Builders.Aggregation(
          operation = Operation.SUM,
          inputColumn = "discount",
          windows = Seq(WindowUtils.Unbounded)
        ),
        Builders.Aggregation(
          operation = Operation.AVERAGE,
          inputColumn = "price",
          windows = Seq(WindowUtils.Unbounded)
        ),
        Builders.Aggregation(
          operation = Operation.LAST,
          inputColumn = "quantity",
          windows = Seq(WindowUtils.Unbounded)
        )
      ),
      derivations = Seq(
        Builders.Derivation(
          name = "net_price",
          expression = "price_sum - discount_sum"
        ),
        Builders.Derivation(
          name = "discount_rate",
          expression = "discount_sum / price_sum"
        )
      ),
      accuracy = Accuracy.TEMPORAL
    )

    val kvStore = OnlineUtils.buildInMemoryKVStore("decimal_test")
    val endDs = "2023-08-14"
    val kvStoreFunc = () => OnlineUtils.buildInMemoryKVStore("decimal_test")

    OnlineUtils.serve(tableUtils, kvStore, kvStoreFunc, "decimal_test", endDs, userTransactionsGroupBy, debug = false)

    kvStoreFunc().show()

    val api = new MockApi(kvStoreFunc, "decimal_test")
    val fetcher = api.buildFetcher(debug = true)
    val requests = Seq(
      Fetcher.Request("user_transactions", Map("user_id" -> "user1"), Some(ts("08-14 12:00"))),
      Fetcher.Request("user_transactions", Map("user_id" -> "user2"), Some(ts("08-14 12:00")))
    )
    val responseF = fetcher.fetchGroupBys(requests)
    val responses = Await.result(responseF, 10.seconds)

    // Validate user1: price_sum=150.75, discount_sum=15.05
    // Expected: net_price = 150.75 - 15.05 = 135.70, discount_rate = 15.05 / 150.75 ≈ 0.0998
    val user1Response = responses(0)
    val user1NetPrice = user1Response.values.get("net_price").asInstanceOf[java.math.BigDecimal]
    user1NetPrice.compareTo(new java.math.BigDecimal("135.70")) shouldBe 0

    val user1DiscountRate = user1Response.values.get("discount_rate").asInstanceOf[java.math.BigDecimal]
    math.abs(user1DiscountRate.doubleValue() - 0.0998) should be < 0.0001

    // Validate user2: price_sum=200.75, discount_sum=20.00
    // Expected: net_price = 200.75 - 20.00 = 180.75, discount_rate = 20.00 / 200.75 ≈ 0.0996
    val user2Response = responses(1)
    val user2NetPrice = user2Response.values.get("net_price").asInstanceOf[java.math.BigDecimal]
    user2NetPrice.compareTo(new java.math.BigDecimal("180.75")) shouldBe 0

    val user2DiscountRate = user2Response.values.get("discount_rate").asInstanceOf[java.math.BigDecimal]
    math.abs(user2DiscountRate.doubleValue() - 0.0996) should be < 0.0001
  }
}

object GroupByUploadTest {
  import org.scalatest.matchers.should.Matchers._

  def runAndValidateActualTemporalBatchData(namespace: String, sparkSession: SparkSession, tableUtils: TableUtils, eventsTable: String): Unit = {
    import ai.chronon.spark.Extensions.DataframeOps
    // Setup data
    tableUtils.sql(s"USE $namespace")
    val eventColumns = Seq("user", "views", "ts", "ds")
    val eventData =
      Seq(
        ("user1", 10, TsUtils.datetimeToTs(s"2023-08-13 11:00:00"), "2023-08-13"),
        ("user1", 10, TsUtils.datetimeToTs(s"2023-08-13 10:00:00"), "2023-08-13"),

        // purposely 7 -2 = 5 days before 08-14 to test 7 day window tail hop. first tail hop
        ("user1", 10, TsUtils.datetimeToTs(s"2023-08-09 11:00:00"), "2023-08-09"),

        // second tail hop for 7 day window
        ("user1", 10, TsUtils.datetimeToTs(s"2023-08-09 10:00:00"), "2023-08-09"),
        ("user1", 10, TsUtils.datetimeToTs(s"2023-08-09 10:15:00"), "2023-08-09"),

        // purposely 30 -2 = 28 days before 08-14 to test 30 day window tail hop. expect only 1 tail hop.
        ("user1", 10, TsUtils.datetimeToTs(s"2023-07-17 10:00:00"), "2023-07-17"),
        ("user1", 10, TsUtils.datetimeToTs(s"2023-07-17 11:00:00"), "2023-07-17"),
      )

    val eventDf = sparkSession.createDataFrame(eventData).toDF(eventColumns: _*)
    eventDf.save(eventsTable)
    eventDf.show()

    // Define the GroupBy
    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(Operation.SUM, "views", Seq(new Window(30, TimeUnit.DAYS),new Window(7, TimeUnit.DAYS)))
    )
    val keys = Seq("user").toArray
    val groupByConf =
      Builders.GroupBy(
        sources = Seq(Builders.Source.events(Builders.Query(), table = eventsTable)),
        keyColumns = keys,
        aggregations = aggregations,
        metaData = Builders.MetaData(namespace = namespace, name = "test_multiple_avg_upload"),
        accuracy = Accuracy.TEMPORAL
      )
    val result = GroupByUpload.generateDf(groupByConf, endDs = "2023-08-14", tableUtils = tableUtils)

    val kvDf = result.kvDf
    kvDf.columns should contain allOf ("key_bytes", "value_bytes", "key_json", "value_json")
    kvDf.count() shouldBe 1 // only one user

    // Null counts should be empty since all values are non-null
    result.nullCounts shouldBe empty

    // Decode the Avro value bytes to verify aggregation correctness
    val endTs = tableUtils.partitionSpec.epochMillis(tableUtils.partitionSpec.after("2023-08-14"))
    val chrononSchema = SparkConversions.toChrononSchema(eventDf.schema)
    val sawtoothAggregator = new SawtoothOnlineAggregator(endTs, aggregations, chrononSchema, FiveMinuteResolution)
    val valueZSchema = ai.chronon.api.StructType.from("Value", sawtoothAggregator.batchIrSchema)
    val valueSchemaStr = AvroConversions.fromChrononSchema(valueZSchema).toString(true)
    val valueCodec = new AvroCodec(valueSchemaStr)

    val row = kvDf.collect()(0)
    val valueBytes = row.getAs[Array[Byte]]("value_bytes")
    val decoded = valueCodec.decodeArray(valueBytes)

    // decoded(0) = collapsedIr (struct → Array[Any])
    val collapsed = decoded(0).asInstanceOf[Array[Any]]
    collapsed(0).asInstanceOf[Long] shouldBe 50L // collapsedIr for 30 day window
    collapsed(1).asInstanceOf[Long] shouldBe 20L // collapsedIr for 7 day window

    // decoded(1) = tailHopIrs (list of list of structs)
    val tailHops = decoded(1).asInstanceOf[java.util.List[java.util.List[Array[Any]]]]
    tailHops.size() shouldBe 3

    // inspect the first index - daily
    val dailyResolution = tailHops.get(0)
    dailyResolution.size() shouldBe 1 // only 30day window
    val dailyElement = dailyResolution.get(0)
    dailyElement(0).asInstanceOf[Long] shouldBe 20L
    dailyElement(1).asInstanceOf[Long] shouldBe 1689552000000L // 07-17 2023 12:00:00 AM UTC

    // inspect the second index - hourly
    val hourlyResolution = tailHops.get(1)
    hourlyResolution.size() shouldBe 2 // 2 hourly tail hops for 7 day window
    val firstHourlyElement = hourlyResolution.get(0)
    firstHourlyElement(0).asInstanceOf[Long] shouldBe 20L // added 10 + 10
    firstHourlyElement(1).asInstanceOf[Long] shouldBe 1691575200000L // August 9, 2023 10:00:00 AM

    val secondHourlyElement = hourlyResolution.get(1)
    secondHourlyElement(0).asInstanceOf[Long] shouldBe 10L // single 10
    secondHourlyElement(1).asInstanceOf[Long] shouldBe 1691578800000L // August 9, 2023 11:00:00 AM

    // inspect the third index - five minute
    tailHops.get(2).size() shouldBe 0
  }

}
