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

package ai.chronon.spark.fetcher

import ai.chronon.api._
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.online.serde.SparkConversions
import ai.chronon.online.fetcher.Fetcher.Request
import ai.chronon.online.fetcher.{FetchContext, MetadataStore}
import ai.chronon.spark.Extensions._
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.utils.{MockApi, OnlineUtils, SparkTestBase}
import org.apache.spark.sql.Row
import org.junit.Assert.assertEquals
import org.slf4j.{Logger, LoggerFactory}

import java.util.{Arrays => JArrays, TimeZone}
import scala.concurrent.Await
import scala.concurrent.duration.{Duration, SECONDS}

/**
 * Minimal fetcher test using static (hand-written) data instead of DataFrameGen.
 * One SNAPSHOT GroupBy with LAST and HISTOGRAM aggregations.
 * Useful for debugging the offline→online comparison with full data visibility.
 */
class FetcherStaticTest extends SparkTestBase {

  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  private val tableUtils = TableUtils(spark)
  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

  def toTs(arg: String): Long = TsUtils.datetimeToTs(arg)

  it should "test snapshot vendor_ratings aggs with static data - 2 days" in {
    runVendorRatingsTest("static_fetch_test_2d", endDs = "2021-04-10", numSourceDays = 2)
  }

  it should "test snapshot vendor_ratings aggs with static data - 3 days" in {
    runVendorRatingsTest("static_fetch_test_3d", endDs = "2021-04-11", numSourceDays = 3)
  }

  it should "test snapshot vendor_ratings aggs with static data - 4 days" in {
    runVendorRatingsTest("static_fetch_test_4d", endDs = "2021-04-12", numSourceDays = 4)
  }

  it should "test snapshot vendor_ratings aggs with keyMapping (vendor_id -> vendor)" in {
    runVendorRatingsTestWithKeyMapping("static_fetch_keymapping", endDs = "2021-04-10", numSourceDays = 2)
  }

  // Reproduces the FetcherGeneratedTest pattern: join with two join parts (vendor-keyed + user-keyed),
  // left table has both vendor_id and user_id, some rows have user_id=null.
  // Tests that vendor histogram is non-null online even when user_id is null in the request.
  it should "test vendor histogram online is non-null when user_id key is null in request" in {
    runNullKeyTest("static_fetch_null_key", endDs = "2021-04-10")
  }

  it should "fetch join online using selected keys" in {
    val namespace = "static_fetch_selected_keys"
    val endDs = "2021-04-10"
    SparkTestBase.createDatabase(spark, namespace)

    val sourceSchema = StructType(
      s"selected_key_events_$namespace",
      Array(
        StructField("user_id",     LongType),
        StructField("query",       StringType),
        StructField("event_count", IntType),
        StructField("ts",          LongType),
        StructField("ds",          StringType)
      )
    )
    val sourceData = Seq(
      Row(Long.box(1234567890123L), "Shoes", 2, toTs("2021-04-08 10:00:00"), "2021-04-09"),
      Row(Long.box(1234567890123L), "shoes", 5, toTs("2021-04-08 11:00:00"), "2021-04-09"),
      Row(Long.box(124L),           "shoes", 3, toTs("2021-04-08 12:00:00"), "2021-04-09")
    )
    spark
      .createDataFrame(sourceData.toJava, SparkConversions.fromChrononSchema(sourceSchema))
      .save(s"$namespace.${sourceSchema.name}")

    val querySchema = StructType(
      s"selected_key_queries_$namespace",
      Array(
        StructField("user_id", LongType),
        StructField("query",   StringType),
        StructField("ts",      LongType),
        StructField("ds",      StringType)
      )
    )
    val queryData = Seq(Row(Long.box(1234567890123L), "SHOES", toTs(s"$endDs 09:00:00"), endDs))
    spark
      .createDataFrame(queryData.toJava, SparkConversions.fromChrononSchema(querySchema))
      .save(s"$namespace.${querySchema.name}")

    val bucketSelect = "cast(user_id % 100 as int)"
    val groupBy = Builders.GroupBy(
      sources = Seq(
        Builders.Source.events(
          query = Builders.Query(
            selects = Map("bucket" -> bucketSelect, "query" -> "lower(query)", "event_count" -> "event_count"),
            startPartition = "2021-04-09"
          ),
          table = s"$namespace.${sourceSchema.name}"
        )
      ),
      keyColumns = Seq("bucket", "query"),
      aggregations = Seq(
        Builders.Aggregation(
          operation = Operation.SUM,
          inputColumn = "event_count",
          windows = Seq(new Window(30, TimeUnit.DAYS))
        )
      ),
      metaData = Builders.MetaData(name = s"unit_test.selected_keys_$namespace", namespace = namespace, team = "chronon"),
      accuracy = Accuracy.SNAPSHOT
    )

    val queryNormalizedGroupBy = Builders.GroupBy(
      sources = Seq(
        Builders.Source.events(
          query = Builders.Query(
            selects = Map("query_normalized" -> "lower(query)", "event_count" -> "event_count"),
            startPartition = "2021-04-09"
          ),
          table = s"$namespace.${sourceSchema.name}"
        )
      ),
      keyColumns = Seq("query_normalized"),
      aggregations = Seq(
        Builders.Aggregation(
          operation = Operation.SUM,
          inputColumn = "event_count",
          windows = Seq(new Window(30, TimeUnit.DAYS))
        )
      ),
      metaData =
        Builders.MetaData(name = s"unit_test.selected_query_normalized_$namespace", namespace = namespace, team = "chronon"),
      accuracy = Accuracy.SNAPSHOT
    )

    val joinConf = Builders.Join(
      left = Builders.Source.events(
        query = Builders.Query(
          selects = Map("bucket" -> bucketSelect, "query" -> "lower(query)", "query_normalized" -> "lower(query)"),
          startPartition = endDs
        ),
        table = s"$namespace.${querySchema.name}"
      ),
      joinParts = Seq(
        Builders.JoinPart(groupBy = groupBy, keyMapping = Map("bucket" -> "bucket", "query" -> "query"))
          .setUseLongNames(false),
        Builders.JoinPart(groupBy = queryNormalizedGroupBy, keyMapping = Map("query_normalized" -> "query_normalized"))
      ),
      metaData =
        Builders.MetaData(name = s"unit_test.selected_keys_join_$namespace", namespace = namespace, team = "chronon")
    )

    val storeName = s"FetcherStaticTest_$namespace"
    val kvStoreFunc = () => OnlineUtils.buildInMemoryKVStore(storeName)
    val inMemoryKvStore = kvStoreFunc()
    OnlineUtils.serve(tableUtils, inMemoryKvStore, kvStoreFunc, namespace, endDs, groupBy, dropDsOnWrite = false)
    OnlineUtils.serve(tableUtils, inMemoryKvStore, kvStoreFunc, namespace, endDs, queryNormalizedGroupBy, dropDsOnWrite = false)

    val metadataStore = new MetadataStore(FetchContext(inMemoryKvStore))
    inMemoryKvStore.create(Constants.MetadataDataset)
    Await.result(metadataStore.putJoinConf(joinConf), Duration(30, SECONDS))
    assertEquals(Some(LongType), metadataStore.getGroupByServingInfo(groupBy.metaData.name).get.inputChrononSchema.typeOf(Constants.TimeColumn))
    assertEquals(
      Some(LongType),
      metadataStore.getGroupByServingInfo(queryNormalizedGroupBy.metaData.name).get.inputChrononSchema.typeOf(Constants.TimeColumn)
    )

    val fetcher = new MockApi(kvStoreFunc, namespace).buildFetcher(debug = true)
    val joinSchema = fetcher.fetchJoinSchema(joinConf.metaData.name).get
    assert(joinSchema.keySchema.contains("\"name\":\"user_id\",\"type\":[\"null\",\"long\"]"), joinSchema.keySchema)
    assert(joinSchema.keySchema.contains("\"name\":\"bucket\",\"type\":[\"null\",\"int\"]"), joinSchema.keySchema)
    assert(joinSchema.keySchema.contains("\"name\":\"query\",\"type\":[\"null\",\"string\"]"), joinSchema.keySchema)
    assert(joinSchema.keySchema.contains("\"name\":\"query_normalized\",\"type\":[\"null\",\"string\"]"), joinSchema.keySchema)
    assert(joinSchema.valueInfos.exists(_.leftKeys.toSet == Set("user_id", "query")))
    assert(joinSchema.valueInfos.exists(_.leftKeys.toSet == Set("query")))

    def sum30dValues(values: Map[String, AnyRef]): Map[String, Long] = {
      val matches = values.collect {
        case (name, value: java.lang.Number) if name.endsWith("event_count_sum_30d") => name -> value.longValue()
      }
      assertEquals(s"Expected two event_count_sum_30d features in [${values.keys.mkString(", ")}]", 2, matches.size)
      assert(matches.keys.exists(_.contains("query_normalized")), matches.keys.mkString(", "))
      matches
    }

    def assertSelectedKeyValues(values: Map[String, AnyRef]): Unit = {
      val sums = sum30dValues(values)
      assertEquals(Some(7L), sums.find { case (name, _) => !name.contains("query_normalized") }.map(_._2))
      assertEquals(Some(10L), sums.find { case (name, _) => name.contains("query_normalized") }.map(_._2))
    }

    val requestTs = toTs(s"$endDs 09:00:00")
    val rawKeyResponse = Await.result(
      fetcher.fetchJoin(
        Seq(Request(
          joinConf.metaData.name,
          Map("user_id" -> Long.box(1234567890123L), "query" -> "SHOES"),
          Some(requestTs)))),
      Duration(30, SECONDS)
    ).head
    assert(rawKeyResponse.values.isSuccess, rawKeyResponse.values.failed.map(_.getMessage).getOrElse(""))
    assertSelectedKeyValues(rawKeyResponse.values.get)

    val directKeyResponse = Await.result(
      fetcher.fetchJoin(
        Seq(Request(
          joinConf.metaData.name,
          Map("bucket" -> Int.box(23), "query" -> "shoes", "query_normalized" -> "shoes"),
          Some(requestTs)))),
      Duration(30, SECONDS)
    ).head
    assert(directKeyResponse.values.isSuccess, directKeyResponse.values.failed.map(_.getMessage).getOrElse(""))
    assertSelectedKeyValues(directKeyResponse.values.get)
  }

  /**
   * Builds a vendor_ratings SNAPSHOT GroupBy test with `numSourceDays` days of data.
   *
   * Source events use day-stamped txn_types tokens (e.g. "d7_a" for April 7th) so that
   * the histogram result tells you exactly which days' events are included.
   * An off-by-one alignment error would show the wrong day's tokens.
   *
   * Day N events live in ds = April (7+N), with ts = April (6+N) 10:00:00.
   *   numSourceDays=2 → events on Apr 7+Apr 8, queries on Apr 10 (endDs), prevDs Apr 9
   *   numSourceDays=3 → adds Apr 9 events, queries on Apr 11 (endDs), prevDs Apr 10
   *   numSourceDays=4 → adds Apr 10 events, queries on Apr 12 (endDs), prevDs Apr 11
   */
  // Same as runVendorRatingsTest but the left query table uses "vendor_id" with keyMapping = {"vendor_id" -> "vendor"},
  // matching the pattern in FetcherGeneratedTest.
  private def runVendorRatingsTestWithKeyMapping(namespace: String, endDs: String, numSourceDays: Int): Unit = {
    SparkTestBase.createDatabase(spark, namespace)

    val baseDay = 7
    val sourceData: Seq[Row] = (0 until numSourceDays).flatMap { i =>
      val eventDay  = baseDay + i
      val dsDay     = baseDay + i + 1
      val eventDate = f"2021-04-$eventDay%02d"
      val dsDate    = f"2021-04-$dsDay%02d"
      val tokens    = java.util.Arrays.asList(s"d${eventDay}_a", s"d${eventDay}_b")
      Seq(
        Row("user1", "vendor1", 4, "bucket_a", tokens, toTs(s"$eventDate 10:00:00"), dsDate),
        Row("user2", "vendor1", 2, "bucket_b", tokens, toTs(s"$eventDate 12:00:00"), dsDate),
        Row("user3", "vendor2", 3, "bucket_a", tokens, toTs(s"$eventDate 11:00:00"), dsDate),
        Row("user4", "vendor2", 5, "bucket_b", tokens, toTs(s"$eventDate 13:00:00"), dsDate)
      )
    }

    val sourceSchema = StructType(
      s"vendor_ratings_src_$namespace",
      Array(
        StructField("user",      StringType),
        StructField("vendor",    StringType),
        StructField("rating",    IntType),
        StructField("bucket",    StringType),
        StructField("txn_types", ListType(StringType)),
        StructField("ts",        LongType),
        StructField("ds",        StringType)
      )
    )

    spark
      .createDataFrame(sourceData.toJava, SparkConversions.fromChrononSchema(sourceSchema))
      .save(s"$namespace.${sourceSchema.name}")

    // Left query table uses "vendor_id" instead of "vendor"
    val queryData = Seq(
      Row("vendor1", toTs(s"${endDs} 09:00:00"), endDs),
      Row("vendor2", toTs(s"${endDs} 12:00:00"), endDs)
    )

    val querySchema = StructType(
      s"query_events_$namespace",
      Array(
        StructField("vendor_id", StringType),
        StructField("ts",        LongType),
        StructField("ds",        StringType)
      )
    )

    spark
      .createDataFrame(queryData.toJava, SparkConversions.fromChrononSchema(querySchema))
      .save(s"$namespace.${querySchema.name}")

    val vendorGroupBy = Builders.GroupBy(
      sources = Seq(
        Builders.Source.events(
          query = Builders.Query(startPartition = "2021-04-07"),
          table = s"$namespace.${sourceSchema.name}"
        )
      ),
      keyColumns = Seq("vendor"),
      aggregations = Seq(
        Builders.Aggregation(
          operation = Operation.AVERAGE,
          inputColumn = "rating",
          windows = Seq(new Window(2, TimeUnit.DAYS), new Window(30, TimeUnit.DAYS)),
          buckets = Seq("bucket")
        ),
        Builders.Aggregation(
          operation = Operation.HISTOGRAM,
          inputColumn = "txn_types",
          windows = Seq(new Window(3, TimeUnit.DAYS))
        ),
        Builders.Aggregation(
          operation = Operation.LAST_K,
          argMap = Map("k" -> "300"),
          inputColumn = "user",
          windows = Seq(new Window(2, TimeUnit.DAYS), new Window(30, TimeUnit.DAYS))
        )
      ),
      metaData = Builders.MetaData(name = s"unit_test.vendor_ratings_$namespace", namespace = namespace, team = "chronon"),
      accuracy = Accuracy.SNAPSHOT
    )

    val joinConf = Builders.Join(
      left = Builders.Source.events(
        query = Builders.Query(startPartition = endDs),
        table = s"$namespace.${querySchema.name}"
      ),
      joinParts = Seq(
        Builders.JoinPart(groupBy = vendorGroupBy, keyMapping = Map("vendor_id" -> "vendor")).setUseLongNames(false)
      ),
      metaData = Builders.MetaData(name = s"unit_test.vendor_ratings_join_$namespace", namespace = namespace, team = "chronon")
    )

    FetcherTestUtil.compareTemporalFetch(
      joinConf,
      endDs = endDs,
      namespace = namespace,
      consistencyCheck = false,
      dropDsOnWrite = false
    )(spark)
  }

  /**
   * Two join parts: vendorRatings (keyed on vendor, keyMapping vendor_id→vendor) and userPings
   * (keyed on user_id). Left query table has vendor_id + user_id; some rows have user_id=null.
   * Verifies that vendor histogram is non-null online for rows where vendor_id is non-null,
   * regardless of whether user_id is null.
   */
  private def runNullKeyTest(namespace: String, endDs: String): Unit = {
    SparkTestBase.createDatabase(spark, namespace)

    val numSourceDays = 2
    val baseDay = 7

    // Source events for vendorRatings GroupBy
    val ratingsData: Seq[Row] = (0 until numSourceDays).flatMap { i =>
      val eventDay  = baseDay + i
      val dsDay     = baseDay + i + 1
      val eventDate = f"2021-04-$eventDay%02d"
      val dsDate    = f"2021-04-$dsDay%02d"
      val tokens    = JArrays.asList(s"d${eventDay}_a", s"d${eventDay}_b")
      Seq(
        Row("user1", "vendor1", 4, "bucket_a", tokens, toTs(s"$eventDate 10:00:00"), dsDate),
        Row("user2", "vendor1", 2, "bucket_b", tokens, toTs(s"$eventDate 12:00:00"), dsDate),
        Row("user3", "vendor2", 3, "bucket_a", tokens, toTs(s"$eventDate 11:00:00"), dsDate),
        Row("user4", "vendor2", 5, "bucket_b", tokens, toTs(s"$eventDate 13:00:00"), dsDate)
      )
    }

    val ratingsSchema = StructType(
      s"ratings_src_$namespace",
      Array(
        StructField("user",      StringType),
        StructField("vendor",    StringType),
        StructField("rating",    IntType),
        StructField("bucket",    StringType),
        StructField("txn_types", ListType(StringType)),
        StructField("ts",        LongType),
        StructField("ds",        StringType)
      )
    )
    spark
      .createDataFrame(ratingsData.toJava, SparkConversions.fromChrononSchema(ratingsSchema))
      .save(s"$namespace.${ratingsSchema.name}")

    // Source events for userPings GroupBy (minimal — just to make user_id a join key)
    val pingsData: Seq[Row] = Seq(
      Row("user1", toTs(s"2021-04-08 08:00:00"), "2021-04-09"),
      Row("user2", toTs(s"2021-04-08 09:00:00"), "2021-04-09")
    )
    val pingsSchema = StructType(
      s"pings_src_$namespace",
      Array(
        StructField("user_id", StringType),
        StructField("ts",      LongType),
        StructField("ds",      StringType)
      )
    )
    spark
      .createDataFrame(pingsData.toJava, SparkConversions.fromChrononSchema(pingsSchema))
      .save(s"$namespace.${pingsSchema.name}")

    // Left query table: some rows have user_id=null, vendor_id always non-null
    val queryData = Seq(
      Row("user1",  "vendor1", toTs(s"${endDs} 09:00:00"), endDs),
      Row(null,     "vendor1", toTs(s"${endDs} 10:00:00"), endDs),
      Row("user2",  "vendor2", toTs(s"${endDs} 11:00:00"), endDs),
      Row(null,     "vendor2", toTs(s"${endDs} 12:00:00"), endDs)
    )
    val querySchema = StructType(
      s"query_events_$namespace",
      Array(
        StructField("user_id",   StringType),
        StructField("vendor_id", StringType),
        StructField("ts",        LongType),
        StructField("ds",        StringType)
      )
    )
    spark
      .createDataFrame(queryData.toJava, SparkConversions.fromChrononSchema(querySchema))
      .save(s"$namespace.${querySchema.name}")

    val vendorRatingsGroupBy = Builders.GroupBy(
      sources = Seq(
        Builders.Source.events(
          query = Builders.Query(startPartition = "2021-04-07"),
          table = s"$namespace.${ratingsSchema.name}"
        )
      ),
      keyColumns = Seq("vendor"),
      aggregations = Seq(
        Builders.Aggregation(
          operation = Operation.AVERAGE,
          inputColumn = "rating",
          windows = Seq(new Window(2, TimeUnit.DAYS), new Window(30, TimeUnit.DAYS)),
          buckets = Seq("bucket")
        ),
        Builders.Aggregation(
          operation = Operation.SKEW,
          inputColumn = "rating",
          windows = Seq(new Window(2, TimeUnit.DAYS), new Window(30, TimeUnit.DAYS)),
          buckets = Seq("bucket")
        ),
        Builders.Aggregation(
          operation = Operation.HISTOGRAM,
          inputColumn = "txn_types",
          windows = Seq(new Window(3, TimeUnit.DAYS))
        ),
        Builders.Aggregation(
          operation = Operation.APPROX_FREQUENT_K,
          inputColumn = "txn_types",
          windows = Seq(new Window(3, TimeUnit.DAYS))
        ),
        Builders.Aggregation(
          operation = Operation.LAST_K,
          argMap = Map("k" -> "300"),
          inputColumn = "user",
          windows = Seq(new Window(2, TimeUnit.DAYS), new Window(30, TimeUnit.DAYS))
        )
      ),
      metaData = Builders.MetaData(name = s"unit_test.vendor_ratings_$namespace", namespace = namespace, team = "chronon"),
      accuracy = Accuracy.SNAPSHOT
    )

    // Minimal GroupBy keyed on user_id — its presence in the join makes user_id a left key,
    // meaning the fetch request will contain {user_id: ..., vendor_id: ...}.
    val userPingsGroupBy = Builders.GroupBy(
      sources = Seq(
        Builders.Source.events(
          query = Builders.Query(startPartition = "2021-04-07"),
          table = s"$namespace.${pingsSchema.name}"
        )
      ),
      keyColumns = Seq("user_id"),
      aggregations = Seq(
        Builders.Aggregation(
          operation = Operation.COUNT,
          inputColumn = "user_id",
          windows = Seq(new Window(7, TimeUnit.DAYS))
        )
      ),
      metaData = Builders.MetaData(name = s"unit_test.user_pings_$namespace", namespace = namespace, team = "chronon"),
      accuracy = Accuracy.SNAPSHOT
    )

    val joinConf = Builders.Join(
      left = Builders.Source.events(
        query = Builders.Query(startPartition = endDs),
        table = s"$namespace.${querySchema.name}"
      ),
      joinParts = Seq(
        Builders.JoinPart(groupBy = vendorRatingsGroupBy, keyMapping = Map("vendor_id" -> "vendor")).setUseLongNames(false),
        Builders.JoinPart(groupBy = userPingsGroupBy).setUseLongNames(false)
      ),
      metaData = Builders.MetaData(name = s"unit_test.null_key_join_$namespace", namespace = namespace, team = "chronon")
    )

    FetcherTestUtil.compareTemporalFetch(
      joinConf,
      endDs = endDs,
      namespace = namespace,
      consistencyCheck = false,
      dropDsOnWrite = false
    )(spark)
  }

  private def runVendorRatingsTest(namespace: String, endDs: String, numSourceDays: Int): Unit = {
    SparkTestBase.createDatabase(spark, namespace)

    // Tokens are unique per event day so the histogram reveals which days landed.
    // ts is at 10:00 UTC on the event day; ds is the NEXT day (Chronon convention: event
    // processed the following partition).
    val baseDay = 7 // April 7 = day index 0
    val sourceData: Seq[Row] = (0 until numSourceDays).flatMap { i =>
      val eventDay  = baseDay + i           // 7, 8, 9, 10
      val dsDay     = baseDay + i + 1       // 8, 9, 10, 11  (partition day)
      val eventDate = f"2021-04-$eventDay%02d"
      val dsDate    = f"2021-04-$dsDay%02d"
      val tokens    = JArrays.asList(s"d${eventDay}_a", s"d${eventDay}_b")
      Seq(
        Row("user1", "vendor1", 4, "bucket_a", tokens, toTs(s"$eventDate 10:00:00"), dsDate),
        Row("user2", "vendor1", 2, "bucket_b", tokens, toTs(s"$eventDate 12:00:00"), dsDate),
        Row("user3", "vendor2", 3, "bucket_a", tokens, toTs(s"$eventDate 11:00:00"), dsDate),
        Row("user4", "vendor2", 5, "bucket_b", tokens, toTs(s"$eventDate 13:00:00"), dsDate)
      )
    }

    val sourceSchema = StructType(
      s"vendor_ratings_src_$namespace",
      Array(
        StructField("user",      StringType),
        StructField("vendor",    StringType),
        StructField("rating",    IntType),
        StructField("bucket",    StringType),
        StructField("txn_types", ListType(StringType)),
        StructField("ts",        LongType),
        StructField("ds",        StringType)
      )
    )

    spark
      .createDataFrame(sourceData.toJava, SparkConversions.fromChrononSchema(sourceSchema))
      .save(s"$namespace.${sourceSchema.name}")

    // Query events: one query per vendor on endDs.
    val queryData = Seq(
      Row("vendor1", toTs(s"${endDs} 09:00:00"), endDs),
      Row("vendor2", toTs(s"${endDs} 12:00:00"), endDs)
    )

    val querySchema = StructType(
      s"query_events_$namespace",
      Array(
        StructField("vendor", StringType),
        StructField("ts",     LongType),
        StructField("ds",     StringType)
      )
    )

    spark
      .createDataFrame(queryData.toJava, SparkConversions.fromChrononSchema(querySchema))
      .save(s"$namespace.${querySchema.name}")

    val vendorGroupBy = Builders.GroupBy(
      sources = Seq(
        Builders.Source.events(
          query = Builders.Query(startPartition = "2021-04-07"),
          table = s"$namespace.${sourceSchema.name}"
        )
      ),
      keyColumns = Seq("vendor"),
      aggregations = Seq(
        Builders.Aggregation(
          operation = Operation.AVERAGE,
          inputColumn = "rating",
          windows = Seq(new Window(2, TimeUnit.DAYS), new Window(30, TimeUnit.DAYS)),
          buckets = Seq("bucket")
        ),
        Builders.Aggregation(
          operation = Operation.SKEW,
          inputColumn = "rating",
          windows = Seq(new Window(2, TimeUnit.DAYS), new Window(30, TimeUnit.DAYS)),
          buckets = Seq("bucket")
        ),
        Builders.Aggregation(
          operation = Operation.HISTOGRAM,
          inputColumn = "txn_types",
          windows = Seq(new Window(3, TimeUnit.DAYS))
        ),
        Builders.Aggregation(
          operation = Operation.APPROX_FREQUENT_K,
          inputColumn = "txn_types",
          windows = Seq(new Window(3, TimeUnit.DAYS))
        ),
        Builders.Aggregation(
          operation = Operation.LAST_K,
          argMap = Map("k" -> "300"),
          inputColumn = "user",
          windows = Seq(new Window(2, TimeUnit.DAYS), new Window(30, TimeUnit.DAYS))
        )
      ),
      metaData = Builders.MetaData(name = s"unit_test.vendor_ratings_$namespace", namespace = namespace, team = "chronon"),
      accuracy = Accuracy.SNAPSHOT
    )

    val joinConf = Builders.Join(
      left = Builders.Source.events(
        query = Builders.Query(startPartition = endDs),
        table = s"$namespace.${querySchema.name}"
      ),
      joinParts = Seq(
        Builders.JoinPart(groupBy = vendorGroupBy).setUseLongNames(false)
      ),
      metaData = Builders.MetaData(name = s"unit_test.vendor_ratings_join_$namespace", namespace = namespace, team = "chronon")
    )

    FetcherTestUtil.compareTemporalFetch(
      joinConf,
      endDs = endDs,
      namespace = namespace,
      consistencyCheck = false,
      dropDsOnWrite = false
    )(spark)
  }
}
