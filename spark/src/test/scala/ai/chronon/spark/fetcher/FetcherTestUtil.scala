package ai.chronon.spark.fetcher

import ai.chronon.aggregator.test.Column
import ai.chronon.api
import ai.chronon.api.Extensions._
import ai.chronon.api.Constants.MetadataDataset
import ai.chronon.api.Builders.Derivation
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.api._
import ai.chronon.online.fetcher.{FetchContext, MetadataStore}
import ai.chronon.online.fetcher.Fetcher.{Request, Response, ResponseV2}
import ai.chronon.online.fetcher.FeaturesResponseType
import ai.chronon.online.serde.AvroCodec
import org.apache.avro.generic.GenericRecord
import ai.chronon.online.serde.SparkConversions
import ai.chronon.online._
import ai.chronon.spark.Extensions._
import ai.chronon.spark.batch.ModularMonolith
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.stats.ConsistencyJob
import ai.chronon.spark.utils.{DataFrameGen, MockApi, OnlineUtils, SchemaEvolutionUtils, SparkTestBase}
import ai.chronon.spark.{Join => _, _}
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.functions.{avg, col, lit}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.Executors
import java.{lang, util}

import scala.compat.java8.FutureConverters
import scala.concurrent.duration.{Duration, SECONDS}
import scala.concurrent.{Await, ExecutionContext}
import scala.util.{Failure, Success}

object FetcherTestUtil {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  def joinResponses(spark: SparkSession,
                    requests: Array[Request],
                    mockApi: MockApi,
                    useJavaFetcher: Boolean = false,
                    runCount: Int = 1,
                    samplePercent: Double = -1,
                    logToHive: Boolean = false,
                    debug: Boolean = false)(implicit ec: ExecutionContext): (List[Response], DataFrame) = {
    val chunkSize = 100
    @transient lazy val fetcher = mockApi.buildFetcher(debug)
    @transient lazy val javaFetcher = mockApi.buildJavaFetcher()

    def fetchOnce = {
      var latencySum: Long = 0
      var latencyCount = 0
      val blockStart = System.currentTimeMillis()
      val result = requests.iterator
        .grouped(chunkSize)
        .map { oldReqs =>
          // deliberately mis-type a few keys
          val r = oldReqs
            .map(r =>
              r.copy(keys = r.keys.mapValues { v =>
                if (v.isInstanceOf[java.lang.Long]) v.toString else v
                // v
              }.toMap))
          val responses = if (useJavaFetcher) {
            // Converting to java request and using the toScalaRequest functionality to test conversion
            val convertedJavaRequests = r.map(new JavaRequest(_)).toJava
            val javaResponse = javaFetcher.fetchJoin(convertedJavaRequests)
            FutureConverters
              .toScala(javaResponse)
              .map(
                _.toScala.map(jres =>
                  Response(
                    Request(jres.request.name, jres.request.keys.toScala.toMap, Option(jres.request.atMillis)),
                    jres.values.toScala.map(_.toScala)
                  )))
          } else {
            fetcher.fetchJoin(r)
          }

          // fix mis-typed keys in the request
          val fixedResponses =
            responses.map(resps => resps.zip(oldReqs).map { case (resp, req) => resp.copy(request = req) })
          System.currentTimeMillis() -> fixedResponses
        }
        .flatMap { case (start, future) =>
          val result = Await.result(future, Duration(10000, SECONDS)) // todo: change back to millis
          val latency = System.currentTimeMillis() - start
          latencySum += latency
          latencyCount += 1
          result
        }
        .toList
      val latencyMillis = latencySum.toFloat / latencyCount.toFloat
      val qps = (requests.length * 1000.0) / (System.currentTimeMillis() - blockStart).toFloat
      (latencyMillis, qps, result)
    }

    // to overwhelm the profiler with fetching code path
    // so as to make it prominent in the flamegraph & collect enough stats

    var latencySum = 0.0
    var qpsSum = 0.0
    var loggedValues: Seq[LoggableResponseBase64] = null
    var result: List[Response] = null
    (0 until runCount).foreach { _ =>
      val (latency, qps, resultVal) = fetchOnce
      result = resultVal
      loggedValues = mockApi.flushLoggedValues
      latencySum += latency
      qpsSum += qps
    }
    val fetcherNameString = if (useJavaFetcher) "Java" else "Scala"

    logger.info(s"""
                   |Averaging fetching stats for $fetcherNameString Fetcher over ${requests.length} requests $runCount times
                   |with batch size: $chunkSize
                   |average qps: ${qpsSum / runCount}
                   |average latency: ${latencySum / runCount}
                   |""".stripMargin)
    val loggedDf = mockApi.loggedValuesToDf(loggedValues, spark)
    if (logToHive) {
      TableUtils(spark).insertPartitions(
        loggedDf,
        mockApi.logTable,
        partitionColumns = List("ds", "name")
      )
    }
    if (samplePercent > 0) {
      logger.info(s"logged count: ${loggedDf.count()}")
      loggedDf.show()
    }
    result -> loggedDf
  }

  // Compute a join until endDs and compare the result of fetching the aggregations with the computed join values.
  def compareTemporalFetch(joinConf: api.Join,
                           endDs: String,
                           namespace: String,
                           consistencyCheck: Boolean,
                           dropDsOnWrite: Boolean,
                           enableTiling: Boolean = false,
                           partitionSpec: PartitionSpec = PartitionSpec.daily,
                           offlineAssertion: DataFrame => Unit = _ => ())(implicit spark: SparkSession): Unit = {
    implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1))
    implicit val tableUtils: TableUtils = TableUtils(spark, partitionSpec)
    val kvStoreFunc = () => OnlineUtils.buildInMemoryKVStore("FetcherTest")
    val inMemoryKvStore = kvStoreFunc()

    val tilingEnabledFlagStore = new FlagStore {
      override def isSet(flagName: String, attributes: util.Map[String, String]): lang.Boolean = {
        if (flagName == FlagStoreConstants.TILING_ENABLED) {
          enableTiling
        } else {
          false
        }
      }
    }

    val mockApi = new MockApi(kvStoreFunc, namespace)
    mockApi.setFlagStore(tilingEnabledFlagStore)

    // Determine actual date range from the left source table to avoid empty data issues
    // The test may generate data for "today" but then query for a different date
    val leftTable = joinConf.left.table
    val availablePartitions: Seq[String] =
      try {
        tableUtils.partitions(leftTable, Map.empty[String, String])
      } catch {
        case _: Exception => Seq.empty[String]
      }

    val (startDs, actualEndDs) = if (availablePartitions.nonEmpty) {
      // Use the actual partition range that exists in the data
      val sortedPartitions = availablePartitions.sorted
      (sortedPartitions.head, sortedPartitions.last)
    } else {
      // Fallback to endDs if we can't determine partitions
      (endDs, endDs)
    }

    val dateRange: DateRange = new DateRange()
      .setStartDate(startDs)
      .setEndDate(actualEndDs)

    // Run ModularMonolith pipeline
    ModularMonolith.run(joinConf, dateRange)(tableUtils)

    // JoinPlanner writes derivation output to outputTable + "__derived" when derivations are present.
    // Use the derivation table so the offline comparison reflects post-derivation feature values.
    val finalOutputTable =
      if (joinConf.hasDerivations) joinConf.metaData.outputTable + "__derived"
      else joinConf.metaData.outputTable
    val joinedDf = tableUtils.sql(s"SELECT * FROM $finalOutputTable")
    val joinTable = s"$namespace.join_test_expected_${joinConf.metaData.cleanName}"
    logger.info("Printing Joined DataFrame results:")
    joinedDf.show()
    joinedDf.save(joinTable)
    val endDsExpected = tableUtils.sql(s"SELECT * FROM $joinTable WHERE ds='$endDs'")
    offlineAssertion(endDsExpected)

    joinConf.joinParts.toScala.foreach { jp =>
      val groupByPartitionSpec = Option(jp.groupBy.metaData)
        .map(metadata => metadata.partitionSpec(partitionSpec))
        .getOrElse(partitionSpec)
      val groupByEndDs = partitionSpec.translate(endDs, groupByPartitionSpec)
      val groupByTableUtils =
        if (groupByPartitionSpec == partitionSpec) tableUtils else TableUtils(spark, groupByPartitionSpec)
      OnlineUtils.serve(groupByTableUtils,
                        inMemoryKvStore,
                        kvStoreFunc,
                        namespace,
                        groupByEndDs,
                        jp.groupBy,
                        dropDsOnWrite = dropDsOnWrite,
                        tilingEnabled = enableTiling)
    }

    // Extract queries for the EndDs. Filter by ds to avoid including future-dated events produced by
    // ModularMonolith (which computes the full date range), which would have no matching row in
    // endDsExpected (filtered to ds='$endDs').
    val endDsEvents = {
      tableUtils.sql(
        s"SELECT * FROM $joinTable WHERE ds='$endDs' AND ts >= unix_timestamp('$endDs', '${tableUtils.partitionSpec.format}')")
    }
    // Keep only left-side columns (keys, ts, ds) and drop all feature columns
    val keys = joinConf.leftKeyCols
    val leftSideColumns = keys ++ Array(Constants.TimeColumn, tableUtils.partitionColumn)
    val columnsToKeep = endDsEvents.schema.fieldNames.filter(leftSideColumns.contains)
    val endDsQueries = endDsEvents.select(columnsToKeep.map(col): _*)
    val keyIndices = keys.map(endDsQueries.schema.fieldIndex)
    val tsIndex = endDsQueries.schema.fieldIndex(Constants.TimeColumn)
    val metadataStore = new MetadataStore(FetchContext(inMemoryKvStore))
    inMemoryKvStore.create(MetadataDataset)
    metadataStore.putJoinConf(joinConf)

    def buildRequests(lagMs: Int = 0): Array[Request] =
      endDsQueries
        .collect()
        .map { row =>
          val keyMap = keyIndices.indices.map { idx =>
            keys(idx) -> row.get(keyIndices(idx)).asInstanceOf[AnyRef]
          }.toMap
          val ts = row.get(tsIndex).asInstanceOf[Long]
          Request(joinConf.metaData.name, keyMap, Some(ts - lagMs))
        }

    val requests = buildRequests()

    if (consistencyCheck) {
      val lagMs = -100000
      val laggedRequests = buildRequests(lagMs)
      val laggedResponseDf =
        FetcherTestUtil.joinResponses(spark, laggedRequests, mockApi, samplePercent = 5, logToHive = true)._2
      val correctedLaggedResponse = laggedResponseDf
        .withColumn("ts_lagged", laggedResponseDf.col("ts_millis") + lagMs)
        .withColumn("ts_millis", col("ts_lagged"))
        .drop("ts_lagged")
      logger.info("corrected lagged response")
      correctedLaggedResponse.save(mockApi.logTable, partitionColumns = Seq(tableUtils.partitionColumn, "name"))

      // build flattened log table
      val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
      SchemaEvolutionUtils.runLogSchemaGroupBy(mockApi, today, today)
      val flattenerJob = new LogFlattenerJob(spark, joinConf, today, mockApi.logTable, mockApi.schemaTable)
      flattenerJob.buildLogTable(Option(today))

      // build consistency metrics
      val consistencyJob = new ConsistencyJob(spark, joinConf, today)
      val metrics = consistencyJob.buildConsistencyMetrics()
      logger.info(s"ooc metrics: $metrics".stripMargin)
      OnlineUtils.serveConsistency(tableUtils, inMemoryKvStore, today, joinConf)
    }
    // benchmark
    FetcherTestUtil.joinResponses(spark, requests, mockApi, runCount = 5, useJavaFetcher = true)
    FetcherTestUtil.joinResponses(spark, requests, mockApi, runCount = 5)

    // comparison
    val columns = endDsExpected.schema.fields.map(_.name)
    val responseRows: Seq[Row] =
      FetcherTestUtil.joinResponses(spark, requests, mockApi, useJavaFetcher = true, debug = true)._1.map { res =>
        val all: Map[String, AnyRef] =
          res.request.keys ++
            res.values.get ++
            Map(tableUtils.partitionColumn -> tableUtils.partitionSpec.at(System.currentTimeMillis())) ++
            Map(Constants.TimeColumn -> lang.Long.valueOf(res.request.atMillis.get))
        val values: Array[Any] = columns.map(all.get(_).orNull)
        SparkConversions
          .toSparkRow(values, StructType.from("record", SparkConversions.toChrononSchema(endDsExpected.schema)))
          .asInstanceOf[GenericRow]
      }

    logger.info(endDsExpected.schema.pretty)

    val keyishColumns = keys.toList ++ List(tableUtils.partitionColumn, Constants.TimeColumn)
    var responseDf =
      tableUtils.sparkSession.createDataFrame(java.util.Arrays.asList(responseRows.toSeq: _*), endDsExpected.schema)
    val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
    if (endDs != today) {
      responseDf = responseDf.drop("ds").withColumn("ds", lit(endDs))
    }
    logger.info("expected:")
    endDsExpected.show()
    logger.info("response:")
    responseDf.show()

    val diff = Comparison.sideBySide(responseDf, endDsExpected, keyishColumns, aName = "online", bName = "offline")
    assert(endDsQueries.count() == responseDf.count())
    if (diff.count() > 0) {
      logger.info("queries:")
      endDsQueries.show()
      logger.info(s"Total count: ${responseDf.count()}")
      logger.info(s"Diff count: ${diff.count()}")
      logger.info("diff result rows:")
      diff
        .withTimeFormattedColumn("ts_string", "ts", "yy-MM-dd HH:mm")
        .select("ts_string", diff.schema.fieldNames: _*)
        .show()
    }
    assert(diff.count() == 0)
  }

  /** Generate deterministic data for testing and checkpointing IRs and streaming data.
    */
  def generateMutationData(namespace: String, tableUtils: TableUtils, spark: SparkSession): api.Join = {
    SparkTestBase.createDatabase(spark, namespace)
    def toTs(arg: String): Long = TsUtils.datetimeToTs(arg)
    val eventData = Seq(
      Row(595125622443733822L, toTs("2021-04-10 09:00:00"), "2021-04-10"),
      Row(595125622443733822L, toTs("2021-04-10 23:00:00"), "2021-04-10"), // Query for added event
      Row(595125622443733822L, toTs("2021-04-10 23:45:00"), "2021-04-10"), // Query for mutated event
      Row(1L, toTs("2021-04-10 00:10:00"), "2021-04-10"), // query for added event
      Row(1L, toTs("2021-04-10 03:10:00"), "2021-04-10") // query for mutated event
    )
    val snapshotData = Seq(
      Row(1L, toTs("2021-04-04 00:30:00"), 4, "2021-04-08"),
      Row(1L, toTs("2021-04-04 12:30:00"), 4, "2021-04-08"),
      Row(1L, toTs("2021-04-05 00:30:00"), 4, "2021-04-08"),
      Row(1L, toTs("2021-04-08 02:30:00"), 4, "2021-04-08"),
      Row(595125622443733822L, toTs("2021-04-04 01:40:00"), 3, "2021-04-08"),
      Row(595125622443733822L, toTs("2021-04-05 03:40:00"), 3, "2021-04-08"),
      Row(595125622443733822L, toTs("2021-04-06 03:45:00"), 4, "2021-04-08"),
      // {listing_id, ts, rating, ds}
      Row(1L, toTs("2021-04-04 00:30:00"), 4, "2021-04-09"),
      Row(1L, toTs("2021-04-04 12:30:00"), 4, "2021-04-09"),
      Row(1L, toTs("2021-04-05 00:30:00"), 4, "2021-04-09"),
      Row(1L, toTs("2021-04-08 02:30:00"), 4, "2021-04-09"),
      Row(595125622443733822L, toTs("2021-04-04 01:40:00"), 3, "2021-04-09"),
      Row(595125622443733822L, toTs("2021-04-05 03:40:00"), 3, "2021-04-09"),
      Row(595125622443733822L, toTs("2021-04-06 03:45:00"), 4, "2021-04-09"),
      Row(595125622443733822L, toTs("2021-04-09 05:45:00"), 5, "2021-04-09")
    )
    val mutationData = Seq(
      Row(595125622443733822L, toTs("2021-04-09 05:45:00"), 2, "2021-04-09", toTs("2021-04-09 05:45:00"), false),
      Row(595125622443733822L, toTs("2021-04-09 05:45:00"), 2, "2021-04-09", toTs("2021-04-09 07:00:00"), true),
      Row(595125622443733822L, toTs("2021-04-09 05:45:00"), 5, "2021-04-09", toTs("2021-04-09 07:00:00"), false),
      // {listing_id, ts, rating, ds, mutation_ts, is_before}
      Row(1L, toTs("2021-04-10 00:30:00"), 5, "2021-04-10", toTs("2021-04-10 00:30:00"), false),
      Row(1L, toTs("2021-04-10 00:30:00"), 5, "2021-04-10", toTs("2021-04-10 02:30:00"), true), // mutation delete event
      Row(595125622443733822L, toTs("2021-04-10 10:00:00"), 4, "2021-04-10", toTs("2021-04-10 10:00:00"), false),
      Row(595125622443733822L, toTs("2021-04-10 10:00:00"), 4, "2021-04-10", toTs("2021-04-10 23:30:00"), true),
      Row(595125622443733822L, toTs("2021-04-10 10:00:00"), 3, "2021-04-10", toTs("2021-04-10 23:30:00"), false)
    )
    // Schemas
    val snapshotSchema = StructType(
      "listing_ratings_snapshot_fetcher",
      Array(StructField("listing", LongType),
            StructField("ts", LongType),
            StructField("rating", IntType),
            StructField("ds", StringType))
    )

    // {..., mutation_ts (timestamp of mutation), is_before (previous value or the updated value),...}
    // Change the names to make sure mappings work properly
    val mutationSchema = StructType(
      "listing_ratings_mutations_fetcher",
      snapshotSchema.fields ++ Seq(
        StructField("mutation_time", LongType),
        StructField("is_before_reversal", BooleanType)
      )
    )

    // {..., event (generic event column), ...}
    val eventSchema = StructType("listing_events_fetcher",
                                 Array(
                                   StructField("listing_id", LongType),
                                   StructField("ts", LongType),
                                   StructField("ds", StringType)
                                 ))

    val sourceData: Map[StructType, Seq[Row]] = Map(
      eventSchema -> eventData,
      mutationSchema -> mutationData,
      snapshotSchema -> snapshotData
    )

    sourceData.foreach { case (schema, rows) =>
      spark
        .createDataFrame(rows.toJava, SparkConversions.fromChrononSchema(schema))
        .save(s"$namespace.${schema.name}")

    }
    logger.info("saved all data hand written for fetcher test")

    val startPartition = "2021-04-08"
    val endPartition = "2021-04-10"
    val rightSource = Builders.Source.entities(
      query = Builders.Query(
        selects = Map("listing_id" -> "listing", "ts" -> "ts", "rating" -> "rating"),
        startPartition = startPartition,
        endPartition = endPartition,
        mutationTimeColumn = "mutation_time",
        reversalColumn = "is_before_reversal"
      ),
      snapshotTable = s"$namespace.${snapshotSchema.name}",
      mutationTable = s"$namespace.${mutationSchema.name}",
      mutationTopic = "blank"
    )

    val leftSource =
      Builders.Source.events(
        query = Builders.Query(
          selects = Builders.Selects("listing_id", "ts"),
          startPartition = startPartition
        ),
        table = s"$namespace.${eventSchema.name}"
      )

    val groupBy = Builders.GroupBy(
      sources = Seq(rightSource),
      keyColumns = Seq("listing_id"),
      aggregations = Seq(
        Builders.Aggregation(
          operation = Operation.SUM,
          inputColumn = "rating",
          windows = null
        ),
        Builders.Aggregation(
          operation = Operation.AVERAGE,
          inputColumn = "rating",
          windows = Seq(new Window(1, TimeUnit.DAYS))
        ),
        Builders.Aggregation(
          operation = Operation.SKEW,
          inputColumn = "rating",
          windows = Seq(new Window(1, TimeUnit.DAYS))
        )
      ),
      accuracy = Accuracy.TEMPORAL,
      metaData = Builders.MetaData(name = "unit_test.fetcher_mutations_gb", namespace = namespace, team = "chronon")
    )

    val joinConf = Builders.Join(
      left = leftSource,
      joinParts = Seq(Builders.JoinPart(groupBy = groupBy)),
      metaData = Builders.MetaData(name = "unit_test.fetcher_mutations_join", namespace = namespace, team = "chronon")
    )
    joinConf
  }

  def generateMutationDataWithUniqueTopK(namespace: String, tableUtils: TableUtils, spark: SparkSession): api.Join = {
    SparkTestBase.createDatabase(spark, namespace)
    def toTs(arg: String): Long = TsUtils.datetimeToTs(arg)

    // Create manual struct data for UniqueTopK testing
    val eventData = Seq(
      Row(1L, toTs("2021-04-10 09:00:00"), "2021-04-10"),
      Row(2L, toTs("2021-04-10 23:00:00"), "2021-04-10")
    )

    val structData = Seq(
      Row(1L, toTs("2021-04-04 00:30:00"), Row("z", 1L, 100), "2021-04-09"),
      Row(1L, toTs("2021-04-05 00:30:00"), Row("y", 2L, 200), "2021-04-09"),
      Row(1L, toTs("2021-04-06 00:30:00"), Row("x", 3L, 300), "2021-04-09"),
      Row(1L, toTs("2021-04-07 00:30:00"), Row("w", 4L, 500), "2021-04-08"),
      Row(2L, toTs("2021-04-04 01:30:00"), Row("a", 5L, 150), "2021-04-08"),
      Row(2L, toTs("2021-04-05 01:30:00"), Row("b", 6L, 250), "2021-04-08"),
      Row(1L, toTs("2021-04-04 00:30:00"), Row("z", 1L, 100), "2021-04-10"),
      Row(1L, toTs("2021-04-05 00:30:00"), Row("y", 2L, 200), "2021-04-10"),
      Row(1L, toTs("2021-04-06 00:30:00"), Row("x", 3L, 300), "2021-04-10"),
      Row(1L, toTs("2021-04-07 00:30:00"), Row("w", 4L, 500), "2021-04-10"),
      Row(2L, toTs("2021-04-04 01:30:00"), Row("a", 5L, 150), "2021-04-10"),
      Row(2L, toTs("2021-04-05 01:30:00"), Row("b", 6L, 250), "2021-04-10")
    )

    val mutationData = Seq(
      Row(1L,
          toTs("2021-04-08 00:30:00"),
          Row("z", 1L, 400),
          "2021-04-09",
          toTs("2021-04-08 00:30:00"),
          false
      ), // duplicate unique_id
      Row(1L, toTs("2021-04-09 00:30:00"), Row("v", 7L, 600), "2021-04-09", toTs("2021-04-09 00:30:00"), false)
    )

    // Event schema
    val eventSchema = StructType(
      "listing_events_struct",
      Array(StructField("listing_id", LongType), StructField("ts", LongType), StructField("ds", StringType))
    )

    // Struct snapshot schema
    val structSnapshotSchema = StructType(
      "listing_struct_snapshot",
      Array(
        StructField("listing_id", LongType),
        StructField("ts", LongType),
        StructField("rating_struct",
                    StructType("RatingStruct",
                               Array(
                                 StructField("sort_key", StringType),
                                 StructField("unique_id", LongType),
                                 StructField("value", IntType)
                               ))),
        StructField("ds", StringType)
      )
    )

    // Struct mutation schema
    val structMutationSchema = StructType(
      "listing_struct_mutation",
      Array(
        StructField("listing_id", LongType),
        StructField("ts", LongType),
        StructField("rating_struct",
                    StructType("RatingStruct",
                               Array(
                                 StructField("sort_key", StringType),
                                 StructField("unique_id", LongType),
                                 StructField("value", IntType)
                               ))),
        StructField("ds", StringType),
        StructField("mutation_time", LongType),
        StructField("is_before_reversal", BooleanType)
      )
    )

    spark
      .createDataFrame(eventData.toJava, SparkConversions.fromChrononSchema(eventSchema))
      .save(s"$namespace.${eventSchema.name}")

    spark
      .createDataFrame(structData.toJava, SparkConversions.fromChrononSchema(structSnapshotSchema))
      .save(s"$namespace.${structSnapshotSchema.name}")

    spark
      .createDataFrame(mutationData.toJava, SparkConversions.fromChrononSchema(structMutationSchema))
      .save(s"$namespace.${structMutationSchema.name}")

    val structSource = Builders.Source.entities(
      query = Builders.Query(
        selects = Map("listing_id" -> "listing_id", "ts" -> "ts", "rating_struct" -> "rating_struct"),
        startPartition = "2021-04-01",
        endPartition = "2021-04-10",
        mutationTimeColumn = "mutation_time",
        reversalColumn = "is_before_reversal"
      ),
      snapshotTable = s"$namespace.${structSnapshotSchema.name}",
      mutationTable = s"$namespace.${structMutationSchema.name}",
      mutationTopic = "blank"
    )

    val leftSource = Builders.Source.events(
      query = Builders.Query(
        selects = Builders.Selects("listing_id", "ts"),
        startPartition = "2021-04-01"
      ),
      table = s"$namespace.${eventSchema.name}"
    )

    val groupBy = Builders.GroupBy(
      sources = Seq(structSource),
      keyColumns = Seq("listing_id"),
      aggregations = Seq(
        Builders.Aggregation(
          operation = Operation.UNIQUE_TOP_K,
          inputColumn = "rating_struct",
          argMap = Map("k" -> "3"),
          windows = null
        )
      ),
      derivations = Seq(
        Derivation("ids", "transform(rating_struct_unique_top3, x -> x.unique_id)")
      ),
      accuracy = Accuracy.TEMPORAL,
      metaData = Builders.MetaData(name = "unit_test.struct_unique_topk_gb", namespace = namespace, team = "chronon")
    )

    val joinConf = Builders.Join(
      left = leftSource,
      joinParts = Seq(Builders.JoinPart(groupBy = groupBy)),
      metaData = Builders.MetaData(name = "unit_test.struct_unique_topk_join", namespace = namespace, team = "chronon")
    )
    joinConf
  }

  def generateRandomData(namespace: String,
                         tableUtils: TableUtils,
                         spark: SparkSession,
                         topic: String,
                         today: String,
                         yesterday: String,
                         keyCount: Int = 4,
                         cardinality: Int = 100): api.Join = {
    SparkTestBase.createDatabase(spark, namespace)
    val rowCount = cardinality * keyCount * 50
    val userCol = Column("user", StringType, keyCount)
    val vendorCol = Column("vendor", StringType, keyCount)
    // temporal events
    val paymentCols = Seq(userCol, vendorCol, Column("payment", LongType, 100), Column("notes", StringType, 20))
    val paymentsTable = s"$namespace.payments_table"
    val paymentsDf = DataFrameGen.events(spark, paymentCols, rowCount, 60)
    val tsColString = "ts_string"

    paymentsDf.withTimeFormattedColumn(tsColString, Constants.TimeColumn, "yyyy-MM-dd HH:mm:ss").save(paymentsTable)
    // temporal events
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

    // snapshot events
    val ratingCols =
      Seq(
        userCol,
        vendorCol,
        Column("rating", IntType, 5),
        Column("bucket", StringType, 5),
        Column("sub_rating", ListType(DoubleType), 5),
        Column("txn_types", ListType(StringType), 5)
      )
    val ratingsTable = s"$namespace.ratings_table"
    DataFrameGen.events(spark, ratingCols, rowCount, 180).save(ratingsTable)

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

    // no-agg
    val userBalanceCols = Seq(userCol, Column("balance", IntType, 5000))
    val balanceTable = s"$namespace.balance_table"

    DataFrameGen
      .entities(spark, userBalanceCols, rowCount, 180)
      .groupBy("user", "ds")
      .agg(avg("balance") as "avg_balance")
      .save(balanceTable)

    val userBalanceGroupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.entities(query = Builders.Query(), snapshotTable = balanceTable)),
      keyColumns = Seq("user"),
      metaData = Builders.MetaData(name = "unit_test.user_balance", namespace = namespace)
    )

    // snapshot-entities
    val userVendorCreditCols =
      Seq(Column("account", StringType, 100),
          vendorCol, // will be renamed
          Column("credit", IntType, 500),
          Column("ts", LongType, 100))
    val creditTable = s"$namespace.credit_table"
    DataFrameGen
      .entities(spark, userVendorCreditCols, rowCount, 100)
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

    // temporal-entities
    val vendorReviewCols =
      Seq(Column("vendor", StringType, 10), // will be renamed
          Column("review", LongType, 10))
    val snapshotTable = s"$namespace.reviews_table_snapshot"
    val mutationTable = s"$namespace.reviews_table_mutations"
    val mutationTopic = "reviews_mutation_topic"
    val (snapshotDf, mutationsDf) =
      DataFrameGen.mutations(spark, vendorReviewCols, rowCount, 35, 0.2, 1, keyColumnName = "vendor")
    snapshotDf.withColumnRenamed("vendor", "vendor_id").save(snapshotTable)
    mutationsDf.withColumnRenamed("vendor", "vendor_id").save(mutationTable)
    val reviewGroupBy = Builders.GroupBy(
      sources = Seq(
        Builders.Source
          .entities(
            query = Builders.Query(
              startPartition = tableUtils.partitionSpec.before(yesterday)
            ),
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

    // queries
    val queryCols = Seq(userCol, vendorCol)
    val queriesTable = s"$namespace.queries_table"
    val queriesDf = DataFrameGen
      .events(spark, queryCols, rowCount, 4)
      .withColumnRenamed("user", "user_id")
      .withColumnRenamed("vendor", "vendor_id")
    queriesDf.save(queriesTable)

    val joinConf = Builders
      .Join(
        left = Builders.Source.events(Builders.Query(startPartition = today), table = queriesTable),
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
        metaData = Builders.MetaData(name = "test.payments_join",
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
    joinConf
  }

  def joinResponsesV2(spark: SparkSession,
                      requests: Array[Request],
                      mockApi: MockApi,
                      responseType: FeaturesResponseType.ResponseType = FeaturesResponseType.AvroString,
                      debug: Boolean = false)(implicit ec: ExecutionContext): List[ResponseV2] = {
    val chunkSize = 100
    @transient lazy val fetcher = mockApi.buildFetcher(debug)

    val result = requests.iterator
      .grouped(chunkSize)
      .flatMap { requestChunk =>
        val responses = fetcher.fetchJoinV2(requestChunk, None, responseType)
        Await.result(responses, Duration(10, SECONDS))
      }
      .toList

    if (debug) {
      result.foreach { response =>
        logger.info(
          s"FetchJoinV2 Response for ${response.request.name}: " +
            s"responseType=${response.getResponseValueType}, " +
            s"hasErrors=${response.errors.isFailure || response.errors.get.nonEmpty}")
      }
    }

    result
  }

  def testFetchJoinV2(spark: SparkSession, requests: Array[Request], mockApi: MockApi, debug: Boolean = false)(implicit
      ec: ExecutionContext): Unit = {
    // Get the join schema and valueCodec for decoding
    val fetcher = mockApi.buildFetcher(debug)
    val joinName = requests.head.name
    val joinSchemaResponse = fetcher.fetchJoinSchema(joinName).get
    val valueCodec = new AvroCodec(joinSchemaResponse.valueSchema)

    // Test AvroString response type
    val avroStringResponses = joinResponsesV2(spark, requests, mockApi, FeaturesResponseType.AvroString, debug)

    // Test AvroBytes response type
    val avroBytesResponses = joinResponsesV2(spark, requests, mockApi, FeaturesResponseType.AvroBytes, debug)

    // Basic validation
    assert(avroStringResponses.length == requests.length, "AvroString response count should match request count")
    assert(avroBytesResponses.length == requests.length, "AvroBytes response count should match request count")

    // Validate response types are correct and decode actual values
    avroStringResponses.foreach { response =>
      assert(response.getResponseValueType == FeaturesResponseType.AvroString,
             s"Response for ${response.request.name} should be AvroString type")

      // Assert no per-feature errors and validate AvroString payload
      response.errors match {
        case Success(errs) =>
          assert(errs.isEmpty, s"Expected no per-feature errors for ${response.request.name}, got: $errs")
        case Failure(e) => throw new AssertionError(s"errors Try failed for ${response.request.name}: ${e.getMessage}")
      }
      response.value match {
        case ai.chronon.online.fetcher.Fetcher.AvroResponseValue.AvroString(valueResult) =>
          valueResult match {
            case Success(base64String) =>
              assert(base64String.nonEmpty, "Base64 string should not be empty")
              // Validate it's proper base64 and decode the actual Avro record
              try {
                val decodedBytes = java.util.Base64.getDecoder.decode(base64String)
                val decodedRecord: GenericRecord = valueCodec.decode(decodedBytes)
                assert(decodedRecord != null, s"Decoded Avro record should not be null for ${response.request.name}")

                if (debug) {
                  logger.info(s"AvroString decoded record for ${response.request.name}: ${decodedRecord}")
                }
              } catch {
                case _: IllegalArgumentException =>
                  throw new AssertionError(s"Invalid base64 string for ${response.request.name}")
                case e: Exception =>
                  throw new AssertionError(
                    s"Failed to decode Avro record for ${response.request.name}: ${e.getMessage}")
              }
            case Failure(exception) =>
              throw new AssertionError(
                s"AvroString conversion failed for ${response.request.name}: ${exception.getMessage}")
          }
        case _ => throw new AssertionError("Expected AvroString response value")
      }
    }

    avroBytesResponses.foreach { response =>
      assert(response.getResponseValueType == FeaturesResponseType.AvroBytes,
             s"Response for ${response.request.name} should be AvroBytes type")

      // Assert no per-feature errors and validate AvroBytes payload
      response.errors match {
        case Success(errs) =>
          assert(errs.isEmpty, s"Expected no per-feature errors for ${response.request.name}, got: $errs")
        case Failure(e) => throw new AssertionError(s"errors Try failed for ${response.request.name}: ${e.getMessage}")
      }
      response.value match {
        case ai.chronon.online.fetcher.Fetcher.AvroResponseValue.AvroBytes(valueResult) =>
          valueResult match {
            case Success(bytes) =>
              assert(bytes.length > 0, "Byte array should not be empty")
              // Decode the actual Avro record from bytes
              try {
                val decodedRecord: GenericRecord = valueCodec.decode(bytes)
                assert(decodedRecord != null, s"Decoded Avro record should not be null for ${response.request.name}")

                if (debug) {
                  logger.info(s"AvroBytes decoded record for ${response.request.name}: ${decodedRecord}")
                }
              } catch {
                case e: Exception =>
                  throw new AssertionError(
                    s"Failed to decode Avro record from bytes for ${response.request.name}: ${e.getMessage}")
              }
            case Failure(exception) =>
              throw new AssertionError(
                s"AvroBytes conversion failed for ${response.request.name}: ${exception.getMessage}")
          }
        case _ => throw new AssertionError("Expected AvroBytes response value")
      }
    }

    if (debug) {
      logger.info(s"FetchJoinV2 test completed successfully for ${requests.length} requests")
      logger.info(
        s"AvroString responses: ${avroStringResponses.length}, AvroBytes responses: ${avroBytesResponses.length}")
    }
  }

  def generateEventOnlyData(namespace: String,
                            tableUtils: TableUtils,
                            spark: SparkSession,
                            groupByCustomJson: Option[String] = None): api.Join = {
    SparkTestBase.createDatabase(spark, namespace)

    def toTs(arg: String): Long = TsUtils.datetimeToTs(arg)

    val listingEventData = Seq(
      Row(1L, toTs("2021-04-10 03:10:00"), "2021-04-10"),
      Row(2L, toTs("2021-04-10 03:10:00"), "2021-04-10")
    )
    val ratingEventData = Seq(
      // 1L listing id event data
      Row(1L, toTs("2021-04-08 00:30:00"), 2, "2021-04-08"),
      Row(1L, toTs("2021-04-09 05:35:00"), 4, "2021-04-09"),
      Row(1L, toTs("2021-04-10 02:30:00"), 5, "2021-04-10"),
      Row(1L, toTs("2021-04-10 02:30:00"), 5, "2021-04-10"),
      Row(1L, toTs("2021-04-10 02:30:00"), 8, "2021-04-10"),
      Row(1L, toTs("2021-04-10 02:30:00"), 8, "2021-04-10"),
      // 2L listing id event data
      Row(2L, toTs("2021-04-06 00:30:00"), 10, "2021-04-06"), // excluded from all aggs with start partition 4/7
      Row(2L, toTs("2021-04-06 00:30:00"), 10, "2021-04-06"), // excluded from all aggs with start partition 4/7
      Row(2L, toTs("2021-04-07 00:30:00"), 10, "2021-04-07"), // excluded from avg agg
      Row(2L, toTs("2021-04-07 00:30:00"), 10, "2021-04-07"), // excluded from avg agg
      Row(2L, toTs("2021-04-08 00:30:00"), 2, "2021-04-08"),
      Row(2L, toTs("2021-04-09 05:35:00"), 4, "2021-04-09"),
      Row(2L, toTs("2021-04-10 02:30:00"), 5, "2021-04-10"),
      Row(2L, toTs("2021-04-10 02:30:00"), 5, "2021-04-10"),
      Row(2L, toTs("2021-04-10 02:30:00"), 8, "2021-04-10"),
      Row(2L, toTs("2021-04-10 02:30:00"), 8, "2021-04-10"),
      Row(2L, toTs("2021-04-07 00:30:00"), 10, "2021-04-10") // dated 4/10 but excluded from avg agg based on ts
    )
    // Schemas
    // {..., event (generic event column), ...}
    val listingsSchema = StructType("listing_events_fetcher",
                                    Array(
                                      StructField("listing_id", LongType),
                                      StructField("ts", LongType),
                                      StructField("ds", StringType)
                                    ))

    val ratingsSchema = StructType(
      "listing_ratings_fetcher",
      Array(StructField("listing_id", LongType),
            StructField("ts", LongType),
            StructField("rating", IntType),
            StructField("ds", StringType))
    )

    val sourceData: Map[StructType, Seq[Row]] = Map(
      listingsSchema -> listingEventData,
      ratingsSchema -> ratingEventData
    )

    sourceData.foreach { case (schema, rows) =>
      val tableName = s"$namespace.${schema.name}"

      spark.sql(s"DROP TABLE IF EXISTS $tableName")

      spark
        .createDataFrame(rows.toJava, SparkConversions.fromChrononSchema(schema))
        .save(tableName)
    }
    println("saved all data hand written for fetcher test")

    val startPartition = "2021-04-07"

    val leftSource =
      Builders.Source.events(
        query = Builders.Query(
          selects = Builders.Selects("listing_id", "ts"),
          startPartition = startPartition
        ),
        table = s"$namespace.${listingsSchema.name}"
      )

    val rightSource =
      Builders.Source.events(
        query = Builders.Query(
          selects = Builders.Selects("listing_id", "ts", "rating"),
          startPartition = startPartition
        ),
        table = s"$namespace.${ratingsSchema.name}",
        topic = "fake_topic2"
      )

    val groupBy = Builders.GroupBy(
      sources = Seq(rightSource),
      keyColumns = Seq("listing_id"),
      aggregations = Seq(
        Builders.Aggregation(
          operation = Operation.SUM,
          inputColumn = "rating",
          windows = null
        ),
        Builders.Aggregation(
          operation = Operation.AVERAGE,
          inputColumn = "rating",
          windows = Seq(new Window(2, TimeUnit.DAYS))
        ),
        Builders.Aggregation(
          operation = Operation.APPROX_FREQUENT_K,
          inputColumn = "rating",
          windows = Seq(new Window(1, TimeUnit.DAYS))
        )
      ),
      accuracy = Accuracy.TEMPORAL,
      metaData = Builders.MetaData(
        name = "unit_test/fetcher_tiled_gb",
        namespace = namespace,
        team = "chronon",
        customJson = groupByCustomJson.orNull
      )
    )

    val joinConf = Builders.Join(
      left = leftSource,
      joinParts = Seq(Builders.JoinPart(groupBy = groupBy)),
      metaData = Builders.MetaData(name = "unit_test/fetcher_tiled_join", namespace = namespace, team = "chronon")
    )
    joinConf
  }
}
