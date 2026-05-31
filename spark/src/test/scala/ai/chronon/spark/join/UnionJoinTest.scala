package ai.chronon.spark.join

import ai.chronon.aggregator.test.Column
import ai.chronon.api
import ai.chronon.api.Extensions.MetadataOps
import ai.chronon.api._
import ai.chronon.spark.Extensions._
import ai.chronon.spark.utils.DataFrameGen
import org.scalatest.matchers.should.Matchers._

class UnionJoinTest extends BaseJoinTest {

  it should "test events events temporal case for UnionJoin.computeJoinAndSave" in {

    val viewsSchema = List(
      Column("user", api.StringType, 1),
      Column("item", api.StringType, 1),
      Column("time_spent_ms", api.LongType, 5000)
    )

    val viewsTable = s"$namespace.view_union_temporal"
    DataFrameGen
      .events(spark, viewsSchema, count = 10000, partitions = 20)
      .save(viewsTable, Map("tblProp1" -> "1"))

    val viewsDf = tableUtils.loadTable(viewsTable)

    val viewsSource = Builders.Source.events(
      table = viewsTable,
      topic = "",
      query = Builders.Query(selects = Builders.Selects("time_spent_ms"),
                             startPartition = tableUtils.partitionSpec.minus(today, new Window(20, TimeUnit.DAYS)))
    )

    val viewsGroupBy = Builders
      .GroupBy(
        sources = Seq(viewsSource),
        keyColumns = Seq("item"),
        aggregations = Seq(
          Builders.Aggregation(operation = Operation.AVERAGE, inputColumn = "time_spent_ms"),
          Builders.Aggregation(
            operation = Operation.LAST_K,
            argMap = Map("k" -> "50"),
            inputColumn = "time_spent_ms",
            windows = Seq(new Window(2, TimeUnit.DAYS))
          )
        ),
        metaData = Builders.MetaData(name = "unit_test.item_views", namespace = namespace)
      )
      .setAccuracy(Accuracy.TEMPORAL)

    // left side
    val itemQueries = List(Column("item", api.StringType, 1))
    val itemQueriesTable = s"$namespace.item_queries_union_temporal"
    val itemQueriesDf = DataFrameGen
      .events(spark, itemQueries, 10000, partitions = 10)

    // duplicate the events
    itemQueriesDf.union(itemQueriesDf).save(itemQueriesTable)

    val queriesDf = tableUtils.loadTable(itemQueriesTable)

    val start = tableUtils.partitionSpec.minus(today, new Window(20, TimeUnit.DAYS))
    val dateRange = PartitionRange(start, today)(tableUtils.partitionSpec)

    val joinConf = Builders.Join(
      left = Builders.Source.events(Builders.Query(startPartition = start), table = itemQueriesTable),
      joinParts = Seq(Builders.JoinPart(groupBy = viewsGroupBy, prefix = "user")),
      metaData =
        Builders.MetaData(name = s"test.item_temporal_features.union_join", namespace = namespace, team = "item_team")
    )

    // Test UnionJoin.computeJoinAndSave method

    UnionJoin.computeJoinAndSave(joinConf, dateRange)

    val outputDf = tableUtils.loadTable(joinConf.metaData.outputTable)

    val outputData = outputDf.where("item IS NOT NULL and ts IS NOT NULL").collect()
    val queriesData = queriesDf.where("item IS NOT NULL and ts IS NOT NULL").collect()
    outputData.length shouldBe queriesData.length
  }

  it should "test UnionJoin with GroupBy and Join derivations" in {

    val eventsSchema = List(
      Column("user_id", api.StringType, 1),
      Column("item_id", api.StringType, 1),
      Column("amount", api.LongType, 100),
      Column("category", api.StringType, 3)
    )

    val eventsTable = s"$namespace.events_with_derivations"
    DataFrameGen
      .events(spark, eventsSchema, count = 50000, partitions = 40) // Increased count and partitions
      .save(eventsTable)

    val eventsSource = Builders.Source.events(
      table = eventsTable,
      query = Builders.Query(
        selects = Builders.Selects("amount", "category"),
        startPartition = tableUtils.partitionSpec.minus(today, new Window(40, TimeUnit.DAYS)) // Increased window
      )
    )

    // GroupBy with derivations
    val groupByWithDerivations = Builders
      .GroupBy(
        sources = Seq(eventsSource),
        keyColumns = Seq("user_id"),
        aggregations = Seq(
          Builders.Aggregation(operation = Operation.SUM,
                               inputColumn = "amount",
                               windows = Seq(new Window(7, TimeUnit.DAYS))),
          Builders.Aggregation(operation = Operation.COUNT,
                               inputColumn = "amount",
                               windows = Seq(new Window(7, TimeUnit.DAYS))),
          Builders.Aggregation(operation = Operation.SUM,
                               inputColumn = "amount",
                               windows = Seq(new Window(30, TimeUnit.DAYS)))
        ),
        derivations = Seq(
          Builders.Derivation(name = "*", expression = "*"), // Include all base columns
          Builders.Derivation(name = "avg_amount_7d", expression = "amount_sum_7d / amount_count_7d"),
          Builders.Derivation(name = "amount_ratio", expression = "amount_sum_7d / amount_sum_30d")
        ),
        metaData = Builders.MetaData(name = "unit_test.user_features_with_derivations", namespace = namespace)
      )
      .setAccuracy(Accuracy.TEMPORAL)

    // Left side events
    val leftSchema = List(Column("user_id", api.StringType, 1))
    val leftTable = s"$namespace.user_queries_with_derivations"
    DataFrameGen
      .events(spark, leftSchema, count = 5000, partitions = 20) // Increased count and partitions
      .save(leftTable)

    val start = tableUtils.partitionSpec.minus(today, new Window(40, TimeUnit.DAYS))
    val dateRange = PartitionRange(start, today)(tableUtils.partitionSpec)

    // Join with derivations
    val joinWithDerivations = Builders.Join(
      left = Builders.Source.events(Builders.Query(startPartition = start), table = leftTable),
      joinParts = Seq(Builders.JoinPart(groupBy = groupByWithDerivations).setUseLongNames(false)),
      derivations = Seq(
        Builders.Derivation(name = "*", expression = "*"), // Include all columns
        Builders.Derivation(name = "spend_efficiency", expression = "user_id_avg_amount_7d * user_id_amount_ratio"),
        Builders.Derivation(
          name = "user_tier",
          expression =
            "CASE WHEN user_id_amount_sum_30d > 1000 THEN 'high' WHEN user_id_amount_sum_30d > 500 THEN 'medium' ELSE 'low' END")
      ),
      metaData =
        Builders.MetaData(name = "test.user_features_derived.union_join", namespace = namespace, team = "user_team")
    )

    // Execute UnionJoin with derivations
    UnionJoin.computeJoinAndSave(joinWithDerivations, dateRange)

    val outputDf = tableUtils.loadTable(joinWithDerivations.metaData.outputTable)

    // Verify that derived columns exist
    val schema = outputDf.schema
    schema.fieldNames should contain("user_id_avg_amount_7d") // GroupBy derivation
    schema.fieldNames should contain("user_id_amount_ratio") // GroupBy derivation
    schema.fieldNames should contain("spend_efficiency") // Join derivation
    schema.fieldNames should contain("user_tier") // Join derivation

    // Verify that base aggregation columns still exist due to wildcard
    schema.fieldNames should contain("user_id_amount_sum_7d")
    schema.fieldNames should contain("user_id_amount_count_7d")
    schema.fieldNames should contain("user_id_amount_sum_30d")

    val outputData = outputDf.where("user_id IS NOT NULL and ts IS NOT NULL").collect()
    outputData.length should be > 0

    // Verify derivation logic works - ensure we actually test the calculation
    val validRows = outputData.filter { row =>
      val avgAmount7d = Option(row.getAs[Double]("user_id_avg_amount_7d"))
      val amountSum7d = Option(row.getAs[Long]("user_id_amount_sum_7d"))
      val amountCount7d = Option(row.getAs[Long]("user_id_amount_count_7d"))
      avgAmount7d.isDefined && amountSum7d.isDefined && amountCount7d.isDefined &&
      amountCount7d.get > 0 && amountSum7d.get > 0
    }

    validRows.length should be > 0 // Ensure we have at least one row to test

    // Verify GroupBy derivation calculation on a valid row
    val sampleRow = validRows.head
    val avgAmount7d = sampleRow.getAs[Double]("user_id_avg_amount_7d")
    val amountSum7d = sampleRow.getAs[Long]("user_id_amount_sum_7d")
    val amountCount7d = sampleRow.getAs[Long]("user_id_amount_count_7d")

    Math.abs(avgAmount7d - (amountSum7d.toDouble / amountCount7d)) should be < 0.001
  }

  it should "preserve true left columns for rename-only join derivations" in {

    val rightSchema = List(
      Column("user_id", api.StringType, 3),
      Column("listing_id", api.StringType, 5)
    )

    val rightTable = s"$namespace.rename_only_join_derivation_right"
    DataFrameGen
      .events(spark, rightSchema, count = 2000, partitions = 20)
      .save(rightTable)

    val rightSource = Builders.Source.events(
      table = rightTable,
      query = Builders.Query(
        selects = Builders.Selects("listing_id"),
        startPartition = tableUtils.partitionSpec.minus(today, new Window(14, TimeUnit.DAYS))
      )
    )

    val listingByUser = Builders
      .GroupBy(
        sources = Seq(rightSource),
        keyColumns = Seq("user_id"),
        aggregations = Seq(
          Builders.Aggregation(
            operation = Operation.LAST,
            inputColumn = "listing_id",
            windows = Seq(new Window(7, TimeUnit.DAYS))
          )
        ),
        metaData = Builders.MetaData(name = "unit_test.rename_only_listing_by_user", namespace = namespace)
      )
      .setAccuracy(Accuracy.TEMPORAL)

    val leftTable = s"$namespace.rename_only_join_derivation_left"
    DataFrameGen
      .events(spark, List(Column("user_id", api.StringType, 3)), count = 500, partitions = 10)
      .save(leftTable)

    val start = tableUtils.partitionSpec.minus(today, new Window(14, TimeUnit.DAYS))
    val dateRange = PartitionRange(start, today)(tableUtils.partitionSpec)

    val joinConf = Builders.Join(
      left = Builders.Source.events(
        table = leftTable,
        query = Builders.Query(startPartition = start)
      ),
      joinParts = Seq(Builders.JoinPart(groupBy = listingByUser).setUseLongNames(false)),
      derivations = Seq(
        Builders.Derivation(
          name = "derived_listing_id",
          expression = "user_id_listing_id_last_7d"
        )
      ),
      metaData =
        Builders.MetaData(name = "test.rename_only_join_derivation.union_join", namespace = namespace, team = "test")
    )

    UnionJoin.computeJoinAndSave(joinConf, dateRange)

    val outputDf = tableUtils.loadTable(joinConf.metaData.outputTable)
    val schema = outputDf.schema

    schema.fieldNames should contain("ds")
    schema.fieldNames should contain("ts")
    schema.fieldNames should contain("user_id")
    schema.fieldNames should contain("derived_listing_id")
    schema.fieldNames should not contain("user_id_listing_id_last_7d")

    outputDf.where("user_id IS NOT NULL and ts IS NOT NULL").count() should be > 0L
  }

  it should "test UnionJoin schema with GB derivations" in {

    val namespace = "union_join_gb_derivation"
    createDatabase(namespace)

    val eventsSchema = List(
      Column("user_id", api.StringType, 1),
      Column("request_id", api.StringType, 100),
      Column("request_ts", api.LongType, 100000),
      Column("amount", api.LongType, 100),
      Column("category", api.StringType, 3)
    )

    val eventsTable = s"$namespace.events_with_derivations"
    DataFrameGen
      .events(spark, eventsSchema, count = 50000, partitions = 40) // Increased count and partitions
      .save(eventsTable)

    val eventsSource = Builders.Source.events(
      table = eventsTable,
      query = Builders.Query(
        selects = Builders.Selects("amount", "category"),
        startPartition = tableUtils.partitionSpec.minus(today, new Window(40, TimeUnit.DAYS)) // Increased window
      )
    )

    // GroupBy with derivations
    val groupByWithDerivations = Builders
      .GroupBy(
        sources = Seq(eventsSource),
        keyColumns = Seq("user_id"),
        aggregations = Seq(
          Builders.Aggregation(operation = Operation.SUM,
                               inputColumn = "amount",
                               windows = Seq(new Window(7, TimeUnit.DAYS))),
          Builders.Aggregation(operation = Operation.COUNT,
                               inputColumn = "amount",
                               windows = Seq(new Window(7, TimeUnit.DAYS))),
          Builders.Aggregation(operation = Operation.SUM,
                               inputColumn = "amount",
                               windows = Seq(new Window(30, TimeUnit.DAYS)))
        ),
        derivations = Seq(
          Builders.Derivation(name = "avg_amount_7d", expression = "amount_sum_7d / amount_count_7d"),
          Builders.Derivation(name = "amount_ratio", expression = "amount_sum_7d / amount_sum_30d")
        ),
        metaData = Builders.MetaData(name = "unit_test.user_features_with_derivations", namespace = namespace)
      )
      .setAccuracy(Accuracy.TEMPORAL)

    val start = tableUtils.partitionSpec.minus(today, new Window(40, TimeUnit.DAYS))
    val dateRange = PartitionRange(start, today)(tableUtils.partitionSpec)

    // Join with derivations
    val longNameJoin = Builders.Join(
      left = Builders.Source.events(
        Builders.Query(selects =
                         Builders.Selects("user_id", "request_id", "request_ts", "amount", "category"),
                       startPartition = start),
        table = eventsTable),
      joinParts = Seq(Builders.JoinPart(groupBy = groupByWithDerivations)),
      metaData =
        Builders.MetaData(name = "test.user_features_derived.union_join_long_names",
                          namespace = namespace,
                          team = "user_team")
    )

    // Execute UnionJoin with derivations
    UnionJoin.computeJoinAndSave(longNameJoin, dateRange)

    val longNameOutputDf = tableUtils.loadTable(longNameJoin.metaData.outputTable)
    val longNameSchema = longNameOutputDf.schema
    println(longNameSchema.pretty)
    // Verify that derived columns exist
    longNameSchema.fieldNames should contain("user_id")
    longNameSchema.fieldNames should contain("request_id")
    longNameSchema.fieldNames should contain("request_ts")
    longNameSchema.fieldNames should contain("amount")
    longNameSchema.fieldNames should contain("category")
    // Derivation cols
    longNameSchema.fieldNames should contain("unit_test_user_features_with_derivations_avg_amount_7d")
    longNameSchema.fieldNames should contain("unit_test_user_features_with_derivations_amount_ratio")
    longNameSchema.fieldNames should not contain("unit_test_user_features_with_derivations_request_id")
    longNameSchema.fieldNames should not contain("unit_test_user_features_with_derivations_request_ts")
    longNameSchema.fieldNames should not contain("unit_test_user_features_with_derivations_amount")
    longNameSchema.fieldNames should not contain("unit_test_user_features_with_derivations_category")

    // Join with derivations
    val shortNameJoin = Builders.Join(
      left = Builders.Source.events(
        Builders.Query(selects =
                         Builders.Selects("user_id", "request_id", "request_ts", "amount", "category"),
                       startPartition = start),
        table = eventsTable),
      joinParts = Seq(Builders.JoinPart(groupBy = groupByWithDerivations).setUseLongNames(false)),
      metaData =
        Builders.MetaData(name = "test.user_features_derived.union_join_short_names",
                          namespace = namespace,
                          team = "user_team")
    )

    // Execute UnionJoin with derivations
    UnionJoin.computeJoinAndSave(shortNameJoin, dateRange)

    val shortNameOutputDf = tableUtils.loadTable(shortNameJoin.metaData.outputTable)
    val shortNameSchema = shortNameOutputDf.schema
    println(shortNameSchema.pretty)
    // Verify that derived columns exist
    shortNameSchema.fieldNames should contain("user_id")
    shortNameSchema.fieldNames should contain("request_id")
    shortNameSchema.fieldNames should contain("request_ts")
    shortNameSchema.fieldNames should contain("amount")
    shortNameSchema.fieldNames should contain("category")
    shortNameSchema.fieldNames should not contain("user_id_request_id")
    shortNameSchema.fieldNames should not contain("user_id_request_ts")
    shortNameSchema.fieldNames should not contain("user_id_amount")
    shortNameSchema.fieldNames should not contain("user_id_category")
    // Derivation cols
    shortNameSchema.fieldNames should contain("user_id_avg_amount_7d")
    shortNameSchema.fieldNames should contain("user_id_amount_ratio")
  }
}
