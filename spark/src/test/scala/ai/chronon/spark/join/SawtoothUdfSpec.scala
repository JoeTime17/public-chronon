package ai.chronon.spark.join

import ai.chronon.aggregator.windowing.FiveMinuteResolution
import ai.chronon.api._
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.{Row => SparkRow}
import org.scalatest.matchers.should.Matchers

class SawtoothUdfSpec extends BaseJoinTest with Matchers {

  "SawtoothUdf.sawtoothAggregate" should "perform point-in-time aggregation correctly" in {
    // Test scenario based on the comments:
    // - left: search requests with query and timestamp
    // - right: item-click events with item attributes like item prices
    // - aggregating:
    //   - average item prices viewed in the last two days
    //   - most recently viewed item ids in the last week

    // Create test data
    // Using a base timestamp that's easy to work with
    val baseTs = 1600000000000L // Some fixed base time

    // Create left rows (search requests)
    val leftRows =
      Seq(SparkRow(1, "query1", baseTs), SparkRow(1, "query2", baseTs + 3600000)) // Second search query, 1 hour later

    // Create right rows (item-click events)
    val rightRows = Seq(
      // Events that happened before first query
      SparkRow(1, "item1", 10.5, baseTs - 3600000), // 1 hour before
      SparkRow(1, "item2", 20.5, baseTs - 1800000), // 30 min before

      // Events that happened between first and second query
      SparkRow(1, "item3", 30.5, baseTs + 1800000) // 30 min after first query
    )
    // Create schemas
    val leftSchema = org.apache.spark.sql.types.StructType(Seq(
      org.apache.spark.sql.types.StructField("user_id", org.apache.spark.sql.types.IntegerType),
      org.apache.spark.sql.types.StructField("query", org.apache.spark.sql.types.StringType),
      org.apache.spark.sql.types.StructField(Constants.TimeColumn, org.apache.spark.sql.types.LongType)
    ))

    val rightSchema = org.apache.spark.sql.types.StructType(Seq(
      org.apache.spark.sql.types.StructField("user_id", org.apache.spark.sql.types.IntegerType),
      org.apache.spark.sql.types.StructField("item_id", org.apache.spark.sql.types.StringType),
      org.apache.spark.sql.types.StructField("price", org.apache.spark.sql.types.DoubleType),
      org.apache.spark.sql.types.StructField(Constants.TimeColumn, org.apache.spark.sql.types.LongType)
    ))

    // Define our aggregations
    val aggregations = Seq(
      Builders.Aggregation(
        operation = Operation.AVERAGE,
        inputColumn = "price",
        windows = Seq(new Window(2, TimeUnit.DAYS)) // Average price in last 2 days
      ),
      Builders.Aggregation(
        operation = Operation.COUNT,
        inputColumn = "price",
        windows = Seq(new Window(7, TimeUnit.DAYS)) // Count of items in last 7 days
      )
    )

    // Create GroupBy definition
    val groupBy = Builders.GroupBy(
      sources = Seq(),
      keyColumns = Seq("user_id"),
      aggregations = aggregations,
      metaData = null,
      accuracy = null
    )

    // Create the aggregators
    val minQueryTs = baseTs - 7 * 24 * 3600 * 1000 // 7 days before base time
    val aggregators = AggregationInfo.from(
      groupBy = groupBy,
      minQueryTs = minQueryTs,
      leftSchema = leftSchema,
      rightSchema = rightSchema,
      resolution = FiveMinuteResolution
    )

    // Call the sawtoothAggregate function
    val result = SawtoothUdf.sawtoothAggregate(aggregators)(leftRows, rightRows).toArray

    // Verify the results
    // We should have one result row for each input left row
    result.size shouldBe 2

    // First result corresponds to the first query at baseTs
    // At this time, only item1 and item2 are in the window
    val firstResult = result(0)
    firstResult.isInstanceOf[CGenericRow] shouldBe true

    // We have 3 fields from left (user_id, query, ts) + aggregation results
    firstResult.length shouldBe 5 // 3 left fields + 2 aggregation results

    // The user_id, query, and timestamp should match the first query
    firstResult.get(0) shouldBe 1
    firstResult.get(1) shouldBe "query1"
    firstResult.get(2) shouldBe baseTs

    // Verify aggregation results for first query - should include item1 and item2
    // avg_price_2d should be average of 10.5 and 20.5 = 15.5
    firstResult.get(3).asInstanceOf[Double] shouldBe 15.5 +- 0.001
    // count_price_7d should be 2
    firstResult.get(4).asInstanceOf[Long] shouldBe 2L

    // Second result corresponds to the second query at baseTs + 1 hour
    // At this time, all three items (item1, item2, item3) are in the window
    val secondResult = result(1)
    secondResult.isInstanceOf[CGenericRow] shouldBe true

    // The user_id, query, and timestamp should match the second query
    secondResult.get(0) shouldBe 1
    secondResult.get(1) shouldBe "query2"
    secondResult.get(2) shouldBe baseTs + 3600000

    // Verify aggregation results for second query - should include all three items
    // avg_price_2d should be average of 10.5, 20.5, and 30.5 = 20.5
    secondResult.get(3).asInstanceOf[Double] shouldBe 20.5 +- 0.001
    // count_price_7d should be 3
    secondResult.get(4).asInstanceOf[Long] shouldBe 3L
  }

  it should "handle empty inputs gracefully" in {
    // Test with empty inputs
    val emptyLeftRows = Seq.empty[SparkRow]
    val emptyRightRows = Seq.empty[SparkRow]

    // Define schemas
    val schema = org.apache.spark.sql.types.StructType(Seq(
      org.apache.spark.sql.types.StructField("user_id", org.apache.spark.sql.types.IntegerType),
      org.apache.spark.sql.types.StructField("value", org.apache.spark.sql.types.DoubleType),
      org.apache.spark.sql.types.StructField(Constants.TimeColumn, org.apache.spark.sql.types.LongType)
    ))

    // Create a simple aggregation
    val aggregations = Seq(
      Builders.Aggregation(
        operation = Operation.SUM,
        inputColumn = "value",
        windows = Seq(new Window(1, TimeUnit.DAYS))
      )
    )

    // Create GroupBy definition
    val groupBy = Builders.GroupBy(
      sources = Seq(),
      keyColumns = Seq("user_id"),
      aggregations = aggregations,
      metaData = null,
      accuracy = null
    )

    // Create the aggregators
    val minQueryTs = System.currentTimeMillis() - 7 * 24 * 3600 * 1000 // 7 days ago
    val aggregators = AggregationInfo.from(
      groupBy = groupBy,
      minQueryTs = minQueryTs,
      leftSchema = schema,
      rightSchema = schema,
      resolution = FiveMinuteResolution
    )

    // Test with empty input - should not throw exceptions
    val result = SawtoothUdf.sawtoothAggregate(aggregators)(emptyLeftRows, emptyRightRows)

    // Should return empty result
    result.size shouldBe 0
  }

  it should "handle non-overlapping time windows" in {
    // Create test data with non-overlapping time windows
    val baseTs = 1600000000000L

    // Create left rows (queries)
    val leftRows = Seq(SparkRow(1, "query1", baseTs))

    // Create right rows (events), but all events are outside the window
    val rightRows = Seq(SparkRow(1, "item1", 10.5, baseTs - 3 * 24 * 3600 * 1000),
                          SparkRow(1, "item2", 20.5, baseTs + 100000)
    ) // After query

    // Define schemas
    val leftSchema = org.apache.spark.sql.types.StructType(Seq(
      org.apache.spark.sql.types.StructField("user_id", org.apache.spark.sql.types.IntegerType),
      org.apache.spark.sql.types.StructField("query", org.apache.spark.sql.types.StringType),
      org.apache.spark.sql.types.StructField(Constants.TimeColumn, org.apache.spark.sql.types.LongType)
    ))

    val rightSchema = org.apache.spark.sql.types.StructType(Seq(
      org.apache.spark.sql.types.StructField("user_id", org.apache.spark.sql.types.IntegerType),
      org.apache.spark.sql.types.StructField("item_id", org.apache.spark.sql.types.StringType),
      org.apache.spark.sql.types.StructField("price", org.apache.spark.sql.types.DoubleType),
      org.apache.spark.sql.types.StructField(Constants.TimeColumn, org.apache.spark.sql.types.LongType)
    ))

    // Define a 1-day window aggregation
    val aggregations = Seq(
      Builders.Aggregation(
        operation = Operation.AVERAGE,
        inputColumn = "price",
        windows = Seq(new Window(1, TimeUnit.DAYS)) // 1 day window - should exclude the 3-day old event
      )
    )

    // Create GroupBy definition
    val groupBy = Builders.GroupBy(
      sources = Seq(),
      keyColumns = Seq("user_id"),
      aggregations = aggregations,
      metaData = null,
      accuracy = null
    )

    // Create the aggregators
    val minQueryTs = baseTs - 7 * 24 * 3600 * 1000 // 7 days before base time
    val aggregators = AggregationInfo.from(
      groupBy = groupBy,
      minQueryTs = minQueryTs,
      leftSchema = leftSchema,
      rightSchema = rightSchema,
      resolution = FiveMinuteResolution
    )

    // Call the sawtoothAggregate function
    val result = SawtoothUdf.sawtoothAggregate(aggregators)(leftRows, rightRows).toArray

    // Should have one result for the one query
    result.size shouldBe 1

    // The result should contain default values for the aggregations
    // since no events fall in the window
    val queryResult = result(0)
    queryResult.isInstanceOf[CGenericRow] shouldBe true

    // Verify all fields
    queryResult.length shouldBe 4 // 3 left fields + 1 aggregation result

    // Original query fields should be preserved
    queryResult.get(0) shouldBe 1
    queryResult.get(1) shouldBe "query1"
    queryResult.get(2) shouldBe baseTs

    // The avg_price_1d should be either null, NaN or 0 since no items fall within the window
    // The exact default value depends on the implementation of the aggregation
    // We'll accept any of these values
    val avgPrice = queryResult.get(3)
    (avgPrice == null ||
      (avgPrice.isInstanceOf[Double] && (avgPrice.asInstanceOf[Double].isNaN || avgPrice
        .asInstanceOf[Double] == 0.0))) shouldBe true
  }

  it should "exclude events with the same timestamp as the query for JoinSource group bys" in {
    val baseTs = 1600000000000L

    val leftRows = Seq(SparkRow(1, "query1", baseTs))
    val rightRows = Seq(
      SparkRow(1, "item0", 80.0, baseTs - 1),
      SparkRow(1, "item1", 100.0, baseTs)
    )

    val leftSchema = org.apache.spark.sql.types.StructType(Seq(
      org.apache.spark.sql.types.StructField("user_id", org.apache.spark.sql.types.IntegerType),
      org.apache.spark.sql.types.StructField("query", org.apache.spark.sql.types.StringType),
      org.apache.spark.sql.types.StructField(Constants.TimeColumn, org.apache.spark.sql.types.LongType)
    ))

    val rightSchema = org.apache.spark.sql.types.StructType(Seq(
      org.apache.spark.sql.types.StructField("user_id", org.apache.spark.sql.types.IntegerType),
      org.apache.spark.sql.types.StructField("item_id", org.apache.spark.sql.types.StringType),
      org.apache.spark.sql.types.StructField("price", org.apache.spark.sql.types.DoubleType),
      org.apache.spark.sql.types.StructField(Constants.TimeColumn, org.apache.spark.sql.types.LongType)
    ))

    val aggregations = Seq(
      Builders.Aggregation(
        operation = Operation.COUNT,
        inputColumn = "price",
        windows = Seq(new Window(1, TimeUnit.DAYS))
      ),
      Builders.Aggregation(
        operation = Operation.SUM,
        inputColumn = "price",
        windows = Seq(new Window(1, TimeUnit.DAYS))
      )
    )

    val parentGroupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.events(Builders.Query(), table = "dummy_right")),
      keyColumns = Seq("user_id"),
      aggregations = aggregations,
      metaData = Builders.MetaData(name = "parent_gb", namespace = "test"),
      accuracy = Accuracy.TEMPORAL
    )
    val parentJoin: ai.chronon.api.Join = Builders.Join(
      metaData = Builders.MetaData(name = "parent_join", namespace = "test"),
      left = Builders.Source.events(Builders.Query(), table = "dummy_left"),
      joinParts = Seq(Builders.JoinPart(groupBy = parentGroupBy))
    )
    val groupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.joinSource(parentJoin, Builders.Query())),
      keyColumns = Seq("user_id"),
      aggregations = aggregations,
      metaData = null,
      accuracy = null
    )

    val minQueryTs = baseTs - 24 * 3600 * 1000
    val aggregators = AggregationInfo.from(
      groupBy = groupBy,
      minQueryTs = minQueryTs,
      leftSchema = leftSchema,
      rightSchema = rightSchema,
      resolution = FiveMinuteResolution
    )

    val result = SawtoothUdf.sawtoothAggregate(aggregators)(leftRows, rightRows).toArray

    result.size shouldBe 1

    val queryResult = result(0)
    queryResult.isInstanceOf[CGenericRow] shouldBe true

    queryResult.get(0) shouldBe 1
    queryResult.get(1) shouldBe "query1"
    queryResult.get(2) shouldBe baseTs

    queryResult.get(3).asInstanceOf[Long] shouldBe 1L
    queryResult.get(4).asInstanceOf[Double] shouldBe 80.0 +- 0.001
  }

}
