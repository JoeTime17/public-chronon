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

package ai.chronon.spark.join

import ai.chronon.aggregator.test.Column
import ai.chronon.api
import ai.chronon.api._
import ai.chronon.spark._
import ai.chronon.spark.Extensions._
import ai.chronon.spark.utils.DataFrameGen

class HeterogeneousPartitionColumnsTest extends BaseJoinTest {

  it should "test hetergeneous partition columns" in {
    // Test for different partition columns and formats in the same join
    // First GroupBy with default format
    val namespace = "test_heterogeneous"
    createDatabase(namespace)

    val viewsSchema = List(
      Column("user", api.StringType, 10),
      Column("item", api.StringType, 10),
      Column("time_spent_ms", api.LongType, 5000)
    )

    val viewsTable = s"$namespace.view_events_hetero"
    DataFrameGen.events(spark, viewsSchema, count = 10000, partitions = 200).drop("ts").save(viewsTable)

    // ViewsSource --> Default partition column
    val viewsSource = Builders.Source.events(
      query = Builders.Query(selects = Builders.Selects("time_spent_ms"), startPartition = yearAgo),
      table = viewsTable
    )

    val viewsGroupBy = Builders.GroupBy(
      sources = Seq(viewsSource),
      keyColumns = Seq("item"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.AVERAGE, inputColumn = "time_spent_ms")
      ),
      metaData = Builders.MetaData(name = "unit_test.item_views_hetero", namespace = namespace),
      accuracy = Accuracy.SNAPSHOT
    )

    // Second GroupBy with custom format (yyyy-MM-dd)
    val clicksSchema = List(
      Column("user", api.StringType, 10),
      Column("item", api.StringType, 10),
      Column("click_count", api.LongType, 30)
    )

    val clicksTable = s"$namespace.clicks_hetero"
    val customFormat = "yyyy-MM-dd"
    val customPartitionCol = "date_col"

    // Convert yearAgo to the custom format
    val customFormatSpec = tableUtils.partitionSpec.copy(column = customPartitionCol, format = customFormat)
    val yearAgoCustom = customFormatSpec.at(tableUtils.partitionSpec.epochMillis(yearAgo))

    val eventsDf = DataFrameGen
      .events(
        spark,
        clicksSchema,
        count = 10000,
        partitions = 200,
        partitionColumn = Some(customPartitionCol),
        partitionFormat = Some(customFormat)
      )
      .drop("ts")

    eventsDf.save(clicksTable, partitionColumns = Seq(customPartitionCol))

    // ClicksSource --> Custom partition column
    val clicksSource = Builders.Source.events(
      query = Builders
        .Query(
          selects = Builders.Selects("click_count"),
          startPartition = yearAgoCustom,
          partitionColumn = customPartitionCol
        )
        .setPartitionFormat(customFormat),
      table = clicksTable
    )

    val clicksGroupBy = Builders.GroupBy(
      sources = Seq(clicksSource),
      keyColumns = Seq("item"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.SUM, inputColumn = "click_count")
      ),
      metaData = Builders.MetaData(name = "unit_test.item_clicks_hetero", namespace = namespace),
      accuracy = Accuracy.SNAPSHOT
    )

    // Left side with custom format (MM-dd-yyyy)
    val itemQueries = List(Column("item", api.StringType, 10))
    val itemQueriesTable = s"$namespace.item_queries_hetero"

    // Convert start date to the left custom format
    val leftFormatSpec = tableUtils.partitionSpec.copy(column = customPartitionCol, format = customFormat)
    val start = tableUtils.partitionSpec.minus(threeDaysAgo, new Window(200, TimeUnit.DAYS))
    val startCustom = leftFormatSpec.at(tableUtils.partitionSpec.epochMillis(start))
    val monthAgoCustom = leftFormatSpec.at(tableUtils.partitionSpec.epochMillis(monthAgo))

    // Left uses custom format as well

    val leftCustomFormat = "yyyyMMdd"
    val leftCustomPartitionCol = "UNDERSCORE_DATE"

    val leftDf = DataFrameGen.events(
      spark,
      itemQueries,
      10000,
      partitions = 200,
      partitionColumn = Some(leftCustomPartitionCol),
      partitionFormat = Some(leftCustomFormat)
    )

    leftDf.save(itemQueriesTable, partitionColumns = Seq(leftCustomPartitionCol))

    val leftEndFormatSpec = tableUtils.partitionSpec.copy(column = leftCustomPartitionCol, format = leftCustomFormat)

    // Pin start past the boundary partition. DataFrameGen.events derives its time window from
    // CStream.TimeStream(minTs = currentTimeMillis() - window), so the first written partition
    // spans only the fraction of a day from minTs's time-of-day to midnight and is materially
    // sparser than the rest. tableUtils.partitions normalizes grid-matching ds values to the
    // global format, so translate back into the query's declared format.
    val writtenPartitions =
      tableUtils.partitions(itemQueriesTable, tablePartitionSpec = Some(leftEndFormatSpec)).sorted
    val leftStartPartition = writtenPartitions
      .drop(1)
      .headOption
      .map(p => tableUtils.partitionSpec.translate(p, leftEndFormatSpec))
      .getOrElse(throw new IllegalStateException(
        s"Expected at least 2 partitions in $itemQueriesTable to skip sparse boundary, " +
          s"found: ${writtenPartitions.mkString(", ")}"))

    val joinConf = Builders.Join(
      left = Builders.Source.events(
        Builders
          .Query(
            partitionColumn = leftCustomPartitionCol,
            startPartition = leftStartPartition
          )
          .setPartitionFormat(leftCustomFormat),
        table = itemQueriesTable
      ),
      joinParts = Seq(
        Builders.JoinPart(groupBy = viewsGroupBy, prefix = "user")
      ),
      metaData = Builders.MetaData(name = "test.item_hetero_features", namespace = namespace, team = "chronon")
    )

    // Use a dynamic end partition that's within the generated data range
    // Start is threeDaysAgo - 200 days, so use a date that's within the data range
    val endPartitionDate = tableUtils.partitionSpec.minus(threeDaysAgo, new Window(100, TimeUnit.DAYS))
    val endPartition = leftEndFormatSpec.at(tableUtils.partitionSpec.epochMillis(endPartitionDate))

    val join = new ai.chronon.spark.Join(joinConf = joinConf, endPartition = endPartition, tableUtils = tableUtils)
    val computed = join.computeJoin()
    val computedRows = computed.collect()
    assert(computedRows.nonEmpty)

    // Multi-day coverage is what this test exists to exercise - without an explicit assertion,
    // a future regression in the engine's spec handling could collapse the compute range to
    // a one-day result that still passes the non-empty check above.
    val outputPartitions = computedRows.map(_.getAs[String]("ds")).filter(_ != null).toSet
    assert(
      outputPartitions.size > 1,
      s"Expected computed output to span multiple partitions, got: ${outputPartitions.toList.sorted.mkString(", ")}"
    )
  }
}
