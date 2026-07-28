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

package ai.chronon.online.fetcher

import ai.chronon.api._
import ai.chronon.online.GroupByServingInfoParsed
import ai.chronon.online.KVStore
import ai.chronon.online.KVStore.TimedValue
import ai.chronon.online.fetcher.FetcherCache.BatchResponses
import ai.chronon.online.metrics.Metrics
import ai.chronon.online.serde.AvroConversions
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import scala.util.Success

class GroupByResponseHandlerTest extends AnyFlatSpec with Matchers with MockitoSugar {

  it should "finalize an empty temporal response when batch and streaming data are both absent" in {
    val servingInfo = emptyTemporalServingInfo()
    val fetchContext = FetchContext(mock[KVStore])
    val handler = new GroupByResponseHandler(fetchContext, mock[MetadataStore])
    val requestContext = handler.RequestContext(
      servingInfo,
      queryTimeMs = servingInfo.batchEndTsMillis + 1L,
      startTimeMs = System.currentTimeMillis(),
      metricsContext = Metrics.Context(Metrics.Environment.GroupByFetching, servingInfo.groupBy),
      keys = Map("id" -> "missing")
    )

    val result = handler.decodeAndMerge(
      BatchResponses(Success(Seq.empty[TimedValue])),
      Some(Seq.empty[TimedValue]),
      requestContext
    )

    result shouldBe Map(
      "value_count_1d" -> java.lang.Long.valueOf(0L),
      "value_unique_count_1d" -> java.lang.Long.valueOf(0L),
      "value_approx_unique_count_1d" -> java.lang.Long.valueOf(0L),
      "value_sum_1d" -> null
    )
  }

  private def emptyTemporalServingInfo(): GroupByServingInfoParsed = {
    val groupBy = Builders.GroupBy(
      sources = Seq(
        Builders.Source.events(
          table = "events.empty",
          topic = "events.empty",
          query = Builders.Query(
            selects = Map("id" -> "id", "value" -> "value", "ts" -> "ts"),
            wheres = Seq.empty,
            timeColumn = "ts",
            startPartition = "20231106"
          )
        )
      ),
      keyColumns = Seq("id"),
      aggregations = Seq(
        Builders.Aggregation(Operation.COUNT, "value", windows = Seq(new Window(1, TimeUnit.DAYS))),
        Builders.Aggregation(Operation.UNIQUE_COUNT, "value", windows = Seq(new Window(1, TimeUnit.DAYS))),
        Builders.Aggregation(Operation.APPROX_UNIQUE_COUNT, "value", windows = Seq(new Window(1, TimeUnit.DAYS))),
        Builders.Aggregation(Operation.SUM, "value", windows = Seq(new Window(1, TimeUnit.DAYS)))
      ),
      accuracy = Accuracy.TEMPORAL,
      metaData = Builders.MetaData(name = "empty_temporal_response")
    )
    val servingInfo = new GroupByServingInfo()
    servingInfo.setGroupBy(groupBy)
    servingInfo.setBatchEndDate("2023-11-06")
    servingInfo.setDateFormat("yyyy-MM-dd")
    servingInfo.setInputAvroSchema(
      AvroConversions
        .fromChrononSchema(
          StructType(
            "Input",
            Array(StructField("id", StringType), StructField("value", LongType), StructField("ts", LongType))
          )
        )
        .toString(true)
    )
    servingInfo.setKeyAvroSchema(
      AvroConversions.fromChrononSchema(StructType("Key", Array(StructField("id", StringType)))).toString(true)
    )
    servingInfo.setSelectedAvroSchema(
      AvroConversions
        .fromChrononSchema(
          StructType(
            "Selected",
            Array(StructField("id", StringType), StructField("value", LongType), StructField("ts", LongType))
          )
        )
        .toString(true)
    )
    new GroupByServingInfoParsed(servingInfo)
  }
}
