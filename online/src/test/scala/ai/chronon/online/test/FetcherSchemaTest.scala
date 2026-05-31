package ai.chronon.online.test

import ai.chronon.api.Extensions.GroupByOps
import ai.chronon.api.{Constants, ThriftJsonCodec}
import ai.chronon.online.KVStore.PutRequest
import ai.chronon.online.fetcher.Fetcher
import ai.chronon.online.serde.AvroCodec
import ai.chronon.online.InMemoryKvStore
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

class FetcherSchemaTest extends AnyFlatSpec with Matchers {

  private def putString(kvStore: InMemoryKvStore, key: String, value: String, dataset: String): Unit = {
    Await.result(kvStore.put(PutRequest(key.getBytes(Constants.UTF8), value.getBytes(Constants.UTF8), dataset)), 1.second)
  }

  it should "fetch schema for online groupBys" in {
    val kvStore = InMemoryKvStore.build(s"FetcherSchemaTest_online_${System.nanoTime()}")
    kvStore.create(Constants.MetadataDataset)

    val servingInfo = GroupByDerivationsTest.makeTestGroupByServingInfoParsed().groupByServingInfo
    servingInfo.groupBy.metaData.setOnline(true)
    val groupByName = servingInfo.groupBy.metaData.name
    val batchDataset = new GroupByOps(servingInfo.groupBy).batchDataset
    kvStore.create(batchDataset)

    putString(kvStore, servingInfo.groupBy.keyNameForKvStore, ThriftJsonCodec.toJsonStr(servingInfo.groupBy), Constants.MetadataDataset)
    putString(kvStore, Constants.GroupByServingInfoKey, ThriftJsonCodec.toJsonStr(servingInfo), batchDataset)

    val fetcher = new Fetcher(kvStore, Constants.MetadataDataset)
    val response = fetcher.fetchGroupBySchema(groupByName).get

    response.groupByName shouldBe groupByName
    response.keySchema shouldBe servingInfo.keyAvroSchema
    response.inputSchema shouldBe servingInfo.inputAvroSchema
    response.selectedSchema shouldBe servingInfo.selectedAvroSchema

    val valueCodec = AvroCodec.of(response.valueSchema)
    valueCodec.fieldNames.toSet shouldBe Set("int_val", "id_last2_1d", "id2_last2_1d")
  }

  it should "fetch status for online groupBys" in {
    val kvStore = InMemoryKvStore.build(s"FetcherSchemaTest_status_${System.nanoTime()}")
    kvStore.create(Constants.MetadataDataset)

    val servingInfo = GroupByDerivationsTest.makeTestGroupByServingInfoParsed().groupByServingInfo
    servingInfo.groupBy.metaData.setOnline(true)
    val groupByName = servingInfo.groupBy.metaData.name
    val batchDataset = new GroupByOps(servingInfo.groupBy).batchDataset
    kvStore.create(batchDataset)

    putString(kvStore, servingInfo.groupBy.keyNameForKvStore, ThriftJsonCodec.toJsonStr(servingInfo.groupBy), Constants.MetadataDataset)
    putString(kvStore, Constants.GroupByServingInfoKey, ThriftJsonCodec.toJsonStr(servingInfo), batchDataset)

    val fetcher = new Fetcher(kvStore, Constants.MetadataDataset)
    val response = fetcher.fetchGroupByStatus(groupByName).get

    response.groupByName shouldBe groupByName
    response.batchEndDate shouldBe servingInfo.batchEndDate
  }

  it should "return a user-facing status error for offline groupBys" in {
    val kvStore = InMemoryKvStore.build(s"FetcherSchemaTest_status_offline_${System.nanoTime()}")
    kvStore.create(Constants.MetadataDataset)

    val servingInfo = GroupByDerivationsTest.makeTestGroupByServingInfoParsed().groupByServingInfo
    servingInfo.groupBy.metaData.setOnline(false)
    val groupByName = servingInfo.groupBy.metaData.name
    val batchDataset = new GroupByOps(servingInfo.groupBy).batchDataset
    kvStore.create(batchDataset)

    putString(kvStore, servingInfo.groupBy.keyNameForKvStore, ThriftJsonCodec.toJsonStr(servingInfo.groupBy), Constants.MetadataDataset)
    putString(kvStore, Constants.GroupByServingInfoKey, ThriftJsonCodec.toJsonStr(servingInfo), batchDataset)

    val fetcher = new Fetcher(kvStore, Constants.MetadataDataset)
    val failure = fetcher.fetchGroupByStatus(groupByName).failed.get

    failure shouldBe a[IllegalArgumentException]
    failure.getMessage should include("online=True")
  }

  it should "return a user-facing error for offline groupBys" in {
    val kvStore = InMemoryKvStore.build(s"FetcherSchemaTest_offline_${System.nanoTime()}")
    kvStore.create(Constants.MetadataDataset)

    val groupBy = GroupByDerivationsTest.makeGroupBy()
    groupBy.metaData.setOnline(false)
    putString(kvStore, groupBy.keyNameForKvStore, ThriftJsonCodec.toJsonStr(groupBy), Constants.MetadataDataset)

    val fetcher = new Fetcher(kvStore, Constants.MetadataDataset)
    val failure = fetcher.fetchGroupBySchema(groupBy.metaData.name).failed.get

    failure shouldBe a[IllegalArgumentException]
    failure.getMessage should include("Iceberg catalog schema via eval")
    failure.getMessage should include("online=True")
  }
}
