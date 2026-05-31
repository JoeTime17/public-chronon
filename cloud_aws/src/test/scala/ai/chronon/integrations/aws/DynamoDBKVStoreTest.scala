package ai.chronon.integrations.aws

import ai.chronon.api.Constants.{ContinuationKey, KvEnableTtlArg, KvReplicaRegionsArg, KvTablePrefixArg, ListLimit}
import ai.chronon.api.ScalaJavaConversions._
import java.time.LocalDate
import ai.chronon.api.TilingUtils
import ai.chronon.integrations.aws.DynamoDBKVStoreConstants.isTimedSorted
import ai.chronon.online.KVStore._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.testcontainers.containers.GenericContainer
import org.testcontainers.utility.DockerImageName
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

import java.net.URI
import java.nio.charset.StandardCharsets
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success, Try}

object DDBTestUtils {

  // different types of tables to store
  case class Model(modelId: String, modelName: String, online: Boolean)
  case class TimeSeries(joinName: String, featureName: String, tileTs: Long, metric: String, summary: Array[Double])

}

class DynamoDBKVStoreTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  import DDBTestUtils._
  import DynamoDBKVStoreConstants._

  var dynamoContainer: GenericContainer[_] = _
  var client: DynamoDbAsyncClient = _
  var kvStoreImpl: DynamoDBKVStoreImpl = _

  private val objectMapper = new ObjectMapper()
  objectMapper.registerModule(DefaultScalaModule)

  def modelKeyEncoder(model: Model): Array[Byte] = {
    objectMapper.writeValueAsString(model.modelId).getBytes(StandardCharsets.UTF_8)
  }

  def modelValueEncoder(model: Model): Array[Byte] = {
    objectMapper.writeValueAsString(model).getBytes(StandardCharsets.UTF_8)
  }

  def timeSeriesKeyEncoder(timeSeries: TimeSeries): Array[Byte] = {
    objectMapper.writeValueAsString((timeSeries.joinName, timeSeries.featureName)).getBytes(StandardCharsets.UTF_8)
  }

  def timeSeriesValueEncoder(series: TimeSeries): Array[Byte] = {
    objectMapper.writeValueAsString(series).getBytes(StandardCharsets.UTF_8)
  }

  override def beforeAll(): Unit = {
    // Start the DynamoDB Local container
    dynamoContainer = new GenericContainer(DockerImageName.parse("amazon/dynamodb-local:latest"))
    dynamoContainer.withExposedPorts(8000: Integer)
    dynamoContainer.withCommand("-jar", "DynamoDBLocal.jar", "-inMemory", "-sharedDb")
    dynamoContainer.start()

    // Create the DynamoDbClient pointing to the container
    val dynamoEndpoint = s"http://${dynamoContainer.getHost}:${dynamoContainer.getMappedPort(8000)}"
    client = DynamoDbAsyncClient
      .builder()
      .endpointOverride(URI.create(dynamoEndpoint))
      .region(Region.US_WEST_2)
      .credentialsProvider(
        StaticCredentialsProvider.create(
          AwsBasicCredentials.create("dummy", "dummy")
        ))
      .build()
  }

  override def afterAll(): Unit = {
    if (client != null) {
      client.close()
    }
    if (dynamoContainer != null) {
      dynamoContainer.stop()
    }
  }

  // Test creation of a table with primary keys only (e.g. model)
  it should "create p key only table" in {
    val dataset = "models_p_key_only"
    val props = Map(isTimedSorted -> "false")
    val kvStore = new DynamoDBKVStoreImpl(client)
    kvStore.create(dataset, props)
    // Verify that the table exists
    val tables = client.listTables().join().tableNames()
    tables.contains(dataset) shouldBe true
    // try another create for an existing table, should not fail
    kvStore.create(dataset, props)
  }

  // Test creation of a table with primary + sort keys (e.g. time series)
  it should "create p key and sort key table" in {
    val dataset = "timeseries_p_and_sort_key"
    val props = Map(isTimedSorted -> "true")
    val kvStore = new DynamoDBKVStoreImpl(client)
    kvStore.create(dataset, props)
    // Verify that the table exists
    val tables = client.listTables().join().tableNames()
    tables.contains(dataset) shouldBe true
    // here too, another create should not fail
    kvStore.create(dataset, props)
  }

  // Streaming tables auto-configure the sort key without needing isTimedSorted prop
  it should "auto-configure sort key for streaming table created without props" in {
    val dataset = "AUTO_SORT_KEY_STREAMING"
    val kvStore = new DynamoDBKVStoreImpl(client)
    kvStore.create(dataset) // no props — _STREAMING suffix should trigger sort key
    val tables = client.listTables().join().tableNames()
    tables.contains(dataset) shouldBe true

    // Verify sort key works by writing and reading tiled data
    val entityKeyBytes = "test_entity".getBytes(StandardCharsets.UTF_8)
    val tileSizeMillis = 1.hour.toMillis
    val tileStart = 1728000000000L
    val putReq = {
      val tileKey = TilingUtils.buildTileKey(dataset, entityKeyBytes, Some(tileSizeMillis), Some(tileStart))
      val tileKeyBytes = TilingUtils.serializeTileKey(tileKey)
      PutRequest(tileKeyBytes, "value".getBytes(StandardCharsets.UTF_8), dataset, Some(tileStart))
    }
    val putResults = Await.result(kvStore.multiPut(Seq(putReq)), 1.minute)
    putResults shouldBe Seq(true)

    val readTileKey = TilingUtils.buildTileKey(dataset, entityKeyBytes, Some(tileSizeMillis), None)
    val readKeyBytes = TilingUtils.serializeTileKey(readTileKey)
    val getRequest = GetRequest(readKeyBytes, dataset, Some(tileStart), Some(tileStart + tileSizeMillis))
    val getResults = Await.result(kvStore.multiGet(Seq(getRequest)), 1.minute)
    getResults.size shouldBe 1
    getResults.head.values.isSuccess shouldBe true
    getResults.head.values.get.nonEmpty shouldBe true
  }

  // Test table scan with pagination
  it should "table scan with pagination" in {
    val dataset = "models_scan_pagination"
    val props = Map(isTimedSorted -> "false")
    val kvStore = new DynamoDBKVStoreImpl(client)
    kvStore.create(dataset, props)

    val putReqs = (0 until 100).map { i =>
      val model = Model(s"my_model_$i", s"test model $i", online = true)
      buildModelPutRequest(model, dataset)
    }

    val putResults = Await.result(kvStore.multiPut(putReqs), 1.minute)
    putResults.length shouldBe putReqs.length
    putResults.foreach(r => r shouldBe true)

    // call list - first call is only for 10 elements
    val listReq1 = ListRequest(dataset, Map(ListLimit -> 10))
    val listResults1 = Await.result(kvStore.list(listReq1), 1.minute)
    listResults1.resultProps.contains(ContinuationKey) shouldBe true
    validateExpectedListResponse(listResults1.values, 10)

    // call list - with continuation key
    val listReq2 =
      ListRequest(dataset, Map(ListLimit -> 100, ContinuationKey -> listResults1.resultProps(ContinuationKey)))
    val listResults2 = Await.result(kvStore.list(listReq2), 1.minute)
    listResults2.resultProps.contains(ContinuationKey) shouldBe false
    validateExpectedListResponse(listResults2.values, 100)
  }

  // Test write & read of a simple blob dataset
  it should "blob data round trip" in {
    val dataset = "models_blob_round_trip"
    val props = Map(isTimedSorted -> "false")
    val kvStore = new DynamoDBKVStoreImpl(client)
    kvStore.create(dataset, props)

    val model1 = Model("my_model_1", "test model 1", online = true)
    val model2 = Model("my_model_2", "test model 2", online = true)
    val model3 = Model("my_model_3", "test model 3", online = false)

    val putReq1 = buildModelPutRequest(model1, dataset)
    val putReq2 = buildModelPutRequest(model2, dataset)
    val putReq3 = buildModelPutRequest(model3, dataset)

    val putResults = Await.result(kvStore.multiPut(Seq(putReq1, putReq2, putReq3)), 1.minute)
    putResults shouldBe Seq(true, true, true)

    // let's try and read these
    val getReq1 = buildModelGetRequest(model1, dataset)
    val getReq2 = buildModelGetRequest(model2, dataset)
    val getReq3 = buildModelGetRequest(model3, dataset)

    val getResult1 = Await.result(kvStore.multiGet(Seq(getReq1)), 1.minute)
    val getResult2 = Await.result(kvStore.multiGet(Seq(getReq2)), 1.minute)
    val getResult3 = Await.result(kvStore.multiGet(Seq(getReq3)), 1.minute)

    validateExpectedModelResponse(model1, getResult1)
    validateExpectedModelResponse(model2, getResult2)
    validateExpectedModelResponse(model3, getResult3)
  }

  // Test write and query of a time series dataset (streaming table)
  it should "time series query" in {
    val dataset = "TIMESERIES_QUERY_V1_STREAMING"
    kvStoreImpl = setupStreamingDataset(dataset)

    val entityKeyBytes = "my_join#my_feature_1".getBytes(StandardCharsets.UTF_8)
    val tileSizeMillis = 1.hour.toMillis

    // generate some hourly timestamps from 10/04/24 00:00 to 10/16 (12 days)
    val tsRange = (1728000000000L until 1729036800000L by 1.hour.toMillis)

    val putRequests = tsRange.map { ts =>
      createTilePutRequest(dataset, entityKeyBytes, tileSizeMillis, ts, s"value_at_$ts")
    }

    val putResult = Await.result(kvStoreImpl.multiPut(putRequests), 1.minute)
    validatePutResults(putResult, tsRange.length)

    // query in time range: 10/05/24 00:00 to 10/10 (spans multiple days)
    val startTs = 1728086400000L
    val endTs = 1728518400000L
    val getResult = queryTiledData(entityKeyBytes, dataset, startTs, endTs, tileSizeMillis)

    getResult.length shouldBe 1
    getResult.head.values match {
      case Success(timedValues) =>
        val expectedCount = ((endTs - startTs) / 1.hour.toMillis).toInt + 1
        timedValues.length shouldBe expectedCount
        validateTimestampRange(timedValues, startTs, endTs)
      case Failure(ex) =>
        fail(s"Failed to read time series data: $ex")
    }
  }

  // Test write and query of a tiled streaming dataset
  // This simulates the Flink TiledGroupByJob writing tiles and the Fetcher reading them
  it should "tiled streaming data round trip" in {
    // Streaming tables end with _STREAMING suffix
    val dataset = "MY_GROUPBY_V1_STREAMING"
    kvStoreImpl = setupStreamingDataset(dataset)

    val entityKeyBytes = "entity_key_123".getBytes(StandardCharsets.UTF_8)
    val tileSizeMillis = 1.hour.toMillis

    // Simulate Flink writing tiles (like TiledAvroCodecFn does)
    // Each tile has a TileKey with tileStartTimestampMillis set
    val tileTimestamps = Seq(1728000000000L, 1728003600000L, 1728007200000L, 1728010800000L) // 4 hours of tiles
    val putRequests = tileTimestamps.map { tileStart =>
      createTilePutRequest(dataset, entityKeyBytes, tileSizeMillis, tileStart, s"tile_value_at_$tileStart")
    }

    val putResults = Await.result(kvStoreImpl.multiPut(putRequests), 1.minute)
    validatePutResults(putResults, tileTimestamps.length)

    // Simulate Fetcher reading tiles (like GroupByFetcher does)
    // Query for tiles in range [tileTimestamps(1), tileTimestamps(3)]
    val startTs = tileTimestamps(1) // 1728003600000L
    val endTs = tileTimestamps(3) // 1728010800000L
    val getResults = queryTiledData(entityKeyBytes, dataset, startTs, endTs, tileSizeMillis)

    getResults.length shouldBe 1
    // Should get 3 tiles back (startTs, startTs+1hour, startTs+2hour)
    getResults.head.values match {
      case Success(timedValues) =>
        timedValues.length shouldBe 3
        validateTimestampRange(timedValues, startTs, endTs)
      case Failure(ex) =>
        fail(s"Failed to read tiled streaming data: $ex")
    }
  }

  // Test that writes multiple tiles for different entity keys
  it should "handle multiple entity keys with tiled streaming data" in {
    val dataset = "MULTI_ENTITY_V1_STREAMING"
    kvStoreImpl = setupStreamingDataset(dataset)

    val tileSizeMillis = 1.hour.toMillis
    val tileStart = 1728000000000L

    // Write tiles for two different entity keys
    val entityKey1 = "entity_key_1".getBytes(StandardCharsets.UTF_8)
    val entityKey2 = "entity_key_2".getBytes(StandardCharsets.UTF_8)

    val putReq1 = createTilePutRequest(dataset, entityKey1, tileSizeMillis, tileStart, "value_for_entity_1")
    val putReq2 = createTilePutRequest(dataset, entityKey2, tileSizeMillis, tileStart, "value_for_entity_2")

    val putResults = Await.result(kvStoreImpl.multiPut(Seq(putReq1, putReq2)), 1.minute)
    validatePutResults(putResults, 2)

    // Read back entity 1 only
    val getResults1 = queryTiledData(entityKey1, dataset, tileStart, tileStart, tileSizeMillis)
    validateSingleValueResponse(getResults1, "value_for_entity_1", "Failed to read entity 1")

    // Read back entity 2 only
    val getResults2 = queryTiledData(entityKey2, dataset, tileStart, tileStart, tileSizeMillis)
    validateSingleValueResponse(getResults2, "value_for_entity_2", "Failed to read entity 2")
  }

  it should "repeated streaming tile updates return latest value" in {
    val dataset = "REPEATED_UPDATES_V1_STREAMING"
    kvStoreImpl = setupStreamingDataset(dataset)

    val entityKey = "my_key".getBytes(StandardCharsets.UTF_8)
    val tileSizeMillis = 1.hour.toMillis
    val tileTimestamp = 1728000000000L
    val tileKey = TilingUtils.buildTileKey(dataset, entityKey, Some(tileSizeMillis), Some(tileTimestamp))
    val tileKeyBytes = TilingUtils.serializeTileKey(tileKey)

    // Write a series of updates to the tile to mimic streaming updates
    for (i <- 0 to 10) {
      val fakePayload = s"""{"name": "my_key", "my_feature_ir": "$i"}"""
      val putReq =
        PutRequest(tileKeyBytes, fakePayload.getBytes(StandardCharsets.UTF_8), dataset, Some(tileTimestamp + i * 1000))
      Await.result(kvStoreImpl.multiPut(Seq(putReq)), 1.minute)
    }

    // Query to get the latest tile
    val queryStartsTs = 1728000000000L
    val queryEndTs = 1728036000000L
    val getResult = queryTiledData(entityKey, dataset, queryStartsTs, queryEndTs, tileSizeMillis)

    getResult.size shouldBe 1
    getResult.head.values match {
      case Success(timedValues) =>
        timedValues.length shouldBe 1
        timedValues.head.millis shouldBe tileTimestamp
        val payload = new String(timedValues.head.bytes, StandardCharsets.UTF_8)
        payload shouldBe """{"name": "my_key", "my_feature_ir": "10"}"""
      case Failure(ex) =>
        fail(s"Failed to read repeated updates: $ex")
    }
  }

  it should "streaming tiled query_multiple days and multiple keys" in {
    val dataset = "MULTI_KEY_MULTI_DAY_V1_STREAMING"
    kvStoreImpl = setupStreamingDataset(dataset)

    val entityKey1 = "my_key1".getBytes(StandardCharsets.UTF_8)
    val entityKey2 = "my_key2".getBytes(StandardCharsets.UTF_8)
    val tileSizeMillis = 1.hour.toMillis

    val fakePayload1 = """{"name": "my_key1", "my_feature_ir": "123"}"""
    val fakePayload2 = """{"name": "my_key2", "my_feature_ir": "456"}"""
    val tsRange = (1728000000000L until 1729036800000L by 1.hour.toMillis)

    writeTiledData(entityKey1, dataset, tsRange, tileSizeMillis, fakePayload1)
    writeTiledData(entityKey2, dataset, tsRange, tileSizeMillis, fakePayload2)

    // Query both keys in same time range: 10/05/24 00:00 to 10/10
    val queryStartsTs = 1728086400000L
    val queryEndTs = 1728518400000L

    val readTileKey1 = TilingUtils.buildTileKey(dataset, entityKey1, Some(tileSizeMillis), None)
    val readKeyBytes1 = TilingUtils.serializeTileKey(readTileKey1)
    val readTileKey2 = TilingUtils.buildTileKey(dataset, entityKey2, Some(tileSizeMillis), None)
    val readKeyBytes2 = TilingUtils.serializeTileKey(readTileKey2)

    val getRequest1 = GetRequest(readKeyBytes1, dataset, Some(queryStartsTs), Some(queryEndTs))
    val getRequest2 = GetRequest(readKeyBytes2, dataset, Some(queryStartsTs), Some(queryEndTs))
    val getResult = Await.result(kvStoreImpl.multiGet(Seq(getRequest1, getRequest2)), 1.minute)

    getResult.size shouldBe 2
    val expectedTiles = (queryStartsTs to queryEndTs by 1.hour.toMillis).toSeq

    getResult.foreach { result =>
      validateTimedValuesResponse(result, expectedTiles, "Failed to read multi-key multi-day")
    }
  }

  it should "streaming tiled query_multiple days and multiple keys with different batch end times" in {
    val dataset1 = "GROUPBY_A_V1_STREAMING"
    val dataset2 = "GROUPBY_B_V1_STREAMING"
    kvStoreImpl = new DynamoDBKVStoreImpl(client)
    kvStoreImpl.create(dataset1, Map(isTimedSorted -> "true"))
    kvStoreImpl.create(dataset2, Map(isTimedSorted -> "true"))

    val entityKey1 = "my_key1".getBytes(StandardCharsets.UTF_8)
    val entityKey2 = "my_key2".getBytes(StandardCharsets.UTF_8)
    val tileSizeMillis = 1.hour.toMillis

    val fakePayload1 = """{"name": "my_key1", "my_feature_ir": "123"}"""
    val fakePayload2 = """{"name": "my_key2", "my_feature_ir": "456"}"""
    val tsRange = (1728000000000L until 1729036800000L by 1.hour.toMillis)

    // Write data for both keys to different datasets
    val putRequests1 = tsRange.map { ts => createTilePutRequest(dataset1, entityKey1, tileSizeMillis, ts, fakePayload1) }
    val putRequests2 = tsRange.map { ts => createTilePutRequest(dataset2, entityKey2, tileSizeMillis, ts, fakePayload2) }

    Await.result(kvStoreImpl.multiPut(putRequests1), 1.minute)
    Await.result(kvStoreImpl.multiPut(putRequests2), 1.minute)

    // Query with different time ranges for each key
    val queryStartsTs1 = 1728086400000L
    val queryEndTs1 = 1728518400000L
    val queryStartsTs2 = 1728518400000L
    val queryEndTs2 = 1728604800000L

    val readTileKey1 = TilingUtils.buildTileKey(dataset1, entityKey1, Some(tileSizeMillis), None)
    val readKeyBytes1 = TilingUtils.serializeTileKey(readTileKey1)
    val readTileKey2 = TilingUtils.buildTileKey(dataset2, entityKey2, Some(tileSizeMillis), None)
    val readKeyBytes2 = TilingUtils.serializeTileKey(readTileKey2)

    val getRequest1 = GetRequest(readKeyBytes1, dataset1, Some(queryStartsTs1), Some(queryEndTs1))
    val getRequest2 = GetRequest(readKeyBytes2, dataset2, Some(queryStartsTs2), Some(queryEndTs2))
    val getResult = Await.result(kvStoreImpl.multiGet(Seq(getRequest1, getRequest2)), 1.minute)

    getResult.size shouldBe 2

    val datasetToResult = getResult.map(r => (r.request.dataset, r)).toMap

    val expectedTilesKey1 = (queryStartsTs1 to queryEndTs1 by 1.hour.toMillis).toSeq
    validateTimedValuesResponse(datasetToResult(dataset1), expectedTilesKey1, "Failed to read dataset1")

    val expectedTilesKey2 = (queryStartsTs2 to queryEndTs2 by 1.hour.toMillis).toSeq
    validateTimedValuesResponse(datasetToResult(dataset2), expectedTilesKey2, "Failed to read dataset2")
  }

  it should "streaming tiled query_one day" in {
    val dataset = "ONE_DAY_V1_STREAMING"
    kvStoreImpl = setupStreamingDataset(dataset)

    val entityKeyBytes = "my_key".getBytes(StandardCharsets.UTF_8)
    val tileSizeMillis = 1.hour.toMillis
    val fakePayload = """{"name": "my_key", "my_feature_ir": "123"}"""
    val tsRange = (1728000000000L until 1729036800000L by 1.hour.toMillis)

    writeTiledData(entityKeyBytes, dataset, tsRange, tileSizeMillis, fakePayload)

    // Query exactly one day: 10/05/24 00:00 to 10/06/24 00:00
    val queryStartsTs = 1728086400000L
    val queryEndTs = 1728172800000L
    val getResult = queryTiledData(entityKeyBytes, dataset, queryStartsTs, queryEndTs, tileSizeMillis)

    getResult.size shouldBe 1
    val expectedTiles = (queryStartsTs to queryEndTs by 1.hour.toMillis).toSeq
    validateTimedValuesResponse(getResult.head, expectedTiles, "Failed to read one day")
  }

  it should "streaming tiled query_same day" in {
    val dataset = "SAME_DAY_V1_STREAMING"
    kvStoreImpl = setupStreamingDataset(dataset)

    val entityKeyBytes = "my_key".getBytes(StandardCharsets.UTF_8)
    val tileSizeMillis = 1.hour.toMillis
    val fakePayload = """{"name": "my_key", "my_feature_ir": "123"}"""
    val tsRange = (1728000000000L until 1729036800000L by 1.hour.toMillis)

    writeTiledData(entityKeyBytes, dataset, tsRange, tileSizeMillis, fakePayload)

    // Query within same day: 10/05/24 00:00 to 10/05/24 22:20
    val queryStartsTs = 1728086400000L
    val queryEndTs = 1728166800000L
    val getResult = queryTiledData(entityKeyBytes, dataset, queryStartsTs, queryEndTs, tileSizeMillis)

    getResult.size shouldBe 1
    val expectedTiles = (queryStartsTs to queryEndTs by 1.hour.toMillis).toSeq
    validateTimedValuesResponse(getResult.head, expectedTiles, "Failed to read same day")
  }

  it should "streaming tiled query_days without data" in {
    val dataset = "DAYS_WITHOUT_DATA_V1_STREAMING"
    kvStoreImpl = setupStreamingDataset(dataset)

    val entityKeyBytes = "my_key".getBytes(StandardCharsets.UTF_8)
    val tileSizeMillis = 1.hour.toMillis
    val fakePayload = """{"name": "my_key", "my_feature_ir": "123"}"""

    // Write data from 10/04/24 00:00 to 10/16
    val dataStartTs = 1728000000000L
    val dataEndTs = 1729036800000L
    val tsRange = (dataStartTs until dataEndTs by 1.hour.toMillis)

    writeTiledData(entityKeyBytes, dataset, tsRange, tileSizeMillis, fakePayload)

    // Query in time range that extends beyond data: 10/15/24 00:00 to 10/30/24 00:00
    val queryStartsTs = 1728950400000L
    val queryEndTs = 1730246400000L
    val getResult = queryTiledData(entityKeyBytes, dataset, queryStartsTs, queryEndTs, tileSizeMillis)

    getResult.size shouldBe 1
    // We expect results to only cover the time range where we have data
    val expectedTiles = (queryStartsTs until dataEndTs by 1.hour.toMillis).toSeq
    validateTimedValuesResponse(getResult.head, expectedTiles, "Failed to read days without data")
  }

  it should "handle multiple entities with different time ranges in single query" in {
    val dataset = "DIFF_TIME_RANGES_V1_STREAMING"
    kvStoreImpl = setupStreamingDataset(dataset)

    val entity1Key = "entity1".getBytes(StandardCharsets.UTF_8)
    val entity2Key = "entity2".getBytes(StandardCharsets.UTF_8)
    val tileSizeMillis = 1.hour.toMillis
    val fakePayload1 = """{"name": "entity1", "feature": "value1"}"""
    val fakePayload2 = """{"name": "entity2", "feature": "value2"}"""

    // Generate hourly data from 10/01/24 00:00 to 10/10/24 00:00
    val tsRange = (1727740800000L until 1728518400000L by 1.hour.toMillis)

    val putRequests1 = tsRange.map { ts => createTilePutRequest(dataset, entity1Key, tileSizeMillis, ts, fakePayload1) }
    val putRequests2 = tsRange.map { ts => createTilePutRequest(dataset, entity2Key, tileSizeMillis, ts, fakePayload2) }

    Await.result(kvStoreImpl.multiPut(putRequests1), 1.minute)
    Await.result(kvStoreImpl.multiPut(putRequests2), 1.minute)

    // Query with different time ranges
    // Entity 1: 10/02/24 00:00 to 10/04/24 00:00
    val queryStartTs1 = 1727827200000L
    val queryEndTs1 = 1728000000000L

    // Entity 2: 10/05/24 00:00 to 10/07/24 00:00
    val queryStartTs2 = 1728086400000L
    val queryEndTs2 = 1728259200000L

    val readTileKey1 = TilingUtils.buildTileKey(dataset, entity1Key, Some(tileSizeMillis), None)
    val readKeyBytes1 = TilingUtils.serializeTileKey(readTileKey1)
    val readTileKey2 = TilingUtils.buildTileKey(dataset, entity2Key, Some(tileSizeMillis), None)
    val readKeyBytes2 = TilingUtils.serializeTileKey(readTileKey2)

    val getRequest1 = GetRequest(readKeyBytes1, dataset, Some(queryStartTs1), Some(queryEndTs1))
    val getRequest2 = GetRequest(readKeyBytes2, dataset, Some(queryStartTs2), Some(queryEndTs2))

    // Fetch both entities in a single multiGet call
    val getResults = Await.result(kvStoreImpl.multiGet(Seq(getRequest1, getRequest2)), 1.minute)

    // Verify we get results for both entities
    getResults.size shouldBe 2

    // Find responses by matching the request
    val result1 = getResults.find(_.request == getRequest1).get
    val result2 = getResults.find(_.request == getRequest2).get

    // Each should have data for their respective time ranges
    val expectedTimestamps1 = (queryStartTs1 to queryEndTs1 by 1.hour.toMillis).toSeq
    val expectedTimestamps2 = (queryStartTs2 to queryEndTs2 by 1.hour.toMillis).toSeq

    validateTimedValuesResponse(result1, expectedTimestamps1, "Failed to read entity1")
    validateTimedValuesResponse(result2, expectedTimestamps2, "Failed to read entity2")
  }

  it should "handle entities where some have data and others don't" in {
    val dataset = "SOME_WITH_DATA_V1_STREAMING"
    kvStoreImpl = setupStreamingDataset(dataset)

    val entity1Key = "entity1_with_data".getBytes(StandardCharsets.UTF_8)
    val entity2Key = "entity2_without_data".getBytes(StandardCharsets.UTF_8)
    val tileSizeMillis = 1.hour.toMillis
    val fakePayload1 = """{"name": "entity1", "feature": "value1"}"""

    // Generate hourly data from 10/04/24 00:00 to 10/06/24 00:00
    val tsRange = (1728000000000L until 1728172800000L by 1.hour.toMillis)

    // Write data only for entity1
    writeTiledData(entity1Key, dataset, tsRange, tileSizeMillis, fakePayload1)
    // entity2 has no data written

    // Query both entities with the same time range
    val queryStartTs = 1728043200000L
    val queryEndTs = 1728129600000L

    val readTileKey1 = TilingUtils.buildTileKey(dataset, entity1Key, Some(tileSizeMillis), None)
    val readKeyBytes1 = TilingUtils.serializeTileKey(readTileKey1)

    val readTileKey2 = TilingUtils.buildTileKey(dataset, entity2Key, Some(tileSizeMillis), None)
    val readKeyBytes2 = TilingUtils.serializeTileKey(readTileKey2)

    val getRequest1 = GetRequest(readKeyBytes1, dataset, Some(queryStartTs), Some(queryEndTs))
    val getRequest2 = GetRequest(readKeyBytes2, dataset, Some(queryStartTs), Some(queryEndTs))

    // Fetch both entities in a single multiGet call
    val getResults = Await.result(kvStoreImpl.multiGet(Seq(getRequest1, getRequest2)), 1.minute)

    // Verify we get results for both requests (even if one is empty)
    getResults.size shouldBe 2

    // Entity1 should have data
    val result1 = getResults.find(_.request == getRequest1).get
    val expectedTimestamps = (queryStartTs to queryEndTs by 1.hour.toMillis).toSeq
    validateTimedValuesResponse(result1, expectedTimestamps, "Failed to read entity1")

    // Entity2 should have no data (empty response)
    val result2 = getResults.find(_.request == getRequest2).get
    result2.values.isSuccess shouldBe true
    result2.values.get.isEmpty shouldBe true
  }

  // ===== gcOldBatchTables / enableTtl =====

  it should "enable TTL on table when enableTtl=true (default)" in {
    val dataset = "TTL_ENABLED_TABLE"
    val kvStore = new DynamoDBKVStoreImpl(client)
    kvStore.create(dataset)

    val ttlDesc = client.describeTimeToLive(
      software.amazon.awssdk.services.dynamodb.model.DescribeTimeToLiveRequest.builder().tableName(dataset).build()
    ).join().timeToLiveDescription()
    ttlDesc.timeToLiveStatus() shouldBe software.amazon.awssdk.services.dynamodb.model.TimeToLiveStatus.ENABLED
  }

  it should "not enable TTL on table when enableTtl=false" in {
    val dataset = "TTL_DISABLED_TABLE"
    val kvStore = new DynamoDBKVStoreImpl(client, Map(KvEnableTtlArg -> "false"))
    kvStore.create(dataset)

    val ttlDesc = client.describeTimeToLive(
      software.amazon.awssdk.services.dynamodb.model.DescribeTimeToLiveRequest.builder().tableName(dataset).build()
    ).join().timeToLiveDescription()
    ttlDesc.timeToLiveStatus() shouldBe software.amazon.awssdk.services.dynamodb.model.TimeToLiveStatus.DISABLED
  }

  it should "gcOldBatchTables deletes tables older than the GC threshold" in {
    val logicalName = "GC_CLEANUP_TEST"
    val kvStore = new DynamoDBKVStoreImpl(client)

    // threshold + 10 days old — should be deleted
    val oldDate = LocalDate.now().minusDays(DynamoDBKVStoreConstants.BatchTableGCAgeDays + 10)
    val fmt = DynamoDBKVStoreConstants.BatchTableDateFormatter
    val oldTableName = s"${logicalName}_${oldDate.format(fmt)}_1000000"
    kvStore.create(oldTableName)

    // threshold - 10 days old — should NOT be deleted
    val recentDate = LocalDate.now().minusDays(DynamoDBKVStoreConstants.BatchTableGCAgeDays - 10)
    val recentTableName = s"${logicalName}_${recentDate.format(fmt)}_2000000"
    kvStore.create(recentTableName)

    kvStore.gcOldBatchTables(logicalName)

    val tables = client.listTables().join().tableNames()
    tables.contains(oldTableName) shouldBe false
    tables.contains(recentTableName) shouldBe true
  }

  it should "gcOldBatchTables respects the BatchTableGCMaxDelete limit" in {
    val logicalName = "GC_LIMIT_TEST"
    val kvStore = new DynamoDBKVStoreImpl(client)
    val fmt = DynamoDBKVStoreConstants.BatchTableDateFormatter

    // Create BatchTableGCMaxDelete + 5 tables all older than the GC threshold (threshold + 5 days)
    val total = DynamoDBKVStoreConstants.BatchTableGCMaxDelete + 5
    val oldDate = LocalDate.now().minusDays(DynamoDBKVStoreConstants.BatchTableGCAgeDays + 5)
    (1 to total).foreach { i =>
      kvStore.create(s"${logicalName}_${oldDate.format(fmt)}_${i}000000")
    }

    kvStore.gcOldBatchTables(logicalName)

    val remaining = client.listTables().join().tableNames().toScala.filter(_.startsWith(logicalName))
    // At most BatchTableGCMaxDelete deleted, so at least 5 remain
    remaining.length should be >= (total - DynamoDBKVStoreConstants.BatchTableGCMaxDelete)
    remaining.length should be <= total
  }

  it should "gcOldBatchTables does not delete tables with malformed date in name" in {
    val logicalName = "GC_MALFORMED_TEST"
    val kvStore = new DynamoDBKVStoreImpl(client)

    // Table with unparseable date segment — should be left alone
    val malformedName = s"${logicalName}_NOTADATE_12345"
    kvStore.create(malformedName)

    kvStore.gcOldBatchTables(logicalName)

    val tables = client.listTables().join().tableNames()
    tables.contains(malformedName) shouldBe true
  }

  it should "gcOldBatchTables does not delete tables belonging to a different logical dataset" in {
    val logicalName = "GC_ISOLATION_A"
    val otherLogicalName = "GC_ISOLATION_B"
    val kvStore = new DynamoDBKVStoreImpl(client)
    val fmt = DynamoDBKVStoreConstants.BatchTableDateFormatter

    val oldDate = LocalDate.now().minusDays(DynamoDBKVStoreConstants.BatchTableGCAgeDays + 10)
    val tableA = s"${logicalName}_${oldDate.format(fmt)}_1000000"
    val tableB = s"${otherLogicalName}_${oldDate.format(fmt)}_1000000"
    kvStore.create(tableA)
    kvStore.create(tableB)

    kvStore.gcOldBatchTables(logicalName)

    val tables = client.listTables().join().tableNames()
    tables.contains(tableA) shouldBe false  // GC'd
    tables.contains(tableB) shouldBe true   // different dataset, untouched
  }

  it should "gcOldBatchTables swallows errors and does not throw" in {
    // Use a dataset name for which there are no matching tables — GC should complete silently
    val kvStore = new DynamoDBKVStoreImpl(client)
    kvStore.gcOldBatchTables("NONEXISTENT_DATASET_XYZ")
  }

  it should "gcOldBatchTables works correctly when a table prefix is configured" in {
    val tablePrefix = "MY_PREFIX_"
    val logicalName = "GC_PREFIX_TEST"
    val kvStore = new DynamoDBKVStoreImpl(client, Map(KvTablePrefixArg -> tablePrefix))
    val fmt = DynamoDBKVStoreConstants.BatchTableDateFormatter

    // threshold + 10 days old — should be deleted
    val oldDate = LocalDate.now().minusDays(DynamoDBKVStoreConstants.BatchTableGCAgeDays + 10)
    val oldTableName = s"${logicalName}_${oldDate.format(fmt)}_1000000"
    kvStore.create(oldTableName)

    // threshold - 10 days old — should NOT be deleted
    val recentDate = LocalDate.now().minusDays(DynamoDBKVStoreConstants.BatchTableGCAgeDays - 10)
    val recentTableName = s"${logicalName}_${recentDate.format(fmt)}_2000000"
    kvStore.create(recentTableName)

    kvStore.gcOldBatchTables(logicalName)

    // Verify via the raw client (which sees physical prefixed names)
    val physicalTables = client.listTables().join().tableNames()
    physicalTables.contains(tablePrefix + oldTableName) shouldBe false
    physicalTables.contains(tablePrefix + recentTableName) shouldBe true
  }

  it should "gcOldBatchTables does not run when enableTtl=false" in {
    val logicalName = "GC_DISABLED_TTL_TEST"
    val kvStore = new DynamoDBKVStoreImpl(client, Map(KvEnableTtlArg -> "false"))
    val fmt = DynamoDBKVStoreConstants.BatchTableDateFormatter

    // threshold + 10 days old — would be deleted if GC ran
    val oldDate = LocalDate.now().minusDays(DynamoDBKVStoreConstants.BatchTableGCAgeDays + 10)
    val oldTableName = s"${logicalName}_${oldDate.format(fmt)}_1000000"
    kvStore.create(oldTableName)

    kvStore.gcOldBatchTables(logicalName)

    // Table must still exist — GC is skipped when TTL is disabled
    val tables = client.listTables().join().tableNames()
    tables.contains(oldTableName) shouldBe true
  }

  private def buildModelPutRequest(model: Model, dataset: String): PutRequest = {
    val keyBytes = modelKeyEncoder(model)
    val valueBytes = modelValueEncoder(model)
    PutRequest(keyBytes, valueBytes, dataset, None)
  }

  private def buildModelGetRequest(model: Model, dataset: String): GetRequest = {
    val keyBytes = modelKeyEncoder(model)
    GetRequest(keyBytes, dataset, None, None)
  }

  private def validateExpectedModelResponse(expectedModel: Model, response: Seq[GetResponse]): Unit = {
    response.length shouldBe 1
    for (
      tSeq <- response.head.values;
      tv <- tSeq
    ) {
      tSeq.length shouldBe 1
      val jsonStr = new String(tv.bytes, StandardCharsets.UTF_8)
      val returnedModel = objectMapper.readValue(jsonStr, classOf[Model])
      returnedModel shouldBe expectedModel
    }
  }

  private def validateExpectedListResponse(response: Try[Seq[ListValue]], maxElements: Int): Unit = {
    response match {
      case Success(mSeq) =>
        mSeq.length <= maxElements shouldBe true
        mSeq.foreach { modelKV =>
          val jsonStr = new String(modelKV.valueBytes, StandardCharsets.UTF_8)
          val returnedModel = objectMapper.readValue(jsonStr, classOf[Model])
          val returnedKeyJsonStr = new String(modelKV.keyBytes, StandardCharsets.UTF_8)
          val returnedKey = objectMapper.readValue(returnedKeyJsonStr, classOf[String])
          returnedModel.modelId shouldBe returnedKey
        }
      case Failure(exception) =>
        fail(s"List response failed with exception: $exception")
    }
  }

  private def setupStreamingDataset(dataset: String): DynamoDBKVStoreImpl = {
    val kvStore = new DynamoDBKVStoreImpl(client)
    kvStore.create(dataset)
    kvStore
  }

  private def writeTiledData(entityKey: Array[Byte],
                             dataset: String,
                             tsRange: Seq[Long],
                             tileSizeMillis: Long,
                             payload: String): Unit = {
    val putRequests = tsRange.map { ts =>
      val tileKey = TilingUtils.buildTileKey(dataset, entityKey, Some(tileSizeMillis), Some(ts))
      val tileKeyBytes = TilingUtils.serializeTileKey(tileKey)
      PutRequest(tileKeyBytes, payload.getBytes(StandardCharsets.UTF_8), dataset, Some(ts))
    }
    Await.result(kvStoreImpl.multiPut(putRequests), 1.minute)
  }

  private def queryTiledData(entityKey: Array[Byte],
                             dataset: String,
                             startTs: Long,
                             endTs: Long,
                             tileSizeMillis: Long): Seq[GetResponse] = {
    val readTileKey = TilingUtils.buildTileKey(dataset, entityKey, Some(tileSizeMillis), None)
    val readKeyBytes = TilingUtils.serializeTileKey(readTileKey)
    val getRequest = GetRequest(readKeyBytes, dataset, Some(startTs), Some(endTs))
    Await.result(kvStoreImpl.multiGet(Seq(getRequest)), 1.minute)
  }

  private def validateTimedValuesResponse(response: GetResponse,
                                          expectedTimestamps: Seq[Long],
                                          errorMsg: String): Unit = {
    response.values match {
      case Success(timedValues) =>
        timedValues.map(_.millis).toSet shouldBe expectedTimestamps.toSet
        timedValues.length shouldBe expectedTimestamps.length
      case Failure(ex) =>
        fail(s"$errorMsg: $ex")
    }
  }

  // Global Tables replication tests.
  // DynamoDB Local does not support UpdateTable for replica operations, so we use SpyingDynamoDBKVStore
  // (see DynamoDBKVStoreTestUtils.scala) to intercept addReplicaRegions calls and assert on their
  // arguments without hitting the unsupported API.

  it should "call addReplicaRegions with correct table name and regions on create" in {
    val conf = Map(KvReplicaRegionsArg -> "us-west-2,eu-west-1", KvEnableTtlArg -> "false")
    val kvStore = new SpyingDynamoDBKVStore(client, conf)
    val dataset = "REPLICA_CALL_VERIFIED_TABLE"
    kvStore.create(dataset)
    client.listTables().join().tableNames().contains(dataset) shouldBe true
    kvStore.replicaCalls.length shouldBe 1
    kvStore.replicaCalls.head._1 shouldBe dataset
    kvStore.replicaCalls.head._2 shouldBe List("us-west-2", "eu-west-1")
  }

  it should "not call addReplicaRegions when no replica regions configured" in {
    val conf = Map(KvEnableTtlArg -> "false")
    val kvStore = new SpyingDynamoDBKVStore(client, conf)
    kvStore.create("NO_REPLICA_REGIONS_TABLE")
    kvStore.replicaCalls shouldBe empty
  }

  it should "not call addReplicaRegions when replica regions config is empty string" in {
    val conf = Map(KvReplicaRegionsArg -> "", KvEnableTtlArg -> "false")
    val kvStore = new SpyingDynamoDBKVStore(client, conf)
    kvStore.create("EMPTY_REPLICA_REGIONS_TABLE")
    kvStore.replicaCalls shouldBe empty
  }

  it should "call addReplicaRegions after TTL when both are configured" in {
    // TTL is applied via real DynamoDB Local; addReplicaRegions is intercepted by spy
    val conf = Map(KvReplicaRegionsArg -> "eu-west-1", KvEnableTtlArg -> "true")
    val kvStore = new SpyingDynamoDBKVStore(client, conf)
    val dataset = "TTL_AND_REPLICA_SPY_TABLE"
    kvStore.create(dataset)
    client.listTables().join().tableNames().contains(dataset) shouldBe true
    import software.amazon.awssdk.services.dynamodb.model.DescribeTimeToLiveRequest
    val ttlResponse =
      client.describeTimeToLive(DescribeTimeToLiveRequest.builder().tableName(dataset).build()).join()
    ttlResponse.timeToLiveDescription().timeToLiveStatus().toString shouldBe "ENABLED"
    // addReplicaRegions called exactly once, after TTL
    kvStore.replicaCalls.length shouldBe 1
    kvStore.replicaCalls.head._1 shouldBe dataset
    kvStore.replicaCalls.head._2 shouldBe List("eu-west-1")
  }

  private def validatePutResults(results: Seq[Boolean], expectedCount: Int): Unit = {
    results.length shouldBe expectedCount
    results.foreach(r => r shouldBe true)
  }

  private def createTilePutRequest(dataset: String,
                                  entityKey: Array[Byte],
                                  tileSizeMillis: Long,
                                  timestamp: Long,
                                  payload: String): PutRequest = {
    val tileKey = TilingUtils.buildTileKey(dataset, entityKey, Some(tileSizeMillis), Some(timestamp))
    val tileKeyBytes = TilingUtils.serializeTileKey(tileKey)
    PutRequest(tileKeyBytes, payload.getBytes(StandardCharsets.UTF_8), dataset, Some(timestamp))
  }

  private def validateSingleValueResponse(response: Seq[GetResponse],
                                         expectedValue: String,
                                         errorMsg: String): Unit = {
    response.length shouldBe 1
    response.head.values match {
      case Success(timedValues) =>
        timedValues.length shouldBe 1
        new String(timedValues.head.bytes, StandardCharsets.UTF_8) shouldBe expectedValue
      case Failure(ex) =>
        fail(s"$errorMsg: $ex")
    }
  }

  private def validateTimestampRange(timedValues: Seq[TimedValue], startTs: Long, endTs: Long): Unit = {
    timedValues.foreach { tv =>
      (tv.millis >= startTs) shouldBe true
      (tv.millis <= endTs) shouldBe true
    }
  }
}
