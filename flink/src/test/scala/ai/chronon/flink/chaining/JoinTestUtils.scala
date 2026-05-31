package ai.chronon.flink.chaining

import ai.chronon.api
import ai.chronon.api._
import ai.chronon.flink.SparkExpressionEvalFn
import ai.chronon.flink.deser.ProjectedEvent
import ai.chronon.flink.source.FlinkSource
import ai.chronon.online.KVStore.{GetRequest, GetResponse, PutRequest}
import ai.chronon.online.fetcher.{FetchContext, Fetcher, MetadataStore}
import ai.chronon.online.metrics.TTLCache
import ai.chronon.online.serde.SerDe
import ai.chronon.online._
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

import java.time.Duration
import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.util.Success

case class JoinTestEvent(user_id: String, listing_id: String, price: Double, created: Long)

class MockJoinSource(mockEvents: Seq[JoinTestEvent], sparkExprEvalFn: SparkExpressionEvalFn[JoinTestEvent])
  extends FlinkSource[ProjectedEvent] {

  implicit val parallelism: Int = 1

  override def getDataStream(topic: String, groupName: String)(
    env: StreamExecutionEnvironment,
    parallelism: Int): SingleOutputStreamOperator[ProjectedEvent] = {

    val watermarkStrategy = WatermarkStrategy
      .forBoundedOutOfOrderness[JoinTestEvent](Duration.ofSeconds(5))
      .withTimestampAssigner(new SerializableTimestampAssigner[JoinTestEvent] {
        override def extractTimestamp(event: JoinTestEvent, previousElementTimestamp: Long): Long = {
          event.created
        }
      })

    env
      .fromCollection(mockEvents.asJava)
      .assignTimestampsAndWatermarks(watermarkStrategy)
      .flatMap(sparkExprEvalFn)
      .map(e => ProjectedEvent(e, System.currentTimeMillis()))
  }
}

// Serializable implementations of Api, KVStore, Fetcher, and MetadataStore for testing purposes
// We primarily want to override the Fetcher and MetadataStore (for the join codec)
class TestApi extends Api(Map.empty) with Serializable {

  // Implement required abstract methods
  override def externalRegistry: ExternalSourceRegistry = null
  override def genMetricsKvStore(tableBaseName: String): KVStore = new TestKVStore()
  override def genEnhancedStatsKvStore(tableBaseName: String): KVStore = new TestKVStore()
  override def logResponse(resp: LoggableResponse): Unit = {}
  override def streamDecoder(groupByServingInfoParsed: GroupByServingInfoParsed): SerDe = null

  override def genKvStore: KVStore = new TestKVStore()

  override def buildFetcher(debug: Boolean = false,
                             callerName: String = null,
                             disableErrorThrows: Boolean = false,
                             joinConfTtlMillis: Long = TTLCache.DefaultTtlMillis,
                             joinCodecTtlMillis: Long = TTLCache.DefaultTtlMillis): Fetcher = {
    new TestFetcher(genKvStore)
  }
}

class TestKVStore extends KVStore with Serializable {
  override def get(request: GetRequest): Future[GetResponse] = {
    Future.successful(GetResponse(request, Success(Seq.empty)))
  }

  override def multiGet(requests: Seq[GetRequest]): Future[Seq[GetResponse]] = {
    Future.successful(requests.map(req => GetResponse(req, Success(Seq.empty))))
  }

  override def put(putRequest: PutRequest): Future[Boolean] = Future.successful(true)
  override def multiPut(putRequests: Seq[KVStore.PutRequest]): Future[Seq[Boolean]] = Future.successful(putRequests.map(_ => true))
  override def create(dataset: String): Unit = {}
  override def bulkPut(sourceOfflineTable: String, destinationOnlineDataSet: String, partition: String): Unit = {}
}

class TestFetcher(kvStore: KVStore) extends Fetcher(
  kvStore = kvStore,
  metaDataSet = "test_metadata",
  timeoutMillis = 5000L,
  logFunc = null,
  debug = false,
  externalSourceRegistry = null,
  callerName = "test",
  flagStore = null,
  disableErrorThrows = true,
  executionContextOverride = null
) with Serializable {

  override val metadataStore: MetadataStore = new TestMetadataStore()

  override def fetchJoin(requests: Seq[Fetcher.Request], joinConf: Option[Join] = None): Future[Seq[Fetcher.Response]] = {
    println(s"TestFetcher.fetchJoin called with ${requests.size} requests")
    requests.foreach { request =>
      println(s"Join request: name=${request.name}, keys=${request.keys}, atMillis=${request.atMillis}")
    }

    val responses = requests.map { request =>
      val listingId = request.keys.getOrElse("listing_id", "unknown").toString

      // Return enrichment values based on listing_id - parent join returns price_discounted_last for each listing
      val enrichmentValues = Map[String, AnyRef](
        "price_discounted_last" -> (listingId match {
          case "listing1" => 50.0.asInstanceOf[AnyRef]  // Last price for listing1
          case "listing2" => 150.0.asInstanceOf[AnyRef] // Last price for listing2
          case "listing3" => 25.0.asInstanceOf[AnyRef]  // Last price for listing3
          case _ => 50.0.asInstanceOf[AnyRef]
        })
      )

      println(s"Enrichment response for listing $listingId: $enrichmentValues")
      Fetcher.Response(request, Success(enrichmentValues))
    }

    println(s"TestFetcher returning ${responses.size} responses")
    Future.successful(responses)
  }
}

class TestMetadataStore extends MetadataStore(FetchContext(new TestKVStore(), null)) with Serializable {
  override def buildJoinCodec(join: Join, refreshOnFail: Boolean = false): JoinCodec = TestJoinCodec.build(join)
}

object TestJoinCodec {
  import ai.chronon.api.Extensions.JoinOps
  import ai.chronon.online.serde.{AvroCodec, AvroConversions}

  def build(parentJoin: Join): JoinCodec = {
    // Key schema: listing_id (from the GroupBy that the parent join queries)
    val keyFields = Array(StructField("listing_id", StringType))
    val keySchema = StructType("parent_join_key", keyFields)

    // Value schema: only price_last (the feature from GroupBy 1)
    val valueFields = Array(
      StructField("price_discounted_last", DoubleType)  // Feature from GroupBy 1
    )
    val valueSchema = StructType("parent_join_value", valueFields)

    val conf = new JoinOps(parentJoin)

    // Create proper Avro codecs using AvroConversions
    val keyAvroSchema = AvroConversions.fromChrononSchema(keySchema)
    val valueAvroSchema = AvroConversions.fromChrononSchema(valueSchema)

    val keyCodec = AvroCodec.of(keyAvroSchema.toString())
    val valueCodec = AvroCodec.of(valueAvroSchema.toString())

    new JoinCodec(conf,
                  keySchema,
                  valueSchema,
                  keyCodec,
                  valueCodec,
                  Array.empty,
                  joinPartKeyMappings = Map.empty,
                  hasPartialFailure = false)
  }
}

object JoinTestUtils {
  // Build our chained GroupBy.
  // We have:
  // Source: listing_views (user_id, listing_id, price, created)
  // GroupBy: listing_features (listing_id) -> last(price)
  // Join: parentJoin JOIN listing_features ON listing_id
  // GroupBy: user_price_features (user_id) -> last_k(price, k=10, window=1d)
  def buildJoinSourceTerminalGroupBy(): api.GroupBy = {
    val parentJoin = Builders.Join(
      left = Builders.Source.events(
        query = Builders.Query(
          selects = Map("user_id" -> "user_id", "listing_id" -> "listing_id", "price_discounted" -> "price * 0.95"),
          timeColumn = "created"
        ),
        table = "test.listing_views",
        topic = "kafka://test-topic/serde=custom/provider_class=ai.chronon.flink.deser.MockCustomSchemaProvider/schema_name=item_event"
      ),
      joinParts = Seq(
        // Mock join part representing listing enrichment
        Builders.JoinPart(
          groupBy = Builders.GroupBy(
            sources = Seq(Builders.Source.events(
              query = Builders.Query(),
              table = "test.listings"
            )),
            keyColumns = Seq("listing_id"),
            aggregations = Seq(
              Builders.Aggregation(
                operation = Operation.LAST,
                inputColumn = "price_discounted"
              )
            ),
            metaData = Builders.MetaData(name = "test.listing_features")
          )
        )
      ),
      metaData = Builders.MetaData(name = "test.upstream_join")
    )

    // Create the chaining GroupBy that uses the upstream join as source
    val chainGroupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.joinSource(
        join = parentJoin,
        query =
          Builders.Query(
            selects = Map(
              "user_id" -> "user_id",
              "listing_id" -> "listing_id",
              "final_price" -> "price_discounted_last",
            ),
          )
      )),
      keyColumns = Seq("user_id"),
      aggregations = Seq(
        Builders.Aggregation(
          operation = Operation.LAST_K,
          inputColumn = "final_price",  // Use simple price field
          argMap = Map("k" -> "10"),
          windows = Seq(new Window(1, TimeUnit.DAYS))
        )
      ),
      metaData = Builders.MetaData(name = "test.user_price_features"),
      accuracy = Accuracy.TEMPORAL
    )

    chainGroupBy
  }
}
