package ai.chronon.online.status

import ai.chronon.api.{GroupBy, ThriftJsonCodec}
import ai.chronon.api.Extensions._
import ai.chronon.online.{Api, TopicInfo}
import ai.chronon.online.fetcher.{FetchContext, MetadataStore}
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, OffsetSpec}
import org.rogach.scallop.{ScallopConf, ScallopOption}
import org.slf4j.{Logger, LoggerFactory}

import java.io.File
import java.util.Properties
import scala.collection.JavaConverters._
import scala.reflect.internal.util.ScalaClassLoader
import scala.util.{Failure, Success, Try}

object StatusMain {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  class Args(args: Array[String]) extends ScallopConf(args) {
    val mode: ScallopOption[String] = choice(
      Seq("upload-to-kv", "streaming"),
      required = true,
      descr = "Job mode to check status for."
    )
    val confPath: ScallopOption[String] = opt[String](
      required = true,
      name = "conf-path",
      descr = "Path to the compiled GroupBy conf relative to repo root."
    )
    val repo: ScallopOption[String] = opt[String](
      required = false,
      default = Some("."),
      descr = "Path to the chronon repo root."
    )
    // Required for upload-to-kv to instantiate the KVStore implementation
    val onlineJar: ScallopOption[String] = opt[String](
      required = false,
      name = "online-jar",
      descr = "Path to the jar containing the Api implementation (required for upload-to-kv)."
    )
    val onlineClass: ScallopOption[String] = opt[String](
      required = false,
      name = "online-class",
      descr = "Fully qualified Api implementation class (required for upload-to-kv)."
    )
    val enableDebug: ScallopOption[Boolean] = opt[Boolean](
      required = false,
      name = "enable-debug",
      default = Some(false),
      descr = "Enables verbose debug logging."
    )
    val extraProps: Map[String, String] = props[String]('Z')
    verify()
  }

  def main(baseArgs: Array[String]): Unit = {
    val args = new Args(baseArgs)
    if (args.enableDebug()) logger.info("Debug logging enabled")
    val fullConfPath = s"${args.repo()}/${args.confPath()}"
    val groupBy = ThriftJsonCodec.fromJsonFile[GroupBy](fullConfPath, check = false)

    val result = args.mode() match {
      case "upload-to-kv" => uploadToKvStatus(groupBy, args)
      case "streaming"    => streamingStatus(groupBy)
    }

    println(result)
    System.exit(0)
  }

  private def loadApi(args: Args): Api = {
    // Use the current classloader (class is on -cp), fall back to --online-jar if provided
    val cls = if (args.onlineJar.isDefined) {
      val urls = Array(new File(args.onlineJar()).toURI.toURL)
      val cl = ScalaClassLoader.fromURLs(urls, StatusMain.getClass.getClassLoader)
      cl.loadClass(args.onlineClass())
    } else {
      Class.forName(args.onlineClass())
    }
    val constructor = cls.getConstructors.apply(0)
    constructor.newInstance(args.extraProps).asInstanceOf[Api]
  }

  private def uploadToKvStatus(groupBy: GroupBy, args: Args): String = {
    require(
      args.onlineClass.isDefined,
      "--online-class is required for upload-to-kv mode"
    )
    val api = loadApi(args)
    val metadataStore = new MetadataStore(FetchContext(api.genKvStore))

    // TTLCache may throw directly instead of returning Failure, so wrap with Try and flatten
    Try(metadataStore.getGroupByServingInfo(groupBy.metaData.name)).flatten match {
      case Success(info) =>
        s"""{"batchEndDate":"${info.batchEndDate}"}"""
      case Failure(e) =>
        throw new RuntimeException(
          s"Failed to get serving info for ${groupBy.metaData.name}. " +
            "Make sure batch upload has completed successfully.",
          e
        )
    }
  }

  // TODO: currently only supports Kafka. Future work:
  //  - Pub/Sub: use oldest_unacked_message_age as lag metric
  //  - Kinesis: use Statistic.MAXIMUM on GetRecords.IteratorAgeMilliseconds as lag metric
  private def streamingStatus(groupBy: GroupBy): String = {
    val source = groupBy.streamingSource.getOrElse(
      throw new IllegalArgumentException(
        s"GroupBy ${groupBy.metaData.name} has no streaming source"
      )
    )
    val topicInfo = TopicInfo.parse(source.topic)

    require(
      topicInfo.messageBus == "kafka",
      s"streaming status only supports kafka, got: ${topicInfo.messageBus}"
    )

    val bootstrap = topicInfo.params
      .get("bootstrap")
      .orElse {
        for {
          host <- topicInfo.params.get("host")
          port <- topicInfo.params.get("port")
        } yield s"$host:$port"
      }
      .getOrElse(
        throw new IllegalArgumentException(
          s"No bootstrap server in topic params for ${groupBy.metaData.name}. " +
            "Expected 'bootstrap' or 'host'+'port' in the topic URI."
        )
      )

    val consumerGroup = s"chronon-${groupBy.metaData.name}"
    val lag = computeKafkaLag(topicInfo.name, bootstrap, consumerGroup, topicInfo.params)
    s"""{"lag":$lag}"""
  }

  // Lag = sum of (log-end-offset - consumer-offset) for partitions the consumer group has committed to.
  private def computeKafkaLag(
      topic: String,
      bootstrap: String,
      consumerGroup: String,
      additionalProps: Map[String, String]
  ): Long = {
    val props = new Properties()
    additionalProps.foreach { case (k, v) => props.put(k, v) }
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)

    val adminClient = AdminClient.create(props)
    try {
      val consumerOffsets = adminClient
        .listConsumerGroupOffsets(consumerGroup)
        .partitionsToOffsetAndMetadata()
        .get()
        .asScala
        .filter { case (tp, _) => tp.topic() == topic }

      if (consumerOffsets.isEmpty) {
        logger.warn(s"No committed offsets for consumer group '$consumerGroup' on topic '$topic'")
        return 0L
      }

      val endOffsets = adminClient
        .listOffsets(consumerOffsets.keys.map(_ -> OffsetSpec.latest()).toMap.asJava)
        .all()
        .get()
        .asScala

      consumerOffsets.map { case (tp, om) =>
        val endOffset = endOffsets.get(tp).map(_.offset()).getOrElse(om.offset())
        Math.max(0L, endOffset - om.offset())
      }.sum
    } finally {
      adminClient.close()
    }
  }
}
