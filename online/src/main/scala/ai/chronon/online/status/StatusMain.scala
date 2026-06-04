package ai.chronon.online.status

import ai.chronon.api.{GroupBy, ThriftJsonCodec}
import ai.chronon.api.Extensions._
import ai.chronon.online.Api
import ai.chronon.online.fetcher.{FetchContext, MetadataStore}
import com.google.gson.{JsonObject, JsonParser}
import org.rogach.scallop.{ScallopConf, ScallopOption}
import org.slf4j.{Logger, LoggerFactory}
import sttp.client3._

import java.io.File
import scala.jdk.CollectionConverters._
import scala.reflect.internal.util.ScalaClassLoader
import scala.util.{Failure, Success, Try}

case class FlinkAccess(url: String, authHeaders: Map[String, String] = Map.empty)

object StatusMain {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  private val FreshnessMetricSuffix = "event_created_to_sink_time"
  private val HealthyCheckpointThreshold = 3
  private val HttpTimeoutMs = 30000

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
    val flinkUrl: ScallopOption[String] = opt[String](
      required = false,
      name = "flink-url",
      descr = "Flink Job Manager REST URL (required for streaming mode)."
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
    val result = run(baseArgs)
    println(result)
    val exitCode = if (result.contains(""""error":""")) 1 else 0
    System.exit(exitCode)
  }

  private[online] def run(baseArgs: Array[String]): String = {
    val args = new Args(baseArgs)
    if (args.enableDebug()) logger.info("Debug logging enabled")
    val fullConfPath = s"${args.repo()}/${args.confPath()}"
    val groupBy = ThriftJsonCodec.fromJsonFile[GroupBy](fullConfPath, check = false)

    args.mode() match {
      case "upload-to-kv" => uploadToKvStatus(groupBy, args)
      case "streaming"    => streamingStatus(groupBy, args)
    }
  }

  private[online] def loadApi(args: Args): Api = {
    val cls = if (args.onlineJar.isDefined) {
      val urls = Array(new File(args.onlineJar()).toURI.toURL).toIndexedSeq
      val cl = ScalaClassLoader.fromURLs(urls, StatusMain.getClass.getClassLoader)
      cl.loadClass(args.onlineClass())
    } else {
      Class.forName(args.onlineClass())
    }
    val constructor = cls.getConstructors.apply(0)
    constructor.newInstance(args.extraProps).asInstanceOf[Api]
  }

  private[online] def uploadToKvStatus(groupBy: GroupBy, args: Args): String = {
    require(
      args.onlineClass.isDefined,
      "--online-class is required for upload-to-kv mode"
    )
    val api = loadApi(args)
    val metadataStore = new MetadataStore(FetchContext(api.genKvStore))

    Try(metadataStore.getGroupByServingInfo(groupBy.metaData.name)).flatten match {
      case Success(info) =>
        s"""{"batchEndDate":"${info.batchEndDate}"}"""
      case Failure(e) =>
        val msg = s"Failed to get serving info for ${groupBy.metaData.name}. " +
          "Make sure batch upload has completed successfully."
        logger.error(msg, e)
        s"""{"error":"$msg"}"""
    }
  }

  private[online] def resolveFlinkAccess(groupByName: String, args: Args): FlinkAccess = {
    if (args.flinkUrl.isDefined) {
      FlinkAccess(args.flinkUrl().stripSuffix("/"))
    } else if (args.onlineClass.isDefined) {
      val api = loadApi(args)
      api.resolveFlinkUrl(groupByName) match {
        case Some(url) =>
          logger.info(s"Auto-discovered Flink URL for $groupByName: $url")
          FlinkAccess(url.stripSuffix("/"), api.flinkAuthHeaders)
        case None =>
          throw new RuntimeException(
            s"Could not auto-discover Flink URL for $groupByName. " +
              "Provide --flink-url explicitly or ensure a Flink job is running."
          )
      }
    } else {
      throw new IllegalArgumentException(
        "--flink-url or --online-class is required for streaming mode"
      )
    }
  }

  private[online] def streamingStatus(groupBy: GroupBy, args: Args): String = {
    val groupByName = groupBy.metaData.name
    val access = resolveFlinkAccess(groupByName, args)
    logger.info(s"Querying Flink REST API at: ${access.url}")
    if (access.authHeaders.nonEmpty) logger.info("Using authenticated requests")

    Try {
      val backend = HttpClientSyncBackend()
      try {
        val jobId = fetchRunningFlinkJobId(backend, access)
        val checkpoints = fetchCheckpointCounts(backend, access, jobId)
        val freshnessValues = fetchFreshnessMetrics(backend, access, jobId, groupByName)
        val healthy = checkpoints >= HealthyCheckpointThreshold

        val result = new JsonObject()
        val metrics = new JsonObject()
        val freshness = new JsonObject()
        for (suffix <- Seq("p99", "p95", "mean")) {
          freshnessValues.get(suffix) match {
            case Some(v) => freshness.addProperty(suffix, v)
            case None    => freshness.add(suffix, com.google.gson.JsonNull.INSTANCE)
          }
        }
        metrics.add("event_created_to_sink_time", freshness)
        result.add("metrics", metrics)
        result.addProperty("completedCheckpoints", checkpoints)
        result.addProperty("healthy", healthy)
        result.addProperty("flinkJobId", jobId)
        result.toString
      } finally {
        backend.close()
      }
    } match {
      case Success(json) => json
      case Failure(e) =>
        logger.error(s"Failed to get streaming status for $groupByName", e)
        val msg = Option(e.getMessage).getOrElse(e.getClass.getName)
        s"""{"error":"${msg.replace("\"", "\\\"")}"}"""
    }
  }

  private def flinkRequest(access: FlinkAccess) = {
    var req = basicRequest.readTimeout(scala.concurrent.duration.Duration(HttpTimeoutMs, "ms"))
    access.authHeaders.foreach { case (k, v) => req = req.header(k, v) }
    req
  }

  private[online] def fetchRunningFlinkJobId(backend: SttpBackend[Identity, Any], access: FlinkAccess): String = {
    val response = flinkRequest(access)
      .get(uri"${access.url}/jobs")
      .send(backend)

    val body = response.body match {
      case Right(b) => b
      case Left(err) =>
        throw new RuntimeException(s"Failed to fetch Flink jobs: HTTP ${response.code} - $err")
    }

    logger.debug(s"Flink /jobs response (HTTP ${response.code}): ${body.take(500)}")

    if (body.trim.startsWith("<")) {
      throw new RuntimeException(
        s"Flink REST API at ${access.url}/jobs returned HTML instead of JSON (HTTP ${response.code}). " +
          "The YARN proxy may require authentication or is redirecting. " +
          "Try providing a direct Flink REST URL with --flink-url."
      )
    }

    parseRunningJobId(body)
  }

  private[online] def parseRunningJobId(jobsJson: String): String = {
    val root = JsonParser.parseString(jobsJson).getAsJsonObject
    val jobs = root.getAsJsonArray("jobs")
    if (jobs == null || jobs.size() == 0)
      throw new RuntimeException("No Flink jobs found")

    val runningJob = jobs.asScala
      .map(_.getAsJsonObject)
      .find(j => j.get("status").getAsString == "RUNNING")
      .getOrElse(throw new RuntimeException("No running Flink job found"))

    runningJob.get("id").getAsString
  }

  private[online] def fetchCheckpointCounts(
      backend: SttpBackend[Identity, Any],
      access: FlinkAccess,
      jobId: String
  ): Int = {
    val response = flinkRequest(access)
      .get(uri"${access.url}/jobs/$jobId/checkpoints")
      .send(backend)

    response.body match {
      case Right(b) => parseCheckpointCounts(b)
      case Left(err) =>
        logger.warn(s"Failed to fetch checkpoints for job $jobId: $err")
        0
    }
  }

  private[online] def parseCheckpointCounts(checkpointsJson: String): Int = {
    val root = JsonParser.parseString(checkpointsJson).getAsJsonObject
    val counts = root.getAsJsonObject("counts")
    if (counts == null) 0
    else counts.get("completed").getAsInt
  }

  private[online] def fetchFreshnessMetrics(
      backend: SttpBackend[Identity, Any],
      access: FlinkAccess,
      jobId: String,
      groupByName: String
  ): Map[String, Double] = {
    val verticesResponse = flinkRequest(access)
      .get(uri"${access.url}/jobs/$jobId")
      .send(backend)

    val vertices = verticesResponse.body match {
      case Right(b) => parseVertexIds(b)
      case Left(err) =>
        logger.warn(s"Failed to fetch job vertices: $err")
        return Map.empty
    }

    vertices.iterator
      .map { vertexId =>
        findAndFetchFreshnessForVertex(backend, access, jobId, vertexId, groupByName)
      }
      .collectFirst { case m if m.nonEmpty => m }
      .getOrElse(Map.empty)
  }

  private[online] def parseVertexIds(jobDetailJson: String): Seq[String] = {
    val root = JsonParser.parseString(jobDetailJson).getAsJsonObject
    val vertices = root.getAsJsonArray("vertices")
    if (vertices == null) Seq.empty
    else vertices.asScala.map(_.getAsJsonObject.get("id").getAsString).toSeq
  }

  private val FreshnessTargetSuffixes = Seq("_p99", "_p95", "_mean")

  private def findAndFetchFreshnessForVertex(
      backend: SttpBackend[Identity, Any],
      access: FlinkAccess,
      jobId: String,
      vertexId: String,
      groupByName: String
  ): Map[String, Double] = {
    val listResponse = flinkRequest(access)
      .get(uri"${access.url}/jobs/$jobId/vertices/$vertexId/metrics")
      .send(backend)

    val metricIds = listResponse.body match {
      case Right(b) => parseMetricIds(b)
      case Left(_)  => return Map.empty
    }

    val normalizedName = groupByName.replace(".", "_")
    val freshnessMetrics = metricIds.filter(_.contains(FreshnessMetricSuffix))
    if (freshnessMetrics.nonEmpty) {
      logger.debug(s"Vertex $vertexId freshness metrics: ${freshnessMetrics.mkString(", ")}")
    }

    val results = FreshnessTargetSuffixes.flatMap { suffix =>
      val label = suffix.stripPrefix("_")
      freshnessMetrics
        .find(id => id.contains(normalizedName) && id.endsWith(suffix))
        .flatMap { metricId =>
          val valueResponse = flinkRequest(access)
            .get(uri"${access.url}/jobs/$jobId/vertices/$vertexId/metrics?get=$metricId")
            .send(backend)
          valueResponse.body match {
            case Right(b) => parseMetricValue(b).map(label -> _)
            case Left(_)  => None
          }
        }
    }
    results.toMap
  }

  private[online] def parseMetricIds(metricsListJson: String): Seq[String] = {
    val arr = JsonParser.parseString(metricsListJson).getAsJsonArray
    if (arr == null) Seq.empty
    else arr.asScala.map(_.getAsJsonObject.get("id").getAsString).toSeq
  }

  private[online] def parseMetricValue(metricValueJson: String): Option[Double] = {
    val arr = JsonParser.parseString(metricValueJson).getAsJsonArray
    if (arr == null || arr.size() == 0) return None
    val obj = arr.get(0).getAsJsonObject
    val value = obj.get("value")
    if (value == null || value.isJsonNull) None
    else Try(value.getAsString.toDouble).toOption
  }
}
