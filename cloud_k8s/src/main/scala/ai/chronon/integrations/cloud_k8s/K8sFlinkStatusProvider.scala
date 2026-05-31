package ai.chronon.integrations.cloud_k8s

import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.client.WebClient
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.jdk.CollectionConverters._
import scala.util.Try

/** Cloud-agnostic Flink status provider for Kubernetes-hosted Flink deployments.
  *
  * Queries the Flink REST API over the nginx ingress URL to determine job health and fetch the
  * internal Flink job ID. The Flink REST API is cloud-agnostic; only the ingress URL origin differs
  * between AKS, EKS, and GKE deployments.
  */
class K8sFlinkStatusProvider {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  private lazy val client = {
    val vertx = Vertx.vertx()
    WebClient.create(vertx)
  }

  def getFlinkInternalJobId(flinkUriOpt: Option[String])(implicit
      executionContext: ExecutionContext): Future[Option[String]] =
    flinkUriOpt match {
      case Some(flinkUri) => fetchJobId(flinkUri)
      case None           => Future.successful(None)
    }

  def isFlinkJobHealthy(flinkUriOpt: Option[String])(implicit executionContext: ExecutionContext): Future[Boolean] =
    flinkUriOpt match {
      case Some(flinkUri) =>
        fetchJobId(flinkUri).flatMap {
          case Some(jobId) =>
            fetchCheckpointCounts(flinkUri, jobId).map {
              case Some(completedCount) =>
                val healthy = isHealthyBasedOnCheckpoints(completedCount)
                logger.info(s"Job $jobId has $completedCount completed checkpoints, isHealthy: $healthy")
                healthy
              case None =>
                logger.warn(s"Could not retrieve checkpoint counts for job $jobId")
                false
            }
          case None =>
            logger.warn(s"Could not retrieve / parse job ID from $flinkUri")
            Future.successful(false)
        }
      case None =>
        logger.warn("Flink URI not provided")
        Future.successful(false)
    }

  private def fetchJobId(flinkUri: String)(implicit ec: ExecutionContext): Future[Option[String]] =
    fetchJobsResponse(flinkUri).map(_.flatMap(parseJobId))

  private[cloud_k8s] def parseJobId(jobsResponseJson: String): Option[String] =
    Try {
      val jsonBody = new JsonObject(jobsResponseJson)
      val jobsArray = jsonBody.getJsonArray("jobs")
      if (jobsArray != null && jobsArray.size() > 0)
        jobsArray.asScala.headOption.map(_.asInstanceOf[JsonObject].getString("id"))
      else None
    }.toOption.flatten

  private def fetchCheckpointCounts(flinkUri: String, jobId: String)(implicit
      ec: ExecutionContext): Future[Option[Int]] =
    fetchCheckpointsResponse(flinkUri, jobId).map(_.flatMap(parseCheckpointCounts))

  private[cloud_k8s] def parseCheckpointCounts(checkpointResponseJson: String): Option[Int] =
    Try {
      val jsonBody = new JsonObject(checkpointResponseJson)
      val counts = jsonBody.getJsonObject("counts")
      if (counts != null) Some(counts.getInteger("completed", 0).intValue()) else None
    }.toOption.flatten

  private[cloud_k8s] def isHealthyBasedOnCheckpoints(completedCheckpoints: Int): Boolean =
    completedCheckpoints >= 3

  protected def fetchJobsResponse(flinkUri: String): Future[Option[String]] = {
    val promise = Promise[Option[String]]()
    val endpoint = s"$flinkUri/jobs"
    client
      .getAbs(endpoint)
      .putHeader("Content-Type", "application/json")
      .putHeader("Accept", "application/json")
      .timeout(K8sFlinkStatusProvider.RequestTimeoutMs)
      .send()
      .onComplete { ar =>
        if (ar.succeeded()) {
          val response = ar.result()
          if (response.statusCode() == 200) {
            val body = response.bodyAsString()
            logger.debug(s"GET $endpoint -> ${response.statusCode()}: $body")
            promise.success(Some(body))
          } else {
            logger.warn(
              s"Failed to retrieve Flink jobs from $endpoint, status: ${response.statusCode()} ${response.statusMessage()}")
            promise.success(None)
          }
        } else {
          logger.error(s"Error retrieving Flink jobs from $endpoint", ar.cause())
          promise.success(None)
        }
      }
    promise.future
  }

  protected def fetchCheckpointsResponse(flinkUri: String, jobId: String): Future[Option[String]] = {
    val promise = Promise[Option[String]]()
    val endpoint = s"$flinkUri/jobs/$jobId/checkpoints"
    client
      .getAbs(endpoint)
      .putHeader("Content-Type", "application/json")
      .putHeader("Accept", "application/json")
      .timeout(K8sFlinkStatusProvider.RequestTimeoutMs)
      .send()
      .onComplete { ar =>
        if (ar.succeeded()) {
          val response = ar.result()
          if (response.statusCode() == 200) {
            val body = response.bodyAsString()
            logger.debug(s"GET $endpoint -> ${response.statusCode()}: $body")
            promise.success(Some(body))
          } else {
            logger.warn(
              s"Failed to retrieve checkpoints from $endpoint, status: ${response.statusCode()} ${response.statusMessage()}")
            promise.success(None)
          }
        } else {
          logger.error(s"Error retrieving checkpoints from $endpoint", ar.cause())
          promise.success(None)
        }
      }
    promise.future
  }
}

object K8sFlinkStatusProvider {
  // Per-request HTTP timeout. Keeps individual health-check calls from hanging indefinitely
  // when the Flink REST endpoint is slow or unreachable.
  val RequestTimeoutMs: Long = 10000L
}
