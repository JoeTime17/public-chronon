package ai.chronon.spark.submission

import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.client.{WebClient, WebClientOptions}
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.{CompletableFuture, ExecutionException, TimeUnit, TimeoutException}

case class CrucibleApiException(message: String, statusCode: Option[Int] = None, cause: Throwable = null)
    extends RuntimeException(message, cause)

case class CrucibleJobStatus(status: String,
                             httpCode: Int,
                             sparkApplicationId: Option[String] = None,
                             historyUrl: Option[String] = None)

class CrucibleClient(val baseUrl: String, val namespace: String) {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  private val timeoutSeconds = 60L
  private val vertx: Vertx = Vertx.vertx()
  private val client: WebClient = {
    val options = new WebClientOptions()
      .setConnectTimeout(10000)
      .setIdleTimeout(60)
    WebClient.create(vertx, options)
  }

  private val apiBase = s"/api/v1/namespaces/$namespace"

  def submitJob(body: JsonObject): String = {
    val uri = s"$apiBase/jobs"
    logger.info(s"Submitting job to Crucible: ${body.getString("name", "unnamed")} type=${body.getString("type")}")

    val future = new CompletableFuture[String]()
    client
      .postAbs(s"$baseUrl$uri")
      .putHeader("Content-Type", "application/json")
      .sendJsonObject(
        body,
        ar => {
          if (ar.succeeded()) {
            val response = ar.result()
            if (response.statusCode() >= 200 && response.statusCode() < 300) {
              try {
                val respBody = new JsonObject(response.bodyAsString())
                val jobId = Option(respBody.getString("id")).filter(_.nonEmpty).getOrElse {
                  throw new IllegalStateException("Crucible submit response missing required field 'id'")
                }
                logger.info(s"Job submitted successfully. ID: $jobId")
                future.complete(jobId)
              } catch {
                case e: Exception =>
                  val errorMsg = s"Invalid Crucible submit response: ${e.getMessage}"
                  logger.error(errorMsg, e)
                  future.completeExceptionally(CrucibleApiException(errorMsg, Some(response.statusCode()), e))
              }
            } else {
              val errorMsg = s"Failed to submit job: HTTP ${response.statusCode()} - ${response.bodyAsString()}"
              logger.error(errorMsg)
              future.completeExceptionally(CrucibleApiException(errorMsg, Some(response.statusCode())))
            }
          } else {
            val errorMsg = s"Failed to submit job: ${ar.cause().getMessage}"
            logger.error(errorMsg, ar.cause())
            future.completeExceptionally(CrucibleApiException(errorMsg, cause = ar.cause()))
          }
        }
      )

    await(future, "submitting Crucible job")
  }

  def getJobStatus(jobId: String): CrucibleJobStatus = {
    val uri = s"$apiBase/jobs/$jobId"
    val future = new CompletableFuture[CrucibleJobStatus]()

    client
      .getAbs(s"$baseUrl$uri")
      .send(ar => {
        if (ar.succeeded()) {
          val response = ar.result()
          if (response.statusCode() == 200) {
            try {
              val respBody = new JsonObject(response.bodyAsString())
              val status = Option(respBody.getString("status")).filter(_.nonEmpty).getOrElse {
                throw new IllegalStateException("Crucible status response missing required field 'status'")
              }
              future.complete(
                CrucibleJobStatus(
                  status = status,
                  httpCode = 200,
                  sparkApplicationId = Option(respBody.getString("sparkApplicationId")).filter(_.nonEmpty),
                  historyUrl = Option(respBody.getString("historyUrl")).filter(_.nonEmpty)
                )
              )
            } catch {
              case e: Exception =>
                val errorMsg = s"Invalid Crucible status response for job $jobId: ${e.getMessage}"
                logger.error(errorMsg, e)
                future.completeExceptionally(CrucibleApiException(errorMsg, Some(response.statusCode()), e))
            }
          } else {
            future.complete(CrucibleJobStatus(statusLabel(response.statusCode()), response.statusCode()))
          }
        } else {
          val errorMsg = s"Failed to get job status: ${ar.cause().getMessage}"
          logger.error(errorMsg, ar.cause())
          future.completeExceptionally(CrucibleApiException(errorMsg, cause = ar.cause()))
        }
      })

    await(future, s"getting Crucible status for job $jobId")
  }

  def killJob(jobId: String): Unit = {
    val uri = s"$apiBase/jobs/$jobId"
    logger.info(s"Killing job $jobId")

    val future = new CompletableFuture[Unit]()
    client
      .deleteAbs(s"$baseUrl$uri")
      .send(ar => {
        if (ar.succeeded()) {
          val response = ar.result()
          if (response.statusCode() >= 200 && response.statusCode() < 300) {
            logger.info(s"Kill request sent for job $jobId: HTTP ${response.statusCode()}")
            future.complete(())
          } else {
            val errorMsg = s"Failed to kill job $jobId: HTTP ${response.statusCode()} - ${response.bodyAsString()}"
            logger.error(errorMsg)
            future.completeExceptionally(CrucibleApiException(errorMsg, Some(response.statusCode())))
          }
        } else {
          val errorMsg = s"Failed to kill job: ${ar.cause().getMessage}"
          logger.error(errorMsg, ar.cause())
          future.completeExceptionally(CrucibleApiException(errorMsg, cause = ar.cause()))
        }
      })

    await(future, s"killing Crucible job $jobId")
  }

  private def await[T](future: CompletableFuture[T], action: String): T =
    try {
      future.get(timeoutSeconds, TimeUnit.SECONDS)
    } catch {
      case e: ExecutionException =>
        throw Option(e.getCause).getOrElse(e)
      case e: TimeoutException =>
        throw CrucibleApiException(s"Timed out after ${timeoutSeconds}s while $action", cause = e)
    }

  private def statusLabel(statusCode: Int): String =
    statusCode match {
      case 404                               => "NOT_FOUND"
      case code if code >= 400 && code < 500 => "CLIENT_ERROR"
      case code if code >= 500 && code < 600 => "SERVER_ERROR"
      case _                                 => "ERROR"
    }

  def close(): Unit = {
    client.close()
    vertx.close()
  }
}
