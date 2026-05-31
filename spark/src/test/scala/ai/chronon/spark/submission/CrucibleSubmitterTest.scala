package ai.chronon.spark.submission

import ai.chronon.spark.submission.JobSubmitterConstants._
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets

class CrucibleSubmitterTest extends AnyFlatSpec with Matchers {

  private val submitter = new CrucibleSubmitter(
    baseUrl = "http://crucible",
    namespace = "test-ns",
    sparkImage = "spark-image",
    flinkImage = "flink-image"
  )

  it should "include kinesis connector jar when ENABLE_KINESIS is true" in {
    val props = submitter.buildFlinkSubmissionProps(
      env = Map(
        "FLINK_STATE_URI" -> "s3://warehouse/flink-state",
        "ENABLE_KINESIS" -> "true"
      ),
      version = "test-version",
      artifactPrefix = "s3://artifacts"
    )

    props(FlinkMainJarURI) shouldBe "s3://artifacts/release/test-version/jars/flink_assembly_deploy.jar"
    props(FlinkCheckpointUri) shouldBe "s3://warehouse/flink-state/checkpoints"
    props(FlinkKinesisConnectorJarURI) shouldBe "s3://artifacts/release/test-version/jars/connectors_kinesis_deploy.jar"
  }

  it should "include pubsub and kinesis connector jars independently" in {
    val props = submitter.buildFlinkSubmissionProps(
      env = Map(
        "FLINK_STATE_URI" -> "s3://warehouse/flink-state",
        "ENABLE_PUBSUB" -> "true",
        "ENABLE_KINESIS" -> "true"
      ),
      version = "test-version",
      artifactPrefix = "s3://artifacts"
    )

    props(FlinkPubSubConnectorJarURI) shouldBe "s3://artifacts/release/test-version/jars/connectors_pubsub_deploy.jar"
    props(FlinkKinesisConnectorJarURI) shouldBe "s3://artifacts/release/test-version/jars/connectors_kinesis_deploy.jar"
  }

  it should "return Spark history URL from Crucible job status when available" in {
    withCrucibleJobResponse(
      """{"status":"RUNNING","sparkApplicationId":"spark-app-1","historyUrl":"https://shs/history/spark-app-1/jobs/"}"""
    ) { baseUrl =>
      val submitter = new CrucibleSubmitter(
        baseUrl = baseUrl,
        namespace = "test-ns",
        sparkImage = "spark-image",
        flinkImage = "flink-image"
      )
      try {
        submitter.getSparkUrl("job-1") shouldBe Some("https://shs/history/spark-app-1/jobs/")
      } finally {
        submitter.close()
      }
    }
  }

  it should "return Spark history URL verbatim when colocated under the Crucible base prefix" in {
    withCrucibleJobResponse(
      """{"status":"COMPLETED","sparkApplicationId":"spark-app-1","historyUrl":"https://crucible.example.com/spark-history/history/spark-app-1/jobs/"}"""
    ) { baseUrl =>
      val submitter = new CrucibleSubmitter(
        baseUrl = baseUrl,
        namespace = "test-ns",
        sparkImage = "spark-image",
        flinkImage = "flink-image"
      )
      try {
        submitter.getSparkUrl("job-1") shouldBe Some(
          "https://crucible.example.com/spark-history/history/spark-app-1/jobs/")
      } finally {
        submitter.close()
      }
    }
  }

  it should "fall back to prefixed Crucible live Spark UI URL when Spark history URL is not yet available" in {
    withCrucibleJobResponse("""{"status":"PENDING"}""") { baseUrl =>
      val submitter = new CrucibleSubmitter(
        baseUrl = baseUrl,
        namespace = "test-ns",
        sparkImage = "spark-image",
        flinkImage = "flink-image"
      )
      try {
        submitter.getSparkUrl("job-1") shouldBe Some(s"$baseUrl/jobs/test-ns/job-1/ui")
      } finally {
        submitter.close()
      }
    }
  }

  it should "return prefixed Crucible live Flink UI URL" in {
    val submitter = new CrucibleSubmitter(
      baseUrl = "https://crucible.example.com/spark-history",
      namespace = "test-ns",
      sparkImage = "spark-image",
      flinkImage = "flink-image"
    )

    submitter.getFlinkUrl("flink-job-1") shouldBe Some(
      "https://crucible.example.com/spark-history/flink/test-ns/flink-job-1/ui")
  }

  it should "expand flink dependency jar base path for crucible submissions" in {
    val jars = CrucibleSubmitter.flinkAdditionalJars(
      submissionProperties = Map(
        FlinkPubSubConnectorJarURI -> "s3://artifacts/connectors_pubsub_deploy.jar",
        FlinkKinesisConnectorJarURI -> "s3://artifacts/connectors_kinesis_deploy.jar",
        FlinkJarsUri -> "s3://artifacts/flink-libs",
        AdditionalJars -> "s3://extra/one.jar, s3://extra/two.jar"
      ),
      jarUri = "s3://artifacts/cloud_aws_lib_deploy.jar"
    )

    jars.take(4) shouldBe Seq(
      "s3://artifacts/cloud_aws_lib_deploy.jar",
      "s3://artifacts/connectors_pubsub_deploy.jar",
      "s3://artifacts/connectors_kinesis_deploy.jar",
      "s3://artifacts/flink-libs/commons-collections4-4.4.jar"
    )
    jars should contain("s3://artifacts/flink-libs/spark-sql_2.12-3.5.3.jar")
    jars.takeRight(2) shouldBe Seq("s3://extra/one.jar", "s3://extra/two.jar")
  }

  private def withCrucibleJobResponse(responseJson: String)(test: String => Unit): Unit = {
    val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    server.createContext(
      "/api/v1/namespaces/test-ns/jobs/job-1",
      new HttpHandler {
        override def handle(exchange: HttpExchange): Unit = {
          val response = responseJson.getBytes(StandardCharsets.UTF_8)
          exchange.getResponseHeaders.add("Content-Type", "application/json")
          exchange.sendResponseHeaders(200, response.length.toLong)
          val body = exchange.getResponseBody
          try {
            body.write(response)
          } finally {
            body.close()
          }
        }
      }
    )
    server.start()
    try {
      test(s"http://127.0.0.1:${server.getAddress.getPort}")
    } finally {
      server.stop(0)
    }
  }
}
