package ai.chronon.spark.submission

import ai.chronon.api.JobStatusType
import ai.chronon.api.submission.JobSubmitterConstants._
import io.vertx.core.json.{JsonArray, JsonObject}
import org.slf4j.{Logger, LoggerFactory}

import java.io.File
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.Locale
import scala.concurrent.ExecutionContext

class CrucibleSubmitter(
    baseUrl: String,
    namespace: String,
    sparkImage: String,
    flinkImage: String,
    override val jarName: String = "cloud_gcp_deploy.jar",
    override val onlineClass: String = "",
    override val dqMetricsDataset: String = "",
    override val kvStoreApiProperties: Map[String, String] = Map.empty,
    storageClient: Option[StorageClient] = None,
    jarUriOverride: Option[String] = None,
    spot: Boolean = false
) extends JobSubmitter {

  @transient override lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  private val normalizedBaseUrl = baseUrl.stripSuffix("/")
  private val client = new CrucibleClient(normalizedBaseUrl, namespace)

  override def submit(
      jobType: JobType,
      submissionProperties: Map[String, String],
      jobProperties: Map[String, String],
      files: List[String],
      labels: Map[String, String],
      envVars: Map[String, String],
      args: String*
  ): String = {
    val mainClass = submissionProperties.getOrElse(MainClass, "")
    val requestedJarUri = submissionProperties.getOrElse(JarURI, "")
    val jarUri = jarUriOverride.getOrElse(requestedJarUri)
    val jobName = labels.getOrElse(MetadataName, s"chronon-${System.currentTimeMillis()}")

    val body = new JsonObject()
    body.put("name", sanitizeName(jobName))
    if (spot) {
      body.put("spot", true)
    }

    jobType match {
      case SparkJob =>
        body.put("type", "spark")
        body.put("image", sparkImage)
        body.put("mainClass", mainClass)
        body.put("jar", jarUri)

        val conf = jobProperties ++
          envVarsToSparkProperties(envVars) ++
          labels.map { case (k, v) => s"spark.zipline.label.$k" -> v }

        val confObj = new JsonObject()
        conf.foreach { case (k, v) => confObj.put(k, v) }
        if (jarUri.nonEmpty) {
          val localJarPath = localClasspathFor(jarUri)
          confObj.put("spark.driver.extraClassPath",
                      CrucibleSubmitter.appendClasspath(confObj.getString("spark.driver.extraClassPath"), localJarPath))
          confObj.put(
            "spark.executor.extraClassPath",
            CrucibleSubmitter.appendClasspath(confObj.getString("spark.executor.extraClassPath"), localJarPath))
        }
        if (confObj.size() > 0) {
          body.put("conf", confObj)
        }

      case FlinkJob =>
        body.put("type", "flink")
        body.put("image", flinkImage)
        body.put("mainClass", submissionProperties.getOrElse(MainClass, FlinkMainClass))
        body.put("jar", submissionProperties.getOrElse(FlinkMainJarURI, jarUri))

        val additionalJars = CrucibleSubmitter.flinkAdditionalJars(submissionProperties, jarUri)
        if (additionalJars.nonEmpty) {
          val jarsArray = new JsonArray()
          additionalJars.foreach(jarsArray.add)
          body.put("jars", jarsArray)
        }

        val flinkConf = new JsonObject()
        val sparkDriverEnvPrefix = "spark.kubernetes.driverEnv."
        (jobProperties ++ envVarsToSparkProperties(envVars)).foreach { case (k, v) =>
          flinkConf.put(k, v)
          if (k.startsWith(sparkDriverEnvPrefix)) {
            val envName = k.stripPrefix(sparkDriverEnvPrefix)
            flinkConf.put(s"containerized.master.env.$envName", v)
            flinkConf.put(s"containerized.taskmanager.env.$envName", v)
          }
        }
        flinkConf.remove("spark.driver.memory")
        val sparkMemOpts = "-Dspark.driver.memory=128m -Dspark.testing.reservedMemory=0"
        for (key <- Seq("env.java.opts.jobmanager", "env.java.opts.taskmanager")) {
          val existing = Option(flinkConf.getString(key)).getOrElse("")
          val sep = if (existing.nonEmpty) " " else ""
          flinkConf.put(key, existing + sep + sparkMemOpts)
        }
        submissionProperties.get(FlinkCheckpointUri).foreach { uri =>
          flinkConf.put("state.checkpoints.dir", uri)
          flinkConf.put("state.savepoints.dir", uri)
        }
        submissionProperties.get(SavepointUri).foreach(uri => flinkConf.put("execution.savepoint.path", uri))
        if (flinkConf.size() > 0) {
          body.put("conf", flinkConf)
        }
    }

    val appArgs = JobSubmitter.getApplicationArgs(jobType, args.toArray)
    if (appArgs.nonEmpty) {
      val argsArray = new JsonArray()
      appArgs.foreach(argsArray.add)
      body.put("args", argsArray)
    }
    submissionProperties.get(JobId).foreach(body.put("idempotencyKey", _))

    client.submitJob(body)
  }

  override def status(jobId: String): JobStatusType = {
    try {
      val job = client.getJobStatus(jobId)
      if (job.httpCode == 404) {
        logger.warn(s"Job $jobId not found in Crucible namespace $namespace")
        JobStatusType.UNKNOWN
      } else {
        mapStatus(job.status)
      }
    } catch {
      case e: CrucibleApiException =>
        logger.error(s"Error getting Crucible status for job $jobId: ${e.getMessage}")
        JobStatusType.UNKNOWN
      case e: Exception =>
        logger.error(s"Unexpected error getting Crucible status for job $jobId", e)
        JobStatusType.UNKNOWN
    }
  }

  override def kill(jobId: String): Unit = client.killJob(jobId)

  override def close(): Unit = client.close()

  override def getJobUrl(jobId: String): Option[String] =
    Some(jobUrl(jobId))

  override def getSparkUrl(jobId: String): Option[String] = {
    try {
      val job = client.getJobStatus(jobId)
      if (job.httpCode == 200) {
        job.historyUrl.orElse(Some(sparkUIUrl(jobId)))
      } else {
        Some(sparkUIUrl(jobId))
      }
    } catch {
      case e: Exception =>
        logger.warn(s"Failed to resolve Crucible Spark history URL for job $jobId; using live UI URL", e)
        Some(sparkUIUrl(jobId))
    }
  }

  override def getFlinkUrl(jobId: String): Option[String] =
    Some(flinkUIUrl(jobId))

  override def isClusterCreateNeeded(isLongRunning: Boolean): Boolean = false

  override def ensureClusterReady(clusterName: String, clusterConf: Option[Map[String, String]])(implicit
      ec: ExecutionContext): Option[String] = Some(clusterName)

  override def buildFlinkSubmissionProps(env: Map[String, String],
                                         version: String,
                                         artifactPrefix: String): Map[String, String] = {
    val flinkJarUri = s"$artifactPrefix/release/$version/jars/$flinkJarName"
    val flinkStateUri = env.getOrElse(
      "FLINK_STATE_URI",
      throw new IllegalArgumentException("FLINK_STATE_URI must be set for GROUP_BY_STREAMING"))
    val base = Map(
      FlinkMainJarURI -> flinkJarUri,
      FlinkCheckpointUri -> s"$flinkStateUri/checkpoints"
    )
    val enablePubSub = env.getOrElse("ENABLE_PUBSUB", "false").toBoolean
    val enableKinesis = env.getOrElse("ENABLE_KINESIS", "false").toBoolean
    base ++
      (if (enablePubSub)
         Map(FlinkPubSubConnectorJarURI -> s"$artifactPrefix/release/$version/jars/connectors_pubsub_deploy.jar")
       else Map.empty) ++
      (if (enableKinesis)
         Map(FlinkKinesisConnectorJarURI -> s"$artifactPrefix/release/$version/jars/connectors_kinesis_deploy.jar")
       else Map.empty)
  }

  override def getLatestCheckpointPath(flinkInternalJobId: String, flinkStateUri: String): Option[String] =
    storageClient.flatMap(sc => StorageClient.resolveLatestCheckpointPath(sc, flinkInternalJobId, flinkStateUri))

  override def resolveConfPath(stagedFileUri: String): String = stagedFileUri

  private def mapStatus(crucibleStatus: String): JobStatusType = crucibleStatus match {
    case "PENDING"   => JobStatusType.PENDING
    case "RUNNING"   => JobStatusType.RUNNING
    case "ARCHIVING" => JobStatusType.RUNNING
    case "COMPLETED" => JobStatusType.SUCCEEDED
    case "FAILED"    => JobStatusType.FAILED
    case "KILLED"    => JobStatusType.CANCELLED
    case other =>
      logger.warn(s"Unknown Crucible status: $other")
      JobStatusType.UNKNOWN
  }

  private def localClasspathFor(jarUri: String): String =
    if (jarUri.startsWith("local://")) jarUri.stripPrefix("local://")
    else "/opt/spark/work-dir/" + jarUri.split("/").last

  private def jobUrl(jobId: String): String =
    s"$normalizedBaseUrl/api/v1/namespaces/$namespace/jobs/$jobId"

  private def sparkUIUrl(jobId: String): String =
    s"$normalizedBaseUrl/jobs/$namespace/$jobId/ui"

  private def flinkUIUrl(jobId: String): String =
    s"$normalizedBaseUrl/flink/$namespace/$jobId/ui"

  private def sanitizeName(name: String): String = {
    val cleaned = name.toLowerCase
      .replaceAll("[^a-z0-9-]", "-")
      .replaceAll("-+", "-")
      .stripPrefix("-")
      .stripSuffix("-")
    val base = if (cleaned.nonEmpty) cleaned else "chronon"
    val maxLength = 52
    if (base.length <= maxLength) {
      base
    } else {
      val suffix = shortHash(name)
      val prefixLength = maxLength - suffix.length - 1
      base.take(prefixLength).stripSuffix("-") + "-" + suffix
    }
  }

  private def shortHash(value: String): String =
    MessageDigest
      .getInstance("SHA-1")
      .digest(value.getBytes(StandardCharsets.UTF_8))
      .take(4)
      .map(byte => f"${byte & 0xff}%02x")
      .mkString
}

object CrucibleSubmitter {

  private[submission] def appendClasspath(existing: String, localJarPath: String): String =
    Option(existing).filter(_.trim.nonEmpty).map(_ + File.pathSeparator + localJarPath).getOrElse(localJarPath)

  private[submission] def flinkAdditionalJars(submissionProperties: Map[String, String],
                                              jarUri: String): Seq[String] = {
    val jars = scala.collection.mutable.ArrayBuffer[String]()
    if (jarUri.nonEmpty) jars += jarUri
    submissionProperties.get(FlinkPubSubConnectorJarURI).foreach(jars += _)
    submissionProperties.get(FlinkKinesisConnectorJarURI).foreach(jars += _)
    submissionProperties.get(FlinkJarsUri).foreach(base => additionalFlinkJars(base).foreach(jars += _))
    submissionProperties
      .get(AdditionalJars)
      .foreach(_.split(",").map(_.trim).filter(_.nonEmpty).foreach(jars += _))
    jars.toSeq
  }

  private def envFlag(name: String): Boolean =
    sys.env.get(name).exists { value =>
      value.trim.toLowerCase(Locale.ROOT) match {
        case "1" | "true" | "yes" | "y" => true
        case _                          => false
      }
    }

  def fromEnv(storageClient: Option[StorageClient] = None): CrucibleSubmitter = {
    val baseUrl = sys.env.getOrElse("CRUCIBLE_URL",
                                    throw new IllegalArgumentException("CRUCIBLE_URL environment variable is required"))
    val namespace = sys.env.getOrElse("CRUCIBLE_NAMESPACE", "default")
    val sparkImage =
      sys.env.getOrElse("CRUCIBLE_SPARK_IMAGE", "us-docker.pkg.dev/crucible-io/crucible/spark:3.5-crucible-latest")
    val flinkImage =
      sys.env.getOrElse("CRUCIBLE_FLINK_IMAGE", "us-docker.pkg.dev/crucible-io/crucible/flink:1.19-crucible-latest")
    val jarNameVal = sys.env.getOrElse("CRUCIBLE_JAR_NAME", "cloud_gcp_deploy.jar")
    val onlineClassVal = sys.env.getOrElse("CHRONON_ONLINE_CLASS", "")
    val jarUriOverride = sys.env.get("CRUCIBLE_JAR_URI").filter(_.nonEmpty)
    val spot = envFlag("CRUCIBLE_SPOT_EXECUTORS") || envFlag("CRUCIBLE_SPOT")

    val kvProps = Seq(
      sys.env.get("GCP_PROJECT_ID").map("GCP_PROJECT_ID" -> _),
      sys.env.get("GCP_BIGTABLE_INSTANCE_ID").map("GCP_BIGTABLE_INSTANCE_ID" -> _),
      sys.env.get("GCP_REGION").map("GCP_REGION" -> _),
      sys.env.get("AWS_REGION").map("AWS_REGION" -> _),
      sys.env.get("AZURE_CLIENT_ID").map("AZURE_CLIENT_ID" -> _),
      sys.env.get("AZURE_TENANT_ID").map("AZURE_TENANT_ID" -> _),
      sys.env.get("AZURE_REGION").map("AZURE_REGION" -> _),
      sys.env.get("AZURE_STORAGE_ACCOUNT").map("AZURE_STORAGE_ACCOUNT" -> _),
      sys.env.get("AZURE_KEYVAULT_NAME").map("AZURE_KEYVAULT_NAME" -> _),
      sys.env.get("AZURE_KEYVAULT_URI").map("AZURE_KEYVAULT_URI" -> _),
      sys.env.get("SNOWFLAKE_PRIVATE_KEY_VAULT_URI").map("SNOWFLAKE_PRIVATE_KEY_VAULT_URI" -> _),
      sys.env.get("SNOWFLAKE_VAULT_URI").map("SNOWFLAKE_VAULT_URI" -> _)
    ).flatten.toMap

    new CrucibleSubmitter(
      baseUrl = baseUrl,
      namespace = namespace,
      sparkImage = sparkImage,
      flinkImage = flinkImage,
      jarName = jarNameVal,
      onlineClass = onlineClassVal,
      dqMetricsDataset = sys.env.getOrElse("CRUCIBLE_DQ_METRICS_DATASET", "DATA_QUALITY_METRICS"),
      kvStoreApiProperties = kvProps,
      storageClient = storageClient,
      jarUriOverride = jarUriOverride,
      spot = spot
    )
  }
}
