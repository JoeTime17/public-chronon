package ai.chronon.integrations.cloud_azure

import ai.chronon.api.JobStatusType
import ai.chronon.integrations.cloud_k8s.{K8sFlinkStatusProvider, K8sFlinkSubmitter}
import ai.chronon.spark.submission.JobSubmitterConstants._
import ai.chronon.spark.submission.{
  JobSubmitter,
  JobType,
  KyuubiAuth,
  KyuubiSubmitter,
  StorageClient,
  FlinkJob => TypeFlinkJob,
  SparkJob => TypeSparkJob
}
import com.azure.storage.blob.BlobServiceClientBuilder

import java.time.Duration
import java.util.concurrent.Executors
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

/** Unified Azure job submitter for AKS.
  *
  * Dispatches Spark jobs to a Kyuubi server running on AKS, and Flink streaming jobs to the
  * Flink Kubernetes Operator via [[K8sFlinkSubmitter]] (built by [[AksFlinkSubmitter]]).
  *
  * Flink job IDs are prefixed with "flink:<namespace>:" to distinguish them from Spark batch IDs.
  */
class AzureSubmitter(
    kyuubiSubmitter: KyuubiSubmitter,
    aksFlinkSubmitter: K8sFlinkSubmitter,
    aksServiceAccount: Option[String] = None,
    aksNamespace: Option[String] = None,
    ingressBaseUrl: Option[String] = None,
    storageClient: Option[StorageClient] = None,
    flinkHealthCheckFn: Option[String] => Boolean = _ => true,
    flinkInternalJobIdFetchFn: Option[String] => Option[String] = _ => None,
    override val dqMetricsDataset: String = "",
    override val kvStoreApiProperties: Map[String, String] = Map.empty,
    override val jarName: String = "",
    override val onlineClass: String = ""
) extends JobSubmitter {

  override def submit(
      jobType: JobType,
      submissionProperties: Map[String, String],
      jobProperties: Map[String, String],
      files: List[String],
      labels: Map[String, String],
      envVars: Map[String, String],
      args: String*
  ): String = {
    jobType match {
      case TypeSparkJob =>
        // Pass raw args — KyuubiSubmitter.submit runs its own getApplicationArgs
        // normalization internally, so pre-filtering here would be a double-filter.
        kyuubiSubmitter.submit(jobType, submissionProperties, jobProperties, files, labels, envVars, args: _*)

      case TypeFlinkJob =>
        // K8sFlinkSubmitter doesn't normalize args, so do it here for the Flink path.
        val userArgs = JobSubmitter.getApplicationArgs(jobType, args.toArray)
        val jobId = submissionProperties.getOrElse(JobId, throw new RuntimeException("Job ID not found"))
        val mainClass =
          submissionProperties.getOrElse(MainClass, throw new RuntimeException("Main class not found"))
        val mainJarUri = submissionProperties.getOrElse(
          FlinkMainJarURI,
          throw new RuntimeException(s"Missing expected $FlinkMainJarURI"))
        val jarUri = submissionProperties.getOrElse(JarURI, throw new RuntimeException("Jar URI not found"))
        val flinkCheckpointPath = submissionProperties.getOrElse(
          FlinkCheckpointUri,
          throw new RuntimeException(s"Missing expected $FlinkCheckpointUri"))
        val maybeSavepointUri = submissionProperties.get(SavepointUri)
        val maybeAdditionalJarsUri = submissionProperties.get(AdditionalJars)
        val additionalJars = maybeAdditionalJarsUri
          .map(_.split(",").map(_.trim).filter(_.nonEmpty))
          .getOrElse(Array.empty)
        val jarUris = Array(jarUri) ++ additionalJars

        val serviceAccount = aksServiceAccount
          .orElse(submissionProperties.get(EksServiceAccount))
          .getOrElse(throw new RuntimeException(s"Missing expected $EksServiceAccount"))
        val namespace = aksNamespace
          .orElse(submissionProperties.get(EksNamespace))
          .getOrElse(throw new RuntimeException(s"Missing expected $EksNamespace"))

        val deploymentName = aksFlinkSubmitter.submit(
          jobId = jobId,
          mainClass = mainClass,
          mainJarUri = mainJarUri,
          jarUris = jarUris,
          flinkCheckpointUri = flinkCheckpointPath,
          maybeSavepointUri = maybeSavepointUri,
          maybeFlinkJarsUri = submissionProperties.get(FlinkJarsUri),
          jobProperties = jobProperties,
          args = userArgs.toSeq,
          serviceAccount = serviceAccount,
          namespace = namespace,
          envVars = envVars
        )
        s"flink:$namespace:$deploymentName"
    }
  }

  override def status(jobId: String): JobStatusType = {
    if (jobId.startsWith(AzureSubmitter.FlinkJobIdPrefix)) {
      val parts = jobId.split(":", 3)
      val (flinkStatus, creationTime) =
        aksFlinkSubmitter.statusWithCreationTime(deploymentName = parts(2), namespace = parts(1))
      flinkStatus match {
        case JobStatusType.RUNNING if flinkHealthCheckFn(getFlinkUrl(jobId)) => JobStatusType.RUNNING
        case JobStatusType.RUNNING                                           =>
          // Health check failed — stay PENDING within the grace window before declaring failure
          JobSubmitter.flinkStatusWithGrace(
            jobId,
            creationTime,
            AzureSubmitter.FlinkHealthCheckGracePeriod,
            logger
          )
        case other => other
      }
    } else {
      kyuubiSubmitter.status(jobId)
    }
  }

  override def kill(jobId: String): Unit = {
    if (jobId.startsWith(AzureSubmitter.FlinkJobIdPrefix)) {
      val parts = jobId.split(":", 3)
      if (parts.length != 3)
        return
      aksFlinkSubmitter.delete(deploymentName = parts(2), namespace = parts(1))
    } else {
      kyuubiSubmitter.kill(jobId)
    }
  }

  override def getFlinkUrl(jobId: String): Option[String] = {
    if (!jobId.startsWith(AzureSubmitter.FlinkJobIdPrefix)) return None
    val parts = jobId.split(":", 3)
    if (parts.length != 3) return None
    val deploymentName = parts(2)
    ingressBaseUrl.map(base => s"${base.stripSuffix("/")}/flink/$deploymentName/")
  }

  override def getSparkUrl(jobId: String): Option[String] =
    if (jobId.startsWith(AzureSubmitter.FlinkJobIdPrefix)) None
    else kyuubiSubmitter.getSparkUrl(jobId)

  override def getFlinkInternalJobId(jobId: String): Option[String] =
    flinkInternalJobIdFetchFn(getFlinkUrl(jobId))

  override def getLatestCheckpointPath(flinkInternalJobId: String, flinkStateUri: String): Option[String] = {
    val sc = storageClient.getOrElse {
      logger.warn(s"Storage client not available, cannot resolve checkpoint path for Flink job $flinkInternalJobId")
      return None
    }
    val result = StorageClient.resolveLatestCheckpointPath(sc, flinkInternalJobId, flinkStateUri)
    result match {
      case Some(path) =>
        logger.info(s"Resolved latest checkpoint for Flink job $flinkInternalJobId: $path")
      case None =>
        logger.warn(
          s"No checkpoints found for Flink job $flinkInternalJobId at $flinkStateUri/checkpoints/$flinkInternalJobId")
    }
    result
  }

  override def buildFlinkSubmissionProps(env: Map[String, String],
                                         version: String,
                                         artifactPrefix: String): Map[String, String] = {
    val flinkJarUri = s"$artifactPrefix/release/$version/jars/$flinkJarName"
    val flinkStateUri = env.getOrElse(
      "FLINK_STATE_URI",
      throw new IllegalArgumentException("FLINK_STATE_URI must be set for GROUP_BY_STREAMING"))
    val serviceAccount = aksServiceAccount
      .orElse(env.get("FLINK_AKS_SERVICE_ACCOUNT"))
      .getOrElse(throw new IllegalArgumentException("FLINK_AKS_SERVICE_ACCOUNT must be set for GROUP_BY_STREAMING"))
    val namespace = aksNamespace
      .orElse(env.get("FLINK_AKS_NAMESPACE"))
      .getOrElse(throw new IllegalArgumentException("FLINK_AKS_NAMESPACE must be set for GROUP_BY_STREAMING"))
    Map(
      FlinkMainJarURI -> flinkJarUri,
      FlinkCheckpointUri -> s"$flinkStateUri/checkpoints",
      EksServiceAccount -> serviceAccount,
      EksNamespace -> namespace
    )
  }

  override def close(): Unit = {
    try { kyuubiSubmitter.close() }
    catch {
      case _: Exception => logger.info("Error closing Kyuubi submitter")
    }
  }
}

object AzureSubmitter {

  val FlinkJobIdPrefix = "flink:"

  // Total grace period from CRD creation before an unhealthy STABLE Flink job is marked FAILED.
  // K8sFlinkSubmitter already allows up to 10 minutes for the deployment to reach STABLE,
  // so this window covers that plus additional time for checkpoints to accumulate post-STABLE.
  val FlinkHealthCheckGracePeriod: Duration = Duration.ofMinutes(15)

  /** Constructs an [[AzureSubmitter]] from environment variables. Intended for CLI usage. */
  def apply(args: Array[String]): AzureSubmitter = {
    val kyuubiUrl =
      sys.env.getOrElse("KYUUBI_URL", throw new IllegalArgumentException("KYUUBI_URL env var must be set"))
    val flinkImage = sys.env.getOrElse("FLINK_IMAGE", AksFlinkSubmitter.FlinkImage)
    val flinkJarsBasePath =
      sys.env.getOrElse("FLINK_JARS_BASE_PATH", AksFlinkSubmitter.DefaultAzureFlinkJarsBasePath)
    val aksServiceAccount = sys.env.get("FLINK_AKS_SERVICE_ACCOUNT")
    val aksNamespace = sys.env.get("FLINK_AKS_NAMESPACE")
    val ingressBaseUrl = sys.env.get("HUB_BASE_URL")
    val storageAccountUrl = sys.env.get("AZURE_STORAGE_ACCOUNT_URL")

    val storageClient = storageAccountUrl.map { url =>
      val blobServiceClient = new BlobServiceClientBuilder().endpoint(url).buildClient()
      new AzureBlobStorageClient(blobServiceClient)
    }

    val kyuubiSubmitter = KyuubiSubmitter(kyuubiUrl, storageClient = storageClient)
    val aksFlinkSubmitter = AksFlinkSubmitter(
      flinkImage = flinkImage,
      defaultJarsBasePath = flinkJarsBasePath,
      ingressBaseUrl = ingressBaseUrl
    )
    val flinkStatusProvider = new K8sFlinkStatusProvider()
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(Executors.newCachedThreadPool())

    new AzureSubmitter(
      kyuubiSubmitter = kyuubiSubmitter,
      aksFlinkSubmitter = aksFlinkSubmitter,
      aksServiceAccount = aksServiceAccount,
      aksNamespace = aksNamespace,
      ingressBaseUrl = ingressBaseUrl,
      storageClient = storageClient,
      flinkHealthCheckFn = uri => Await.result(flinkStatusProvider.isFlinkJobHealthy(uri), 30.seconds),
      flinkInternalJobIdFetchFn = uri => Await.result(flinkStatusProvider.getFlinkInternalJobId(uri), 30.seconds)
    )
  }
}
