package ai.chronon.integrations.aws

import ai.chronon.api.JobStatusType
import ai.chronon.integrations.cloud_k8s.{K8sFlinkStatusProvider, K8sFlinkSubmitter}
import ai.chronon.spark.submission.JobSubmitterConstants._
import ai.chronon.spark.submission.{
  JobSubmitter,
  JobType,
  StorageClient,
  FlinkJob => TypeFlinkJob,
  SparkJob => TypeSparkJob
}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.emr.EmrClient
import software.amazon.awssdk.services.emr.model.ListStudiosRequest
import software.amazon.awssdk.services.emrserverless.EmrServerlessClient
import software.amazon.awssdk.services.emrserverless.model._
import software.amazon.awssdk.services.s3.S3Client

import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import scala.jdk.CollectionConverters._

class EmrServerlessSubmitter(
    emrServerlessClient: EmrServerlessClient,
    executionRoleArn: String,
    s3LogUri: String,
    eksFlinkSubmitter: Option[K8sFlinkSubmitter] = None,
    awsRegion: String = "",
    override val dqMetricsDataset: String = "",
    override val kvStoreApiProperties: Map[String, String] = Map.empty,
    flinkEksServiceAccount: Option[String] = None,
    flinkEksNamespace: Option[String] = None,
    eksClusterName: Option[String] = None,
    ingressBaseUrl: Option[String] = None,
    emrStudioId: Option[String] = None,
    flinkHealthCheckFn: Option[String] => Boolean = _ => true,
    flinkInternalJobIdFetchFn: Option[String] => Option[String] = _ => None,
    s3Client: Option[S3Client] = None,
    cloudWatchLogGroupName: Option[String] = None,
    applicationName: String = EmrServerlessSubmitter.DefaultApplicationName
) extends JobSubmitter {

  // Cache of jobRunId → appId for reverse lookups in status/kill/URL
  private val jobAppIdCache = new ConcurrentHashMap[String, String]()

  private val EnvVarPattern = """\{([A-Z_][A-Z0-9_]*)\}""".r

  private def resolveEnvVars(properties: Map[String, String]): Map[String, String] =
    properties.map { case (k, v) =>
      k -> EnvVarPattern.replaceAllIn(v,
                                      m => {
                                        val varName = m.group(1)
                                        sys.env.getOrElse(varName, m.matched)
                                      })
    }

  // EMR Serverless-specific env var expansion. Unlike YARN/K8s setups, driver env vars
  // must use spark.emr-serverless.driverEnv.<KEY> — see
  // https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/jobs-spark.html
  override def envVarsToSparkProperties(env: Map[String, String]): Map[String, String] =
    env.flatMap { case (key, value) =>
      Seq(
        s"spark.emr-serverless.driverEnv.$key" -> value,
        s"spark.executorEnv.$key" -> value
      )
    }

  private lazy val resolvedStudioId: Option[String] =
    emrStudioId.orElse(EmrServerlessSubmitter.resolveEmrStudioId(awsRegion))

  // Lazily resolved for the CLI path
  lazy val resolvedApplicationId: String =
    findApplication(applicationName)
      .getOrElse(
        throw new RuntimeException(
          s"No EMR Serverless application found with name '$applicationName'. " +
            "Applications must be created via Terraform with proper VPC network configuration."))

  private def findApplication(appName: String): Option[String] = {
    val existingApps = {
      val buf = scala.collection.mutable.Buffer.empty[ApplicationSummary]
      var nextToken: Option[String] = None
      do {
        val requestBuilder = ListApplicationsRequest.builder()
        nextToken.foreach(requestBuilder.nextToken)
        val response = emrServerlessClient.listApplications(requestBuilder.build())
        buf ++= response.applications().asScala
        nextToken = Option(response.nextToken())
      } while (nextToken.isDefined)
      buf.filter(app => app.name() == appName && app.state() != ApplicationState.TERMINATED)
    }

    if (existingApps.nonEmpty) {
      val appId = existingApps.head.id()
      logger.info(s"Found existing EMR Serverless application: $appId (name=$appName)")
      Some(appId)
    } else {
      None
    }
  }

  override def submit(
      jobType: JobType,
      submissionProperties: Map[String, String],
      jobProperties: Map[String, String],
      files: List[String],
      labels: Map[String, String],
      envVars: Map[String, String],
      args: String*
  ): String = {
    val userArgs = JobSubmitter.getApplicationArgs(jobType, args.toArray)

    jobType match {
      case TypeFlinkJob =>
        val jobId = submissionProperties.getOrElse(JobId, throw new RuntimeException("Job ID not found"))
        val mainClass = submissionProperties.getOrElse(MainClass, throw new RuntimeException("Main class not found"))
        val mainJarUri = submissionProperties.getOrElse(
          FlinkMainJarURI,
          throw new RuntimeException(s"Missing expected $FlinkMainJarURI"))
        val jarUri = submissionProperties.getOrElse(JarURI, throw new RuntimeException("Jar URI not found"))
        val flinkCheckpointPath = submissionProperties.getOrElse(
          FlinkCheckpointUri,
          throw new RuntimeException(s"Missing expected $FlinkCheckpointUri"))
        val maybeSavepointUri = submissionProperties.get(SavepointUri)
        val maybeKinesisConnectorJarUri = submissionProperties.get(FlinkKinesisConnectorJarURI)
        val maybeAdditionalJarsUri = submissionProperties.get(AdditionalJars)
        val additionalJars = maybeAdditionalJarsUri
          .map(_.split(",").map(_.trim).filter(_.nonEmpty))
          .getOrElse(Array.empty)
        val jarUris = Array(jarUri) ++ maybeKinesisConnectorJarUri.toList ++ additionalJars

        val serviceAccount = submissionProperties.getOrElse(
          EksServiceAccount,
          throw new RuntimeException(s"Missing expected $EksServiceAccount"))
        val namespace =
          submissionProperties.getOrElse(EksNamespace, throw new RuntimeException(s"Missing expected $EksNamespace"))
        val nodeSelector = submissionProperties
          .get(EksNodeSelector)
          .map(EmrServerlessSubmitter.parseNodeSelector)
          .getOrElse(Map.empty)

        val deploymentName = eksFlinkSubmitter
          .getOrElse(
            throw new RuntimeException("K8sFlinkSubmitter is required for Flink jobs")
          )
          .submit(
            jobId = jobId,
            mainClass = mainClass,
            mainJarUri = mainJarUri,
            jarUris = jarUris,
            flinkCheckpointUri = flinkCheckpointPath,
            maybeSavepointUri = maybeSavepointUri,
            maybeFlinkJarsUri = submissionProperties.get(FlinkJarsUri),
            jobProperties = jobProperties,
            args = userArgs,
            serviceAccount = serviceAccount,
            namespace = namespace,
            envVars = envVars,
            nodeSelector = nodeSelector
          )
        s"flink:$namespace:$deploymentName"

      case TypeSparkJob =>
        submitSparkJob(submissionProperties, jobProperties, envVars, files, labels, userArgs: _*)
    }
  }

  private def submitSparkJob(
      submissionProperties: Map[String, String],
      jobProperties: Map[String, String],
      envVars: Map[String, String],
      files: List[String],
      labels: Map[String, String],
      args: String*
  ): String = {
    val mainClass = submissionProperties.getOrElse(MainClass, throw new RuntimeException("Main class not found"))
    val jarUri = submissionProperties.getOrElse(JarURI, throw new RuntimeException("Jar URI not found"))

    // Prefer app ID from submission props (set by NodeSubmitter via ensureClusterReady),
    // fall back to resolvedApplicationId for CLI path
    val appId = submissionProperties.getOrElse(clusterIdentifierKey, resolvedApplicationId)

    val filesParam = if (files.nonEmpty) s"--files ${files.mkString(",")}" else ""

    // EMR Serverless caps properties per Configuration at 100. Anything beyond
    // that spills into --conf flags on sparkSubmitParameters (same effect on SparkConf,
    // without the console-side property-count limit).
    // envVars get expanded into EMR-Serverless-specific driver/executor Spark props here
    // (see envVarsToSparkProperties) and merged with caller-provided jobProperties.
    val resolvedProps = resolveEnvVars(jobProperties ++ envVarsToSparkProperties(envVars))
    val (inlineProps, overflowProps) = EmrServerlessSubmitter.splitForConfigCap(resolvedProps)
    if (overflowProps.nonEmpty) {
      logger.warn(
        s"jobProperties has ${resolvedProps.size} entries, exceeding EMR Serverless cap of " +
          s"${EmrServerlessSubmitter.MaxPropertiesPerClassification} per classification; " +
          s"routing ${overflowProps.size} through sparkSubmitParameters as --conf flags")
    }
    val overflowConf = overflowProps.toSeq
      .sortBy(_._1)
      .map { case (k, v) =>
        s"--conf ${EmrServerlessSubmitter.maybeShellQuote(s"$k=$v")}"
      }
      .mkString(" ")

    val sparkSubmitParams =
      Seq(s"--class $mainClass", filesParam, overflowConf).filter(_.nonEmpty).mkString(" ")

    val jobName = submissionProperties.getOrElse(JobId, s"chronon-job-${System.currentTimeMillis()}")
    logger.info(
      s"EMR Serverless submission for $jobName: ${resolvedProps.size} jobProperties " +
        s"(${inlineProps.size} in spark-defaults, ${overflowProps.size} as --conf overflow)")

    val jobDriverBuilder = JobDriver
      .builder()
      .sparkSubmit(
        SparkSubmit
          .builder()
          .entryPoint(jarUri)
          .entryPointArguments(args.toList.asJava)
          .sparkSubmitParameters(sparkSubmitParams)
          .build()
      )

    val configOverridesBuilder = ConfigurationOverrides
      .builder()
      .monitoringConfiguration(
        MonitoringConfiguration
          .builder()
          .s3MonitoringConfiguration(
            S3MonitoringConfiguration.builder().logUri(s3LogUri).build()
          )
          .cloudWatchLoggingConfiguration({
            val cwBuilder = CloudWatchLoggingConfiguration.builder().enabled(true)
            cloudWatchLogGroupName.foreach(cwBuilder.logGroupName)
            cwBuilder.build()
          })
          .build()
      )

    if (inlineProps.nonEmpty) {
      configOverridesBuilder.applicationConfiguration(
        Configuration
          .builder()
          .classification("spark-defaults")
          .properties(inlineProps.asJava)
          .build()
      )
    }

    val startJobRunRequest = StartJobRunRequest
      .builder()
      .applicationId(appId)
      .executionRoleArn(executionRoleArn)
      .name(jobName)
      .jobDriver(jobDriverBuilder.build())
      .configurationOverrides(configOverridesBuilder.build())
      .tags(labels.asJava)
      .build()

    val response = emrServerlessClient.startJobRun(startJobRunRequest)
    val jobRunId = response.jobRunId()

    // Cache the mapping so status/kill/URL can find the app
    jobAppIdCache.put(jobRunId, appId)
    logger.info(s"Started EMR Serverless job run: $jobRunId on application $appId")

    jobRunId
  }

  /** Scan applications to find which one owns the given jobRunId.
    * In practice there are 1-2 apps per environment, so this is 1-2 API calls.
    */
  private def findApplicationForJob(jobRunId: String): String = {
    val cached = jobAppIdCache.get(jobRunId)
    if (cached != null) return cached

    // List all applications (including terminated so we can query jobs from replaced apps)
    val apps = {
      val buf = scala.collection.mutable.Buffer.empty[ApplicationSummary]
      var nextToken: Option[String] = None
      do {
        val requestBuilder = ListApplicationsRequest.builder()
        nextToken.foreach(requestBuilder.nextToken)
        val response = emrServerlessClient.listApplications(requestBuilder.build())
        buf ++= response.applications().asScala
        nextToken = Option(response.nextToken())
      } while (nextToken.isDefined)
      buf
    }

    for (app <- apps) {
      try {
        val request = GetJobRunRequest.builder().applicationId(app.id()).jobRunId(jobRunId).build()
        emrServerlessClient.getJobRun(request)
        jobAppIdCache.put(jobRunId, app.id())
        return app.id()
      } catch {
        case _: ResourceNotFoundException => // job not on this app, try next
      }
    }
    throw new RuntimeException(s"Could not find application for job run $jobRunId across ${apps.size} applications")
  }

  override def status(jobId: String): JobStatusType = {
    if (jobId.startsWith("flink:")) {
      val parts = jobId.split(":", 3)
      val (eksStatus, creationTime) = eksFlinkSubmitter
        .getOrElse(throw new RuntimeException("K8sFlinkSubmitter is required for Flink jobs"))
        .statusWithCreationTime(deploymentName = parts(2), namespace = parts(1))
      eksStatus match {
        case JobStatusType.RUNNING if flinkHealthCheckFn(getFlinkUrl(jobId)) => JobStatusType.RUNNING
        case JobStatusType.RUNNING                                           =>
          // Health check failed: job is STABLE on K8s but checkpoints haven't accumulated yet.
          // Within the grace period this is expected (K8s startup consumes part of the window),
          // so stay PENDING rather than triggering a retrigger.
          JobSubmitter.flinkStatusWithGrace(
            jobId,
            creationTime,
            EmrServerlessSubmitter.FlinkHealthCheckGracePeriod,
            logger
          )
        case other => other
      }
    } else {
      try {
        val appId = findApplicationForJob(jobId)
        val getRequest = GetJobRunRequest
          .builder()
          .applicationId(appId)
          .jobRunId(jobId)
          .build()

        val jobRun = emrServerlessClient.getJobRun(getRequest).jobRun()
        val state = jobRun.state()

        state match {
          case JobRunState.SUBMITTED | JobRunState.PENDING | JobRunState.SCHEDULED => JobStatusType.PENDING
          case JobRunState.RUNNING                                                 => JobStatusType.RUNNING
          case JobRunState.SUCCESS                                                 => JobStatusType.SUCCEEDED
          case JobRunState.CANCELLING                                              => JobStatusType.RUNNING
          case JobRunState.FAILED | JobRunState.CANCELLED                          => JobStatusType.FAILED
          case _                                                                   => JobStatusType.UNKNOWN
        }
      } catch {
        case e: Exception =>
          logger.error(s"Error checking job status for jobId $jobId: ${e.getMessage}")
          JobStatusType.UNKNOWN
      }
    }
  }

  override def kill(jobId: String): Unit = {
    if (jobId.startsWith("flink:")) {
      val parts = jobId.split(":", 3)
      eksFlinkSubmitter
        .getOrElse(throw new RuntimeException("K8sFlinkSubmitter is required for Flink jobs"))
        .delete(deploymentName = parts(2), namespace = parts(1))
    } else {
      try {
        val appId = findApplicationForJob(jobId)
        val cancelRequest = CancelJobRunRequest
          .builder()
          .applicationId(appId)
          .jobRunId(jobId)
          .build()

        emrServerlessClient.cancelJobRun(cancelRequest)
        logger.info(s"Cancelled EMR Serverless job run $jobId on application $appId")
      } catch {
        case e: Exception =>
          logger.error(s"Error killing job $jobId: ${e.getMessage}")
      }
    }
  }

  override def jarName: String = "cloud_aws_lib_deploy.jar"
  override def onlineClass: String = "ai.chronon.integrations.aws.AwsApiImpl"

  // Conf files are distributed via --files; Spark makes them available by filename in the working dir

  override def getJobUrl(jobId: String): Option[String] = {
    if (jobId.startsWith("flink:")) {
      val parts = jobId.split(":")
      if (parts.length == 3) {
        val namespace = parts(1)
        val deploymentName = parts(2)
        eksClusterName.map { clusterName =>
          s"https://$awsRegion.console.aws.amazon.com/eks/clusters/$clusterName/deployments/$deploymentName?namespace=$namespace&region=$awsRegion"
        }
      } else None
    } else {
      resolvedStudioId.flatMap { rawStudioId =>
        try {
          val studioId = rawStudioId.stripPrefix("es-").toLowerCase
          val appId = findApplicationForJob(jobId)
          Some(s"https://es-$studioId.emrstudio-prod.$awsRegion.amazonaws.com/#/serverless-applications/$appId/$jobId")
        } catch {
          case e: Exception =>
            logger.debug(s"Could not get job URL for $jobId: ${e.getMessage}")
            None
        }
      }
    }
  }

  override def getSparkUrl(jobId: String): Option[String] = {
    if (jobId.startsWith("flink:")) {
      None
    } else {
      try {
        val appId = findApplicationForJob(jobId)
        val request = GetDashboardForJobRunRequest
          .builder()
          .applicationId(appId)
          .jobRunId(jobId)
          .build()
        Some(emrServerlessClient.getDashboardForJobRun(request).url())
      } catch {
        case e: Exception =>
          logger.debug(s"Could not get Spark UI URL for $jobId: ${e.getMessage}")
          None
      }
    }
  }

  override def getFlinkUrl(jobId: String): Option[String] = {
    if (!jobId.startsWith("flink:")) return None
    val parts = jobId.split(":", 3)
    if (parts.length != 3) return None
    val deploymentName = parts(2)
    ingressBaseUrl.map(base => s"${base.stripSuffix("/")}/flink/$deploymentName/")
  }

  override def getFlinkInternalJobId(jobId: String): Option[String] =
    flinkInternalJobIdFetchFn(getFlinkUrl(jobId))

  override def getLatestCheckpointPath(flinkInternalJobId: String, flinkStateUri: String): Option[String] = {
    val s3 = s3Client.getOrElse {
      logger.warn(s"S3 client not available, cannot resolve checkpoint path for Flink job $flinkInternalJobId")
      return None
    }
    val result = StorageClient.resolveLatestCheckpointPath(new S3StorageClient(s3), flinkInternalJobId, flinkStateUri)
    result match {
      case Some(path) => logger.info(s"Resolved latest checkpoint for Flink job $flinkInternalJobId: $path")
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
    val eksServiceAccount = this.flinkEksServiceAccount
      .orElse(env.get("FLINK_EKS_SERVICE_ACCOUNT"))
      .getOrElse(throw new IllegalArgumentException("FLINK_EKS_SERVICE_ACCOUNT must be set for GROUP_BY_STREAMING"))
    val eksNamespace = this.flinkEksNamespace
      .orElse(env.get("FLINK_EKS_NAMESPACE"))
      .getOrElse(throw new IllegalArgumentException("FLINK_EKS_NAMESPACE must be set for GROUP_BY_STREAMING"))
    val maybeNodeSelector = env.get("FLINK_EKS_NODE_SELECTOR")
    val base = Map(
      FlinkMainJarURI -> flinkJarUri,
      FlinkCheckpointUri -> s"$flinkStateUri/checkpoints",
      EksServiceAccount -> eksServiceAccount,
      EksNamespace -> eksNamespace
    ) ++ maybeNodeSelector.map(EksNodeSelector -> _)
    val enableKinesis = env.getOrElse("ENABLE_KINESIS", "false").toBoolean
    if (enableKinesis)
      base + (FlinkKinesisConnectorJarURI -> s"$artifactPrefix/release/$version/jars/connectors_kinesis_deploy.jar")
    else base
  }

  override def deprecatedClusterNameEnvVars: Seq[String] = Seq.empty

  // Batch jobs need cluster resolution (app name → app ID); Flink goes to EKS
  override def isClusterCreateNeeded(isLongRunning: Boolean): Boolean = !isLongRunning

  override def ensureClusterReady(clusterName: String, clusterConf: Option[Map[String, String]])(implicit
      ec: ExecutionContext): Option[String] = {
    val appName = clusterName
    findApplication(appName) match {
      case Some(appId) =>
        val getRequest = GetApplicationRequest.builder().applicationId(appId).build()
        val app = emrServerlessClient.getApplication(getRequest).application()
        app.state() match {
          case ApplicationState.CREATED | ApplicationState.STARTED | ApplicationState.STOPPED =>
            logger.info(s"Application $appId (name=$appName) is ready (state: ${app.state()})")
            Some(appId)
          case ApplicationState.CREATING | ApplicationState.STARTING =>
            logger.info(s"Application $appId (name=$appName) is starting (state: ${app.state()}), will retry")
            None
          case other =>
            throw new RuntimeException(
              s"Application $appId (name=$appName) is in non-usable state: $other. " +
                "Recreate the application via Terraform.")
        }
      case None =>
        throw new RuntimeException(
          s"No EMR Serverless application found with name '$appName'. " +
            "Applications must be created via Terraform with proper VPC network configuration.")
    }
  }

  override def close(): Unit = {
    try {
      emrServerlessClient.close()
    } catch {
      case _: Exception => logger.info("Error shutting down AWS clients.")
    }
  }
}

object EmrServerlessSubmitter {

  private val logger = org.slf4j.LoggerFactory.getLogger(getClass)
  val DefaultApplicationName = "chronon-serverless-app"

  // Parses "zipline.ai/node-type=flink,kubernetes.io/arch=amd64" into the equivalent Map
  private[aws] def parseNodeSelector(raw: String): Map[String, String] = {
    if (raw.trim.isEmpty)
      throw new IllegalArgumentException("nodeSelector value must not be blank")
    raw
      .split(",")
      .map(_.trim)
      .filter(_.nonEmpty)
      .map { pair =>
        val idx = pair.indexOf('=')
        if (idx <= 0)
          throw new IllegalArgumentException(s"Malformed nodeSelector pair: '$pair' (expected key=value)")
        pair.substring(0, idx).trim -> pair.substring(idx + 1).trim
      }
      .toMap
  }

  // EMR Serverless StartJobRun API rejects any applicationConfiguration entry whose
  // properties map has more than 100 keys (service-side validation).
  val MaxPropertiesPerClassification: Int = 100

  // Characters that force shell-style quoting when embedding a k=v pair into
  // sparkSubmitParameters. EMR Serverless tokenizes that string shell-style before
  // invoking spark-submit, so values containing whitespace or shell metacharacters
  // must be single-quoted to travel as one token.
  private val NeedsQuoting = """[\s"'\\$`&|;<>(){}*?~#\[\]]""".r

  private[aws] def maybeShellQuote(s: String): String =
    if (NeedsQuoting.findFirstIn(s).isDefined) "'" + s.replace("'", "'\\''") + "'"
    else s

  private[aws] def splitForConfigCap(props: Map[String, String]): (Map[String, String], Map[String, String]) =
    if (props.size <= MaxPropertiesPerClassification) (props, Map.empty)
    else {
      val sorted = props.toSeq.sortBy(_._1)
      val (head, tail) = sorted.splitAt(MaxPropertiesPerClassification)
      (head.toMap, tail.toMap)
    }

  // Total grace period from CRD creation before an unhealthy STABLE job is marked FAILED.
  // K8sFlinkSubmitter already allows up to 10 minutes for the deployment to reach STABLE,
  // so this window covers that plus additional time for checkpoints to accumulate post-STABLE.
  val FlinkHealthCheckGracePeriod: Duration = Duration.ofMinutes(15)

  // Fallback when EMR_STUDIO_ID is not configured. Any studio in the same account/region
  // can view any serverless application, so picking the first one from ListStudios works.
  // Prefer setting EMR_STUDIO_ID explicitly via terraform/config for deterministic behavior.
  private[aws] def resolveEmrStudioId(awsRegion: String): Option[String] = {
    try {
      val emrClient = EmrClient.builder().region(Region.of(awsRegion)).build()
      try {
        val response = emrClient.listStudios(ListStudiosRequest.builder().build())
        val studios = response.studios()
        if (studios.isEmpty) {
          logger.debug("No EMR Studios found in account")
          None
        } else {
          val studioId = studios.get(0).studioId()
          logger.info(s"Resolved EMR Studio ID: $studioId")
          Some(studioId)
        }
      } finally {
        emrClient.close()
      }
    } catch {
      case e: Exception =>
        logger.debug(s"Could not resolve EMR Studio ID: ${e.getMessage}")
        None
    }
  }

  def apply(
      awsRegion: String,
      executionRoleArn: String,
      s3LogUri: String,
      dqMetricsDataset: String = "",
      flinkEksServiceAccount: Option[String] = None,
      flinkEksNamespace: Option[String] = None,
      eksClusterName: Option[String] = None,
      ingressBaseUrl: Option[String] = None,
      emrStudioId: Option[String] = None,
      cloudWatchLogGroupName: Option[String] = None,
      k8sConfig: Option[io.fabric8.kubernetes.client.Config] = None,
      flinkHealthCheckFn: Option[String] => Boolean = _ => true,
      flinkInternalJobIdFetchFn: Option[String] => Option[String] = _ => None,
      s3Client: Option[S3Client] = None,
      applicationName: String = DefaultApplicationName,
      kvStoreApiProperties: Map[String, String] = Map.empty
  ): EmrServerlessSubmitter = {
    val client = EmrServerlessClient
      .builder()
      .region(Region.of(awsRegion))
      .build()

    new EmrServerlessSubmitter(
      client,
      executionRoleArn,
      s3LogUri,
      eksFlinkSubmitter = Some(EksFlinkSubmitter(k8sConfig, ingressBaseUrl = ingressBaseUrl)),
      awsRegion = awsRegion,
      dqMetricsDataset = dqMetricsDataset,
      flinkEksServiceAccount = flinkEksServiceAccount,
      flinkEksNamespace = flinkEksNamespace,
      eksClusterName = eksClusterName,
      ingressBaseUrl = ingressBaseUrl,
      emrStudioId = emrStudioId,
      flinkHealthCheckFn = flinkHealthCheckFn,
      flinkInternalJobIdFetchFn = flinkInternalJobIdFetchFn,
      s3Client = s3Client,
      cloudWatchLogGroupName = cloudWatchLogGroupName,
      applicationName = applicationName,
      kvStoreApiProperties = kvStoreApiProperties
    )
  }

  private[aws] def createSubmissionPropsMap(jobType: JobType, args: Array[String]): Map[String, String] = {
    val jarUri = JobSubmitter
      .getArgValue(args, JarUriArgKeyword)
      .getOrElse(throw new Exception("Missing required argument: " + JarUriArgKeyword))
    val mainClass = JobSubmitter
      .getArgValue(args, MainClassKeyword)
      .getOrElse(throw new Exception("Missing required argument: " + MainClassKeyword))

    jobType match {
      case TypeSparkJob =>
        val jobId = JobSubmitter
          .getArgValue(args, JobIdArgKeyword)
          .getOrElse(s"chronon-job-${System.currentTimeMillis()}")
        Map(
          MainClass -> mainClass,
          JarURI -> jarUri,
          JobId -> jobId
        )
      case TypeFlinkJob =>
        val jobId = JobSubmitter
          .getArgValue(args, JobIdArgKeyword)
          .getOrElse(throw new Exception("Missing required argument: " + JobIdArgKeyword))
        val flinkCheckpointUri = JobSubmitter
          .getArgValue(args, StreamingCheckpointPathArgKeyword)
          .getOrElse(throw new Exception(s"Missing required argument $StreamingCheckpointPathArgKeyword"))
        val flinkMainJarUri = JobSubmitter
          .getArgValue(args, FlinkMainJarUriArgKeyword)
          .getOrElse(throw new Exception("Missing required argument: " + FlinkMainJarUriArgKeyword))
        val eksServiceAccount = JobSubmitter
          .getArgValue(args, EksServiceAccountArgKeyword)
          .getOrElse(throw new Exception("Missing required argument: " + EksServiceAccountArgKeyword))
        val eksNamespace = JobSubmitter
          .getArgValue(args, EksNamespaceArgKeyword)
          .getOrElse(throw new Exception("Missing required argument: " + EksNamespaceArgKeyword))
        val maybeKinesisJarUri = JobSubmitter.getArgValue(args, FlinkKinesisJarUriArgKeyword)
        val maybeFlinkJarsUri = JobSubmitter.getArgValue(args, FlinkJarsUriArgKeyword)
        val maybeAdditionalJarsUri = JobSubmitter.getArgValue(args, AdditionalJarsUriArgKeyword)
        val maybeNodeSelector = JobSubmitter.getArgValue(args, EksNodeSelectorArgKeyword)

        val baseJobProps = Map(
          JobId -> jobId,
          MainClass -> mainClass,
          JarURI -> jarUri,
          FlinkMainJarURI -> flinkMainJarUri,
          FlinkCheckpointUri -> flinkCheckpointUri,
          EksServiceAccount -> eksServiceAccount,
          EksNamespace -> eksNamespace
        ) ++ maybeKinesisJarUri.map(FlinkKinesisConnectorJarURI -> _) ++
          maybeFlinkJarsUri.map(FlinkJarsUri -> _) ++
          maybeAdditionalJarsUri.map(AdditionalJars -> _) ++
          maybeNodeSelector.map(EksNodeSelector -> _)

        val userPassedSavepoint = JobSubmitter.getArgValue(args, StreamingCustomSavepointArgKeyword)
        val maybeSavepointUri =
          if (userPassedSavepoint.isDefined) {
            userPassedSavepoint
          } else if (args.contains(StreamingLatestSavepointArgKeyword)) {
            System.err.println("Latest savepoint not yet supported for EMR Serverless, proceeding without savepoint")
            None
          } else {
            None
          }

        maybeSavepointUri match {
          case Some(uri) =>
            System.err.println(s"Deploying Flink app with savepoint uri $uri")
            baseJobProps + (SavepointUri -> uri)
          case None => baseJobProps
        }
    }
  }

  def main(args: Array[String]): Unit = {
    val internalArgs = SharedInternalArgs

    val userArgs = args.filter(arg => !internalArgs.exists(arg.startsWith))

    val jobTypeValue = JobSubmitter
      .getArgValue(args, JobTypeArgKeyword)
      .getOrElse(throw new Exception("Missing required argument: " + JobTypeArgKeyword))

    val jobType = jobTypeValue.toLowerCase match {
      case "spark" => TypeSparkJob
      case "flink" => TypeFlinkJob
      case _       => throw new Exception("Invalid job type. Must be 'spark' or 'flink'.")
    }

    val submissionProps = createSubmissionPropsMap(jobType, args)

    val files = args.find(_.startsWith(FilesArgKeyword)) match {
      case None => Array.empty[String]
      case Some(arg) =>
        val eqIdx = arg.indexOf('=')
        if (eqIdx < 0 || eqIdx == arg.length - 1) {
          throw new IllegalArgumentException(
            s"Malformed files argument: '$arg'. Expected format: ${FilesArgKeyword}=file1,file2")
        }
        arg.substring(eqIdx + 1).split(",")
    }

    val finalArgs = userArgs.toSeq
    val modeConfigProperties = JobSubmitter.getModeConfigProperties(args)

    val region = sys.env.getOrElse("AWS_REGION", sys.env.getOrElse("AWS_DEFAULT_REGION", "us-west-2"))
    val appName = sys.env.get(SparkClusterNameEnvVar).filter(_.nonEmpty).getOrElse(DefaultApplicationName)
    val flinkStatusProvider = new K8sFlinkStatusProvider()
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(Executors.newCachedThreadPool())
    val submitter = EmrServerlessSubmitter(
      awsRegion = region,
      executionRoleArn = sys.env.getOrElse("EMR_EXECUTION_ROLE_ARN",
                                           throw new Exception("EMR_EXECUTION_ROLE_ARN environment variable not set")),
      s3LogUri = sys.env.getOrElse("EMR_LOG_URI", throw new Exception("EMR_LOG_URI environment variable not set")),
      applicationName = appName,
      flinkEksServiceAccount = sys.env.get("FLINK_EKS_SERVICE_ACCOUNT"),
      flinkEksNamespace = sys.env.get("FLINK_EKS_NAMESPACE"),
      eksClusterName = sys.env.get("EKS_CLUSTER_NAME"),
      ingressBaseUrl = sys.env.get("HUB_BASE_URL"),
      emrStudioId = sys.env.get("EMR_STUDIO_ID"),
      cloudWatchLogGroupName = sys.env.get("EMR_CLOUDWATCH_LOG_GROUP"),
      flinkHealthCheckFn = uri => Await.result(flinkStatusProvider.isFlinkJobHealthy(uri), 30.seconds),
      flinkInternalJobIdFetchFn = uri => Await.result(flinkStatusProvider.getFlinkInternalJobId(uri), 30.seconds)
    )
    val resultJobId = submitter.submit(
      jobType = jobType,
      submissionProperties = submissionProps,
      jobProperties = modeConfigProperties.getOrElse(Map.empty),
      files = files.toList,
      labels = Map.empty,
      envVars = Map.empty,
      args = finalArgs: _*
    )

    val outputId = jobType match {
      case TypeSparkJob => s"${submitter.resolvedApplicationId}|$resultJobId"
      case TypeFlinkJob => resultJobId
    }
    println(s"Job submitted with ID: $outputId")
  }
}
