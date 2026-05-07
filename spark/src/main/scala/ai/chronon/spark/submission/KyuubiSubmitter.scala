package ai.chronon.spark.submission

import ai.chronon.api.Builders.MetaData
import ai.chronon.api.JobStatusType
import ai.chronon.spark.submission.JobSubmitterConstants._
import org.slf4j.{Logger, LoggerFactory}

/** Exception thrown when multiple running Flink jobs are found for the same GroupBy */
case class KyuubiMoreThanOneRunningFlinkJob(message: String) extends Exception(message)

/** Exception thrown when no running Flink job is found when expected */
case class KyuubiNoRunningFlinkJob(message: String) extends Exception(message)

/** Utility functions for Kyuubi job management */
object KyuubiUtils {

  /** Formats a label by converting to lowercase and replacing invalid characters with underscores.
    *
    * @param label The label to format
    * @return Formatted label with invalid characters replaced by underscores
    */
  def formatLabel(label: String): String = {
    label.replaceAll("[^a-zA-Z0-9_-]", "_").toLowerCase
  }

  /** Converts JobType to Kyuubi batch type string.
    *
    * @param jobType The job type (SparkJob or FlinkJob)
    * @return "SPARK" or "FLINK"
    */
  def jobTypeToBatchType(jobType: JobType): String = {
    jobType match {
      case SparkJob => "SPARK"
      case FlinkJob => "FLINK"
    }
  }

  /** Creates formatted labels/metadata from submission properties.
    *
    * @param jobType The job type
    * @param submissionProperties Submission properties containing metadata
    * @param additionalLabels Additional custom labels
    * @return Map of formatted labels
    */
  def createFormattedLabels(
      jobType: JobType,
      submissionProperties: Map[String, String],
      additionalLabels: Map[String, String] = Map.empty
  ): Map[String, String] = {
    val metadataName = submissionProperties.getOrElse(MetadataName, "")
    val ziplineVersion = submissionProperties.getOrElse(ZiplineVersion, "")

    val baseLabels = Map(
      JobType -> (jobType match {
        case SparkJob => SparkJobType
        case FlinkJob => FlinkJobType
      }),
      MetadataName -> metadataName,
      ZiplineVersion -> ziplineVersion
    ).filter(_._2.nonEmpty)

    (baseLabels ++ additionalLabels)
      .map(entry => (formatLabel(entry._1), formatLabel(entry._2)))
  }

  /** Generate a deterministic job name.
    *
    * @param metadataName The metadata name
    * @param jobType The job type
    * @return A deterministic job name
    */
  def generateJobName(metadataName: String, jobType: JobType): String = {
    val typePrefix = jobTypeToBatchType(jobType).toLowerCase
    val sanitizedName = formatLabel(metadataName)
    s"$typePrefix-$sanitizedName"
  }
}

/** Cloud-agnostic job submitter using Kyuubi REST API.
  *
  * Submits Spark and Flink jobs to a Kyuubi server which manages a Spark cluster.
  * Use the companion object's apply() methods to construct instances.
  *
  * @param baseUrl The base URL of the Kyuubi server (e.g., "http://kyuubi-server:10099")
  * @param kyuubiClient HTTP client for Kyuubi REST API
  * @param storageClient Optional storage client for checkpoint management (for Flink streaming jobs)
  */
class KyuubiSubmitter private[submission] (
    baseUrl: String,
    private val kyuubiClient: KyuubiClient,
    storageClient: Option[StorageClient],
    sparkHistoryServerUrl: Option[String] = None,
    override val jarName: String = "",
    override val onlineClass: String = "",
    override val dqMetricsDataset: String = "",
    override val kvStoreApiProperties: Map[String, String] = Map.empty
) extends JobSubmitter {

  /** List running Flink jobs for a given GroupBy name.
    *
    * @param groupByName The GroupBy name to search for
    * @return List of job IDs for running Flink jobs matching the name
    */
  def listRunningGroupByFlinkJobs(groupByName: String): List[String] = {
    val formattedName = KyuubiUtils.formatLabel(groupByName)

    logger.info(s"Searching for running Flink jobs with name pattern containing: $formattedName")

    val response = kyuubiClient.listBatches(
      batchType = Some("FLINK"),
      batchState = Some("RUNNING")
    )

    // Filter by name containing the groupBy name
    val matchingBatches = response.batches.filter { batch =>
      batch.name.exists(_.toLowerCase.contains(formattedName))
    }

    matchingBatches.map(_.id).toList
  }

  /** Get Zipline version label from a running job.
    *
    * @param jobId The job ID
    * @return Optional version string extracted from the job name
    */
  def getZiplineVersionOfJob(jobId: String): Option[String] = {
    val status = kyuubiClient.getBatchStatus(jobId)
    // Extract version from job name if encoded there (format: type-name-version-timestamp)
    status.name.flatMap { name =>
      val versionPattern = "v([0-9_]+)".r
      versionPattern.findFirstMatchIn(name).map(_.group(1))
    }
  }

  /** Get latest Flink checkpoint if storage client is available.
    *
    * @param groupByName The GroupBy name
    * @param manifestBucketPath The manifest bucket path
    * @param flinkCheckpointUri The Flink checkpoint URI
    * @return Optional URI to the latest checkpoint
    */
  def getLatestFlinkCheckpoint(
      groupByName: String,
      manifestBucketPath: String,
      flinkCheckpointUri: String
  ): Option[String] = {
    storageClient match {
      case None =>
        logger.warn("No storage client configured. Cannot retrieve Flink checkpoints.")
        None
      case Some(client) =>
        val manifestFileName = "manifest.txt"
        val groupByCheckpointPath = s"$manifestBucketPath/$groupByName"
        val manifestObjectPath = s"$groupByCheckpointPath/$manifestFileName"

        logger.info(s"Checking for manifest file at $manifestObjectPath")

        if (!client.fileExists(manifestObjectPath)) {
          logger.info(s"No manifest file found for $groupByName. Returning no checkpoints.")
          return None
        }

        val manifestStr = new String(client.downloadObjectToMemory(manifestObjectPath))
        val manifestTuples = manifestStr.split(",")
        val flinkJobTuple = manifestTuples.find(_.startsWith("flinkJobId"))
        val flinkJobId = flinkJobTuple
          .map(_.split("=")(1))
          .getOrElse(throw new RuntimeException("Flink job id not found in manifest file."))

        val flinkJobIdCheckpointPath = s"$flinkCheckpointUri/$flinkJobId"
        val matchedFiles = client.listFiles(flinkJobIdCheckpointPath).toList
        val allCheckpoints = matchedFiles
          .filter(_.split("/").exists(_.startsWith("chk-")))
          .map(_.split("/").find(_.startsWith("chk-")).get)
          .distinct
          .sortBy(chk => chk.substring(4).toInt)(Ordering.Int.reverse)

        logger.info(s"Flink checkpoints for $groupByName: $allCheckpoints")

        val latestCheckpoint = allCheckpoints.headOption
        val latestCheckpointUri = latestCheckpoint.map(chk => s"$flinkJobIdCheckpointPath/$chk")

        if (latestCheckpointUri.isEmpty) {
          logger.info(s"No checkpoints found for $groupByName.")
        } else {
          logger.info(s"Latest checkpoint for $groupByName: ${latestCheckpointUri.get}")
        }

        latestCheckpointUri
    }
  }

  override def status(jobId: String): JobStatusType = {
    try {
      val response = kyuubiClient.getBatchStatus(jobId)
      response.state.toUpperCase match {
        case "PENDING"             => JobStatusType.PENDING
        case "RUNNING"             => JobStatusType.RUNNING
        case "FINISHED"            => JobStatusType.SUCCEEDED
        case "ERROR"               => JobStatusType.FAILED
        case "KILLED" | "CANCELED" => JobStatusType.FAILED
        case _                     => JobStatusType.UNKNOWN
      }
    } catch {
      case e: KyuubiApiException =>
        logger.error(s"Error getting job status: ${e.getMessage}")
        JobStatusType.UNKNOWN
    }
  }

  override def kill(jobId: String): Unit = {
    try {
      kyuubiClient.deleteBatch(jobId)
      logger.info(s"Successfully killed job: $jobId")
    } catch {
      case e: KyuubiApiException =>
        logger.error(s"Error killing job $jobId: ${e.getMessage}")
        throw e
    }
  }

  override def submit(
      jobType: JobType,
      submissionProperties: Map[String, String],
      jobProperties: Map[String, String],
      files: List[String],
      labels: Map[String, String],
      envVars: Map[String, String],
      rawArgs: String*
  ): String = {
    val args = JobSubmitter.getApplicationArgs(jobType, rawArgs.toArray)
    val mainClass = submissionProperties.getOrElse(MainClass, throw new RuntimeException("Main class not found"))
    val jarUri = submissionProperties.getOrElse(JarURI, throw new RuntimeException("Jar URI not found"))
    val metadataName =
      submissionProperties.getOrElse(MetadataName, throw new RuntimeException("Metadata name not found"))

    val batchType = KyuubiUtils.jobTypeToBatchType(jobType)
    val jobName = KyuubiUtils.generateJobName(metadataName, jobType)

    // Build configuration map with labels
    val labelConf = labels.map { case (k, v) => s"spark.zipline.label.$k" -> v }

    // Add files to spark.files configuration
    val filesConf = if (files.nonEmpty) {
      Map("spark.files" -> files.mkString(","))
    } else {
      Map.empty[String, String]
    }

    // Handle additional JARs
    val additionalJarsConf = submissionProperties
      .get(AdditionalJars)
      .map { jars =>
        "spark.jars" -> jars
      }
      .toMap

    // Handle Flink-specific configuration
    val flinkConf = jobType match {
      case FlinkJob =>
        val checkpointConf = submissionProperties
          .get(FlinkCheckpointUri)
          .map { uri =>
            Map(
              "state.checkpoints.dir" -> uri,
              "state.savepoints.dir" -> uri
            )
          }
          .getOrElse(Map.empty)

        val savepointConf = submissionProperties
          .get(SavepointUri)
          .map { uri =>
            Map("execution.savepoint.path" -> uri)
          }
          .getOrElse(Map.empty)

        checkpointConf ++ savepointConf
      case SparkJob => Map.empty[String, String]
    }

    val completeConf =
      jobProperties ++ envVarsToSparkProperties(envVars) ++ labelConf ++ filesConf ++ additionalJarsConf ++ flinkConf

    val request = BatchSubmitRequest(
      batchType = batchType,
      resource = jarUri,
      className = mainClass,
      name = Some(jobName),
      args = args.toSeq,
      conf = completeConf,
      proxyUser = submissionProperties.get("proxyUser")
    )

    try {
      val response = kyuubiClient.submitBatch(request)
      logger.info(s"Kyuubi batch job submitted. ID: ${response.id}")
      logger.info(s"Job name: $jobName, State: ${response.state}")
      response.id
    } catch {
      case e: KyuubiApiException =>
        throw new RuntimeException(s"Failed to submit job: ${e.getMessage}", e)
    }
  }

  /** High-level run method similar to DataprocSubmitter.run.
    *
    * @param args Command-line arguments
    * @param labels Additional labels for the job
    * @return The submitted job ID
    */
  def run(
      args: Array[String],
      labels: Map[String, String] = Map.empty
  ): String = {
    // Get the job type
    val jobTypeValue = JobSubmitter
      .getArgValue(args, JobTypeArgKeyword)
      .getOrElse(throw new Exception("Missing required argument: " + JobTypeArgKeyword))

    val jobType = jobTypeValue.toLowerCase match {
      case SparkJobType => SparkJob
      case FlinkJobType => FlinkJob
      case _            => throw new Exception(s"Invalid job type: $jobTypeValue")
    }

    // Additional checks for streaming/Flink jobs
    if (jobType == FlinkJob) {
      val groupByName = JobSubmitter
        .getArgValue(args, GroupByNameArgKeyword)
        .getOrElse(throw new Exception("Missing required argument: " + GroupByNameArgKeyword))

      val maybeJobId = findRunningFlinkJobId(groupByName)

      val streamingMode = JobSubmitter
        .getArgValue(args, StreamingModeArgKeyword)
        .getOrElse(throw new Exception("Missing required argument: " + StreamingModeArgKeyword))

      if (CheckIfJobIsRunning.equals(streamingMode)) {
        if (maybeJobId.isEmpty) {
          logger.info(s"No running Flink job found for GroupBy name $groupByName.")
          throw KyuubiNoRunningFlinkJob(s"No running Flink job found for GroupBy name $groupByName")
        } else {
          logger.info(s"One running Flink job found for GroupBy name $groupByName. Job id = ${maybeJobId.get}")
          return maybeJobId.get
        }
      } else if (StreamingDeploy.equals(streamingMode)) {
        // Validate savepoint strategy
        KyuubiSubmitter.validateOnlyOneFlinkSavepointDeploymentStrategySet(args)

        if (maybeJobId.isDefined) {
          val matchedId = maybeJobId.get
          kill(matchedId)
          logger.info(s"Cancelled running Flink job with id $matchedId for GroupBy name $groupByName.")
        }
      }
    }

    // Build submission properties
    val submissionProps = createSubmissionPropsMap(jobType, args)

    // Get job properties from mode config
    val jobProperties = JobSubmitter.getModeConfigProperties(args).getOrElse(Map.empty)

    // Submit the job
    val jobId = submit(
      jobType = jobType,
      submissionProperties = submissionProps,
      jobProperties = jobProperties,
      files = KyuubiSubmitter.getFilesArgs(args),
      labels = labels,
      envVars = Map.empty,
      rawArgs = args: _*
    )

    logger.info(s"Kyuubi job submitted. ID: $jobId")
    logger.info(s"Monitor job at: ${baseUrl}/api/v1/batches/$jobId")
    jobId
  }

  /** Create submission properties map from arguments.
    *
    * @param jobType The job type
    * @param args Command-line arguments
    * @return Map of submission properties
    */
  private[submission] def createSubmissionPropsMap(
      jobType: JobType,
      args: Array[String]
  ): Map[String, String] = {
    val jarUri = JobSubmitter
      .getArgValue(args, JarUriArgKeyword)
      .getOrElse(throw new Exception("Missing required argument: " + JarUriArgKeyword))
    val mainClass = JobSubmitter
      .getArgValue(args, MainClassKeyword)
      .getOrElse(throw new Exception("Missing required argument: " + MainClassKeyword))

    val metadataName = Option(JobSubmitter.getMetadata(args).getOrElse(MetaData()).getName).getOrElse("")

    val jobId = JobSubmitter
      .getArgValue(args, JobIdArgKeyword)
      .getOrElse(throw new Exception("Missing required argument: " + JobIdArgKeyword))

    val additionalJarsProps = JobSubmitter.getArgValue(args, AdditionalJarsUriArgKeyword).map(AdditionalJars -> _)

    val baseProps = jobType match {
      case SparkJob =>
        Map(
          MainClass -> mainClass,
          JarURI -> jarUri,
          MetadataName -> metadataName,
          JobId -> jobId
        ) ++ additionalJarsProps

      case FlinkJob =>
        val flinkCheckpointUri = JobSubmitter
          .getArgValue(args, StreamingCheckpointPathArgKeyword)
          .getOrElse(throw new Exception(s"Missing required argument $StreamingCheckpointPathArgKeyword"))
        val flinkMainJarUri = JobSubmitter
          .getArgValue(args, FlinkMainJarUriArgKeyword)
          .getOrElse(throw new Exception("Missing required argument: " + FlinkMainJarUriArgKeyword))

        val groupByName = JobSubmitter
          .getArgValue(args, GroupByNameArgKeyword)
          .getOrElse(throw new Exception("Missing required argument: " + GroupByNameArgKeyword))

        val baseJobProps = Map(
          MainClass -> mainClass,
          JarURI -> jarUri,
          FlinkMainJarURI -> flinkMainJarUri,
          FlinkCheckpointUri -> flinkCheckpointUri,
          MetadataName -> groupByName,
          JobId -> jobId
        ) ++ additionalJarsProps

        // Handle savepoint
        val userPassedSavepoint = JobSubmitter.getArgValue(args, StreamingCustomSavepointArgKeyword)
        val maybeSavepointUri = if (userPassedSavepoint.isDefined) {
          userPassedSavepoint
        } else if (args.contains(StreamingLatestSavepointArgKeyword)) {
          getLatestFlinkCheckpoint(
            groupByName = groupByName,
            manifestBucketPath = JobSubmitter
              .getArgValue(args, StreamingManifestPathArgKeyword)
              .getOrElse(throw new Exception("Missing required argument: " + StreamingManifestPathArgKeyword)),
            flinkCheckpointUri = flinkCheckpointUri
          )
        } else {
          None
        }

        maybeSavepointUri match {
          case Some(uri) =>
            logger.info(s"Deploying Flink app with savepoint uri $uri")
            baseJobProps + (SavepointUri -> uri)
          case None => baseJobProps
        }
    }

    // Add version info
    baseProps ++ Map(
      ZiplineVersion -> JobSubmitter
        .getArgValue(args, ZiplineVersionArgKeyword)
        .getOrElse(throw new Exception("Missing required argument: " + ZiplineVersionArgKeyword))
    )
  }

  private def findRunningFlinkJobId(groupByName: String): Option[String] = {
    val matchedIds = listRunningGroupByFlinkJobs(groupByName)
    if (matchedIds.size > 1) {
      throw KyuubiMoreThanOneRunningFlinkJob(
        s"Multiple running Flink jobs found for GroupBy name $groupByName. " +
          s"Only one should be active. Job ids = ${matchedIds.mkString(", ")}"
      )
    } else if (matchedIds.isEmpty) {
      logger.info(s"No running Flink jobs found for GroupBy name $groupByName.")
      None
    } else {
      Some(matchedIds.head)
    }
  }

  override def getSparkUrl(jobId: String): Option[String] = {
    try {
      val batchStatus = kyuubiClient.getBatchStatus(jobId)
      batchStatus.appId match {
        case Some(sparkAppId) =>
          sparkHistoryServerUrl match {
            case Some(shsUrl) =>
              Some(s"${shsUrl.stripSuffix("/")}/history/$sparkAppId/jobs/")
            case None => batchStatus.appUrl
          }
        case None => None
      }
    } catch {
      case e: Exception =>
        logger.error(s"Failed to get Spark URL for job $jobId: ${e.getMessage}", e)
        None
    }
  }

  override def close(): Unit = {
    try {
      kyuubiClient.close()
    } catch {
      case _: Exception => logger.info("Error shutting down Kyuubi client.")
    }
  }
}

object KyuubiSubmitter {

  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  def apply(
      baseUrl: String,
      auth: KyuubiAuth = KyuubiAuth.NoAuth,
      storageClient: Option[StorageClient] = None,
      sparkHistoryServerUrl: Option[String] = None,
      jarName: String = "",
      onlineClass: String = "",
      dqMetricsDataset: String = "",
      kvStoreApiProperties: Map[String, String] = Map.empty
  ): KyuubiSubmitter = {
    val client = KyuubiClient(baseUrl, auth)
    new KyuubiSubmitter(baseUrl,
                        client,
                        storageClient,
                        sparkHistoryServerUrl,
                        jarName,
                        onlineClass,
                        dqMetricsDataset,
                        kvStoreApiProperties)
  }

  /** Factory method for testing with a mock client. */
  def apply(kyuubiClient: KyuubiClient): KyuubiSubmitter = {
    new KyuubiSubmitter(kyuubiClient.baseUrl, kyuubiClient, None)
  }

  /** Get files arguments from command line.
    *
    * @param args Command-line arguments
    * @return List of file paths
    */
  private[submission] def getFilesArgs(args: Array[String] = Array.empty): List[String] = {
    val filesArgs = args.filter(_.startsWith(FilesArgKeyword))
    if (filesArgs.isEmpty) {
      List.empty
    } else {
      filesArgs(0).split("=")(1).split(",").toList
    }
  }

  /** Validate that only one Flink savepoint strategy is set.
    *
    * @param args Command-line arguments
    * @throws Exception if zero or multiple savepoint strategies are set
    */
  private def validateOnlyOneFlinkSavepointDeploymentStrategySet(args: Array[String]): Unit = {
    val isStreamingLatestSet = args.contains(StreamingLatestSavepointArgKeyword)
    val isStreamingWithSavepointSet = JobSubmitter.getArgValue(args, StreamingCustomSavepointArgKeyword).isDefined
    val isStreamingNoSavepointSet = args.contains(StreamingNoSavepointArgKeyword)

    List(isStreamingLatestSet, isStreamingWithSavepointSet, isStreamingNoSavepointSet).count(_ == true) match {
      case 0 => throw new Exception("No savepoint deploy strategy provided")
      case 1 => // OK
      case _ =>
        throw new Exception(
          "Multiple savepoint deploy strategies provided. " +
            s"Only one of $StreamingLatestSavepointArgKeyword, $StreamingCustomSavepointArgKeyword, " +
            s"$StreamingNoSavepointArgKeyword should be provided"
        )
    }
  }

  /** Main entry point for KyuubiSubmitter.
    *
    * Reads the Kyuubi server URL from SPARK_CLUSTER_NAME environment variable.
    * The SPARK_CLUSTER_NAME should be in the format "host:port" (e.g., "kyuubi-server:10099").
    *
    * @param args Command-line arguments
    */
  def main(args: Array[String]): Unit = {
    val clusterName = sys.env
      .getOrElse(
        SparkClusterNameEnvVar,
        throw new Exception(
          s"$SparkClusterNameEnvVar is not set. " +
            s"Please set $SparkClusterNameEnvVar to the Kyuubi server URL (e.g., 'https://kyuubi-server:10099').")
      )

    val baseUrl = KyuubiClient.resolveUrl(clusterName)

    logger.info(s"Connecting to Kyuubi server at: $baseUrl")

    val submitter = KyuubiSubmitter(baseUrl)
    val jobId = submitter.run(args)
    logger.info(s"KyuubiSubmitter job id: $jobId")
  }

}
