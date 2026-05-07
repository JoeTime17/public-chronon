package ai.chronon.integrations.cloud_gcp
import ai.chronon.api.Builders.MetaData
import ai.chronon.api.JobStatusType
import ai.chronon.spark.submission.JobSubmitterConstants._
import ai.chronon.spark.submission.{
  JobSubmitter,
  JobType,
  StorageClient,
  FlinkJob => TypeFlinkJob,
  SparkJob => TypeSparkJob
}
import com.google.api.gax.rpc.ApiException
import com.google.cloud.dataproc.v1._
import com.google.cloud.storage.{Storage, StorageOptions}
import com.google.protobuf.util.JsonFormat

import java.time.{Duration, Instant}
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.util.matching.Regex

case class MoreThanOneRunningFlinkJob(message: String) extends Exception(message)

case class NoRunningFlinkJob(message: String) extends Exception(message)

class DataprocSubmitter(jobControllerClient: JobControllerClient,
                        gcsClient: GCSClient,
                        val region: String,
                        val projectId: String,
                        clusterControllerClient: Option[ClusterControllerClient] = None,
                        rawStorageClient: Option[Storage] = None,
                        bigtableInstanceId: String = "",
                        override val dqMetricsDataset: String = "",
                        flinkHealthCheckFn: Option[String] => Boolean = _ => true,
                        flinkInternalJobIdFetchFn: Option[String] => Option[String] = _ => None)
    extends JobSubmitter {

  def listRunningGroupByFlinkJobs(groupByName: String): List[String] = {
    val groupByNameDataprocLabel = DataprocUtils.formatDataprocLabel(groupByName)

    //  String values for filters cannot have quotations or else search doesn't work.
    // for example "labels.job-type = flink" and NOT "labels.job-type = 'flink'"
    val filters =
      s"status.state = ACTIVE AND labels.job-type = flink AND labels.metadata-name = $groupByNameDataprocLabel"

    logger.info(s"Searching for running flink jobs with filter: [$filters]")

    val listResult = jobControllerClient.listJobs(
      ListJobsRequest
        .newBuilder()
        .setProjectId(projectId)
        .setRegion(region)
        .setFilter(filters)
        .build()
    )
    listResult.iterateAll().iterator().asScala.toList.map(_.getReference.getJobId)
  }

  def getZiplineVersionOfDataprocJob(jobId: String): String = {
    val currentJob: Job = jobControllerClient.getJob(projectId, region, jobId)
    val labels = currentJob.getLabelsMap
    labels.get(DataprocUtils.formatDataprocLabel(ZiplineVersion))
  }

  def getLatestFlinkCheckpoint(groupByName: String,
                               manifestBucketPath: String,
                               flinkCheckpointUri: String): Option[String] = {
    val manifestFileName = "manifest.txt"

    val groupByCheckpointPath = s"$manifestBucketPath/$groupByName"
    val manifestObjectPath = s"$groupByCheckpointPath/$manifestFileName"
    logger.info(s"Checking for manifest file at $manifestObjectPath")

    if (!gcsClient.fileExists(manifestObjectPath)) {
      logger.info(s"No manifest file found for $groupByName. Returning no checkpoints.")
      return None // No manifest file found
    }

    val manifestStr = new String(gcsClient.downloadObjectToMemory(manifestObjectPath))
    val manifestTuples = manifestStr.split(",")
    val flinkJobTuple = manifestTuples.find(_.startsWith("flinkJobId"))
    val flinkJobId = flinkJobTuple
      .map(_.split("=")(1))
      .getOrElse(throw new RuntimeException("Flink job id not found in manifest file."))

    // flinkCheckpointUri is already the checkpoints base path (e.g. gs://bucket/flink-state/checkpoints).
    // List directly under it rather than going through StorageClient.resolveLatestCheckpointPath which
    // appends /checkpoints/ and uses a different path convention.
    val jobCheckpointPath = s"$flinkCheckpointUri/$flinkJobId"
    val latestCheckpoint = gcsClient
      .listFiles(jobCheckpointPath)
      .filter(_.split("/").exists(_.startsWith("chk-")))
      .map(_.split("/").find(_.startsWith("chk-")).get)
      .toList
      .distinct
      .sortBy(_.substring(4).toInt)(Ordering.Int.reverse)
      .headOption
    val latestCheckpointUri = latestCheckpoint.map(chk => s"$jobCheckpointPath/$chk")
    logger.info(s"Latest checkpoint for $groupByName: $latestCheckpointUri")
    latestCheckpointUri
  }

  override def status(jobId: String): JobStatusType = {
    try {
      val job: Job = jobControllerClient.getJob(projectId, region, jobId)

      val isFlinkJob =
        job.getTypeJobCase == Job.TypeJobCase.FLINK_JOB || job.getLabelsMap.asScala.get(JobType).contains(FlinkJobType)
      lazy val isRunningAndHealthy =
        job.getStatus.getState == JobStatus.State.RUNNING && flinkHealthCheckFn(getFlinkUrl(jobId))

      val jobStatusType = job.getStatus.getState match {
        case JobStatus.State.PENDING                                       => JobStatusType.PENDING
        case JobStatus.State.ERROR                                         => JobStatusType.FAILED
        case JobStatus.State.DONE                                          => JobStatusType.SUCCEEDED
        case JobStatus.State.RUNNING if isFlinkJob && isRunningAndHealthy  => JobStatusType.RUNNING
        case JobStatus.State.RUNNING if isFlinkJob && !isRunningAndHealthy =>
          // Health check failed. Derive submission time from the already-fetched job object
          // (no extra RPC) and delegate grace period logic to the shared utility.
          // Filter out entries whose stateStartTime is unset (epoch 0) to avoid treating
          // the proto default as a real submission time, which would make withinGrace always false.
          val submissionTime = job.getStatusHistoryList.asScala
            .find(s => s.hasStateStartTime && s.getStateStartTime.getSeconds > 0)
            .map(s => Instant.ofEpochSecond(s.getStateStartTime.getSeconds))
          JobSubmitter.flinkStatusWithGrace(
            jobId,
            submissionTime,
            DataprocSubmitter.FlinkHealthCheckGracePeriod,
            logger
          )
        case JobStatus.State.RUNNING | JobStatus.State.SETUP_DONE => JobStatusType.RUNNING
        case JobStatus.State.CANCEL_STARTED | JobStatus.State.CANCEL_PENDING | JobStatus.State.CANCELLED =>
          JobStatusType.FAILED
        case _ => JobStatusType.UNKNOWN
      }

      emitJobRuntimeMetrics(job, jobStatusType)
      jobStatusType
    } catch {
      case e: ApiException =>
        logger.error(s"Error monitoring job: ${e.getMessage}")
        JobStatusType.UNKNOWN
    }
  }

  private def emitJobRuntimeMetrics(job: Job, jobStatusType: JobStatusType): Unit = {
    import ai.chronon.online.metrics.Metrics

    val isTerminalState = jobStatusType match {
      case JobStatusType.SUCCEEDED | JobStatusType.FAILED | JobStatusType.CANCELLED => true
      case _                                                                        => false
    }

    if (isTerminalState) {
      try {
        val placement = job.getPlacement
        val submittedTime = if (job.getStatusHistoryList.asScala.nonEmpty) {
          job.getStatusHistoryList.asScala.head.getStateStartTime
        } else null

        val endTime = job.getStatus.getStateStartTime

        if (submittedTime != null && endTime != null) {
          val runtimeMillis = (endTime.getSeconds - submittedTime.getSeconds) * 1000

          val ctx = Metrics.Context(Metrics.Environment.Orchestrator)

          val jobType = job.getTypeJobCase match {
            case Job.TypeJobCase.SPARK_JOB => "spark"
            case Job.TypeJobCase.FLINK_JOB => "flink"
            case _                         => job.getLabelsMap.asScala.getOrElse(JobType, "unknown")
          }

          val tags = Map(
            "job_type" -> jobType,
            "status" -> jobStatusType.toString,
            "cluster" -> placement.getClusterName
          )

          ctx.distribution("job.runtime.millis", runtimeMillis, tags)
        }
      } catch {
        case ex: Exception =>
          logger.warn(s"Failed to emit job runtime metrics for job ${job.getReference.getJobId}", ex)
      }
    }
  }

  override def kill(jobId: String): Unit = {
    val job = jobControllerClient.cancelJob(projectId, region, jobId)
    job.getDone
  }

  override def submit(jobType: JobType,
                      submissionProperties: Map[String, String],
                      jobProperties: Map[String, String],
                      files: List[String],
                      labels: Map[String, String],
                      envVars: Map[String, String],
                      rawArgs: String*): String = {
    val args = JobSubmitter.getApplicationArgs(
      jobType = jobType,
      args = rawArgs.toArray
    )
    val mainClass = submissionProperties.getOrElse(MainClass, throw new RuntimeException("Main class not found"))
    val jarUri = submissionProperties.getOrElse(JarURI, throw new RuntimeException("Jar URI not found"))
    val clusterName = submissionProperties.getOrElse(ClusterName, throw new RuntimeException("Cluster name not found"))

    val jobId = submissionProperties.getOrElse(JobId, throw new RuntimeException("No generated job id found"))

    val maybeAdditionalJarsUri = submissionProperties.get(AdditionalJars)
    val additionalJars = maybeAdditionalJarsUri
      .map(_.split(",").map(_.trim).filter(_.nonEmpty))
      .getOrElse(Array.empty)

    val effectiveJobProperties = jobProperties ++ envVarsToSparkProperties(envVars)

    val jobBuilder = jobType match {
      case TypeSparkJob =>
        val jarUris = Array(jarUri) ++ additionalJars
        buildSparkJob(mainClass, jarUris, files, effectiveJobProperties, args: _*)
      case TypeFlinkJob =>
        val mainJarUri =
          submissionProperties.getOrElse(FlinkMainJarURI,
                                         throw new RuntimeException(s"Missing expected $FlinkMainJarURI"))
        val flinkCheckpointPath =
          submissionProperties.getOrElse(FlinkCheckpointUri,
                                         throw new RuntimeException(s"Missing expected $FlinkCheckpointUri"))
        val maybeSavepointUri = submissionProperties.get(SavepointUri)
        val maybeFlinkJarsBasePath = submissionProperties.get(FlinkJarsUri)
        val maybePubSubConnectorJarUri = submissionProperties.get(FlinkPubSubConnectorJarURI)
        val jarUris = Array(jarUri) ++ maybePubSubConnectorJarUri.toList ++ additionalJars
        val flinkDeploymentMode = JobSubmitter
          .getArgValue(rawArgs.toArray, FlinkDeploymentModeArgKeyword)
          .getOrElse(FlinkDeploymentModeDefault)
        val flinkArgs = (args :+ "--parent-job-id" :+ jobId)

        flinkDeploymentMode match {
          case FlinkDeploymentModeApplication =>
            buildFlinkApplicationJob(mainClass,
                                     mainJarUri,
                                     jarUri,
                                     jarUris,
                                     flinkCheckpointPath,
                                     maybeSavepointUri,
                                     maybeFlinkJarsBasePath,
                                     effectiveJobProperties,
                                     flinkArgs: _*)
          case _ =>
            buildFlinkJob(mainClass,
                          mainJarUri,
                          jarUris,
                          flinkCheckpointPath,
                          maybeSavepointUri,
                          maybeFlinkJarsBasePath,
                          effectiveJobProperties,
                          flinkArgs: _*)
        }
    }

    val metadataName =
      submissionProperties.getOrElse(MetadataName, throw new RuntimeException("Metadata name not found"))

    val jobPlacement = JobPlacement
      .newBuilder()
      .setClusterName(clusterName)
      .build()

    try {
      val formattedDataprocLabels = DataprocUtils.createFormattedDataprocLabels(
        jobType = jobType,
        submissionProperties = submissionProperties,
        additionalLabels = labels
      )

      logger.info(s"Submitting Dataproc job with ID: $jobId")
      logger.info(s"Cluster name: ${jobPlacement.getClusterName}")
      logger.info(s"Formatted labels: $formattedDataprocLabels")
      logger.info(s"Metadata name from submission properties: ${submissionProperties.get(MetadataName)}")

      val job = jobBuilder
        .setReference(jobReference(jobId))
        .setPlacement(jobPlacement)
        .putAllLabels(formattedDataprocLabels.asJava)
        .build()

      val submittedJob = jobControllerClient.submitJob(projectId, region, job)
      val submittedJobId = submittedJob.getReference.getJobId

      if (jobId != submittedJobId) {
        throw new IllegalStateException(s"Computed job id $jobId does not match submitted job id $submittedJobId")
      }
      submittedJobId

    } catch {
      case e: ApiException =>
        throw new RuntimeException(s"Failed to submit job: ${e.getMessage}", e)
    }
  }

  private def buildSparkJob(mainClass: String,
                            jarUris: Array[String],
                            files: List[String],
                            jobProperties: Map[String, String],
                            args: String*): Job.Builder = {
    val sparkJob = SparkJob
      .newBuilder()
      .putAllProperties(jobProperties.asJava)
      .setMainClass(mainClass)
      .addAllJarFileUris(jarUris.toIterable.asJava)
      .addAllFileUris(files.asJava)
      .addAllArgs(args.toIterable.asJava)
      .build()
    Job.newBuilder().setSparkJob(sparkJob)
  }

  // JobManager is primarily responsible for coordinating the job (task slots, checkpoint triggering) and not much else
  // so 4G should suffice.
  // We go with 64G TM containers (4 task slots per container)
  // Broadly Flink splits TM memory into:
  // 1) Metaspace, framework offheap etc
  // 2) Network buffers
  // 3) Managed Memory (rocksdb)
  // 4) JVM heap
  // We tune down the network buffers to 1G-2G (default would be ~6.3G) and use some of the extra memory for
  // managed mem + jvm heap
  // Good doc - https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/memory/mem_setup_tm
  private[cloud_gcp] def flinkEnvProps(flinkCheckpointUri: String): Map[String, String] =
    Map(
      "jobmanager.memory.process.size" -> "4G",
      "taskmanager.memory.process.size" -> "64G",
      "taskmanager.memory.network.min" -> "1G",
      "taskmanager.memory.network.max" -> "2G",
      // explicitly set the number of task slots as otherwise it defaults to the number of cores
      // we go with multiple slots per TM as it allows us to squeeze more parallelism out of our resources
      "taskmanager.numberOfTaskSlots" -> "4",
      "taskmanager.memory.managed.fraction" -> "0.5f",
      // default is 256m, we seem to be close to the limit so we give ourselves some headroom
      "taskmanager.memory.jvm-metaspace.size" -> "512m",
      // bump this a bit as Kafka and KV stores often need direct buffers
      "taskmanager.memory.task.off-heap.size" -> "1G",
      "yarn.classpath.include-user-jar" -> "FIRST",
      "state.savepoints.dir" -> flinkCheckpointUri,
      "state.checkpoints.dir" -> flinkCheckpointUri,
      // override the local dir for rocksdb as the default ends up being too large file name size wise
      "state.backend.rocksdb.localdir" -> "/tmp/flink-state",
      "state.checkpoint-storage" -> "filesystem",
      "rest.flamegraph.enabled" -> "true",
      // wire up prometheus reporter - prom reporter plays well with Google ops agent that can be installed in DataProc
      // as we can have a couple of containers on a given node, we use a port range
      "metrics.reporters" -> "prom",
      "metrics.reporter.prom.factory.class" -> "org.apache.flink.metrics.prometheus.PrometheusReporterFactory",
      "metrics.reporter.prom.host" -> "localhost",
      "metrics.reporter.prom.port" -> "9250-9260",
      "metrics.reporter.statsd.interval" -> "60 SECONDS",
      "state.backend.type" -> "rocksdb",
      "state.backend.incremental" -> "true",
      "state.checkpoints.num-retained" -> MaxRetainedCheckpoints
    )

  private[cloud_gcp] def buildFlinkJob(mainClass: String,
                                       mainJarUri: String,
                                       jarUris: Array[String],
                                       flinkCheckpointUri: String,
                                       maybeSavePointUri: Option[String],
                                       maybeFlinkJarsBasePath: Option[String],
                                       jobProperties: Map[String, String],
                                       args: String*): Job.Builder = {

    val envProps = flinkEnvProps(flinkCheckpointUri)

    val updatedJarUris = jarUris ++ DataprocAdditionalJars.additionalFlinkJobJars(maybeFlinkJarsBasePath)

    val flinkJobBuilder = FlinkJob
      .newBuilder()
      .setMainClass(mainClass)
      .setMainJarFileUri(mainJarUri)
      .putAllProperties((envProps ++ jobProperties).asJava)
      .addAllJarFileUris(updatedJarUris.toIterable.asJava)
      .addAllArgs(args.toIterable.asJava)

    val updatedFlinkJobBuilder =
      maybeSavePointUri match {
        case Some(savePointUri) => flinkJobBuilder.setSavepointUri(savePointUri)
        case None               => flinkJobBuilder
      }

    Job
      .newBuilder()
      .setFlinkJob(updatedFlinkJobBuilder.build())
  }

  private[cloud_gcp] def buildFlinkApplicationJob(mainClass: String,
                                                  mainJarUri: String,
                                                  launcherJarUri: String,
                                                  jarUris: Array[String],
                                                  flinkCheckpointUri: String,
                                                  maybeSavePointUri: Option[String],
                                                  maybeFlinkJarsBasePath: Option[String],
                                                  jobProperties: Map[String, String],
                                                  args: String*): Job.Builder = {

    val allProperties = flinkEnvProps(flinkCheckpointUri) ++ jobProperties

    // Serialize Flink properties as launcher arguments
    val launcherArgs = new scala.collection.mutable.ArrayBuffer[String]()
    launcherArgs += "--flink-main-class"
    launcherArgs += mainClass
    launcherArgs += "--flink-main-jar"
    launcherArgs += mainJarUri
    launcherArgs += "--flink-checkpoint-uri"
    launcherArgs += flinkCheckpointUri

    allProperties.foreach { case (k, v) =>
      launcherArgs += "--flink-property"
      launcherArgs += s"$k=$v"
    }

    val updatedJarUris = jarUris ++ DataprocAdditionalJars.additionalFlinkJobJars(maybeFlinkJarsBasePath)
    if (updatedJarUris.nonEmpty) {
      launcherArgs += "--flink-jar-uris"
      launcherArgs += updatedJarUris.mkString(",")
    }

    maybeSavePointUri.foreach { uri =>
      launcherArgs += "--savepoint-uri"
      launcherArgs += uri
    }

    // Pass application args for FlinkJob.main()
    args.foreach { arg =>
      launcherArgs += "--flink-arg"
      launcherArgs += arg
    }

    val hadoopJob = HadoopJob
      .newBuilder()
      .setMainClass("ai.chronon.integrations.cloud_gcp.FlinkApplicationLauncher")
      .addAllJarFileUris(Seq(launcherJarUri).asJava)
      .addAllArgs(launcherArgs.toSeq.asJava)
      .build()

    Job
      .newBuilder()
      .setHadoopJob(hadoopJob)
  }

  private def jobReference(jobId: String): JobReference = {
    JobReference
      .newBuilder()
      .setJobId(jobId)
      .build()
  }

  def run(args: Array[String],
          envMap: Map[String, Option[String]] = Map.empty,
          maybeConf: Option[Map[String, String]] = None,
          clusterName: String,
          labels: Map[String, String] = Map.empty): String = {
    // Get the job type
    val jobTypeValue = JobSubmitter
      .getArgValue(args, JobTypeArgKeyword)
      .getOrElse(throw new Exception("Missing required argument: " + JobTypeArgKeyword))
    val jobType = jobTypeValue.toLowerCase match {
      case SparkJobType => TypeSparkJob
      case FlinkJobType => TypeFlinkJob
      case _            => throw new Exception("Invalid job type")
    }

    // Additional checks for streaming
    if (jobType == TypeFlinkJob) {
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
          throw NoRunningFlinkJob(
            s"No running Flink job found for GroupBy name $groupByName"
          )
        } else {
          logger.info(s"One running Flink job found for GroupBy name $groupByName. Job id = ${maybeJobId.get}")
          return maybeJobId.get
        }
      } else if (StreamingDeploy.equals(streamingMode)) {
        if (args.contains(StreamingVersionCheckDeploy) && maybeJobId.isDefined) {
          if (compareZiplineVersionOfRunningFlinkJob(args, maybeJobId.get)) {
            logger.info(s"Local Zipline version matches running Flink app's Zipline version. Exiting")
            return maybeJobId.get
          } else {
            logger.info(s"Local Zipline version does not match remote Zipline version. Proceeding with deployment.")
          }
        }

        // Check that only one type of savepoint deploy strategy is provided
        DataprocSubmitter.validateOnlyOneFlinkSavepointDeploymentStrategySet(args)

        if (maybeJobId.isDefined) {
          val matchedId = maybeJobId.get
          kill(matchedId)
          logger.info(s"Cancelled running Flink job with id $matchedId for GroupBy name $groupByName.")
        }
      }
    }

    // Filter and finalize application args
    val jobId = submit(
      jobType = jobType,
      submissionProperties =
        createSubmissionPropsMap(jobType = jobType, args = args, envMap = envMap, clusterName = clusterName),
      jobProperties = maybeConf.getOrElse(JobSubmitter.getModeConfigProperties(args).getOrElse(Map.empty)),
      files = DataprocSubmitter.getDataprocFilesArgs(args),
      labels = labels,
      envVars = Map.empty,
      rawArgs = args: _*
    )
    logger.info("Dataproc submitter job id: " + jobId)
    logger.info(
      s"Safe to exit. Follow the job status at: https://console.cloud.google.com/dataproc/jobs/${jobId}/configuration?region=${region}&project=${projectId}")
    jobId
  }

  private[cloud_gcp] def createSubmissionPropsMap(jobType: JobType,
                                                  envMap: Map[String, Option[String]] = Map.empty,
                                                  args: Array[String],
                                                  clusterName: String): Map[String, String] = {
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

    // include additional jars if present
    val additionalJarsProps = JobSubmitter.getArgValue(args, AdditionalJarsUriArgKeyword).map(AdditionalJars -> _)

    val submissionProps = jobType match {
      case TypeSparkJob =>
        Map(MainClass -> mainClass,
            JarURI -> jarUri,
            MetadataName -> metadataName,
            JobId -> jobId) ++ additionalJarsProps
      case TypeFlinkJob =>
        val flinkCheckpointUri = JobSubmitter
          .getArgValue(args, StreamingCheckpointPathArgKeyword)
          .getOrElse(throw new Exception(s"Missing required argument $StreamingCheckpointPathArgKeyword"))
        val flinkMainJarUri = JobSubmitter
          .getArgValue(args, FlinkMainJarUriArgKeyword)
          .getOrElse(throw new Exception("Missing required argument: " + FlinkMainJarUriArgKeyword))
        // pull the pubsub connector uri if it has been passed
        val maybePubSubJarUri = JobSubmitter
          .getArgValue(args, FlinkPubSubJarUriArgKeyword)
        // pull the flink jars base path if it has been passed
        val maybeFlinkJarsUri = JobSubmitter
          .getArgValue(args, FlinkJarsUriArgKeyword)

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
        ) ++ (maybePubSubJarUri.map(FlinkPubSubConnectorJarURI -> _) ++
          maybeFlinkJarsUri.map(FlinkJarsUri -> _) ++
          additionalJarsProps)

        val userPassedSavepoint = JobSubmitter
          .getArgValue(args, StreamingCustomSavepointArgKeyword)
        val maybeSavepointUri =
          if (userPassedSavepoint.isDefined) {
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

        if (maybeSavepointUri.isEmpty) {
          baseJobProps
        } else {
          val savepointUri = maybeSavepointUri.get
          logger.info(s"Deploying Flink app with savepoint uri $savepointUri")
          baseJobProps + (SavepointUri -> savepointUri)
        }
      case _ => throw new Exception("Invalid job type")
    }

    // Add additional properties
    submissionProps ++ Map(
      ZiplineVersion -> JobSubmitter
        .getArgValue(args, ZiplineVersionArgKeyword)
        .getOrElse(throw new Exception("Missing required argument: " + ZiplineVersionArgKeyword)),
      ClusterName -> clusterName
    )
  }

  private def compareZiplineVersionOfRunningFlinkJob(
      args: Array[String],
      jobId: String
  ): Boolean = {
    val localZiplineVersion = DataprocUtils.formatDataprocLabel(
      JobSubmitter
        .getArgValue(args, LocalZiplineVersionArgKeyword)
        .getOrElse(throw new Exception("Missing required argument: " + LocalZiplineVersionArgKeyword)))
    val remoteZiplineVersion = getZiplineVersionOfDataprocJob(jobId)
    logger.info(
      s"Local Zipline version: $localZiplineVersion. " +
        s"Remote Zipline version for Dataproc job $jobId: $remoteZiplineVersion")
    localZiplineVersion == remoteZiplineVersion
  }

  private def findRunningFlinkJobId(groupByName: String): Option[String] = {
    val matchedIds =
      listRunningGroupByFlinkJobs(groupByName = groupByName)
    if (matchedIds.size > 1) {
      throw MoreThanOneRunningFlinkJob(
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

  // --- Phase 1+2 overrides ---

  override def jarName: String = "cloud_gcp_lib_deploy.jar"
  override def onlineClass: String = "ai.chronon.integrations.cloud_gcp.GcpApiImpl"

  override def getJobUrl(jobId: String): Option[String] =
    Some(s"https://console.cloud.google.com/dataproc/jobs/$jobId?region=$region&project=$projectId")

  private val yarnAppIdPattern: Regex = """application_\d+_\d+""".r

  private def extractYarnAppIdFromTrackingUrl(trackingUrl: String): Option[String] =
    yarnAppIdPattern.findFirstIn(trackingUrl)

  override def getSparkUrl(jobId: String): Option[String] = {
    clusterControllerClient.flatMap { ccClient =>
      val dataprocJob = jobControllerClient.getJob(
        GetJobRequest.newBuilder().setProjectId(projectId).setRegion(region).setJobId(jobId).build())
      val clusterId = dataprocJob.getPlacement.getClusterName
      for {
        headApp <- dataprocJob.getYarnApplicationsList.asScala.filterNot(_.getName.contains("Flink")).headOption
        appId <- extractYarnAppIdFromTrackingUrl(headApp.getTrackingUrl)
        cluster <- Option(ccClient.getCluster(projectId, region, clusterId))
        config <- Option(cluster.getConfig)
        endpoint <- Option(config.getEndpointConfig)
        addr <- Option(endpoint.getHttpPortsMap.get("Spark History Server")).map(_.stripSuffix("/"))
      } yield s"$addr/history/$appId"
    }
  }

  override def getFlinkUrl(jobId: String): Option[String] = {
    clusterControllerClient.flatMap { ccClient =>
      val dataprocJob = jobControllerClient.getJob(
        GetJobRequest.newBuilder().setProjectId(projectId).setRegion(region).setJobId(jobId).build())
      val clusterId = dataprocJob.getPlacement.getClusterName
      for {
        flinkApp <- dataprocJob.getYarnApplicationsList.asScala.find(app =>
          app.getName.contains("Flink") && app.getState == YarnApplication.State.RUNNING)
        appId <- extractYarnAppIdFromTrackingUrl(flinkApp.getTrackingUrl)
        cluster <- Option(ccClient.getCluster(projectId, region, clusterId))
        config <- Option(cluster.getConfig)
        endpoint <- Option(config.getEndpointConfig)
        yarnResourceManagerUrl <- Option(endpoint.getHttpPortsMap.get("YARN ResourceManager")).map(_.stripSuffix("/"))
      } yield {
        val baseUrl = yarnResourceManagerUrl.stripSuffix("/yarn")
        s"$baseUrl/gateway/default/yarn/proxy/$appId/"
      }
    }
  }

  override def getFlinkInternalJobId(jobId: String): Option[String] =
    flinkInternalJobIdFetchFn(getFlinkUrl(jobId))

  override def getLatestCheckpointPath(flinkInternalJobId: String, flinkStateUri: String): Option[String] = {
    val result = StorageClient.resolveLatestCheckpointPath(gcsClient, flinkInternalJobId, flinkStateUri)
    result match {
      case Some(path) => logger.info(s"Resolved latest checkpoint for Flink job $flinkInternalJobId: $path")
      case None =>
        logger.warn(
          s"No checkpoints found for Flink job $flinkInternalJobId at $flinkStateUri/checkpoints/$flinkInternalJobId")
    }
    result
  }

  override def deprecatedClusterNameEnvVars: Seq[String] = Seq(GcpDataprocClusterNameEnvVar)

  override def ensureClusterReady(clusterName: String, clusterConf: Option[Map[String, String]])(implicit
      ec: ExecutionContext): Option[String] = {
    clusterControllerClient match {
      case None => Some(clusterName)
      case Some(ccClient) =>
        try {
          val cluster = ccClient.getCluster(projectId, region, clusterName)
          if (cluster == null) {
            triggerAsyncClusterCreation(clusterName, clusterConf, ccClient)
            None
          } else {
            cluster.getStatus.getState match {
              case ClusterStatus.State.RUNNING | ClusterStatus.State.UPDATING =>
                Some(clusterName)
              case ClusterStatus.State.UNKNOWN | ClusterStatus.State.CREATING | ClusterStatus.State.STARTING |
                  ClusterStatus.State.REPAIRING =>
                logger.info(s"Cluster $clusterName not ready (state: ${cluster.getStatus.getState}).")
                None
              case state =>
                throw new IllegalStateException(
                  s"Cluster $clusterName is in state $state and cannot be used for job submission.")
            }
          }
        } catch {
          case _: com.google.api.gax.rpc.NotFoundException =>
            triggerAsyncClusterCreation(clusterName, clusterConf, ccClient)
            None
          case ex: Exception =>
            logger.error(s"Error checking cluster $clusterName readiness", ex)
            throw ex
        }
    }
  }

  private def triggerAsyncClusterCreation(clusterName: String,
                                          clusterConf: Option[Map[String, String]],
                                          ccClient: ClusterControllerClient)(implicit ec: ExecutionContext): Unit = {
    if (clusterConf.isDefined && clusterConf.get.contains("dataproc.config")) {
      logger.info(s"Cluster $clusterName not found. Triggering creation asynchronously.")
      Future {
        DataprocSubmitter.getOrCreateCluster(clusterName, clusterConf, projectId, region, ccClient)
      }.recover { case ex: Exception =>
        logger.error(s"Failed to create cluster $clusterName asynchronously", ex)
      }
    } else {
      logger.error(s"Cluster $clusterName does not exist and no cluster configuration provided to create it.")
      throw new IllegalArgumentException(
        s"Cluster $clusterName does not exist and no cluster configuration provided to create it. Please provide one.")
    }
  }

  override def close(): Unit = {
    try {
      jobControllerClient.shutdown()
      clusterControllerClient.foreach(_.shutdown())
    } catch {
      case _: Exception => logger.info("Error shutting down GCP clients.")
    }
  }

  override def kvStoreApiProperties: Map[String, String] = Map(
    GcpProjectIdEnvVar -> projectId,
    GcpBigtableInstanceIdEnvVar -> bigtableInstanceId,
    GcpLocationEnvVar -> region
  )

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
    if (enablePubSub)
      base + (FlinkPubSubConnectorJarURI -> s"$artifactPrefix/release/$version/jars/connectors_pubsub_deploy.jar")
    else base
  }
}

object DataprocSubmitter {

  // Grace period from job submission before an unhealthy RUNNING Flink job is marked FAILED.
  // Dataproc goes straight to RUNNING (no K8s operator layer), so this window covers full
  // startup including YARN app initialisation and checkpoint accumulation.
  val FlinkHealthCheckGracePeriod: Duration = Duration.ofMinutes(15)

  def apply(jobControllerClient: JobControllerClient,
            storageClient: Storage,
            region: String,
            projectId: String): DataprocSubmitter = {
    new DataprocSubmitter(jobControllerClient,
                          GCSClient(storageClient),
                          region,
                          projectId,
                          rawStorageClient = Some(storageClient))
  }

  private[cloud_gcp] def getDataprocFilesArgs(args: Array[String] = Array.empty): List[String] = {
    val gcsFilesArgs = args.filter(_.startsWith(FilesArgKeyword))
    val gcsFiles = if (gcsFilesArgs.isEmpty) {
      Array.empty[String]
    } else {
      gcsFilesArgs(0).split("=")(1).split(",")
    }
    gcsFiles.toList
  }

  private def validateOnlyOneFlinkSavepointDeploymentStrategySet(args: Array[String]): Unit = {
    val isStreamingLatestSet = args.contains(StreamingLatestSavepointArgKeyword)
    val isStreamingWithSavepointSet = JobSubmitter.getArgValue(args, StreamingCustomSavepointArgKeyword).isDefined
    val isStreamingNoSavepointSet = args.contains(StreamingNoSavepointArgKeyword)
    List(isStreamingLatestSet, isStreamingWithSavepointSet, isStreamingNoSavepointSet).count(_ == true) match {
      case 0 => throw new Exception("No savepoint deploy strategy provided")
      case 1 => // OK
      case _ =>
        throw new Exception("Multiple savepoint deploy strategies provided. " +
          s"Only one of $StreamingLatestSavepointArgKeyword, $StreamingCustomSavepointArgKeyword, $StreamingNoSavepointArgKeyword should be provided")
    }
  }

  /** Gets an existing Dataproc cluster or creates a new one if it doesn't exist.
    *
    * @param clusterName The name of the cluster to get or create
    * @param maybeClusterConfig Optional cluster configuration for creation
    * @param projectId The GCP project ID
    * @param region The GCP region
    * @param dataprocClient The Dataproc client for API calls
    * @return The name of the cluster
    * @throws IllegalArgumentException if parameters are invalid
    * @throws Exception if cluster operations fail
    */
  def getOrCreateCluster(clusterName: String,
                         maybeClusterConfig: Option[Map[String, String]],
                         projectId: String,
                         region: String,
                         dataprocClient: ClusterControllerClient): String = {
    require(projectId.nonEmpty, "projectId cannot be empty")
    require(region.nonEmpty, "region cannot be empty")
    require(dataprocClient != null, "dataprocClient cannot be null")
    if (clusterName != "") {
      try {
        val cluster = dataprocClient.getCluster(projectId, region, clusterName)
        if (
          cluster != null && Set(ClusterStatus.State.RUNNING,
                                 ClusterStatus.State.UPDATING,
                                 ClusterStatus.State.CREATING,
                                 ClusterStatus.State.STARTING,
                                 ClusterStatus.State.REPAIRING).contains(cluster.getStatus.getState)
        ) {
          val currentStatus = waitForClusterReadiness(clusterName, projectId, region, dataprocClient)

          System.err.println(s"Dataproc cluster $clusterName already exists and is in state ${currentStatus.getState}.")
          clusterName
        } else if (cluster != null) {
          if (maybeClusterConfig.isDefined && maybeClusterConfig.get.contains("dataproc.config")) {
            // Print to stderr so that it flushes immediately
            System.err.println(
              s"Dataproc cluster $clusterName is in bad state ${cluster.getStatus.getState}. " +
                s"Deleting and recreating it with the provided config.")
            recreateDataprocCluster(
              clusterName,
              projectId,
              region,
              dataprocClient,
              maybeClusterConfig.get.getOrElse("dataproc.config", "")
            )

          } else {
            throw new Exception(
              s"Dataproc cluster $clusterName is in bad state ${cluster.getStatus.getState}. " +
                s"Please provide a cluster config in teams.py to recreate it.")
          }
        } else if (maybeClusterConfig.isDefined && maybeClusterConfig.get.contains("dataproc.config")) {
          // Print to stderr so that it flushes immediately
          System.err.println(
            s"Dataproc cluster $clusterName does not exist or is not running. Creating it with the provided config.")
          createDataprocCluster(clusterName,
                                projectId,
                                region,
                                dataprocClient,
                                maybeClusterConfig.get.getOrElse("dataproc.config", ""))
        } else {
          throw new Exception(s"Dataproc cluster $clusterName does not exist and no cluster config provided.")
        }
      } catch {
        case _: ApiException if maybeClusterConfig.isDefined && maybeClusterConfig.get.contains("dataproc.config") =>
          // Print to stderr so that it flushes immediately
          System.err.println(s"Dataproc cluster $clusterName does not exist. Creating it with the provided config.")
          createDataprocCluster(clusterName,
                                projectId,
                                region,
                                dataprocClient,
                                maybeClusterConfig.get.getOrElse("dataproc.config", ""))
        case _: ApiException =>
          throw new Exception(s"Dataproc cluster $clusterName does not exist and no cluster config provided.")
      }
    } else if (maybeClusterConfig.isDefined && maybeClusterConfig.get.contains("dataproc.config")) {
      // Print to stderr so that it flushes immediately
      System.err.println(s"Creating a transient dataproc cluster based on config.")
      val transientClusterName = s"zipline-${java.util.UUID.randomUUID()}"
      createDataprocCluster(transientClusterName,
                            projectId,
                            region,
                            dataprocClient,
                            maybeClusterConfig.get.getOrElse("dataproc.config", ""))
    } else {
      throw new Exception(
        s"$SparkClusterNameEnvVar (or $GcpDataprocClusterNameEnvVar) is not set and no cluster config was provided. " +
          s"Please set $SparkClusterNameEnvVar or provide a cluster config in teams.py.")
    }
  }

  /** Creates a Dataproc cluster with the given name, project ID, region, and configuration.
    *
    * @param clusterName The name of the cluster to create.
    * @param projectId The GCP project ID.
    * @param region The region where the cluster will be created.
    * @param dataprocClient The ClusterControllerClient to interact with the Dataproc API.
    * @param clusterConfigStr The JSON string representing the cluster configuration.
    * @return The name of the created cluster.
    */
  private[cloud_gcp] def createDataprocCluster(clusterName: String,
                                               projectId: String,
                                               region: String,
                                               dataprocClient: ClusterControllerClient,
                                               clusterConfigStr: String): String = {

    val builder = ClusterConfig.newBuilder()
    val clusterConfig =
      try {
        JsonFormat.parser().merge(clusterConfigStr, builder)
        builder.build()
      } catch {
        case e: Exception =>
          throw new IllegalArgumentException(s"Failed to parse JSON: ${e.getMessage}", e)
      }

    val cluster: Cluster = Cluster
      .newBuilder()
      .setClusterName(clusterName)
      .setProjectId(projectId)
      .setConfig(clusterConfig)
      .build()

    val createRequest = CreateClusterRequest
      .newBuilder()
      .setProjectId(projectId)
      .setRegion(region)
      .setCluster(cluster)
      .build()

    // Asynchronously create the cluster and wait for it to be ready
    try {
      val operation = dataprocClient
        .createClusterAsync(createRequest)
        .get(30, java.util.concurrent.TimeUnit.MINUTES)
      if (operation == null) {
        throw new RuntimeException("Failed to create Dataproc cluster.")
      }
      System.err.println(s"Created Dataproc cluster: $clusterName")
    } catch {
      case e: java.util.concurrent.TimeoutException =>
        throw new RuntimeException(s"Timeout waiting for cluster creation: ${e.getMessage}", e)
      case e: Exception =>
        throw new RuntimeException(s"Error creating Dataproc cluster: ${e.getMessage}", e)
    }
    val currentStatus = waitForClusterReadiness(clusterName, projectId, region, dataprocClient)
    val currentState = currentStatus.getState
    currentState match {
      case ClusterStatus.State.RUNNING =>
        System.err.println(s"Dataproc cluster $clusterName is running.")
        clusterName
      case ClusterStatus.State.ERROR =>
        throw new RuntimeException(
          s"Failed to create Dataproc cluster $clusterName: ERROR state: ${currentStatus.toString}")
      case _ =>
        throw new RuntimeException(s"Dataproc cluster $clusterName is in unexpected state: $currentState.")
    }
  }

  /** Creates a Dataproc cluster with the given name, project ID, region, and configuration.
    *
    * @param clusterName The name of the cluster to create.
    * @param projectId The GCP project ID.
    * @param region The region where the cluster will be created.
    * @param dataprocClient The ClusterControllerClient to interact with the Dataproc API.
    * @param clusterConfigStr The JSON string representing the cluster configuration.
    * @return The name of the created cluster.
    */
  def recreateDataprocCluster(clusterName: String,
                              projectId: String,
                              region: String,
                              dataprocClient: ClusterControllerClient,
                              clusterConfigStr: String): String = {
    val deleteClusterRequest = DeleteClusterRequest
      .newBuilder()
      .setProjectId(projectId)
      .setRegion(region)
      .setClusterName(clusterName)
      .build()
    dataprocClient.deleteClusterAsync(deleteClusterRequest).get()

    System.err.println(s"Dataproc cluster $clusterName deleted successfully. Creating a new cluster.")

    createDataprocCluster(clusterName, projectId, region, dataprocClient, clusterConfigStr)

  }

  def waitForClusterReadiness(clusterName: String,
                              projectId: String,
                              region: String,
                              dataprocClient: ClusterControllerClient): ClusterStatus = {
    // Check status of the cluster creation
    var currentStatus = dataprocClient.getCluster(projectId, region, clusterName).getStatus
    var currentState = currentStatus.getState
    // Maximum time to wait for the cluster to become RUNNING (30 minutes)
    val maxWaitTime = 30 * 60 * 1000
    val startTime = System.currentTimeMillis()
    while (
      currentState != ClusterStatus.State.RUNNING &&
      currentState != ClusterStatus.State.UPDATING &&
      currentState != ClusterStatus.State.ERROR &&
      currentState != ClusterStatus.State.STOPPING
    ) {
      if (System.currentTimeMillis() - startTime > maxWaitTime) {
        throw new RuntimeException(
          s"Timeout waiting for cluster $clusterName to reach healthy state"
        )
      }
      System.err.println(
        s"Waiting for Dataproc cluster $clusterName to be in healthy state. Current state: $currentState")
      Thread.sleep(30000) // Wait for 30 seconds before checking again
      currentStatus = dataprocClient.getCluster(projectId, region, clusterName).getStatus
      currentState = currentStatus.getState
    }
    currentStatus

  }

  def main(args: Array[String]): Unit = {
    // Use generic CLUSTER_NAME env var first, fallback to GCP-specific one for backwards compatibility
    val clusterName = sys.env
      .get(SparkClusterNameEnvVar)
      .orElse(sys.env.get(GcpDataprocClusterNameEnvVar))
      .getOrElse("")
    val maybeClusterConfig = JobSubmitter.getClusterConfig(args)

    val projectId = sys.env.getOrElse(GcpProjectIdEnvVar, throw new Exception(s"$GcpProjectIdEnvVar not set"))
    val region = sys.env.getOrElse(GcpRegionEnvVar, throw new Exception(s"$GcpRegionEnvVar not set"))
    val dataprocClient = ClusterControllerClient.create(
      ClusterControllerSettings.newBuilder().setEndpoint(s"$region-dataproc.googleapis.com:443").build())

    val submitterClusterName = getOrCreateCluster(clusterName, maybeClusterConfig, projectId, region, dataprocClient)

    val endPoint = s"${region}-dataproc.googleapis.com:443"
    val jobControllerClient =
      JobControllerClient.create(JobControllerSettings.newBuilder().setEndpoint(endPoint).build())
    val gcsStorageClient = StorageOptions.newBuilder().setProjectId(projectId).build().getService
    val submitter = DataprocSubmitter(jobControllerClient, gcsStorageClient, region, projectId)

    val envMap = Map(
      GcpBigtableInstanceIdEnvVar -> sys.env.get(GcpBigtableInstanceIdEnvVar),
      GcpProjectIdEnvVar -> sys.env.get(GcpProjectIdEnvVar)
    )
    val jobId = submitter.run(args = args, envMap = envMap, clusterName = submitterClusterName)
  }

}
