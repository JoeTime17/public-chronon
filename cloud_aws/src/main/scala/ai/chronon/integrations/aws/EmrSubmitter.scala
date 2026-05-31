package ai.chronon.integrations.aws

import ai.chronon.api.JobStatusType
import ai.chronon.integrations.aws.EmrSubmitter.{
  DatabricksOAuthTokenVar,
  DefaultClusterIdleTimeout,
  DefaultClusterInstanceCount,
  DefaultClusterInstanceType
}
import ai.chronon.spark.submission.JobSubmitterConstants._
import ai.chronon.spark.submission.{
  JobSubmitter,
  JobType,
  StorageClient,
  FlinkJob => TypeFlinkJob,
  SparkJob => TypeSparkJob
}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import ai.chronon.integrations.cloud_k8s.{K8sFlinkStatusProvider, K8sFlinkSubmitter}
import io.fabric8.kubernetes.client.Config
import software.amazon.awssdk.core.exception.SdkException
import software.amazon.awssdk.services.ec2.Ec2Client
import software.amazon.awssdk.services.ec2.model.{DescribeSecurityGroupsRequest, DescribeSubnetsRequest, Filter}
import software.amazon.awssdk.services.emr.EmrClient
import software.amazon.awssdk.services.emr.model.{Unit => _, _}
import software.amazon.awssdk.services.s3.S3Client

import java.time.Instant
import java.util.concurrent.Executors
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

class EmrSubmitter(customerId: String,
                   emrClient: EmrClient,
                   ec2Client: Ec2Client,
                   eksFlinkSubmitter: Option[K8sFlinkSubmitter] = None,
                   s3Client: Option[S3Client] = None,
                   awsRegion: String = "",
                   override val dqMetricsDataset: String = "",
                   flinkEksServiceAccount: Option[String] = None,
                   flinkEksNamespace: Option[String] = None,
                   eksClusterName: Option[String] = None,
                   ingressBaseUrl: Option[String] = None,
                   flinkHealthCheckFn: Option[String] => Boolean = _ => true,
                   flinkInternalJobIdFetchFn: Option[String] => Option[String] = _ => None)
    extends JobSubmitter {

  private val ClusterApplications = List(
    "Flink",
    "Zeppelin",
    "JupyterEnterpriseGateway",
    "Hive",
    "Hadoop",
    "Livy",
    "Spark"
  )

  private val DefaultEmrReleaseLabel = "emr-7.2.0"

  /** Looks up a subnet ID from a subnet name tag.
    *
    * @param subnetName The value of the Name tag for the subnet
    * @return The subnet ID
    */
  private def lookupSubnetIdFromName(subnetName: String): String = {
    try {
      val request = DescribeSubnetsRequest
        .builder()
        .filters(
          Filter.builder().name("tag:Name").values(subnetName).build()
        )
        .build()

      val response = ec2Client.describeSubnets(request)
      val subnets = response.subnets().asScala

      if (subnets.isEmpty) {
        throw new RuntimeException(s"No subnet found with Name tag: $subnetName")
      }
      if (subnets.size > 1) {
        logger.warn(s"Multiple subnets found with Name tag: $subnetName. Using the first one.")
      }

      val subnetId = subnets.head.subnetId()
      logger.info(s"Resolved subnet name '$subnetName' to subnet ID: $subnetId")
      subnetId
    } catch {
      case e: Exception =>
        throw new RuntimeException(s"Error looking up subnet ID for name '$subnetName': ${e.getMessage}", e)
    }
  }

  /** Looks up a security group ID from a security group name.
    *
    * @param securityGroupName The name of the security group
    * @return The security group ID
    */
  private def lookupSecurityGroupIdFromName(securityGroupName: String): String = {
    try {
      val request = DescribeSecurityGroupsRequest
        .builder()
        .filters(
          Filter.builder().name("group-name").values(securityGroupName).build()
        )
        .build()

      val response = ec2Client.describeSecurityGroups(request)
      val securityGroups = response.securityGroups().asScala

      if (securityGroups.isEmpty) {
        throw new RuntimeException(s"No security group found with name: $securityGroupName")
      }
      if (securityGroups.size > 1) {
        logger.warn(s"Multiple security groups found with name: $securityGroupName. Using the first one.")
      }

      val securityGroupId = securityGroups.head.groupId()
      logger.info(s"Resolved security group name '$securityGroupName' to security group ID: $securityGroupId")
      securityGroupId
    } catch {
      case e: Exception =>
        throw new RuntimeException(s"Error looking up security group ID for name '$securityGroupName': ${e.getMessage}",
                                   e)
    }
  }

  private def createClusterRequestBuilder(emrReleaseLabel: String = DefaultEmrReleaseLabel,
                                          clusterIdleTimeout: Int = DefaultClusterIdleTimeout,
                                          masterInstanceType: String = DefaultClusterInstanceType,
                                          slaveInstanceType: String = DefaultClusterInstanceType,
                                          instanceCount: Int = DefaultClusterInstanceCount,
                                          clusterName: Option[String] = None,
                                          subnetId: Option[String] = None,
                                          securityGroupId: Option[String] = None) = {
    val runJobFlowRequestBuilder = if (clusterName.isDefined) {
      RunJobFlowRequest
        .builder()
        .name(clusterName.get)
    } else {
      RunJobFlowRequest
        .builder()
        .name(s"job-${java.util.UUID.randomUUID.toString}")
    }

    // Cluster infra configurations:
    val finalSubnetId =
      subnetId.getOrElse(throw new RuntimeException(s"Subnet ID must be provided in cluster configuration"))
    val finalSecurityGroupId = securityGroupId.getOrElse(
      throw new RuntimeException(s"Security group ID must be provided in cluster configuration"))

    runJobFlowRequestBuilder
      .autoTerminationPolicy(
        AutoTerminationPolicy
          .builder()
          .idleTimeout(clusterIdleTimeout.toLong)
          .build())
      .configurations(
        Configuration.builder
          .classification("spark-hive-site")
          .properties(Map(
            "hive.metastore.client.factory.class" -> "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory").asJava)
          .build()
      )
      .applications(ClusterApplications.map(app => Application.builder().name(app).build()): _*)
      // TODO: Could make this generalizable. or use a separate logs bucket
      .logUri(s"s3://zipline-logs-${customerId}/emr/")
      .instances(
        JobFlowInstancesConfig
          .builder()
          .ec2SubnetId(finalSubnetId)
          .emrManagedMasterSecurityGroup(finalSecurityGroupId)
          .emrManagedSlaveSecurityGroup(finalSecurityGroupId)
          .instanceGroups(
            InstanceGroupConfig
              .builder()
              .instanceRole(InstanceRoleType.MASTER)
              .instanceType(masterInstanceType)
              .instanceCount(1)
              .build(),
            InstanceGroupConfig
              .builder()
              .instanceRole(InstanceRoleType.CORE)
              .instanceType(slaveInstanceType)
              .instanceCount(1)
              .build()
          )
          .keepJobFlowAliveWhenNoSteps(true) // Keep the cluster alive after the job is done
          .build())
      .managedScalingPolicy(
        ManagedScalingPolicy
          .builder()
          .computeLimits(
            ComputeLimits
              .builder()
              .maximumCapacityUnits(instanceCount)
              .minimumCapacityUnits(1)
              .unitType(ComputeLimitsUnitType.INSTANCES)
              .build()
          )
          .build()
      )
      .serviceRole(s"zipline_${customerId}_emr_service_role")
      .jobFlowRole(s"zipline_${customerId}_emr_profile_role")
      .releaseLabel(emrReleaseLabel)

  }

  private def createStepConfig(filesToMount: List[String],
                               submissionProperties: Map[String, String],
                               jobProperties: Map[String, String],
                               args: String*): StepConfig = {
    // Copy files from s3 to cluster
    // TODO: see if we can use the spark.files or --files instead of doing this ourselves
    val awsS3CpArgs = filesToMount.map(file => s"aws s3 cp $file /mnt/zipline/")
    val tokenFetchScript = maybeBuildDatabricksTokenFetchScript(submissionProperties)

    // Escape single quotes for safe shell interpolation inside bash -c '...'
    // Values referencing the Databricks OAuth token use double quotes to allow shell variable expansion
    val confArgs = jobProperties
      .map { case (k, v) =>
        if (v.contains(DatabricksOAuthTokenVar)) {
          val escapedKey = k.replace("\"", "\\\"")
          val escapedValue = v.replace("\"", "\\\"")
          s"""--conf "$escapedKey=$escapedValue""""
        } else {
          val escapedKey = k.replace("'", "'\\''")
          val escapedValue = v.replace("'", "'\\''")
          s"--conf '$escapedKey=$escapedValue'"
        }
      }
      .mkString(" ")
    val mainClass = submissionProperties(MainClass)
    val jarUri = submissionProperties(JarURI)
    val sparkSubmitCmd =
      s"${tokenFetchScript.getOrElse("")}spark-submit $confArgs --class $mainClass $jarUri ${args.mkString(" ")}"

    val finalArgs = List("bash", "-c", (awsS3CpArgs ++ List(sparkSubmitCmd)).mkString("; \n"))
    // Don't log $finalArgs directly — it contains the fully-rendered spark-submit
    // command with --conf <secret>=<value> for every jobProperty. Log keys and
    // counts instead, matching the redaction policy on EmrServerlessSubmitter.
    logger.debug(
      s"Step config: confKeys=${jobProperties.keys.toSeq.sorted.mkString(",")}; " +
        s"filesToMount=${filesToMount.size}; argsCount=${args.size}")
    StepConfig
      .builder()
      .name("Run Zipline Job")
      .actionOnFailure(ActionOnFailure.CONTINUE)
      .hadoopJarStep(
        HadoopJarStepConfig
          .builder()
          // Using command-runner.jar from AWS:
          // https://docs.aws.amazon.com/en_us/emr/latest/ReleaseGuide/emr-spark-submit-step.html
          .jar("command-runner.jar")
          .args(finalArgs: _*)
          .build()
      )
      .build()
  }

  private def maybeBuildDatabricksTokenFetchScript(submissionProperties: Map[String, String]): Option[String] = {
    val databricksHost = submissionProperties.get("DATABRICKS_HOST")
    val databricksSecretName = submissionProperties.get("DATABRICKS_SECRET_NAME")
    (databricksHost, databricksSecretName) match {
      case (Some(host), Some(secretName)) => Some(buildDatabricksTokenFetchScript(host, secretName))
      case (Some(_), None) | (None, Some(_)) =>
        throw new IllegalArgumentException("Both DATABRICKS_HOST and DATABRICKS_SECRET_NAME must be set together")
      case _ => None
    }
  }

  private def buildDatabricksTokenFetchScript(host: String, secretName: String): String = {
    require(awsRegion.nonEmpty, "AWS_REGION must be set when using Databricks OAuth token fetch")
    val tokenUrl = s"${host.stripSuffix("/")}/oidc/v1/token"
    Seq(
      s"""SECRET_JSON=$$(aws secretsmanager get-secret-value --secret-id '$secretName' --query 'SecretString' --output text --region '$awsRegion')""",
      s"""DB_CLIENT_ID=$$(echo "$$SECRET_JSON" | jq -r '.client_id')""",
      s"""DB_CLIENT_SECRET=$$(echo "$$SECRET_JSON" | jq -r '.client_secret')""",
      s"""DATABRICKS_OAUTH_TOKEN=$$(curl --fail -s -X POST '$tokenUrl' -H 'Content-Type: application/x-www-form-urlencoded' -u "$$DB_CLIENT_ID:$$DB_CLIENT_SECRET" -d 'grant_type=client_credentials&scope=all-apis' | jq -r '.access_token')""",
      s"""{ [ -n "$$DATABRICKS_OAUTH_TOKEN" ] && [ "$$DATABRICKS_OAUTH_TOKEN" != "null" ]; } || { echo "Failed to fetch Databricks OAuth token" >&2; exit 1; }"""
    ).mkString(" && ") + "; "
  }

  /** Finds an EMR cluster by name, paginating through all results.
    * Only searches for clusters in active states (STARTING, BOOTSTRAPPING, RUNNING, WAITING).
    *
    * @param clusterName The name of the cluster to find
    * @return Option[ClusterSummary] if found, None otherwise
    */
  def findClusterByName(clusterName: String): Option[ClusterSummary] = {
    import scala.annotation.tailrec

    @tailrec
    def searchPages(marker: Option[String]): Option[ClusterSummary] = {
      val requestBuilder = ListClustersRequest
        .builder()
        .clusterStates(
          ClusterState.STARTING,
          ClusterState.BOOTSTRAPPING,
          ClusterState.RUNNING,
          ClusterState.WAITING
        )

      val request = marker match {
        case Some(m) => requestBuilder.marker(m).build()
        case None    => requestBuilder.build()
      }

      val response = emrClient.listClusters(request)

      response.clusters().asScala.find(_.name() == clusterName) match {
        case some @ Some(_) => some
        case None =>
          Option(response.marker()) match {
            case Some(nextMarker) if nextMarker.nonEmpty => searchPages(Some(nextMarker))
            case _                                       => None
          }
      }
    }

    logger.info(s"Searching for EMR cluster: $clusterName")
    searchPages(None)
  }

  /** Gets or creates an EMR cluster with the given configuration.
    * Similar to DataprocSubmitter.getOrCreateCluster.
    *
    * @param clusterName The name of the cluster
    * @param maybeClusterConfig Optional cluster configuration map
    * @return The cluster ID
    */
  def getOrCreateCluster(clusterName: String, maybeClusterConfig: Option[Map[String, String]]): String = {
    require(clusterName.nonEmpty, "clusterName cannot be empty")

    try {
      // Find cluster by name, paginating through all results (active states only)
      val matchingCluster: Option[ClusterSummary] = findClusterByName(clusterName)

      matchingCluster match {
        case Some(cluster) if Set(ClusterState.RUNNING, ClusterState.WAITING).contains(cluster.status().state()) =>
          logger.info(s"EMR cluster $clusterName already exists and is in state ${cluster.status().state()}.")
          cluster.id()

        case Some(cluster) =>
          logger.info(
            s"EMR cluster $clusterName exists but is in state ${cluster.status().state()}. Waiting for it to be ready.")
          waitForClusterReadiness(cluster.id(), clusterName)

        case None =>
          // Cluster doesn't exist (or is terminated), create it if config is provided
          if (maybeClusterConfig.isDefined && maybeClusterConfig.get.contains("emr.config")) {
            logger.info(s"EMR cluster $clusterName does not exist. Creating it with the provided config.")

            createEmrCluster(clusterName, maybeClusterConfig.get.getOrElse("emr.config", ""))
          } else {
            throw new Exception(s"EMR cluster $clusterName does not exist and no cluster config provided.")
          }
      }
    } catch {
      case e: EmrException =>
        if (maybeClusterConfig.isDefined && maybeClusterConfig.get.contains("emr.config")) {
          logger.info(s"Error checking for EMR cluster. Creating it with the provided config.")

          createEmrCluster(clusterName, maybeClusterConfig.get.getOrElse("emr.config", ""))
        } else {
          throw new Exception(s"EMR cluster $clusterName does not exist and no cluster config provided.", e)
        }
    }
  }

  /** Waits for an EMR cluster to reach a ready state (RUNNING or WAITING).
    * Fails fast if the cluster transitions to TERMINATED state.
    *
    * @param clusterId The ID of the cluster to wait for
    * @param clusterName The name of the cluster (for logging)
    * @return The cluster ID if successful
    */
  private def waitForClusterReadiness(clusterId: String, clusterName: String): String = {
    val maxWaitTimeMs = 600000 // 10 minutes
    val pollIntervalMs = 10000 // 10 seconds
    val startTime = System.currentTimeMillis()

    logger.info(s"Waiting for EMR cluster $clusterName (ID: $clusterId) to be ready...")

    while (System.currentTimeMillis() - startTime < maxWaitTimeMs) {
      val describeClusterRequest = DescribeClusterRequest.builder().clusterId(clusterId).build()
      val clusterDetails = emrClient.describeCluster(describeClusterRequest).cluster()
      val currentState = clusterDetails.status().state()

      currentState match {
        case ClusterState.RUNNING | ClusterState.WAITING =>
          logger.info(s"EMR cluster $clusterName is now ready in state: $currentState")
          return clusterId

        case ClusterState.TERMINATED | ClusterState.TERMINATING | ClusterState.TERMINATED_WITH_ERRORS =>
          val stateChangeReason = clusterDetails.status().stateChangeReason()
          val terminationMessage = if (stateChangeReason != null && stateChangeReason.message() != null) {
            stateChangeReason.message()
          } else {
            "No termination reason provided"
          }
          logger.error(s"EMR cluster $clusterName entered terminal state $currentState. Reason: $terminationMessage")
          throw new RuntimeException(
            s"EMR cluster $clusterName (ID: $clusterId) entered terminal state: $currentState. " +
              s"Termination reason: $terminationMessage. Cannot proceed with job submission."
          )

        case ClusterState.STARTING | ClusterState.BOOTSTRAPPING =>
          logger.info(s"EMR cluster $clusterName is in state $currentState, waiting...")
          Thread.sleep(pollIntervalMs)

        case _ =>
          logger.warn(s"EMR cluster $clusterName is in unexpected state: $currentState")
          Thread.sleep(pollIntervalMs)
      }
    }

    throw new RuntimeException(
      s"Timeout waiting for EMR cluster $clusterName (ID: $clusterId) to be ready after ${maxWaitTimeMs / 1000} seconds"
    )
  }

  /** Creates an EMR cluster with the given configuration.
    *
    * @param clusterName The name of the cluster to create
    * @param clusterConfigStr JSON string representing the cluster configuration
    * @return The cluster ID
    */
  private def createEmrCluster(clusterName: String, clusterConfigStr: String): String = {
    try {
      val mapper = new ObjectMapper()
      mapper.registerModule(DefaultScalaModule)
      val configNode = mapper.readTree(clusterConfigStr)

      // Parse the EMR configuration from JSON
      val emrReleaseLabel = Option(configNode.get("releaseLabel"))
        .map(_.asText())
        .getOrElse(DefaultEmrReleaseLabel)

      val idleTimeout = Option(configNode.get("autoTerminationPolicy"))
        .flatMap(atp => Option(atp.get("idleTimeout")))
        .map(_.asInt())
        .getOrElse(DefaultClusterIdleTimeout)

      val instanceType = Option(configNode.get("instanceType"))
        .map(_.asText())
        .getOrElse(DefaultClusterInstanceType)

      val instanceCount = Option(configNode.get("instanceCount"))
        .map(_.asInt())
        .getOrElse(DefaultClusterInstanceCount)

      // Handle subnet - look up ID from name if provided
      val subnetId = Option(configNode.get("subnetName"))
        .map(_.asText())
        .map(lookupSubnetIdFromName)
        .orElse(Option(configNode.get("subnetId")).map(_.asText()))

      // Handle security group - look up ID from name if provided
      val securityGroupId = Option(configNode.get("securityGroupName"))
        .map(_.asText())
        .map(lookupSecurityGroupIdFromName)
        .orElse(Option(configNode.get("securityGroupId")).map(_.asText()))

      // Create the cluster using the existing builder
      val runJobFlowBuilder = createClusterRequestBuilder(
        emrReleaseLabel = emrReleaseLabel,
        clusterIdleTimeout = idleTimeout,
        masterInstanceType = instanceType,
        slaveInstanceType = instanceType,
        instanceCount = instanceCount,
        clusterName = Some(clusterName),
        subnetId = subnetId,
        securityGroupId = securityGroupId
      )

      val response = emrClient.runJobFlow(runJobFlowBuilder.build())
      val clusterId = response.jobFlowId()

      logger.info(s"Created EMR cluster: $clusterName with ID: $clusterId")
      clusterId

    } catch {
      case e: Exception =>
        throw new RuntimeException(s"Error creating EMR cluster $clusterName: ${e.getMessage}", e)
    }
  }

  override def submit(jobType: JobType,
                      submissionProperties: Map[String, String],
                      jobProperties: Map[String, String],
                      files: List[String],
                      labels: Map[String, String],
                      envVars: Map[String, String],
                      args: String*): String = {
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
        // Encode namespace into the job ID so status/kill can target the right namespace
        s"flink:$namespace:$deploymentName"

      case TypeSparkJob =>
        val existingJobId = submissionProperties.getOrElse(ClusterId, throw new RuntimeException("JobFlowId not found"))
        val sparkJobProperties = jobProperties ++ envVarsToSparkProperties(envVars)
        val stepConfig = createStepConfig(files, submissionProperties, sparkJobProperties, userArgs: _*)

        val request = AddJobFlowStepsRequest
          .builder()
          .jobFlowId(existingJobId)
          .steps(stepConfig)
          .build()

        val responseStepId = emrClient.addJobFlowSteps(request).stepIds().get(0)

        logger.info(s"EMR step id: $responseStepId")
        logger.info(
          s"Safe to exit. Follow the job status at: https://$awsRegion.console.aws.amazon.com/emr/home?region=$awsRegion#/clusterDetails/$existingJobId")
        // Return composite ID so status/kill/getJobUrl can resolve both cluster and step
        s"$existingJobId:$responseStepId"
    }
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
      val (clusterId, stepId) = if (jobId.contains(":")) {
        val parts = jobId.split(":")
        (parts(0), parts(1))
      } else {
        throw new IllegalArgumentException(s"Job ID must be in format 'clusterId:stepId', got: $jobId")
      }

      try {
        val state = emrClient
          .describeStep(
            DescribeStepRequest
              .builder()
              .clusterId(clusterId)
              .stepId(stepId)
              .build()
          )
          .step()
          .status()
          .state()

        state match {
          case StepState.PENDING        => JobStatusType.PENDING
          case StepState.CANCEL_PENDING => JobStatusType.PENDING
          case StepState.RUNNING        => JobStatusType.RUNNING
          case StepState.COMPLETED      => JobStatusType.SUCCEEDED
          case StepState.CANCELLED      => JobStatusType.FAILED
          case StepState.FAILED         => JobStatusType.FAILED
          case StepState.INTERRUPTED    => JobStatusType.FAILED
          case _                        => JobStatusType.UNKNOWN
        }
      } catch {
        case e: SdkException =>
          logger.error(s"Failed to get status for job $jobId: ${e.getMessage}", e)
          JobStatusType.UNKNOWN
      }
    }
  }

  override def kill(jobId: String): scala.Unit = {
    if (jobId.startsWith("flink:")) {
      val parts = jobId.split(":", 3)
      eksFlinkSubmitter
        .getOrElse(throw new RuntimeException("K8sFlinkSubmitter is required for Flink jobs"))
        .delete(deploymentName = parts(2), namespace = parts(1))
    } else {
      val parts = jobId.split(":")
      require(parts.length == 2, s"Expected jobId in 'clusterId:stepId' format, got: $jobId")
      emrClient.cancelSteps(CancelStepsRequest.builder().clusterId(parts(0)).stepIds(parts(1)).build())
    }
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

  override def jarName: String = "cloud_aws_lib_deploy.jar"
  override def onlineClass: String = "ai.chronon.integrations.aws.AwsApiImpl"

  override def resolveConfPath(stagedFileUri: String): String =
    s"/mnt/zipline/${stagedFileUri.split("/").last}"

  override def getJobUrl(jobId: String): Option[String] = {
    if (jobId.startsWith("flink:")) {
      // flink:<namespace>:<deploymentName>
      val parts = jobId.split(":")
      if (parts.length == 3) {
        val namespace = parts(1)
        val deploymentName = parts(2)
        eksClusterName.map { clusterName =>
          s"https://$awsRegion.console.aws.amazon.com/eks/clusters/$clusterName/deployments/$deploymentName?namespace=$namespace&region=$awsRegion"
        }
      } else None
    } else if (jobId.contains(":")) {
      val parts = jobId.split(":")
      Some(s"https://$awsRegion.console.aws.amazon.com/emr/home?region=$awsRegion#/clusterDetails/${parts(0)}")
    } else None
  }

  // Base SHS URL only — EMR DescribeStep API doesn't provide yarnApplicationId for deep linking
  override def getSparkUrl(jobId: String): Option[String] = {
    if (jobId.startsWith("flink:")) {
      None
    } else if (jobId.contains(":")) {
      val clusterId = jobId.split(":")(0)
      val clusterIdLower = clusterId.stripPrefix("j-").toLowerCase
      Some(s"https://p-$clusterIdLower-shs.emrappui-prod.$awsRegion.amazonaws.com/shs/")
    } else None
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

  override def deprecatedClusterNameEnvVars: Seq[String] = Seq(EmrClusterNameEnvVar)

  override def clusterIdentifierKey: String = ClusterId

  // We submit long-running jobs to EKS, others to EMR, so we only need to create clusters for batch jobs (finite duration)
  override def isClusterCreateNeeded(isLongRunning: Boolean): Boolean = {
    !isLongRunning
  }

  override def ensureClusterReady(clusterName: String, clusterConf: Option[Map[String, String]])(implicit
      ec: ExecutionContext): Option[String] = {
    Try(findClusterByName(clusterName)) match {
      case Success(Some(cluster))
          if Set(ClusterState.RUNNING, ClusterState.WAITING).contains(cluster.status().state()) =>
        Some(cluster.id())
      case Success(Some(_)) =>
        logger.info(s"EMR cluster $clusterName exists but is not ready.")
        None
      case Success(None) =>
        if (clusterConf.isDefined && clusterConf.get.contains("emr.config")) {
          logger.info(s"EMR cluster $clusterName not found. Triggering creation asynchronously.")
          Future {
            getOrCreateCluster(clusterName, clusterConf)
          }.recover { case ex: Exception =>
            logger.error(s"Failed to create EMR cluster $clusterName asynchronously", ex)
          }
        }
        None
      case Failure(ex) =>
        throw new RuntimeException(s"Failed to look up EMR cluster $clusterName", ex)
    }
  }

  override def close(): Unit = {
    try {
      emrClient.close()
      ec2Client.close()
      s3Client.foreach(_.close())
    } catch {
      case _: Exception => logger.info("Error shutting down AWS clients.")
    }
  }

  override def kvStoreApiProperties: Map[String, String] = Map(
    "AWS_DEFAULT_REGION" -> awsRegion
  )
}

object EmrSubmitter {
  private val DatabricksOAuthTokenVar = "$DATABRICKS_OAUTH_TOKEN"

  def apply(k8sConfig: Option[Config] = None,
            flinkHealthCheckFn: Option[String] => Boolean = _ => true,
            flinkInternalJobIdFetchFn: Option[String] => Option[String] = _ => None): EmrSubmitter = {
    val customerId = sys.env.getOrElse("CUSTOMER_ID", throw new Exception("CUSTOMER_ID not set")).toLowerCase
    val awsRegion = sys.env.getOrElse("AWS_REGION", sys.env.getOrElse("AWS_DEFAULT_REGION", ""))
    val ingressBaseUrl = sys.env.get("HUB_BASE_URL")

    new EmrSubmitter(
      customerId,
      EmrClient.builder().build(),
      Ec2Client.builder().build(),
      eksFlinkSubmitter = Some(EksFlinkSubmitter(k8sConfig, ingressBaseUrl = ingressBaseUrl)),
      awsRegion = awsRegion,
      flinkEksServiceAccount = sys.env.get("FLINK_EKS_SERVICE_ACCOUNT"),
      flinkEksNamespace = sys.env.get("FLINK_EKS_NAMESPACE"),
      eksClusterName = sys.env.get("EKS_CLUSTER_NAME"),
      ingressBaseUrl = ingressBaseUrl,
      flinkHealthCheckFn = flinkHealthCheckFn,
      flinkInternalJobIdFetchFn = flinkInternalJobIdFetchFn
    )
  }

  private val ClusterInstanceTypeArgKeyword = "--cluster-instance-type"
  private val ClusterInstanceCountArgKeyword = "--cluster-instance-count"
  private val ClusterIdleTimeoutArgKeyword = "--cluster-idle-timeout"
  private val DefaultClusterInstanceType = "m5.xlarge"
  private val DefaultClusterInstanceCount = 3
  private val DefaultClusterIdleTimeout = 60 * 60 * 1 // 1h in seconds

  private[aws] def createSubmissionPropsMap(jobType: JobType,
                                            args: Array[String],
                                            clusterId: String): Map[String, String] = {
    val jarUri = JobSubmitter
      .getArgValue(args, JarUriArgKeyword)
      .getOrElse(throw new Exception("Missing required argument: " + JarUriArgKeyword))
    val mainClass = JobSubmitter
      .getArgValue(args, MainClassKeyword)
      .getOrElse(throw new Exception("Missing required argument: " + MainClassKeyword))

    val clusterInstanceType = JobSubmitter
      .getArgValue(args, ClusterInstanceTypeArgKeyword)
      .getOrElse(DefaultClusterInstanceType)
    val clusterInstanceCount = JobSubmitter
      .getArgValue(args, ClusterInstanceCountArgKeyword)
      .getOrElse(DefaultClusterInstanceCount.toString)
    val clusterIdleTimeout = JobSubmitter
      .getArgValue(args, ClusterIdleTimeoutArgKeyword)
      .getOrElse(DefaultClusterIdleTimeout.toString)

    jobType match {
      case TypeSparkJob =>
        Map(
          MainClass -> mainClass,
          JarURI -> jarUri,
          ClusterInstanceType -> clusterInstanceType,
          ClusterInstanceCount -> clusterInstanceCount,
          ClusterIdleTimeout -> clusterIdleTimeout,
          ClusterId -> clusterId
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
            System.err.println("Latest savepoint not yet supported for EMR, proceeding without savepoint")
            None
          } else if (args.contains(StreamingNoSavepointArgKeyword)) {
            None
          } else {
            None
          }

        if (maybeSavepointUri.isEmpty) {
          baseJobProps
        } else {
          val savepointUri = maybeSavepointUri.get
          System.err.println(s"Deploying Flink app with savepoint uri $savepointUri")
          baseJobProps + (SavepointUri -> savepointUri)
        }
    }
  }

  def main(args: Array[String]): scala.Unit = {
    // List of args that are not application args
    val internalArgs = Set(
      ClusterInstanceTypeArgKeyword,
      ClusterInstanceCountArgKeyword,
      ClusterIdleTimeoutArgKeyword
    ) ++ SharedInternalArgs

    val userArgs = args.filter(arg => !internalArgs.exists(arg.startsWith))

    val jarUri = JobSubmitter
      .getArgValue(args, JarUriArgKeyword)
      .getOrElse(throw new Exception("Missing required argument: " + JarUriArgKeyword))
    val mainClass = JobSubmitter
      .getArgValue(args, MainClassKeyword)
      .getOrElse(throw new Exception("Missing required argument: " + MainClassKeyword))
    val jobTypeValue =
      JobSubmitter
        .getArgValue(args, JobTypeArgKeyword)
        .getOrElse(throw new Exception("Missing required argument: " + JobTypeArgKeyword))

    val clusterInstanceType = JobSubmitter
      .getArgValue(args, ClusterInstanceTypeArgKeyword)
      .getOrElse(DefaultClusterInstanceType)
    val clusterInstanceCount = JobSubmitter
      .getArgValue(args, ClusterInstanceCountArgKeyword)
      .getOrElse(DefaultClusterInstanceCount.toString)
    val clusterIdleTimeout = JobSubmitter
      .getArgValue(args, ClusterIdleTimeoutArgKeyword)
      .getOrElse(DefaultClusterIdleTimeout.toString)

    // Use generic CLUSTER_NAME env var first, fallback to EMR_CLUSTER_NAME for backwards compatibility
    val clusterName = sys.env
      .get(SparkClusterNameEnvVar)
      .orElse(sys.env.get(EmrClusterNameEnvVar))
      .getOrElse(throw new Exception(s"$SparkClusterNameEnvVar (or $EmrClusterNameEnvVar) is not set. " +
        s"Please set $SparkClusterNameEnvVar or provide a cluster config in teams.py."))

    // search args array for prefix `--files`
    val filesArgs = args.filter(_.startsWith(FilesArgKeyword))
    assert(filesArgs.length == 0 || filesArgs.length == 1)

    val files = if (filesArgs.isEmpty) {
      Array.empty[String]
    } else {
      filesArgs(0).split("=")(1).split(",")
    }

    val flinkStatusProvider = new K8sFlinkStatusProvider()
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(Executors.newCachedThreadPool())
    val emrSubmitter = EmrSubmitter(
      flinkHealthCheckFn = uri => Await.result(flinkStatusProvider.isFlinkJobHealthy(uri), 30.seconds),
      flinkInternalJobIdFetchFn = uri => Await.result(flinkStatusProvider.getFlinkInternalJobId(uri), 30.seconds)
    )

    val jobType = jobTypeValue.toLowerCase match {
      case "spark" => TypeSparkJob
      case "flink" => TypeFlinkJob
      case _       => throw new Exception("Invalid job type")
    }

    val submissionProps = jobType match {
      case TypeFlinkJob =>
        createSubmissionPropsMap(jobType, args, "")
      case TypeSparkJob =>
        val maybeClusterConfig = JobSubmitter.getClusterConfig(args)
        val clusterId = emrSubmitter.getOrCreateCluster(clusterName, maybeClusterConfig)
        createSubmissionPropsMap(jobType, args, clusterId)
    }

    val finalArgs = userArgs.toSeq
    val modeConfigProperties = JobSubmitter.getModeConfigProperties(args)

    emrSubmitter.submit(
      jobType = jobType,
      submissionProperties = submissionProps,
      jobProperties = modeConfigProperties.getOrElse(Map.empty),
      files = files.toList,
      labels = Map.empty,
      envVars = Map.empty,
      args = finalArgs: _*
    )
  }
}
