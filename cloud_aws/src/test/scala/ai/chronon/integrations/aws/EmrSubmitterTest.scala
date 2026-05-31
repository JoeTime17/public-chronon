package ai.chronon.integrations.aws

import ai.chronon.api.JobStatusType
import ai.chronon.integrations.cloud_k8s.K8sFlinkSubmitter
import ai.chronon.api.ScalaJavaConversions.ListOps
import ai.chronon.spark.submission.JobSubmitterConstants._
import ai.chronon.spark.submission.{FlinkJob, SparkJob, StorageClient}
import io.fabric8.kubernetes.client.Config
import org.junit.Assert.assertEquals
import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mockito.{verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import software.amazon.awssdk.services.ec2.Ec2Client
import software.amazon.awssdk.services.emr.EmrClient
import software.amazon.awssdk.services.emr.model.{AddJobFlowStepsRequest, AddJobFlowStepsResponse}

class EmrSubmitterTest extends AnyFlatSpec with Matchers with MockitoSugar {
  "EmrSubmitterClient" should "return job id when a job is submitted and assert EMR request args" in {
    val stepId = "mock-step-id"
    val clusterId = "j-MOCKCLUSTERID123"

    val mockEmrClient = mock[EmrClient]
    val mockEc2Client = mock[Ec2Client]
    val mockEksSubmitter = mock[K8sFlinkSubmitter]

    val requestCaptor = org.mockito.ArgumentCaptor.forClass(classOf[AddJobFlowStepsRequest])

    when(
      mockEmrClient.addJobFlowSteps(
        requestCaptor.capture()
      )).thenReturn(AddJobFlowStepsResponse.builder().stepIds(stepId).build())

    val expectedCustomerId = "canary"
    val expectedApplicationArgs = Seq("group-by-backfill", "arg1", "arg2")
    val expectedFiles = List("s3://random-conf", "s3://random-data")
    val expectedMainClass = "some-main-class"
    val expectedJarURI = "s3://-random-jar-uri"

    val expectedJobProperties = Map("spark.executor.memory" -> "4g", "spark.executor.cores" -> "2")

    val submitter = new EmrSubmitter(expectedCustomerId, mockEmrClient, mockEc2Client, Some(mockEksSubmitter))
    val submittedStepId = submitter.submit(
      jobType = SparkJob,
      submissionProperties = Map(
        MainClass -> expectedMainClass,
        JarURI -> expectedJarURI,
        ClusterId -> clusterId
      ),
      jobProperties = expectedJobProperties,
      files = expectedFiles,
      labels = Map.empty,
      envVars = Map.empty,
      args = expectedApplicationArgs: _*
    )
    assertEquals(submittedStepId, s"$clusterId:$stepId")

    val actualRequest = requestCaptor.getValue

    // Verify the cluster ID is correct
    assertEquals(actualRequest.jobFlowId(), clusterId)

    // Verify step configuration
    assertEquals(actualRequest.steps().size(), 1)

    val stepConfig = actualRequest.steps().get(0)
    assertEquals(stepConfig.actionOnFailure().name(), "CONTINUE")
    assertEquals(stepConfig.name(), "Run Zipline Job")
    assertEquals(stepConfig.hadoopJarStep().jar(), "command-runner.jar")

    val actualArgs = stepConfig.hadoopJarStep().args().toScala.mkString(" ")
    // Verify file copy commands are present
    assert(actualArgs.contains("aws s3 cp s3://random-conf /mnt/zipline/"))
    assert(actualArgs.contains("aws s3 cp s3://random-data /mnt/zipline/"))
    // Verify spark-submit with --conf args for job properties
    assert(actualArgs.contains("--conf 'spark.executor.memory=4g'"))
    assert(actualArgs.contains("--conf 'spark.executor.cores=2'"))
    // Verify spark-submit class, jar, and application args
    assert(actualArgs.contains(s"--class $expectedMainClass"))
    assert(actualArgs.contains(expectedJarURI))
    assert(actualArgs.contains(expectedApplicationArgs.mkString(" ")))
  }

  it should "test createSubmissionPropsMap for Spark job on EMR on EC2" in {
    val clusterId = "j-SPARKCLUSTER123"
    val mainClass = "ai.chronon.spark.Driver"
    val jarURI = "s3://zipline-jars/cloud-aws.jar"

    val actual = EmrSubmitter.createSubmissionPropsMap(
      jobType = SparkJob,
      args = Array(
        s"$JarUriArgKeyword=$jarURI",
        s"$MainClassKeyword=$mainClass"
      ),
      clusterId = clusterId
    )

    assertEquals(actual(MainClass), mainClass)
    assertEquals(actual(JarURI), jarURI)
    assertEquals(actual(ClusterId), clusterId)
    assertEquals(actual(ClusterInstanceType), "m5.xlarge") // default value
    assert(actual.contains(ClusterInstanceCount))
    assert(actual.contains(ClusterIdleTimeout))
  }

  it should "test createSubmissionPropsMap for Flink job" in {
    val jobId = "test-job-id-123"
    val mainClass = "ai.chronon.flink.FlinkJob"
    val jarURI = "s3://zipline-jars/cloud-aws.jar"
    val flinkMainJarURI = "s3://zipline-jars/flink-assembly.jar"
    val flinkCheckpointUri = "s3://zipline-warehouse/flink-checkpoints"
    val kinesisConnectorJarURI = "s3://zipline-jars/kinesis-connector.jar"
    val flinkJarsUri = "s3://zipline-artifacts/flink-jars/"
    val customSavepoint = "s3://zipline-warehouse/flink-checkpoints/chk-100"
    val eksServiceAccount = "zipline-flink-sa"
    val eksNamespace = "zipline-flink"

    val actual = EmrSubmitter.createSubmissionPropsMap(
      jobType = FlinkJob,
      args = Array(
        s"$JobIdArgKeyword=$jobId",
        s"$JarUriArgKeyword=$jarURI",
        s"$MainClassKeyword=$mainClass",
        s"$FlinkMainJarUriArgKeyword=$flinkMainJarURI",
        s"$FlinkKinesisJarUriArgKeyword=$kinesisConnectorJarURI",
        s"$FlinkJarsUriArgKeyword=$flinkJarsUri",
        s"$StreamingCheckpointPathArgKeyword=$flinkCheckpointUri",
        s"$StreamingCustomSavepointArgKeyword=$customSavepoint",
        s"$EksServiceAccountArgKeyword=$eksServiceAccount",
        s"$EksNamespaceArgKeyword=$eksNamespace"
      ),
      clusterId = ""
    )

    assertEquals(actual(JobId), jobId)
    assertEquals(actual(MainClass), mainClass)
    assertEquals(actual(JarURI), jarURI)
    assertEquals(actual(FlinkMainJarURI), flinkMainJarURI)
    assertEquals(actual(FlinkKinesisConnectorJarURI), kinesisConnectorJarURI)
    assertEquals(actual(FlinkJarsUri), flinkJarsUri)
    assertEquals(actual(FlinkCheckpointUri), flinkCheckpointUri)
    assertEquals(actual(SavepointUri), customSavepoint)
    assertEquals(actual(EksServiceAccount), eksServiceAccount)
    assertEquals(actual(EksNamespace), eksNamespace)
    assert(!actual.contains(ClusterId))
  }

  // --- buildFlinkSubmissionProps ---

  private val testArtifactPrefix = "s3://zipline-artifacts-test"
  private val testVersion = "1.0.0"
  private val baseFlinkEnv = Map(
    "FLINK_STATE_URI" -> "s3://test-bucket/flink-state",
    "FLINK_EKS_SERVICE_ACCOUNT" -> "zipline-flink-sa",
    "FLINK_EKS_NAMESPACE" -> "zipline-flink"
  )
  private val kinesisConnectorJarUri =
    s"$testArtifactPrefix/release/$testVersion/jars/connectors_kinesis_deploy.jar"

  private def createTestSubmitter(): EmrSubmitter =
    new EmrSubmitter("test-customer", mock[EmrClient], mock[Ec2Client], Some(mock[K8sFlinkSubmitter]), awsRegion = "us-west-2")

  "buildFlinkSubmissionProps" should "include flink jar URI, checkpoint URI, EKS account and namespace" in {
    val submitter = createTestSubmitter()
    val props = submitter.buildFlinkSubmissionProps(baseFlinkEnv, testVersion, testArtifactPrefix)

    props(FlinkMainJarURI) shouldBe s"$testArtifactPrefix/release/$testVersion/jars/flink_assembly_deploy.jar"
    props(FlinkCheckpointUri) shouldBe "s3://test-bucket/flink-state/checkpoints"
    props(EksServiceAccount) shouldBe "zipline-flink-sa"
    props(EksNamespace) shouldBe "zipline-flink"
  }

  it should "include kinesis connector jar when ENABLE_KINESIS is true" in {
    val submitter = createTestSubmitter()
    val env = baseFlinkEnv + ("ENABLE_KINESIS" -> "true")

    val props = submitter.buildFlinkSubmissionProps(env, testVersion, testArtifactPrefix)

    props(FlinkKinesisConnectorJarURI) shouldBe kinesisConnectorJarUri
  }

  it should "omit kinesis connector jar when ENABLE_KINESIS is false or absent" in {
    val submitter = createTestSubmitter()

    val props = submitter.buildFlinkSubmissionProps(baseFlinkEnv, testVersion, testArtifactPrefix)

    props.contains(FlinkKinesisConnectorJarURI) shouldBe false
  }

  it should "throw exception when FLINK_STATE_URI is not set" in {
    val submitter = createTestSubmitter()
    val env = baseFlinkEnv - "FLINK_STATE_URI"

    intercept[IllegalArgumentException] {
      submitter.buildFlinkSubmissionProps(env, testVersion, testArtifactPrefix)
    }
  }

  it should "throw exception when FLINK_EKS_SERVICE_ACCOUNT is not set" in {
    val submitter = createTestSubmitter()
    val env = baseFlinkEnv - "FLINK_EKS_SERVICE_ACCOUNT"

    intercept[IllegalArgumentException] {
      submitter.buildFlinkSubmissionProps(env, testVersion, testArtifactPrefix)
    }
  }

  it should "throw exception when FLINK_EKS_NAMESPACE is not set" in {
    val submitter = createTestSubmitter()
    val env = baseFlinkEnv - "FLINK_EKS_NAMESPACE"

    intercept[IllegalArgumentException] {
      submitter.buildFlinkSubmissionProps(env, testVersion, testArtifactPrefix)
    }
  }

  // --- status / kill routing ---

  "status" should "return RUNNING for flink job when EKS is RUNNING and health check passes" in {
    val mockEks = mock[K8sFlinkSubmitter]
    when(mockEks.statusWithCreationTime("my-deployment", "zipline-flink"))
      .thenReturn((JobStatusType.RUNNING, Some(java.time.Instant.now())))

    val submitter = new EmrSubmitter("test-customer", mock[EmrClient], mock[Ec2Client], Some(mockEks),
      flinkHealthCheckFn = _ => true)
    val result = submitter.status("flink:zipline-flink:my-deployment")

    result shouldBe JobStatusType.RUNNING
    verify(mockEks).statusWithCreationTime("my-deployment", "zipline-flink")
  }

  it should "return PENDING for flink job when EKS is RUNNING, health check fails, within grace period" in {
    val mockEks = mock[K8sFlinkSubmitter]
    // CRD created just now — within the 20-min grace window
    when(mockEks.statusWithCreationTime("my-deployment", "zipline-flink"))
      .thenReturn((JobStatusType.RUNNING, Some(java.time.Instant.now())))

    val submitter = new EmrSubmitter("test-customer", mock[EmrClient], mock[Ec2Client], Some(mockEks),
      flinkHealthCheckFn = _ => false)
    val result = submitter.status("flink:zipline-flink:my-deployment")

    result shouldBe JobStatusType.PENDING
  }

  it should "return FAILED for flink job when EKS is RUNNING, health check fails, past grace period" in {
    val mockEks = mock[K8sFlinkSubmitter]
    val oldCreationTime = java.time.Instant.now().minus(java.time.Duration.ofMinutes(25))
    when(mockEks.statusWithCreationTime("my-deployment", "zipline-flink"))
      .thenReturn((JobStatusType.RUNNING, Some(oldCreationTime)))

    val submitter = new EmrSubmitter("test-customer", mock[EmrClient], mock[Ec2Client], Some(mockEks),
      flinkHealthCheckFn = _ => false)
    val result = submitter.status("flink:zipline-flink:my-deployment")

    result shouldBe JobStatusType.FAILED
  }

  it should "return PENDING when health check fails and no creation time is available" in {
    val mockEks = mock[K8sFlinkSubmitter]
    // No creation time — conservatively stay PENDING
    when(mockEks.statusWithCreationTime("my-deployment", "zipline-flink"))
      .thenReturn((JobStatusType.RUNNING, None))

    val submitter = new EmrSubmitter("test-customer", mock[EmrClient], mock[Ec2Client], Some(mockEks),
      flinkHealthCheckFn = _ => false)
    val result = submitter.status("flink:zipline-flink:my-deployment")

    result shouldBe JobStatusType.PENDING
  }

  it should "pass the flink URL derived from ingressBaseUrl to the health check fn" in {
    val mockEks = mock[K8sFlinkSubmitter]
    when(mockEks.statusWithCreationTime("my-deployment", "zipline-flink"))
      .thenReturn((JobStatusType.RUNNING, Some(java.time.Instant.now())))

    var capturedUrl: Option[String] = None
    val submitter = new EmrSubmitter("test-customer", mock[EmrClient], mock[Ec2Client], Some(mockEks),
      ingressBaseUrl = Some("https://hub.example.com"),
      flinkHealthCheckFn = url => { capturedUrl = url; true })
    submitter.status("flink:zipline-flink:my-deployment")

    capturedUrl shouldBe Some("https://hub.example.com/flink/my-deployment/")
  }

  it should "propagate non-RUNNING EKS status without invoking health check" in {
    val mockEks = mock[K8sFlinkSubmitter]
    when(mockEks.statusWithCreationTime("my-deployment", "zipline-flink"))
      .thenReturn((JobStatusType.PENDING, None))

    var healthCheckCalled = false
    val submitter = new EmrSubmitter("test-customer", mock[EmrClient], mock[Ec2Client], Some(mockEks),
      flinkHealthCheckFn = _ => { healthCheckCalled = true; true })
    val result = submitter.status("flink:zipline-flink:my-deployment")

    result shouldBe JobStatusType.PENDING
    healthCheckCalled shouldBe false
  }

  "kill" should "delegate to EksFlinkSubmitter for flink: prefixed job IDs" in {
    val mockEks = mock[K8sFlinkSubmitter]
    val submitter = new EmrSubmitter("test-customer", mock[EmrClient], mock[Ec2Client], Some(mockEks))
    submitter.kill("flink:zipline-flink:my-deployment")

    verify(mockEks).delete("my-deployment", "zipline-flink")
  }

  "submit" should "return flink:namespace:deploymentName for FlinkJob" in {
    val mockEks = mock[K8sFlinkSubmitter]
    when(
      mockEks.submit(
        jobId = org.mockito.ArgumentMatchers.anyString(),
        mainClass = org.mockito.ArgumentMatchers.anyString(),
        mainJarUri = org.mockito.ArgumentMatchers.anyString(),
        jarUris = org.mockito.ArgumentMatchers.any(),
        flinkCheckpointUri = org.mockito.ArgumentMatchers.anyString(),
        maybeSavepointUri = org.mockito.ArgumentMatchers.any(),
        maybeFlinkJarsUri = org.mockito.ArgumentMatchers.any(),
        jobProperties = org.mockito.ArgumentMatchers.any(),
        args = org.mockito.ArgumentMatchers.any(),
        serviceAccount = org.mockito.ArgumentMatchers.anyString(),
        namespace = org.mockito.ArgumentMatchers.anyString(),
        envVars = org.mockito.ArgumentMatchers.any(),
        nodeSelector = org.mockito.ArgumentMatchers.any()
      )
    ).thenReturn("flink-abc123")

    val submitter = new EmrSubmitter("test-customer", mock[EmrClient], mock[Ec2Client], Some(mockEks))
    val jobId = submitter.submit(
      jobType = FlinkJob,
      submissionProperties = Map(
        JobId -> "test-job-id",
        MainClass -> "ai.chronon.flink.FlinkJob",
        JarURI -> "s3://bucket/cloud_aws_lib_deploy.jar",
        FlinkMainJarURI -> "s3://bucket/flink_assembly_deploy.jar",
        FlinkCheckpointUri -> "s3://bucket/checkpoints",
        EksServiceAccount -> "zipline-flink-sa",
        EksNamespace -> "zipline-flink"
      ),
      jobProperties = Map.empty,
      files = List.empty,
      labels = Map.empty,
      envVars = Map.empty
    )

    jobId shouldBe "flink:zipline-flink:flink-abc123"
  }

  it should "pass nodeSelector to K8sFlinkSubmitter when present in submissionProperties" in {
    val mockEks = mock[K8sFlinkSubmitter]
    val nodeSelectorCaptor = org.mockito.ArgumentCaptor.forClass(classOf[Map[String, String]])
    when(
      mockEks.submit(
        jobId = org.mockito.ArgumentMatchers.anyString(),
        mainClass = org.mockito.ArgumentMatchers.anyString(),
        mainJarUri = org.mockito.ArgumentMatchers.anyString(),
        jarUris = org.mockito.ArgumentMatchers.any(),
        flinkCheckpointUri = org.mockito.ArgumentMatchers.anyString(),
        maybeSavepointUri = org.mockito.ArgumentMatchers.any(),
        maybeFlinkJarsUri = org.mockito.ArgumentMatchers.any(),
        jobProperties = org.mockito.ArgumentMatchers.any(),
        args = org.mockito.ArgumentMatchers.any(),
        serviceAccount = org.mockito.ArgumentMatchers.anyString(),
        namespace = org.mockito.ArgumentMatchers.anyString(),
        envVars = org.mockito.ArgumentMatchers.any(),
        nodeSelector = nodeSelectorCaptor.capture()
      )
    ).thenReturn("flink-abc123")

    val submitter = new EmrSubmitter("test-customer", mock[EmrClient], mock[Ec2Client], Some(mockEks))
    submitter.submit(
      jobType = FlinkJob,
      submissionProperties = Map(
        JobId -> "test-job-id",
        MainClass -> "ai.chronon.flink.FlinkJob",
        JarURI -> "s3://bucket/cloud_aws_lib_deploy.jar",
        FlinkMainJarURI -> "s3://bucket/flink_assembly_deploy.jar",
        FlinkCheckpointUri -> "s3://bucket/checkpoints",
        EksServiceAccount -> "zipline-flink-sa",
        EksNamespace -> "zipline-flink",
        EksNodeSelector -> "sardine.ai/node-type=flink"
      ),
      jobProperties = Map.empty,
      files = List.empty,
      labels = Map.empty,
      envVars = Map.empty
    )

    nodeSelectorCaptor.getValue shouldBe Map("sardine.ai/node-type" -> "flink")
  }

  it should "use single quotes for regular confs and double quotes for Databricks token confs" in {
    val stepId = "mock-step-id"
    val clusterId = "j-MOCKCLUSTERID123"

    val mockEmrClient = mock[EmrClient]
    val mockEc2Client = mock[Ec2Client]
    val mockEksSubmitter = mock[K8sFlinkSubmitter]

    val requestCaptor = org.mockito.ArgumentCaptor.forClass(classOf[AddJobFlowStepsRequest])
    when(
      mockEmrClient.addJobFlowSteps(requestCaptor.capture())
    ).thenReturn(AddJobFlowStepsResponse.builder().stepIds(stepId).build())

    val submitter = new EmrSubmitter("canary", mockEmrClient, mockEc2Client, Some(mockEksSubmitter), awsRegion = "us-west-2")
    submitter.submit(
      jobType = SparkJob,
      submissionProperties = Map(
        MainClass -> "some-main-class",
        JarURI -> "s3://jar-uri",
        ClusterId -> clusterId,
        "DATABRICKS_HOST" -> "https://my-workspace.cloud.databricks.com",
        "DATABRICKS_SECRET_NAME" -> "my-secret"
      ),
      jobProperties = Map(
        "spark.executor.memory" -> "4g",
        "spark.sql.catalog.workspace.token" -> "$DATABRICKS_OAUTH_TOKEN"
      ),
      files = List.empty,
      labels = Map.empty,
      envVars = Map.empty,
      args = "arg1"
    )

    val actualArgs = requestCaptor.getValue.steps().get(0).hadoopJarStep().args().toScala.mkString(" ")
    // Regular confs use single quotes (no shell expansion)
    assert(actualArgs.contains("--conf 'spark.executor.memory=4g'"))
    // Token confs use double quotes (shell expansion for $DATABRICKS_OAUTH_TOKEN)
    assert(actualArgs.contains("""--conf "spark.sql.catalog.workspace.token=$DATABRICKS_OAUTH_TOKEN""""))
    // Token fetch script is present
    assert(actualArgs.contains("aws secretsmanager get-secret-value"))
    assert(actualArgs.contains("DATABRICKS_OAUTH_TOKEN="))
  }

  it should "throw exception when only DATABRICKS_HOST is set without DATABRICKS_SECRET_NAME" in {
    val mockEmrClient = mock[EmrClient]
    val mockEc2Client = mock[Ec2Client]
    val mockEksSubmitter = mock[K8sFlinkSubmitter]

    val submitter = new EmrSubmitter("canary", mockEmrClient, mockEc2Client, Some(mockEksSubmitter), awsRegion = "us-west-2")
    intercept[IllegalArgumentException] {
      submitter.submit(
        jobType = SparkJob,
        submissionProperties = Map(
          MainClass -> "some-main-class",
          JarURI -> "s3://jar-uri",
          ClusterId -> "j-CLUSTER",
          "DATABRICKS_HOST" -> "https://my-workspace.cloud.databricks.com"
        ),
        jobProperties = Map.empty,
        files = List.empty,
        labels = Map.empty,
        envVars = Map.empty,
        args = "arg1"
      )
    }
  }

  // --- getJobUrl / getSparkUrl ---

  "getJobUrl" should "return regionalized EMR console URL for Spark jobs" in {
    val submitter = createTestSubmitter()
    val url = submitter.getJobUrl("j-1794O33LZQKP:s-ABC123")
    url shouldBe Some("https://us-west-2.console.aws.amazon.com/emr/home?region=us-west-2#/clusterDetails/j-1794O33LZQKP")
  }

  it should "return EKS URL for Flink jobs" in {
    val submitter = new EmrSubmitter("test-customer", mock[EmrClient], mock[Ec2Client], Some(mock[K8sFlinkSubmitter]),
      awsRegion = "us-west-2", eksClusterName = Some("test-eks-cluster"))
    val url = submitter.getJobUrl("flink:zipline-flink:my-deployment")
    url shouldBe Some("https://us-west-2.console.aws.amazon.com/eks/clusters/test-eks-cluster/deployments/my-deployment?namespace=zipline-flink&region=us-west-2")
  }

  it should "return None for invalid job ID format" in {
    val submitter = createTestSubmitter()
    submitter.getJobUrl("invalid-no-colon") shouldBe None
  }

  "getSparkUrl" should "return persistent SHS URL for Spark jobs" in {
    val submitter = createTestSubmitter()
    val url = submitter.getSparkUrl("j-1794O33LZQKP:s-ABC123")
    url shouldBe Some("https://p-1794o33lzqkp-shs.emrappui-prod.us-west-2.amazonaws.com/shs/")
  }

  it should "return None for Flink jobs" in {
    val submitter = createTestSubmitter()
    submitter.getSparkUrl("flink:zipline-flink:my-deployment") shouldBe None
  }

  it should "return None for invalid job ID format" in {
    val submitter = createTestSubmitter()
    submitter.getSparkUrl("invalid-no-colon") shouldBe None
  }

  // --- isClusterCreateNeeded ---

  "isClusterCreateNeeded" should "return true for non-long-running jobs (EMR batch)" in {
    val submitter = createTestSubmitter()
    submitter.isClusterCreateNeeded(isLongRunning = false) shouldBe true
  }

  it should "return false for long-running jobs (routed to EKS)" in {
    val submitter = createTestSubmitter()
    submitter.isClusterCreateNeeded(isLongRunning = true) shouldBe false
  }

  it should "test Kinesis Flink job end-to-end on EMR on EKS. Do NOT enable in CI / CD" ignore {
    // This test submits an actual Flink job to EMR on EKS to verify the FlinkDeployment submission works
    // To run:
    // CUSTOMER_ID=canary ./mill cloud_aws.test.testOnly "ai.chronon.integrations.aws.EmrSubmitterTest" -- -z "test Kinesis Flink job"

    // Skip if CUSTOMER_ID is not set (CI/CD or local env without setup)
    assume(sys.env.contains("CUSTOMER_ID"), "CUSTOMER_ID environment variable must be set to run E2E tests")

    val region = "us-west-2"
    val streamName = "user-activities"
    val registryName = "zipline-canary"
    val schemaName = "user-activities"
    val jobId = java.util.UUID.randomUUID.toString.take(8)

    // JARs - update these paths to match your deployment
    val flinkMainJarUri = "s3://zipline-artifacts-canary/jars/flink_deploy.jar"
    val cloudAwsJarUri = "s3://zipline-artifacts-canary/jars/cloud_aws_lib_deploy.jar"
    val kinesisConnectorJarUri = "s3://zipline-artifacts-canary/jars/connectors_kinesis_deploy.jar"
    val checkpointPath = "s3://zipline-warehouse-canary/flink-checkpoints/kinesis-user-activities-test"

    // Explicitly target the canary EKS cluster rather than relying on the ambient kubeconfig context
    val k8sConfig = Config.autoConfigure("arn:aws:eks:us-west-2:345594603419:cluster/canary-eks")
    val eksFlinkSubmitter = EksFlinkSubmitter(Some(k8sConfig))

    val deploymentName = eksFlinkSubmitter.submit(
      jobId = jobId,
      mainClass = "ai.chronon.flink_connectors.kinesis.KinesisUserActivitiesJob",
      mainJarUri = flinkMainJarUri,
      jarUris = Array(cloudAwsJarUri, kinesisConnectorJarUri),
      flinkCheckpointUri = checkpointPath,
      maybeSavepointUri = None,
      maybeFlinkJarsUri = None,
      jobProperties = Map.empty,
      args = Seq(
        s"--stream-name=$streamName",
        s"--registry-name=$registryName",
        s"--schema-name=$schemaName",
        s"--region=$region",
        s"--checkpoint-dir=$checkpointPath"
      ),
      serviceAccount = "zipline-flink-sa",
      namespace = "zipline-flink"
    )

    println(s"✅ Submitted Flink job")
    println(s"   Deployment Name: $deploymentName")
    println(s"   Monitor with: kubectl get flinkdeployment $deploymentName -n zipline-flink")
    println(s"   View logs: kubectl logs -n zipline-flink -l app=$deploymentName")

    assert(deploymentName.nonEmpty, "Deployment name should not be empty")
  }

  it should "getLatestCheckpointPath returns the highest-numbered checkpoint" in {
    val mockStorageClient = mock[StorageClient]
    val flinkJobId = "abc123"
    val flinkStateUri = "s3://my-bucket/flink-state"
    val checkpointBase = s"$flinkStateUri/checkpoints/$flinkJobId"

    when(mockStorageClient.listFiles(checkpointBase)).thenReturn(
      Iterator(
        s"$checkpointBase/chk-5/_metadata",
        s"$checkpointBase/chk-1/_metadata",
        s"$checkpointBase/chk-12/_metadata"
      )
    )

    val result = StorageClient.resolveLatestCheckpointPath(mockStorageClient, flinkJobId, flinkStateUri)
    assertEquals(Some(s"$checkpointBase/chk-12"), result)
  }

  it should "getLatestCheckpointPath returns None when no checkpoints exist" in {
    val mockStorageClient = mock[StorageClient]
    val flinkJobId = "abc123"
    val flinkStateUri = "s3://my-bucket/flink-state"

    when(mockStorageClient.listFiles(anyString())).thenReturn(Iterator.empty)

    val result = StorageClient.resolveLatestCheckpointPath(mockStorageClient, flinkJobId, flinkStateUri)
    assertEquals(None, result)
  }

  it should "getLatestCheckpointPath returns None when s3Client is not set" in {
    val submitter = new EmrSubmitter("canary", mock[EmrClient], mock[Ec2Client], None, s3Client = None)
    val result = submitter.getLatestCheckpointPath("abc123", "s3://my-bucket/flink-state")
    assertEquals(None, result)
  }

  it should "getFlinkInternalJobId delegates to flinkInternalJobIdFetchFn" in {
    val flinkJobId = "flink-internal-uuid-123"
    val submitter = new EmrSubmitter(
      "canary",
      mock[EmrClient],
      mock[Ec2Client],
      None,
      flinkInternalJobIdFetchFn = _ => Some(flinkJobId)
    )
    val result = submitter.getFlinkInternalJobId("emr:cluster:flink:my-deployment")
    assertEquals(Some(flinkJobId), result)
  }

  it should "Used to iterate locally. Do not enable this in CI/CD!" ignore {
    val emrSubmitter = new EmrSubmitter(
      "canary",
      EmrClient.builder().build(),
      Ec2Client.builder().build(),
      Some(EksFlinkSubmitter())
    )
    val jobId = emrSubmitter.submit(
      jobType = SparkJob,
      submissionProperties = Map(
        MainClass -> "ai.chronon.spark.Driver",
        JarURI -> "s3://zipline-artifacts-canary/jars/cloud_aws_lib_deploy.jar",
        ClusterId -> "j-13BASWFP15TLR"
      ),
      jobProperties = Map.empty,
      files = List("s3://zipline-warehouse-canary/purchases.v1"),
      labels = Map.empty,
      envVars = Map.empty,
      args = Seq(
        "group-by-backfill",
        "--conf-path",
        "/mnt/zipline/purchases.v1",
        "--end-date",
        "2025-02-26",
        "--conf-type",
        "group_bys"
      ): _*
    )
    println("EMR job id: " + jobId)
    0
  }

}
