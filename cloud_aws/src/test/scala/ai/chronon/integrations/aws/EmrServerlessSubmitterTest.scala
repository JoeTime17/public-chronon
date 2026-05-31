package ai.chronon.integrations.aws

import ai.chronon.api.JobStatusType
import ai.chronon.integrations.cloud_k8s.K8sFlinkSubmitter
import ai.chronon.spark.submission
import ai.chronon.spark.submission.JobSubmitterConstants._
import ai.chronon.spark.submission.{FlinkJob, StorageClient}
import org.junit.Assert.assertEquals
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.emrserverless.EmrServerlessClient
import software.amazon.awssdk.services.emrserverless.model._

import scala.jdk.CollectionConverters._

class EmrServerlessSubmitterTest extends AnyFlatSpec with Matchers with MockitoSugar {

  /** Mock ListApplications to return a single app with the given name and ID.
    * Most tests use this to set up the name-based resolution path.
    */
  private def mockListApplications(mockClient: EmrServerlessClient,
                                    appId: String,
                                    appName: String = "test-app"): Unit = {
    val appSummary = ApplicationSummary
      .builder()
      .id(appId)
      .name(appName)
      .state(ApplicationState.STARTED)
      .build()
    val listResponse = ListApplicationsResponse.builder().applications(List(appSummary).asJava).build()
    when(mockClient.listApplications(any[ListApplicationsRequest])).thenReturn(listResponse)
  }

  /** Also mock GetJobRun so findApplicationForJob succeeds for status/kill/URL tests. */
  private def mockFindApplicationForJob(mockClient: EmrServerlessClient,
                                        appId: String,
                                        jobRunId: String): Unit = {
    mockListApplications(mockClient, appId)
    val getJobRunResponse = GetJobRunResponse
      .builder()
      .jobRun(JobRun.builder().applicationId(appId).jobRunId(jobRunId).state(JobRunState.RUNNING).build())
      .build()
    when(mockClient.getJobRun(any[GetJobRunRequest])).thenReturn(getJobRunResponse)
  }

  private def createSubmitter(
      mockClient: EmrServerlessClient,
      executionRoleArn: String = "arn:aws:iam::123456789012:role/EMRServerlessRole",
      s3LogUri: String = "s3://my-bucket/logs/",
      eksFlinkSubmitter: Option[K8sFlinkSubmitter] = None,
      awsRegion: String = "us-east-1",
      eksClusterName: Option[String] = None,
      ingressBaseUrl: Option[String] = None,
      emrStudioId: Option[String] = None,
      applicationName: String = "test-app",
      kvStoreApiProperties: Map[String, String] = Map(
        "AWS_DEFAULT_REGION" -> "us-east-1"
      ),
      s3Client: Option[software.amazon.awssdk.services.s3.S3Client] = None
  ): EmrServerlessSubmitter = {
    new EmrServerlessSubmitter(
      mockClient,
      executionRoleArn,
      s3LogUri,
      eksFlinkSubmitter = eksFlinkSubmitter,
      awsRegion = awsRegion,
      eksClusterName = eksClusterName,
      ingressBaseUrl = ingressBaseUrl,
      emrStudioId = emrStudioId,
      applicationName = applicationName,
      kvStoreApiProperties = kvStoreApiProperties,
      s3Client = s3Client
    )
  }

  "EmrServerlessSubmitter" should "submit a Spark job using app ID from submission props" in {
    val mockClient = mock[EmrServerlessClient]
    val applicationId = "app-123456"

    val jobRunId = "job-run-123"
    val startJobRunResponse = StartJobRunResponse
      .builder()
      .applicationId(applicationId)
      .jobRunId(jobRunId)
      .build()

    when(mockClient.startJobRun(any[StartJobRunRequest]))
      .thenReturn(startJobRunResponse)

    val submitter = createSubmitter(mockClient)

    val submittedJobId = submitter.submit(
      submission.SparkJob,
      Map(
        MainClass -> "ai.chronon.spark.Driver",
        JarURI -> "s3://my-bucket/jars/cloud-aws.jar",
        JobId -> "test-job-id",
        submitter.clusterIdentifierKey -> applicationId
      ),
      Map("spark.executor.memory" -> "4g"),
      List.empty,
      Map("team" -> "chronon"),
      Map.empty
    )

    assertEquals(jobRunId, submittedJobId)
    verify(mockClient).startJobRun(any[StartJobRunRequest])

    val requestCaptor = ArgumentCaptor.forClass(classOf[StartJobRunRequest])
    verify(mockClient).startJobRun(requestCaptor.capture())
    assertEquals(applicationId, requestCaptor.getValue.applicationId())
  }

  it should "resolve app by name via ListApplications when no app ID in submission props" in {
    val mockClient = mock[EmrServerlessClient]
    val appId = "app-resolved-456"

    mockListApplications(mockClient, appId, "my-emr-app")

    val jobRunId = "job-run-456"
    when(mockClient.startJobRun(any[StartJobRunRequest]))
      .thenReturn(StartJobRunResponse.builder().applicationId(appId).jobRunId(jobRunId).build())

    val submitter = createSubmitter(mockClient, applicationName = "my-emr-app")

    val submittedJobId = submitter.submit(
      submission.SparkJob,
      Map(
        MainClass -> "ai.chronon.spark.Driver",
        JarURI -> "s3://my-bucket/jars/cloud-aws.jar",
        JobId -> "test-job-id"
      ),
      Map.empty,
      List.empty,
      Map.empty,
      Map.empty
    )

    assertEquals(jobRunId, submittedJobId)
    verify(mockClient, never()).createApplication(any[CreateApplicationRequest])
  }

  it should "fail with clear error if no application found by name" in {
    val mockClient = mock[EmrServerlessClient]

    val listAppsResponse = ListApplicationsResponse
      .builder()
      .applications(List.empty[ApplicationSummary].asJava)
      .build()
    when(mockClient.listApplications(any[ListApplicationsRequest]))
      .thenReturn(listAppsResponse)

    val submitter = createSubmitter(mockClient, applicationName = "nonexistent-app")

    val exception = intercept[RuntimeException] {
      submitter.submit(
        submission.SparkJob,
        Map(
          MainClass -> "ai.chronon.spark.Driver",
          JarURI -> "s3://my-bucket/jars/cloud-aws.jar",
          JobId -> "test-job-id"
        ),
        Map.empty,
        List.empty,
        Map.empty,
        Map.empty
      )
    }

    exception.getMessage should include("No EMR Serverless application found")
    exception.getMessage should include("Terraform")
    verify(mockClient, never()).createApplication(any[CreateApplicationRequest])
  }

  it should "return PENDING status for a submitted job" in {
    val mockClient = mock[EmrServerlessClient]
    val applicationId = "app-123"
    val jobRunId = "job-run-123"

    mockFindApplicationForJob(mockClient, applicationId, jobRunId)
    // Override the GetJobRun mock to return PENDING
    val getJobRunResponse = GetJobRunResponse
      .builder()
      .jobRun(
        JobRun.builder()
          .applicationId(applicationId)
          .jobRunId(jobRunId)
          .state(JobRunState.PENDING)
          .build()
      )
      .build()
    when(mockClient.getJobRun(any[GetJobRunRequest]))
      .thenReturn(getJobRunResponse)

    val submitter = createSubmitter(mockClient)

    assertEquals(JobStatusType.PENDING, submitter.status(jobRunId))
  }

  it should "return RUNNING status for a running job" in {
    val mockClient = mock[EmrServerlessClient]
    val applicationId = "app-123"
    val jobRunId = "job-run-123"

    mockFindApplicationForJob(mockClient, applicationId, jobRunId)

    val submitter = createSubmitter(mockClient)

    assertEquals(JobStatusType.RUNNING, submitter.status(jobRunId))
  }

  it should "return SUCCEEDED status for a completed job" in {
    val mockClient = mock[EmrServerlessClient]
    val applicationId = "app-123"
    val jobRunId = "job-run-123"

    mockListApplications(mockClient, applicationId)
    val getJobRunResponse = GetJobRunResponse
      .builder()
      .jobRun(
        JobRun.builder()
          .applicationId(applicationId)
          .jobRunId(jobRunId)
          .state(JobRunState.SUCCESS)
          .build()
      )
      .build()
    when(mockClient.getJobRun(any[GetJobRunRequest]))
      .thenReturn(getJobRunResponse)

    val submitter = createSubmitter(mockClient)

    assertEquals(JobStatusType.SUCCEEDED, submitter.status(jobRunId))
  }

  it should "return FAILED status for a failed job" in {
    val mockClient = mock[EmrServerlessClient]
    val applicationId = "app-123"
    val jobRunId = "job-run-123"

    mockListApplications(mockClient, applicationId)
    val getJobRunResponse = GetJobRunResponse
      .builder()
      .jobRun(
        JobRun.builder()
          .applicationId(applicationId)
          .jobRunId(jobRunId)
          .state(JobRunState.FAILED)
          .build()
      )
      .build()
    when(mockClient.getJobRun(any[GetJobRunRequest]))
      .thenReturn(getJobRunResponse)

    val submitter = createSubmitter(mockClient)

    assertEquals(JobStatusType.FAILED, submitter.status(jobRunId))
  }

  it should "cancel a job successfully" in {
    val mockClient = mock[EmrServerlessClient]
    val applicationId = "app-123"
    val jobRunId = "job-run-123"

    mockFindApplicationForJob(mockClient, applicationId, jobRunId)

    val cancelResponse = CancelJobRunResponse.builder().build()
    when(mockClient.cancelJobRun(any[CancelJobRunRequest]))
      .thenReturn(cancelResponse)

    val submitter = createSubmitter(mockClient)
    submitter.kill(jobRunId)

    val requestCaptor = ArgumentCaptor.forClass(classOf[CancelJobRunRequest])
    verify(mockClient).cancelJobRun(requestCaptor.capture())

    val capturedRequest = requestCaptor.getValue
    assertEquals(applicationId, capturedRequest.applicationId())
    assertEquals(jobRunId, capturedRequest.jobRunId())
  }

  it should "delegate Flink status to eksFlinkSubmitter" in {
    val mockClient = mock[EmrServerlessClient]
    val mockFlinkSubmitter = mock[K8sFlinkSubmitter]
    when(mockFlinkSubmitter.statusWithCreationTime("my-deployment", "my-namespace"))
      .thenReturn((JobStatusType.RUNNING, Some(java.time.Instant.now())))

    val submitter = createSubmitter(mockClient, eksFlinkSubmitter = Some(mockFlinkSubmitter))

    val status = submitter.status("flink:my-namespace:my-deployment")
    assertEquals(JobStatusType.RUNNING, status)
    verify(mockFlinkSubmitter).statusWithCreationTime("my-deployment", "my-namespace")
  }

  it should "delegate Flink kill to eksFlinkSubmitter" in {
    val mockClient = mock[EmrServerlessClient]
    val mockFlinkSubmitter = mock[K8sFlinkSubmitter]

    val submitter = createSubmitter(mockClient, eksFlinkSubmitter = Some(mockFlinkSubmitter))

    submitter.kill("flink:my-namespace:my-deployment")
    verify(mockFlinkSubmitter).delete("my-deployment", "my-namespace")
  }

  it should "capture StartJobRun request with correct spark submit parameters" in {
    val mockClient = mock[EmrServerlessClient]
    val applicationId = "app-123"

    val startJobRunResponse = StartJobRunResponse
      .builder()
      .applicationId(applicationId)
      .jobRunId("job-run-123")
      .build()

    when(mockClient.startJobRun(any[StartJobRunRequest]))
      .thenReturn(startJobRunResponse)

    val submitter = createSubmitter(mockClient)

    submitter.submit(
      submission.SparkJob,
      Map(
        MainClass -> "ai.chronon.spark.Driver",
        JarURI -> "s3://my-bucket/jars/cloud-aws.jar",
        JobId -> "test-job-id",
        submitter.clusterIdentifierKey -> applicationId
      ),
      Map(
        "spark.executor.memory" -> "4g",
        "spark.executor.cores" -> "2"
      ),
      List.empty,
      Map("team" -> "chronon"),
      Map.empty,
      "--arg1",
      "--arg2=value"
    )

    val requestCaptor = ArgumentCaptor.forClass(classOf[StartJobRunRequest])
    verify(mockClient).startJobRun(requestCaptor.capture())

    val capturedRequest = requestCaptor.getValue
    assertEquals(applicationId, capturedRequest.applicationId())

    val sparkSubmit = capturedRequest.jobDriver().sparkSubmit()
    assertEquals("s3://my-bucket/jars/cloud-aws.jar", sparkSubmit.entryPoint())

    val args = sparkSubmit.entryPointArguments().asScala
    assert(args.contains("--arg1"))
    assert(args.contains("--arg2=value"))

    assertEquals("chronon", capturedRequest.tags().get("team"))
  }

  it should "include monitoring configuration with S3 log URI" in {
    val mockClient = mock[EmrServerlessClient]
    val applicationId = "app-123"
    val s3LogUri = "s3://my-bucket/emr-logs/"

    val startJobRunResponse = StartJobRunResponse
      .builder()
      .applicationId(applicationId)
      .jobRunId("job-run-123")
      .build()

    when(mockClient.startJobRun(any[StartJobRunRequest]))
      .thenReturn(startJobRunResponse)

    val submitter = createSubmitter(mockClient, s3LogUri = s3LogUri)

    submitter.submit(
      submission.SparkJob,
      Map(
        MainClass -> "ai.chronon.spark.Driver",
        JarURI -> "s3://my-bucket/jars/test.jar",
        JobId -> "test-job",
        submitter.clusterIdentifierKey -> applicationId
      ),
      Map.empty,
      List.empty,
      Map.empty,
      Map.empty
    )

    val requestCaptor = ArgumentCaptor.forClass(classOf[StartJobRunRequest])
    verify(mockClient).startJobRun(requestCaptor.capture())

    val capturedRequest = requestCaptor.getValue
    val monitoringConfig = capturedRequest.configurationOverrides().monitoringConfiguration()
    assertEquals(s3LogUri, monitoringConfig.s3MonitoringConfiguration().logUri())
  }

  it should "return correct jarName" in {
    val mockClient = mock[EmrServerlessClient]
    val submitter = createSubmitter(mockClient)
    assertEquals("cloud_aws_lib_deploy.jar", submitter.jarName)
  }

  it should "return correct onlineClass" in {
    val mockClient = mock[EmrServerlessClient]
    val submitter = createSubmitter(mockClient)
    assertEquals("ai.chronon.integrations.aws.AwsApiImpl", submitter.onlineClass)
  }

  it should "return filename from resolveConfPath for Spark --files distribution" in {
    val mockClient = mock[EmrServerlessClient]
    val submitter = createSubmitter(mockClient)
    val s3Uri = "s3://my-bucket/configs/join.json"
    assertEquals("join.json", submitter.resolveConfPath(s3Uri))
  }

  it should "return true for isClusterCreateNeeded for batch jobs" in {
    val mockClient = mock[EmrServerlessClient]
    val submitter = createSubmitter(mockClient)
    assertEquals(true, submitter.isClusterCreateNeeded(isLongRunning = false))
  }

  it should "return false for isClusterCreateNeeded for long-running jobs" in {
    val mockClient = mock[EmrServerlessClient]
    val submitter = createSubmitter(mockClient)
    assertEquals(false, submitter.isClusterCreateNeeded(isLongRunning = true))
  }

  it should "return app ID from ensureClusterReady when app is ready" in {
    val mockClient = mock[EmrServerlessClient]
    val appId = "app-ready-123"
    val appName = "test-app"

    mockListApplications(mockClient, appId, appName)

    val getAppResponse = GetApplicationResponse
      .builder()
      .application(
        Application.builder().applicationId(appId).state(ApplicationState.STARTED).build()
      )
      .build()
    when(mockClient.getApplication(any[GetApplicationRequest]))
      .thenReturn(getAppResponse)

    val submitter = createSubmitter(mockClient)

    import scala.concurrent.ExecutionContext.Implicits.global
    val result = submitter.ensureClusterReady(appName, None)
    assertEquals(Some(appId), result)
  }

  it should "return None from ensureClusterReady when app is starting" in {
    val mockClient = mock[EmrServerlessClient]
    val appId = "app-starting-123"
    val appName = "starting-app"

    val existingApp = ApplicationSummary
      .builder()
      .id(appId)
      .name(appName)
      .state(ApplicationState.STARTING)
      .build()
    val listAppsResponse = ListApplicationsResponse
      .builder()
      .applications(List(existingApp).asJava)
      .build()
    when(mockClient.listApplications(any[ListApplicationsRequest]))
      .thenReturn(listAppsResponse)

    val getAppResponse = GetApplicationResponse
      .builder()
      .application(
        Application.builder().applicationId(appId).state(ApplicationState.STARTING).build()
      )
      .build()
    when(mockClient.getApplication(any[GetApplicationRequest]))
      .thenReturn(getAppResponse)

    val submitter = createSubmitter(mockClient)

    import scala.concurrent.ExecutionContext.Implicits.global
    val result = submitter.ensureClusterReady(appName, None)
    assertEquals(None, result)
  }

  it should "return app ID from ensureClusterReady when app is stopped (auto-start on job submit)" in {
    val mockClient = mock[EmrServerlessClient]
    val appId = "app-stopped-123"
    val appName = "stopped-app"

    val existingApp = ApplicationSummary
      .builder()
      .id(appId)
      .name(appName)
      .state(ApplicationState.STOPPED)
      .build()
    val listAppsResponse = ListApplicationsResponse
      .builder()
      .applications(List(existingApp).asJava)
      .build()
    when(mockClient.listApplications(any[ListApplicationsRequest]))
      .thenReturn(listAppsResponse)

    val getAppResponse = GetApplicationResponse
      .builder()
      .application(
        Application.builder().applicationId(appId).state(ApplicationState.STOPPED).build()
      )
      .build()
    when(mockClient.getApplication(any[GetApplicationRequest]))
      .thenReturn(getAppResponse)

    val submitter = createSubmitter(mockClient)

    import scala.concurrent.ExecutionContext.Implicits.global
    val result = submitter.ensureClusterReady(appName, None)
    assertEquals(Some(appId), result)
  }

  it should "throw from ensureClusterReady when app is terminated" in {
    val mockClient = mock[EmrServerlessClient]
    val appId = "app-terminated-123"
    val appName = "terminated-app"

    val existingApp = ApplicationSummary
      .builder()
      .id(appId)
      .name(appName)
      .state(ApplicationState.TERMINATED)
      .build()
    val listAppsResponse = ListApplicationsResponse
      .builder()
      .applications(List(existingApp).asJava)
      .build()
    when(mockClient.listApplications(any[ListApplicationsRequest]))
      .thenReturn(listAppsResponse)

    val submitter = createSubmitter(mockClient)

    import scala.concurrent.ExecutionContext.Implicits.global
    // findApplication filters out TERMINATED apps, so this hits the "no app found" path
    val exception = intercept[RuntimeException] {
      submitter.ensureClusterReady(appName, None)
    }
    exception.getMessage should include("No EMR Serverless application found")
  }

  it should "throw from ensureClusterReady when no app found" in {
    val mockClient = mock[EmrServerlessClient]

    val listAppsResponse = ListApplicationsResponse
      .builder()
      .applications(List.empty[ApplicationSummary].asJava)
      .build()
    when(mockClient.listApplications(any[ListApplicationsRequest]))
      .thenReturn(listAppsResponse)

    val submitter = createSubmitter(mockClient)

    import scala.concurrent.ExecutionContext.Implicits.global
    val exception = intercept[RuntimeException] {
      submitter.ensureClusterReady("nonexistent-app", None)
    }
    exception.getMessage should include("No EMR Serverless application found")
    exception.getMessage should include("Terraform")
  }

  it should "return correct kvStoreApiProperties" in {
    val mockClient = mock[EmrServerlessClient]
    val submitter = createSubmitter(
      mockClient,
      awsRegion = "us-west-2",
      kvStoreApiProperties = Map(
        "AWS_DEFAULT_REGION" -> "us-west-2"
      )
    )
    val props = submitter.kvStoreApiProperties
    assertEquals("us-west-2", props("AWS_DEFAULT_REGION"))
  }

  it should "return EMR Studio URL when studio ID is provided" in {
    val mockClient = mock[EmrServerlessClient]
    val appId = "app-123"
    val jobRunId = "job-run-456"

    mockFindApplicationForJob(mockClient, appId, jobRunId)

    val submitter = createSubmitter(mockClient, awsRegion = "us-west-2",
      emrStudioId = Some("5zs3voh3ex6jahzji73kwcc14"))
    val url = submitter.getJobUrl(jobRunId)
    assert(url.isDefined)
    assertEquals(
      s"https://es-5zs3voh3ex6jahzji73kwcc14.emrstudio-prod.us-west-2.amazonaws.com/#/serverless-applications/$appId/$jobRunId",
      url.get
    )
  }

  it should "normalize studio ID with es- prefix and uppercase from ListStudios API" in {
    val mockClient = mock[EmrServerlessClient]
    val appId = "app-123"
    val jobRunId = "job-run-456"

    mockFindApplicationForJob(mockClient, appId, jobRunId)

    val submitter = createSubmitter(mockClient, awsRegion = "us-west-2",
      emrStudioId = Some("es-5ZS3VOH3EX6JAHZJI73KWCC14"))
    val url = submitter.getJobUrl(jobRunId)
    assert(url.isDefined)
    assertEquals(
      s"https://es-5zs3voh3ex6jahzji73kwcc14.emrstudio-prod.us-west-2.amazonaws.com/#/serverless-applications/$appId/$jobRunId",
      url.get
    )
  }

  it should "return None for getJobUrl on flink jobs without eksClusterName" in {
    val mockClient = mock[EmrServerlessClient]
    val submitter = createSubmitter(mockClient, awsRegion = "us-west-2", eksClusterName = None)
    val url = submitter.getJobUrl("flink:ns:deploy")
    assert(url.isEmpty)
  }

  it should "construct correct EMR Studio URL with realistic IDs" in {
    val mockClient = mock[EmrServerlessClient]
    val appId = "00g43cs10nq1310l"
    val jobRunId = "00g43m46p6r9p80n"

    mockFindApplicationForJob(mockClient, appId, jobRunId)

    val submitter = createSubmitter(
      mockClient,
      awsRegion = "us-west-2",
      emrStudioId = Some("5zs3voh3ex6jahzji73kwcc14")
    )
    val url = submitter.getJobUrl(jobRunId)
    assert(url.isDefined)
    assertEquals(
      s"https://es-5zs3voh3ex6jahzji73kwcc14.emrstudio-prod.us-west-2.amazonaws.com/#/serverless-applications/$appId/$jobRunId",
      url.get
    )
  }

  it should "return Spark UI URL via dashboard API" in {
    val mockClient = mock[EmrServerlessClient]
    val appId = "app-123"
    val jobRunId = "job-run-456"

    mockFindApplicationForJob(mockClient, appId, jobRunId)

    val dashboardUrl = "https://es-abc123.emrstudio-prod.us-west-2.amazonaws.com/spark-ui"
    val response = GetDashboardForJobRunResponse.builder().url(dashboardUrl).build()
    when(mockClient.getDashboardForJobRun(any[GetDashboardForJobRunRequest])).thenReturn(response)
    val submitter = createSubmitter(mockClient, awsRegion = "us-west-2")
    val url = submitter.getSparkUrl(jobRunId)
    assert(url.isDefined)
    assertEquals(dashboardUrl, url.get)
  }

  it should "return None for getSparkUrl when dashboard API fails" in {
    val mockClient = mock[EmrServerlessClient]
    val appId = "app-123"
    val jobRunId = "job-run-456"

    mockFindApplicationForJob(mockClient, appId, jobRunId)

    when(mockClient.getDashboardForJobRun(any[GetDashboardForJobRunRequest]))
      .thenThrow(ValidationException.builder().message("Dashboard not available").build())
    val submitter = createSubmitter(mockClient, awsRegion = "us-west-2")
    val url = submitter.getSparkUrl(jobRunId)
    assert(url.isEmpty)
  }

  it should "return EKS URL for Flink job" in {
    val mockClient = mock[EmrServerlessClient]
    val submitter = createSubmitter(
      mockClient,
      awsRegion = "us-east-1",
      eksClusterName = Some("my-cluster")
    )
    val url = submitter.getJobUrl("flink:my-ns:my-deploy")
    assert(url.isDefined)
    assert(url.get.contains("eks/clusters/my-cluster/deployments/my-deploy"))
  }

  it should "return Flink UI URL via ingressBaseUrl" in {
    val mockClient = mock[EmrServerlessClient]
    val submitter = createSubmitter(
      mockClient,
      ingressBaseUrl = Some("https://hub.example.com")
    )
    val url = submitter.getFlinkUrl("flink:my-ns:my-deploy")
    assert(url.isDefined)
    assertEquals("https://hub.example.com/flink/my-deploy/", url.get)
  }

  it should "return None for getFlinkUrl on non-flink job" in {
    val mockClient = mock[EmrServerlessClient]
    val submitter = createSubmitter(mockClient, ingressBaseUrl = Some("https://hub.example.com"))
    assertEquals(None, submitter.getFlinkUrl("job-run-456"))
  }

  it should "return empty deprecatedClusterNameEnvVars" in {
    val mockClient = mock[EmrServerlessClient]
    val submitter = createSubmitter(mockClient)
    assert(submitter.deprecatedClusterNameEnvVars.isEmpty)
  }

  // --- findApplicationForJob ---

  "findApplicationForJob" should "find app by scanning applications" in {
    val mockClient = mock[EmrServerlessClient]
    val appId = "app-found-123"
    val jobRunId = "job-run-scan-456"

    mockFindApplicationForJob(mockClient, appId, jobRunId)

    val submitter = createSubmitter(mockClient)

    val status = submitter.status(jobRunId)
    assertEquals(JobStatusType.RUNNING, status)
  }

  it should "cache app ID after first lookup" in {
    val mockClient = mock[EmrServerlessClient]
    val appId = "app-cached-123"
    val jobRunId = "job-run-cached-456"

    mockFindApplicationForJob(mockClient, appId, jobRunId)

    val submitter = createSubmitter(mockClient)

    // First call triggers scan
    submitter.status(jobRunId)
    // Second call should use cache — listApplications should only be called once
    submitter.status(jobRunId)

    verify(mockClient, times(1)).listApplications(any[ListApplicationsRequest])
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

  private def createTestServerlessSubmitter(): EmrServerlessSubmitter =
    new EmrServerlessSubmitter(mock[EmrServerlessClient],
      executionRoleArn = "arn:aws:iam::123456789012:role/TestRole",
      s3LogUri = "s3://test-bucket/logs/",
      eksFlinkSubmitter = Some(mock[K8sFlinkSubmitter]))

  "buildFlinkSubmissionProps" should "include flink jar URI, checkpoint URI, EKS service account and namespace" in {
    val submitter = createTestServerlessSubmitter()
    val props = submitter.buildFlinkSubmissionProps(baseFlinkEnv, testVersion, testArtifactPrefix)

    props(FlinkMainJarURI) shouldBe s"$testArtifactPrefix/release/$testVersion/jars/flink_assembly_deploy.jar"
    props(FlinkCheckpointUri) shouldBe "s3://test-bucket/flink-state/checkpoints"
    props(EksServiceAccount) shouldBe "zipline-flink-sa"
    props(EksNamespace) shouldBe "zipline-flink"
  }

  it should "include kinesis connector jar when ENABLE_KINESIS is true" in {
    val submitter = createTestServerlessSubmitter()
    val env = baseFlinkEnv + ("ENABLE_KINESIS" -> "true")

    val props = submitter.buildFlinkSubmissionProps(env, testVersion, testArtifactPrefix)

    props(FlinkKinesisConnectorJarURI) shouldBe kinesisConnectorJarUri
  }

  it should "omit kinesis connector jar when ENABLE_KINESIS is false or absent" in {
    val submitter = createTestServerlessSubmitter()

    val props = submitter.buildFlinkSubmissionProps(baseFlinkEnv, testVersion, testArtifactPrefix)

    props.contains(FlinkKinesisConnectorJarURI) shouldBe false
  }

  it should "throw exception when FLINK_STATE_URI is not set" in {
    val submitter = createTestServerlessSubmitter()
    val env = baseFlinkEnv - "FLINK_STATE_URI"

    intercept[IllegalArgumentException] {
      submitter.buildFlinkSubmissionProps(env, testVersion, testArtifactPrefix)
    }
  }

  it should "throw exception when FLINK_EKS_SERVICE_ACCOUNT is not set" in {
    val submitter = createTestServerlessSubmitter()
    val env = baseFlinkEnv - "FLINK_EKS_SERVICE_ACCOUNT"

    intercept[IllegalArgumentException] {
      submitter.buildFlinkSubmissionProps(env, testVersion, testArtifactPrefix)
    }
  }

  it should "throw exception when FLINK_EKS_NAMESPACE is not set" in {
    val submitter = createTestServerlessSubmitter()
    val env = baseFlinkEnv - "FLINK_EKS_NAMESPACE"

    intercept[IllegalArgumentException] {
      submitter.buildFlinkSubmissionProps(env, testVersion, testArtifactPrefix)
    }
  }

  it should "prefer constructor-level flinkEksServiceAccount/flinkEksNamespace over env vars" in {
    val submitter = new EmrServerlessSubmitter(
      mock[EmrServerlessClient],
      executionRoleArn = "arn:aws:iam::123456789012:role/TestRole",
      s3LogUri = "s3://test-bucket/logs/",
      eksFlinkSubmitter = Some(mock[K8sFlinkSubmitter]),
      flinkEksServiceAccount = Some("constructor-sa"),
      flinkEksNamespace = Some("constructor-ns")
    )
    val env = baseFlinkEnv + ("FLINK_EKS_SERVICE_ACCOUNT" -> "env-sa", "FLINK_EKS_NAMESPACE" -> "env-ns")

    val props = submitter.buildFlinkSubmissionProps(env, testVersion, testArtifactPrefix)

    props(EksServiceAccount) shouldBe "constructor-sa"
    props(EksNamespace) shouldBe "constructor-ns"
  }

  // --- status Flink health check behavior ---

  "status" should "return PENDING for flink job when EKS is RUNNING but health check fails within grace period" in {
    val mockClient = mock[EmrServerlessClient]
    val mockFlinkSubmitter = mock[K8sFlinkSubmitter]
    // CRD was created just now — well within the 20-min grace window
    when(mockFlinkSubmitter.statusWithCreationTime("my-deployment", "zipline-flink"))
      .thenReturn((JobStatusType.RUNNING, Some(java.time.Instant.now())))

    val submitter = new EmrServerlessSubmitter(
      mockClient,
      executionRoleArn = "arn:aws:iam::123456789012:role/TestRole",
      s3LogUri = "s3://test-bucket/logs/",
      eksFlinkSubmitter = Some(mockFlinkSubmitter),
      flinkHealthCheckFn = _ => false
    )
    val result = submitter.status("flink:zipline-flink:my-deployment")

    result shouldBe JobStatusType.PENDING
  }

  it should "return FAILED for flink job when EKS is RUNNING but health check fails past grace period" in {
    val mockClient = mock[EmrServerlessClient]
    val mockFlinkSubmitter = mock[K8sFlinkSubmitter]
    // CRD was created 25 minutes ago — past the 20-min grace window
    val oldCreationTime = java.time.Instant.now().minus(java.time.Duration.ofMinutes(25))
    when(mockFlinkSubmitter.statusWithCreationTime("my-deployment", "zipline-flink"))
      .thenReturn((JobStatusType.RUNNING, Some(oldCreationTime)))

    val submitter = new EmrServerlessSubmitter(
      mockClient,
      executionRoleArn = "arn:aws:iam::123456789012:role/TestRole",
      s3LogUri = "s3://test-bucket/logs/",
      eksFlinkSubmitter = Some(mockFlinkSubmitter),
      flinkHealthCheckFn = _ => false
    )
    val result = submitter.status("flink:zipline-flink:my-deployment")

    result shouldBe JobStatusType.FAILED
  }

  it should "return PENDING for flink job when EKS is RUNNING, health check fails, and no creation time is available" in {
    val mockClient = mock[EmrServerlessClient]
    val mockFlinkSubmitter = mock[K8sFlinkSubmitter]
    // No creation time returned — conservatively stays PENDING (cannot determine grace window)
    when(mockFlinkSubmitter.statusWithCreationTime("my-deployment", "zipline-flink"))
      .thenReturn((JobStatusType.RUNNING, None))

    val submitter = new EmrServerlessSubmitter(
      mockClient,
      executionRoleArn = "arn:aws:iam::123456789012:role/TestRole",
      s3LogUri = "s3://test-bucket/logs/",
      eksFlinkSubmitter = Some(mockFlinkSubmitter),
      flinkHealthCheckFn = _ => false
    )
    val result = submitter.status("flink:zipline-flink:my-deployment")

    result shouldBe JobStatusType.PENDING
  }

  it should "pass the flink URL derived from ingressBaseUrl to the health check fn" in {
    val mockClient = mock[EmrServerlessClient]
    val mockFlinkSubmitter = mock[K8sFlinkSubmitter]
    when(mockFlinkSubmitter.statusWithCreationTime("my-deployment", "zipline-flink"))
      .thenReturn((JobStatusType.RUNNING, Some(java.time.Instant.now())))

    var capturedUrl: Option[String] = None
    val submitter = new EmrServerlessSubmitter(
      mockClient,
      executionRoleArn = "arn:aws:iam::123456789012:role/TestRole",
      s3LogUri = "s3://test-bucket/logs/",
      eksFlinkSubmitter = Some(mockFlinkSubmitter),
      ingressBaseUrl = Some("https://hub.example.com"),
      flinkHealthCheckFn = url => { capturedUrl = url; true }
    )
    submitter.status("flink:zipline-flink:my-deployment")

    capturedUrl shouldBe Some("https://hub.example.com/flink/my-deployment/")
  }

  it should "propagate non-RUNNING EKS status without invoking health check" in {
    val mockClient = mock[EmrServerlessClient]
    val mockFlinkSubmitter = mock[K8sFlinkSubmitter]
    when(mockFlinkSubmitter.statusWithCreationTime("my-deployment", "zipline-flink"))
      .thenReturn((JobStatusType.PENDING, None))

    var healthCheckCalled = false
    val submitter = new EmrServerlessSubmitter(
      mockClient,
      executionRoleArn = "arn:aws:iam::123456789012:role/TestRole",
      s3LogUri = "s3://test-bucket/logs/",
      eksFlinkSubmitter = Some(mockFlinkSubmitter),
      flinkHealthCheckFn = _ => { healthCheckCalled = true; true }
    )
    val result = submitter.status("flink:zipline-flink:my-deployment")

    result shouldBe JobStatusType.PENDING
    healthCheckCalled shouldBe false
  }

  // --- submit for FlinkJob ---

  "submit" should "return flink:namespace:deploymentName for FlinkJob" in {
    val mockClient = mock[EmrServerlessClient]
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

    val submitter = createSubmitter(mockClient, eksFlinkSubmitter = Some(mockEks))
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

  // --- createSubmissionPropsMap for Flink ---

  "createSubmissionPropsMap" should "parse all required Flink args from command-line style args" in {
    val jobId = "test-job-id-123"
    val mainClass = "ai.chronon.flink.FlinkJob"
    val jarURI = "s3://zipline-jars/cloud-aws.jar"
    val flinkMainJarURI = "s3://zipline-jars/flink-assembly.jar"
    val flinkCheckpointUri = "s3://zipline-warehouse/flink-checkpoints"
    val kinesisConnectorJarURI = "s3://zipline-jars/kinesis-connector.jar"
    val flinkJarsUri = "s3://zipline-artifacts/flink-jars/"
    val eksServiceAccount = "zipline-flink-sa"
    val eksNamespace = "zipline-flink"

    val actual = EmrServerlessSubmitter.createSubmissionPropsMap(
      jobType = FlinkJob,
      args = Array(
        s"$JobIdArgKeyword=$jobId",
        s"$JarUriArgKeyword=$jarURI",
        s"$MainClassKeyword=$mainClass",
        s"$FlinkMainJarUriArgKeyword=$flinkMainJarURI",
        s"$FlinkKinesisJarUriArgKeyword=$kinesisConnectorJarURI",
        s"$FlinkJarsUriArgKeyword=$flinkJarsUri",
        s"$StreamingCheckpointPathArgKeyword=$flinkCheckpointUri",
        s"$EksServiceAccountArgKeyword=$eksServiceAccount",
        s"$EksNamespaceArgKeyword=$eksNamespace"
      )
    )

    assertEquals(actual(JobId), jobId)
    assertEquals(actual(MainClass), mainClass)
    assertEquals(actual(JarURI), jarURI)
    assertEquals(actual(FlinkMainJarURI), flinkMainJarURI)
    assertEquals(actual(FlinkKinesisConnectorJarURI), kinesisConnectorJarURI)
    assertEquals(actual(FlinkJarsUri), flinkJarsUri)
    assertEquals(actual(FlinkCheckpointUri), flinkCheckpointUri)
    assertEquals(actual(EksServiceAccount), eksServiceAccount)
    assertEquals(actual(EksNamespace), eksNamespace)
  }

  it should "include savepoint URI when custom savepoint arg is provided" in {
    val customSavepoint = "s3://zipline-warehouse/flink-checkpoints/chk-100"

    val actual = EmrServerlessSubmitter.createSubmissionPropsMap(
      jobType = FlinkJob,
      args = Array(
        s"$JobIdArgKeyword=test-job",
        s"$JarUriArgKeyword=s3://bucket/jar.jar",
        s"$MainClassKeyword=ai.chronon.flink.FlinkJob",
        s"$FlinkMainJarUriArgKeyword=s3://bucket/flink.jar",
        s"$StreamingCheckpointPathArgKeyword=s3://bucket/checkpoints",
        s"$EksServiceAccountArgKeyword=sa",
        s"$EksNamespaceArgKeyword=ns",
        s"$StreamingCustomSavepointArgKeyword=$customSavepoint"
      )
    )

    assertEquals(actual(SavepointUri), customSavepoint)
  }

  it should "omit savepoint URI when StreamingLatestSavepointArgKeyword is present (not yet supported)" in {
    val actual = EmrServerlessSubmitter.createSubmissionPropsMap(
      jobType = FlinkJob,
      args = Array(
        s"$JobIdArgKeyword=test-job",
        s"$JarUriArgKeyword=s3://bucket/jar.jar",
        s"$MainClassKeyword=ai.chronon.flink.FlinkJob",
        s"$FlinkMainJarUriArgKeyword=s3://bucket/flink.jar",
        s"$StreamingCheckpointPathArgKeyword=s3://bucket/checkpoints",
        s"$EksServiceAccountArgKeyword=sa",
        s"$EksNamespaceArgKeyword=ns",
        StreamingLatestSavepointArgKeyword
      )
    )

    assert(!actual.contains(SavepointUri))
  }

  // --- Required field validation on submit for Flink ---

  it should "throw when JobId is missing from submissionProperties for Flink" in {
    val mockClient = mock[EmrServerlessClient]
    val mockEks = mock[K8sFlinkSubmitter]
    val submitter = createSubmitter(mockClient, eksFlinkSubmitter = Some(mockEks))

    intercept[RuntimeException] {
      submitter.submit(
        jobType = FlinkJob,
        submissionProperties = Map(
          MainClass -> "ai.chronon.flink.FlinkJob",
          JarURI -> "s3://bucket/jar.jar",
          FlinkMainJarURI -> "s3://bucket/flink.jar",
          FlinkCheckpointUri -> "s3://bucket/checkpoints",
          EksServiceAccount -> "sa",
          EksNamespace -> "ns"
        ),
        jobProperties = Map.empty,
        files = List.empty,
        labels = Map.empty,
        envVars = Map.empty
      )
    }
  }

  it should "throw when FlinkMainJarURI is missing from submissionProperties" in {
    val mockClient = mock[EmrServerlessClient]
    val mockEks = mock[K8sFlinkSubmitter]
    val submitter = createSubmitter(mockClient, eksFlinkSubmitter = Some(mockEks))

    intercept[RuntimeException] {
      submitter.submit(
        jobType = FlinkJob,
        submissionProperties = Map(
          JobId -> "test-job",
          MainClass -> "ai.chronon.flink.FlinkJob",
          JarURI -> "s3://bucket/jar.jar",
          FlinkCheckpointUri -> "s3://bucket/checkpoints",
          EksServiceAccount -> "sa",
          EksNamespace -> "ns"
        ),
        jobProperties = Map.empty,
        files = List.empty,
        labels = Map.empty,
        envVars = Map.empty
      )
    }
  }

  it should "throw when EksServiceAccount is missing from submissionProperties" in {
    val mockClient = mock[EmrServerlessClient]
    val mockEks = mock[K8sFlinkSubmitter]
    val submitter = createSubmitter(mockClient, eksFlinkSubmitter = Some(mockEks))

    intercept[RuntimeException] {
      submitter.submit(
        jobType = FlinkJob,
        submissionProperties = Map(
          JobId -> "test-job",
          MainClass -> "ai.chronon.flink.FlinkJob",
          JarURI -> "s3://bucket/jar.jar",
          FlinkMainJarURI -> "s3://bucket/flink.jar",
          FlinkCheckpointUri -> "s3://bucket/checkpoints",
          EksNamespace -> "ns"
        ),
        jobProperties = Map.empty,
        files = List.empty,
        labels = Map.empty,
        envVars = Map.empty
      )
    }
  }

  "resolveEnvVars" should "resolve {VAR} placeholders from environment variables in job properties" in {
    val mockClient = mock[EmrServerlessClient]
    val applicationId = "app-env-123"

    when(mockClient.startJobRun(any[StartJobRunRequest]))
      .thenReturn(StartJobRunResponse.builder().applicationId(applicationId).jobRunId("job-env-1").build())

    val submitter = createSubmitter(mockClient)

    // HOME is a reliable env var present in all environments
    val homeValue = sys.env("HOME")

    submitter.submit(
      submission.SparkJob,
      Map(
        MainClass -> "ai.chronon.spark.Driver",
        JarURI -> "s3://bucket/jar.jar",
        JobId -> "test-env-resolve",
        submitter.clusterIdentifierKey -> applicationId
      ),
      Map("spark.some.config" -> "{HOME}/data"),
      List.empty,
      Map.empty,
      Map.empty
    )

    val requestCaptor = ArgumentCaptor.forClass(classOf[StartJobRunRequest])
    verify(mockClient).startJobRun(requestCaptor.capture())

    val appConfigs = requestCaptor.getValue.configurationOverrides().applicationConfiguration()
    appConfigs should not be empty
    val props = appConfigs.get(0).properties()
    props.get("spark.some.config") shouldBe s"$homeValue/data"
  }

  it should "leave properties without placeholders unchanged" in {
    val mockClient = mock[EmrServerlessClient]
    val applicationId = "app-env-456"

    when(mockClient.startJobRun(any[StartJobRunRequest]))
      .thenReturn(StartJobRunResponse.builder().applicationId(applicationId).jobRunId("job-env-2").build())

    val submitter = createSubmitter(mockClient)

    submitter.submit(
      submission.SparkJob,
      Map(
        MainClass -> "ai.chronon.spark.Driver",
        JarURI -> "s3://bucket/jar.jar",
        JobId -> "test-no-placeholder",
        submitter.clusterIdentifierKey -> applicationId
      ),
      Map("spark.executor.memory" -> "4g", "spark.driver.cores" -> "2"),
      List.empty,
      Map.empty,
      Map.empty
    )

    val requestCaptor = ArgumentCaptor.forClass(classOf[StartJobRunRequest])
    verify(mockClient).startJobRun(requestCaptor.capture())

    val props = requestCaptor.getValue.configurationOverrides().applicationConfiguration().get(0).properties()
    props.get("spark.executor.memory") shouldBe "4g"
    props.get("spark.driver.cores") shouldBe "2"
  }

  it should "keep spark-defaults at <=100 and spill the rest into --conf on sparkSubmitParameters" in {
    val mockClient = mock[EmrServerlessClient]
    val applicationId = "app-overflow-123"

    when(mockClient.startJobRun(any[StartJobRunRequest]))
      .thenReturn(StartJobRunResponse.builder().applicationId(applicationId).jobRunId("job-overflow-1").build())

    val submitter = createSubmitter(mockClient)

    // 150 props forces overflow past the 100-prop EMR Serverless cap.
    val props = (1 to 150).map(i => f"spark.chronon.prop.$i%03d" -> s"val$i").toMap

    submitter.submit(
      submission.SparkJob,
      Map(
        MainClass -> "ai.chronon.spark.Driver",
        JarURI -> "s3://bucket/jar.jar",
        JobId -> "test-overflow",
        submitter.clusterIdentifierKey -> applicationId
      ),
      props,
      List.empty,
      Map.empty,
      Map.empty
    )

    val requestCaptor = ArgumentCaptor.forClass(classOf[StartJobRunRequest])
    verify(mockClient).startJobRun(requestCaptor.capture())

    val captured = requestCaptor.getValue
    val inline = captured.configurationOverrides().applicationConfiguration().get(0).properties()
    inline.size() shouldBe 100
    // Deterministic alphabetic split: 001..100 inline, 101..150 overflowed as --conf
    inline.containsKey("spark.chronon.prop.001") shouldBe true
    inline.containsKey("spark.chronon.prop.100") shouldBe true
    inline.containsKey("spark.chronon.prop.101") shouldBe false

    val submitParams = captured.jobDriver().sparkSubmit().sparkSubmitParameters()
    submitParams should include("--conf spark.chronon.prop.101=val101")
    submitParams should include("--conf spark.chronon.prop.150=val150")
    submitParams should not include "--conf spark.chronon.prop.100="
  }

  it should "not emit overflow --conf flags when props fit under the cap" in {
    val mockClient = mock[EmrServerlessClient]
    val applicationId = "app-no-overflow"

    when(mockClient.startJobRun(any[StartJobRunRequest]))
      .thenReturn(StartJobRunResponse.builder().applicationId(applicationId).jobRunId("job-no-overflow").build())

    val submitter = createSubmitter(mockClient)

    submitter.submit(
      submission.SparkJob,
      Map(
        MainClass -> "ai.chronon.spark.Driver",
        JarURI -> "s3://bucket/jar.jar",
        JobId -> "test-fits",
        submitter.clusterIdentifierKey -> applicationId
      ),
      Map("spark.executor.memory" -> "4g", "spark.executor.cores" -> "2"),
      List.empty,
      Map.empty,
      Map.empty
    )

    val requestCaptor = ArgumentCaptor.forClass(classOf[StartJobRunRequest])
    verify(mockClient).startJobRun(requestCaptor.capture())

    val submitParams = requestCaptor.getValue.jobDriver().sparkSubmit().sparkSubmitParameters()
    submitParams should not include "--conf"
  }

  "maybeShellQuote" should "leave simple values unquoted" in {
    EmrServerlessSubmitter.maybeShellQuote("spark.executor.memory=4g") shouldBe "spark.executor.memory=4g"
    EmrServerlessSubmitter.maybeShellQuote("spark.x=s3://bucket/path") shouldBe "spark.x=s3://bucket/path"
  }

  it should "single-quote values containing whitespace or shell metacharacters" in {
    EmrServerlessSubmitter.maybeShellQuote("spark.x=hello world") shouldBe "'spark.x=hello world'"
    EmrServerlessSubmitter.maybeShellQuote("spark.x=a$b") shouldBe "'spark.x=a$b'"
    // Embedded single quotes use the POSIX close/escape/reopen trick
    EmrServerlessSubmitter.maybeShellQuote("a'b") shouldBe "'a'\\''b'"
  }

  "splitForConfigCap" should "return all in the first map when size <= cap" in {
    val props = (1 to 50).map(i => s"k$i" -> s"v$i").toMap
    val (head, tail) = EmrServerlessSubmitter.splitForConfigCap(props)
    head shouldBe props
    tail shouldBe Map.empty[String, String]
  }

  it should "split deterministically at 100 when size > cap" in {
    val props = (1 to 150).map(i => f"k$i%03d" -> s"v$i").toMap
    val (head, tail) = EmrServerlessSubmitter.splitForConfigCap(props)
    head.size shouldBe 100
    tail.size shouldBe 50
    head.contains("k001") shouldBe true
    head.contains("k100") shouldBe true
    tail.contains("k101") shouldBe true
    tail.contains("k150") shouldBe true
  }

  "envVarsToSparkProperties" should "emit spark.emr-serverless.driverEnv and spark.executorEnv entries only" in {
    val submitter = createSubmitter(mock[EmrServerlessClient])
    val result = submitter.envVarsToSparkProperties(Map("CUSTOMER_ID" -> "canary", "AWS_REGION" -> "us-west-2"))
    result shouldBe Map(
      "spark.emr-serverless.driverEnv.CUSTOMER_ID" -> "canary",
      "spark.executorEnv.CUSTOMER_ID" -> "canary",
      "spark.emr-serverless.driverEnv.AWS_REGION" -> "us-west-2",
      "spark.executorEnv.AWS_REGION" -> "us-west-2"
    )
    // Crucially, none of the legacy YARN / K8s / bogus driverEnv prefixes appear.
    result.keys.exists(_.startsWith("spark.yarn.appMasterEnv.")) shouldBe false
    result.keys.exists(_.startsWith("spark.kubernetes.driverEnv.")) shouldBe false
    result.keys.exists(k => k.startsWith("spark.driverEnv.") && !k.startsWith("spark.emr-serverless.")) shouldBe false
  }

  it should "thread envVars into spark-defaults with the EMR Serverless expansion" in {
    val mockClient = mock[EmrServerlessClient]
    val applicationId = "app-envvars-123"

    when(mockClient.startJobRun(any[StartJobRunRequest]))
      .thenReturn(StartJobRunResponse.builder().applicationId(applicationId).jobRunId("job-envvars-1").build())

    val submitter = createSubmitter(mockClient)

    submitter.submit(
      submission.SparkJob,
      Map(
        MainClass -> "ai.chronon.spark.Driver",
        JarURI -> "s3://bucket/jar.jar",
        JobId -> "test-envvars",
        submitter.clusterIdentifierKey -> applicationId
      ),
      Map.empty,
      List.empty,
      Map.empty,
      Map("CUSTOMER_ID" -> "canary")
    )

    val requestCaptor = ArgumentCaptor.forClass(classOf[StartJobRunRequest])
    verify(mockClient).startJobRun(requestCaptor.capture())
    val props = requestCaptor.getValue.configurationOverrides().applicationConfiguration().get(0).properties()
    props.get("spark.emr-serverless.driverEnv.CUSTOMER_ID") shouldBe "canary"
    props.get("spark.executorEnv.CUSTOMER_ID") shouldBe "canary"
    props.containsKey("spark.yarn.appMasterEnv.CUSTOMER_ID") shouldBe false
  }

  it should "keep unresolvable {VAR} placeholders as-is when env var is not set" in {
    val mockClient = mock[EmrServerlessClient]
    val applicationId = "app-env-789"

    when(mockClient.startJobRun(any[StartJobRunRequest]))
      .thenReturn(StartJobRunResponse.builder().applicationId(applicationId).jobRunId("job-env-3").build())

    val submitter = createSubmitter(mockClient)

    submitter.submit(
      submission.SparkJob,
      Map(
        MainClass -> "ai.chronon.spark.Driver",
        JarURI -> "s3://bucket/jar.jar",
        JobId -> "test-unresolvable",
        submitter.clusterIdentifierKey -> applicationId
      ),
      Map("spark.some.token" -> "{CHRONON_NONEXISTENT_VAR_XYZ}"),
      List.empty,
      Map.empty,
      Map.empty
    )

    val requestCaptor = ArgumentCaptor.forClass(classOf[StartJobRunRequest])
    verify(mockClient).startJobRun(requestCaptor.capture())

    val props = requestCaptor.getValue.configurationOverrides().applicationConfiguration().get(0).properties()
    props.get("spark.some.token") shouldBe "{CHRONON_NONEXISTENT_VAR_XYZ}"
  }

  "getLatestCheckpointPath" should "return the highest-numbered checkpoint" in {
    val mockStorageClient = mock[StorageClient]
    val flinkJobId = "abc123"
    val flinkStateUri = "s3://my-bucket/flink-state"
    val checkpointBase = s"$flinkStateUri/checkpoints/$flinkJobId"

    when(mockStorageClient.listFiles(checkpointBase)).thenReturn(
      Iterator(
        s"$checkpointBase/chk-3/_metadata",
        s"$checkpointBase/chk-7/_metadata",
        s"$checkpointBase/chk-1/_metadata"
      )
    )

    val result = StorageClient.resolveLatestCheckpointPath(mockStorageClient, flinkJobId, flinkStateUri)
    assertEquals(Some(s"$checkpointBase/chk-7"), result)
  }

  it should "return None when no checkpoints exist" in {
    val mockStorageClient = mock[StorageClient]
    when(mockStorageClient.listFiles(any[String])).thenReturn(Iterator.empty)

    val result = StorageClient.resolveLatestCheckpointPath(mockStorageClient, "abc123", "s3://my-bucket/flink-state")
    assertEquals(None, result)
  }

  it should "return None when s3Client is not set" in {
    val submitter = createSubmitter(mock[EmrServerlessClient], s3Client = None)
    val result = submitter.getLatestCheckpointPath("abc123", "s3://my-bucket/flink-state")
    assertEquals(None, result)
  }

  "getFlinkInternalJobId" should "delegate to flinkInternalJobIdFetchFn" in {
    val mockClient = mock[EmrServerlessClient]
    mockListApplications(mockClient, "app-123")
    val flinkJobId = "flink-internal-uuid-456"
    val submitter = new EmrServerlessSubmitter(
      mockClient,
      executionRoleArn = "arn:aws:iam::123456789012:role/EMRServerlessRole",
      s3LogUri = "s3://my-bucket/logs/",
      applicationName = "test-app",
      flinkInternalJobIdFetchFn = _ => Some(flinkJobId)
    )
    val result = submitter.getFlinkInternalJobId("flink:app-123:my-deployment")
    assertEquals(Some(flinkJobId), result)
  }

  it should "return None when fetch fn returns None" in {
    val mockClient = mock[EmrServerlessClient]
    mockListApplications(mockClient, "app-123")
    val submitter = new EmrServerlessSubmitter(
      mockClient,
      executionRoleArn = "arn:aws:iam::123456789012:role/EMRServerlessRole",
      s3LogUri = "s3://my-bucket/logs/",
      applicationName = "test-app",
      flinkInternalJobIdFetchFn = _ => None
    )
    val result = submitter.getFlinkInternalJobId("flink:app-123:my-deployment")
    assertEquals(None, result)
  }

  /**
    * Integration test that submits a real Spark job to EMR Serverless.
    *
    * This test uses default configuration values that can be overridden with environment variables.
    *
    * Default configuration (modify these or set environment variables):
    * - AWS_REGION: us-east-1
    * - AWS_ACCOUNT_ID: 123456789012 (MUST be set to your actual AWS account)
    * - CUSTOMER_ID: canary (used for role and bucket naming)
    * - SPARK_CLUSTER_NAME: EMR Serverless application name (required)
    * - EMR_EXECUTION_ROLE_ARN: Defaults to arn:aws:iam::{ACCOUNT_ID}:role/zipline_{CUSTOMER_ID}_emr_serverless_role
    * - EMR_LOG_URI: Defaults to s3://zipline-logs-{CUSTOMER_ID}/emr-serverless/
    * - TEST_JAR_URI: Defaults to s3://zipline-artifacts-{CUSTOMER_ID}/jars/cloud-aws.jar
    * - TEST_MAIN_CLASS: ai.chronon.spark.Driver
    * - POLL_JOB_STATUS: false (set to "true" to wait for completion)
    *
    * Example setup:
    * export AWS_ACCOUNT_ID=123456789012
    * export CUSTOMER_ID=canary
    * export SPARK_CLUSTER_NAME=zipline-emr-canary
    * export POLL_JOB_STATUS=true
    *
    * Then run:
    * ./mill cloud_aws.test.testOnly ai.chronon.integrations.aws.EmrServerlessSubmitterTest -- -n Integration
    */
  it should "submit a real Spark job to EMR Serverless" taggedAs IntegrationTest ignore {
    val awsAccountId = sys.env.getOrElse("AWS_ACCOUNT_ID", "123456789012")
    val customerId = sys.env.getOrElse("CUSTOMER_ID", "canary")
    val region = sys.env.getOrElse("AWS_REGION", "us-east-1")
    val appName = sys.env.getOrElse("SPARK_CLUSTER_NAME", "zipline-emr-canary")

    val executionRoleArn = sys.env.getOrElse(
      "EMR_EXECUTION_ROLE_ARN",
      s"arn:aws:iam::$awsAccountId:role/zipline_${customerId}_emr_serverless_role"
    )
    val s3LogUri = sys.env.getOrElse(
      "EMR_LOG_URI",
      s"s3://zipline-logs-$customerId/emr-serverless/"
    )
    val jarUri = sys.env.getOrElse(
      "TEST_JAR_URI",
      s"s3://zipline-artifacts-$customerId/jars/cloud-aws.jar"
    )
    val mainClass = sys.env.getOrElse("TEST_MAIN_CLASS", "ai.chronon.spark.Driver")
    val pollStatus = sys.env.getOrElse("POLL_JOB_STATUS", "false").toBoolean

    println(s"=== EMR Serverless Integration Test ===")
    println(s"AWS Account ID: $awsAccountId")
    println(s"Customer ID: $customerId")
    println(s"Region: $region")
    println(s"Application Name: $appName")
    println(s"Execution Role: $executionRoleArn")
    println(s"Log URI: $s3LogUri")
    println(s"JAR URI: $jarUri")
    println(s"Main Class: $mainClass")
    println(s"Poll Status: $pollStatus")
    println()

    val client = EmrServerlessClient.builder()
      .region(Region.of(region))
      .build()

    val submitter = new EmrServerlessSubmitter(
      client,
      executionRoleArn,
      s3LogUri,
      applicationName = appName
    )

    val jobId = s"chronon-test-${System.currentTimeMillis()}"

    println(s"Submitting job with ID: $jobId")

    val submittedJobId = submitter.submit(
      submission.SparkJob,
      Map(
        MainClass -> mainClass,
        JarURI -> jarUri,
        JobId -> jobId
      ),
      Map(
        "spark.executor.cores" -> "2",
        "spark.executor.memory" -> "4g",
        "spark.driver.cores" -> "1",
        "spark.driver.memory" -> "2g"
      ),
      List.empty,
      Map(
        "test" -> "integration",
        "framework" -> "chronon"
      ),
      Map.empty,
      "--help"
    )

    println(s"Job submitted successfully!")
    println(s"Job Run ID: $submittedJobId")
    println()

    assert(submittedJobId.nonEmpty, "Job run ID should not be empty")

    if (pollStatus) {
      println("Polling job status...")
      var currentStatus = submitter.status(submittedJobId)
      var attempts = 0
      val maxAttempts = 60

      while (attempts < maxAttempts && (currentStatus == JobStatusType.PENDING || currentStatus == JobStatusType.RUNNING)) {
        println(s"Attempt ${attempts + 1}/$maxAttempts: Status = $currentStatus")
        Thread.sleep(10000)
        attempts += 1
        currentStatus = submitter.status(submittedJobId)
      }

      println(s"Final Status: $currentStatus (after $attempts attempts)")

      if (attempts >= maxAttempts) {
        println(s"Timeout: Job still in $currentStatus state after ${maxAttempts * 10} seconds")
      } else {
        assert(
          currentStatus == JobStatusType.SUCCEEDED || currentStatus == JobStatusType.FAILED,
          s"Job should reach terminal state, but got: $currentStatus"
        )
      }
    } else {
      println("Skipping status polling (set POLL_JOB_STATUS=true to enable)")
    }
  }

  "nodeSelector" should "parse multiple comma-separated key=value pairs" in {
    EmrServerlessSubmitter.parseNodeSelector("sardine.ai/node-type=flink,kubernetes.io/arch=amd64") shouldBe Map(
      "sardine.ai/node-type" -> "flink",
      "kubernetes.io/arch"   -> "amd64"
    )
  }

  "nodeSelector" should "return None when arg is absent" in {
    val args = Array("--other-arg=value")
    ai.chronon.spark.submission.JobSubmitter.getArgValue(args, "--eks-node-selector") shouldBe None
  }

  // regression test: values containing '=' must not be truncated
  "nodeSelector" should "parse a value that contains = (e.g. label value with equals sign)" in {
    val args = Array("--eks-node-selector=sardine.ai/node-type=flink")
    ai.chronon.spark.submission.JobSubmitter.getArgValue(args, "--eks-node-selector") shouldBe
      Some("sardine.ai/node-type=flink")
  }

  "nodeSelector" should "throw on malformed pair missing '='" in {
    an[IllegalArgumentException] should be thrownBy EmrServerlessSubmitter.parseNodeSelector("sardine.ai/node-type=flink,badtoken")
  }

  "nodeSelector" should "throw on blank input" in {
    an[IllegalArgumentException] should be thrownBy EmrServerlessSubmitter.parseNodeSelector("")
    an[IllegalArgumentException] should be thrownBy EmrServerlessSubmitter.parseNodeSelector("   ")
  }
}

object IntegrationTest extends org.scalatest.Tag("Integration")
