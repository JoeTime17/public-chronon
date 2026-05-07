package ai.chronon.integrations.cloud_azure

import ai.chronon.api.JobStatusType
import ai.chronon.integrations.cloud_k8s.K8sFlinkSubmitter
import ai.chronon.spark.submission
import ai.chronon.spark.submission.JobSubmitterConstants._
import ai.chronon.spark.submission.{FlinkJob, KyuubiSubmitter, StorageClient}
import org.junit.Assert._
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

class AzureSubmitterTest extends AnyFlatSpec with Matchers with MockitoSugar {

  private def createSubmitter(
      mockKyuubi: KyuubiSubmitter = mock[KyuubiSubmitter],
      mockAksFlink: K8sFlinkSubmitter = mock[K8sFlinkSubmitter],
      aksServiceAccount: Option[String] = Some("zipline-flink-sa"),
      aksNamespace: Option[String] = Some("zipline-flink"),
      ingressBaseUrl: Option[String] = None,
      storageClient: Option[StorageClient] = None,
      flinkHealthCheckFn: Option[String] => Boolean = _ => true,
      flinkInternalJobIdFetchFn: Option[String] => Option[String] = _ => None
  ): AzureSubmitter =
    new AzureSubmitter(
      kyuubiSubmitter = mockKyuubi,
      aksFlinkSubmitter = mockAksFlink,
      aksServiceAccount = aksServiceAccount,
      aksNamespace = aksNamespace,
      ingressBaseUrl = ingressBaseUrl,
      storageClient = storageClient,
      flinkHealthCheckFn = flinkHealthCheckFn,
      flinkInternalJobIdFetchFn = flinkInternalJobIdFetchFn
    )

  private val baseFlinkSubmissionProps = Map(
    JobId -> "test-job-id",
    MainClass -> "ai.chronon.flink.FlinkJob",
    JarURI -> "abfss://libs@account.dfs.core.windows.net/jars/cloud_azure_lib_deploy.jar",
    FlinkMainJarURI -> "abfss://libs@account.dfs.core.windows.net/jars/flink_assembly_deploy.jar",
    FlinkCheckpointUri -> "abfss://checkpoints@account.dfs.core.windows.net/flink/checkpoints",
    EksServiceAccount -> "zipline-flink-sa",
    EksNamespace -> "zipline-flink"
  )

  // --- submit: Spark jobs ---

  "submit" should "route Spark jobs to KyuubiSubmitter and return the job ID unchanged" in {
    val mockKyuubi = mock[KyuubiSubmitter]
    val submitter = createSubmitter(mockKyuubi = mockKyuubi)

    when(mockKyuubi.submit(any(), any(), any(), any(), any(), any(), any[String]()))
      .thenReturn("kyuubi-batch-123")

    val result = submitter.submit(
      jobType = submission.SparkJob,
      submissionProperties = Map(
        MainClass -> "ai.chronon.spark.Driver",
        JarURI -> "abfss://libs@account.dfs.core.windows.net/jars/cloud_azure.jar",
        MetadataName -> "my_feature",
        JobId -> "spark-job-1"
      ),
      jobProperties = Map("spark.executor.memory" -> "4g"),
      files = List("conf.json"),
      labels = Map("team" -> "chronon"),
      envVars = Map.empty
    )

    result shouldBe "kyuubi-batch-123"
    verify(mockKyuubi).submit(any(), any(), any(), any(), any(), any(), any[String]())
  }

  it should "pass submissionProperties and labels unchanged to KyuubiSubmitter for Spark jobs" in {
    val mockKyuubi = mock[KyuubiSubmitter]
    val submitter = createSubmitter(mockKyuubi = mockKyuubi)

    val submissionProps = Map(
      MainClass -> "ai.chronon.spark.Driver",
      JarURI -> "abfss://libs@account.dfs.core.windows.net/jars/cloud_azure.jar",
      MetadataName -> "my_feature",
      JobId -> "spark-job-2"
    )
    val labels = Map("env" -> "prod", "team" -> "zipline")

    when(
      mockKyuubi.submit(
        any(),
        org.mockito.ArgumentMatchers.eq(submissionProps),
        any(),
        any(),
        org.mockito.ArgumentMatchers.eq(labels),
        any(),
        any[String]()
      )).thenReturn("kyuubi-batch-456")

    val result = submitter.submit(
      jobType = submission.SparkJob,
      submissionProperties = submissionProps,
      jobProperties = Map.empty,
      files = List.empty,
      labels = labels,
      envVars = Map.empty
    )

    result shouldBe "kyuubi-batch-456"
  }

  it should "forward envVars verbatim to KyuubiSubmitter for Spark jobs" in {
    val mockKyuubi = mock[KyuubiSubmitter]
    val submitter = createSubmitter(mockKyuubi = mockKyuubi)

    val envVars = Map("FOO" -> "bar", "CUSTOMER_ID" -> "canary")

    when(
      mockKyuubi.submit(
        any(),
        any(),
        any(),
        any(),
        any(),
        org.mockito.ArgumentMatchers.eq(envVars),
        any[String]()
      )).thenReturn("kyuubi-batch-env")

    val result = submitter.submit(
      jobType = submission.SparkJob,
      submissionProperties = Map(
        MainClass -> "ai.chronon.spark.Driver",
        JarURI -> "abfss://libs@account.dfs.core.windows.net/jars/cloud_azure.jar",
        MetadataName -> "my_feature",
        JobId -> "spark-job-env"
      ),
      jobProperties = Map.empty,
      files = List.empty,
      labels = Map.empty,
      envVars = envVars
    )

    result shouldBe "kyuubi-batch-env"
    verify(mockKyuubi).submit(
      any(),
      any(),
      any(),
      any(),
      any(),
      org.mockito.ArgumentMatchers.eq(envVars),
      any[String]()
    )
  }

  // --- submit: Flink jobs ---

  it should "route Flink jobs to K8sFlinkSubmitter and prefix job ID with 'flink:'" in {
    val mockAksFlink = mock[K8sFlinkSubmitter]
    val submitter = createSubmitter(mockAksFlink = mockAksFlink)

    when(
      mockAksFlink.submit(
        jobId = anyString(),
        mainClass = anyString(),
        mainJarUri = anyString(),
        jarUris = any(),
        flinkCheckpointUri = anyString(),
        maybeSavepointUri = any(),
        maybeFlinkJarsUri = any(),
        jobProperties = any(),
        args = any(),
        serviceAccount = anyString(),
        namespace = anyString(),
        envVars = any(),
        nodeSelector = any()
      )).thenReturn("flink-abc123")

    val result = submitter.submit(
      jobType = FlinkJob,
      submissionProperties = baseFlinkSubmissionProps,
      jobProperties = Map.empty,
      files = List.empty,
      labels = Map.empty,
      envVars = Map.empty
    )

    result shouldBe "flink:zipline-flink:flink-abc123"
  }

  it should "forward custom savepoint URI to K8sFlinkSubmitter" in {
    val mockAksFlink = mock[K8sFlinkSubmitter]
    val submitter = createSubmitter(mockAksFlink = mockAksFlink)
    val savepointUri = "abfss://checkpoints@account.dfs.core.windows.net/flink/checkpoints/chk-100"

    when(
      mockAksFlink.submit(
        jobId = anyString(),
        mainClass = anyString(),
        mainJarUri = anyString(),
        jarUris = any(),
        flinkCheckpointUri = anyString(),
        maybeSavepointUri = org.mockito.ArgumentMatchers.eq(Some(savepointUri)),
        maybeFlinkJarsUri = any(),
        jobProperties = any(),
        args = any(),
        serviceAccount = anyString(),
        namespace = anyString(),
        envVars = any(),
        nodeSelector = any()
      )).thenReturn("flink-abc123")

    submitter.submit(
      jobType = FlinkJob,
      submissionProperties = baseFlinkSubmissionProps + (SavepointUri -> savepointUri),
      jobProperties = Map.empty,
      files = List.empty,
      labels = Map.empty,
      envVars = Map.empty
    )

    verify(mockAksFlink).submit(
      jobId = anyString(),
      mainClass = anyString(),
      mainJarUri = anyString(),
      jarUris = any(),
      flinkCheckpointUri = anyString(),
      maybeSavepointUri = org.mockito.ArgumentMatchers.eq(Some(savepointUri)),
      maybeFlinkJarsUri = any(),
      jobProperties = any(),
      args = any(),
      serviceAccount = anyString(),
      namespace = anyString(),
      envVars = any(),
      nodeSelector = any()
    )
  }

  it should "submit Flink job without savepoint when SavepointUri is absent" in {
    val mockAksFlink = mock[K8sFlinkSubmitter]
    val submitter = createSubmitter(mockAksFlink = mockAksFlink)

    when(
      mockAksFlink.submit(
        jobId = anyString(),
        mainClass = anyString(),
        mainJarUri = anyString(),
        jarUris = any(),
        flinkCheckpointUri = anyString(),
        maybeSavepointUri = org.mockito.ArgumentMatchers.eq(None),
        maybeFlinkJarsUri = any(),
        jobProperties = any(),
        args = any(),
        serviceAccount = anyString(),
        namespace = anyString(),
        envVars = any(),
        nodeSelector = any()
      )).thenReturn("flink-no-sp")

    val result = submitter.submit(
      jobType = FlinkJob,
      submissionProperties = baseFlinkSubmissionProps, // no SavepointUri key
      jobProperties = Map.empty,
      files = List.empty,
      labels = Map.empty,
      envVars = Map.empty
    )

    result shouldBe "flink:zipline-flink:flink-no-sp"
  }

  it should "throw RuntimeException for Flink submit when JobId is missing" in {
    val submitter = createSubmitter()

    intercept[RuntimeException] {
      submitter.submit(
        jobType = FlinkJob,
        submissionProperties = baseFlinkSubmissionProps - JobId,
        jobProperties = Map.empty,
        files = List.empty,
        labels = Map.empty,
        envVars = Map.empty
      )
    }
  }

  it should "throw RuntimeException for Flink submit when FlinkMainJarURI is missing" in {
    val submitter = createSubmitter()

    intercept[RuntimeException] {
      submitter.submit(
        jobType = FlinkJob,
        submissionProperties = baseFlinkSubmissionProps - FlinkMainJarURI,
        jobProperties = Map.empty,
        files = List.empty,
        labels = Map.empty,
        envVars = Map.empty
      )
    }
  }

  it should "throw RuntimeException for Flink submit when EksServiceAccount is not in props or constructor" in {
    val submitter = createSubmitter(aksServiceAccount = None, aksNamespace = Some("zipline-flink"))

    intercept[RuntimeException] {
      submitter.submit(
        jobType = FlinkJob,
        submissionProperties = baseFlinkSubmissionProps - EksServiceAccount,
        jobProperties = Map.empty,
        files = List.empty,
        labels = Map.empty,
        envVars = Map.empty
      )
    }
  }

  // --- status ---

  "status" should "delegate non-flink jobId to KyuubiSubmitter" in {
    val mockKyuubi = mock[KyuubiSubmitter]
    val submitter = createSubmitter(mockKyuubi = mockKyuubi)

    when(mockKyuubi.status("kyuubi-batch-123")).thenReturn(JobStatusType.RUNNING)

    submitter.status("kyuubi-batch-123") shouldBe JobStatusType.RUNNING
    verify(mockKyuubi).status("kyuubi-batch-123")
  }

  it should "return SUCCEEDED for Kyuubi SUCCEEDED status" in {
    val mockKyuubi = mock[KyuubiSubmitter]
    val submitter = createSubmitter(mockKyuubi = mockKyuubi)

    when(mockKyuubi.status("batch-done")).thenReturn(JobStatusType.SUCCEEDED)
    submitter.status("batch-done") shouldBe JobStatusType.SUCCEEDED
  }

  it should "return FAILED for Kyuubi FAILED status" in {
    val mockKyuubi = mock[KyuubiSubmitter]
    val submitter = createSubmitter(mockKyuubi = mockKyuubi)

    when(mockKyuubi.status("batch-err")).thenReturn(JobStatusType.FAILED)
    submitter.status("batch-err") shouldBe JobStatusType.FAILED
  }

  it should "delegate flink:namespace:deployment status to K8sFlinkSubmitter" in {
    val mockAksFlink = mock[K8sFlinkSubmitter]
    val submitter = createSubmitter(mockAksFlink = mockAksFlink)

    when(mockAksFlink.statusWithCreationTime("my-deployment", "zipline-flink"))
      .thenReturn((JobStatusType.RUNNING, Some(java.time.Instant.now())))

    submitter.status("flink:zipline-flink:my-deployment") shouldBe JobStatusType.RUNNING
    verify(mockAksFlink).statusWithCreationTime("my-deployment", "zipline-flink")
  }

  it should "return RUNNING for Flink job when health check passes" in {
    val mockAksFlink = mock[K8sFlinkSubmitter]
    val submitter = createSubmitter(
      mockAksFlink = mockAksFlink,
      flinkHealthCheckFn = _ => true
    )

    when(mockAksFlink.statusWithCreationTime("dep", "ns"))
      .thenReturn((JobStatusType.RUNNING, Some(java.time.Instant.now())))

    submitter.status("flink:ns:dep") shouldBe JobStatusType.RUNNING
  }

  it should "return PENDING for Flink job when health check fails within grace period" in {
    val mockAksFlink = mock[K8sFlinkSubmitter]
    val submitter = createSubmitter(
      mockAksFlink = mockAksFlink,
      flinkHealthCheckFn = _ => false
    )

    // CRD created just now — well within the 15-min grace window
    when(mockAksFlink.statusWithCreationTime("dep", "ns"))
      .thenReturn((JobStatusType.RUNNING, Some(java.time.Instant.now())))

    submitter.status("flink:ns:dep") shouldBe JobStatusType.PENDING
  }

  it should "return FAILED for Flink job when health check fails after grace period" in {
    val mockAksFlink = mock[K8sFlinkSubmitter]
    val submitter = createSubmitter(
      mockAksFlink = mockAksFlink,
      flinkHealthCheckFn = _ => false
    )

    // CRD created 20 minutes ago — past the 15-min grace window
    val oldCreation = java.time.Instant.now().minus(java.time.Duration.ofMinutes(20))
    when(mockAksFlink.statusWithCreationTime("dep", "ns"))
      .thenReturn((JobStatusType.RUNNING, Some(oldCreation)))

    submitter.status("flink:ns:dep") shouldBe JobStatusType.FAILED
  }

  it should "return PENDING for Flink job when health check fails and no creation time is available" in {
    val mockAksFlink = mock[K8sFlinkSubmitter]
    val submitter = createSubmitter(
      mockAksFlink = mockAksFlink,
      flinkHealthCheckFn = _ => false
    )

    // No creation time — conservatively stay PENDING
    when(mockAksFlink.statusWithCreationTime("dep", "ns"))
      .thenReturn((JobStatusType.RUNNING, None))

    submitter.status("flink:ns:dep") shouldBe JobStatusType.PENDING
  }

  it should "pass non-RUNNING Flink status through without invoking health check" in {
    val mockAksFlink = mock[K8sFlinkSubmitter]
    var healthCheckCalled = false
    val submitter = createSubmitter(
      mockAksFlink = mockAksFlink,
      flinkHealthCheckFn = _ => { healthCheckCalled = true; true }
    )

    when(mockAksFlink.statusWithCreationTime("dep", "ns"))
      .thenReturn((JobStatusType.FAILED, None))

    submitter.status("flink:ns:dep") shouldBe JobStatusType.FAILED
    healthCheckCalled shouldBe false
  }

  it should "pass flink URL from ingressBaseUrl to the health check fn" in {
    val mockAksFlink = mock[K8sFlinkSubmitter]
    var capturedUrl: Option[String] = None
    val submitter = createSubmitter(
      mockAksFlink = mockAksFlink,
      ingressBaseUrl = Some("https://hub.example.com"),
      flinkHealthCheckFn = url => { capturedUrl = url; true }
    )

    when(mockAksFlink.statusWithCreationTime("my-dep", "ns"))
      .thenReturn((JobStatusType.RUNNING, Some(java.time.Instant.now())))

    submitter.status("flink:ns:my-dep")
    capturedUrl shouldBe Some("https://hub.example.com/flink/my-dep/")
  }

  // --- kill ---

  "kill" should "delegate non-flink kill to KyuubiSubmitter" in {
    val mockKyuubi = mock[KyuubiSubmitter]
    val submitter = createSubmitter(mockKyuubi = mockKyuubi)

    submitter.kill("kyuubi-batch-123")
    verify(mockKyuubi).kill("kyuubi-batch-123")
  }

  it should "delegate flink kill to K8sFlinkSubmitter.delete" in {
    val mockAksFlink = mock[K8sFlinkSubmitter]
    val submitter = createSubmitter(mockAksFlink = mockAksFlink)

    submitter.kill("flink:zipline-flink:my-deployment")
    verify(mockAksFlink).delete("my-deployment", "zipline-flink")
  }

  // --- getFlinkUrl / getSparkUrl ---

  "getFlinkUrl" should "return None for non-flink job ID" in {
    val submitter = createSubmitter(ingressBaseUrl = Some("https://hub.example.com"))
    submitter.getFlinkUrl("kyuubi-batch-123") shouldBe None
  }

  it should "return the ingress URL for a flink job ID when ingressBaseUrl is set" in {
    val submitter = createSubmitter(ingressBaseUrl = Some("https://hub.example.com"))
    submitter.getFlinkUrl("flink:ns:my-deploy") shouldBe Some("https://hub.example.com/flink/my-deploy/")
  }

  it should "strip trailing slash from ingressBaseUrl before appending path" in {
    val submitter = createSubmitter(ingressBaseUrl = Some("https://hub.example.com/"))
    submitter.getFlinkUrl("flink:ns:my-deploy") shouldBe Some("https://hub.example.com/flink/my-deploy/")
  }

  it should "return None when ingressBaseUrl is not set" in {
    val submitter = createSubmitter(ingressBaseUrl = None)
    submitter.getFlinkUrl("flink:ns:my-deploy") shouldBe None
  }

  "getSparkUrl" should "delegate to KyuubiSubmitter for Spark job IDs" in {
    val mockKyuubi = mock[KyuubiSubmitter]
    val submitter = createSubmitter(mockKyuubi = mockKyuubi)

    when(mockKyuubi.getSparkUrl("batch-123")).thenReturn(Some("https://spark-history/history/app1"))
    submitter.getSparkUrl("batch-123") shouldBe Some("https://spark-history/history/app1")
  }

  it should "return None for flink job ID" in {
    val submitter = createSubmitter()
    submitter.getSparkUrl("flink:ns:dep") shouldBe None
  }

  // --- close ---

  "close" should "close the KyuubiSubmitter" in {
    val mockKyuubi = mock[KyuubiSubmitter]
    val submitter = createSubmitter(mockKyuubi = mockKyuubi)
    submitter.close()
    verify(mockKyuubi).close()
  }

  // --- buildFlinkSubmissionProps ---

  private val testArtifactPrefix = "abfss://artifacts@account.dfs.core.windows.net"
  private val testVersion = "1.0.0"
  private val baseFlinkEnv = Map(
    "FLINK_STATE_URI" -> "abfss://checkpoints@account.dfs.core.windows.net/flink",
    "FLINK_AKS_SERVICE_ACCOUNT" -> "zipline-flink-sa",
    "FLINK_AKS_NAMESPACE" -> "zipline-flink"
  )

  "buildFlinkSubmissionProps" should "include FlinkMainJarURI, FlinkCheckpointUri, service account, and namespace" in {
    val submitter = createSubmitter(aksServiceAccount = None, aksNamespace = None)
    val props = submitter.buildFlinkSubmissionProps(baseFlinkEnv, testVersion, testArtifactPrefix)

    props(FlinkMainJarURI) shouldBe s"$testArtifactPrefix/release/$testVersion/jars/flink_assembly_deploy.jar"
    props(FlinkCheckpointUri) shouldBe "abfss://checkpoints@account.dfs.core.windows.net/flink/checkpoints"
    props(EksServiceAccount) shouldBe "zipline-flink-sa"
    props(EksNamespace) shouldBe "zipline-flink"
  }

  it should "prefer constructor-level aksServiceAccount and aksNamespace over env vars" in {
    val submitter = createSubmitter(
      aksServiceAccount = Some("constructor-sa"),
      aksNamespace = Some("constructor-ns")
    )
    val env = baseFlinkEnv + ("FLINK_AKS_SERVICE_ACCOUNT" -> "env-sa", "FLINK_AKS_NAMESPACE" -> "env-ns")
    val props = submitter.buildFlinkSubmissionProps(env, testVersion, testArtifactPrefix)

    props(EksServiceAccount) shouldBe "constructor-sa"
    props(EksNamespace) shouldBe "constructor-ns"
  }

  it should "throw IllegalArgumentException when FLINK_STATE_URI is missing" in {
    val submitter = createSubmitter(aksServiceAccount = None, aksNamespace = None)
    val env = baseFlinkEnv - "FLINK_STATE_URI"

    intercept[IllegalArgumentException] {
      submitter.buildFlinkSubmissionProps(env, testVersion, testArtifactPrefix)
    }
  }

  it should "throw IllegalArgumentException when FLINK_AKS_SERVICE_ACCOUNT is missing and not in constructor" in {
    val submitter = createSubmitter(aksServiceAccount = None, aksNamespace = Some("ns"))
    val env = baseFlinkEnv - "FLINK_AKS_SERVICE_ACCOUNT"

    intercept[IllegalArgumentException] {
      submitter.buildFlinkSubmissionProps(env, testVersion, testArtifactPrefix)
    }
  }

  it should "throw IllegalArgumentException when FLINK_AKS_NAMESPACE is missing and not in constructor" in {
    val submitter = createSubmitter(aksServiceAccount = Some("sa"), aksNamespace = None)
    val env = baseFlinkEnv - "FLINK_AKS_NAMESPACE"

    intercept[IllegalArgumentException] {
      submitter.buildFlinkSubmissionProps(env, testVersion, testArtifactPrefix)
    }
  }
}
