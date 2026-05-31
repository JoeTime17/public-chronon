package ai.chronon.spark.submission

import ai.chronon.api.JobStatusType
import ai.chronon.spark.submission.JobSubmitterConstants._
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.mockito.MockitoSugar

class KyuubiSubmitterTest extends AnyFlatSpec with MockitoSugar {

  private def createMockClient(): KyuubiClient = {
    val mockClient = mock[KyuubiClient]
    when(mockClient.baseUrl).thenReturn("http://localhost:10099")
    mockClient
  }

  it should "submit a Spark job successfully" in {
    val mockClient = createMockClient()
    val submitter = KyuubiSubmitter(mockClient)

    val expectedResponse = BatchSubmitResponse(
      id = "batch-12345",
      user = Some("testuser"),
      batchType = "SPARK",
      name = Some("spark-test-job"),
      state = "PENDING",
      createTime = System.currentTimeMillis(),
      endTime = None
    )

    when(mockClient.submitBatch(any[BatchSubmitRequest])).thenReturn(expectedResponse)

    val jobId = submitter.submit(
      SparkJob,
      Map(
        MainClass -> "ai.chronon.spark.Driver",
        JarURI -> "s3://bucket/chronon.jar",
        MetadataName -> "test-job",
        ZiplineVersion -> "1.0.0",
        JobId -> "job-123"
      ),
      Map.empty,
      List.empty,
      Map.empty,
      Map.empty
    )

    assert(jobId == "batch-12345")
    verify(mockClient).submitBatch(any[BatchSubmitRequest])
  }

  it should "get job status correctly - RUNNING" in {
    val mockClient = createMockClient()
    val submitter = KyuubiSubmitter(mockClient)

    val statusResponse = BatchStatusResponse(
      id = "batch-12345",
      user = Some("testuser"),
      batchType = "SPARK",
      name = Some("test-job"),
      state = "RUNNING",
      createTime = System.currentTimeMillis(),
      endTime = None,
      appId = Some("application_123"),
      appUrl = Some("http://spark-ui"),
      appState = Some("RUNNING"),
      appDiagnostic = None
    )

    when(mockClient.getBatchStatus("batch-12345")).thenReturn(statusResponse)

    val status = submitter.status("batch-12345")
    assert(status == JobStatusType.RUNNING)
  }

  it should "get job status correctly - FINISHED" in {
    val mockClient = createMockClient()
    val submitter = KyuubiSubmitter(mockClient)

    val statusResponse = BatchStatusResponse(
      id = "batch-12345",
      user = Some("testuser"),
      batchType = "SPARK",
      name = Some("test-job"),
      state = "FINISHED",
      createTime = System.currentTimeMillis(),
      endTime = Some(System.currentTimeMillis()),
      appId = Some("application_123"),
      appUrl = Some("http://spark-ui"),
      appState = Some("FINISHED"),
      appDiagnostic = None
    )

    when(mockClient.getBatchStatus("batch-12345")).thenReturn(statusResponse)

    val status = submitter.status("batch-12345")
    assert(status == JobStatusType.SUCCEEDED)
  }

  it should "get job status correctly - ERROR" in {
    val mockClient = createMockClient()
    val submitter = KyuubiSubmitter(mockClient)

    val statusResponse = BatchStatusResponse(
      id = "batch-12345",
      user = Some("testuser"),
      batchType = "SPARK",
      name = Some("test-job"),
      state = "ERROR",
      createTime = System.currentTimeMillis(),
      endTime = Some(System.currentTimeMillis()),
      appId = Some("application_123"),
      appUrl = Some("http://spark-ui"),
      appState = Some("FAILED"),
      appDiagnostic = Some("Out of memory")
    )

    when(mockClient.getBatchStatus("batch-12345")).thenReturn(statusResponse)

    val status = submitter.status("batch-12345")
    assert(status == JobStatusType.FAILED)
  }

  it should "kill a job successfully" in {
    val mockClient = createMockClient()
    val submitter = KyuubiSubmitter(mockClient)

    doNothing().when(mockClient).deleteBatch("batch-12345")

    submitter.kill("batch-12345")

    verify(mockClient).deleteBatch("batch-12345")
  }

  it should "list running Flink jobs by groupBy name" in {
    val mockClient = createMockClient()
    val submitter = KyuubiSubmitter(mockClient)

    val listResponse = BatchListResponse(
      from = 0,
      total = 2,
      batches = Seq(
        BatchSummary("batch-1", Some("user"), "FLINK", Some("flink-test_groupby-123"), "RUNNING", 0),
        BatchSummary("batch-2", Some("user"), "FLINK", Some("flink-other_job-456"), "RUNNING", 0)
      )
    )

    // Use any() matchers for all parameters since Mockito has issues with Scala named params
    when(
      mockClient.listBatches(
        any[Option[String]](),
        any[Option[String]](),
        any[Option[String]](),
        any[Option[String]](),
        any[Int](),
        any[Int]()
      )
    ).thenReturn(listResponse)

    val jobs = submitter.listRunningGroupByFlinkJobs("test_groupby")

    assert(jobs.size == 1)
    assert(jobs.head == "batch-1")
  }

  it should "filter out internal args from application args" in {
    val args = Array(
      s"$JarUriArgKeyword=s3://bucket/jar.jar",
      s"$MainClassKeyword=com.example.Main",
      "--user-arg=value",
      s"$ZiplineVersionArgKeyword=1.0.0"
    )

    val filtered = JobSubmitter.getApplicationArgs(SparkJob, args)

    assert(filtered.length == 1)
    assert(filtered(0) == "--user-arg=value")
  }

  it should "parse files args correctly" in {
    val args = Array(
      s"$FilesArgKeyword=s3://bucket/file1.txt,s3://bucket/file2.txt"
    )

    val files = KyuubiSubmitter.getFilesArgs(args)

    assert(files.size == 2)
    assert(files.head == "s3://bucket/file1.txt")
    assert(files(1) == "s3://bucket/file2.txt")
  }

  it should "return empty list when no files args" in {
    val files = KyuubiSubmitter.getFilesArgs(Array.empty)
    assert(files.isEmpty)
  }

  it should "format labels correctly" in {
    val label = KyuubiUtils.formatLabel("Team.GroupBy_Test")
    assert(label == "team_groupby_test")
  }

  it should "generate correct job name" in {
    val name = KyuubiUtils.generateJobName("test.groupby.v1", SparkJob)
    assert(name == "spark-test_groupby_v1")
  }

  it should "convert JobType to batch type correctly" in {
    assert(KyuubiUtils.jobTypeToBatchType(SparkJob) == "SPARK")
    assert(KyuubiUtils.jobTypeToBatchType(FlinkJob) == "FLINK")
  }

  it should "create formatted labels from submission properties" in {
    val labels = KyuubiUtils.createFormattedLabels(
      SparkJob,
      Map(
        MetadataName -> "test.job",
        ZiplineVersion -> "1.0.0"
      ),
      Map("team" -> "ml-platform")
    )

    assert(labels("job-type") == "spark")
    assert(labels("metadata-name") == "test_job")
    assert(labels("zipline-version") == "1_0_0")
    assert(labels("team") == "ml-platform")
  }

  it should "return UNKNOWN status on API exception" in {
    val mockClient = createMockClient()
    val submitter = KyuubiSubmitter(mockClient)

    when(mockClient.getBatchStatus("batch-12345")).thenThrow(new KyuubiApiException("Connection failed", Some(500)))

    val status = submitter.status("batch-12345")
    assert(status == JobStatusType.UNKNOWN)
  }
}
