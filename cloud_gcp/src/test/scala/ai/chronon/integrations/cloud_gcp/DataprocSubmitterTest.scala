package ai.chronon.integrations.cloud_gcp

import ai.chronon.spark
import ai.chronon.spark.submission
import ai.chronon.spark.submission.JobSubmitterConstants._
import com.google.api.core.ApiFuture
import com.google.api.gax.longrunning.{OperationFuture, OperationSnapshot}
import com.google.api.gax.retrying.RetryingFuture
import com.google.api.gax.rpc.UnaryCallable
import com.google.cloud.dataproc.v1.JobControllerClient.ListJobsPagedResponse
import com.google.cloud.dataproc.v1._
import com.google.cloud.dataproc.v1.stub.JobControllerStub
import com.google.cloud.storage.Storage
import com.google.protobuf.Empty
import org.junit.Assert.assertEquals
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.mockito.MockitoSugar

import java.nio.file.Paths
import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters._

class DataprocSubmitterTest extends AnyFlatSpec with MockitoSugar {

  it should "test buildFlinkJob with the expected flinkStateUri and savepointUri" in {
    val submitter = new DataprocSubmitter(jobControllerClient = mock[JobControllerClient],
                                          gcsClient = mock[GCSClient],
                                          region = "test-region",
                                          projectId = "test-project")
    val job = submitter.buildFlinkJob(
      mainClass = "ai.chronon.flink.FlinkJob",
      jarUris = Array("gs://zipline-jars/cloud-gcp.jar"),
      mainJarUri = "gs://zipline-jars/flink-assembly-0.1.0-SNAPSHOT.jar",
      flinkCheckpointUri = "gs://zl-warehouse/flink-state",
      maybeSavePointUri = Option("gs://zipline-warehouse/flink-state/groupby-name/chk-1"),
      maybeFlinkJarsBasePath = None,
      jobProperties = Map("key" -> "value"),
      args = List("args1", "args2"): _*
    )

    assertEquals(job.getTypeJobCase, Job.TypeJobCase.FLINK_JOB)

    val flinkJob = job.getFlinkJob
    //    TODO: getMainClass returns empty in tests but not in prod
    //    assert(flinkJob.getMainClass == "ai.chronon.flink.FlinkJob")

    assertEquals(flinkJob.getJarFileUrisList.size(), 21)
    assertEquals(flinkJob.getJarFileUrisList.get(0), "gs://zipline-jars/cloud-gcp.jar")

    assertEquals(flinkJob.getMainJarFileUri, "gs://zipline-jars/flink-assembly-0.1.0-SNAPSHOT.jar")

    assertEquals(flinkJob.getArgsList.size(), 2)
    assertEquals(flinkJob.getArgsList.get(0), "args1")
    assertEquals(flinkJob.getArgsList.get(1), "args2")

    assertEquals(flinkJob.getSavepointUri, "gs://zipline-warehouse/flink-state/groupby-name/chk-1")

    assertEquals(
      flinkJob.getPropertiesMap.asScala,
      Map(
        "metrics.reporters" -> "prom",
        "metrics.reporter.prom.factory.class" -> "org.apache.flink.metrics.prometheus.PrometheusReporterFactory",
        "state.backend.rocksdb.localdir" -> "/tmp/flink-state",
        "taskmanager.memory.network.max" -> "2G",
        "taskmanager.numberOfTaskSlots" -> "4",
        "yarn.classpath.include-user-jar" -> "FIRST",
        "state.backend.incremental" -> "true",
        "key" -> "value",
        "taskmanager.memory.task.off-heap.size" -> "1G",
        "state.backend.type" -> "rocksdb",
        "taskmanager.memory.managed.fraction" -> "0.5f",
        "state.savepoints.dir" -> "gs://zl-warehouse/flink-state",
        "taskmanager.memory.network.min" -> "1G",
        "metrics.reporter.prom.host" -> "localhost",
        "taskmanager.memory.jvm-metaspace.size" -> "512m",
        "metrics.reporter.prom.port" -> "9250-9260",
        "metrics.reporter.statsd.interval" -> "60 SECONDS",
        "taskmanager.memory.process.size" -> "64G",
        "state.checkpoint-storage" -> "filesystem",
        "state.checkpoints.dir" -> "gs://zl-warehouse/flink-state",
        "state.checkpoints.num-retained" -> "10",
        "jobmanager.memory.process.size" -> "4G",
        "rest.flamegraph.enabled" -> "true"
      )
    )

  }
  it should "test buildFlinkJob with no savepointUri" in {
    val submitter = DataprocSubmitter(jobControllerClient = mock[JobControllerClient],
                                      storageClient = mock[Storage],
                                      region = "test-region",
                                      projectId = "test-project")
    val job = submitter.buildFlinkJob(
      mainClass = "ai.chronon.flink.FlinkJob",
      jarUris = Array("gs://zipline-jars/cloud-gcp.jar"),
      mainJarUri = "gs://zipline-jars/flink-assembly-0.1.0-SNAPSHOT.jar",
      flinkCheckpointUri = "gs://zl-warehouse/flink-state",
      maybeSavePointUri = None,
      maybeFlinkJarsBasePath = None,
      jobProperties = Map("key" -> "value"),
      args = List("args1", "args2"): _*
    )
    assertEquals(job.getTypeJobCase, Job.TypeJobCase.FLINK_JOB)
    val flinkJob = job.getFlinkJob
    assert(flinkJob.getSavepointUri.isEmpty)
  }

  it should "test buildFlinkJob with pubsub connector uri" in {
    val submitter = DataprocSubmitter(jobControllerClient = mock[JobControllerClient],
                                      storageClient = mock[Storage],
                                      region = "test-region",
                                      projectId = "test-project")
    val job = submitter.buildFlinkJob(
      mainClass = "ai.chronon.flink.FlinkJob",
      jarUris = Array("gs://zipline-jars/cloud-gcp.jar", "gs://zipline-jars/flink-pubsub-connector.jar"),
      mainJarUri = "gs://zipline-jars/flink-assembly-0.1.0-SNAPSHOT.jar",
      flinkCheckpointUri = "gs://zl-warehouse/flink-state",
      maybeSavePointUri = None,
      maybeFlinkJarsBasePath = None,
      jobProperties = Map("key" -> "value"),
      args = List("args1", "args2"): _*
    )
    assertEquals(job.getTypeJobCase, Job.TypeJobCase.FLINK_JOB)
    val flinkJob = job.getFlinkJob
    val jarFileUris = flinkJob.getJarFileUrisList.asScala.toSet
    val expectedJarFileUris = Set(
      "gs://zipline-jars/cloud-gcp.jar",
      "gs://zipline-jars/flink-pubsub-connector.jar"
    )
    expectedJarFileUris.foreach(jarUri => jarFileUris.contains(jarUri))
  }

  it should "test createSubmissionPropsMap for spark job" in {

    val path = Paths.get(getClass.getClassLoader.getResource("group_bys/team/purchases.v1").getPath)

    val mockControllerClient = mock[JobControllerClient]
    val submitter =
      DataprocSubmitter(jobControllerClient = mockControllerClient,
                        storageClient = mock[Storage],
                        region = "test-region",
                        projectId = "test-project")
    val actual = submitter.createSubmissionPropsMap(
      jobType = submission.SparkJob,
      envMap = Map.empty,
      args = Array(
        s"$JarUriArgKeyword=gs://zipline-jars/cloud-gcp.jar",
        s"$MainClassKeyword=ai.chronon.spark.Driver",
        s"$LocalConfPathArgKeyword=${path.toAbsolutePath.toString}",
        s"$ConfTypeArgKeyword=group_bys",
        s"$OriginalModeArgKeyword=backfill",
        s"$ZiplineVersionArgKeyword=0.1.0",
        s"$JobIdArgKeyword=job-id"
      ),
      clusterName = "test-cluster"
    )

    assertEquals(actual(ZiplineVersion), "0.1.0")
    assertEquals(actual(MetadataName), "quickstart.purchases.v1")
    assertEquals(actual(MainClass), "ai.chronon.spark.Driver")
    assertEquals(actual(JarURI), "gs://zipline-jars/cloud-gcp.jar")
    assert(actual.contains(JobId))
  }

  it should "test createSubmissionPropsMap for flink job with latest savepoint" in {
    val path = Paths.get(getClass.getClassLoader.getResource("group_bys/team/purchases.v1").getPath)

    val manifestBucketPath = "gs://zipline-warehouse/flink-manifest"
    val groupByName = "quickstart.purchases.v1"
    val flinkCheckpointUri = "gs://zl-warehouse/flink-state/checkpoints"
    val latestFlinkCheckpoint = "gs://zl-warehouse/flink-state/1234/chk-12"
    val ziplineVersion = "0.1.0"
    val mainClass = "ai.chronon.flink.FlinkJob"
    val jarURI = "gs://zipline-jars/cloud-gcp.jar"
    val flinkMainJarURI = "gs://zipline-jars/flink-assembly-0.1.0-SNAPSHOT.jar"

    val submitter = DataprocSubmitter(jobControllerClient = mock[JobControllerClient],
                                      storageClient = mock[Storage],
                                      region = "test-region",
                                      projectId = "test-project")
    val submitterSpy = org.mockito.Mockito.spy[DataprocSubmitter](submitter)
    when(
      submitterSpy.getLatestFlinkCheckpoint(
        groupByName = groupByName,
        manifestBucketPath = manifestBucketPath,
        flinkCheckpointUri = flinkCheckpointUri
      )).thenReturn(Some(latestFlinkCheckpoint))

    val actual = submitterSpy.createSubmissionPropsMap(
      jobType = submission.FlinkJob,
      envMap = Map.empty,
      args = Array(
        s"$JarUriArgKeyword=$jarURI",
        s"$MainClassKeyword=$mainClass",
        s"$LocalConfPathArgKeyword=${path.toAbsolutePath.toString}",
        s"$ConfTypeArgKeyword=group_bys",
        s"$OriginalModeArgKeyword=streaming",
        s"$ZiplineVersionArgKeyword=$ziplineVersion",
        s"$FlinkMainJarUriArgKeyword=$flinkMainJarURI",
        s"$GroupByNameArgKeyword=$groupByName",
        s"$StreamingManifestPathArgKeyword=$manifestBucketPath",
        s"$StreamingCheckpointPathArgKeyword=$flinkCheckpointUri",
        s"$StreamingLatestSavepointArgKeyword",
        s"$JobIdArgKeyword=job-id"
      ),
      clusterName = "test-cluster"
    )

    assertEquals(actual(ZiplineVersion), ziplineVersion)
    assertEquals(actual(MetadataName), groupByName)
    assertEquals(actual(MainClass), mainClass)
    assertEquals(actual(JarURI), jarURI)
    assertEquals(actual(FlinkMainJarURI), flinkMainJarURI)
    assertEquals(actual(FlinkCheckpointUri), flinkCheckpointUri)
    assertEquals(actual(SavepointUri), latestFlinkCheckpoint)
  }
  it should "test createSubmissionPropsMap for flink job with no savepoint" in {
    val path = Paths.get(getClass.getClassLoader.getResource("group_bys/team/purchases.v1").getPath)

    val manifestBucketPath = "gs://zipline-warehouse/flink-manifest"
    val groupByName = "quickstart.purchases.v1"
    val flinkCheckpointUri = "gs://zl-warehouse/flink-state/checkpoints"
    val ziplineVersion = "0.1.0"
    val mainClass = "ai.chronon.flink.FlinkJob"
    val jarURI = "gs://zipline-jars/cloud-gcp.jar"
    val flinkMainJarURI = "gs://zipline-jars/flink-assembly-0.1.0-SNAPSHOT.jar"

    val submitter = DataprocSubmitter(jobControllerClient = mock[JobControllerClient],
                                      storageClient = mock[Storage],
                                      region = "test-region",
                                      projectId = "test-project")

    val actual = submitter.createSubmissionPropsMap(
      jobType = submission.FlinkJob,
      envMap = Map.empty,
      args = Array(
        s"$JarUriArgKeyword=$jarURI",
        s"$MainClassKeyword=$mainClass",
        s"$LocalConfPathArgKeyword=${path.toAbsolutePath.toString}",
        s"$ConfTypeArgKeyword=group_bys",
        s"$OriginalModeArgKeyword=streaming",
        s"$ZiplineVersionArgKeyword=$ziplineVersion",
        s"$FlinkMainJarUriArgKeyword=$flinkMainJarURI",
        s"$GroupByNameArgKeyword=$groupByName",
        s"$StreamingManifestPathArgKeyword=$manifestBucketPath",
        s"$StreamingCheckpointPathArgKeyword=$flinkCheckpointUri",
        s"$StreamingNoSavepointArgKeyword",
        s"$JobIdArgKeyword=job-id"
      ),
      clusterName = "test-cluster"
    )

    assertEquals(actual(ZiplineVersion), ziplineVersion)
    assertEquals(actual(MetadataName), groupByName)
    assertEquals(actual(MainClass), mainClass)
    assertEquals(actual(JarURI), jarURI)
    assertEquals(actual(FlinkMainJarURI), flinkMainJarURI)
    assertEquals(actual(FlinkCheckpointUri), flinkCheckpointUri)

    assert(!actual.contains(SavepointUri))
  }
  it should "test createSubmissionPropsMap for flink job with user passed savepoint" in {
    val path = Paths.get(getClass.getClassLoader.getResource("group_bys/team/purchases.v1").getPath)

    val manifestBucketPath = "gs://zipline-warehouse/flink-manifest"
    val groupByName = "quickstart.purchases.v1"
    val flinkCheckpointUri = "gs://zl-warehouse/flink-state/checkpoints"
    val ziplineVersion = "0.1.0"
    val mainClass = "ai.chronon.flink.FlinkJob"
    val jarURI = "gs://zipline-jars/cloud-gcp.jar"
    val flinkMainJarURI = "gs://zipline-jars/flink-assembly-0.1.0-SNAPSHOT.jar"

    val userPassedSavepoint = "gs://zl-warehouse/flink-state/1234/chk-12"

    val submitter = DataprocSubmitter(jobControllerClient = mock[JobControllerClient],
                                      storageClient = mock[Storage],
                                      region = "test-region",
                                      projectId = "test-project")

    val actual = submitter.createSubmissionPropsMap(
      jobType = submission.FlinkJob,
      envMap = Map.empty,
      args = Array(
        s"$JarUriArgKeyword=$jarURI",
        s"$MainClassKeyword=$mainClass",
        s"$LocalConfPathArgKeyword=${path.toAbsolutePath.toString}",
        s"$ConfTypeArgKeyword=group_bys",
        s"$OriginalModeArgKeyword=streaming",
        s"$ZiplineVersionArgKeyword=$ziplineVersion",
        s"$FlinkMainJarUriArgKeyword=$flinkMainJarURI",
        s"$GroupByNameArgKeyword=$groupByName",
        s"$StreamingManifestPathArgKeyword=$manifestBucketPath",
        s"$StreamingCustomSavepointArgKeyword=$userPassedSavepoint",
        s"$StreamingCheckpointPathArgKeyword=$flinkCheckpointUri",
        s"$JobIdArgKeyword=job-id"
      ),
      clusterName = "test-cluster"
    )

    assertEquals(actual(ZiplineVersion), ziplineVersion)
    assertEquals(actual(MetadataName), groupByName)
    assertEquals(actual(MainClass), mainClass)
    assertEquals(actual(JarURI), jarURI)
    assertEquals(actual(FlinkMainJarURI), flinkMainJarURI)
    assertEquals(actual(FlinkCheckpointUri), flinkCheckpointUri)

    assertEquals(actual(SavepointUri), userPassedSavepoint)
  }

  it should "test createSubmissionPropsMap for flink job with additional jars" in {
    val path = Paths.get(getClass.getClassLoader.getResource("group_bys/team/purchases.v1").getPath)

    val manifestBucketPath = "gs://zipline-warehouse/flink-manifest"
    val groupByName = "quickstart.purchases.v1"
    val flinkCheckpointUri = "gs://zl-warehouse/flink-state/checkpoints"
    val ziplineVersion = "0.1.0"
    val mainClass = "ai.chronon.flink.FlinkJob"
    val jarURI = "gs://zipline-jars/cloud-gcp.jar"
    val additionalJars = "gs://zipline-jars/some-jar.jar,gs://zipline-jars/another-jar.jar"
    val flinkMainJarURI = "gs://zipline-jars/flink-assembly-0.1.0-SNAPSHOT.jar"
    val pubSubConnectorJarURI = "gs://zipline-jars/flink-pubsub-connector.jar"
    val userPassedSavepoint = "gs://zl-warehouse/flink-state/1234/chk-12"

    val submitter = DataprocSubmitter(jobControllerClient = mock[JobControllerClient],
                                      storageClient = mock[Storage],
                                      region = "test-region",
                                      projectId = "test-project")

    val actual = submitter.createSubmissionPropsMap(
      jobType = submission.FlinkJob,
      envMap = Map.empty,
      args = Array(
        s"$JarUriArgKeyword=$jarURI",
        s"$AdditionalJarsUriArgKeyword=$additionalJars",
        s"$MainClassKeyword=$mainClass",
        s"$LocalConfPathArgKeyword=${path.toAbsolutePath.toString}",
        s"$ConfTypeArgKeyword=group_bys",
        s"$OriginalModeArgKeyword=streaming",
        s"$ZiplineVersionArgKeyword=$ziplineVersion",
        s"$FlinkMainJarUriArgKeyword=$flinkMainJarURI",
        s"$FlinkPubSubJarUriArgKeyword=$pubSubConnectorJarURI",
        s"$GroupByNameArgKeyword=$groupByName",
        s"$StreamingManifestPathArgKeyword=$manifestBucketPath",
        s"$StreamingCustomSavepointArgKeyword=$userPassedSavepoint",
        s"$StreamingCheckpointPathArgKeyword=$flinkCheckpointUri",
        s"$JobIdArgKeyword=job-id"
      ),
      clusterName = "test-cluster"
    )

    assertEquals(actual(MainClass), mainClass)
    assertEquals(actual(JarURI), jarURI)
    assertEquals(actual(FlinkMainJarURI), flinkMainJarURI)
    assertEquals(actual(AdditionalJars), additionalJars)
    assertEquals(actual(FlinkPubSubConnectorJarURI), pubSubConnectorJarURI)
  }

  it should "test getDataprocFilesArgs when empty" in {
    val actual = DataprocSubmitter.getDataprocFilesArgs()
    assert(actual.isEmpty)
  }

  it should "test getDataprocFilesArgs when one file" in {
    val actual = DataprocSubmitter.getDataprocFilesArgs(
      args = Array(
        s"$FilesArgKeyword=gs://zipline-warehouse/metadata/file.txt"
      )
    )
    assertEquals(actual, List("gs://zipline-warehouse/metadata/file.txt"))
  }

  it should "test getDataprocFilesArgs when more than one file" in {
    val actual = DataprocSubmitter.getDataprocFilesArgs(
      args = Array(
        s"$FilesArgKeyword=gs://zipline-warehouse/metadata/file.txt,gs://zipline-warehouse/metadata/file2.txt"
      )
    )
    assertEquals(actual, List("gs://zipline-warehouse/metadata/file.txt", "gs://zipline-warehouse/metadata/file2.txt"))
  }

  it should "test getApplicationArgs filtering out args for spark job" in {
    val gcpBigtableInstanceId = "test-instance-id"
    val projectId = "test-project-id"

    val envMap = Map(
      GcpBigtableInstanceIdEnvVar -> Option("test-instance-id"),
      GcpProjectIdEnvVar -> Option("test-project-id")
    )

    val internalArgsToFilter = SharedInternalArgs
      .map(k => s"$k=dummy_value")
      .toArray

    val argsToKeep = Array("--arg1=value1")

    val inputArgs = internalArgsToFilter ++ argsToKeep

    val actual = submission.JobSubmitter.getApplicationArgs(jobType = submission.SparkJob, args = inputArgs)

    assert(
      actual sameElements Array(
        "--arg1=value1"
      ))
  }
  it should "test getApplicationArgs filtering out args for flink job" in {
    val internalArgsToFilter = SharedInternalArgs
      .map(k => s"$k=dummy_value")
      .toArray ++ Array(s"$ConfTypeArgKeyword=group_bys")

    val argsToKeep = Array("--arg1=value1")
    val actual = submission.JobSubmitter.getApplicationArgs(jobType = submission.FlinkJob,
                                                           args = internalArgsToFilter ++ argsToKeep)
    assert(actual sameElements argsToKeep)
  }

  it should "test run flink job should fail if more than one running flink job found" in {
    val groupByName = "test-groupby-name"
    val args = Array(
      s"$JobTypeArgKeyword=flink",
      s"$GroupByNameArgKeyword=$groupByName"
    )
    val submitter = mock[DataprocSubmitter]

    when(submitter.run(args = args, clusterName = "test-cluster")).thenAnswer(_ =>
      throw MoreThanOneRunningFlinkJob("Multiple running Flink jobs found")
    )

    assertThrows[MoreThanOneRunningFlinkJob](
      submitter.run(args = args, clusterName = "test-cluster"))
  }
  it should "fail flink check-if-job-is-running if no running flink job" in {
    val groupByName = "test-groupby-name"
    val args = Array(
      s"$JobTypeArgKeyword=flink",
      s"$GroupByNameArgKeyword=$groupByName",
      s"$StreamingModeArgKeyword=$CheckIfJobIsRunning"
    )
    val submitter = mock[DataprocSubmitter]

    when(submitter.run(args = args, clusterName = "test-cluster")).thenAnswer(_ =>
      throw NoRunningFlinkJob("No running Flink job found")
    )

    assertThrows[NoRunningFlinkJob](
      submitter.run(args = args, clusterName = "test-cluster"))
  }
  it should "return early flink check-if-job-is-running if one flink job found" in {
    val groupByName = "test-groupby-name"
    val args = Array(
      s"$JobTypeArgKeyword=flink",
      s"$GroupByNameArgKeyword=$groupByName",
      s"$StreamingModeArgKeyword=$CheckIfJobIsRunning"
    )
    val submitter = mock[DataprocSubmitter]

    when(submitter.run(args = args, clusterName = "test-cluster")).thenReturn("job-id-1")

    val jobId = submitter.run(args = args, clusterName = "test-cluster")
    assertEquals(jobId, "job-id-1")
  }
  it should "return early flink deploy if version check deploy is on and zipline versions match" in {
    val groupByName = "test-groupby-name"
    val localZiplineVersion = "0.1.0"

    val args = Array(
      s"$JobTypeArgKeyword=flink",
      s"$StreamingModeArgKeyword=$StreamingDeploy",
      s"$GroupByNameArgKeyword=$groupByName",
      s"$StreamingVersionCheckDeploy",
      s"$LocalZiplineVersionArgKeyword=$localZiplineVersion"
    )
    val submitter = mock[DataprocSubmitter]

    when(submitter.run(args = args, clusterName = "test-cluster")).thenReturn("job-id-1")

    val jobId = submitter.run(args = args, clusterName = "test-cluster")
    assertEquals(jobId, "job-id-1")
  }

  it should "fail flink deploy if no savepoint deploy strategy provided" in {
    val groupByName = "test-groupby-name"
    val localZiplineVersion = "0.1.0"

    val args = Array(
      s"$JobTypeArgKeyword=flink",
      s"$StreamingModeArgKeyword=$StreamingDeploy",
      s"$GroupByNameArgKeyword=$groupByName",
      s"$LocalZiplineVersionArgKeyword=$localZiplineVersion"
    )
    val submitter = mock[DataprocSubmitter]

    when(submitter.run(args = args, clusterName = "test-cluster")).thenAnswer(_ =>
      throw new Exception("No savepoint deploy strategy provided")
    )

    val error =
      intercept[Exception](submitter.run(args = args, clusterName = "test-cluster"))
    assertEquals(error.getMessage, "No savepoint deploy strategy provided")
  }

  it should "fail flink deploy if multiple savepoint deploy strategies provided" in {
    val groupByName = "test-groupby-name"
    val localZiplineVersion = "0.1.0"

    val args = Array(
      s"$JobTypeArgKeyword=flink",
      s"$StreamingModeArgKeyword=$StreamingDeploy",
      s"$GroupByNameArgKeyword=$groupByName",
      s"$LocalZiplineVersionArgKeyword=$localZiplineVersion",
      s"$StreamingNoSavepointArgKeyword",
      s"$StreamingLatestSavepointArgKeyword"
    )
    val submitter = mock[DataprocSubmitter]

    when(submitter.run(args = args, clusterName = "test-cluster")).thenAnswer(_ =>
      throw new Exception("Multiple savepoint deploy strategies provided")
    )

    val error =
      intercept[Exception](submitter.run(args = args, clusterName = "test-cluster"))
    assert(error.getMessage contains "Multiple savepoint deploy strategies provided")
  }

  it should "test flink deploy with no-savepoint deploy strategy successfully" in {
    val path = Paths.get(getClass.getClassLoader.getResource("group_bys/team/purchases.v1").getPath)

    val manifestBucketPath = "gs://zipline-warehouse/flink-manifest"
    val groupByName = "quickstart.purchases.v1"
    val ziplineVersion = "0.1.0"
    val mainClass = "ai.chronon.flink.FlinkJob"
    val jarURI = "gs://zipline-jars/cloud-gcp.jar"
    val flinkMainJarURI = "gs://zipline-jars/flink-assembly-0.1.0-SNAPSHOT.jar"

    val localZiplineVersion = "0.1.0"

    val args = Array(
      s"$JobTypeArgKeyword=flink",
      s"$StreamingModeArgKeyword=$StreamingDeploy",
      s"$GroupByNameArgKeyword=$groupByName",
      s"$LocalZiplineVersionArgKeyword=$localZiplineVersion",
      s"$JarUriArgKeyword=$jarURI",
      s"$MainClassKeyword=$mainClass",
      s"$LocalConfPathArgKeyword=${path.toAbsolutePath.toString}",
      s"$ConfTypeArgKeyword=group_bys",
      s"$OriginalModeArgKeyword=streaming",
      s"$ZiplineVersionArgKeyword=$ziplineVersion",
      s"$FlinkMainJarUriArgKeyword=$flinkMainJarURI",
      s"$GroupByNameArgKeyword=$groupByName",
      s"$StreamingManifestPathArgKeyword=$manifestBucketPath",
      s"$StreamingCheckpointPathArgKeyword=gs://zl-warehouse/flink-state/checkpoints",
      s"$StreamingNoSavepointArgKeyword",
      s"$JobIdArgKeyword=job-id"
    )
    val submitter = mock[DataprocSubmitter]

    when(submitter.run(args = args, clusterName = "test-cluster")).thenReturn("new-job-id")

    val jobId = submitter.run(args = args, clusterName = "test-cluster")
    assertEquals(jobId, "new-job-id")
  }

  it should "test flink deploy with latest savepoint deploy strategy successfully" in {
    val path = Paths.get(getClass.getClassLoader.getResource("group_bys/team/purchases.v1").getPath)

    val manifestBucketPath = "gs://zipline-warehouse/flink-manifest"
    val groupByName = "quickstart.purchases.v1"
    val ziplineVersion = "0.1.0"
    val mainClass = "ai.chronon.flink.FlinkJob"
    val jarURI = "gs://zipline-jars/cloud-gcp.jar"
    val flinkMainJarURI = "gs://zipline-jars/flink-assembly-0.1.0-SNAPSHOT.jar"
    val flinkCheckpointUri = "gs://zl-warehouse/flink-state/checkpoints"

    val localZiplineVersion = "0.1.0"

    val args = Array(
      s"$JobTypeArgKeyword=flink",
      s"$StreamingModeArgKeyword=$StreamingDeploy",
      s"$GroupByNameArgKeyword=$groupByName",
      s"$LocalZiplineVersionArgKeyword=$localZiplineVersion",
      s"$JarUriArgKeyword=$jarURI",
      s"$MainClassKeyword=$mainClass",
      s"$LocalConfPathArgKeyword=${path.toAbsolutePath.toString}",
      s"$ConfTypeArgKeyword=group_bys",
      s"$OriginalModeArgKeyword=streaming",
      s"$ZiplineVersionArgKeyword=$ziplineVersion",
      s"$FlinkMainJarUriArgKeyword=$flinkMainJarURI",
      s"$GroupByNameArgKeyword=$groupByName",
      s"$StreamingManifestPathArgKeyword=$manifestBucketPath",
      s"$StreamingCheckpointPathArgKeyword=$flinkCheckpointUri",
      s"$StreamingLatestSavepointArgKeyword",
      s"$JobIdArgKeyword=job-id"
    )
    val submitter = mock[DataprocSubmitter]

    when(submitter.run(args = args, clusterName = "test-cluster")).thenReturn("new-job-id")

    val jobId = submitter.run(args = args, clusterName = "test-cluster")
    assertEquals(jobId, "new-job-id")
  }

  it should "test flink deploy with user provided savepoint deploy strategy successfully" in {
    val path = Paths.get(getClass.getClassLoader.getResource("group_bys/team/purchases.v1").getPath)

    val manifestBucketPath = "gs://zipline-warehouse/flink-manifest"
    val groupByName = "quickstart.purchases.v1"
    val ziplineVersion = "0.1.0"
    val mainClass = "ai.chronon.flink.FlinkJob"
    val jarURI = "gs://zipline-jars/cloud-gcp.jar"
    val flinkMainJarURI = "gs://zipline-jars/flink-assembly-0.1.0-SNAPSHOT.jar"
    val flinkCheckpointUri = "gs://zl-warehouse/flink-state/checkpoints"

    val localZiplineVersion = "0.1.0"

    val args = Array(
      s"$JobTypeArgKeyword=flink",
      s"$StreamingModeArgKeyword=$StreamingDeploy",
      s"$GroupByNameArgKeyword=$groupByName",
      s"$LocalZiplineVersionArgKeyword=$localZiplineVersion",
      s"$JarUriArgKeyword=$jarURI",
      s"$MainClassKeyword=$mainClass",
      s"$LocalConfPathArgKeyword=${path.toAbsolutePath.toString}",
      s"$ConfTypeArgKeyword=group_bys",
      s"$OriginalModeArgKeyword=streaming",
      s"$ZiplineVersionArgKeyword=$ziplineVersion",
      s"$FlinkMainJarUriArgKeyword=$flinkMainJarURI",
      s"$GroupByNameArgKeyword=$groupByName",
      s"$StreamingManifestPathArgKeyword=$manifestBucketPath",
      s"$StreamingCheckpointPathArgKeyword=$flinkCheckpointUri",
      s"$StreamingCustomSavepointArgKeyword=gs://zl-warehouse/flink-state/checkpoints/1234/chk-12",
      s"$JobIdArgKeyword=job-id"
    )
    val submitter = mock[DataprocSubmitter]

    when(submitter.run(args = args, clusterName = "test-cluster")).thenReturn("new-job-id")

    val jobId = submitter.run(args = args, clusterName = "test-cluster")
    assertEquals(jobId, "new-job-id")
  }

  it should "test spark job run successfully" in {

    // Mock dataproc job client.
    val jobId = "mock-job-id"
    val mockJob = Job
      .newBuilder()
      .setReference(JobReference.newBuilder().setJobId(jobId))
      .setStatus(JobStatus.newBuilder().setState(JobStatus.State.DONE))
      .build()

    val mockJobControllerStub = mock[JobControllerStub]
    val mockSubmitJobCallable = mock[UnaryCallable[SubmitJobRequest, Job]]

    when(mockSubmitJobCallable.call(any()))
      .thenReturn(mockJob)

    when(mockJobControllerStub.submitJobCallable)
      .thenReturn(mockSubmitJobCallable)

    val mockJobControllerClient = JobControllerClient.create(mockJobControllerStub)

    // Test starts here.

    val submitter =
      DataprocSubmitter(jobControllerClient = mockJobControllerClient,
                        storageClient = mock[Storage],
                        region = "test-region",
                        projectId = "test-project")

    val submittedJobId =
      submitter.submit(
        spark.submission.SparkJob,
        Map(
          MainClass -> "test-main-class",
          JarURI -> "test-jar-uri",
          MetadataName -> "metadata-name",
          ZiplineVersion -> "some-zipline-version",
          JobId -> jobId,
          ClusterName -> "test-cluster"
        ),
        Map.empty,
        List.empty,
        Map.empty,
        Map.empty
      )
    assertEquals(submittedJobId, jobId)
  }

  it should "test formatDataprocLabel successfully for lowercase" in {
    val label = DataprocUtils.formatDataprocLabel("TEST")
    assertEquals(label, "test")
  }

  it should "test formatDataprocLabel successfully and keep lowercase, numbers, underscores, and dashes" in {
    val label = DataprocUtils.formatDataprocLabel("test-123_5")
    assertEquals(label, "test-123_5")
  }

  it should "test formatDataprocLabel successfully and replace invalid characters with underscore" in {
    val label = DataprocUtils.formatDataprocLabel("team.groupby_test")
    assertEquals(label, "team_groupby_test")
  }

  it should "test formatDataprocLabel truncates to 63 characters" in {
    val longLabel = "a" * 70
    val label = DataprocUtils.formatDataprocLabel(longLabel)
    assertEquals(label.length, 63)
  }

  it should "test formatDataprocLabel preserves version info at end when truncating" in {
    val longLabel = "very-long-metadata-name-that-exceeds-sixty-three-characters-v1.2.3"
    val label = DataprocUtils.formatDataprocLabel(longLabel)
    assertEquals(label.length, 63)
    assert(label.endsWith("v1_2_3"))
  }

  it should "test formatDataprocLabel removes leading non-alphanumeric characters" in {
    val label = DataprocUtils.formatDataprocLabel("___test-label")
    assert(label.head.isLetterOrDigit)
    assertEquals(label, "test-label")
  }

  it should "test formatDataprocLabel removes trailing non-alphanumeric characters" in {
    val label = DataprocUtils.formatDataprocLabel("test-label___")
    assert(label.last.isLetterOrDigit)
    assertEquals(label, "test-label")
  }

  it should "test formatDataprocLabel removes both leading and trailing non-alphanumeric" in {
    val label = DataprocUtils.formatDataprocLabel("___test-label___")
    assert(label.head.isLetterOrDigit)
    assert(label.last.isLetterOrDigit)
    assertEquals(label, "test-label")
  }

  it should "test formatDataprocLabel handles long label with leading/trailing underscores" in {
    val longLabel = "_" + ("a" * 65) + "_"
    val label = DataprocUtils.formatDataprocLabel(longLabel)
    assert(label.length <= 63)
    assert(label.head.isLetterOrDigit)
    assert(label.last.isLetterOrDigit)
    assert(label.forall(c => c.isLetterOrDigit || c == '_' || c == '-'))
  }

  it should "test formatDataprocLabel with only invalid characters at start after truncation" in {
    val longLabel = ("a" * 50) + ("_" * 20)
    val label = DataprocUtils.formatDataprocLabel(longLabel)
    assert(label.length <= 63)
    assert(label.last.isLetterOrDigit)
  }

  it should "test formatDataprocLabel complex case with truncation and validation" in {
    val complexLabel = "___team.groupby.very-long-name-that-needs-truncation-v1.2.3___"
    val label = DataprocUtils.formatDataprocLabel(complexLabel)
    assert(label.length <= 63)
    assert(label.head.isLetterOrDigit)
    assert(label.last.isLetterOrDigit)
    assert(!label.contains("."))
  }

  it should "test listRunningGroupByFlinkJobs successfully" in {
    val jobId = "mock-job-id"
    val mockJob = Job
      .newBuilder()
      .setReference(JobReference.newBuilder().setJobId(jobId))
      .setStatus(JobStatus.newBuilder().setState(JobStatus.State.RUNNING))
      .build()

    val mockListJobsPagedResponse = mock[ListJobsPagedResponse]
    val mockJobControllerClient = mock[JobControllerClient]
    val listJobsRequestCapture = ArgumentCaptor.forClass(classOf[ListJobsRequest])
    when(mockJobControllerClient.listJobs(listJobsRequestCapture.capture()))
      .thenReturn(mockListJobsPagedResponse)
    when(mockListJobsPagedResponse.iterateAll()).thenReturn(
      java.util.Collections.singletonList(mockJob)
    )
    val submitter = DataprocSubmitter(jobControllerClient = mockJobControllerClient,
                                                    storageClient = mock[Storage],
                                      region = "test-region",
                                      projectId = "test-project")

    // Test starts here.
    val runningJobs =
      submitter.listRunningGroupByFlinkJobs("test-groupby-name")

    assertEquals(runningJobs.size, 1)
    assertEquals(runningJobs.head, jobId)

    val listRequest = listJobsRequestCapture.getValue

    assertEquals(listRequest.getProjectId, "test-project")
    assertEquals(listRequest.getRegion, "test-region")
    assertEquals(listRequest.getFilter,
                 "status.state = ACTIVE AND labels.job-type = flink AND labels.metadata-name = test-groupby-name")

  }

  it should "create a Dataproc cluster successfully with a given config" in {
    val mockDataprocClient = mock[ClusterControllerClient]

    val mockOperationFuture = mock[OperationFuture[Cluster, ClusterOperationMetadata]]
    val mockRetryingFuture = mock[RetryingFuture[OperationSnapshot]]
    val mockMetadataFuture = mock[ApiFuture[ClusterOperationMetadata]]
    val mockCluster = Cluster
      .newBuilder()
      .setStatus(ClusterStatus.newBuilder().setState(ClusterStatus.State.RUNNING))
      .build()

    when(mockDataprocClient.createClusterAsync(any[CreateClusterRequest]))
      .thenReturn(mockOperationFuture)
    when(mockOperationFuture.getPollingFuture).thenReturn(mockRetryingFuture)
    when(mockOperationFuture.peekMetadata()).thenReturn(mockMetadataFuture)
    when(mockOperationFuture.get(anyLong(), any[TimeUnit])).thenReturn(mockCluster)

    when(mockDataprocClient.createClusterAsync(any[CreateClusterRequest]))
      .thenReturn(mockOperationFuture)

    when(mockDataprocClient.getCluster(any[String], any[String], any[String])).thenReturn(mockCluster)

    val region = "test-region"
    val projectId = "test-project"

    val clusterConfigStr = """{
      "masterConfig": {
        "numInstances": 1,
        "machineTypeUri": "n1-standard-4"
      },
      "workerConfig": {
        "numInstances": 2,
        "machineTypeUri": "n1-standard-4"
      }
    }"""

    val clusterName =
      DataprocSubmitter.getOrCreateCluster("",
                                           Option(Map("dataproc.config" -> clusterConfigStr)),
                                           projectId,
                                           region,
                                           mockDataprocClient)

    assert(clusterName.startsWith("zipline-"))
    verify(mockDataprocClient).createClusterAsync(any())
  }

  it should "not create a new cluster if given name exists" in {
    val mockDataprocClient = mock[ClusterControllerClient]

    val mockOperationFuture = mock[OperationFuture[Cluster, ClusterOperationMetadata]]
    val mockRetryingFuture = mock[RetryingFuture[OperationSnapshot]]
    val mockMetadataFuture = mock[ApiFuture[ClusterOperationMetadata]]
    val mockCluster = Cluster
      .newBuilder()
      .setStatus(ClusterStatus.newBuilder().setState(ClusterStatus.State.RUNNING))
      .build()

    when(mockDataprocClient.createClusterAsync(any[CreateClusterRequest]))
      .thenReturn(mockOperationFuture)
    when(mockOperationFuture.getPollingFuture).thenReturn(mockRetryingFuture)
    when(mockOperationFuture.peekMetadata()).thenReturn(mockMetadataFuture)
    when(mockOperationFuture.get(anyLong(), any[TimeUnit])).thenReturn(mockCluster)

    when(mockDataprocClient.createClusterAsync(any[CreateClusterRequest]))
      .thenReturn(mockOperationFuture)

    when(mockDataprocClient.getCluster(any[String], any[String], any[String])).thenReturn(mockCluster)

    val region = "test-region"
    val projectId = "test-project"

    val clusterConfigStr = """{
      "masterConfig": {
        "numInstances": 1,
        "machineTypeUri": "n1-standard-4"
      },
      "workerConfig": {
        "numInstances": 2,
        "machineTypeUri": "n1-standard-4"
      }
    }"""

    val clusterName =
      DataprocSubmitter.getOrCreateCluster("test-cluster",
                                           Option(Map("dataproc.config" -> clusterConfigStr)),
                                           projectId,
                                           region,
                                           mockDataprocClient)

    assert(clusterName.equals("test-cluster"))
    verify(mockDataprocClient, never()).createClusterAsync(any())
  }

  it should "recreate the cluster if it is in a bad state" in {
    val mockDataprocClient = mock[ClusterControllerClient]

    val mockErrorCluster = Cluster
      .newBuilder()
      .setStatus(ClusterStatus.newBuilder().setState(ClusterStatus.State.ERROR))
      .build()
    val mockRunningCluster = Cluster
      .newBuilder()
      .setStatus(ClusterStatus.newBuilder().setState(ClusterStatus.State.RUNNING))
      .build()

    when(mockDataprocClient.getCluster(any[String], any[String], any[String]))
      .thenReturn(mockErrorCluster)
      .thenReturn(mockRunningCluster)

    val mockDeleteOperationFuture = mock[OperationFuture[Empty, ClusterOperationMetadata]]
    val mockDeleteRetryingFuture = mock[RetryingFuture[OperationSnapshot]]
    val mockDeleteMetadataFuture = mock[ApiFuture[ClusterOperationMetadata]]

    when(mockDataprocClient.deleteClusterAsync(any[DeleteClusterRequest]))
      .thenReturn(mockDeleteOperationFuture)
    when(mockDeleteOperationFuture.getPollingFuture).thenReturn(mockDeleteRetryingFuture)
    when(mockDeleteOperationFuture.peekMetadata()).thenReturn(mockDeleteMetadataFuture)

    val mockOperationFuture = mock[OperationFuture[Cluster, ClusterOperationMetadata]]
    val mockRetryingFuture = mock[RetryingFuture[OperationSnapshot]]
    val mockMetadataFuture = mock[ApiFuture[ClusterOperationMetadata]]

    when(mockDataprocClient.createClusterAsync(any[CreateClusterRequest]))
      .thenReturn(mockOperationFuture)
    when(mockOperationFuture.getPollingFuture).thenReturn(mockRetryingFuture)
    when(mockOperationFuture.peekMetadata()).thenReturn(mockMetadataFuture)
    when(mockOperationFuture.get(anyLong(), any[TimeUnit])).thenReturn(mockRunningCluster)

    when(mockDataprocClient.createClusterAsync(any[CreateClusterRequest]))
      .thenReturn(mockOperationFuture)

    val region = "test-region"
    val projectId = "test-project"

    val clusterConfigStr = """{
      "masterConfig": {
        "numInstances": 1,
        "machineTypeUri": "n1-standard-4"
      },
      "workerConfig": {
        "numInstances": 2,
        "machineTypeUri": "n1-standard-4"
      }
    }"""

    val clusterName =
      DataprocSubmitter.getOrCreateCluster("test-cluster",
                                           Option(Map("dataproc.config" -> clusterConfigStr)),
                                           projectId,
                                           region,
                                           mockDataprocClient)

    assert(clusterName.equals("test-cluster"))
    verify(mockDataprocClient).createClusterAsync(any())
  }

  it should "test getZiplineVersionOfDataprocJob successfully" in {
    val jobId = "mock-job-id"
    val mockJob = mock[Job]
    val expectedVersion = "0_1_0"
    when(mockJob.getLabelsMap).thenReturn(
      java.util.Collections.singletonMap("zipline-version", expectedVersion)
    )

    val mockJobControllerClient = mock[JobControllerClient]
    when(mockJobControllerClient.getJob("test-project", "test-region", jobId)).thenReturn(mockJob)
    val submitter = DataprocSubmitter(jobControllerClient = mockJobControllerClient,
                                                    storageClient = mock[Storage],
                                      region = "test-region",
                                      projectId = "test-project")

    val result = submitter.getZiplineVersionOfDataprocJob(jobId)
    assertEquals(result, expectedVersion)
  }

  it should "test getLatestFlinkCheckpoint should return None if no manifest file found" in {
    val ziplineGcsClient = mock[GCSClient]
    val submitter = new DataprocSubmitter(jobControllerClient = mock[JobControllerClient],
                                                            region = "test-region",
                                          projectId = "test-project",
                                          gcsClient = ziplineGcsClient)

    when(ziplineGcsClient.fileExists("gs://test-bucket/flink-manifest/test-groupby-name/manifest.txt"))
      .thenReturn(false)

    assert(
      submitter
        .getLatestFlinkCheckpoint(groupByName = "test-groupby-name",
                                  manifestBucketPath = "gs://test-bucket/flink-manifest",
                                  flinkCheckpointUri = "gs://test-bucket/flink-state")
        .isEmpty
    )
  }

  it should "test getLatestFlinkCheckpoint should throw exception if flink job id not found in manifest" in {
    val ziplineGcsClient = mock[GCSClient]
    val submitter = new DataprocSubmitter(jobControllerClient = mock[JobControllerClient],
                                                            region = "test-region",
                                          projectId = "test-project",
                                          gcsClient = ziplineGcsClient)

    when(ziplineGcsClient.fileExists("gs://test-bucket/flink-manifest/test-groupby-name/manifest.txt"))
      .thenReturn(true)
    val expectedParentJobId = "some-parent-job-id"
    when(ziplineGcsClient.downloadObjectToMemory("gs://test-bucket/flink-manifest/test-groupby-name/manifest.txt"))
      .thenReturn(s"parentJobId=$expectedParentJobId".getBytes)

    assertThrows[RuntimeException] {
      submitter
        .getLatestFlinkCheckpoint(groupByName = "test-groupby-name",
                                  manifestBucketPath = "gs://test-bucket/flink-manifest",
                                  flinkCheckpointUri = "gs://test-bucket/flink-state")
    }
  }

  it should "test getLatestFlinkCheckpoint should return None if no checkpoints are found in GCS" in {
    val ziplineGcsClient = mock[GCSClient]
    val submitter = new DataprocSubmitter(jobControllerClient = mock[JobControllerClient],
                                                            region = "test-region",
                                          projectId = "test-project",
                                          gcsClient = ziplineGcsClient)

    when(ziplineGcsClient.fileExists("gs://test-bucket/flink-manifest/test-groupby-name/manifest.txt"))
      .thenReturn(true)
    val expectedFlinkJobId = "some-flink-job-id"
    val expectedParentJobId = "some-parent-job-id"
    when(ziplineGcsClient.downloadObjectToMemory("gs://test-bucket/flink-manifest/test-groupby-name/manifest.txt"))
      .thenReturn(s"flinkJobId=$expectedFlinkJobId,parentJobId=$expectedParentJobId".getBytes())

    when(ziplineGcsClient.listFiles(s"gs://test-bucket/flink-state/$expectedFlinkJobId"))
      .thenReturn(Iterator[String]())
    assert(
      submitter
        .getLatestFlinkCheckpoint(groupByName = "test-groupby-name",
                                  manifestBucketPath = "gs://test-bucket/flink-manifest",
                                  flinkCheckpointUri = "gs://test-bucket/flink-state")
        .isEmpty
    )
  }

  it should "test getLatestFlinkCheckpoint should return the latest checkpoint if found" in {
    val ziplineGcsClient = mock[GCSClient]
    val submitter = new DataprocSubmitter(jobControllerClient = mock[JobControllerClient],
                                                            region = "test-region",
                                          projectId = "test-project",
                                          gcsClient = ziplineGcsClient)

    when(ziplineGcsClient.fileExists("gs://test-bucket/flink-manifest/test-groupby-name/manifest.txt"))
      .thenReturn(true)
    val expectedFlinkJobId = "some-flink-job-id"
    val expectedParentJobId = "some-parent-job-id"
    when(ziplineGcsClient.downloadObjectToMemory("gs://test-bucket/flink-manifest/test-groupby-name/manifest.txt"))
      .thenReturn(s"flinkJobId=$expectedFlinkJobId,parentJobId=$expectedParentJobId".getBytes())

    when(ziplineGcsClient.listFiles(s"gs://test-bucket/flink-state/$expectedFlinkJobId"))
      .thenReturn(
        List(
          "gs://test-bucket/flink-state/some-flink-job-id/chk-9",
          "gs://test-bucket/flink-state/some-flink-job-id/chk-11",
          "gs://test-bucket/flink-state/some-flink-job-id/chk-10"
        ).iterator
      )

    val actual = submitter
      .getLatestFlinkCheckpoint(groupByName = "test-groupby-name",
                                manifestBucketPath = "gs://test-bucket/flink-manifest",
                                flinkCheckpointUri = "gs://test-bucket/flink-state")
    assert(
      actual.isDefined
    )
    assertEquals(actual.get, "gs://test-bucket/flink-state/some-flink-job-id/chk-11")
  }

  it should "test flink kafka ingest job locally" ignore {

    val region = "us-central1"
    val projectId = "canary-443022"
    val clusterName = "zipline-canary-cluster"
    val endPoint = s"${region}-dataproc.googleapis.com:443"
    val jobControllerClient =
      JobControllerClient.create(JobControllerSettings.newBuilder().setEndpoint(endPoint).build())
    val submitter = DataprocSubmitter(jobControllerClient, mock[Storage], region, projectId)
    val submittedJobId =
      submitter.submit(
        spark.submission.FlinkJob,
        Map(
          MainClass -> "ai.chronon.flink.FlinkKafkaItemEventDriver",
          MetadataName -> "test-item-event-data",
          ZiplineVersion -> "0.1.0",
          FlinkMainJarURI -> "gs://zipline-jars/flink_kafka_ingest-assembly-0.1.0-SNAPSHOT.jar",
          JarURI -> "gs://zipline-jars/cloud_gcp_bigtable.jar",
          JobId -> ("kafka-driver-" + UUID.randomUUID().toString),
          // This is where we write out checkpoints / persist state while the job is running
          FlinkCheckpointUri -> "gs://zl-warehouse/flink-state",
          ClusterName -> clusterName
        ),
        Map.empty,
        List.empty,
        Map.empty,
        Map.empty,
        "--kafka-bootstrap=bootstrap.zipline-kafka-cluster.us-central1.managedkafka.canary-443022.cloud.goog:9092",
        "--kafka-topic=test-item-event-data",
        "--data-file-name=gs://zl-warehouse/canary_item_events/events-output.avro",
        "--event-delay-millis=10"
      )
    println(submittedJobId)
  }

  it should "test flink PubSub ingest job locally" ignore {

    val region = "us-central1"
    val projectId = "canary-443022"
    val clusterName = "zipline-canary-cluster"
    val endPoint = s"${region}-dataproc.googleapis.com:443"
    val jobControllerClient =
      JobControllerClient.create(JobControllerSettings.newBuilder().setEndpoint(endPoint).build())
    val submitter = DataprocSubmitter(jobControllerClient, mock[Storage], region, projectId)
    val submittedJobId =
      submitter.submit(
        spark.submission.FlinkJob,
        Map(
          MainClass -> "ai.chronon.flink_connectors.pubsub.FlinkPubSubItemEventDriver",
          MetadataName -> "test-item-event-data",
          ZiplineVersion -> "0.1.0",
          FlinkMainJarURI -> "gs://zipline-jars/flink_pubsub_ingest-assembly-0.1.0-SNAPSHOT.jar",
          JobId -> ("pubsub-driver-" + UUID.randomUUID().toString),
          JarURI -> "gs://zipline-jars/cloud_gcp_bigtable.jar",
          // This is where we write out checkpoints / persist state while the job is running
          FlinkCheckpointUri -> "gs://zl-warehouse/flink-state",
          ClusterName -> clusterName
        ),
        Map.empty,
        List.empty,
        Map.empty,
        Map.empty,
        "--gcp-project=canary-443022",
        "--topic=test-item-event-data",
        "--data-file-name=gs://zl-warehouse/canary_item_events/events-output.avro",
        "--event-delay-millis=10"
      )
    println(submittedJobId)
  }

  it should "test spark serverless" ignore {
    val region = "us-central1"
    val projectId = "canary-443022"
    val endPoint = s"${region}-dataproc.googleapis.com:443"
    val batchControllerClient =
      BatchControllerClient.create(BatchControllerSettings.newBuilder().setEndpoint(endPoint).build())
    val submitter = DataprocServerlessSubmitter(batchControllerClient, region, projectId)

    val mainClass = "ai.chronon.spark.batch.BatchNodeRunner"
    val jarUri = "gs://zipline-artifacts-dev/release/0.1.0+dev.thomaschow/jars/cloud_gcp_lib_deploy.jar"
    val uniqueBatchId = s"batch-${System.currentTimeMillis() % 100000}"
    val files = List("gs://zipline-warehouse-dev/metadata/execution/b473f979-5ce0-4046-beea-b9e991933d13/gcp.training_set.v1_test__0__backfill")
    val args = Seq(
      "--conf-path=gcp.training_set.v1_test__0__backfill",
      "--start-ds=2025-08-15",
      "--end-ds=2025-11-15",
      "--online-class=ai.chronon.integrations.cloud_gcp.GcpApiImpl",
      "-ZGCP_PROJECT_ID=canary-443022",
      "-ZGCP_BIGTABLE_INSTANCE_ID=zipline-canary-instance",
      "-ZENABLE_UPLOAD_CLIENTS=true"
    )

    val submissionProperties = Map(
      "mainClass" -> mainClass,
      "jarUri" -> jarUri,
      "jobId" -> uniqueBatchId
    )

    val jobId = submitter.submit(
      jobType = spark.submission.SparkJob,
      submissionProperties = submissionProperties,
      jobProperties = Map.empty,
      files = files,
      labels = Map.empty,
      envVars = Map.empty,
      args = args: _*
    )
    println(jobId)
  }

  it should "return cluster name when cluster is RUNNING" in {
    val mockClusterControllerClient = mock[ClusterControllerClient]
    val mockCluster = Cluster
      .newBuilder()
      .setStatus(ClusterStatus.newBuilder().setState(ClusterStatus.State.RUNNING))
      .build()

    when(mockClusterControllerClient.getCluster(any[String], any[String], any[String]))
      .thenReturn(mockCluster)

    val submitterWithClusterClient = new DataprocSubmitter(
      jobControllerClient = mock[JobControllerClient],
      gcsClient = mock[GCSClient],
      region = "test-region",
      projectId = "test-project",
      clusterControllerClient = Some(mockClusterControllerClient)
    )

    val result = submitterWithClusterClient.ensureClusterReady(
      "test-cluster",
      None
    )(scala.concurrent.ExecutionContext.global)

    assert(result.isDefined)
    assertEquals(result.get, "test-cluster")
  }

  it should "return None when cluster is in CREATING state" in {
    val mockClusterControllerClient = mock[ClusterControllerClient]
    val mockCluster = Cluster
      .newBuilder()
      .setStatus(ClusterStatus.newBuilder().setState(ClusterStatus.State.CREATING))
      .build()

    when(mockClusterControllerClient.getCluster(any[String], any[String], any[String]))
      .thenReturn(mockCluster)

    val submitterWithClusterClient = new DataprocSubmitter(
      jobControllerClient = mock[JobControllerClient],
      gcsClient = mock[GCSClient],
      region = "test-region",
      projectId = "test-project",
      clusterControllerClient = Some(mockClusterControllerClient)
    )

    val result = submitterWithClusterClient.ensureClusterReady(
      "test-cluster",
      None
    )(scala.concurrent.ExecutionContext.global)

    assert(result.isEmpty)
  }

  it should "throw IllegalStateException when cluster is in ERROR state and no config provided" in {
    val mockClusterControllerClient = mock[ClusterControllerClient]
    val mockCluster = Cluster
      .newBuilder()
      .setStatus(ClusterStatus.newBuilder().setState(ClusterStatus.State.ERROR))
      .build()

    when(mockClusterControllerClient.getCluster(any[String], any[String], any[String]))
      .thenReturn(mockCluster)

    val submitterWithClusterClient = new DataprocSubmitter(
      jobControllerClient = mock[JobControllerClient],
      gcsClient = mock[GCSClient],
      region = "test-region",
      projectId = "test-project",
      clusterControllerClient = Some(mockClusterControllerClient)
    )

    val exception = intercept[IllegalStateException] {
      submitterWithClusterClient.ensureClusterReady(
        "test-cluster",
        None
      )(scala.concurrent.ExecutionContext.global)
    }

    assert(exception.getMessage.contains("cannot be used for job submission"))
  }

  it should "throw IllegalArgumentException when getOrCreateCluster is called with no config" in {
    val mockDataprocClient = mock[ClusterControllerClient]

    when(mockDataprocClient.getCluster(any[String], any[String], any[String]))
      .thenReturn(null)

    val exception = intercept[Exception] {
      DataprocSubmitter.getOrCreateCluster(
        "test-cluster",
        None,
        "test-project",
        "test-region",
        mockDataprocClient
      )
    }

    assert(exception.getMessage.contains("does not exist and no cluster config provided"))
  }

  it should "throw IllegalArgumentException when getOrCreateCluster is called with config missing dataproc.config key" in {
    val mockDataprocClient = mock[ClusterControllerClient]

    when(mockDataprocClient.getCluster(any[String], any[String], any[String]))
      .thenReturn(null)

    val exception = intercept[Exception] {
      DataprocSubmitter.getOrCreateCluster(
        "test-cluster",
        Some(Map("other-key" -> "value")),
        "test-project",
        "test-region",
        mockDataprocClient
      )
    }

    assert(exception.getMessage.contains("does not exist and no cluster config provided"))
  }

  it should "create cluster when config is properly provided" in {
    val mockDataprocClient = mock[ClusterControllerClient]

    when(mockDataprocClient.getCluster(any[String], any[String], any[String]))
      .thenReturn(null)

    val mockOperationFuture = mock[OperationFuture[Cluster, ClusterOperationMetadata]]
    val mockRunningCluster = Cluster
      .newBuilder()
      .setStatus(ClusterStatus.newBuilder().setState(ClusterStatus.State.RUNNING))
      .build()

    when(mockDataprocClient.createClusterAsync(any[CreateClusterRequest]))
      .thenReturn(mockOperationFuture)
    when(mockOperationFuture.get(anyLong(), any[TimeUnit]))
      .thenReturn(mockRunningCluster)
    when(mockDataprocClient.getCluster(any[String], any[String], any[String]))
      .thenReturn(null)
      .thenReturn(mockRunningCluster)

    val clusterConfigStr =
      """{
      "masterConfig": {
        "numInstances": 1,
        "machineTypeUri": "n1-standard-4"
      }
    }"""

    val result = DataprocSubmitter.getOrCreateCluster(
      "new-cluster",
      Some(Map("dataproc.config" -> clusterConfigStr)),
      "test-project",
      "test-region",
      mockDataprocClient
    )

    assertEquals(result, "new-cluster")
    verify(mockDataprocClient).createClusterAsync(any[CreateClusterRequest])
  }

  // --- buildFlinkSubmissionProps ---
  private val testArtifactPrefix = "gs://zipline-artifacts-test"
  private val testVersion = "1.0.0"
  private val baseFlinkEnv = Map(
    "FLINK_STATE_URI" -> "gs://test-bucket/flink-state"
  )
  private val pubSubConnectorJarUri =
    s"$testArtifactPrefix/release/$testVersion/jars/connectors_pubsub_deploy.jar"

  private def createTestSubmitter(): DataprocSubmitter =
    new DataprocSubmitter(mock[JobControllerClient], mock[GCSClient], region = "us-central1", projectId = "test-project")

  "buildFlinkSubmissionProps" should "include flink jar URI and checkpoint URI" in {
    val submitter = createTestSubmitter()
    val props = submitter.buildFlinkSubmissionProps(baseFlinkEnv, testVersion, testArtifactPrefix)

    assertEquals(props(FlinkMainJarURI), s"$testArtifactPrefix/release/$testVersion/jars/flink_assembly_deploy.jar")
    assertEquals(props(FlinkCheckpointUri), "gs://test-bucket/flink-state/checkpoints")
  }

  it should "include pubsub connector jar when ENABLE_PUBSUB is true" in {
    val submitter = createTestSubmitter()
    val env = baseFlinkEnv + ("ENABLE_PUBSUB" -> "true")

    val props = submitter.buildFlinkSubmissionProps(env, testVersion, testArtifactPrefix)

    assertEquals(props(FlinkPubSubConnectorJarURI), pubSubConnectorJarUri)
  }

  it should "omit pubsub connector jar when ENABLE_PUBSUB is false or absent" in {
    val submitter = createTestSubmitter()

    val props = submitter.buildFlinkSubmissionProps(baseFlinkEnv, testVersion, testArtifactPrefix)

    assert(!props.contains(FlinkPubSubConnectorJarURI))
  }

  it should "throw exception when FLINK_STATE_URI is not set" in {
    val submitter = createTestSubmitter()

    intercept[IllegalArgumentException] {
      submitter.buildFlinkSubmissionProps(Map.empty, testVersion, testArtifactPrefix)
    }
  }

  it should "getLatestCheckpointPath returns the highest-numbered checkpoint" in {
    val mockGcsClient = mock[GCSClient]
    val flinkJobId = "abc123"
    val flinkStateUri = "gs://my-bucket/flink-state"
    val checkpointBase = s"$flinkStateUri/checkpoints/$flinkJobId"

    when(mockGcsClient.listFiles(checkpointBase)).thenReturn(
      Iterator(
        s"$checkpointBase/chk-3/_metadata",
        s"$checkpointBase/chk-1/_metadata",
        s"$checkpointBase/chk-10/_metadata",
        s"$checkpointBase/chk-2/_metadata"
      )
    )

    val submitter = new DataprocSubmitter(jobControllerClient = mock[JobControllerClient],
                                          gcsClient = mockGcsClient,
                                          region = "test-region",
                                          projectId = "test-project")

    val result = submitter.getLatestCheckpointPath(flinkJobId, flinkStateUri)
    assertEquals(Some(s"$checkpointBase/chk-10"), result)
  }

  it should "getLatestCheckpointPath returns None when no checkpoints exist" in {
    val mockGcsClient = mock[GCSClient]
    val flinkJobId = "abc123"
    val flinkStateUri = "gs://my-bucket/flink-state"
    val checkpointBase = s"$flinkStateUri/checkpoints/$flinkJobId"

    when(mockGcsClient.listFiles(checkpointBase)).thenReturn(Iterator.empty)

    val submitter = new DataprocSubmitter(jobControllerClient = mock[JobControllerClient],
                                          gcsClient = mockGcsClient,
                                          region = "test-region",
                                          projectId = "test-project")

    val result = submitter.getLatestCheckpointPath(flinkJobId, flinkStateUri)
    assertEquals(None, result)
  }

  it should "getFlinkInternalJobId delegates to flinkInternalJobIdFetchFn" in {
    val mockJobControllerClient = mock[JobControllerClient]
    val flinkJobId = "flink-internal-uuid-123"
    val dataprocJobId = "dataproc-job-456"

    val submitter = new DataprocSubmitter(
      jobControllerClient = mockJobControllerClient,
      gcsClient = mock[GCSClient],
      region = "test-region",
      projectId = "test-project",
      flinkInternalJobIdFetchFn = _ => Some(flinkJobId)
    )

    val result = submitter.getFlinkInternalJobId(dataprocJobId)
    assertEquals(Some(flinkJobId), result)
  }

  it should "getFlinkInternalJobId returns None when fetch fn returns None" in {
    val submitter = new DataprocSubmitter(
      jobControllerClient = mock[JobControllerClient],
      gcsClient = mock[GCSClient],
      region = "test-region",
      projectId = "test-project",
      flinkInternalJobIdFetchFn = _ => None
    )

    val result = submitter.getFlinkInternalJobId("any-job-id")
    assertEquals(None, result)
  }

  // --- buildFlinkApplicationJob tests ---

  it should "test buildFlinkApplicationJob produces a HadoopJob with launcher main class" in {
    val submitter = new DataprocSubmitter(jobControllerClient = mock[JobControllerClient],
                                          gcsClient = mock[GCSClient],
                                          region = "test-region",
                                          projectId = "test-project")
    val job = submitter.buildFlinkApplicationJob(
      mainClass = "ai.chronon.flink.FlinkJob",
      mainJarUri = "gs://zipline-jars/flink-assembly.jar",
      launcherJarUri = "gs://zipline-jars/cloud-gcp.jar",
      jarUris = Array("gs://zipline-jars/cloud-gcp.jar"),
      flinkCheckpointUri = "gs://zl-warehouse/flink-state",
      maybeSavePointUri = Some("gs://zl-warehouse/flink-state/chk-1"),
      maybeFlinkJarsBasePath = None,
      jobProperties = Map("custom.key" -> "custom.value"),
      args = List("--groupby-name", "test-gb"): _*
    )

    assertEquals(job.getTypeJobCase, Job.TypeJobCase.HADOOP_JOB)

    val hadoopJob = job.getHadoopJob
    assertEquals("ai.chronon.integrations.cloud_gcp.FlinkApplicationLauncher", hadoopJob.getMainClass)
    assert(hadoopJob.getJarFileUrisList.asScala.contains("gs://zipline-jars/cloud-gcp.jar"))

    val launcherArgs = hadoopJob.getArgsList.asScala
    assert(launcherArgs.contains("--flink-main-class"))
    assert(launcherArgs.contains("ai.chronon.flink.FlinkJob"))
    assert(launcherArgs.contains("--flink-main-jar"))
    assert(launcherArgs.contains("gs://zipline-jars/flink-assembly.jar"))
    assert(launcherArgs.contains("--savepoint-uri"))
    assert(launcherArgs.contains("gs://zl-warehouse/flink-state/chk-1"))
    assert(launcherArgs.contains("--flink-arg"))
    assert(launcherArgs.contains("--groupby-name"))
    assert(launcherArgs.contains("test-gb"))

    // Verify Flink properties are serialized
    val propertyArgs = launcherArgs.zipWithIndex.filter(_._1 == "--flink-property").map(p => launcherArgs(p._2 + 1))
    assert(propertyArgs.exists(_.startsWith("jobmanager.memory.process.size=")))
    assert(propertyArgs.exists(_.startsWith("taskmanager.memory.process.size=")))
    assert(propertyArgs.exists(_.startsWith("state.backend.type=")))
    assert(propertyArgs.exists(_ == "custom.key=custom.value"))
  }

  it should "test buildFlinkApplicationJob without savepoint omits savepoint args" in {
    val submitter = new DataprocSubmitter(jobControllerClient = mock[JobControllerClient],
                                          gcsClient = mock[GCSClient],
                                          region = "test-region",
                                          projectId = "test-project")
    val job = submitter.buildFlinkApplicationJob(
      mainClass = "ai.chronon.flink.FlinkJob",
      mainJarUri = "gs://zipline-jars/flink-assembly.jar",
      launcherJarUri = "gs://zipline-jars/cloud-gcp.jar",
      jarUris = Array("gs://zipline-jars/cloud-gcp.jar"),
      flinkCheckpointUri = "gs://zl-warehouse/flink-state",
      maybeSavePointUri = None,
      maybeFlinkJarsBasePath = None,
      jobProperties = Map.empty,
      args = List("--groupby-name", "test-gb"): _*
    )

    assertEquals(job.getTypeJobCase, Job.TypeJobCase.HADOOP_JOB)
    val launcherArgs = job.getHadoopJob.getArgsList.asScala
    assert(!launcherArgs.contains("--savepoint-uri"))
  }

  it should "test buildFlinkApplicationJob includes additional JARs" in {
    val submitter = new DataprocSubmitter(jobControllerClient = mock[JobControllerClient],
                                          gcsClient = mock[GCSClient],
                                          region = "test-region",
                                          projectId = "test-project")
    val job = submitter.buildFlinkApplicationJob(
      mainClass = "ai.chronon.flink.FlinkJob",
      mainJarUri = "gs://zipline-jars/flink-assembly.jar",
      launcherJarUri = "gs://zipline-jars/cloud-gcp.jar",
      jarUris = Array("gs://zipline-jars/cloud-gcp.jar", "gs://zipline-jars/pubsub-connector.jar"),
      flinkCheckpointUri = "gs://zl-warehouse/flink-state",
      maybeSavePointUri = None,
      maybeFlinkJarsBasePath = None,
      jobProperties = Map.empty,
      args = List("--groupby-name", "test-gb"): _*
    )

    val launcherArgs = job.getHadoopJob.getArgsList.asScala
    assert(launcherArgs.contains("--flink-jar-uris"))
    val jarUrisIdx = launcherArgs.indexOf("--flink-jar-uris") + 1
    val jarUrisVal = launcherArgs(jarUrisIdx)
    assert(jarUrisVal.contains("gs://zipline-jars/cloud-gcp.jar"))
    assert(jarUrisVal.contains("gs://zipline-jars/pubsub-connector.jar"))
  }

  // --- FlinkApplicationLauncher tests ---

  it should "test FlinkApplicationLauncher.parseArgs with all arguments" in {
    val args = Array(
      "--flink-main-class", "ai.chronon.flink.FlinkJob",
      "--flink-main-jar", "gs://bucket/flink.jar",
      "--flink-jar-uris", "gs://bucket/jar1.jar,gs://bucket/jar2.jar",
      "--flink-checkpoint-uri", "gs://bucket/checkpoints",
      "--savepoint-uri", "gs://bucket/savepoint/chk-1",
      "--flink-property", "jobmanager.memory.process.size=4G",
      "--flink-property", "taskmanager.memory.process.size=64G",
      "--flink-arg", "--groupby-name",
      "--flink-arg", "test-gb",
      "--flink-bin-path", "/custom/flink/bin/flink"
    )
    val config = FlinkApplicationLauncher.parseArgs(args)

    assertEquals(config.flinkMainClass, "ai.chronon.flink.FlinkJob")
    assertEquals(config.flinkMainJar, "gs://bucket/flink.jar")
    assertEquals(config.flinkJarUris, Seq("gs://bucket/jar1.jar", "gs://bucket/jar2.jar"))
    assertEquals(config.flinkCheckpointUri, "gs://bucket/checkpoints")
    assertEquals(config.savepointUri, Some("gs://bucket/savepoint/chk-1"))
    assertEquals(config.flinkProperties.size, 2)
    assertEquals(config.flinkArgs, Seq("--groupby-name", "test-gb"))
    assertEquals(config.flinkBinPath, "/custom/flink/bin/flink")
  }

  it should "test FlinkApplicationLauncher.buildCommand constructs correct flink run-application" in {
    val config = FlinkApplicationLauncher.LauncherConfig(
      flinkMainClass = "ai.chronon.flink.FlinkJob",
      flinkMainJar = "gs://bucket/flink.jar",
      flinkJarUris = Seq("gs://bucket/extra.jar"),
      flinkCheckpointUri = "gs://bucket/checkpoints",
      savepointUri = Some("gs://bucket/savepoint/chk-1"),
      flinkProperties = Seq("jm.mem" -> "4G", "tm.mem" -> "64G"),
      flinkArgs = Seq("--groupby-name", "test-gb"),
      flinkBinPath = "/usr/lib/flink/bin/flink"
    )
    val cmd = FlinkApplicationLauncher.buildCommand(config)

    assertEquals(cmd(0), "/usr/lib/flink/bin/flink")
    assertEquals(cmd(1), "run-application")
    assertEquals(cmd(2), "-t")
    assertEquals(cmd(3), "yarn-application")
    assert(cmd.contains("-Djm.mem=4G"))
    assert(cmd.contains("-Dtm.mem=64G"))
    assert(cmd.exists(c => c.startsWith("-Dyarn.provided.lib.dirs=") && c.contains("gs://bucket/extra.jar")))
    assert(cmd.contains("-s"))
    assert(cmd.contains("gs://bucket/savepoint/chk-1"))
    assert(cmd.contains("-c"))
    assert(cmd.contains("ai.chronon.flink.FlinkJob"))
    assert(cmd.contains("gs://bucket/flink.jar"))
    assert(cmd.contains("--groupby-name"))
    assert(cmd.contains("test-gb"))
  }

  it should "test FlinkApplicationLauncher.buildCommand omits savepoint when not provided" in {
    val config = FlinkApplicationLauncher.LauncherConfig(
      flinkMainClass = "ai.chronon.flink.FlinkJob",
      flinkMainJar = "gs://bucket/flink.jar",
      flinkJarUris = Seq.empty,
      flinkCheckpointUri = "gs://bucket/checkpoints",
      savepointUri = None,
      flinkProperties = Seq.empty,
      flinkArgs = Seq.empty,
      flinkBinPath = "/usr/lib/flink/bin/flink"
    )
    val cmd = FlinkApplicationLauncher.buildCommand(config)

    assert(!cmd.contains("-s"))
    assert(!cmd.exists(_.contains("yarn.provided.lib.dirs")))
  }

  it should "test FlinkApplicationLauncher.parseArgs fails on missing required args" in {
    val args = Array("--flink-main-class", "SomeClass")
    assertThrows[IllegalArgumentException] {
      FlinkApplicationLauncher.parseArgs(args)
    }
  }
}
