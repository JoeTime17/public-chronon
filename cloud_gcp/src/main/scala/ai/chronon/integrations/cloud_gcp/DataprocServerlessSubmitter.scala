package ai.chronon.integrations.cloud_gcp

import ai.chronon.api.JobStatusType
import ai.chronon.spark.submission.{JobSubmitter, JobType}
import com.google.cloud.dataproc.v1._
import ai.chronon.spark.submission.JobSubmitterConstants._
import com.google.api.gax.rpc.ApiException

import scala.jdk.CollectionConverters._

class DataprocServerlessSubmitter(batchControllerClient: BatchControllerClient,
                                  val region: String,
                                  val projectId: String)
    extends JobSubmitter {

  override def submit(jobType: JobType,
                      submissionProperties: Map[String, String],
                      jobProperties: Map[String, String],
                      files: List[String],
                      labels: Map[String, String],
                      envVars: Map[String, String],
                      args: String*): String = {
    val mainClass = submissionProperties.getOrElse(MainClass, throw new RuntimeException("Main class not found"))
    val jarUri = submissionProperties.getOrElse(JarURI, throw new RuntimeException("Jar URI not found"))
    val jobId = submissionProperties.getOrElse(JobId, throw new RuntimeException("No generated job id found"))

    // Format labels following Dataproc standards using shared utility
    val formattedDataprocLabels = DataprocUtils.createFormattedDataprocLabels(
      jobType = jobType,
      submissionProperties = submissionProperties,
      additionalLabels = labels
    )

    val effectiveJobProperties = jobProperties ++ envVarsToSparkProperties(envVars)
    val batch = buildBatch(mainClass, jarUri, files, effectiveJobProperties, formattedDataprocLabels, args: _*)
    val locationName = LocationName.of(projectId, region)

    try {
      val batchF = batchControllerClient.createBatchAsync(locationName, batch, jobId)
      val result = batchF.get
      logger.info(s"Batch created successfully: ${result.getName}")
      logger.info(s"Batch state: ${result.getState}")
      result.getUuid
    } catch {
      case ex: java.util.concurrent.ExecutionException =>
        logger.error(s"ExecutionException during batch creation", ex)
        throw new RuntimeException(s"Failed to create batch: ${ex.getMessage}", ex)
      case ex: ApiException =>
        logger.error(s"ApiException during batch creation", ex)
        throw new RuntimeException(s"Failed to create batch: ${ex.getMessage}", ex)
    }
  }

  override def status(jobId: String): JobStatusType = {
    try {
      val batchName = s"projects/$projectId/locations/$region/batches/$jobId"
      val batch = batchControllerClient.getBatch(batchName)
      batch.getState match {
        case Batch.State.PENDING    => JobStatusType.PENDING
        case Batch.State.RUNNING    => JobStatusType.RUNNING
        case Batch.State.CANCELLING => JobStatusType.FAILED
        case Batch.State.CANCELLED  => JobStatusType.FAILED
        case Batch.State.SUCCEEDED  => JobStatusType.SUCCEEDED
        case Batch.State.FAILED     => JobStatusType.FAILED
        case _                      => JobStatusType.UNKNOWN
      }
    } catch {
      case e: ApiException =>
        logger.error(s"Error getting batch status: ${e.getMessage}")
        JobStatusType.UNKNOWN
    }
  }

  override def kill(jobId: String): Unit = {
    try {
      val batchName = s"projects/$projectId/locations/$region/batches/$jobId"
      batchControllerClient.deleteBatch(batchName)
      logger.info(s"Batch $jobId deletion requested")
    } catch {
      case e: ApiException =>
        logger.error(s"Error deleting batch: ${e.getMessage}", e)
        throw new RuntimeException(s"Failed to delete batch: ${e.getMessage}", e)
    }
  }

  private def buildBatch(mainClass: String,
                         jarUri: String,
                         files: List[String],
                         jobProperties: Map[String, String],
                         labels: Map[String, String],
                         args: String*): Batch = {
    val sparkJob = SparkBatch
      .newBuilder()
      .setMainClass(mainClass)
      .addJarFileUris(jarUri)
      .addAllFileUris(files.asJava)
      .addAllArgs(args.toIterable.asJava)
      .build()

    val batchBuilder = Batch
      .newBuilder()
      .setSparkBatch(sparkJob)

    // Add RuntimeConfig only if we have job properties
    // Minimal Dataproc Serverless batches don't require RuntimeConfig
    val runtimeConfBuilder = RuntimeConfig
      .newBuilder()
      .setVersion("2.3")

    // Add PeripheralsConfig if needed. This should be set in EnvironmentConfig below.
//    val peripheralsConfig = PeripheralsConfig
//      .newBuilder()
//      .setSparkHistoryServerConfig(
//        SparkHistoryServerConfig
//          .newBuilder()
//          .setDataprocCluster("projects/canary-443022/regions/us-central1/clusters/zipline-canary-cluster")
//          .build()
//      )
//      .build()

    if (jobProperties.nonEmpty) {
      runtimeConfBuilder
        .putAllProperties(jobProperties.asJava)
    }
    batchBuilder.setRuntimeConfig(runtimeConfBuilder.build())

    // Add EnvironmentConfig with ExecutionConfig to specify service account
    val executionConfig = ExecutionConfig
      .newBuilder()
      .setServiceAccount(s"dataproc@${projectId}.iam.gserviceaccount.com")
      .build()

    val environmentConfig = EnvironmentConfig
      .newBuilder()
      .setExecutionConfig(executionConfig)
      // .setPeripheralsConfig(peripheralsConfig)
      .build()

    batchBuilder
      .setEnvironmentConfig(environmentConfig)
      .putAllLabels(labels.asJava)
      .build()
  }
}

object DataprocServerlessSubmitter {
  def apply(batchControllerClient: BatchControllerClient,
            region: String,
            projectId: String): DataprocServerlessSubmitter = {
    new DataprocServerlessSubmitter(batchControllerClient, region, projectId)
  }
}
