package ai.chronon.api.submission

import ai.chronon.api
import ai.chronon.api.ScalaJavaConversions.MapOps
import ai.chronon.api.submission.JobSubmitterConstants.ConfTypeArgKeyword
import ai.chronon.api.submission.JobSubmitterConstants.LocalConfPathArgKeyword
import ai.chronon.api.submission.JobSubmitterConstants.OriginalModeArgKeyword
import ai.chronon.api.{JobStatusType, MetaData, ThriftJsonCodec}
import ai.chronon.api.thrift.TBase
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.ExecutionContext
import scala.reflect.ClassTag

sealed trait JobType
case object SparkJob extends JobType
case object FlinkJob extends JobType

trait JobSubmitter {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  def submit(jobType: JobType,
             submissionProperties: Map[String, String],
             jobProperties: Map[String, String],
             files: List[String],
             labels: Map[String, String],
             envVars: Map[String, String],
             args: String*): String

  /** Translates logical env vars into platform-specific Spark properties that actually
    * set the env in the driver/executors. Default mirrors the historical NodeSubmitter
    * expansion (driver on YARN, driver on K8s, executors); cloud submitters should
    * override to match their runtime (e.g. EMR Serverless uses spark.emr-serverless.driverEnv.*).
    */
  def envVarsToSparkProperties(env: Map[String, String]): Map[String, String] =
    env.flatMap { case (key, value) =>
      Seq(
        s"spark.executorEnv.$key" -> value,
        s"spark.yarn.appMasterEnv.$key" -> value,
        s"spark.kubernetes.driverEnv.$key" -> value
      )
    }

  def status(jobId: String): JobStatusType

  def kill(jobId: String): Unit

  // --- URL methods ---

  def getJobUrl(jobId: String): Option[String] = None

  def getSparkUrl(jobId: String): Option[String] = None

  def getFlinkUrl(jobId: String): Option[String] = None

  def getFlinkInternalJobId(jobId: String): Option[String] = None

  def getLatestCheckpointPath(flinkInternalJobId: String, flinkStateUri: String): Option[String] = None

  // --- Lifecycle methods ---

  def close(): Unit = {}

  // Returns true if a cluster must be created before submitting the job. False if the submitter can submit directly to
  // an existing cluster (e.g. EKS).
  def isClusterCreateNeeded(isLongRunning: Boolean): Boolean = true

  def ensureClusterReady(clusterName: String, clusterConf: Option[Map[String, String]])(implicit
      ec: ExecutionContext): Option[String] = Some(clusterName)

  // --- Platform hooks (override per cloud) ---

  /** Deprecated env var names for cluster name. Override per cloud for backwards compatibility.
    * NodeSubmitter checks these after SparkClusterNameEnvVar.
    */
  def deprecatedClusterNameEnvVars: Seq[String] = Seq.empty

  def jarName: String = ""
  def flinkJarName: String = "flink_assembly_deploy.jar"
  def onlineClass: String = ""
  def dqMetricsDataset: String = ""

  def resolveConfPath(stagedFileUri: String): String = stagedFileUri.split("/").last

  def kvStoreApiProperties: Map[String, String] = Map.empty

  /** Returns infra/cloud-specific submission properties for Flink jobs.
    * Values here are consumed by submitters in submit() and never passed to FlinkJob.main.
    * Override per cloud to populate FlinkMainJarURI, FlinkCheckpointUri, EKS/connector keys, etc.
    */
  def buildFlinkSubmissionProps(env: Map[String, String],
                                version: String,
                                artifactPrefix: String): Map[String, String] = Map.empty

  /** Key used in submissionProperties to pass the cluster identifier to submit().
    * Override per cloud: GCP uses ClusterName, AWS uses ClusterId.
    */
  def clusterIdentifierKey: String = JobSubmitterConstants.ClusterName
}

object JobSubmitter {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  // Used by submitters when a Flink job is RUNNING on the infra layer but the checkpoint
  // health check hasn't passed yet
  def flinkStatusWithGrace(
      jobId: String,
      submissionTime: Option[java.time.Instant],
      gracePeriod: java.time.Duration,
      logger: Logger
  ): JobStatusType = {
    val withinGrace = submissionTime.forall { t =>
      java.time.Instant.now().isBefore(t.plus(gracePeriod))
    }
    if (withinGrace) {
      logger.info(s"Flink job $jobId health check not yet passing — within grace period, staying PENDING")
      JobStatusType.PENDING
    } else {
      logger.warn(s"Flink job $jobId health check has not passed after $gracePeriod — marking FAILED")
      JobStatusType.FAILED
    }
  }

  def getApplicationArgs(jobType: JobType, args: Array[String]): Array[String] = {
    val userArgs = args.filter(arg => !JobSubmitterConstants.SharedInternalArgs.exists(arg.startsWith))
    jobType match {
      case FlinkJob =>
        userArgs.filter(arg => !arg.startsWith(JobSubmitterConstants.ConfTypeArgKeyword))
      case _ => userArgs
    }
  }

  def getArgValue(args: Array[String], argKeyword: String): Option[String] = {
    args
      .find(_.startsWith(s"$argKeyword="))
      .map(arg => arg.substring(argKeyword.length + 1))
  }

  def parseConf[T <: TBase[_, _]: Manifest: ClassTag](confPath: String): T =
    ThriftJsonCodec.fromJsonFile[T](confPath, check = false)

  def getMetadata(args: Array[String]): Option[MetaData] = {
    val localConfPathValue = getArgValue(args, LocalConfPathArgKeyword)
    val confTypeValue = getArgValue(args, ConfTypeArgKeyword)
    val originalMode = getArgValue(args, OriginalModeArgKeyword)

    if (localConfPathValue.isDefined) {
      val metadata = if (confTypeValue.isDefined) {
        confTypeValue.get match {
          case "joins"           => parseConf[api.Join](localConfPathValue.get).metaData
          case "group_bys"       => parseConf[api.GroupBy](localConfPathValue.get).metaData
          case "staging_queries" => parseConf[api.StagingQuery](localConfPathValue.get).metaData
          case "models"          => parseConf[api.Model](localConfPathValue.get).metaData
          case _ =>
            throw new IllegalArgumentException(
              s"Unable to retrieve object metadata due to invalid confType $confTypeValue"
            )
        }
      } else if (originalMode.isDefined && originalMode.get == "metastore") {
        // attempt to parse as a generic MetaData object
        parseConf[api.MetaData](localConfPathValue.get)
      } else {
        throw new IllegalArgumentException("Unable to retrieve object metadata")
      }

      Option(metadata)
    } else None
  }

  def getModeConfigProperties(args: Array[String]): Option[Map[String, String]] = {
    val maybeMetadata = getMetadata(args)
    val modeConfigProperties = if (maybeMetadata.isDefined) {
      val metadata = maybeMetadata.get

      val executionInfo = Option(metadata.getExecutionInfo)

      if (executionInfo.isEmpty) {
        None
      } else {
        val originalMode = getArgValue(args, OriginalModeArgKeyword)

        (Option(executionInfo.get.conf), originalMode) match {
          case (Some(conf), Some(mode)) =>
            val modeConfs = if (conf.isSetModeConfigs && conf.getModeConfigs.containsKey(mode)) {
              conf.getModeConfigs.get(mode).toScala
            } else if (conf.isSetCommon) {
              conf.getCommon.toScala
            } else {
              Map[String, String]()
            }
            Option(modeConfs)
          case _ => None
        }
      }
    } else None

    logger.info(s"Setting job properties: $modeConfigProperties")

    modeConfigProperties
  }

  def getClusterConfig(args: Array[String]): Option[Map[String, String]] = {
    val maybeMetadata = getMetadata(args)
    val clusterConfig = if (maybeMetadata.isDefined) {
      val metadata = maybeMetadata.get

      val executionInfo = Option(metadata.getExecutionInfo)

      if (executionInfo.isEmpty) {
        None
      } else {
        val originalMode = getArgValue(args, OriginalModeArgKeyword)

        (Option(executionInfo.get.clusterConf), originalMode) match {
          case (Some(clusterConf), Some(mode)) =>
            val modeConfig =
              if (clusterConf.isSetModeClusterConfigs && clusterConf.getModeClusterConfigs.containsKey(mode)) {
                clusterConf.getModeClusterConfigs.get(mode).toScala
              } else if (clusterConf.isSetCommon) {
                clusterConf.getCommon.toScala
              } else {
                Map[String, String]()
              }
            Option(modeConfig)
          case _ => None
        }
      }
    } else None
    clusterConfig
  }

}
