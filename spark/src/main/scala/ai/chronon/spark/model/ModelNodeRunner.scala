package ai.chronon.spark.model

import ai.chronon.api._
import ai.chronon.api.Extensions.{MetadataOps, TableInfoOps}
import ai.chronon.api.planner.NodeRunner
import ai.chronon.online._
import ai.chronon.planner.{Node, NodeContent}
import ai.chronon.spark.submission.NodeConfReader
import org.rogach.scallop.ScallopConf
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

class ModelNodeRunner(api: Api) extends NodeRunner {

  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  override def run(metadata: MetaData, conf: NodeContent, range: Option[PartitionRange]): Unit = {
    conf.getSetField match {
      case NodeContent._Fields.CREATE_MODEL_ENDPOINT => doCreateEndpoint(conf)

      case NodeContent._Fields.DEPLOY_MODEL => doDeployModel(conf, range)

      case NodeContent._Fields.TRAIN_MODEL => doTrain(conf, range)

      case _ =>
        throw new IllegalArgumentException(
          "Expected CreateModelEndpointNode or DeployModelNode or TrainModelNode content but got: " + conf.getClass.getSimpleName)
    }
  }

  private def doTrain(conf: NodeContent, range: Option[PartitionRange]): Unit = {
    val model = conf.getTrainModel.model
    val modelName = model.metaData.name

    val startTime = System.currentTimeMillis()
    logger.info(s"Starting training for Model: $modelName")

    val date = range.map(_.end).getOrElse {
      throw new IllegalArgumentException("PartitionRange is required for model deployment (date)")
    }
    try {
      val modelPlatformProvider = api.generateModelPlatformProvider
      if (modelPlatformProvider == null) {
        throw new IllegalStateException("ModelPlatformProvider is not configured in the API")
      }
      val modelBackend = validateModelBackend(model)
      val modelPlatform = modelPlatformProvider.getPlatform(modelBackend, Map.empty)
      val submitTrainingRequest = TrainingRequest(trainingSource = model.trainingConf.trainingDataSource,
                                                  model = model,
                                                  date = date,
                                                  window = model.getTrainingConf.trainingDataWindow)
      val trainingJobName = Await.result(modelPlatform.submitTrainingJob(submitTrainingRequest), 10.minutes)

      // TODO: should hook this up with the orchestration's step checking instead of polling here
      val finalStatus = pollForJobCompletion(modelPlatform, SubmitTrainingJob, trainingJobName, 2.hours.toMillis)

      val duration = (System.currentTimeMillis() - startTime) / 1000
      if (finalStatus.jobStatusType == JobStatusType.SUCCEEDED) {
        logger.info(s"Successfully trained Model: $modelName (resource: $trainingJobName) in $duration seconds")
      } else {
        throw new RuntimeException(
          s"Training job $trainingJobName for Model: $modelName did not succeed. Final status: ${finalStatus.jobStatusType}. ${finalStatus.message}")
      }
    } catch {
      case e: Exception =>
        logger.error(s"Failed to train Model: $modelName", e)
        throw e
    }
  }

  private def doCreateEndpoint(conf: NodeContent): Unit = {
    val model = conf.getCreateModelEndpoint.model
    val modelName = model.metaData.name

    val startTime = System.currentTimeMillis()
    logger.info(s"Starting endpoint creation for Model: $modelName")

    try {
      val modelPlatformProvider = api.generateModelPlatformProvider
      if (modelPlatformProvider == null) {
        throw new IllegalStateException("ModelPlatformProvider is not configured in the API")
      }

      val modelBackend = validateModelBackend(model)
      val modelPlatform = modelPlatformProvider.getPlatform(modelBackend, Map.empty)

      val endpointConfig = model.getDeploymentConf.getEndpointConfig
      val endpointResourceName = Await.result(modelPlatform.createEndpoint(endpointConfig), 10.minutes)

      val duration = (System.currentTimeMillis() - startTime) / 1000
      logger.info(
        s"Successfully created endpoint for Model: $modelName (resource: $endpointResourceName) in $duration seconds")

    } catch {
      case e: Exception =>
        logger.error(s"Failed to create endpoint for Model: $modelName", e)
        throw e
    }
  }

  private def pollForJobCompletion(modelPlatform: ModelPlatform,
                                   jobType: ModelOperation,
                                   jobId: String,
                                   maxWaitTimeMs: Long): ModelJobStatus = {
    def isTerminalState(status: JobStatusType): Boolean = {
      status == JobStatusType.SUCCEEDED ||
      status == JobStatusType.FAILED ||
      status == JobStatusType.CANCELLED
    }

    val pollInterval = 30000 // 30 seconds
    var elapsedTime = 0L
    var currentState = JobStatusType.UNKNOWN

    while (!isTerminalState(currentState)) {
      if (elapsedTime > maxWaitTimeMs) {
        throw new RuntimeException(s"Timeout waiting for job: $jobId")
      }

      logger.info(s"Waiting for job. Current state: $currentState, elapsed: ${elapsedTime / 1000}s")
      Thread.sleep(pollInterval)
      elapsedTime += pollInterval

      val currentStatus = Await.result(modelPlatform.getJobStatus(jobType, jobId), 10.seconds)
      currentState = currentStatus.jobStatusType
    }

    val finalStatus = Await.result(modelPlatform.getJobStatus(jobType, jobId), 10.seconds)
    finalStatus
  }

  private def doDeployModel(conf: NodeContent, range: Option[PartitionRange]): Unit = {
    def isTerminalState(status: JobStatusType): Boolean = {
      status == JobStatusType.SUCCEEDED ||
      status == JobStatusType.FAILED ||
      status == JobStatusType.CANCELLED
    }

    val model = conf.getDeployModel.model
    val modelName = model.metaData.name
    val version = model.metaData.version

    val date = range.map(_.end).getOrElse {
      throw new IllegalArgumentException("PartitionRange is required for model deployment (date)")
    }

    val startTime = System.currentTimeMillis()
    logger.info(s"Starting model deployment for Model: $modelName, version: $version, date: $date")

    try {
      val modelPlatformProvider = api.generateModelPlatformProvider
      if (modelPlatformProvider == null) {
        throw new IllegalStateException("ModelPlatformProvider is not configured in the API")
      }

      val modelBackend = validateModelBackend(model)
      val modelPlatform = modelPlatformProvider.getPlatform(modelBackend, Map.empty)

      val deployRequest = DeployModelRequest(model = model, version = version, date = date)
      val deploymentId = Await.result(modelPlatform.deployModel(deployRequest), 5.minutes)

      // Wait for deployment to complete
      val maxWaitTime = 60 * 60 * 1000 // 60 minutes
      val pollInterval = 30000 // 30 seconds
      var elapsedTime = 0L
      var currentState = JobStatusType.UNKNOWN

      while (!isTerminalState(currentState)) {
        if (elapsedTime > maxWaitTime) {
          throw new RuntimeException(s"Timeout waiting for model deployment: $deploymentId")
        }

        logger.info(s"Waiting for deployment. Current state: $currentState, elapsed: ${elapsedTime / 1000}s")
        Thread.sleep(pollInterval)
        elapsedTime += pollInterval

        val currentStatus = Await.result(modelPlatform.getJobStatus(DeployModel, deploymentId), 10.seconds)
        currentState = currentStatus.jobStatusType
      }

      if (currentState == JobStatusType.FAILED) {
        throw new RuntimeException(s"Model deployment failed: $deploymentId")
      }

      val duration = (System.currentTimeMillis() - startTime) / 1000
      logger.info(
        s"Successfully deployed Model: $modelName, version: $version, deployment ID: $deploymentId in $duration seconds")

    } catch {
      case e: Exception =>
        logger.error(s"Failed to deploy Model: $modelName, version: $version", e)
        throw e
    }
  }

  private def validateModelBackend(model: Model) = {
    val modelBackend = model.getInferenceSpec.getModelBackend
    val backendModelType = Option(model.getInferenceSpec.getModelBackendParams)
      .map(m => {
        m.asScala.toMap
      })
      .getOrElse(Map.empty[String, String])
      .getOrElse("model_type", throw new IllegalArgumentException("model_type is required in modelBackendParams"))

    // model_type must be "custom" for model related runs
    require(backendModelType == "custom",
            s"model_type for endpoint creation must be 'custom', but got: $backendModelType")
    modelBackend
  }
}

object ModelNodeRunner {

  class ModelNodeRunnerArgs(args: Array[String]) extends ScallopConf(args) {
    val confPath = opt[String](required = true, descr = "Path to node configuration file")
    val endDs = opt[String](required = true, descr = "End date string (yyyy-MM-dd format)")
    val onlineClass = opt[String](required = true,
                                  descr =
                                    "Fully qualified Online.Api based class. We expect the jar to be on the class path")
    val apiProps: Map[String, String] = props[String]('Z', descr = "Props to configure API Store")
    verify()
  }

  def main(args: Array[String]): Unit = {
    try {
      val modelArgs = new ModelNodeRunnerArgs(args)

      val props = modelArgs.apiProps.map(identity)
      runFromArgs(modelArgs.confPath(), modelArgs.endDs(), modelArgs.onlineClass(), props) match {
        case Success(_) =>
          println("Model node runner completed successfully")
          System.exit(0)
        case Failure(exception) =>
          println("Model node runner failed", exception)
          System.exit(1)
      }
    } catch {
      case e: Exception =>
        println("Failed to parse arguments or initialize runner", e)
        System.exit(1)
    }
  }

  def instantiateApi(onlineClass: String, props: Map[String, String]): Api = {
    val cl = Thread.currentThread().getContextClassLoader
    val cls = cl.loadClass(onlineClass)
    val constructor = cls.getConstructors.apply(0)
    val onlineImpl = constructor.newInstance(props)
    onlineImpl.asInstanceOf[Api]
  }

  def runFromArgs(confPath: String, endDs: String, onlineClass: String, props: Map[String, String]): Try[Unit] = {
    Try {
      val node = NodeConfReader.read(confPath)
      val metadata = node.metaData

      val api = instantiateApi(onlineClass, props)

      implicit val partitionSpec: PartitionSpec = metadata.partitionSpec(PartitionSpec.daily)

      val range = Some(PartitionRange(null, endDs))
      val runner = new ModelNodeRunner(api)
      runner.run(metadata, node.content, range)
    }
  }
}
