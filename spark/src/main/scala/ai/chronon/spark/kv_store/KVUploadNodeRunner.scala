package ai.chronon.spark.kv_store

import ai.chronon.api.Constants.MetadataDataset
import ai.chronon.api.Extensions.MetadataOps
import ai.chronon.api._
import ai.chronon.api.planner.NodeRunner
import ai.chronon.api.secrets.SecretResolver
import ai.chronon.online.Api
import ai.chronon.online.fetcher.{FetchContext, MetadataStore}
import ai.chronon.planner.{Node, NodeContent}
import org.rogach.scallop.ScallopConf
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success, Try}

class KVUploadNodeRunner(api: Api) extends NodeRunner {

  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  def createFetchContext(): FetchContext = {
    val kvStore = api.genKvStore
    kvStore.create(MetadataDataset)
    FetchContext(kvStore, MetadataDataset)
  }

  override def run(metadata: MetaData, conf: NodeContent, range: Option[PartitionRange]): Unit = {
    conf.getSetField match {
      case NodeContent._Fields.GROUP_BY_UPLOAD_TO_KV => doUploadtoKV(conf, range)

      case NodeContent._Fields.JOIN_METADATA_UPLOAD => doUploadJoinMetadata(conf)

      case NodeContent._Fields.MODEL_TRANSFORMS_UPLOAD => doUploadModelTransforms(conf)

      case _ =>
        throw new IllegalArgumentException(
          "Expected GroupByUploadToKVNode, JoinMetadataUpload, or ModelTransformsUpload content but got: " + conf.getClass.getSimpleName)
    }
  }

  private def doUploadJoinMetadata(conf: NodeContent): Unit = {
    val join = conf.getJoinMetadataUpload.join
    val joinName = join.metaData.name

    val startTime = System.currentTimeMillis()
    logger.info(s"Starting metadata upload for Join: $joinName")

    try {
      val fetchContext = createFetchContext()
      val metadataStore = new MetadataStore(fetchContext)
      val putRequest = metadataStore.putJoinConf(join)
      Await.result(putRequest, 1.hour)
      val duration = (System.currentTimeMillis() - startTime) / 1000
      logger.info(s"Successfully uploaded Join metadata for Join: $joinName in $duration seconds")

    } catch {
      case e: Exception =>
        logger.error(s"Failed to upload Join metadata for Join: $joinName", e)
        throw e
    }
  }

  private def doUploadModelTransforms(conf: NodeContent): Unit = {
    val modelTransforms = conf.getModelTransformsUpload.modelTransforms
    val modelTransformsName = modelTransforms.metaData.name

    val startTime = System.currentTimeMillis()
    logger.info(s"Starting metadata upload for ModelTransforms: $modelTransformsName")

    try {
      val fetchContext = createFetchContext()
      val metadataStore = new MetadataStore(fetchContext)
      val putRequest = metadataStore.putModelTransformsConf(modelTransforms)
      Await.result(putRequest, 1.hour)
      val duration = (System.currentTimeMillis() - startTime) / 1000
      logger.info(
        s"Successfully uploaded ModelTransforms metadata for ModelTransforms: $modelTransformsName in $duration seconds")

    } catch {
      case e: Exception =>
        logger.error(s"Failed to upload ModelTransforms metadata for ModelTransforms: $modelTransformsName", e)
        throw e
    }
  }

  private def doUploadtoKV(conf: NodeContent, range: Option[PartitionRange]): Unit = {
    val groupBy = conf.getGroupByUploadToKV.groupBy
    val offlineTable = groupBy.metaData.uploadTable
    val groupByName = groupBy.metaData.name

    val partitionString = range.map(_.end).getOrElse {
      throw new IllegalArgumentException("PartitionRange is required for KV upload")
    }

    val startTime = System.currentTimeMillis()
    logger.info(s"Starting KV upload for GroupBy: $groupByName, partition: $partitionString, table: $offlineTable")

    try {
      val kvStore = api.genKvStore
      kvStore.bulkPut(offlineTable, groupByName, partitionString)

      val duration = (System.currentTimeMillis() - startTime) / 1000
      logger.info(
        s"Successfully uploaded GroupBy data to KV store for GroupBy: $groupByName; partition: $partitionString in $duration seconds")

    } catch {
      case e: Exception =>
        logger.error(
          s"Failed to upload GroupBy: $groupByName for partition: $partitionString from table: $offlineTable",
          e)
        throw e
    }
  }

}

object KVUploadNodeRunner {

  class KVUploadNodeRunnerArgs(args: Array[String]) extends ScallopConf(args) {
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
      val kvArgs = new KVUploadNodeRunnerArgs(args)

      val resolvedEnv = SecretResolver.resolveVaultUris(sys.env.toMap)
      val driverSecrets = resolvedEnv -- sys.env.keySet
      val props = kvArgs.apiProps.map(identity) ++ driverSecrets
      runFromArgs(kvArgs.confPath(), kvArgs.endDs(), kvArgs.onlineClass(), props) match {
        case Success(_) =>
          println("KV upload node runner completed successfully")
          System.exit(0)
        case Failure(exception) =>
          println("KV upload node runner failed", exception)
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
      val node = ThriftJsonCodec.fromJsonFile[Node](confPath, check = false)
      val metadata = node.metaData

      // Merge Node's common conf into API props so config like upload location flows through.
      // Note: executionInfo is erased from the GroupBy inside node content, but preserved on node.metaData.
      val nodeCommonConf = metadata.commonConf
      val mergedProps = nodeCommonConf ++ props // CLI props take precedence

      val api = instantiateApi(onlineClass, mergedProps)

      implicit val partitionSpec: PartitionSpec = PartitionSpec.daily
      val range = Some(PartitionRange(null, endDs))

      val runner = new KVUploadNodeRunner(api)
      runner.run(metadata, node.content, range)
    }
  }
}
