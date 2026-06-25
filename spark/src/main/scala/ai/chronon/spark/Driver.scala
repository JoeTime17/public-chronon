/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.spark

import ai.chronon.api
import ai.chronon.api.{Constants, DateRange, PartitionRange, PartitionSpec, ThriftJsonCodec, TsUtils}
import ai.chronon.api.Constants.{KvTablePrefixArg, MetadataDataset}
import ai.chronon.api.Extensions.{GroupByOps, JoinPartOps, MetadataOps, SourceOps}
import ai.chronon.api.planner.RelevantLeftForJoinPart
import ai.chronon.api.thrift.TBase
import ai.chronon.online.{Api, MetadataDirWalker, MetadataEndPoint, TopicChecker}
import ai.chronon.online.fetcher.{ConfPathOrName, FetchContext, FetcherMain, MetadataStore}
import ai.chronon.planner.{JoinMergeNode, JoinPartNode, SourceWithFilterNode}
import ai.chronon.spark.batch._
import ai.chronon.spark.catalog.{Format, TableUtils}
import ai.chronon.spark.join.UnionJoin
import ai.chronon.spark.stats.{CompareBaseJob, CompareJob, ConsistencyJob}
import ai.chronon.spark.stats.drift.{Summarizer, SummaryPacker, SummaryUploader}
import ai.chronon.spark.streaming.JoinSourceRunner
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkFiles
import org.apache.spark.sql.{DataFrame, SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.functions
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener.{
  QueryProgressEvent,
  QueryStartedEvent,
  QueryTerminatedEvent
}
import org.rogach.scallop.{ScallopConf, ScallopOption, Subcommand}
import org.slf4j.{Logger, LoggerFactory}

import java.io.File
import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.DurationInt
import scala.reflect.ClassTag
import scala.reflect.internal.util.ScalaClassLoader

// useful to override spark.sql.extensions args - there is no good way to unset that conf apparently
// so we give it dummy extensions
class DummyExtensions extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {}
}

// The mega chronon cli
object Driver {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  def parseConf[T <: TBase[_, _]: Manifest: ClassTag](confPath: String): T =
    ThriftJsonCodec.fromJsonFile[T](confPath, check = false)

  trait SharedSubCommandArgs {
    this: ScallopConf =>
    val isGcp: ScallopOption[Boolean] =
      opt[Boolean](required = false, default = Some(false), descr = "Whether to use GCP")
    val gcpProjectId: ScallopOption[String] =
      opt[String](required = false, descr = "GCP project id")
    val gcpBigtableInstanceId: ScallopOption[String] =
      opt[String](required = false, descr = "GCP BigTable instance id")
    val enableUploadClients: ScallopOption[String] =
      opt[String](required = false, descr = "Enable creation of BigTable Admin and Bigquery clients for upload jobs")

    val confType: ScallopOption[String] =
      opt[String](required = false, descr = "Type of the conf to run. ex: joins, group_bys, models, staging_queries")
    val endDateInternal: ScallopOption[String] =
      opt[String](name = "end-date",
                  required = false,
                  descr = "End date to compute as of, start date is taken from conf.")
  }

  trait OfflineSubcommand extends SharedSubCommandArgs {
    this: ScallopConf =>
    val confPath: ScallopOption[String] = opt[String](required = false, descr = "Path to conf")

    val runFirstHole: ScallopOption[Boolean] =
      opt[Boolean](required = false,
                   default = Some(true),
                   descr = "Skip the first unfilled partition range if some future partitions have been populated.")

    val stepDays: ScallopOption[Int] =
      opt[Int](required = false,
               descr = "Runs offline backfill in steps, step-days at a time. Default is 30 days",
               default = None)

    def effectiveStepDays(metaData: api.MetaData, default: Int = 30): Option[Int] = {
      val cliStepDays = stepDays.toOption
      val confStepDays = Option(metaData.executionInfo)
        .filter(_.isSetStepDays)
        .map(_.stepDays)
      cliStepDays.orElse(confStepDays).orElse(Some(default))
    }

    val startPartition: ScallopOption[String] =
      opt[String](required = false, descr = "Start date to compute offline backfill.")

    val localTableMapping: Map[String, String] = propsLong[String](
      name = "local-table-mapping",
      keyName = "namespace.table",
      valueName = "path_to_local_input_file",
      descr = """Use this option to specify a list of table <> local input file mappings for running local
                |Chronon jobs. For example,
                |`--local-data-list ns_1.table_a=p1/p2/ta.csv ns_2.table_b=p3/tb.csv`
                |will load the two files into the specified tables `table_a` and `table_b` locally.
                |Once this option is used, the `--local-data-path` will be ignored.
                |""".stripMargin
    )
    val localDataPath: ScallopOption[String] =
      opt[String](
        required = false,
        default = None,
        descr =
          """Path to a folder containing csv data to load from. You can refer to these in Sources to run the backfill.
            |The name of each file should be in the format namespace.table.csv. They can be referred to in the confs
            |as "namespace.table". When namespace is not specified we will default to 'default'. We can also
            |auto-convert ts columns encoded as readable strings in the format 'yyyy-MM-dd HH:mm:ss'',
            |into longs values expected by Chronon automatically.
            |""".stripMargin
      )
    val localWarehouseLocation: ScallopOption[String] =
      opt[String](
        required = false,
        default = Some(System.getProperty("user.dir") + "/local_warehouse"),
        descr = "Directory to write locally loaded warehouse data into. This will contain unreadable parquet files"
      )

    lazy val sparkSession: SparkSession = buildSparkSession()

    def endDate(): String = endDateInternal.toOption.getOrElse(buildTableUtils().partitionSpec.now)

    def subcommandName(): String

    protected def isLocal: Boolean = localTableMapping.nonEmpty || localDataPath.isDefined

    protected def buildSparkSession(): SparkSession = {

      // We use the KryoSerializer for group bys and joins since we serialize the IRs.
      // But since staging query is fairly freeform, it's better to stick to the java serializer.
      val session =
        submission.SparkSessionBuilder.build(
          subcommandName(),
          local = isLocal,
          localWarehouseLocation = localWarehouseLocation.toOption,
          enforceKryoSerializer = !subcommandName().contains("staging_query")
        )
      if (localTableMapping.nonEmpty) {
        localTableMapping.foreach { case (table, filePath) =>
          val file = new File(filePath)
          LocalDataLoader.loadDataFileAsTable(file, session, table)
        }
      } else if (localDataPath.isDefined) {
        val dir = new File(localDataPath())
        assert(dir.exists, s"Provided local data path: ${localDataPath()} doesn't exist")
        LocalDataLoader.loadDataRecursively(dir, session)
      }
      session
    }

    def buildTableUtils(): TableUtils = {
      TableUtils(sparkSession)
    }
  }

  trait LocalExportTableAbility {
    this: ScallopConf with OfflineSubcommand =>

    val localTableExportPath: ScallopOption[String] =
      opt[String](
        required = false,
        default = None,
        descr =
          """Path to a folder for exporting all the tables generated during the run. This is only effective when local
            |input data is used: when either `local-table-mapping` or `local-data-path` is set. The name of the file
            |will be of format [<prefix>].<namespace>.<table_name>.<format>. For example: "default.test_table.csv" or
            |"local_prefix.some_namespace.another_table.parquet".
            |""".stripMargin
      )

    val localTableExportFormat: ScallopOption[String] =
      opt[String](
        required = false,
        default = Some("csv"),
        validate = (format: String) => LocalTableExporter.SupportedExportFormat.contains(format.toLowerCase),
        descr = "The table output format, supports csv(default), parquet, json."
      )

    val localTableExportPrefix: ScallopOption[String] =
      opt[String](
        required = false,
        default = None,
        descr = "The prefix to put in the exported file name."
      )

    protected def buildLocalTableExporter(tableUtils: TableUtils): LocalTableExporter =
      new LocalTableExporter(tableUtils,
                             localTableExportPath(),
                             localTableExportFormat(),
                             localTableExportPrefix.toOption)

    def shouldExport(): Boolean = isLocal && localTableExportPath.isDefined

    def exportTableToLocal(namespaceAndTable: String, tableUtils: TableUtils): Unit = {
      val isLocal = localTableMapping.nonEmpty || localDataPath.isDefined
      val shouldExport = localTableExportPath.isDefined
      if (!isLocal || !shouldExport) {
        return
      }

      buildLocalTableExporter(tableUtils).exportTable(namespaceAndTable)
    }
  }

  trait ResultValidationAbility {
    this: ScallopConf with OfflineSubcommand =>

    val expectedResultTable: ScallopOption[String] =
      opt[String](
        required = false,
        default = None,
        descr = """The name of the table containing expected result of a job.
                  |The table should have the exact schema of the output of the job""".stripMargin
      )

    def shouldPerformValidate(): Boolean = expectedResultTable.isDefined

    def validateResult(df: DataFrame, keys: Seq[String], tableUtils: TableUtils): Boolean = {
      val expectedDf = tableUtils.loadTable(expectedResultTable())
      val (_, _, metrics) = CompareBaseJob.compare(df, expectedDf, keys, tableUtils)
      val result = CompareJob.getConsolidatedData(metrics, tableUtils.partitionSpec)

      if (result.nonEmpty) {
        logger.info("[Validation] Failed. Please try exporting the result and investigate.")
        false
      } else {
        logger.info("[Validation] Success.")
        true
      }
    }
  }

  object JoinBackfill {
    @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
    class Args
        extends Subcommand("join")
        with OfflineSubcommand
        with LocalExportTableAbility
        with ResultValidationAbility {
      val selectedJoinParts: ScallopOption[List[String]] =
        opt[List[String]](required = false, descr = "A list of join parts that require backfilling.")
      val useCachedLeft: ScallopOption[Boolean] =
        opt[Boolean](
          required = false,
          default = Some(false),
          descr = "Whether or not to use the cached bootstrap table as the source - used in parallelized join flow.")
      lazy val joinConf: api.Join = parseConf[api.Join](confPath())
      override def subcommandName(): String = s"join_${joinConf.metaData.name}"
    }

    def run(args: Args): Unit = {
      val tableUtils = args.buildTableUtils()

      if (tableUtils.sparkSession.conf.get("spark.chronon.join.backfill.mode.skewFree", "false").toBoolean) {
        logger.info(s" >>> Running join backfill in skew free mode <<< ")
        val startPartition = args.startPartition.toOption.getOrElse(args.joinConf.left.query.startPartition)
        val endPartition = args.endDate()

        val joinName = args.joinConf.metaData.name
        val stepDays = args.effectiveStepDays(args.joinConf.metaData).get

        logger.info(
          s"Filling partitions for join:$joinName, partitions:[$startPartition, $endPartition], steps:$stepDays")

        val partitionRange = PartitionRange(startPartition, endPartition)(tableUtils.partitionSpec)
        val partitionSteps = partitionRange.stepsByDays(stepDays)

        partitionSteps.zipWithIndex.foreach { case (stepRange, idx) =>
          logger.info(s"Processing range $stepRange (${idx + 1}/${partitionSteps.length})")
          UnionJoin.computeJoinAndSave(args.joinConf, stepRange)(tableUtils)
          logger.info(s"Wrote range $stepRange (${idx + 1}/${partitionSteps.length})")
        }

        return
      }

      val join = new Join(
        args.joinConf,
        args.endDate(),
        tableUtils,
        !args.runFirstHole(),
        selectedJoinParts = args.selectedJoinParts.toOption
      )

      if (args.selectedJoinParts.isDefined) {
        val result =
          join.computeJoinOpt(args.effectiveStepDays(args.joinConf.metaData),
                              args.startPartition.toOption,
                              args.useCachedLeft.getOrElse(false))
        if (result.isDefined) {
          logger.info(
            s"Backfilling selected join parts: ${args.selectedJoinParts()} is complete. Skipping the final join. Exiting."
          )
        }
        return
      }

      val finalStepDays = args.effectiveStepDays(args.joinConf.metaData)

      val df = join.computeJoin(finalStepDays, args.startPartition.toOption)

      if (args.shouldExport()) {
        args.exportTableToLocal(args.joinConf.metaData.outputTable, tableUtils)
      }

      if (args.shouldPerformValidate()) {
        val keys = CompareJob.getJoinKeys(args.joinConf, tableUtils)
        args.validateResult(df, keys, tableUtils)
      }

      df.show(numRows = 3, truncate = 0, vertical = true)
      logger.info(
        s"\nShowing three rows of output above.\nQuery table `${args.joinConf.metaData.outputTable}` for more.\n")
    }
  }

  object JoinBackfillLeft {
    @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
    class Args
        extends Subcommand("join-left")
        with OfflineSubcommand
        with LocalExportTableAbility
        with ResultValidationAbility {
      lazy val joinConf: api.Join = parseConf[api.Join](confPath())
      override def subcommandName(): String = s"join_left_${joinConf.metaData.name}"
    }

    def run(args: Args): Unit = {
      args.buildTableUtils()
      val join = new Join(
        args.joinConf,
        args.endDate(),
        args.buildTableUtils(),
        !args.runFirstHole()
      )
      join.computeLeft(args.startPartition.toOption)
    }
  }

  object JoinBackfillFinal {
    @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
    class Args
        extends Subcommand("join-final")
        with OfflineSubcommand
        with LocalExportTableAbility
        with ResultValidationAbility {
      lazy val joinConf: api.Join = parseConf[api.Join](confPath())
      override def subcommandName(): String = s"join_final_${joinConf.metaData.name}"
    }

    def run(args: Args): Unit = {
      args.buildTableUtils()
      val join = new Join(
        args.joinConf,
        args.endDate(),
        args.buildTableUtils(),
        !args.runFirstHole()
      )
      join.computeFinal(args.startPartition.toOption)
    }
  }

  object GroupByBackfill {
    @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
    class Args
        extends Subcommand("group-by-backfill")
        with OfflineSubcommand
        with LocalExportTableAbility
        with ResultValidationAbility {
      lazy val groupByConf: api.GroupBy = parseConf[api.GroupBy](confPath())
      override def subcommandName(): String = s"groupBy_${groupByConf.metaData.name}_backfill"
    }

    def run(args: Args): Unit = {
      val tableUtils = args.buildTableUtils()
      val startPartition = args.startPartition.toOption.getOrElse(
        throw new IllegalArgumentException("--start-partition is required for group-by-backfill")
      )
      GroupBy.computeBackfill(
        args.groupByConf,
        startPartition,
        args.endDate(),
        tableUtils,
        args.effectiveStepDays(args.groupByConf.metaData),
        !args.runFirstHole()
      )

      if (args.shouldExport()) {
        args.exportTableToLocal(args.groupByConf.metaData.outputTable, tableUtils)
      }

      if (args.shouldPerformValidate()) {
        val df = tableUtils.loadTable(args.groupByConf.metaData.outputTable)
        args.validateResult(df, args.groupByConf.keys(tableUtils.partitionColumn).toSeq, tableUtils)
      }
    }
  }

  object Analyzer {
    class Args extends Subcommand("analyze") with OfflineSubcommand {
      val startDate: ScallopOption[String] =
        opt[String](required = false,
                    descr = "Finds skewed keys & time-distributions until a specified start date",
                    default = None)
      val skewKeyCount: ScallopOption[Int] =
        opt[Int](
          required = false,
          descr =
            "Finds the specified number of skewed keys. The larger this number is the more accurate the analysis will be.",
          default = Option(128)
        )
      val sample: ScallopOption[Double] =
        opt[Double](required = false,
                    descr = "Sampling ratio - what fraction of rows into incorporate into the skew key detection",
                    default = Option(0.1))
      val skewDetection: ScallopOption[Boolean] =
        opt[Boolean](
          required = false,
          descr =
            "finds skewed keys if true else will only output schema and exit. Skew detection will take longer time.",
          default = Some(false)
        )

      override def subcommandName() = "analyzer_util"
    }

    def run(args: Args): Unit = {
      val tableUtils = args.buildTableUtils()
      new Analyzer(
        tableUtils,
        args.confPath(),
        args.startDate.getOrElse(tableUtils.partitionSpec.at(TsUtils.daysAgoMillis(3))),
        args.endDate(),
        args.skewKeyCount(),
        args.sample(),
        args.skewDetection(),
        confType = Some(args.confType())
      ).run()
    }
  }

  object MetadataExport {
    class Args extends Subcommand("metadata-export") with OfflineSubcommand {
      val inputRootPath: ScallopOption[String] =
        opt[String](required = true, descr = "Base path of config repo to export from")
      val outputRootPath: ScallopOption[String] =
        opt[String](required = true, descr = "Base path to write output metadata files to")
      override def subcommandName() = "metadata-export"
    }

    def run(args: Args): Unit = {
      MetadataExporter.run(args.inputRootPath(), args.outputRootPath())
    }
  }

  object StagingQueryBackfill {
    class Args extends Subcommand("staging-query-backfill") with OfflineSubcommand with LocalExportTableAbility {
      val enableAutoExpand: ScallopOption[Boolean] =
        opt[Boolean](required = false,
                     descr = "Auto expand hive table if new columns added in staging query",
                     default = Option(true))

      lazy val stagingQueryConf: api.StagingQuery = parseConf[api.StagingQuery](confPath())
      override def subcommandName(): String = s"staging_query_${stagingQueryConf.metaData.name}_backfill"

      val forceOverwrite =
        opt[Boolean]("force-overwrite",
                     descr = "Setting to true overwrites any existing partitions.",
                     default = Some(false))

    }

    def run(args: Args): Unit = {
      val tableUtils = args.buildTableUtils()
      val stagingQueryJob = StagingQuery.from(args.stagingQueryConf, args.endDate(), tableUtils)
      stagingQueryJob.computeStagingQuery(
        args.effectiveStepDays(args.stagingQueryConf.metaData),
        args.enableAutoExpand.toOption,
        args.startPartition.toOption,
        !args.runFirstHole(),
        args.forceOverwrite()
      )

      if (args.shouldExport()) {
        args.exportTableToLocal(args.stagingQueryConf.metaData.outputTable, tableUtils)
      }
    }
  }

  object GroupByUploader {
    class Args extends Subcommand("group-by-upload") with OfflineSubcommand {
      override def subcommandName() = "group-by-upload"

      // jsonPercent
      val jsonPercent: ScallopOption[Int] =
        opt[Int](name = "json-percent",
                 required = false,
                 descr = "Percentage of json encoding to retain for debuggability",
                 default = Some(1))
    }

    def run(args: Args): Unit = {
      GroupByUpload.run(parseConf[api.GroupBy](args.confPath()),
                        args.endDate(),
                        Some(args.buildTableUtils()),
                        jsonPercent = args.jsonPercent.apply())
    }
  }

  object BuildComparisonTable {
    class Args extends Subcommand("build-comparison-table") with OfflineSubcommand {
      override def subcommandName() = "build-comparison-table"
    }

    def run(args: Args): Unit = {
      val joinConf = parseConf[api.Join](args.confPath())
      new ConsistencyJob(
        args.sparkSession,
        joinConf,
        args.endDate()
      ).buildComparisonTable()
    }
  }

  object ConsistencyMetricsCompute {
    class Args extends Subcommand("consistency-metrics-compute") with OfflineSubcommand {
      override def subcommandName() = "consistency-metrics-compute"
    }

    def run(args: Args): Unit = {
      val joinConf = parseConf[api.Join](args.confPath())
      new ConsistencyJob(
        args.sparkSession,
        joinConf,
        args.endDate()
      ).buildConsistencyMetrics()
    }
  }

  object CompareJoinQuery {
    class Args extends Subcommand("compare-join-query") with OfflineSubcommand {
      val queryConf: ScallopOption[String] =
        opt[String](required = true, descr = "Conf to the Staging Query to compare with")
      val startDate: ScallopOption[String] =
        opt[String](required = false, descr = "Partition start date to compare the data from")

      lazy val joinConf: api.Join = parseConf[api.Join](confPath())
      lazy val stagingQueryConf: api.StagingQuery = parseConf[api.StagingQuery](queryConf())
      override def subcommandName(): String =
        s"compare_join_query_${joinConf.metaData.name}_${stagingQueryConf.metaData.name}"
    }

    def run(args: Args): Unit = {
      assert(args.confPath().contains("/joins/"), "Conf should refer to the join path")
      assert(args.queryConf().contains("/staging_queries/"), "Compare path should refer to the staging query path")

      val tableUtils = args.buildTableUtils()
      new CompareJob(
        tableUtils,
        args.joinConf,
        args.stagingQueryConf,
        args.startDate.getOrElse(tableUtils.partitionSpec.at(System.currentTimeMillis())),
        args.endDate()
      ).run()
    }
  }

  // common arguments to all online commands
  trait OnlineSubcommand extends SharedSubCommandArgs { s: ScallopConf =>
    // this is `-Z` and not `-D` because sbt-pack plugin uses that for JAVA_OPTS
    val propsInner: Map[String, String] = props[String]('Z')
    val onlineJar: ScallopOption[String] =
      opt[String](required = true, descr = "Path to the jar contain the implementation of Online.Api class")
    val onlineClass: ScallopOption[String] =
      opt[String](required = true,
                  descr = "Fully qualified Online.Api based class. We expect the jar to be on the class path")

    // hashmap implements serializable
    def serializableProps: Map[String, String] = {
      val map = new mutable.HashMap[String, String]()
      propsInner.foreach { case (key, value) => map.update(key, value) }
      map.toMap
    }

    lazy private val gcpMap = Map(
      "GCP_PROJECT_ID" -> gcpProjectId.toOption.getOrElse(""),
      "GCP_BIGTABLE_INSTANCE_ID" -> gcpBigtableInstanceId.toOption.getOrElse(""),
      "ENABLE_UPLOAD_CLIENTS" -> enableUploadClients.toOption.getOrElse("true")
    )

    lazy val api: Api = isGcp.toOption match {
      case Some(true) => impl(serializableProps ++ gcpMap)
      case _          => impl(serializableProps)
    }

    lazy val fetchContext: FetchContext =
      FetchContext(api.genKvStore, MetadataDataset)
    def metaDataStore =
      new MetadataStore(fetchContext)

    def impl(props: Map[String, String]): Api = {
      val urls = Array(new File(onlineJar()).toURI.toURL)
      val cl = ScalaClassLoader.fromURLs(urls, this.getClass.getClassLoader)
      val cls = cl.loadClass(onlineClass())
      val constructor = cls.getConstructors.apply(0)
      val onlineImpl = constructor.newInstance(props)
      onlineImpl.asInstanceOf[Api]
    }
  }

  object FetcherCli {
    @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

    class Args extends Subcommand("fetch") with FetcherMain.FetcherArgs {}
    def run(args: Args): Unit = {
      FetcherMain.run(args)
    }
  }

  object MetadataUploader {
    @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
    class Args extends Subcommand("metadata-upload") with OnlineSubcommand {
      val confPath: ScallopOption[String] =
        opt[String](required = true, descr = "Path to the Chronon config file or directory")
    }

    def run(args: Args): Unit = {
      val acceptedEndPoints = List(MetadataEndPoint.ConfByKeyEndPointName)
      val dirWalker = new MetadataDirWalker(args.confPath(), acceptedEndPoints, maybeConfType = args.confType.toOption)
      val kvMap: Map[String, Map[String, List[String]]] = dirWalker.run

      // trigger creates of the datasets before we proceed with writes
      acceptedEndPoints.foreach(e => args.metaDataStore.create(e))

      val putRequestsSeq: Seq[Future[scala.Seq[Boolean]]] = kvMap.toSeq.map { case (endPoint, kvMap) =>
        args.metaDataStore.put(kvMap, endPoint)
      }
      val res = putRequestsSeq.flatMap(putRequests => Await.result(putRequests, 1.hour))
      logger.info(
        s"Uploaded Chronon Configs to the KV store, success count = ${res.count(v => v)}, failure count = ${res.count(!_)}")
    }
  }

  object GroupByUploadToKVBulkLoad {
    @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
    class Args extends Subcommand("group-by-upload-bulk-load") with OnlineSubcommand {
      // Expectation that run.py only sets confPath
      val confPath: ScallopOption[String] = opt[String](required = false, descr = "path to groupBy conf")

      def partitionString(): String = endDateInternal.getOrElse(throw new Exception("partition date is not provided!"))

      val uploader: ScallopOption[String] =
        opt[String](required = false,
                    default = Some("bigquery"),
                    descr = "uploader to use when load data to kv store, default is bigquery")

      // Override to add warehouse type to props
      override def serializableProps: Map[String, String] =
        super.serializableProps + ("UPLOADER" -> uploader().toLowerCase)
    }

    def run(args: Args): Unit = {
      val groupByConf = parseConf[api.GroupBy](args.confPath())
      val offlineTable = groupByConf.metaData.uploadTable
      val groupByName = groupByConf.metaData.name
      val startTime = System.currentTimeMillis()
      val uploader = args.uploader()

      logger.info(
        s"Triggering bulk load for GroupBy: ${groupByName} for partition: ${args.partitionString()} " +
          s"from table: ${offlineTable} using ${uploader}")

      val kvStore = args.api.genKvStore

      try {
        // The kvStore implementation will handle different warehouse types based on the configuration
        kvStore.bulkPut(offlineTable, groupByName, args.partitionString())
      } catch {
        case e: Exception =>
          logger.error(s"Failed to upload GroupBy: ${groupByName} for partition: ${args.partitionString()} " +
                         s"table: $offlineTable with $uploader",
                       e)
          throw e
      }

      logger.info(
        s"Uploaded GroupByUpload data to KV store for GroupBy: ${groupByName}; partition: " +
          s"${args.partitionString()} in ${(System.currentTimeMillis() - startTime) / 1000} seconds")
    }
  }

  object LogFlattener {
    class Args extends Subcommand("log-flattener") with OfflineSubcommand {
      val logTable: ScallopOption[String] =
        opt[String](required = true, descr = "Hive table with partitioned raw logs")

      val schemaTable: ScallopOption[String] =
        opt[String](required = true, descr = "Hive table with mapping from schema_hash to schema_value_last")
      lazy val joinConf: api.Join = parseConf[api.Join](confPath())
      override def subcommandName(): String = s"log_flattener_join_${joinConf.metaData.name}"
    }

    def run(args: Args): Unit = {
      val spark = args.sparkSession
      val logFlattenerJob = new LogFlattenerJob(
        spark,
        args.joinConf,
        args.endDate(),
        args.logTable(),
        args.schemaTable(),
        args.effectiveStepDays(args.joinConf.metaData)
      )
      logFlattenerJob.buildLogTable(args.startPartition.toOption)
    }
  }

  object GroupByStreaming {
    @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
    def dataStream(session: SparkSession, host: String, topic: String): DataFrame = {
      TopicChecker.topicShouldExist(topic, host)
      session.streams.addListener(new StreamingQueryListener() {
        override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
          logger.info("Query started: " + queryStarted.id)
        }
        override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
          logger.info("Query terminated: " + queryTerminated.id)
        }
        override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
          logger.info("Query made progress: " + queryProgress.progress)
        }
      })
      session.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", host)
        .option("subscribe", topic)
        .option("enable.auto.commit", "true")
        .load()
        .selectExpr("value")
    }

    class Args extends Subcommand("group-by-streaming") with OnlineSubcommand {
      @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
      val confPath: ScallopOption[String] = opt[String](required = true, descr = "path to groupBy conf")
      val DEFAULT_LAG_MILLIS = 2000 // 2seconds
      val kafkaBootstrap: ScallopOption[String] =
        opt[String](required = false, descr = "host:port of a kafka bootstrap server")
      val mockWrites: ScallopOption[Boolean] = opt[Boolean](required = false,
                                                            default = Some(false),
                                                            descr =
                                                              "flag - to ignore writing to the underlying kv store")
      val debug: ScallopOption[Boolean] = opt[Boolean](
        required = false,
        default = Some(false),
        descr = "Prints details of data flowing through the streaming job, skip writing to kv store")
      val lagMillis: ScallopOption[Int] = opt[Int](
        required = false,
        default = Some(DEFAULT_LAG_MILLIS),
        descr = "Lag time for chaining, before fetching upstream join results, in milliseconds. Default 2 seconds"
      )
      def parseConf[T <: TBase[_, _]: Manifest: ClassTag]: T =
        ThriftJsonCodec.fromJsonFile[T](confPath(), check = false)
    }

    def findFile(path: String): Option[String] = {
      val tail = path.split("/").last
      val possiblePaths = Seq(path, tail, SparkFiles.get(tail))
      val statuses = possiblePaths.map(p => p -> new File(p).exists())

      val messages = statuses.map { case (file, present) =>
        val suffix = if (present) {
          val fileSize = Files.size(Paths.get(file))
          s"exists ${FileUtils.byteCountToDisplaySize(fileSize)}"
        } else {
          "is not found"
        }
        s"$file $suffix"
      }
      logger.info(s"File Statuses:\n  ${messages.mkString("\n  ")}")
      statuses.find(_._2 == true).map(_._1)
    }

    def run(args: Args): Unit = {
      // session needs to be initialized before we can call find file.
      implicit val session: SparkSession = submission.SparkSessionBuilder.buildStreaming(args.debug())

      val confFile = findFile(args.confPath())
      val groupByConf = confFile
        .map(ThriftJsonCodec.fromJsonFile[api.GroupBy](_, check = false))
        .getOrElse(args.metaDataStore.getConf[api.GroupBy](ConfPathOrName(confPath = Some(args.confPath()))).get)

      val onlineJar = findFile(args.onlineJar())
      if (args.debug())
        onlineJar.foreach(session.sparkContext.addJar)
      implicit val apiImpl = args.impl(args.serializableProps)
      val query = if (groupByConf.streamingSource.get.isSetJoinSource) {
        new JoinSourceRunner(groupByConf,
                             args.serializableProps,
                             args.debug(),
                             args.lagMillis.getOrElse(2000)).chainedStreamingQuery.start()
      } else {
        val streamingSource = groupByConf.streamingSource
        assert(streamingSource.isDefined,
               "There is no valid streaming source - with a valid topic, and endDate < today")
        lazy val host = streamingSource.get.topicTokens.get("host")
        lazy val port = streamingSource.get.topicTokens.get("port")
        if (!args.kafkaBootstrap.isDefined)
          assert(
            host.isDefined && port.isDefined,
            "Either specify a kafkaBootstrap url or provide host and port in your topic definition as topic/host=host/port=port")
        val inputStream: DataFrame =
          dataStream(session, args.kafkaBootstrap.getOrElse(s"${host.get}:${port.get}"), streamingSource.get.cleanTopic)
        new streaming.GroupBy(inputStream, session, groupByConf, args.impl(args.serializableProps), args.debug()).run()
      }
      query.awaitTermination()
    }
  }

  object SourceJobRun {
    @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
    class Args
        extends Subcommand("source-job")
        with OfflineSubcommand
        with LocalExportTableAbility
        with ResultValidationAbility {
      lazy val joinConf: api.Join = parseConf[api.Join](confPath())
      override def subcommandName(): String = s"source_job_${joinConf.metaData.name}"
    }

    def run(args: Args): Unit = {
      import ai.chronon.planner.NodeContent
      implicit val tableUtils: TableUtils = args.buildTableUtils()
      val join = args.joinConf

      // Create a SourceWithFilterNode from the join's left source
      val source = join.left
      val outputTable = JoinUtils.computeLeftSourceTableName(join)

      // Create a SourceWithFilterNode with the extracted information
      val sourceWithFilterNode = new SourceWithFilterNode()
      sourceWithFilterNode.setSource(source)
      sourceWithFilterNode.setExcludeKeys(join.skewKeys)

      // Set the metadata
      val sourceOutputTable = JoinUtils.computeFullLeftSourceTableName(join)
      println(s"Source output table: $sourceOutputTable")

      // Split the output table to get namespace and name
      val sourceParts = sourceOutputTable.split("\\.", 2)
      val sourceNamespace = sourceParts(0)
      val sourceName = sourceParts(1)

      // Create metadata for source job
      val sourceMetaData = new api.MetaData()
        .setName(sourceName)
        .setOutputNamespace(sourceNamespace)
        .setTableProperties(join.metaData.tableProperties)

      // Calculate the date range
      val endDate = args.endDate()
      val startDate: String = args.startPartition.getOrElse(args.endDate())
      val dateRange = new DateRange()
        .setStartDate(startDate)
        .setEndDate(endDate)

      // Run the SourceJob
      val sourceJob = new SourceJob(sourceWithFilterNode, sourceMetaData, dateRange)(tableUtils)
      sourceJob.run()

      logger.info(s"SourceJob completed. Output table: ${outputTable}")

      if (args.shouldExport()) {
        args.exportTableToLocal(outputTable, tableUtils)
      }
    }
  }

  object JoinPartJobRun {
    @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
    class Args
        extends Subcommand("join-part-job")
        with OfflineSubcommand
        with LocalExportTableAbility
        with ResultValidationAbility {

      val joinPartName: ScallopOption[String] =
        opt[String](required = true, descr = "Name of the join part to run")

      lazy val joinConf: api.Join = parseConf[api.Join](confPath())
      override def subcommandName(): String = s"join_part_job_${joinConf.metaData.name}"
    }

    def run(args: Args): Unit = {
      implicit val tableUtils: TableUtils = args.buildTableUtils()

      val join = args.joinConf
      val joinPartName = args.joinPartName()

      // Find the selected join part
      val joinPart = join.joinParts.asScala
        .find(part => part.fullPrefix == joinPartName)
        .getOrElse(
          throw new RuntimeException(s"JoinPart with name $joinPartName not found in join ${join.metaData.name}"))

      logger.info(s"Found join part: ${joinPart.fullPrefix}")

      // Create a JoinPartNode from the join part
      val joinPartNode = new JoinPartNode()
        .setJoinPart(joinPart)
        .setLeftSourceTable(JoinUtils.computeFullLeftSourceTableName(join))
        .setLeftDataModel(join.left.dataModel)
        .setSkewKeys(join.skewKeys)

      // Set the metadata
      val joinPartTableName = RelevantLeftForJoinPart.partTableName(join, joinPart)
      val outputNamespace = join.metaData.outputNamespace
      val metadata = new ai.chronon.api.MetaData()
        .setName(joinPartTableName)
        .setOutputNamespace(outputNamespace)

      // Calculate the date range
      val endDate = args.endDate()
      val startDate = args.startPartition.getOrElse(args.endDate())
      val dateRange = new DateRange()
        .setStartDate(startDate)
        .setEndDate(endDate)

      // Run the JoinPartJob
      val joinPartJob = new JoinPartJob(joinPartNode, metadata, dateRange, showDf = false)(tableUtils)
      joinPartJob.run()

      logger.info(s"JoinPartJob completed. Output table: ${metadata.outputTable}")

      if (args.shouldExport()) {
        args.exportTableToLocal(metadata.outputTable, tableUtils)
      }
    }
  }

  object MergeJobRun {
    @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
    class Args
        extends Subcommand("merge-job")
        with OfflineSubcommand
        with LocalExportTableAbility
        with ResultValidationAbility {
      lazy val joinConf: api.Join = parseConf[api.Join](confPath())
      override def subcommandName(): String = s"merge_job_${joinConf.metaData.name}"
    }

    def run(args: Args): Unit = {
      val tableUtils = args.buildTableUtils()
      val joinConf = args.joinConf

      // TODO -- when we support bootstrapping in the modular flow from Driver, we'll need to omit
      // Bootstrapped JoinParts here
      val allJoinParts = joinConf.joinParts.asScala.toSeq

      val endDate = args.endDate()
      val startDate = args.startPartition.getOrElse(endDate)
      val dateRange = new DateRange()
        .setStartDate(startDate)
        .setEndDate(endDate)

      // Create metadata for merge job
      val mergeMetaData = new api.MetaData()
        .setName(joinConf.metaData.name)
        .setOutputNamespace(joinConf.metaData.outputNamespace)

      val mergeNode = new JoinMergeNode()
        .setJoin(joinConf)

      val mergeJob = new MergeJob(mergeNode, mergeMetaData, dateRange, allJoinParts)(tableUtils)

      mergeJob.run()

      logger.info(s"MergeJob completed. Output table: ${joinConf.metaData.outputTable}")
      if (args.shouldExport()) {
        args.exportTableToLocal(joinConf.metaData.outputTable, tableUtils)
      }
    }
  }

  object CompareTablesGeneric {
    @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

    class Args extends Subcommand("compare-tables") with OfflineSubcommand {
      val leftTable: ScallopOption[String] =
        opt[String](required = true, descr = "Fully qualified left table name")
      val rightTable: ScallopOption[String] =
        opt[String](required = true, descr = "Fully qualified right table name")
      val keys: ScallopOption[List[String]] =
        opt[List[String]](required = true, descr = "Comma-separated key columns (using left table names)")
      val mapping: ScallopOption[String] =
        opt[String](required = false,
                    descr = "JSON map of left_col_name -> SQL expr on right table for renames/transforms")
      val timestampMillisLeft: ScallopOption[String] =
        opt[String](required = false, descr = "SQL expression on left table to produce ts (Long millis)")
      val timestampMillisRight: ScallopOption[String] =
        opt[String](required = false, descr = "SQL expression on right table to produce ts (Long millis)")
      val sideBySideTable: ScallopOption[String] =
        opt[String](required = true, descr = "Output table for joined comparison DataFrame")
      val metricsTable: ScallopOption[String] =
        opt[String](required = true, descr = "Output table for computed metrics")
      val leftColumns: ScallopOption[List[String]] =
        opt[List[String]](required = false,
                          descr = "Subset of left table columns to compare. If omitted, all non-key columns.")
      val migrationCheck: ScallopOption[Boolean] =
        opt[Boolean](required = false, default = Some(false), descr = "Allow left side to have extra columns")

      override def subcommandName(): String = "compare_tables"
    }

    def run(args: Args): Unit = {
      val sparkSession = args.sparkSession
      val tableUtils = TableUtils(sparkSession)
      import ai.chronon.spark.Extensions._

      val rawLeftDf = sparkSession.table(args.leftTable())
      val rawRightDf = sparkSession.table(args.rightTable())
      val keyColumns = args.keys()

      // Parse mapping JSON: left_col_name -> SQL expr on right table
      val mappingJson: Map[String, String] = args.mapping.toOption
        .map { json =>
          import scala.collection.JavaConverters._
          val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
          mapper
            .readValue(json, classOf[java.util.Map[String, String]])
            .asScala
            .toMap
        }
        .getOrElse(Map.empty)

      // Determine which key columns have renames via mapping
      val keyRenames: Map[String, String] = mappingJson.filter { case (leftCol, _) => keyColumns.contains(leftCol) }
      val valueMapping: Map[String, String] = mappingJson.filterNot { case (leftCol, _) =>
        keyColumns.contains(leftCol)
      }

      // Select left columns: if --left-columns specified, use those + keys; otherwise all columns
      val leftDf = args.leftColumns.toOption match {
        case Some(cols) =>
          val allLeftCols = (keyColumns ++ cols).distinct
          rawLeftDf.select(allLeftCols.map(functions.col): _*)
        case None => rawLeftDf
      }

      // Determine value columns on left side (non-key columns)
      val leftValueColumns = leftDf.schema.fieldNames.filterNot(keyColumns.contains)

      // Build right-side select expressions:
      // - key columns: use rename from mapping or same name
      // - value columns: use SQL expr from mapping or same name, aliased to left column name
      val rightKeyExprs = keyColumns.map { k =>
        keyRenames.get(k) match {
          case Some(rightExpr) => s"$rightExpr AS $k"
          case None            => k
        }
      }
      val rightValueExprs = leftValueColumns.map { leftCol =>
        valueMapping.get(leftCol) match {
          case Some(rightExpr) => s"$rightExpr AS $leftCol"
          case None            => leftCol
        }
      }
      val rightDf = rawRightDf.selectExpr((rightKeyExprs ++ rightValueExprs): _*)

      // Apply timestamp expressions if provided
      val tsLeft = args.timestampMillisLeft.toOption
      val tsRight = args.timestampMillisRight.toOption
      val (finalLeftDf, finalRightDf, finalKeys) = (tsLeft, tsRight) match {
        case (Some(leftExpr), Some(rightExpr)) =>
          (leftDf.withColumn(Constants.TimeColumn, functions.expr(leftExpr)),
           rightDf.withColumn(Constants.TimeColumn, functions.expr(rightExpr)),
           keyColumns :+ Constants.TimeColumn)
        case (None, None) =>
          (leftDf, rightDf, keyColumns)
        case _ =>
          throw new IllegalArgumentException(
            "Both --timestamp-millis-left and --timestamp-millis-right must be provided together, or neither")
      }

      val (compareDf, metricsFlatDf, metrics) = CompareBaseJob.compare(
        finalLeftDf,
        finalRightDf,
        finalKeys,
        tableUtils,
        mapping = Map.empty, // mapping already applied via selectExpr above
        migrationCheck = args.migrationCheck(),
        name = s"${args.leftTable()}_vs_${args.rightTable()}",
        requireTimeColumn = false
      )

      logger.info("Saving side-by-side comparison output..")
      compareDf.save(args.sideBySideTable(), partitionColumns = List.empty)

      logger.info("Saving metrics output..")
      metricsFlatDf.save(args.metricsTable(), partitionColumns = List.empty)

      logger.info("Printing basic comparison results..")
      CompareJob.printAndGetBasicMetrics(metrics, tableUtils.partitionSpec)

      logger.info("Finished compare-tables.")
    }
  }

  object CheckPartitions {
    private val helpNamingConvention =
      "Please follow the naming convention: --partition-names=schema.table/pk1=pv1/pk2=pv2"

    @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
    class Args extends Subcommand("check-partitions") with OfflineSubcommand {

      val partitionNames = opt[List[String]](
        name = "partition-names",
        descr = "List of partition names",
        default = Some(Nil)
      )

      override def subcommandName(): String = "metastore_check_partitions"
    }

    def run(args: Args): Unit = {
      val partitionNames = args.partitionNames()
      val tablesToPartitionSpec = partitionNames.map((p) =>
        p.split("/").toList match {
          case fullTableName :: partitionParts if partitionParts.nonEmpty =>
            // Join all partition parts with "/" and parse as one combined partition spec.
            val partitionSpec = partitionParts.mkString("/")
            (fullTableName, Format.parseHiveStylePartition(partitionSpec))
          case fullTableName :: Nil =>
            throw new IllegalArgumentException(
              s"A partition spec must be specified for ${fullTableName}. ${helpNamingConvention}")
          case _ =>
            throw new IllegalArgumentException(
              s"Invalid partition name argument: ${partitionNames}. ${helpNamingConvention}")
        })

      val tableUtils = args.buildTableUtils()

      val isAllPartitionsPresent = tablesToPartitionSpec.forall { case (tbl, spec) =>
        val containsSpec = if (tableUtils.tableReachable(tbl)) {
          val partColumnName = spec.head._1
          val partitionSpec =
            PartitionSpec(partColumnName, tableUtils.partitionSpec.format, tableUtils.partitionSpec.spanMillis)
          val partList = tableUtils.partitions(tbl, spec.tail.toMap, tablePartitionSpec = Option(partitionSpec))
          logger.info(
            s"Checking for presence of partition: ${spec.head} for table: ${tbl} with subpartitions: ${spec.tail}")
          partList.contains(spec.head._2)
        } else {
          logger.info(s"Table ${tbl} is not reachable.")
          false
        }
        if (containsSpec) {
          logger.info(s"Table ${tbl} has partition ${spec} present.")
        } else {
          logger.info(s"Table ${tbl} does not have partition ${spec} present.")
        }
        containsSpec
      }
      if (isAllPartitionsPresent) {
        logger.info(s"All partitions ${partitionNames} are present.")
        sys.exit(0)
      } else {
        logger.info(s"Not all partitions ${partitionNames} are present.")
        sys.exit(1)
      }

    }
  }

  object CreateSummaryDataset {
    @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
    class Args extends Subcommand("create-summary-dataset") with OnlineSubcommand

    def run(args: Args): Unit = {
      logger.info(s"Creating table '${Constants.TiledSummaryDataset}'")
      val store = args.api.genKvStore
      val props = Map("is-time-sorted" -> "true")
      store.create(Constants.TiledSummaryDataset, props)
    }
  }

  object SummarizeAndUpload {
    @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
    class Args extends Subcommand("summarize-and-upload") with OnlineSubcommand {

      val confPath: ScallopOption[String] =
        opt[String](required = true, descr = "Name of the conf to summarize - joins/team/file.variable")

      // TODO: we should pull conf from conf path and figure out table name from the conf instead
      val parquetPath: ScallopOption[String] =
        opt[String](required = true, descr = "Location of the parquet containing the data to summarize")

      val timeColumn: ScallopOption[String] =
        opt[String](required = false, descr = "The column in the dataset which tracks the time")
    }

    // drift for all the conf-s go into same online table, but different offline table
    def run(args: Args): Unit = {
      val sparkSession: SparkSession = SparkSession
        .builder()
        .appName("ParquetReader")
        .config("spark.master", sys.env.getOrElse("SPARK_MASTER_URL", "local"))
        .config("spark.jars", args.onlineJar())
        .getOrCreate()
      implicit val tableUtils: TableUtils = TableUtils(sparkSession)
      logger.info("Running Summarizer")
      val confPath = args.confPath()
      val summarizer = new Summarizer(args.api, confPath, timeColumn = args.timeColumn.toOption)
      try {
        val df = sparkSession.read.parquet(args.parquetPath())
        val (result, summaryExprs) = summarizer.computeSummaryDf(df)
        val packer = new SummaryPacker(confPath, summaryExprs, summarizer.tileSize, summarizer.sliceColumns)
        val (packed, _) = packer.packSummaryDf(result)

        val uploader = new SummaryUploader(packed, args.api)
        uploader.run()
      } catch {
        case e: Exception =>
          logger.error(s"Failed to summarize and upload data for conf: $confPath", e)
          throw e
      }
    }
  }

  class Args(args: Array[String]) extends ScallopConf(args) {
    val noExit: ScallopOption[Boolean] = opt[Boolean](
      descr = "Whether to skip System.exit(0) at the end of the run",
      default = Some(false)
    )
    object JoinBackFillArgs extends JoinBackfill.Args
    addSubcommand(JoinBackFillArgs)
    object LogFlattenerArgs extends LogFlattener.Args
    addSubcommand(LogFlattenerArgs)
    object ComparisonTableArgs extends BuildComparisonTable.Args
    addSubcommand(ComparisonTableArgs)
    object ConsistencyMetricsArgs extends ConsistencyMetricsCompute.Args
    addSubcommand(ConsistencyMetricsArgs)
    object GroupByBackfillArgs extends GroupByBackfill.Args
    addSubcommand(GroupByBackfillArgs)
    object StagingQueryBackfillArgs extends StagingQueryBackfill.Args
    addSubcommand(StagingQueryBackfillArgs)
    object GroupByUploadArgs extends GroupByUploader.Args
    addSubcommand(GroupByUploadArgs)
    object FetcherCliArgs extends FetcherCli.Args
    addSubcommand(FetcherCliArgs)
    object MetadataUploaderArgs extends MetadataUploader.Args
    addSubcommand(MetadataUploaderArgs)
    object GroupByUploadToKVBulkLoadArgs extends GroupByUploadToKVBulkLoad.Args
    addSubcommand(GroupByUploadToKVBulkLoadArgs)
    object GroupByStreamingArgs extends GroupByStreaming.Args
    addSubcommand(GroupByStreamingArgs)
    object AnalyzerArgs extends Analyzer.Args
    addSubcommand(AnalyzerArgs)
    object CompareJoinQueryArgs extends CompareJoinQuery.Args
    addSubcommand(CompareJoinQueryArgs)
    object MetadataExportArgs extends MetadataExport.Args
    addSubcommand(MetadataExportArgs)
    object JoinBackfillLeftArgs extends JoinBackfillLeft.Args
    addSubcommand(JoinBackfillLeftArgs)
    object JoinBackfillFinalArgs extends JoinBackfillFinal.Args
    addSubcommand(JoinBackfillFinalArgs)
    object CreateStatsTableArgs extends CreateSummaryDataset.Args
    addSubcommand(CreateStatsTableArgs)
    object SummarizeAndUploadArgs extends SummarizeAndUpload.Args
    addSubcommand(SummarizeAndUploadArgs)
    object SourceJobRunArgs extends SourceJobRun.Args
    addSubcommand(SourceJobRunArgs)
    object JoinPartJobRunArgs extends JoinPartJobRun.Args
    addSubcommand(JoinPartJobRunArgs)
    object MergeJobRunArgs extends MergeJobRun.Args
    addSubcommand(MergeJobRunArgs)
    object CheckPartitionArgs extends CheckPartitions.Args
    addSubcommand(CheckPartitionArgs)
    object CompareTablesGenericArgs extends CompareTablesGeneric.Args
    addSubcommand(CompareTablesGenericArgs)
    requireSubcommand()
    verify()
  }

  def onlineBuilder(userConf: Map[String, String], onlineJar: String, onlineClass: String): Api = {
    val urls = Array(new File(onlineJar).toURI.toURL)
    val cl = ScalaClassLoader.fromURLs(urls, this.getClass.getClassLoader)
    val cls = cl.loadClass(onlineClass)
    val constructor = cls.getConstructors.apply(0)
    val onlineImpl = constructor.newInstance(userConf)
    onlineImpl.asInstanceOf[Api]
  }

  def main(baseArgs: Array[String]): Unit = {
    val args = new Args(baseArgs)
    val skipExit = args.noExit()
    var shouldExit = true
    args.subcommand match {
      case Some(x) =>
        x match {
          case args.JoinBackFillArgs         => JoinBackfill.run(args.JoinBackFillArgs)
          case args.GroupByBackfillArgs      => GroupByBackfill.run(args.GroupByBackfillArgs)
          case args.StagingQueryBackfillArgs => StagingQueryBackfill.run(args.StagingQueryBackfillArgs)
          case args.GroupByUploadArgs        => GroupByUploader.run(args.GroupByUploadArgs)
          case args.GroupByStreamingArgs =>
            shouldExit = false
            GroupByStreaming.run(args.GroupByStreamingArgs)

          case args.MetadataUploaderArgs => MetadataUploader.run(args.MetadataUploaderArgs)
          case args.GroupByUploadToKVBulkLoadArgs =>
            GroupByUploadToKVBulkLoad.run(args.GroupByUploadToKVBulkLoadArgs)
          case args.FetcherCliArgs           => FetcherCli.run(args.FetcherCliArgs)
          case args.LogFlattenerArgs         => LogFlattener.run(args.LogFlattenerArgs)
          case args.ComparisonTableArgs      => BuildComparisonTable.run(args.ComparisonTableArgs)
          case args.ConsistencyMetricsArgs   => ConsistencyMetricsCompute.run(args.ConsistencyMetricsArgs)
          case args.CompareJoinQueryArgs     => CompareJoinQuery.run(args.CompareJoinQueryArgs)
          case args.AnalyzerArgs             => Analyzer.run(args.AnalyzerArgs)
          case args.MetadataExportArgs       => MetadataExport.run(args.MetadataExportArgs)
          case args.JoinBackfillLeftArgs     => JoinBackfillLeft.run(args.JoinBackfillLeftArgs)
          case args.JoinBackfillFinalArgs    => JoinBackfillFinal.run(args.JoinBackfillFinalArgs)
          case args.CreateStatsTableArgs     => CreateSummaryDataset.run(args.CreateStatsTableArgs)
          case args.SummarizeAndUploadArgs   => SummarizeAndUpload.run(args.SummarizeAndUploadArgs)
          case args.SourceJobRunArgs         => SourceJobRun.run(args.SourceJobRunArgs)
          case args.JoinPartJobRunArgs       => JoinPartJobRun.run(args.JoinPartJobRunArgs)
          case args.MergeJobRunArgs          => MergeJobRun.run(args.MergeJobRunArgs)
          case args.CheckPartitionArgs       => CheckPartitions.run(args.CheckPartitionArgs)
          case args.CompareTablesGenericArgs => CompareTablesGeneric.run(args.CompareTablesGenericArgs)
          case _                             => logger.info(s"Unknown subcommand: $x")
        }
      case None => logger.info("specify a subcommand please")
    }
    if (shouldExit && !skipExit) {
      System.exit(0)
    }
  }
}
