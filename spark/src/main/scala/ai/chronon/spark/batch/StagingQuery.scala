package ai.chronon.spark.batch
import ai.chronon.api
import ai.chronon.api.Extensions._
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.api.thrift.TBase
import ai.chronon.api.{EngineType, ParametricMacro, PartitionRange, ThriftJsonCodec}
import ai.chronon.spark.Extensions._
import ai.chronon.spark.RunnerUtils
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.submission.SparkSessionBuilder
import org.rogach.scallop.{ScallopConf, ScallopOption}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.reflect.ClassTag

class StagingQuery(stagingQueryConf: api.StagingQuery, endPartition: String, tableUtils: TableUtils) {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  assert(Option(stagingQueryConf.metaData.outputNamespace).nonEmpty, "output namespace could not be empty or null")
  protected val outputTable = stagingQueryConf.metaData.outputTable
  private val tableProps = Option(stagingQueryConf.metaData.tableProperties)
    .map(_.toScala.toMap)
    .orNull

  private val partitionCols: Seq[String] =
    Seq(tableUtils.partitionColumn) ++
      (Option(stagingQueryConf.metaData.additionalOutputPartitionColumns)
        .map(_.toScala)
        .getOrElse(Seq.empty))

  def computeStagingQuery(stepDays: Option[Int] = None,
                          enableAutoExpand: Option[Boolean] = Some(true),
                          overrideStartPartition: Option[String] = None,
                          skipFirstHole: Boolean = true,
                          forceOverwrite: Boolean = false): Unit = {
    logger.info("Running setups for StagingQuery")
    Option(stagingQueryConf.setups).foreach(_.toScala.foreach(tableUtils.sql))
    val overrideStart = overrideStartPartition.getOrElse(stagingQueryConf.startPartition)
    val rangeToRun =
      if (forceOverwrite) Seq(PartitionRange(overrideStart, endPartition)(tableUtils.partitionSpec))
      else {
        val unfilledRanges =
          tableUtils.unfilledRanges(outputTable,
                                    PartitionRange(overrideStart, endPartition)(tableUtils.partitionSpec),
                                    skipFirstHole = skipFirstHole)

        if (unfilledRanges.isEmpty) {
          logger.info(s"""No unfilled range for $outputTable given
                         |start partition of ${stagingQueryConf.startPartition}
                         |override start partition of $overrideStart
                         |end partition of $endPartition
                         |""".stripMargin)
          return
        }
        unfilledRanges.getOrElse(Seq.empty)
      }
    logger.info(s"--forceOverwrite set to: ${forceOverwrite}. Proceeding Staging Query run with range: ${rangeToRun}")
    val exceptions = mutable.Buffer.empty[String]
    rangeToRun.foreach { stagingQueryUnfilledRange =>
      try {
        val stepRanges = stepDays.map(stagingQueryUnfilledRange.stepsByDays).getOrElse(Seq(stagingQueryUnfilledRange))
        logger.info(s"Staging query ranges to compute: ${stepRanges.map { _.toString }.pretty}")
        stepRanges.zipWithIndex.foreach { case (range, index) =>
          val progress = s"| [${index + 1}/${stepRanges.size}]"
          logger.info(s"Computing staging query for range: $range  $progress")
          compute(range, Seq.empty[String], enableAutoExpand)
        }
        logger.info(s"Finished writing Staging Query data to $outputTable")
      } catch {
        case err: Throwable =>
          exceptions.append(s"Error handling range $stagingQueryUnfilledRange : ${err.getMessage}\n${err.traceString}")
      }
    }
    if (exceptions.nonEmpty) {
      val length = exceptions.length
      val fullMessage = exceptions.zipWithIndex
        .map { case (message, index) =>
          s"[${index + 1}/$length exceptions]\n$message"
        }
        .mkString("\n")
      throw new Exception(fullMessage)
    }
  }

  def compute(range: PartitionRange, setups: Seq[String], enableAutoExpand: Option[Boolean]): Unit = {
    Option(setups).foreach(_.foreach(tableUtils.sql))
    val renderedQuery =
      StagingQuery.substitute(tableUtils, stagingQueryConf.query, range.start, range.end, endPartition)
    logger.info(s"Rendered Staging Query to run is:\n$renderedQuery")
    val df = tableUtils.sql(renderedQuery)
    df.save(outputTable, tableProps, partitionCols, autoExpand = enableAutoExpand.get)
    logger.info(s"Wrote to table $outputTable, into partitions: $range")
    logger.info(s"Finished writing Staging Query data to $outputTable")
  }
}

class Args(args: Seq[String]) extends ScallopConf(args) {
  val confPath: ScallopOption[String] = opt[String](required = true)
  val endDate: ScallopOption[String] = opt[String](required = false)
  val stepDays: ScallopOption[Int] = opt[Int](required = false) // doesn't apply to uploads
  val skipEqualCheck: ScallopOption[Boolean] =
    opt[Boolean](required = false, default = Some(false)) // only applies to join job for versioning
  def parseConf[T <: TBase[_, _]: Manifest: ClassTag]: T =
    ThriftJsonCodec.fromJsonFile[T](confPath(), check = false)

  override def toString(): String = {
    s"""
       |confPath = $confPath
       |endDate = $endDate
       |stepDays = $stepDays
       |skipEqualCheck = $skipEqualCheck""".stripMargin
  }
}

object StagingQuery {

  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  def substitute(tu: TableUtils, query: String, start: String, end: String, latest: String): String = {

    val maxDateMacro = ParametricMacro(
      "max_date",
      args => {
        lazy val table = args("table")
        if (table == null) {
          throw new IllegalArgumentException(s"No table in args:[$args] to macro max_date")
        }
        tu.lastAvailablePartition(table)
          .getOrElse(
            throw new IllegalStateException(s"No partitions exist for table $table to calculate max_date")
          )
      }
    )

    val queryWithBasicMacrosReplaced = ParametricMacro.applyBasicDateMacros(start, end, latest, tu.partitionSpec)(query)

    maxDateMacro.replace(queryWithBasicMacrosReplaced)
  }

  def main(args: Array[String]): Unit = {
    val parsedArgs = new Args(args)
    parsedArgs.verify()
    val stagingQueryConf = parsedArgs.parseConf[api.StagingQuery]
    val sparkSession =
      SparkSessionBuilder.build(s"staging_query_${stagingQueryConf.metaData.name}", enforceKryoSerializer = false)
    val stagingQueryJob = new StagingQuery(
      stagingQueryConf,
      parsedArgs.endDate(),
      RunnerUtils.tableUtilsForMetadata(sparkSession, stagingQueryConf.metaData)
    )
    stagingQueryJob.computeStagingQuery(parsedArgs.stepDays.toOption)
  }

  def from(stagingQueryConf: api.StagingQuery, endDate: String, tableUtils: TableUtils): StagingQuery = {
    scala.Option(stagingQueryConf.engineType) match {
      case Some(EngineType.BIGQUERY) => {
        logger.info("Using BigQuery staging query")
        createBigQueryImport(stagingQueryConf, endDate, tableUtils)
      }
      case Some(EngineType.SNOWFLAKE) => {
        logger.info("Using Snowflake staging query")
        createSnowflakeImport(stagingQueryConf, endDate, tableUtils)
      }
      case Some(EngineType.SPARK) => {
        logger.info("Using Spark staging query")
        new StagingQuery(stagingQueryConf, endDate, tableUtils)
      }
      case None => new StagingQuery(stagingQueryConf, endDate, tableUtils) // default to spark
    }
  }

  private def createBigQueryImport(stagingQueryConf: api.StagingQuery,
                                   endDate: String,
                                   tableUtils: TableUtils): StagingQuery = {
    val bigQueryClass = "ai.chronon.integrations.cloud_gcp.BigQueryImport"
    try {
      val constructor = Class
        .forName(bigQueryClass)
        .getDeclaredConstructor(classOf[api.StagingQuery], classOf[String], classOf[TableUtils])
      constructor.newInstance(stagingQueryConf, endDate, tableUtils).asInstanceOf[StagingQuery]
    } catch {
      case _: ClassNotFoundException =>
        throw new UnsupportedOperationException(
          s"BigQuery support not available. Make sure cloud_gcp module is on the classpath.")
      case ex: Exception =>
        throw new RuntimeException(s"Failed to create BigQuery staging query: ${ex.getMessage}", ex)
    }
  }

  private def createSnowflakeImport(stagingQueryConf: api.StagingQuery,
                                    endDate: String,
                                    tableUtils: TableUtils): StagingQuery = {
    val snowflakeClass = "ai.chronon.integrations.cloud_azure.SnowflakeImport"
    try {
      val constructor = Class
        .forName(snowflakeClass)
        .getDeclaredConstructor(classOf[api.StagingQuery], classOf[String], classOf[TableUtils])
      constructor.newInstance(stagingQueryConf, endDate, tableUtils).asInstanceOf[StagingQuery]
    } catch {
      case _: ClassNotFoundException =>
        throw new UnsupportedOperationException(
          s"Snowflake support not available. Make sure cloud_azure module is on the classpath.")
      case ex: Exception =>
        throw new RuntimeException(s"Failed to create Snowflake staging query: ${ex.getMessage}", ex)
    }
  }
}
