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
import ai.chronon.api.{Accuracy, AggregationPart, Constants, DataModel, DataType, PartitionRange}
import ai.chronon.api.ColorPrinter.ColorString
import ai.chronon.api.DataModel.{ENTITIES, EVENTS}
import ai.chronon.api.Extensions._
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.online.serde.SparkConversions
import ai.chronon.spark.Driver.parseConf
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.submission.ItemSketchSerializable
import org.apache.datasketches.frequencies.ErrorType
import org.apache.spark.sql.{Encoder, Encoders, types, DataFrame, Row}
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructType}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.{immutable, mutable}
import scala.collection.mutable.ListBuffer

class Analyzer(tableUtils: TableUtils,
               conf: Any,
               startDate: String,
               endDate: String,
               count: Int = 64,
               sample: Double = 0.1,
               skewDetection: Boolean = false,
               silenceMode: Boolean = false,
               confType: Option[String] = None) {

  implicit val tu = tableUtils
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  // include ts into heavy hitter analysis - useful to surface timestamps that have wrong units
  // include total approx row count - so it is easy to understand the percentage of skewed data
  def skewKeysWithTsAndCount(df: DataFrame,
                             keys: Array[String],
                             frequentItemMapSize: Int = 1024,
                             sampleFraction: Double = 0.1): Array[(String, Array[(String, Long)])] = {
    val baseDf = df.withColumn("total_count", lit("rows"))
    val baseKeys = keys :+ "total_count"
    if (df.schema.fieldNames.contains(Constants.TimeColumn)) {
      skewKeys(baseDf.withColumn("ts_year", from_unixtime(col("ts") / 1000, "yyyy")),
               baseKeys :+ "ts_year",
               frequentItemMapSize,
               sampleFraction)
    } else {
      skewKeys(baseDf, baseKeys, frequentItemMapSize, sampleFraction)
    }
  }

  // Uses a variant Misra-Gries frequent items algorithm from Data Sketches to find topK most frequent items in data
  // frame. The result is a Array of tuples of (column names, array of tuples of (frequent keys, counts))
  // [(keyCol1, [(key1: count1) ...]), (keyCol2, [...]), ....]
  def skewKeys(df: DataFrame,
               frequentItemKeys: Array[String],
               frequentItemMapSize: Int = 1024,
               sampleFraction: Double = 0.1): Array[(String, Array[(String, Long)])] = {
    assert(frequentItemKeys.nonEmpty, "No column arrays specified for frequent items summary")
    // convert all keys into string
    val stringifiedCols = frequentItemKeys.map { col =>
      val stringified = df.schema.fields.find(_.name == col) match {
        case Some(types.StructField(name, StringType, _, _)) => name
        case Some(types.StructField(name, _, _, _))          => s"CAST($name AS STRING)"
        case None =>
          throw new IllegalArgumentException(s"$col is not present among: [${df.schema.fieldNames.mkString(", ")}]")
      }
      s"COALESCE($stringified, 'NULL')"
    }

    val colsLength = stringifiedCols.length
    val aggregator = new FrequentItemsAggregator(colsLength, frequentItemMapSize, sampleFraction)
    val freqMaps = df
      .selectExpr(stringifiedCols: _*)
      .sample(sampleFraction)
      .select(aggregator.toColumn)
      .as(Encoders.kryo[Array[Array[(String, Long)]]])
      .head()
    frequentItemKeys.zip(freqMaps)
  }

  private val range = PartitionRange(startDate, endDate)(tableUtils.partitionSpec)
  // returns with frequent key analysis for the specified keys
  def analyze(df: DataFrame, keys: Array[String], sourceTable: String): String = {
    val result = skewKeysWithTsAndCount(df, keys, count, sample)
    val header = s"Analyzing frequent keys from table $sourceTable over columns: [${keys.mkString(", ")}]"
    val colPrints = result.flatMap { case (col, skewKeys) =>
      Seq(s"  $col") ++ skewKeys.map { case (name, count) => s"    $name: $count" }
    }
    (header +: colPrints).mkString("\n")
  }

  // Rich version of structType which includes additional info for a groupBy feature schema
  case class AggregationMetadata(name: String,
                                 columnType: DataType,
                                 operation: String = null,
                                 window: String = null,
                                 inputColumn: String = null,
                                 groupByName: String = null) {

    def asMap: Map[String, String] = {
      Map(
        "name" -> name,
        "window" -> window,
        "columnType" -> DataType.toString(columnType),
        "inputColumn" -> inputColumn,
        "operation" -> operation,
        "groupBy" -> groupByName
      )
    }

  }

  def toAggregationMetadata(aggPart: AggregationPart, columnType: DataType): AggregationMetadata = {
    AggregationMetadata(aggPart.outputColumnName,
                        columnType,
                        aggPart.operation.toString.toLowerCase,
                        aggPart.window.str.toLowerCase,
                        aggPart.inputColumn.toLowerCase)
  }

  def toAggregationMetadata(columnName: String, columnType: DataType): AggregationMetadata = {
    AggregationMetadata(columnName, columnType, "No operation", "Unbounded", columnName)
  }

  def analyzeGroupBy(groupByConf: api.GroupBy,
                     prefix: String = "",
                     includeOutputTableName: Boolean = false,
                     skewDetection: Boolean = false): (Array[AggregationMetadata], Map[String, DataType]) = {
    Option(groupByConf.setups).foreach(_.foreach(tableUtils.sql))
    val groupBy = GroupBy.from(groupByConf, range, tableUtils, computeDependency = skewDetection, finalize = true)
    val name = "group_by/" + prefix + groupByConf.metaData.name
    logger.info(s"""Running GroupBy analysis for $name ...""".stripMargin)

    val timestampChecks = runTimestampChecks(groupBy.inputDf)
    validateTimestampChecks(timestampChecks, "GroupBy", name)

    val analysis =
      if (skewDetection)
        analyze(groupBy.inputDf,
                groupByConf.keyColumns.toScala.toArray,
                groupByConf.sources.toScala.map(_.table).mkString(","))
      else ""
    val schema = if (groupByConf.hasDerivations) {
      // handle group by backfill mode for derivations
      // todo: add the similar logic to join derivations
      val keyAndPartitionFields =
        groupBy.keySchema.fields ++ Seq(org.apache.spark.sql.types.StructField(tableUtils.partitionColumn, StringType))
      val sparkSchema = {
        StructType(SparkConversions.fromChrononSchema(groupBy.outputSchema).fields ++ keyAndPartitionFields)
      }
      val dummyOutputDf = tableUtils.sparkSession
        .createDataFrame(java.util.Collections.emptyList[Row](), sparkSchema)
      val finalOutputColumns = groupByConf.derivationsScala.finalOutputColumn(dummyOutputDf.columns).toSeq
      val derivedDummyOutputDf = dummyOutputDf.select(finalOutputColumns: _*)
      val columns = SparkConversions.toChrononSchema(
        StructType(derivedDummyOutputDf.schema.filterNot(keyAndPartitionFields.contains)))
      api.StructType("", columns.map(tup => api.StructField(tup._1, tup._2)))
    } else {
      groupBy.outputSchema
    }
    if (silenceMode) {
      logger.info(s"""ANALYSIS completed for group_by/${name}.""".stripMargin)
    } else {
      logger.info(s"""
           |ANALYSIS for $name:
           |$analysis
               """.stripMargin)
      if (includeOutputTableName)
        logger.info(s"""
             |----- OUTPUT TABLE NAME -----
             |${groupByConf.metaData.outputTable}
               """.stripMargin)
      val keySchema = groupBy.keySchema.fields.map { field => s"  ${field.name} => ${field.dataType}" }
      schema.fields.map { field => s"  ${field.name} => ${field.fieldType}" }
      logger.info(s"""
           |----- KEY SCHEMA -----
           |${keySchema.mkString("\n")}
           |----- OUTPUT SCHEMA -----
           |${schema.mkString("\n")}
           |------ END --------------
           |""".stripMargin)
    }

    val aggMetadata = if (groupByConf.aggregations != null) {
      groupBy.aggPartWithSchema.map { entry => toAggregationMetadata(entry._1, entry._2) }.toArray
    } else {
      schema.map { tup => toAggregationMetadata(tup.name, tup.fieldType) }.toArray
    }
    val keySchemaMap = groupBy.keySchema.map { field =>
      field.name -> SparkConversions.toChrononType(field.name, field.dataType)
    }.toMap
    (aggMetadata, keySchemaMap)
  }

  def analyzeJoin(joinConf: api.Join,
                  skewDetection: Boolean = false,
                  validationAssert: Boolean = false): (Map[String, DataType], ListBuffer[AggregationMetadata]) = {
    val name = "joins/" + joinConf.metaData.name
    logger.info(s"""|Running join analysis for $name ...\n""".stripMargin)
    // run SQL environment setups such as UDFs and JARs
    Option(joinConf.setups).foreach(_.foreach(tableUtils.sql))

    val (analysis, leftDf) = if (skewDetection) {
      val leftDf = JoinUtils.leftDf(joinConf, range, tableUtils, allowEmpty = true).get
      val analysis = analyze(leftDf, joinConf.leftKeyCols, joinConf.left.table)
      (analysis, leftDf)
    } else {
      val analysis = ""
      val leftDf =
        tableUtils.scanDf(joinConf.left.query,
                          joinConf.left.table,
                          fallbackSelects = Some(Map(tableUtils.partitionColumn -> null)),
                          range = Some(range))
      (analysis, leftDf)
    }

    val timestampChecks = runTimestampChecks(leftDf)
    validateTimestampChecks(timestampChecks, "Join", name)

    val leftSchema = leftDf.schema.fields
      .map(field => (field.name, SparkConversions.toChrononType(field.name, field.dataType)))
      .toMap
    val aggregationsMetadata = ListBuffer[AggregationMetadata]()
    val keysWithError: ListBuffer[(String, String)] = ListBuffer.empty[(String, String)]
    val gbTables = ListBuffer[String]()
    val gbStartPartitions = mutable.Map[String, List[String]]()
    // Pair of (table name, group_by name, expected_start) which indicate that the table no not have data available for the required group_by
    val dataAvailabilityErrors: ListBuffer[(String, String, String)] = ListBuffer.empty[(String, String, String)]

    val rangeToFill =
      JoinUtils.getRangeToFill(joinConf.left, tableUtils, endDate, historicalBackfill = joinConf.historicalBackfill)
    logger.info(s"Join range to fill $rangeToFill")
    val unfilledRanges = tableUtils
      .unfilledRanges(
        joinConf.metaData.outputTable,
        rangeToFill,
        Some(Seq(joinConf.left.table)),
        inputPartitionSpecs = Seq(joinConf.left.query.partitionSpec(tableUtils.partitionSpec))
      )
      .getOrElse(Seq.empty)

    joinConf.joinParts.toScala.foreach { part =>
      val (aggMetadata, gbKeySchema) =
        analyzeGroupBy(part.groupBy,
                       Option(part.prefix).map(_ + "_").getOrElse(""),
                       includeOutputTableName = true,
                       skewDetection = skewDetection)
      aggregationsMetadata ++= aggMetadata.map { aggMeta =>
        AggregationMetadata(part.columnPrefix + aggMeta.name,
                            aggMeta.columnType,
                            aggMeta.operation,
                            aggMeta.window,
                            aggMeta.inputColumn,
                            part.getGroupBy.getMetaData.getName)
      }
      // Run validation checks.
      logger.info(s"""
          |left columns: ${leftDf.columns.mkString(", ")}
          |gb columns: ${gbKeySchema.keys.mkString(", ")}
          |""".stripMargin)
      keysWithError ++= runSchemaValidation(leftSchema, gbKeySchema, part.rightToLeft)
      gbTables ++= part.groupBy.sources.toScala.map(_.table)
      dataAvailabilityErrors ++= runDataAvailabilityCheck(joinConf.left.dataModel, part.groupBy, unfilledRanges)
      // list any startPartition dates for conflict checks
      val gbStartPartition = part.groupBy.sources.toScala
        .map(_.query.startPartition)
        .filter(_ != null)
      if (gbStartPartition.nonEmpty)
        gbStartPartitions += (part.groupBy.metaData.name -> gbStartPartition)
    }

    val rightSchema: Map[String, DataType] =
      aggregationsMetadata.map(aggregation => (aggregation.name, aggregation.columnType)).toMap
    if (silenceMode) {
      logger.info(s"""-- ANALYSIS completed for join/${joinConf.metaData.cleanName}. --""".stripMargin.blue)
    } else {
      logger.info(s"""
           |ANALYSIS for join/${joinConf.metaData.cleanName}:
           |$analysis
           |-- OUTPUT TABLE NAME --
           |${joinConf.metaData.outputTable}
           |-- LEFT SIDE SCHEMA --
           |${leftSchema.mkString("\n")}
           |-- RIGHT SIDE SCHEMA --
           |${rightSchema.mkString("\n")}
           |-- END --
           |""".stripMargin.green)
    }

    logger.info(s"-- Validations for join/${joinConf.metaData.cleanName} --")
    if (gbStartPartitions.nonEmpty) {
      logger.info(
        "-- Following GroupBy-s contains a startPartition. Please check if any startPartition will conflict with your backfill. --")
      gbStartPartitions.foreach { case (gbName, startPartitions) =>
        logger.info(s"    $gbName : ${startPartitions.mkString(",")}".yellow)
      }
    }

    if (keysWithError.nonEmpty) {
      logger.info(s"-- Schema validation completed. Found ${keysWithError.size} errors".red)
      val keyErrorSet: Set[(String, String)] = keysWithError.toSet
      logger.info(keyErrorSet.map { case (key, errorMsg) => s"$key => $errorMsg" }.mkString("\n    ").yellow)
    }

    if (dataAvailabilityErrors.nonEmpty) {
      logger.info(s"-- Data availability check completed. Found issue in ${dataAvailabilityErrors.size} tables --".red)
      dataAvailabilityErrors.foreach(error =>
        logger.info(s"    Group_By ${error._2} : Source Tables ${error._1} : Expected start ${error._3}".yellow))
    }

    if (keysWithError.isEmpty && dataAvailabilityErrors.isEmpty) {
      logger.info("-- Backfill validation completed. No errors found. --".green)
    }

    if (validationAssert) {
      if (joinConf.isSetBootstrapParts) {
        // For joins with bootstrap_parts, do not assert on data availability errors, as bootstrap can cover them
        // Only print out the errors as a warning
        assert(
          keysWithError.isEmpty,
          "ERROR: Join validation failed. Please check error message for details."
        )
      } else {
        assert(
          keysWithError.isEmpty && dataAvailabilityErrors.isEmpty,
          "ERROR: Join validation failed. Please check error message for details."
        )
      }
    }
    // (schema map showing the names and datatypes, right side feature aggregations metadata for metadata upload)
    (leftSchema ++ rightSchema, aggregationsMetadata)
  }

  // validate the schema of the left and right side of the join and make sure the types match
  // return a map of keys and corresponding error message that failed validation
  private def runSchemaValidation(left: Map[String, DataType],
                                  right: Map[String, DataType],
                                  keyMapping: Map[String, String]): Map[String, String] = {
    keyMapping.flatMap {
      case (_, leftKey) if !left.contains(leftKey) =>
        Some(leftKey ->
          s"[ERROR]: Left side of the join doesn't contain the key $leftKey. Available keys are [${left.keys.mkString(",")}]")
      case (rightKey, _) if !right.contains(rightKey) =>
        Some(
          rightKey ->
            s"[ERROR]: Right side of the join doesn't contain the key $rightKey. Available keys are [${right.keys
                .mkString(",")}]")
      case (rightKey, leftKey) if left(leftKey) != right(rightKey) =>
        Some(leftKey ->
          s"[ERROR]: Join key, '$leftKey', has mismatched data types - left type: ${left(leftKey)} vs. right type ${right(rightKey)}")
      case _ => None
    }
  }

  // validate that data is available for the group by
  // - For aggregation case, gb table earliest partition should go back to (first_unfilled_partition - max_window) date
  // - For none aggregation case or unbounded window, no earliest partition is required
  // return a list of (table, gb_name, expected_start) that don't have data available
  private def runDataAvailabilityCheck(leftDataModel: DataModel,
                                       groupBy: api.GroupBy,
                                       unfilledRanges: Seq[PartitionRange]): List[(String, String, String)] = {
    if (unfilledRanges.isEmpty) {
      logger.info("No unfilled ranges found.")
      List.empty
    } else {
      val firstUnfilledPartition = unfilledRanges.min.start
      lazy val groupByOps = new GroupByOps(groupBy)
      lazy val leftShiftedPartitionRangeStart = unfilledRanges.min.shiftPartitions(-1).start
      lazy val rightShiftedPartitionRangeStart = unfilledRanges.min.shiftPartitions(1).start
      val maxWindow = groupByOps.maxWindow
      maxWindow match {
        case Some(window) =>
          val expectedStart = (leftDataModel, groupBy.dataModel, groupBy.inferredAccuracy) match {
            // based on the end of the day snapshot
            case (ENTITIES, EVENTS, _)   => tableUtils.partitionSpec.minus(rightShiftedPartitionRangeStart, window)
            case (ENTITIES, ENTITIES, _) => firstUnfilledPartition
            case (EVENTS, ENTITIES, _)   => leftShiftedPartitionRangeStart
            case (EVENTS, EVENTS, Accuracy.SNAPSHOT) =>
              tableUtils.partitionSpec.minus(leftShiftedPartitionRangeStart, window)
            case (EVENTS, EVENTS, Accuracy.TEMPORAL) =>
              tableUtils.partitionSpec.minus(firstUnfilledPartition, window)
          }
          logger.info(
            s"Checking data availability for group_by ${groupBy.metaData.name} ... Expected start partition: $expectedStart")
          if (groupBy.sources.toScala.exists(s => s.isCumulative)) {
            List.empty
          } else {
            val tableToPartitions = groupBy.sources.toScala.map { source =>
              val table = source.table
              logger.info(s"Checking table $table for data availability ...")
              val startOpt = tableUtils.firstAvailablePartition(table)
              val endOpt = tableUtils.lastAvailablePartition(table)
              (table, startOpt, endOpt)
            }
            val minPartition = tableToPartitions.flatMap(_._2).sorted.headOption

            if (minPartition.isEmpty || minPartition.get > expectedStart) {
              logger.info(s"""
                         |Join needs data older than what is available for GroupBy: ${groupBy.metaData.name}
                         |left-${leftDataModel.toString.low.yellow},
                         |right-${groupBy.dataModel.toString.low.yellow},
                         |accuracy-${groupBy.inferredAccuracy.toString.low.yellow}
                         |expected earliest available data partition: $expectedStart\n""".stripMargin.red)
              tableToPartitions.foreach { case (table, startOpt, endOpt) =>
                logger.info(
                  s"Table $table startPartition ${startOpt.getOrElse("empty")} endPartition ${endOpt.getOrElse("empty")}")
              }
              val tables = tableToPartitions.map(_._1)
              List((tables.mkString(", "), groupBy.metaData.name, expectedStart))
            } else {
              List.empty
            }
          }
        case None =>
          List.empty
      }
    }
  }

  // For groupBys validate if the timestamp provided produces some values
  // if all values are null this should be flagged as an error
  def runTimestampChecks(df: DataFrame, sampleNumber: Int = 100): Map[String, String] = {

    val hasTimestamp = df.schema.fieldNames.contains(Constants.TimeColumn)
    val mapTimestampChecks = if (hasTimestamp) {
      // set max sample to 100 rows if larger input is provided
      val sampleN = if (sampleNumber > 100) { 100 }
      else { sampleNumber }
      dataframeToMap(
        df.limit(sampleN)
          .agg(
            // will return 0 if all values are null
            sum(when(col(Constants.TimeColumn).isNull, lit(0)).otherwise(lit(1)))
              .cast(StringType)
              .as("notNullCount"),
            // assumes that we have valid unix milliseconds between the date range of
            // 1971-01-01 00:00:00 (31536000000L) to 2099-12-31 23:59:59 (4102473599999L)
            // will return 0 if all values are within the range
            sum(when(col(Constants.TimeColumn).between(31536000000L, 4102473599999L), lit(0)).otherwise(lit(1)))
              .cast(StringType)
              .as("badRangeCount")
          )
          .select(col("notNullCount"), col("badRangeCount"))
      )
    } else {
      Map(
        "noTsColumn" -> "No Timestamp Column"
      )
    }
    mapTimestampChecks
  }

  /** This method can be used to trigger the assertion checks
    * or print the summary stats once the timestamp checks have been run
    * @param timestampCheckMap
    * @param configType
    * @param configName
    */
  def validateTimestampChecks(timestampCheckMap: Map[String, String], configType: String, configName: String): Unit = {

    if (!timestampCheckMap.contains("noTsColumn")) {
      // do timestamp checks
      assert(
        timestampCheckMap("notNullCount") != "0",
        s"""[ERROR]: $configType validation failed.
           | Please check that source has non-null timestamps.
           | check notNullCount: ${timestampCheckMap("notNullCount")}
           | """.stripMargin
      )
      assert(
        timestampCheckMap("badRangeCount") == "0",
        s"""[ERROR]: $configType validation failed.
           | Please check that source has valid epoch millisecond timestamps.
           | badRangeCount: ${timestampCheckMap("badRangeCount")}
           | """.stripMargin
      )

      logger.info(s"""ANALYSIS TIMESTAMP completed for ${configName}.
                     |check notNullCount: ${timestampCheckMap("notNullCount")}
                     |check badRangeCount: ${timestampCheckMap("badRangeCount")}
                     |""".stripMargin)

    } else {
      logger.info(s"""ANALYSIS TIMESTAMP completed for ${configName}.
                     |check TsColumn: ${timestampCheckMap("noTsColumn")}
                     |""".stripMargin)
    }

  }

  private def dataframeToMap(inputDf: DataFrame): Map[String, String] = {
    val row: Row = inputDf.head()
    val schema = inputDf.schema
    val columns = schema.fieldNames
    val values = row.toSeq
    columns
      .zip(values)
      .map { case (column, value) =>
        (column, value.toString)
      }
      .toMap
  }

  def run(): Unit = {

    val analyzerConf = conf match {
      case confPath: String =>
        if (confType.isDefined) {
          // TODO: davidhan - temporary hack for now as current approach with dataproc has the conf path only have the
          //  filename
          confType.get match {
            case "group_bys" => parseConf[api.GroupBy](confPath)
            case "joins"     => parseConf[api.Join](confPath)
          }
        } else {
          if (confPath.contains("/joins/")) { parseConf[api.Join](confPath) }
          else if (confPath.contains("/group_bys/")) {
            parseConf[api.GroupBy](confPath)
          }
        }
      case groupByConf: api.GroupBy => groupByConf
      case joinConf: api.Join       => joinConf
    }

    analyzerConf match {
      case groupByConf: api.GroupBy =>
        analyzeGroupBy(groupByConf, skewDetection = skewDetection)
      case joinConf: api.Join =>
        analyzeJoin(joinConf, skewDetection = skewDetection)
      case _ => throw new IllegalArgumentException("No configuration found for Analyzer")
    }
  }
}

private[spark] class FrequentItemsAggregator(
    colsLength: Int,
    mapSize: Int,
    sampleFraction: Double
) extends Aggregator[Row, Array[ItemSketchSerializable], Array[Array[(String, Long)]]]
    with Serializable {

  override def zero: Array[ItemSketchSerializable] =
    Array.fill(colsLength)((new ItemSketchSerializable).init(mapSize))

  override def reduce(sketches: Array[ItemSketchSerializable], row: Row): Array[ItemSketchSerializable] = {
    var i = 0
    while (i < colsLength) {
      sketches(i).sketch.update(row.getString(i))
      i += 1
    }
    sketches
  }

  override def merge(
      sketches1: Array[ItemSketchSerializable],
      sketches2: Array[ItemSketchSerializable]
  ): Array[ItemSketchSerializable] = {
    var i = 0
    while (i < colsLength) {
      sketches1(i).sketch.merge(sketches2(i).sketch)
      i += 1
    }
    sketches1
  }

  override def finish(sketches: Array[ItemSketchSerializable]): Array[Array[(String, Long)]] = {
    sketches
      .map(_.sketch.getFrequentItems(ErrorType.NO_FALSE_POSITIVES))
      .map(_.map(sketchRow => sketchRow.getItem -> (sketchRow.getEstimate.toDouble / sampleFraction).toLong).toArray)
  }

  override def bufferEncoder: Encoder[Array[ItemSketchSerializable]] =
    Encoders.kryo[Array[ItemSketchSerializable]]
  override def outputEncoder: Encoder[Array[Array[(String, Long)]]] =
    Encoders.kryo[Array[Array[(String, Long)]]]
}
