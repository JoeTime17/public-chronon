package ai.chronon.spark.batch

import ai.chronon.api
import ai.chronon.api.Extensions.{
  DerivationOps,
  ExternalPartOps,
  ExternalSourceOps,
  GroupByOps,
  JoinOps,
  JoinPartOps,
  QueryOps,
  SourceOps
}
import ai.chronon.api.ScalaJavaConversions.{JListOps, JMapOps, ListOps, MapOps}
import ai.chronon.api.{Constants, PartitionRange, StructField}
import ai.chronon.eval._
import ai.chronon.online.serde.SparkConversions
import ai.chronon.online.serde.SparkConversions.toChrononSchema
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.{GroupBy, JoinUtils}
import org.apache.spark.sql.functions.{col, lit, sum, when}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{DataFrame, Row}

import java.io.{PrintWriter, StringWriter}
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.collection.{immutable, mutable}

class Eval(implicit tableUtils: TableUtils) {
  implicit val partitionSpec = tableUtils.partitionSpec

  private val timestampCheckSampleSize = 100

  private def twoDaysAgo: PartitionRange = {
    val twoDaysAgo = LocalDate.now().minusDays(2)
    val formatted = twoDaysAgo.format(DateTimeFormatter.ISO_LOCAL_DATE) // "YYYY-MM-DD"
    PartitionRange(formatted, formatted)
  }

  private def sampleLeftSource(join: api.Join): (DataFrame, Boolean) = {
    // extract timestampCheckSampleSize rows from a source
    val sourceTable = join.left.table

    // Filter on the latest partition if it exists
    val leftSpec = join.left.query.partitionSpec(partitionSpec)
    val latestPartitionOpt = tableUtils.lastAvailablePartition(sourceTable, tablePartitionSpec = Option(leftSpec))
    // lastAvailablePartition normalizes grid-matching ds values to the global format and
    // leaves other-grid values raw - tag the range with the spec the value actually arrived in
    val arrivalSpec = if (leftSpec.hasSameGrid(partitionSpec)) partitionSpec else leftSpec
    val latestPartitonRange = latestPartitionOpt
      .map { partStr =>
        PartitionRange(partStr, partStr)(arrivalSpec)
      }
      .getOrElse(twoDaysAgo)

    // Even if we don't find partitions, we still want to return the dataframe for schema checks
    val df =
      JoinUtils.leftDf(join, latestPartitonRange, tableUtils, allowEmpty = true).get.limit(timestampCheckSampleSize)
    // Return limit and whether we found a partition or not
    (df, latestPartitionOpt.isDefined)
  }

  private def getLastPartitonOpt(sources: Seq[api.Source]): Option[PartitionRange] = {
    // Partition strings from different sources can be in different specs/formats, so compare
    // them on the time axis and express the result in the ambient spec instead of sorting
    // (and parsing) heterogeneous strings with one spec.
    sources
      .flatMap { source =>
        val sourceSpec = source.query.partitionSpec(partitionSpec)
        // grid-matching ds values come back normalized to the global format; parse with the
        // spec the value is actually in
        val arrivalSpec = if (sourceSpec.hasSameGrid(partitionSpec)) partitionSpec else sourceSpec
        tableUtils
          .lastAvailablePartition(source.table, tablePartitionSpec = Option(sourceSpec))
          .map(arrivalSpec.partitionStartMillis)
      }
      .reduceOption((left, right) => math.max(left, right))
      .map { latestMillis =>
        val latestPartition = partitionSpec.at(latestMillis)
        PartitionRange(latestPartition, latestPartition)
      }
  }

  private def setGroupByKeySchema(groupBy: GroupBy, evalResult: GroupByEvalResult) = {
    val keySchemaFormatted = groupBy.keySchema.fields.map { field =>
      (field.name -> field.dataType.toString)
    }.toMap
    evalResult.setKeySchema(keySchemaFormatted.toJava)
  }

  private def getFullStackTrace(e: Throwable): String = {
    val sw = new StringWriter()
    e.printStackTrace(new PrintWriter(sw))
    sw.toString
  }

  private def setGroupByAggScheam(groupBy: GroupBy, evalResult: GroupByEvalResult): (GroupByEvalResult, Boolean) = {
    try {
      // Set agg schema (pre-derivations)
      val baseSchemaFormatted = groupBy.outputSchema.fields.map { field =>
        (field.name -> field.fieldType.toString)
      }.toMap

      (evalResult.setAggSchema(baseSchemaFormatted.toJava), true)
    } catch {
      case e: Throwable =>
        val aggCheck = new BaseEvalResult()
        aggCheck.setCheckResult(CheckResult.FAILURE)
        aggCheck.setMessage(s"Invalid aggregation definition detected: ${getFullStackTrace(e)}")
        evalResult.setAggExpressionCheck(aggCheck)
        (evalResult, false)
    }
  }

  private def getGroupByDerivationSchema(groupByConf: api.GroupBy,
                                         groupBy: GroupBy,
                                         evalResult: GroupByEvalResult): Option[ai.chronon.api.StructType] = {
    val keyAndPartitionFields =
      groupBy.keySchema.fields ++ Seq(org.apache.spark.sql.types.StructField(tableUtils.partitionColumn, StringType))
    val sparkSchema = {
      StructType(SparkConversions.fromChrononSchema(groupBy.outputSchema).fields ++ keyAndPartitionFields)
    }
    val dummyOutputDf = tableUtils.sparkSession
      .createDataFrame(java.util.Collections.emptyList[Row](), sparkSchema)
    val finalOutputColumns = groupByConf.derivationsScala.finalOutputColumn(dummyOutputDf.columns).toSeq

    // Try to run the derivations, if anything is invalid set the error and return early
    val derivedDummyOutputDf =
      try {
        dummyOutputDf.select(finalOutputColumns: _*)
      } catch {
        case e: Throwable =>
          val derivationCheck = new BaseEvalResult()
          derivationCheck.setCheckResult(CheckResult.FAILURE)
          derivationCheck.setMessage(getFullStackTrace(e))
          evalResult.setDerivationsExpressionCheck(derivationCheck)
          return None
      }

    // Continue with schema extraction
    val columns = SparkConversions.toChrononSchema(
      StructType(derivedDummyOutputDf.schema.filterNot(keyAndPartitionFields.contains)))

    val derivationsSchema = api.StructType("", columns.map(tup => api.StructField(tup._1, tup._2)))

    // Set the formatted version onto the thrift struct
    val derivationsSchemaFormatted = derivationsSchema.fields.map { field =>
      (field.name -> field.fieldType.toString)
    }.toMap
    evalResult.setDerivationsSchema(derivationsSchemaFormatted.toJava)

    Option(derivationsSchema)
  }

  def evalGroupBy(groupByConf: api.GroupBy): (GroupByEvalResult, Option[ai.chronon.api.StructType]) = {
    val evalResult = new GroupByEvalResult()

    val sources = groupByConf.sources.toScala
    val unreachableSources = sources.filterNot(src => tableUtils.tableReachable(src.table))

    // If any of the tables don't exist, throw a failure at this point and skip the rest of the eval
    if (unreachableSources.nonEmpty) {
      val baseEvalResult = new BaseEvalResult()
      baseEvalResult.setCheckResult(CheckResult.FAILURE)
      baseEvalResult.setMessage(
        s"Source tables not found: ${unreachableSources.map(_.table).mkString(", ")}. Please check table names.")
      evalResult.setSourceExpressionCheck(baseEvalResult)
      return (evalResult, None)
    }

    // If source tables are all valid, get last available partition across them -- this will be used
    // for the timestamp check
    val latestPartitionAsRangeOption = getLastPartitonOpt(sources)

    // If table exists but is empty, schema checks will continue to eval
    // If the table doesn't exist, then source expression check will notify the user
    // We compute effective range because we need some range to get the schema, even if it doesn't contain data
    val effectiveRange = latestPartitionAsRangeOption.getOrElse(twoDaysAgo)

    // If the source expression is invalid, set the failure and return early
    val groupBy =
      try {
        groupByConf.setups.foreach(tableUtils.sql)
        GroupBy.from(groupByConf, effectiveRange, tableUtils, computeDependency = false, finalize = true)
      } catch {
        case e: Throwable =>
          val sourceExpressionResult = new BaseEvalResult()
          sourceExpressionResult.setMessage(s"Failed to evaluate source query: ${getFullStackTrace(e)}")
          sourceExpressionResult.setCheckResult(CheckResult.FAILURE)
          evalResult.setSourceExpressionCheck(sourceExpressionResult)
          return (evalResult, None)
      }

    // If latestPartitionAsRangeOption is empty then sources all have no partitions, and we'll skip the timestamp check,
    // but continue with schema checks
    if (latestPartitionAsRangeOption.nonEmpty) {
      val timestampCheck = runTimestampChecks(groupByConf.sources.toScala.toSeq, groupBy.inputDf)
      evalResult.setSourceTimestampCheck(timestampCheck)
    } else {
      // If we couldn't find a partition for the source, we skip with a reason timestamp checks
      val timestampCheckSkipResult = new BaseEvalResult()
      timestampCheckSkipResult.setCheckResult(CheckResult.SKIPPED)
      timestampCheckSkipResult.setMessage(
        s"Could not find any partitions in source ${groupByConf.sources.toScala.map(_.table).mkString(", ")} to run the group by on")
      evalResult.setSourceTimestampCheck(timestampCheckSkipResult)
    }

    // Set key schema
    setGroupByKeySchema(groupBy, evalResult)

    // If agg schema extraction fails, set the error and return early
    val (_, aggSchemaSuccess) = setGroupByAggScheam(groupBy, evalResult)
    if (!aggSchemaSuccess) {
      return (evalResult, None)
    }

    val outputSchema = if (groupByConf.hasDerivations) {
      // If derivations fail to parse, this returns None
      getGroupByDerivationSchema(groupByConf, groupBy, evalResult)
    } else {
      Option(groupBy.outputSchema)
    }

    // Also return the Chronon schema because this function is called from JoinEval, and it needs the raw schema
    // For join derivation computation
    (evalResult, outputSchema)
  }

  private def evalJoinPart(joinConf: api.Join, leftSchemaFormatted: Map[String, String])
      : (List[JoinPartEvalResult], List[Option[Map[String, String]]], List[Option[Array[StructField]]]) = {

    // Validate JoinParts
    // If joinParts share GBs, we don't want to rerun the gb eval
    // We only want to check the new keyMapping on each
    val gbToEvalResult = mutable.Map[String, (GroupByEvalResult, Option[ai.chronon.api.StructType])]()

    // Get a list of JoinPartEval and output schema option (None if the eval failed to produce a schema)
    joinConf.joinParts.toScala.map { part =>
      val joinPartEval = new JoinPartEvalResult()
      val gbName = s"${part.groupBy.metaData.name}"
      val prefix = s"${part.fullPrefix}_"
      val partName = s"$prefix$gbName"
      val columnPrefix = part.columnPrefix
      joinPartEval.setPartName(partName)

      // Eval the GB if not seen before, else use existing eval
      val (gbEval, gbSchemaRawOpt) = gbToEvalResult.getOrElseUpdate(gbName, evalGroupBy(part.groupBy))

      // Get the gbSchemaFormatted and modify name to contain the prefix
      val jpSchemaFormatted = Option(gbEval.getAggSchema).map(
        _.toScala
          .map { case (name, dataTypeStr) =>
            (columnPrefix + name, dataTypeStr)
          }
          .toMap)

      // Also prefix the raw schema for derivation handling
      val jpSchemaRawOpt = gbSchemaRawOpt.map { schema =>
        schema.fields.map { field =>
          StructField(columnPrefix + field.name, field.fieldType)
        }
      }

      joinPartEval.setGbEvalResult(gbEval)

      val gbKeySchema = Option(gbEval.getKeySchema)

      val keySchemaCheck = if (gbKeySchema.isDefined) {
        // Check the key Schema
        checkKeySchema(leftSchemaFormatted, gbKeySchema.get.toScala.toMap, part.rightToLeft)
      } else {
        val keyCheck = new BaseEvalResult()
        keyCheck.setCheckResult(CheckResult.SKIPPED)
        keyCheck.setMessage(s"Could not extract keySchema from GroupBy.")
        keyCheck
      }

      joinPartEval.setKeySchemaCheck(keySchemaCheck)

      (joinPartEval, jpSchemaFormatted, jpSchemaRawOpt)
    }.unzip3
  }

  private def getExternalPartsSchemaOpt(joinConf: api.Join): Option[Map[String, String]] = {
    Option(joinConf.onlineExternalParts).map { externalParts =>
      externalParts.toScala.flatMap { part =>
        // Only use value fields from the external part (as specified in the requirements)
        part.source.valueFields.map { field =>
          val fieldName = s"${part.fullName}_${field.name}"
          fieldName -> field.fieldType.toString
        }
      }.toMap
    }
  }

  private def setJoinDerivationEval(joinConf: api.Join,
                                    evalResult: JoinEvalResult,
                                    leftDF: DataFrame,
                                    joinPartOutputSchemaOpts: List[Option[Array[StructField]]]) = {
    val baseFields = joinPartOutputSchemaOpts.flatMap(_.get).toArray

    val leftSparkFields =
      leftDF.schema.fields ++ Seq(org.apache.spark.sql.types.StructField(tableUtils.partitionColumn, StringType))

    // Create a dummy spark schema combining left and right schemas
    val rightSparkFields = SparkConversions.fromChrononSchema(api.StructType("", baseFields)).fields

    // Add external parts fields to the schema for derivation computation
    val externalPartsSparkFields = Option(joinConf.onlineExternalParts)
      .map { externalParts =>
        externalParts.toScala.flatMap { part =>
          // Only use value fields from the external part (as specified in the requirements)
          part.source.valueFields.map { field =>
            val fieldName = s"${part.fullName}_${field.name}"
            SparkConversions
              .fromChrononSchema(api.StructType("", Array(api.StructField(fieldName, field.fieldType))))
              .fields
              .head
          }
        }.toArray
      }
      .getOrElse(Array.empty)

    val fullSparkSchema = StructType(leftSparkFields ++ rightSparkFields ++ externalPartsSparkFields)

    // Create dummy DataFrame to compute derivation schema
    val dummyOutputDf = tableUtils.sparkSession
      .createDataFrame(java.util.Collections.emptyList[Row](), fullSparkSchema)

    // Apply derivations to compute final output columns
    try {
      val finalOutputColumns = joinConf.derivationsScala.finalOutputColumn(dummyOutputDf.columns).toSeq
      val derivedDummyOutputDf = dummyOutputDf.select(finalOutputColumns: _*)

      // Convert derived schema back to Chronon format, excluding left and partition columns
      val derivedSparkSchema = StructType(derivedDummyOutputDf.schema.filterNot(leftSparkFields.contains))
      val derivedColumns = SparkConversions.toChrononSchema(derivedSparkSchema)
      val derivationsSchema = derivedColumns.map { case (name, dataType) => (name, dataType.toString) }.toMap

      evalResult.setDerivationsSchema(derivationsSchema.toJava)

      val derivationResult = new BaseEvalResult()
      derivationResult.setCheckResult(CheckResult.SUCCESS)
      evalResult.setDerivationValidityCheck(derivationResult)
    } catch {
      case e: Throwable =>
        val derivationResult = new BaseEvalResult()
        derivationResult.setCheckResult(CheckResult.FAILURE)
        derivationResult.setMessage(e.getMessage)
        evalResult.setDerivationValidityCheck(derivationResult)
    }
  }

  def evalJoin(joinConf: api.Join): JoinEvalResult = {
    val evalResult = new JoinEvalResult()

    val leftSource = joinConf.left

    // Try left side schema (catch expression errors)
    val (leftDF, partitionsFound) =
      try {
        // run SQL environment setups such as UDFs and JARs
        joinConf.setups.foreach(tableUtils.sql)
        sampleLeftSource(joinConf)
      } catch {
        case e: Throwable =>
          val leftQueryResult = new BaseEvalResult()
          leftQueryResult.setMessage(s"Failed to run left query: ${getFullStackTrace(e)}")
          leftQueryResult.setCheckResult(CheckResult.FAILURE)
          evalResult.setLeftExpressionCheck(leftQueryResult)
          return evalResult
      }

    // Get left Schema -- convert to Chronon schema for consistency with how we handle GroupBy
    val leftSchemaFormatted = toChrononSchema(leftDF.schema).map { case (name, dataType) =>
      (name, dataType.toString)
    }.toMap
    evalResult.setLeftQuerySchema(leftSchemaFormatted.toJava)

    // Check left side timestamp if the left source found partitions, else set the appropriate exception
    val leftSourceTimestampCheck = if (partitionsFound) {
      runTimestampChecks(Seq(leftSource), leftDF)
    } else {
      val check = new BaseEvalResult()
      check.setCheckResult(CheckResult.SKIPPED)
      check.setMessage(
        s"Could not find partitions in left source table: ${leftSource.table}, skipping timestamp check. Ensure table is not empty.")
      check
    }
    evalResult.setLeftTimestampCheck(leftSourceTimestampCheck)

    // Eval join parts and get output schemas for further processing in derivations section
    val (joinPartEvals, formattedSchemaOptions, outputSchemaOptions) = evalJoinPart(joinConf, leftSchemaFormatted)
    evalResult.setJoinPartChecks(joinPartEvals.toJava)

    // If any of the JoinParts failed to eval and produce output schemas, do not produce an output schema for the join
    // This is better than a partial schema, which can be confusing and make it look like the join compiled
    // I.e. in SQL a syntax error in one subquery causes the whole query to fail an `explain` plan
    if (outputSchemaOptions.exists(_.isEmpty)) {
      return evalResult
    }

    // flatten the various JoinPart schemas into a rightPartsSchema
    val fullRightPartsSchema: Map[String, String] = formattedSchemaOptions.flatMap(_.get).toMap
    evalResult.setRightPartsSchema(fullRightPartsSchema.toJava)

    // Handle external parts schema
    val externalPartsSchemaOpt = getExternalPartsSchemaOpt(joinConf)
    externalPartsSchemaOpt.foreach(schema => evalResult.setExternalPartsSchema(schema.toJava))

    // Handle derivations checking
    if (joinConf.hasDerivations) {
      setJoinDerivationEval(joinConf, evalResult, leftDF, outputSchemaOptions)
    }

    evalResult
  }

  // validate the schema of the left and right side of the join and make sure the types match
  // return a map of keys and corresponding error message that failed validation
  private def checkKeySchema(left: Map[String, String],
                             right: Map[String, String],
                             keyMapping: Map[String, String]): BaseEvalResult = {
    val evalResult = new BaseEvalResult()

    // Key errors *might* be caught at compile time, however we could still miss issues
    // if there is "select *" behavior in the source config, or if there is a schema mismatch
    val keyErrors = keyMapping.flatMap {
      case (_, leftKey) if !left.contains(leftKey) =>
        Some(s"Left side of the join doesn't contain the key $leftKey. Available keys are [${left.keys.mkString(",")}]")
      case (rightKey, _) if !right.contains(rightKey) =>
        Some(
          s"Right side of the join doesn't contain the key $rightKey. Available keys are [${right.keys.mkString(",")}]")
      case (rightKey, leftKey) if left(leftKey) != right(rightKey) =>
        Some(
          s"Join key, '$leftKey', has mismatched data types - left type: ${left(leftKey)} vs. right type ${right(rightKey)}")
      case _ => None
    }

    if (keyErrors.nonEmpty) {
      evalResult.setCheckResult(CheckResult.FAILURE)
      evalResult.setMessage(keyErrors.mkString("\n"))
    } else {
      evalResult.setCheckResult(CheckResult.SUCCESS)
    }

    evalResult
  }

  def evalStagingQuery(stagingQueryConf: api.StagingQuery): StagingQueryEvalResult = {
    val evalResult = new StagingQueryEvalResult()

    try {
      // Run SQL environment setups such as UDFs and JARs
      Option(stagingQueryConf.setups).foreach(_.toScala.foreach(tableUtils.sql))

      // Use 2 days ago as a dummy date range for schema evaluation - doesn't need real data
      val dummyRange = twoDaysAgo

      // Render the query with dummy dates to evaluate schema without computation
      val renderedQuery = StagingQuery.substitute(
        tableUtils,
        stagingQueryConf.query,
        dummyRange.start,
        dummyRange.end,
        dummyRange.end
      )

      // Get the query schema by creating a dummy dataframe with limit 0 (no data, just schema)
      val queryDf = tableUtils.sql(s"SELECT * FROM ($renderedQuery) LIMIT 0")

      // Convert schema to Chronon format for consistency
      val outputSchemaFormatted = toChrononSchema(queryDf.schema).map { case (name, dataType) =>
        (name, dataType.toString)
      }.toMap

      evalResult.setOutputSchema(outputSchemaFormatted.toJava)

      // Set success result
      val queryCheck = new BaseEvalResult()
      queryCheck.setCheckResult(CheckResult.SUCCESS)
      evalResult.setQueryCheck(queryCheck)

    } catch {
      case e: Throwable =>
        val queryCheck = new BaseEvalResult()
        queryCheck.setCheckResult(CheckResult.FAILURE)
        queryCheck.setMessage(s"Failed to evaluate staging query: ${getFullStackTrace(e)}")
        evalResult.setQueryCheck(queryCheck)
    }

    evalResult
  }

  def runTimestampChecks(sources: Seq[api.Source], df: DataFrame): BaseEvalResult = {
    // Initialize check with status true, set to false below if conditions are met
    val evalResult = new BaseEvalResult()
    evalResult.setCheckResult(CheckResult.SUCCESS)

    val timeColumn = Option(sources.head.query.timeColumn).getOrElse(Constants.TimeColumn)
    val timeColumnExpressions = sources.map { source =>
      // Get the expression within source.query that has timeColumn as the key, else just source.timeColumn
      Option(source.query.selects)
        .map { selects =>
          selects.toScala.getOrElse(timeColumn, timeColumn)
        }
        .getOrElse(timeColumn)
    }

    val tableNames = sources.map(_.table).mkString(", ")

    val hasTimestamp = df.schema.fieldNames.contains(Constants.TimeColumn)
    df.show()
    if (hasTimestamp) {
      val resultRow = df
        .limit(timestampCheckSampleSize)
        .agg(
          // will return 0 if all values are null
          sum(when(col(timeColumn).isNull, lit(0)).otherwise(lit(1)))
            .cast(StringType)
            .as("notNullCount"),
          // assumes that we have valid unix milliseconds between the date range of
          // 1971-01-01 00:00:00 (31536000000L) to 2099-12-31 23:59:59 (4102473599999L)
          // will return 0 if all values are within the range
          sum(when(col(timeColumn).between(31536000000L, 4102473599999L), lit(0)).otherwise(lit(1)))
            .cast(StringType)
            .as("badRangeCount")
        )
        .select(col("notNullCount"), col("badRangeCount"))
        .head()

      // Check null to avoid NPE at this stage in case of empty partition
      if (resultRow.isNullAt(resultRow.fieldIndex("notNullCount"))) {
        evalResult.setMessage(
          s"Failed to find rows in the latest partition from $tableNames, verify that source table contains data and that the latest partition is not empty.")
        evalResult.setCheckResult(CheckResult.FAILURE)
      } else {
        // Now get the counts
        val notNullCount = resultRow.getAs[String]("notNullCount")
        val badRangeCount = resultRow.getAs[String]("badRangeCount")

        if (notNullCount == "0") {
          evalResult.setMessage(
            s"Sampled timestamps were all null double check your time column ($timeColumn in $tableNames), and consider filtering out nulls in your source query")
          evalResult.setCheckResult(CheckResult.FAILURE)
        } else if (badRangeCount != "0") {
          // Analyze actual timestamp values to provide more helpful conversion guidance
          val sampleValues = df
            .limit(timestampCheckSampleSize)
            .select(col(timeColumn))
            .filter(col(timeColumn).isNotNull)
            .collect()
            .map(_.getDouble(0))
            .filter(_ > 1) // Only consider positive timestamps

          val detectedFormat = if (sampleValues.nonEmpty) {
            val avgValue = sampleValues.sum / sampleValues.length
            val expressionExamples = timeColumnExpressions
              .map { expr =>
                if (avgValue < 1e10) {
                  // Likely seconds (typical range: 1e9 to 2e9 for recent years)
                  s"($expr) * 1000"
                } else if (avgValue > 1e15) {
                  // Likely nanoseconds (typical range: 1e18 to 4e18 for recent years)
                  s"($expr) / 1000000"
                } else if (avgValue > 1e12) {
                  // Likely microseconds (typical range: 1e15 to 4e15 for recent years)
                  s"($expr) / 1000"
                } else {
                  // Unknown format, provide generic guidance
                  s"convert ($expr) to milliseconds"
                }
              }
              .mkString(", ")

            if (avgValue < 1e10) {
              ("seconds", "multiply by 1000", expressionExamples)
            } else if (avgValue > 1e15) {
              ("nanoseconds", "divide by 1,000,000", expressionExamples)
            } else if (avgValue > 1e12) {
              ("microseconds", "divide by 1,000", expressionExamples)
            } else {
              ("unknown format", "convert to milliseconds", expressionExamples)
            }
          } else {
            val expressionExamples =
              timeColumnExpressions.map(expr => s"convert ($expr) to milliseconds").mkString(", ")
            ("unknown format", "convert to milliseconds", expressionExamples)
          }

          evalResult.setMessage(
            s"Timestamps do not appear to be unix milliseconds. Based on the sample values, $timeColumn appears to be in ${detectedFormat._1}. Please ${detectedFormat._2}: `${detectedFormat._3}`).")
          evalResult.setCheckResult(CheckResult.FAILURE)
        }
      }

    }
    evalResult
  }

}
