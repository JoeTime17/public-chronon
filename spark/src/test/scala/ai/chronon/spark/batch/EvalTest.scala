package ai.chronon.spark.batch

import ai.chronon.aggregator.test.Column
import ai.chronon.api
import ai.chronon.api.Extensions._
import ai.chronon.api.ScalaJavaConversions.{IterableOps, MapOps}
import ai.chronon.api._
import ai.chronon.eval.CheckResult
import ai.chronon.spark.Extensions._
import ai.chronon.spark._
import ai.chronon.spark.batch._
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.utils.{DataFrameGen, SparkTestBase}
import org.apache.spark.sql.functions._
import org.junit.Assert._

class EvalTest extends SparkTestBase {

  private implicit val tableUtils: TableUtils = TableUtils(spark)

  private val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
  private val yearAgo = tableUtils.partitionSpec.minus(today, new Window(365, TimeUnit.DAYS))
  private val monthAgo = tableUtils.partitionSpec.minus(today, new Window(30, TimeUnit.DAYS))

  private val eval = new Eval()(tableUtils)

  it should "detect bad timestamps in GroupBy source data" in {
    val namespace = "testeval_bad_ts"
    createDatabase(namespace)

    // Create a table with bad timestamp data
    val badTimestampData = List(
      Column("user_id", api.StringType, 10),
      Column("amount", api.LongType, 1000)
    )

    val tableName = s"$namespace.bad_timestamp_source"
    spark.sql(s"DROP TABLE IF EXISTS $tableName")

    // Create DataFrame with bad timestamps (convert millis to seconds to make them too small)
    val df = DataFrameGen
      .events(spark, badTimestampData, 10, partitions = 2)
      .withColumn("ts", col("ts") / 1000) // Convert milliseconds to seconds - makes timestamps too small

    df.save(tableName)

    // Create GroupBy configuration
    val source = Builders.Source.events(
      query = Builders.Query(
        selects = Builders.Selects("user_id", "amount", "ts"),
        startPartition = yearAgo
      ),
      table = tableName
    )

    val groupByConf = Builders.GroupBy(
      sources = Seq(source),
      keyColumns = Seq("user_id"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.SUM, inputColumn = "amount")
      ),
      metaData = Builders.MetaData(name = "test_bad_timestamp_groupby", namespace = namespace, team = "chronon")
    )

    // Evaluate the GroupBy
    val (result, schema) = eval.evalGroupBy(groupByConf)

    // Verify that the evaluation completed
    assertNotNull(result)
    assertNotNull(schema)

    // Check that timestamp validation detected the issue
    val timestampCheck = result.getSourceTimestampCheck
    assertNotNull(timestampCheck)
    assertEquals(CheckResult.FAILURE, timestampCheck.getCheckResult)
    assertNotNull(timestampCheck.getMessage)
    println(timestampCheck.getMessage)
    assertTrue(
      timestampCheck.getMessage.contains(
        "Based on the sample values, ts appears to be in seconds. Please multiply by 1000: `(ts) * 1000`"))
  }

  it should "detect bad column expressions in GroupBy source selects" in {
    val namespace = "testeval_bad_select"
    createDatabase(namespace)

    // Create a table with valid data
    val validData = List(
      Column("user_id", api.StringType, 10),
      Column("amount", api.LongType, 1000)
    )

    val tableName = s"$namespace.bad_expression_source"
    spark.sql(s"DROP TABLE IF EXISTS $tableName")

    // Create DataFrame with valid data
    val df = DataFrameGen.events(spark, validData, 10, partitions = 2)
    df.save(tableName)

    // Create GroupBy configuration with misspelled column name in selects
    val source = Builders.Source.events(
      query = Builders.Query(
        selects = Builders.Selects("user_id", "misspelled_amount", "ts"), // "misspelled_amount" doesn't exist
        startPartition = yearAgo,
        endPartition = monthAgo
      ),
      table = tableName
    )

    val groupByConf = Builders.GroupBy(
      sources = Seq(source),
      keyColumns = Seq("user_id"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.SUM, inputColumn = "misspelled_amount")
      ),
      metaData = Builders.MetaData(name = "test_bad_expression_groupby", namespace = namespace, team = "chronon")
    )

    // Evaluate the GroupBy - this should catch the expression error during source evaluation
    val (result, _) = eval.evalGroupBy(groupByConf)

    // Note: The current evalGroupBy implementation doesn't explicitly catch source expression errors
    // but the timestamp check will fail because it can't process the malformed source
    assertNotNull(result)

    // The source expression error should be caught during the timestamp check
    val expressionCheck = result.getSourceExpressionCheck
    assertTrue(expressionCheck.getCheckResult == CheckResult.FAILURE)
    assertTrue(expressionCheck.getMessage.contains("misspelled_amount"))

    // If the column doesn't exist, the expression check should fail
  }

  it should "detect bad aggregation definition in GroupBy" in {
    val namespace = "testeval_bad_agg"
    createDatabase(namespace)

    // Create a table with valid data
    val validData = List(
      Column("user_id", api.StringType, 10),
      Column("amount_str", api.StringType, 1000)
    )

    val tableName = s"$namespace.bad_expression_source"
    spark.sql(s"DROP TABLE IF EXISTS $tableName")

    // Create DataFrame with valid data
    val df = DataFrameGen.events(spark, validData, 10, partitions = 2)
    df.save(tableName)

    // Create GroupBy configuration with misspelled column name in selects
    val source = Builders.Source.events(
      query = Builders.Query(
        selects = Builders.Selects("user_id", "amount_str", "ts"), // "misspelled_amount" doesn't exist
        startPartition = yearAgo,
        endPartition = monthAgo
      ),
      table = tableName
    )

    val groupByConf = Builders.GroupBy(
      sources = Seq(source),
      keyColumns = Seq("user_id"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.SUM, inputColumn = "amount_str")
      ),
      metaData = Builders.MetaData(name = "test_bad_expression_groupby", namespace = namespace, team = "chronon")
    )

    // Evaluate the GroupBy - this should catch the expression error during source evaluation
    val (result, _) = eval.evalGroupBy(groupByConf)

    // The source expression error should be caught during the timestamp check
    val expressionCheck = result.getAggExpressionCheck
    assertTrue(expressionCheck.getCheckResult == CheckResult.FAILURE)
    assertTrue(expressionCheck.getMessage.contains("amount_str"))
  }

  it should "detect bad derivation definition in GroupBy" in {
    val namespace = "testeval_bad_derivation_gb"
    createDatabase(namespace)

    // Even though we're testing Derivations, leave the bad timestamps in here because it shouldn't halt
    // Schema validation
    val badTimestampData = List(
      Column("user_id", api.StringType, 10),
      Column("amount", api.LongType, 1000)
    )

    val tableName = s"$namespace.bad_timestamp_source"
    spark.sql(s"DROP TABLE IF EXISTS $tableName")

    // Create DataFrame with bad timestamps (convert millis to seconds to make them too small)
    val df = DataFrameGen
      .events(spark, badTimestampData, 10, partitions = 2)
      .withColumn("ts", col("ts") / 1000) // Convert milliseconds to seconds - makes timestamps too small

    df.save(tableName)

    // Create GroupBy configuration
    val source = Builders.Source.events(
      query = Builders.Query(
        selects = Builders.Selects("user_id", "amount", "ts"),
        startPartition = yearAgo
      ),
      table = tableName
    )

    val groupByConf = Builders.GroupBy(
      sources = Seq(source),
      keyColumns = Seq("user_id"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.SUM, inputColumn = "amount")
      ),
      derivations = Seq(Builders.Derivation(name = "invalid", expression = "non_existent_col * 100")),
      metaData = Builders.MetaData(name = "test_bad_timestamp_groupby", namespace = namespace, team = "chronon")
    )

    // Evaluate the GroupBy
    val (result, _) = eval.evalGroupBy(groupByConf)

    // Check that timestamp validation detected the issue
    val derivationCheck = result.getDerivationsExpressionCheck
    assertEquals(CheckResult.FAILURE, derivationCheck.getCheckResult)
    assertTrue(derivationCheck.getMessage.contains("non_existent_col"))
  }

  it should "test a join with bad key mapping" in {
    val join = createAndSetupJoin("test_eval_bad_keys", badKeyMapping = true)
    val result = eval.evalJoin(join)

    assertTrue(result.getJoinPartChecks.toScala.exists { result =>
      result.getKeySchemaCheck.getCheckResult == CheckResult.FAILURE
    })
  }

  it should "detect bad source timestamp in Join" in {
    val join = createAndSetupJoin("test_eval_bad_left_timestamp", badLeftSourceTimestamp = true)
    val result = eval.evalJoin(join)

    // Check that left timestamp validation detected the issue
    val leftTimestampCheck = result.getLeftTimestampCheck
    assertNotNull(leftTimestampCheck)
    assertEquals(CheckResult.FAILURE, leftTimestampCheck.getCheckResult)
    assertNotNull(leftTimestampCheck.getMessage)
    assertTrue(leftTimestampCheck.getMessage.contains("unix milliseconds"))
  }

  it should "detect bad source expression in Join" in {
    val join = createAndSetupJoin("test_eval_bad_left_expression", badLeftSourceExpression = true)
    val result = eval.evalJoin(join)

    // Check that left expression validation detected the issue
    val leftExpressionCheck = result.getLeftExpressionCheck
    assertNotNull(leftExpressionCheck)
    assertEquals(CheckResult.FAILURE, leftExpressionCheck.getCheckResult)
    assertNotNull(leftExpressionCheck.getMessage)
    assertTrue(leftExpressionCheck.getMessage.contains("non_existent_column"))
  }

  it should "detect bad source expression in JoinPart" in {
    val join = createAndSetupJoin("test_eval_bad_joinpart_expression", badSourceExpressionInJoinPart = true)
    val result = eval.evalJoin(join)

    // Check that at least one join part has a skipped key schema check (due to failed GB evaluation)
    assertTrue(result.getJoinPartChecks.toScala.exists { joinPartResult =>
      joinPartResult.getKeySchemaCheck.getCheckResult == CheckResult.SKIPPED
    })

    // Verify that output schemas are null/empty since join part evaluation failed
    assertNull(result.getRightPartsSchema)
    assertNull(result.getDerivationsSchema)
    assertNull(result.getExternalPartsSchema)
  }

  it should "detect bad derivation definition in Join" in {
    val join = createAndSetupJoin("test_eval_bad_join_derivation", badDerivation = true)
    val result = eval.evalJoin(join)

    // Check that derivation validation detected the issue
    val derivationCheck = result.getDerivationValidityCheck
    assertNotNull(derivationCheck)
    assertEquals(CheckResult.FAILURE, derivationCheck.getCheckResult)
    assertNotNull(derivationCheck.getMessage)
    assertTrue(derivationCheck.getMessage.contains("non_existent_join_column"))
  }

  it should "test join output schemas" in {
    val join = createAndSetupJoin("test_eval_schema")
    val result = eval.evalJoin(join)
    // JoinEvalResult(leftTimestampCheck:BaseEvalResult(checkResult:SUCCESS), joinPartChecks:[JoinPartEvalResult(partName:unit_test_user_transactions_unit_test.user_transactions, gbEvalResult:GroupByEvalResult(sourceTimestampCheck:BaseEvalResult(checkResult:SUCCESS), keySchema:{user=StringType, user_name=StringType}, aggSchema:{amount_dollars_sum_30d=LongType}), keySchemaCheck:BaseEvalResult(checkResult:SUCCESS)), JoinPartEvalResult(partName:unit_test_user_transactions2_unit_test.user_transactions2, gbEvalResult:GroupByEvalResult(sourceTimestampCheck:BaseEvalResult(checkResult:SUCCESS), keySchema:{user=StringType}, aggSchema:{amount_dollars_sum=LongType}), keySchemaCheck:BaseEvalResult(checkResult:SUCCESS)), JoinPartEvalResult(partName:unit_test_user_transactions_bootstrap_unit_test.user_transactions_bootstrap, gbEvalResult:GroupByEvalResult(sourceTimestampCheck:BaseEvalResult(checkResult:SUCCESS), keySchema:{user=StringType}, aggSchema:{amount_dollars_sum_10d=LongType}), keySchemaCheck:BaseEvalResult(checkResult:SUCCESS))], derivationValidityCheck:BaseEvalResult(checkResult:SUCCESS), leftQuerySchema:{user_name=StringType, user=StringType, ts=LongType, ds=StringType}, rightPartsSchema:{unit_test_user_transactions_amount_dollars_sum_30d=LongType, unit_test_user_transactions2_amount_dollars_sum=LongType, unit_test_user_transactions_bootstrap_amount_dollars_sum_10d=LongType}, derivationsSchema:{ratio_derivation=DoubleType, external_coalesce=IntType}, externalPartsSchema:{ext_return_one_value_number=IntType})

    assertEquals(
      result.getRightPartsSchema.toScala,
      Map(
        "unit_test_user_transactions_amount_dollars_sum_30d" -> "LongType",
        "unit_test_user_transactions2_amount_dollars_sum" -> "LongType",
        "unit_test_user_transactions_bootstrap_amount_dollars_sum_10d" -> "LongType"
      )
    )

    assertEquals(result.getDerivationsSchema.toScala,
                 Map("ratio_derivation" -> "DoubleType", "external_coalesce" -> "IntType"))

    assertEquals(result.getExternalPartsSchema.toScala, Map("ext_return_one_value_number" -> "IntType"))

    assertEquals(result.getLeftQuerySchema.toScala,
                 Map("user_name" -> "StringType", "user" -> "StringType", "ts" -> "LongType", "ds" -> "StringType"))
  }

  it should "evaluate staging query schema successfully" in {
    val namespace = "testeval_staging_query"
    createDatabase(namespace)

    // Create a source table
    val sourceData = List(
      Column("user_id", api.StringType, 10),
      Column("amount", api.LongType, 1000),
      Column("category", api.StringType, 5)
    )

    val sourceTable = s"$namespace.transactions"
    spark.sql(s"DROP TABLE IF EXISTS $sourceTable")

    val df = DataFrameGen.events(spark, sourceData, 100, partitions = 10)
    df.save(sourceTable)

    // Create StagingQuery configuration
    val stagingQueryConf = Builders.StagingQuery(
      query = s"""
        SELECT 
          user_id,
          SUM(amount) as total_amount,
          COUNT(*) as transaction_count,
          FIRST(category) as primary_category
        FROM $sourceTable 
        WHERE ds = "{{ ds }}"
        GROUP BY user_id
      """,
      metaData = Builders.MetaData(
        name = "user_daily_summary",
        namespace = namespace,
        team = "chronon"
      ),
      startPartition = yearAgo
    )

    // Evaluate the StagingQuery
    val result = eval.evalStagingQuery(stagingQueryConf)

    // Verify that the evaluation completed successfully
    assertNotNull(result)
    val queryCheck = result.getQueryCheck
    assertNotNull(queryCheck)
    assertEquals(CheckResult.SUCCESS, queryCheck.getCheckResult)

    // Verify the output schema
    val outputSchema = result.getOutputSchema
    assertNotNull(outputSchema)
    assertEquals(4, outputSchema.size())

    // Check specific schema fields
    assertEquals("StringType", outputSchema.get("user_id"))
    assertEquals("LongType", outputSchema.get("total_amount"))
    assertEquals("LongType", outputSchema.get("transaction_count"))
    assertEquals("StringType", outputSchema.get("primary_category"))
  }

  it should "detect bad query expression in staging query" in {
    val namespace = "testeval_staging_query_bad"
    createDatabase(namespace)

    // Create a source table
    val sourceData = List(
      Column("user_id", api.StringType, 10),
      Column("amount", api.LongType, 1000)
    )

    val sourceTable = s"$namespace.transactions"
    spark.sql(s"DROP TABLE IF EXISTS $sourceTable")

    val df = DataFrameGen.events(spark, sourceData, 100, partitions = 10)
    df.save(sourceTable)

    // Create StagingQuery configuration with bad expression
    val stagingQueryConf = Builders.StagingQuery(
      query = s"""
        SELECT 
          user_id,
          SUM(non_existent_column) as total_amount
        FROM $sourceTable 
        WHERE ds = "{{ ds }}"
        GROUP BY user_id
      """,
      metaData = Builders.MetaData(
        name = "user_daily_summary_bad",
        namespace = namespace,
        team = "chronon"
      ),
      startPartition = yearAgo
    )

    // Evaluate the StagingQuery
    val result = eval.evalStagingQuery(stagingQueryConf)

    // Verify that the evaluation failed
    assertNotNull(result)
    val queryCheck = result.getQueryCheck
    assertNotNull(queryCheck)
    assertEquals(CheckResult.FAILURE, queryCheck.getCheckResult)
    assertNotNull(queryCheck.getMessage)
    assertTrue(queryCheck.getMessage.contains("non_existent_column"))
  }

  private def saveCompactFormatTable(namespace: String): String = {
    val data = List(
      Column("user_id", api.StringType, 10),
      Column("amount", api.LongType, 1000)
    )
    val tableName = s"$namespace.compact_format_source"
    spark.sql(s"DROP TABLE IF EXISTS $tableName")
    DataFrameGen
      .events(spark, data, 100, partitions = 10)
      .withColumn("ds", date_format(to_date(col("ds"), "yyyy-MM-dd"), "yyyyMMdd"))
      .save(tableName)
    tableName
  }

  private def compactQuery(selects: Map[String, String]): Query = {
    val query = Builders.Query(selects = selects, startPartition = "20200101")
    query.setPartitionFormat("yyyyMMdd")
    query
  }

  it should "eval a groupBy over a yyyyMMdd cumulative source without parse errors" in {
    val namespace = "testeval_compact_cumulative"
    createDatabase(namespace)
    val tableName = saveCompactFormatTable(namespace)

    // cumulative events exercise the lastAvailablePartition path in GroupBy.getIntersectedRange,
    // which must use the source's partition spec instead of the ambient yyyy-MM-dd one
    val source = Builders.Source.events(
      query = compactQuery(Builders.Selects("user_id", "amount", "ts")),
      table = tableName,
      isCumulative = true
    )

    val groupByConf = Builders.GroupBy(
      sources = Seq(source),
      keyColumns = Seq("user_id"),
      aggregations = Seq(Builders.Aggregation(operation = Operation.SUM, inputColumn = "amount")),
      metaData = Builders.MetaData(name = "test_compact_cumulative_groupby", namespace = namespace, team = "chronon")
    )

    val (result, schema) = eval.evalGroupBy(groupByConf)

    assertNotNull(result)
    // a partition parse error would surface as a source expression failure
    assertTrue(
      Option(result.getSourceExpressionCheck).forall(_.getCheckResult != CheckResult.FAILURE)
    )
    assertTrue(schema.nonEmpty)
  }

  it should "eval a join with a yyyyMMdd left source without parse errors" in {
    val namespace = "testeval_compact_left"
    createDatabase(namespace)
    val tableName = saveCompactFormatTable(namespace)

    val groupByConf = Builders.GroupBy(
      sources = Seq(
        Builders.Source.events(
          query = compactQuery(Builders.Selects("user_id", "amount", "ts")),
          table = tableName
        )),
      keyColumns = Seq("user_id"),
      aggregations = Seq(Builders.Aggregation(operation = Operation.SUM, inputColumn = "amount")),
      metaData = Builders.MetaData(name = "test_compact_left_groupby", namespace = namespace, team = "chronon")
    )

    val joinConf = Builders.Join(
      left = Builders.Source.events(
        query = compactQuery(Builders.Selects("user_id", "ts")),
        table = tableName
      ),
      joinParts = Seq(Builders.JoinPart(groupBy = groupByConf)),
      metaData = Builders.MetaData(name = "test.compact_left_join", namespace = namespace, team = "chronon")
    )

    val result = eval.evalJoin(joinConf)

    assertNotNull(result)
    // a partition parse error in sampleLeftSource would surface as a left expression failure
    assertTrue(
      Option(result.getLeftExpressionCheck).forall(_.getCheckResult != CheckResult.FAILURE)
    )
    assertEquals(CheckResult.SUCCESS, result.getLeftTimestampCheck.getCheckResult)
  }

  def createAndSetupJoin(namespace: String,
                         badLeftSourceTimestamp: Boolean = false,
                         badLeftSourceExpression: Boolean = false,
                         badKeyMapping: Boolean = false,
                         badSourceExpressionInJoinPart: Boolean = false,
                         badDerivation: Boolean = false) = {
    createDatabase(namespace)

    val start = tableUtils.partitionSpec.minus(today, new Window(60, TimeUnit.DAYS))
    val dayAndMonthBefore = tableUtils.partitionSpec.before(monthAgo)

    // Mix up the types so keys don't match
    val userColumn = if (badKeyMapping) {
      Column("user", LongType, 10)
    } else {
      Column("user", StringType, 10)
    }

    val dollarTransactions = List(
      userColumn,
      Column("user_name", api.StringType, 10),
      Column("ts", LongType, 200),
      Column("amount_dollars", LongType, 1000)
    )

    val rupeeTransactions = List(
      userColumn,
      Column("user_name", api.StringType, 10),
      Column("ts", LongType, 200),
      Column("amount_rupees", LongType, 70000)
    )

    val dollarTable = s"$namespace.dollar_transactions"
    val rupeeTable = s"$namespace.rupee_transactions"
    spark.sql(s"DROP TABLE IF EXISTS $dollarTable")
    spark.sql(s"DROP TABLE IF EXISTS $rupeeTable")
    DataFrameGen.entities(spark, dollarTransactions, 200, partitions = 20).save(dollarTable, Map("tblProp1" -> "1"))
    DataFrameGen.entities(spark, rupeeTransactions, 200, partitions = 20).save(rupeeTable)

    val dollarSource = Builders.Source.entities(
      query = Builders.Query(
        selects = Builders.Selects("ts", "amount_dollars", "user_name", "user"),
        startPartition = yearAgo,
        endPartition = dayAndMonthBefore,
        setups =
          Seq("create temporary function temp_replace_right_a as 'org.apache.hadoop.hive.ql.udf.UDFRegExpReplace'")
      ),
      snapshotTable = dollarTable
    )

    val dollarEventSource = Builders.Source.events(
      query = Builders.Query(
        selects = Builders.Selects("ts", "amount_dollars", "user_name", "user"),
        startPartition = yearAgo,
        endPartition = dayAndMonthBefore,
        setups =
          Seq("create temporary function temp_replace_right_a as 'org.apache.hadoop.hive.ql.udf.UDFRegExpReplace'")
      ),
      table = dollarTable
    )

    val rupeeSource =
      Builders.Source.entities(
        query = Builders.Query(
          selects = Map("ts" -> "ts",
                        "amount_dollars" -> "CAST(amount_rupees/70 as long)",
                        "user_name" -> "user_name",
                        "user" -> "user"),
          startPartition = monthAgo,
          setups = Seq(
            "create temporary function temp_replace_right_b as 'org.apache.hadoop.hive.ql.udf.UDFRegExpReplace'",
            "create temporary function temp_replace_right_c as 'org.apache.hadoop.hive.ql.udf.UDFRegExpReplace'",
            "create temporary function temp_replace_right_c as 'org.apache.hadoop.hive.ql.udf.UDFRegExpReplace'"
          )
        ),
        snapshotTable = rupeeTable
      )

    val groupBy = Builders.GroupBy(
      sources = Seq(dollarSource, rupeeSource),
      keyColumns = Seq("user", "user_name"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.SUM,
                             inputColumn = "amount_dollars",
                             windows = Seq(new Window(30, TimeUnit.DAYS)))),
      metaData = Builders.MetaData(name = "unit_test.user_transactions", namespace = namespace, team = "chronon")
    )

    val groupBy2 = if (badSourceExpressionInJoinPart) {
      // Create a GroupBy with a bad aggregation column to test join part expression validation
      Builders.GroupBy(
        sources = Seq(dollarEventSource),
        keyColumns = Seq("user"),
        aggregations = Seq(
          Builders.Aggregation(operation = Operation.SUM, inputColumn = "non_existent_amount")
        ), // non_existent_amount doesn't exist
        metaData = Builders.MetaData(name = "unit_test.user_transactions2", namespace = namespace, team = "chronon")
      )
    } else {
      Builders.GroupBy(
        sources = Seq(dollarEventSource),
        keyColumns = Seq("user"),
        aggregations = Seq(Builders.Aggregation(operation = Operation.SUM, inputColumn = "amount_dollars")),
        metaData = Builders.MetaData(name = "unit_test.user_transactions2", namespace = namespace, team = "chronon")
      )
    }

    val queriesSchema = List(
      Column("user_name", api.StringType, 10),
      Column("user", api.StringType, 10)
    )

    val queryTable = s"$namespace.queries"
    val queryDf = DataFrameGen
      .events(spark, queriesSchema, 400, partitions = 20, partitionColumn = Some("date"))

    if (badLeftSourceTimestamp) {
      // Convert milliseconds to seconds to make timestamps too small (like in GroupBy test)
      queryDf.withColumn("ts", col("ts") / 1000).save(queryTable, partitionColumns = Seq("date"))
    } else {
      queryDf.save(queryTable, partitionColumns = Seq("date"))
    }

    // Make bootstrap part and table
    val bootstrapSourceTable = s"$namespace.bootstrap"
    val bootstrapCol = "unit_test_user_transactions_amount_dollars_sum_10d"
    tableUtils
      .loadTable(queryTable)
      .select(
        col("user"),
        col("ts"),
        (rand() * 30000)
          .cast(org.apache.spark.sql.types.LongType)
          .as(bootstrapCol),
        col("date").as("ds")
      )
      .save(bootstrapSourceTable)

    val bootstrapGroupBy = Builders.GroupBy(
      sources = Seq(dollarSource, rupeeSource),
      keyColumns = Seq("user"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.SUM,
                             inputColumn = "amount_dollars",
                             windows = Seq(new Window(10, TimeUnit.DAYS)))),
      metaData =
        Builders.MetaData(name = "unit_test.user_transactions_bootstrap", namespace = namespace, team = "chronon")
    )

    val bootstrapPart = Builders.BootstrapPart(
      query = Builders.Query(
        selects = Builders.Selects("user", "ts", "unit_test_user_transactions_amount_dollars_sum_10d"),
        startPartition = start,
        endPartition = today
      ),
      table = s"$namespace.bootstrap",
      keyColumns = Seq("user", "ts")
    )

    val jp1 = Builders.JoinPart(groupBy = groupBy, keyMapping = Map("user_name" -> "user", "user" -> "user_name"))

    val jp2 = Builders.JoinPart(groupBy = groupBy2)

    val returnOneSource = Builders.ExternalSource(
      metadata = Builders.MetaData(
        name = "return_one"
      ),
      keySchema = StructType("key_one", Array(StructField("key_number", IntType))),
      valueSchema = StructType("value_one", Array(StructField("value_number", IntType)))
    )

    val leftQuery = if (badLeftSourceExpression) {
      // Add a bad expression to test left source expression validation
      Builders.Query(
        selects =
          Builders.Selects("user_name", "user", "ts", "non_existent_column"), // non_existent_column doesn't exist
        startPartition = start,
        setups = Seq(
          "create temporary function temp_replace_left as 'org.apache.hadoop.hive.ql.udf.UDFRegExpReplace'",
          "create temporary function temp_replace_right_c as 'org.apache.hadoop.hive.ql.udf.UDFRegExpReplace'"
        ),
        partitionColumn = "date"
      )
    } else {
      Builders.Query(
        startPartition = start,
        setups = Seq(
          "create temporary function temp_replace_left as 'org.apache.hadoop.hive.ql.udf.UDFRegExpReplace'",
          "create temporary function temp_replace_right_c as 'org.apache.hadoop.hive.ql.udf.UDFRegExpReplace'"
        ),
        partitionColumn = "date"
      )
    }

    val joinConf: ai.chronon.api.Join = Builders.Join(
      left = Builders.Source.events(
        query = leftQuery,
        table = queryTable
      ),
      joinParts = Seq(jp1, jp2, Builders.JoinPart(groupBy = bootstrapGroupBy)),
      bootstrapParts = Seq(bootstrapPart), // ext_return_one_number
      derivations = if (badDerivation) {
        // Create derivations with bad expressions to test join derivation validation
        Seq(
          Builders.Derivation(
            "ratio_derivation",
            "unit_test_user_transactions2_amount_dollars_sum / (COALESCE(unit_test_user_transactions_amount_dollars_sum_30d, 0) + 1)"),
          Builders.Derivation("bad_derivation",
                              "non_existent_join_column * 100"
          ) // non_existent_join_column doesn't exist
        )
      } else {
        Seq(
          Builders.Derivation(
            "ratio_derivation",
            "unit_test_user_transactions2_amount_dollars_sum / (COALESCE(unit_test_user_transactions_amount_dollars_sum_30d, 0) + 1)"),
          Builders.Derivation("external_coalesce", "COALESCE(ext_return_one_value_number, 1)")
        )
      },
      externalParts = Seq(Builders.ExternalPart(returnOneSource)),
      metaData = Builders.MetaData(name = "test.user_transaction_features", namespace = namespace, team = "chronon")
    )

    joinConf
  }

}
