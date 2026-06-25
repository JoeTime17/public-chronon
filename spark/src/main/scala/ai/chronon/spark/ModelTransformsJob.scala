package ai.chronon.spark

import ai.chronon.api
import ai.chronon.api.{Constants, DataType, PartitionRange}
import ai.chronon.api.DataModel.EVENTS
import ai.chronon.api.Extensions.{MetadataOps, ModelTransformsOps, QueryOps, SourceOps}
import ai.chronon.online.{ModelPlatform, ModelPlatformProvider, PredictRequest}
import ai.chronon.online.serde.SparkConversions
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.Extensions._
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.{DataFrame, Encoders, Row}
import org.apache.spark.sql.types.{StructField, StructType}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Failure, Success}

object ModelTransformsJob {
  @transient private lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  // Batch size for processing rows per Spark task
  private val BatchSize: Int = 100

  // Pre-computed metadata for each model to avoid redundant computation per batch
  private case class ModelMetadata(
      model: api.Model,
      modelPlatform: ModelPlatform,
      modelName: String,
      inputPrefix: String,
      inputFieldIndices: Seq[(String, Int)],
      expectedOutputFields: Seq[String],
      valueSchema: api.DataType
  )

  def computeOutputSchema(sourceDf: DataFrame,
                          modelTransformsConf: api.ModelTransforms,
                          tableUtils: TableUtils): StructType = {
    validateModelTransformsConfig(modelTransformsConf)

    val models = Option(modelTransformsConf.models).map(_.asScala.toSeq).getOrElse(Seq.empty)
    val passthroughFields = Option(modelTransformsConf.passthroughFields).map(_.asScala.toSet).getOrElse(Set.empty)

    // Always include partition column and time column in passthrough, even if not explicitly specified
    val requiredColumns = Set(tableUtils.partitionColumn, Constants.TimeColumn)
    val allPassthroughFields = passthroughFields ++ requiredColumns.filter(sourceDf.columns.contains)

    if (models.isEmpty) {
      logger.info("No models defined, returning passthrough fields only")
      val fieldsToSelect = sourceDf.columns.filter(allPassthroughFields.contains)
      return StructType(sourceDf.schema.fields.filter(f => fieldsToSelect.contains(f.name)))
    }

    // Apply input mappings using the existing applyAllMappings method
    val dfWithInputMappings = applyAllMappings(sourceDf,
                                               models,
                                               getMappingFn = _.inputMapping,
                                               getPrefixFn = model => s"${model.metaData.cleanName}__input")

    // Add model inference output fields
    val schemaAfterInference = determineInferenceOutputSchema(dfWithInputMappings.schema, models)

    // Create an empty DataFrame with the post-inference schema
    val sparkSession = tableUtils.sparkSession
    val emptyDfAfterInference = sparkSession.createDataFrame(
      java.util.Collections.emptyList[Row](),
      schemaAfterInference
    )

    // Apply output mappings using the existing applyAllMappings method
    val dfWithOutputMappings = applyAllMappings(emptyDfAfterInference,
                                                models,
                                                getMappingFn = _.outputMapping,
                                                getPrefixFn = model => model.metaData.cleanName)

    // Select only the fields we want to keep: passthrough fields + final model output fields
    val fieldsToKeep = computeFinalFieldsToKeep(models, allPassthroughFields)

    StructType(dfWithOutputMappings.schema.fields.filter(f => fieldsToKeep.contains(f.name)))
  }

  def computeBackfill(
      modelTransformsConf: api.ModelTransforms,
      dateRange: PartitionRange,
      tableUtils: TableUtils,
      modelPlatformProvider: ModelPlatformProvider,
      timeoutMillis: Long = 60000
  ): Unit = {

    validateModelTransformsConfig(modelTransformsConf)
    require(modelPlatformProvider != null, "ModelPlatformProvider cannot be null")

    val outputTable = modelTransformsConf.metaData.outputTable
    logger.info(
      s"Starting ModelTransforms backfill for ${modelTransformsConf.metaData.name} " +
        s"from ${dateRange.start} to ${dateRange.end}")

    val sourceDf = readSource(modelTransformsConf, dateRange, tableUtils)
    if (sourceDf.isEmpty) {
      logger.warn(s"No source data found for range $dateRange. Skipping backfill.")
      return
    }
    logger.info(s"Read ${sourceDf.count()} rows from source")

    // invoke the various models to create an enriched dataframe with model outputs
    val resultDf = processWithModels(sourceDf, modelTransformsConf, modelPlatformProvider, timeoutMillis, tableUtils)

    logger.info(s"Saving results to $outputTable")
    resultDf.save(outputTable, tableProperties = modelTransformsConf.metaData.tableProps)

    logger.info(s"Successfully completed ModelTransforms backfill for ${modelTransformsConf.metaData.name}")
  }

  private def readSource(
      modelTransformsConf: api.ModelTransforms,
      dateRange: PartitionRange,
      tableUtils: TableUtils
  ): DataFrame = {

    def scanTableWithPartitioning(query: api.Query, table: String): DataFrame = {
      val timeProjection = Seq(Constants.TimeColumn -> Option(query).map(_.timeColumn).orNull)

      // Get effective partition spec and range
      val effectiveSpec =
        Option(query).map(_.partitionSpec(tableUtils.partitionSpec)).getOrElse(tableUtils.partitionSpec)
      val effectiveRange = dateRange.intersectingRange(effectiveSpec)
      val partitionColumn = effectiveSpec.column

      val df = tableUtils.scanDf(
        query,
        table,
        Some(Map(partitionColumn -> null) ++ timeProjection),
        range = Some(effectiveRange)
      )

      df.translatePartitionSpec(effectiveSpec, tableUtils.partitionSpec)
    }

    val maybeJoinSource = modelTransformsConf.joinSource

    if (maybeJoinSource.nonEmpty) {
      val joinSource = maybeJoinSource.get
      val joinMetadata = Option(joinSource.join).map(_.metaData)
      val joinOutputTable = joinMetadata.map(_.outputTable).orNull

      require(joinOutputTable != null, s"Join $joinMetadata must have an output table defined")
      logger.info(s"Reading from JoinSource based on Join: ${joinMetadata.map(_.name)}, table: $joinOutputTable")

      scanTableWithPartitioning(joinSource.query, joinOutputTable)
    } else {
      val sources = Option(modelTransformsConf.sources)
        .map(_.asScala.toSeq)
        .getOrElse(Seq.empty)
      require(sources.size == 1, "ModelTransforms must have exactly one source defined (multi-src not supported yet)")
      val source = sources.head
      require(source.dataModel == EVENTS, "Only EVENTS sources are currently supported for ModelTransforms backfill")

      logger.info(s"Reading from source table: ${source.table}")

      scanTableWithPartitioning(source.query, source.table)
    }
  }

  private def processWithModels(
      sourceDf: DataFrame,
      modelTransformsConf: api.ModelTransforms,
      modelPlatformProvider: ModelPlatformProvider,
      timeoutMillis: Long,
      tableUtils: TableUtils
  ): DataFrame = {

    val models = Option(modelTransformsConf.models).map(_.asScala.toSeq).getOrElse(Seq.empty)
    val passthroughFields = Option(modelTransformsConf.passthroughFields).map(_.asScala.toSet).getOrElse(Set.empty)

    // Always include partition column and time column in passthrough, even if not explicitly specified
    val requiredColumns = Set(tableUtils.partitionColumn, Constants.TimeColumn)
    val allPassthroughFields = passthroughFields ++ requiredColumns.filter(sourceDf.columns.contains)

    if (models.isEmpty) {
      logger.info("No models defined, returning passthrough fields only")
      val fieldsToSelect = sourceDf.columns.filter(allPassthroughFields.contains)
      return sourceDf.select(fieldsToSelect.map(col): _*)
    }

    logger.info(s"Processing with ${models.size} models, batch size: $BatchSize")

    processBatchedModelInference(
      sourceDf,
      models,
      allPassthroughFields,
      modelPlatformProvider,
      timeoutMillis
    )
  }

  private def processBatchedModelInference(
      sourceDf: DataFrame,
      models: Seq[api.Model],
      passthroughFields: Set[String],
      modelPlatformProvider: ModelPlatformProvider,
      timeoutMillis: Long
  ): DataFrame = {

    logger.info(s"Source schema:\n${sourceDf.schema.catalogString}")

    // apply input mappings for all models. We produce intermediate cols prefixed with the model name
    val dfWithInputMappings = applyAllMappings(sourceDf,
                                               models,
                                               getMappingFn = _.inputMapping,
                                               getPrefixFn = model => s"${model.metaData.cleanName}__input")
    logger.info(s"Schema after input mappings:\n${dfWithInputMappings.schema.catalogString}")

    val dfWithInferenceResults =
      batchedModelInference(dfWithInputMappings, models, modelPlatformProvider, timeoutMillis)
    logger.info(s"Schema after model inference:\n${dfWithInferenceResults.schema.catalogString}")

    val dfWithOutputMappings = applyAllMappings(dfWithInferenceResults,
                                                models,
                                                getMappingFn = _.outputMapping,
                                                getPrefixFn = model => model.metaData.cleanName)
    logger.info(s"Schema after output mappings:\n${dfWithOutputMappings.schema.catalogString}")

    // Select only the fields we want to keep: passthrough fields + final model output fields
    val fieldsToKeep = computeFinalFieldsToKeep(models, passthroughFields)

    val finalDf = dfWithOutputMappings.select(fieldsToKeep.map(col): _*)
    logger.info(s"Final schema after selecting fields:\n${finalDf.schema.catalogString}")

    finalDf
  }

  // Used to apply input or output mappings for all models
  private def applyAllMappings(
      df: DataFrame,
      models: Seq[api.Model],
      getMappingFn: api.Model => java.util.Map[String, String],
      getPrefixFn: api.Model => String
  ): DataFrame = {
    def applyMapping(df: DataFrame, model: api.Model): DataFrame = {
      val mapping = Option(getMappingFn(model)).map(_.asScala.toMap).getOrElse(Map.empty)

      if (mapping.isEmpty) {
        return df
      }

      val fieldPrefix = getPrefixFn(model)
      // Add new columns with SQL expressions, prefixed appropriately
      mapping.foldLeft(df) { case (accDf, (outputField, sqlExpr)) =>
        val prefixedFieldName = s"${fieldPrefix}__$outputField"
        accDf.withColumn(prefixedFieldName, expr(sqlExpr))
      }
    }

    models.foldLeft(df) { (accDf, model) =>
      applyMapping(accDf, model)
    }
  }

  // Kick off model inference for all the models.
  // We process the Dataframe in batches and for each batch make a bulk call to each of the models
  // This is stitched together to produce the final Dataframe with inference results
  private def batchedModelInference(
      dfWithInputMappings: DataFrame,
      models: Seq[api.Model],
      modelPlatformProvider: ModelPlatformProvider,
      timeoutMillis: Long
  ): DataFrame = {
    val schema = dfWithInputMappings.schema

    // Pre-compute model metadata
    val modelMetadataSeq = buildModelMetadata(schema, models, modelPlatformProvider)

    // Determine output schema (original schema + inference result fields)
    val outputSchema = determineInferenceOutputSchema(schema, models)

    // Process in batches via mapPartitions
    dfWithInputMappings.mapPartitions { rows =>
      rows.grouped(BatchSize).flatMap { batch =>
        processBatch(
          batch,
          modelMetadataSeq,
          timeoutMillis
        )
      }
    }(Encoders.row(outputSchema))
  }

  // Build metadata for all models once to avoid redundant computation per batch
  private def buildModelMetadata(schema: StructType,
                                 models: Seq[api.Model],
                                 modelPlatformProvider: ModelPlatformProvider): Seq[ModelMetadata] = {
    models.map { model =>
      val modelName = model.metaData.cleanName
      val inputPrefix = s"${modelName}__input"
      val inputFieldIndices = extractInputFieldIndices(schema, inputPrefix)
      val expectedOutputFields = getModelOutputFields(model).map(_.name).toSeq

      val inferenceSpec = model.inferenceSpec
      require(inferenceSpec != null, s"Model ${model.metaData.name} missing inference specification")

      val modelPlatform = modelPlatformProvider.getPlatform(
        inferenceSpec.modelBackend,
        Option(inferenceSpec.modelBackendParams).map(_.asScala.toMap).getOrElse(Map.empty)
      )

      logger.info(
        s"Prepared ModelMetadata for model: ${model.metaData.name}, " +
          s"inputPrefix: $inputPrefix, inputFields: [${inputFieldIndices.map(_._1).mkString(", ")}], " +
          s"expectedOutputFields: [${expectedOutputFields.mkString(", ")}]")

      ModelMetadata(
        model = model,
        modelPlatform = modelPlatform,
        modelName = modelName,
        inputPrefix = inputPrefix,
        inputFieldIndices = inputFieldIndices,
        expectedOutputFields = expectedOutputFields,
        valueSchema = DataType.fromTDataType(model.valueSchema)
      )
    }
  }

  private def processBatch(
      batch: Seq[Row],
      modelMetadataSeq: Seq[ModelMetadata],
      timeoutMillis: Long
  ): Seq[Row] = {
    // For each model, extract inputs, call inference, and collect results
    val modelResults = modelMetadataSeq.map { metadata =>
      // Extract input fields for this model from each row using pre-computed indices
      val modelInputs = batch.map { row =>
        metadata.inputFieldIndices.map { case (fieldName, idx) =>
          // Remove prefix to get original field name
          val originalFieldName = fieldName.stripPrefix(s"${metadata.inputPrefix}__")
          val value = if (row.isNullAt(idx)) {
            null
          } else {
            row.get(idx) match {
              case structRow: Row => SparkConversions.sparkRowToMap(structRow)
              case other          => other.asInstanceOf[AnyRef]
            }
          }
          originalFieldName -> value
        }.toMap
      }

      val inferenceResults = callModelInference(metadata.model, modelInputs, metadata.modelPlatform, timeoutMillis)

      (inferenceResults, metadata.expectedOutputFields)
    }

    // Combine original rows with model results
    batch.zipWithIndex.map { case (row, idx) =>
      val originalValues = row.toSeq
      val inferenceValues = modelResults.zip(modelMetadataSeq).flatMap {
        case ((results: Seq[Map[String, AnyRef]], expectedFields: Seq[String]), metadata) =>
          val resultMap = results.lift(idx).getOrElse(Map.empty)
          // Extract values in the order of expectedFields, converting to Spark types using SparkConversions
          expectedFields.map { fieldName =>
            val value = resultMap.getOrElse(fieldName, null)
            // Convert value to Spark type if we have a value schema
            if (value != null && metadata.valueSchema.isInstanceOf[api.StructType]) {
              val structType = metadata.valueSchema.asInstanceOf[api.StructType]
              structType.fields
                .find(_.name == fieldName)
                .map { field =>
                  SparkConversions.toSparkRow(value, field.fieldType)
                }
                .getOrElse(value)
            } else {
              value
            }
          }
      }
      Row.fromSeq(originalValues ++ inferenceValues)
    }
  }

  private def extractInputFieldIndices(schema: StructType, prefix: String): Seq[(String, Int)] = {
    schema.fields.zipWithIndex
      .filter { case (field, _) => field.name.startsWith(s"${prefix}__") }
      .map { case (field, idx) => (field.name, idx) }
  }

  private def callModelInference(
      model: api.Model,
      inputs: Seq[Map[String, AnyRef]],
      modelPlatform: ModelPlatform,
      timeoutMillis: Long
  ): Seq[Map[String, AnyRef]] = {
    // Create predict request
    val predictRequest = PredictRequest(model, inputs)

    // Call model and wait for result
    val predictResponseFuture = modelPlatform.predict(predictRequest)
    val predictResponse = Await.result(predictResponseFuture, timeoutMillis.milliseconds)

    predictResponse.outputs match {
      case Success(predictions) =>
        require(predictions.size == inputs.size,
                s"Model ${model.metaData.name} returned ${predictions.size} predictions but expected ${inputs.size}")
        predictions

      case Failure(exception) =>
        logger.error(s"Model ${model.metaData.name} inference failed: ${exception.getMessage}", exception)
        // Return empty maps for all inputs
        inputs.map(_ => Map.empty[String, AnyRef])
    }
  }

  private def getModelOutputFields(model: api.Model): Array[StructField] = {
    val chrononDataType = DataType.fromTDataType(model.valueSchema)
    chrononDataType match {
      case structType: api.StructType =>
        SparkConversions.fromChrononSchema(structType).fields
      case other =>
        // don't expect this as our Python api enforces StructType for model valueSchema
        throw new IllegalStateException(s"Model ${model.metaData.name} valueSchema is not a StructType: $other")
    }
  }

  // Determine the post-model inference schema based on the valueSchema defined on the model
  // object. We prefix these fields with '<modelName>__<field_name>' directly.
  private def determineInferenceOutputSchema(inputSchema: StructType, models: Seq[api.Model]): StructType = {
    val inferenceFields = models.flatMap { model =>
      val modelName = model.metaData.cleanName
      getModelOutputFields(model).map { field =>
        StructField(s"${modelName}__${field.name}", field.dataType, field.nullable)
      }
    }

    StructType(inputSchema.fields ++ inferenceFields)
  }

  private def validateModelTransformsConfig(modelTransformsConf: api.ModelTransforms): Unit = {
    require(modelTransformsConf != null, "ModelTransforms configuration cannot be null")

    // require the models listed in the ModelTransforms to have valueSchema defined
    val models = Option(modelTransformsConf.models).map(_.asScala.toSeq).getOrElse(Seq.empty)
    models.zipWithIndex.foreach { case (model, idx) =>
      require(model.valueSchema != null,
              s"Model at index $idx must have valueSchema defined for ModelTransforms backfill")
      require(model.metaData != null, s"Model at index $idx must have metaData defined for ModelTransforms backfill")
      require(
        model.metaData.name != null && model.metaData.name.nonEmpty,
        s"Model at index $idx must have a non-null, non-empty name in metaData for ModelTransforms backfill. " +
          "Please ensure the compiled model JSON has the 'metaData.name' field set."
      )
    }
  }

  // Compute the final list of fields to keep in the output: passthrough fields + model output fields
  private def computeFinalFieldsToKeep(models: Seq[api.Model], passthroughFields: Set[String]): Seq[String] = {
    val modelOutputFields = models.flatMap { model =>
      val modelName = model.metaData.cleanName

      // If output mappings exist, keep only those fields
      // Otherwise, keep all raw model output fields
      if (Option(model.outputMapping).exists(!_.isEmpty)) {
        model.outputMapping.asScala.keys.map(k => s"${modelName}__$k")
      } else {
        getModelOutputFields(model).map(f => s"${modelName}__${f.name}")
      }
    }

    passthroughFields.toSeq ++ modelOutputFields
  }
}
