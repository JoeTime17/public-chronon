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

import ai.chronon.aggregator.windowing._
import ai.chronon.api
import ai.chronon.api.Extensions.{GroupByOps, MetadataOps, SourceOps, TableInfoOps, WindowUtils}
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.api._
import ai.chronon.online.Extensions.ChrononStructTypeOps
import ai.chronon.online.GroupByServingInfoParsed
import ai.chronon.online.metrics.Metrics
import ai.chronon.online.serde.{AvroConversions, SparkConversions}
import ai.chronon.spark.Extensions._
import ai.chronon.spark.catalog.TableUtils
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types
import org.apache.spark.sql.types.{BinaryType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, Row, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.concurrent.duration.DurationInt
import scala.util.Try

class TemporalEventsAggregator(
    endTs: Long,
    aggregations: Seq[api.Aggregation],
    inputSchema: Seq[(String, api.DataType)],
    resolution: Resolution
) extends Aggregator[api.Row, BatchIr, FinalBatchIr]
    with Serializable {

  @transient private lazy val sawtoothAggregator = new SawtoothOnlineAggregator(
    endTs,
    aggregations,
    inputSchema,
    resolution
  )

  override def zero: BatchIr = sawtoothAggregator.init

  override def reduce(buffer: BatchIr, input: api.Row): BatchIr = {
    if (input == null) buffer
    else sawtoothAggregator.update(buffer, input)
  }

  override def merge(b1: BatchIr, b2: BatchIr): BatchIr = {
    sawtoothAggregator.merge(b1, b2)
  }

  override def finish(reduction: BatchIr): FinalBatchIr = {
    sawtoothAggregator.normalizeBatchIr(reduction)
  }

  override def bufferEncoder: Encoder[BatchIr] = Encoders.kryo[BatchIr]
  override def outputEncoder: Encoder[FinalBatchIr] = Encoders.kryo[FinalBatchIr]
}

class TemporalNullCountAggregator(
    endTs: Long,
    aggregations: Seq[api.Aggregation],
    inputSchema: Seq[(String, api.DataType)],
    resolution: Resolution
) extends Aggregator[FinalBatchIr, mutable.HashMap[String, Long], Map[String, Long]]
    with Serializable {

  @transient private lazy val sawtoothAggregator = new SawtoothOnlineAggregator(
    endTs,
    aggregations,
    inputSchema,
    resolution
  )

  override def zero: mutable.HashMap[String, Long] = mutable.HashMap.empty[String, Long]

  override def reduce(buf: mutable.HashMap[String, Long], input: FinalBatchIr): mutable.HashMap[String, Long] = {
    sawtoothAggregator.updateNullCounts(input, buf)
    buf
  }

  override def merge(b1: mutable.HashMap[String, Long],
                     b2: mutable.HashMap[String, Long]): mutable.HashMap[String, Long] = {
    b2.foreach { case (k, v) => b1.update(k, b1.getOrElse(k, 0L) + v) }
    b1
  }

  override def finish(buf: mutable.HashMap[String, Long]): Map[String, Long] = buf.toMap

  override def bufferEncoder: Encoder[mutable.HashMap[String, Long]] =
    Encoders.kryo[mutable.HashMap[String, Long]]
  override def outputEncoder: Encoder[Map[String, Long]] = Encoders.kryo[Map[String, Long]]
}

class GroupByUpload(endPartition: String,
                    groupBy: ai.chronon.spark.GroupBy,
                    uploadPartitionSpec: PartitionSpec = PartitionSpec.daily)
    extends Serializable {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  implicit val sparkSession: SparkSession = groupBy.sparkSession
  implicit private val partitionSpec: PartitionSpec = uploadPartitionSpec

  private val avroSchema = StructType(
    Seq(
      StructField("key_bytes", BinaryType),
      StructField("value_bytes", BinaryType),
      StructField("key_json", StringType),
      StructField("value_json", StringType)
    ))

  private def toAvroDf(
      df: DataFrame,
      keyColumns: Seq[String],
      valueColumns: Seq[String],
      keySchema: StructType,
      valueSchema: StructType,
      jsonPercent: Int
  ): DataFrame = {
    val keyZSchema = keySchema.toChrononSchema("Key")
    val valueZSchema = valueSchema.toChrononSchema("Value")
    val keyToBytes = AvroConversions.encodeBytes(keyZSchema, GenericRowHandler.func)
    val valueToBytes = AvroConversions.encodeBytes(valueZSchema, GenericRowHandler.func)
    val keyToJson = AvroConversions.encodeJson(keyZSchema, GenericRowHandler.func)
    val valueToJson = AvroConversions.encodeJson(valueZSchema, GenericRowHandler.func)
    val jsonPercentDouble = jsonPercent.toDouble / 100

    val keyIndices = keyColumns.map(df.schema.fieldIndex).toArray
    val valueIndices = valueColumns.map(df.schema.fieldIndex).toArray

    val rowEncoder: ExpressionEncoder[Row] = ExpressionEncoder(avroSchema)

    logger.info(s"""
          |key schema:
          |  ${AvroConversions.fromChrononSchema(keyZSchema).toString(true)}
          |value schema:
          |  ${AvroConversions.fromChrononSchema(valueZSchema).toString(true)}
          |""".stripMargin)
    df.map { row =>
      val keys = keyIndices.map(row.get)
      val values = valueIndices.map(row.get)
      val (keyJson, valueJson) = if (math.random() < jsonPercentDouble) {
        (keyToJson(keys), valueToJson(values))
      } else {
        (null, null)
      }
      Row(keyToBytes(keys), valueToBytes(values), keyJson, valueJson)
    }(rowEncoder)
      .toDF()
  }

  private def computeNullCounts(df: DataFrame, valueColumns: Seq[String]): Map[String, Long] = {
    if (valueColumns.isEmpty) return Map.empty
    val nullCountExprs = valueColumns.map(c => coalesce(sum(when(col(c).isNull, 1L).otherwise(0L)), lit(0L)).alias(c))
    val row = df.agg(nullCountExprs.head, nullCountExprs.tail: _*).collect().head
    valueColumns.zipWithIndex.flatMap { case (name, idx) =>
      val count = row.getLong(idx)
      if (count > 0) Some(name -> count) else None
    }.toMap
  }

  def snapshotEntities(jsonPercent: Int = 1): (DataFrame, Map[String, Long]) = {
    if (groupBy.aggregations == null || groupBy.aggregations.isEmpty) {
      val valueColumns = groupBy.preAggSchema.fieldNames.toSeq
      val nullCounts = computeNullCounts(groupBy.inputDf, valueColumns)

      logger.info(s"""
           |pre-agg upload:
           |  input schema: ${groupBy.inputDf.schema.catalogString}
           |    key schema: ${groupBy.keySchema.catalogString}
           |  value schema: ${groupBy.preAggSchema.catalogString}
           |""".stripMargin)

      val kvDf = toAvroDf(
        groupBy.inputDf,
        groupBy.keySchema.fieldNames.toSeq,
        valueColumns,
        groupBy.keySchema,
        groupBy.preAggSchema,
        jsonPercent
      )
      (kvDf, nullCounts)
    } else {
      snapshotEntitiesWithAggregations(jsonPercent)
    }
  }

  private def snapshotEntitiesWithAggregations(jsonPercent: Int): (DataFrame, Map[String, Long]) = {
    val aggregatedDf = groupBy.snapshotEntities
    val valueColumns = groupBy.postAggSchema.fieldNames.toSeq
    val nullCounts = computeNullCounts(aggregatedDf, valueColumns)
    val kvDf = toAvroDf(
      aggregatedDf,
      groupBy.keyColumns,
      valueColumns,
      groupBy.keySchema,
      groupBy.postAggSchema,
      jsonPercent
    )
    (kvDf, nullCounts)
  }

  def snapshotEvents(jsonPercent: Int = 1): (DataFrame, Map[String, Long]) = {
    val aggregatedDf = groupBy.snapshotEvents(PartitionRange(endPartition, endPartition)(partitionSpec))
    val valueColumns = groupBy.postAggSchema.fieldNames.toSeq
    val nullCounts = computeNullCounts(aggregatedDf, valueColumns)
    val kvDf = toAvroDf(
      aggregatedDf,
      groupBy.keyColumns,
      valueColumns,
      groupBy.keySchema,
      groupBy.postAggSchema,
      jsonPercent
    )
    (kvDf, nullCounts)
  }

  def temporalEvents(jsonPercent: Int = 1,
                     resolution: Resolution = FiveMinuteResolution): (DataFrame, Map[String, Long]) = {
    val endTs = partitionSpec.epochMillis(endPartition)
    logger.info(s"TemporalEvents upload end ts: $endTs")

    val inputSchema = groupBy.inputDf.schema
    val chrononSchema = SparkConversions.toChrononSchema(inputSchema)
    val aggregations = groupBy.aggregations
    val tsIndex = groupBy.tsIndex

    val sawtoothOnlineAggregator = new SawtoothOnlineAggregator(endTs, aggregations, chrononSchema, resolution)
    val irSchema = SparkConversions.fromChrononSchema(sawtoothOnlineAggregator.batchIrSchema)

    val temporalAggregator =
      new TemporalEventsAggregator(endTs, aggregations, chrononSchema, resolution).toColumn.name("ir")

    val keyBuilder = FastHashing.generateKeyBuilder(groupBy.keyColumns.toArray, inputSchema)

    val tupleEncoder: Encoder[(KeyWithHash, api.Row)] = Encoders.kryo[(KeyWithHash, api.Row)]
    val keyEncoder: Encoder[KeyWithHash] = Encoders.kryo[KeyWithHash]
    val chrononRowEncoder: Encoder[api.Row] = Encoders.kryo[api.Row]
    val outputEncoder: Encoder[(Array[Any], Array[Any])] = Encoders.kryo[(Array[Any], Array[Any])]

    // Aggregate using Dataset API with Kryo-backed Aggregator
    val rawAggDs = groupBy.inputDf
      .map { row =>
        (keyBuilder(row), SparkConversions.toChrononRow(row, tsIndex): api.Row)
      }(tupleEncoder)
      .groupByKey(_._1)(keyEncoder)
      .mapValues(_._2)(chrononRowEncoder)
      .agg(temporalAggregator)

    rawAggDs.cache()

    // Compute null counts using a Dataset Aggregator
    val nullCountAgg =
      new TemporalNullCountAggregator(endTs, aggregations, chrononSchema, resolution).toColumn.name("nc")
    val batchIrEncoder: Encoder[FinalBatchIr] = Encoders.kryo[FinalBatchIr]
    val nullCounts: Map[String, Long] = rawAggDs
      .map(_._2)(batchIrEncoder)
      .select(nullCountAgg)
      .as(Encoders.kryo[Map[String, Long]])
      .head()

    val aggregatedDs = rawAggDs.map { case (keyWithHash: KeyWithHash, finalIr: FinalBatchIr) =>
      (keyWithHash.data, Array[Any](finalIr.collapsed, finalIr.tailHops))
    }(outputEncoder)

    // Convert to Avro DataFrame
    val keyZSchema = groupBy.keySchema.toChrononSchema("Key")
    val valueZSchema = irSchema.toChrononSchema("Value")
    val keyToBytes = AvroConversions.encodeBytes(keyZSchema, GenericRowHandler.func)
    val valueToBytes = AvroConversions.encodeBytes(valueZSchema, GenericRowHandler.func)
    val keyToJson = AvroConversions.encodeJson(keyZSchema, GenericRowHandler.func)
    val valueToJson = AvroConversions.encodeJson(valueZSchema, GenericRowHandler.func)
    val jsonPercentDouble = jsonPercent.toDouble / 100

    implicit val rowEncoder: ExpressionEncoder[Row] = ExpressionEncoder(avroSchema)

    logger.info(s"""
          |key schema:
          |  ${AvroConversions.fromChrononSchema(keyZSchema).toString(true)}
          |value schema:
          |  ${AvroConversions.fromChrononSchema(valueZSchema).toString(true)}
          |""".stripMargin)

    val avroDf = aggregatedDs
      .map { case (keys: Array[Any], values: Array[Any]) =>
        val (keyJson, valueJson) = if (math.random() < jsonPercentDouble) {
          (keyToJson(keys), valueToJson(values))
        } else {
          (null, null)
        }
        Row(keyToBytes(keys), valueToBytes(values), keyJson, valueJson)
      }(rowEncoder)
      .toDF()

    (avroDf, nullCounts)
  }

}

object GroupByUpload {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  case class UploadResult(kvDf: DataFrame, nullCounts: Map[String, Long])

  // TODO - remove this if spark streaming can't reach hive tables
  private def buildServingInfo(groupByConf: api.GroupBy,
                               tableUtils: TableUtils,
                               endDs: String): GroupByServingInfoParsed = {
    val groupByServingInfo = new GroupByServingInfo()
    // the upload spec: BatchNodeRunner/run construct tableUtils with the node's output spec
    implicit val partitionSpec: PartitionSpec = tableUtils.partitionSpec
    val batchEndDate = partitionSpec.after(endDs)

    val groupBy = ai.chronon.spark.GroupBy
      .from(groupByConf, PartitionRange(endDs, endDs), tableUtils, computeDependency = false)

    groupByServingInfo.setBatchEndDate(batchEndDate)
    // authoritative watermark of this upload: streaming merges events at or after this boundary
    groupByServingInfo.setBatchEndTs(partitionSpec.epochMillis(batchEndDate))
    groupByServingInfo.setGroupBy(groupByConf)
    groupByServingInfo.setKeyAvroSchema(groupBy.keySchema.toAvroSchema("Key").toString(true))
    groupByServingInfo.setSelectedAvroSchema(groupBy.preAggSchema.toAvroSchema("Value").toString(true))
    groupByServingInfo.setDateFormat(partitionSpec.format)
    // thrift contract: absent interval/offset means daily-at-midnight, so emit them for
    // anything else regardless of what the global spec happens to be
    if (!partitionSpec.isDaily) {
      groupByServingInfo.setPartitionInterval(WindowUtils.fromMillis(partitionSpec.spanMillis))
      if (partitionSpec.offsetMillis != 0)
        groupByServingInfo.setPartitionOffset(WindowUtils.fromMillis(partitionSpec.offsetMillis))
    }

    val inputSources = groupByConf.streamingSource.toSeq ++ groupByConf.sources.toScala
    if (inputSources.nonEmpty) {
      def inputSchemaFor(source: api.Source): types.StructType = {
        val (rootTable, query) =
          if (source.isSetModelTransforms) {
            (source.getModelTransforms.metaData.outputTable, new api.Query())
          } else {
            (source.rootTable, source.rootQuery)
          }
        val fullInputSchema = tableUtils.getSchemaFromTable(rootTable)
        val inputSchema: types.StructType =
          if (Option(query.selects).isEmpty) fullInputSchema
          else {
            val timeExpression = ColumnExpression.getTimeExpression(query)
            val selects = query.selects.toScala ++ Map(Constants.TimeColumn -> timeExpression.expression.orNull)

            /** We don't need to actually use the real table here since we're just trying to extract columns
              * from a static query. We use a dummy table here since users with bigquery tables would have three part
              * names instead of a two part typical spark table name
              */
            val streamingQuery =
              QueryUtils.build(selects, "default.dummy_table", query.wheres.toScala)
            val reqColumns = tableUtils.getColumnsFromQuery(streamingQuery)
            types.StructType(fullInputSchema.filter(col => reqColumns.contains(col.name)))
          }
        inputSchema
      }

      val inputFields = mutable.LinkedHashMap.empty[String, types.StructField]
      inputSources.foreach { source =>
        inputSchemaFor(source).fields.foreach { field =>
          if (!inputFields.contains(field.name)) {
            inputFields.put(field.name, field)
          }
        }
      }
      val inputSchema = types.StructType(inputFields.values.toSeq)
      groupByServingInfo.setInputAvroSchema(inputSchema.toAvroSchema(name = "Input").toString(true))
    } else {
      logger.info("Not setting InputAvroSchema to GroupByServingInfo as there are no sources defined.")
    }

    val result = new GroupByServingInfoParsed(groupByServingInfo)
    val firstSource = groupByConf.sources.get(0)
    logger.info(s"""
        |Built GroupByServingInfo for ${groupByConf.metaData.name}:
        |table: ${firstSource.table} / data-model: ${firstSource.dataModel}
        |     keySchema: ${Try(result.keyChrononSchema.catalogString)}
        |   valueSchema: ${Try(result.valueChrononSchema.catalogString)}
        |mutationSchema: ${Try(result.mutationChrononSchema.catalogString)}
        |   inputSchema: ${Try(result.inputChrononSchema.catalogString)}
        |selectedSchema: ${Try(result.selectedChrononSchema.catalogString)}
        |  streamSchema: ${Try(result.streamChrononSchema.catalogString)}
        |""".stripMargin)
    result
  }

  private[spark] def generateDf(groupByConf: api.GroupBy,
                                endDs: String,
                                showDf: Boolean = false,
                                tableUtils: TableUtils,
                                jsonPercent: Int = 1,
                                maybeContext: Option[Metrics.Context] = None): UploadResult = {
    implicit val partitionSpec: PartitionSpec = tableUtils.partitionSpec
    Option(groupByConf.setups).foreach(_.foreach(tableUtils.sql))
    // add 1 day to the batch end time to reflect data [ds 00:00:00.000, ds + 1 00:00:00.000)
    val batchEndDate = partitionSpec.after(endDs)
    // for snapshot accuracy - we don't need to scan mutations
    lazy val groupBy =
      ai.chronon.spark.GroupBy.from(groupByConf,
                                    PartitionRange(endDs, endDs),
                                    tableUtils,
                                    computeDependency = true,
                                    showDf = showDf)
    lazy val groupByUpload = new GroupByUpload(endDs, groupBy, partitionSpec)
    // for temporal accuracy - we don't need to scan mutations for upload
    // when endDs = xxxx-01-02 the timestamp from airflow is more than (xxxx-01-03 00:00:00)
    // we wait for event partitions of (xxxx-01-02) which contain data until (xxxx-01-02 23:59:59.999)
    lazy val shiftedGroupBy =
      ai.chronon.spark.GroupBy.from(groupByConf,
                                    PartitionRange(endDs, endDs).shiftPartitions(1),
                                    tableUtils,
                                    computeDependency = true,
                                    showDf = showDf)
    lazy val shiftedGroupByUpload = new GroupByUpload(batchEndDate, shiftedGroupBy, partitionSpec)
    // for mutations I need the snapshot from the previous day, but a batch end date of ds +1
    lazy val otherGroupByUpload = new GroupByUpload(batchEndDate, groupBy, partitionSpec)

    logger.info(s"""
                   |GroupBy upload for: ${groupByConf.metaData.team}.${groupByConf.metaData.name}
                   |Accuracy: ${groupByConf.inferredAccuracy}
                   |Data Model: ${groupByConf.dataModel}
                   |""".stripMargin)

    val (kvDf, nullCounts) = (groupByConf.inferredAccuracy, groupByConf.dataModel) match {
      case (Accuracy.SNAPSHOT, DataModel.EVENTS)   => groupByUpload.snapshotEvents(jsonPercent)
      case (Accuracy.SNAPSHOT, DataModel.ENTITIES) => groupByUpload.snapshotEntities(jsonPercent)
      case (Accuracy.TEMPORAL, DataModel.EVENTS)   => shiftedGroupByUpload.temporalEvents(jsonPercent)
      case (Accuracy.TEMPORAL, DataModel.ENTITIES) => otherGroupByUpload.temporalEvents(jsonPercent)
    }

    // Emit null count metrics. We force-flush the OTel meter provider afterwards because
    // the periodic exporter buffers in-process — the JVM exits as soon as upload finishes
    // and the gauges would otherwise be dropped before the next tick. This replaces a racy
    // Thread.sleep(ScrapeWaitSeconds) that overlapped one tick on a hope-and-pray basis.
    maybeContext.foreach { ctx =>
      logger.info(s"Emitting data quality metrics for ${nullCounts.keys.mkString(", ")} ")
      nullCounts.foreach { case (field, count) =>
        ctx.gauge(Metrics.Name.UploadNullCount, count, additionalTags = Map("field" -> field, "endDs" -> endDs))
      }
      ctx.flush(Constants.ScrapeWaitSeconds.seconds.toMillis)
    }

    UploadResult(kvDf, nullCounts)
  }

  def run(groupByConf: api.GroupBy,
          endDs: String,
          tableUtilsOpt: Option[TableUtils] = None,
          showDf: Boolean = false,
          jsonPercent: Int = 1): Unit = {
    import ai.chronon.spark.submission.SparkSessionBuilder
    val tableUtils: TableUtils =
      tableUtilsOpt.getOrElse {
        val sparkSession = SparkSessionBuilder.build(s"groupBy_${groupByConf.metaData.name}_upload")
        RunnerUtils.tableUtilsForMetadata(sparkSession, groupByConf.metaData)
      }
    val context = Metrics.Context(Metrics.Environment.GroupByUpload, groupByConf)
    val startTs = System.currentTimeMillis()
    val result = generateDf(groupByConf = groupByConf,
                            endDs = endDs,
                            showDf = showDf,
                            tableUtils = tableUtils,
                            jsonPercent = jsonPercent,
                            maybeContext = Option(context))
    val kvDf = result.kvDf

    if (showDf) {
      kvDf.prettyPrint()
    }

    val groupByServingInfo = buildServingInfo(groupByConf, tableUtils, endDs).groupByServingInfo

    val metaRows = Seq(
      Row(
        Constants.GroupByServingInfoKey.getBytes(Constants.UTF8),
        ThriftJsonCodec.toJsonStr(groupByServingInfo).getBytes(Constants.UTF8),
        Constants.GroupByServingInfoKey,
        ThriftJsonCodec.toJsonStr(groupByServingInfo)
      ))
    val metaDf = tableUtils.sparkSession.createDataFrame(
      java.util.Arrays.asList(metaRows: _*),
      kvDf.schema
    )

    val sparkConf = tableUtils.sparkSession.conf
    val uploadFormat = sparkConf.getOption(IonPathConfig.UploadFormatKey).getOrElse("parquet")
    val partitionCol =
      sparkConf.getOption(IonPathConfig.PartitionColumnKey).getOrElse(IonPathConfig.DefaultPartitionColumn)
    val uploadDf = kvDf.union(metaDf).withColumn(partitionCol, lit(endDs))

    logger.info(s"GroupBy upload with upload format: $uploadFormat")

    // metaDf adds a single GroupByServingInfoKey row that is always unioned into uploadDf, so the
    // written output is never empty on its own. Guard on kvDf (the actual upload rows) up front so an
    // upload with no data fails uniformly across upload formats, before anything is written.
    if (kvDf.isEmpty) {
      throw new RuntimeException("GroupBy upload resulted in zero rows.")
    }

    if (uploadFormat == "ion") {
      val rootPath = sparkConf.getOption(IonPathConfig.UploadLocationKey)
      val ionPartitionCol =
        if (tableUtils.partitionSpec.spanMillis == PartitionSpec.daily.spanMillis) to_date(col(partitionCol))
        else to_timestamp(col(partitionCol), tableUtils.partitionSpec.format)
      val ionDf = uploadDf.withColumn(partitionCol, ionPartitionCol)
      val result = IonWriter.write(
        ionDf,
        groupByConf.metaData.uploadTable,
        partitionCol,
        endDs,
        rootPath
      )
      context.gauge(Metrics.Name.KeyBytes, result.keyBytes)
      context.gauge(Metrics.Name.ValueBytes, result.valueBytes)
      context.gauge(Metrics.Name.RowCount, result.rowCount)
    } else {
      uploadDf.save(groupByConf.metaData.uploadTable,
                    groupByConf.metaData.tableProps,
                    partitionColumns = List(partitionCol))

      // Metrics are summed from the persisted table with the serving-info meta row excluded; the
      // guard above guarantees there is real data to summarize (a global aggregation returns one row).
      val metricRow = tableUtils
        .loadTable(groupByConf.metaData.uploadTable)
        .where(not(col("key_json").eqNullSafe(Constants.GroupByServingInfoKey)))
        .selectExpr("sum(bit_length(key_bytes))/8", "sum(bit_length(value_bytes))/8", "count(*)")
        .collect()

      context.gauge(Metrics.Name.KeyBytes, metricRow(0).getDouble(0).toLong)
      context.gauge(Metrics.Name.ValueBytes, metricRow(0).getDouble(1).toLong)
      context.gauge(Metrics.Name.RowCount, metricRow(0).getLong(2))
    }

    val jobDuration = (System.currentTimeMillis() - startTs) / 1000
    context.gauge(Metrics.Name.LatencyMinutes, (jobDuration / 60))
    logger.info(s"GroupBy upload completed in $jobDuration seconds")
  }
}
