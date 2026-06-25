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

import ai.chronon.aggregator.base.TimeTuple
import ai.chronon.aggregator.row.{ColumnAggregator, RowAggregator}
import ai.chronon.aggregator.windowing._
import ai.chronon.api
import ai.chronon.api.{
  Accuracy,
  Constants,
  DataModel,
  ParametricMacro,
  PartitionRange,
  PartitionSpec,
  TimeRange,
  TsUtils
}
import ai.chronon.api.DataModel.{ENTITIES, EVENTS}
import ai.chronon.api.Extensions._
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.online.serde.SparkConversions
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.Extensions._
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, Row, SparkSession}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.types._
import org.apache.spark.util.sketch.BloomFilter
import org.slf4j.{Logger, LoggerFactory}

import java.util
import scala.collection.mutable

class GroupBy(val aggregations: Seq[api.Aggregation],
              val keyColumns: Seq[String],
              val inputDf: DataFrame,
              val mutationDfFn: () => DataFrame = null,
              skewFilter: Option[String] = None,
              finalize: Boolean = true)
    extends Serializable {
  @transient private var tableUtilsOverride: TableUtils = _

  private[spark] def this(aggregations: Seq[api.Aggregation],
                          keyColumns: Seq[String],
                          inputDf: DataFrame,
                          mutationDfFn: () => DataFrame,
                          skewFilter: Option[String],
                          finalize: Boolean,
                          tableUtilsOverride: TableUtils) = {
    this(aggregations, keyColumns, inputDf, mutationDfFn, skewFilter, finalize)
    this.tableUtilsOverride = tableUtilsOverride
  }

  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  protected[spark] val tsIndex: Int = inputDf.schema.fieldNames.indexOf(Constants.TimeColumn)
  protected val selectedSchema: Array[(String, api.DataType)] = SparkConversions.toChrononSchema(inputDf.schema)
  implicit private lazy val tableUtils: TableUtils =
    Option(tableUtilsOverride).getOrElse(TableUtils(inputDf.sparkSession))

  val keySchema: StructType = StructType(keyColumns.map(inputDf.schema.apply).toArray)
  implicit val sparkSession: SparkSession = inputDf.sparkSession
  // distinct inputs to aggregations - post projection types that needs to match with
  // streaming's post projection types etc.,
  val preAggSchema: StructType = if (aggregations != null) {
    StructType(
      aggregations
        .flatMap(agg =>
          Option(agg.buckets)
            .map(_.toScala)
            .getOrElse(Seq.empty[String]) :+
            agg.inputColumn)
        .distinct
        .map(inputDf.schema.apply)
        .toSeq)
  } else {

    val values = inputDf.schema
      .map(_.name)
      .filterNot((keyColumns ++ Constants.ReservedColumns(tableUtils.partitionColumn)).contains)
    val valuesIndices = values.map(inputDf.schema.fieldIndex).toArray

    StructType(valuesIndices.map(inputDf.schema))
  }

  lazy val outputSchema: api.StructType = {
    val columns = if (aggregations != null) {
      windowAggregator.outputSchema
    } else {
      SparkConversions.toChrononSchema(preAggSchema)
    }
    api.StructType("", columns.map(tup => api.StructField(tup._1, tup._2)))
  }

  lazy val aggregationParts: Seq[api.AggregationPart] = aggregations.flatMap(_.unpack)

  private lazy val columnAggregators: Array[ColumnAggregator] =
    new RowAggregator(selectedSchema, aggregationParts).columnAggregators

  // should be only used when aggregations != null
  lazy val aggPartWithSchema: Seq[(api.AggregationPart, api.DataType)] =
    aggregationParts.zip(columnAggregators.map(_.outputType))

  lazy val postAggSchema: StructType = {
    val valueChrononSchema = if (finalize) windowAggregator.outputSchema else windowAggregator.irSchema
    SparkConversions.fromChrononSchema(valueChrononSchema)
  }

  @transient
  protected[spark] lazy val windowAggregator: RowAggregator =
    new RowAggregator(selectedSchema, aggregations.flatMap(_.unpack))

  def snapshotEntitiesBase: Dataset[(Array[Any], Array[Any])] =
    tableUtils.withJobDescription(s"snapshotEntities(${keyColumns.mkString(",")})") {
      val keys = (keyColumns :+ tableUtils.partitionColumn).toArray
      val keyBuilder = FastHashing.generateKeyBuilder(keys, inputDf.schema)
      val localTsIndex = tsIndex
      val (preppedInputDf, hasWindows, partitionTsIndex) = if (aggregations.hasWindows) {
        val partitionTs = "ds_ts"
        val inputWithPartitionTs = inputDf.withPartitionBasedTimestamp(partitionTs, spec = tableUtils.partitionSpec)
        (inputWithPartitionTs, true, inputWithPartitionTs.schema.fieldIndex(partitionTs))
      } else {
        (inputDf, false, -1)
      }

      logger.info(s"""
        |Prepped input schema
        |${preppedInputDf.schema.pretty}
        |""".stripMargin)

      val snapshotAgg =
        new SnapshotEntityAggregator(selectedSchema,
                                     aggregations.flatMap(_.unpack),
                                     hasWindows,
                                     tableUtils.partitionSpec.spanMillis).toColumn.name("agg")
      implicit val tupleEncoder: Encoder[(KeyWithHash, (api.Row, Long))] =
        Encoders.kryo[(KeyWithHash, (api.Row, Long))]
      implicit val keyEncoder: Encoder[KeyWithHash] = Encoders.kryo[KeyWithHash]
      implicit val valueEncoder: Encoder[(api.Row, Long)] = Encoders.kryo[(api.Row, Long)]
      implicit val outputEncoder: Encoder[(Array[Any], Array[Any])] = Encoders.kryo[(Array[Any], Array[Any])]

      preppedInputDf
        .map { row =>
          val key = keyBuilder(row)
          val chrononRow = SparkConversions.toChrononRow(row, localTsIndex): api.Row
          val partitionTs = if (hasWindows) row.getLong(partitionTsIndex) else 0L
          (key, (chrononRow, partitionTs))
        }(tupleEncoder)
        .groupByKey(_._1)(keyEncoder)
        .mapValues(_._2)(valueEncoder)
        .agg(snapshotAgg)
        .map { case (keyWithHash, ir) => (keyWithHash.data, normalizeOrFinalize(ir)) }(outputEncoder)
    }

  def snapshotEntities: DataFrame =
    if (aggregations == null || aggregations.isEmpty) {
      inputDf
    } else {
      toDf(snapshotEntitiesBase, Seq(tableUtils.partitionColumn -> StringType))
    }

  def snapshotEventsBase(partitionRange: PartitionRange,
                         resolution: Resolution = DailyResolution): Dataset[(Array[Any], Array[Any])] =
    tableUtils.withJobDescription(s"snapshotEvents(${keyColumns.mkString(",")})") {
      val endTimes: Array[Long] = partitionRange.toTimePoints
      // add 1 day to the end times to include data [ds 00:00:00.000, ds + 1 00:00:00.000)
      val shiftedEndTimes = endTimes.map(_ + tableUtils.partitionSpec.spanMillis)
      val sawtoothAggregator = new SawtoothAggregator(aggregations, selectedSchema, resolution)
      val hops = hopsAggregate(endTimes.min, resolution)
      val localPartitionSpec = tableUtils.partitionSpec
      implicit val outputEncoder: Encoder[(Array[Any], Array[Any])] = Encoders.kryo[(Array[Any], Array[Any])]

      hops
        .flatMap { case (keys, hopsArrays) =>
          // filter out if the all the irs are nulls
          val irs = sawtoothAggregator.computeWindows(hopsArrays, shiftedEndTimes)
          irs.indices.flatMap { i =>
            val result = normalizeOrFinalize(irs(i))
            if (result.forall(_ == null)) None
            else Some((keys.data :+ localPartitionSpec.at(endTimes(i)), result))
          }
        }(outputEncoder)
    }

  // Calculate snapshot accurate windows for ALL keys at pre-defined "endTimes".
  def snapshotEvents(partitionRange: PartitionRange): DataFrame = {
    val resolution =
      if (tableUtils.partitionSpec.spanMillis < WindowUtils.Day.millis) FiveMinuteResolution
      else DailyResolution
    toDf(snapshotEventsBase(partitionRange, resolution), Seq((tableUtils.partitionColumn, StringType)))
  }

  /** Support for entities with mutations.
    * Three way join between:
    *   Queries: grouped by key and dsOf[ts]
    *   Snapshot[InputDf]: Grouped by key and ds providing a FinalBatchIR to be extended.
    *   Mutations[MutationDf]: Grouped by key and dsOf[MutationTs] providing an array of updates/deletes to be done
    * With this process the components (end of day batchIr + day's mutations + day's queries -> output)
    */
  def temporalEntities(queriesUnfilteredDf: DataFrame, resolution: Resolution = FiveMinuteResolution): DataFrame =
    tableUtils.withJobDescription(s"temporalEntities(${keyColumns.mkString(",")})") {

      // Add extra column to the queries and generate the key hash.
      val queriesDf = queriesUnfilteredDf.removeNulls(keyColumns)
      val timeBasedPartitionColumn = "ds_of_ts"
      val queriesWithTimeBasedPartition =
        queriesDf.withTimeBasedColumn(timeBasedPartitionColumn, spec = tableUtils.partitionSpec)

      val queriesKeyHashFx = FastHashing.generateKeyBuilder(keyColumns.toArray, queriesWithTimeBasedPartition.schema)
      val timeBasedPartitionIndex = queriesWithTimeBasedPartition.schema.fieldIndex(timeBasedPartitionColumn)
      val timeIndex = queriesWithTimeBasedPartition.schema.fieldIndex(Constants.TimeColumn)
      val partitionIndex = queriesWithTimeBasedPartition.schema.fieldIndex(tableUtils.partitionColumn)

      type CKey = (KeyWithHash, String)
      implicit val ckeyEncoder: Encoder[CKey] = Encoders.kryo[CKey]
      implicit val outputPairEncoder: Encoder[(Array[Any], Array[Any])] = Encoders.kryo[(Array[Any], Array[Any])]
      type TimeTupleArray = Array[java.util.ArrayList[Any]]

      // queries by key & ds_of_ts
      val queriesByKeysDs: Dataset[(CKey, TimeTupleArray)] = {
        implicit val tupleEncoder: Encoder[(CKey, java.util.ArrayList[Any])] =
          Encoders.kryo[(CKey, java.util.ArrayList[Any])]
        implicit val resultEncoder: Encoder[(CKey, TimeTupleArray)] =
          Encoders.kryo[(CKey, TimeTupleArray)]

        queriesWithTimeBasedPartition
          .map { row =>
            val ts = row.getLong(timeIndex)
            val partition = row.getString(partitionIndex)
            val key: CKey = (queriesKeyHashFx(row), row.getString(timeBasedPartitionIndex))
            (key, TimeTuple.make(ts, partition))
          }(tupleEncoder)
          .groupByKey(_._1)(ckeyEncoder)
          .mapGroups { (key, iter) =>
            (key, iter.map(_._2).toArray.uniqSort(TimeTuple))
          }(resultEncoder)
      }

      // Snapshot data needs to be shifted. We need to extract the end state of the IR by EOD before mutations.
      // Since partition data for <ds> contains all history up to and including <ds>, we need to join with the previous ds.
      val shiftedColumnName = "end_of_day_ds"
      val shiftedColumnNameTs = "end_of_day_ts"
      val expandedInputDf = inputDf
        .withShiftedPartition(shiftedColumnName, spec = tableUtils.partitionSpec)
        .withPartitionBasedTimestamp(shiftedColumnNameTs, shiftedColumnName, spec = tableUtils.partitionSpec)
      val shiftedColumnIndex = expandedInputDf.schema.fieldIndex(shiftedColumnName)
      val shiftedColumnIndexTs = expandedInputDf.schema.fieldIndex(shiftedColumnNameTs)
      val snapshotKeyHashFx = FastHashing.generateKeyBuilder(keyColumns.toArray, expandedInputDf.schema)
      val sawtoothAggregator =
        new SawtoothMutationAggregator(aggregations,
                                       SparkConversions.toChrononSchema(expandedInputDf.schema),
                                       resolution)
      val localTsIndex = tsIndex

      // end of day IR using aggregator
      val snapshotByKeysDs: Dataset[(CKey, FinalBatchIr)] = {
        implicit val tupleEncoder: Encoder[(CKey, (api.Row, Long))] =
          Encoders.kryo[(CKey, (api.Row, Long))]
        implicit val valueEncoder: Encoder[(api.Row, Long)] = Encoders.kryo[(api.Row, Long)]

        val snapshotAgg = new TemporalSnapshotAggregator(sawtoothAggregator).toColumn.name("agg")

        expandedInputDf
          .map { row =>
            val key: CKey = (snapshotKeyHashFx(row), row.getString(shiftedColumnIndex))
            val chrononRow = SparkConversions.toChrononRow(row, localTsIndex): api.Row
            val shiftedTs = row.getLong(shiftedColumnIndexTs)
            (key, (chrononRow, shiftedTs))
          }(tupleEncoder)
          .groupByKey(_._1)(ckeyEncoder)
          .mapValues(_._2)(valueEncoder)
          .agg(snapshotAgg)
      }

      // Preprocess for mutations: Add a ds of mutation ts column, collect sorted mutations by keys and ds of mutation.
      val mutationDf = mutationDfFn()
      val mutationsTsIndex = mutationDf.schema.fieldIndex(Constants.MutationTimeColumn)
      val mTsIndex = mutationDf.schema.fieldIndex(Constants.TimeColumn)
      val mutationsReversalIndex = mutationDf.schema.fieldIndex(Constants.ReversalColumn)
      val mutationsHashFx = FastHashing.generateKeyBuilder(keyColumns.toArray, mutationDf.schema)
      val mutationPartitionIndex = mutationDf.schema.fieldIndex(tableUtils.partitionColumn)

      // mutations by ds, sorted
      val mutationsByKeysDs: Dataset[(CKey, Array[api.Row])] = {
        implicit val tupleEncoder: Encoder[(CKey, api.Row)] = Encoders.kryo[(CKey, api.Row)]
        implicit val resultEncoder: Encoder[(CKey, Array[api.Row])] = Encoders.kryo[(CKey, Array[api.Row])]

        mutationDf
          .map { row =>
            val key: CKey = (mutationsHashFx(row), row.getString(mutationPartitionIndex))
            (key, SparkConversions.toChrononRow(row, mTsIndex, mutationsReversalIndex, mutationsTsIndex): api.Row)
          }(tupleEncoder)
          .groupByKey(_._1)(ckeyEncoder)
          .mapGroups { (key, iter) =>
            (key, iter.map(_._2).toBuffer.sortWith(_.mutationTs < _.mutationTs).toArray)
          }(resultEncoder)
      }

      // Three-way join using two sequential cogroups
      // First: queries LEFT JOIN snapshots
      type FirstJoinResult = (CKey, TimeTupleArray, Option[FinalBatchIr])
      implicit val firstJoinEncoder: Encoder[FirstJoinResult] = Encoders.kryo[FirstJoinResult]
      val localPartitionSpec = tableUtils.partitionSpec

      val firstJoinDs = queriesByKeysDs
        .groupByKey(_._1)(ckeyEncoder)
        .cogroup(snapshotByKeysDs.groupByKey(_._1)(ckeyEncoder)) {
          (key: CKey, queryIter: Iterator[(CKey, TimeTupleArray)], snapshotIter: Iterator[(CKey, FinalBatchIr)]) =>
            val queriesOpt = queryIter.toSeq.headOption.map(_._2)
            val snapshotOpt = snapshotIter.toSeq.headOption.map(_._2)
            queriesOpt match {
              case Some(queries) => Iterator((key, queries, snapshotOpt))
              case None          => Iterator.empty
            }
        }(firstJoinEncoder)

      // Second: firstJoin LEFT JOIN mutations → output
      val outputDs = firstJoinDs
        .groupByKey(_._1)(ckeyEncoder)
        .cogroup(mutationsByKeysDs.groupByKey(_._1)(ckeyEncoder)) {
          (key: CKey, firstIter: Iterator[FirstJoinResult], mutationsIter: Iterator[(CKey, Array[api.Row])]) =>
            val (keyWithHash, ds) = key
            firstIter.toSeq.headOption match {
              case Some((_, timeQueries, eodIr)) =>
                val sortedQueries = timeQueries.map { TimeTuple.getTs }
                val finalizedEodIr = eodIr.orNull
                val dayMutations = mutationsIter.toSeq.headOption.map(_._2).orNull

                val irs = sawtoothAggregator.lambdaAggregateIrMany(localPartitionSpec.epochMillis(ds),
                                                                   finalizedEodIr,
                                                                   dayMutations,
                                                                   sortedQueries)
                sortedQueries.indices.iterator.flatMap { i =>
                  val result = normalizeOrFinalize(irs(i))
                  val queryTimeTuple = timeQueries(i)
                  Seq((keyWithHash.data ++ queryTimeTuple.toArray, result))
                }
              case None => Iterator.empty
            }
        }(outputPairEncoder)

      toDf(outputDs, Seq(Constants.TimeColumn -> LongType, tableUtils.partitionColumn -> StringType))
    }

  // Use another dataframe with the same key columns and time columns to
  // generate aggregates within the Sawtooth of the time points
  // we expect queries to contain the partition column
  def temporalEvents(queriesUnfilteredDf: DataFrame,
                     queryTimeRange: Option[TimeRange] = None,
                     resolution: Resolution = FiveMinuteResolution): DataFrame =
    tableUtils.withJobDescription(s"temporalEvents(${keyColumns.mkString(",")})") {

      val queriesDf = skewFilter
        .map { queriesUnfilteredDf.filter }
        .getOrElse(queriesUnfilteredDf.removeNulls(keyColumns))

      val TimeRange(minQueryTs, maxQueryTs) = queryTimeRange.getOrElse(queriesDf.calculateTimeRange)
      val hopsDs = hopsAggregate(minQueryTs, resolution)

      def headStart(ts: Long): Long = TsUtils.round(ts, resolution.hopSizes.min)
      queriesDf.validateJoinKeys(inputDf, keyColumns)

      val queriesKeyGen = FastHashing.generateKeyBuilder(keyColumns.toArray, queriesDf.schema)
      val queryTsIndex = queriesDf.schema.fieldIndex(Constants.TimeColumn)
      val queryTsType = queriesDf.schema(queryTsIndex).dataType
      assert(queryTsType == LongType, s"ts column needs to be long type, but found $queryTsType")
      val partitionIndex = queriesDf.schema.fieldIndex(tableUtils.partitionColumn)

      type CKey = (KeyWithHash, Long)
      implicit val ckeyEncoder: Encoder[CKey] = Encoders.kryo[CKey]
      implicit val kwHashEncoder: Encoder[KeyWithHash] = Encoders.kryo[KeyWithHash]
      implicit val outputPairEncoder: Encoder[(Array[Any], Array[Any])] = Encoders.kryo[(Array[Any], Array[Any])]
      type TimeTupleArray = Array[java.util.ArrayList[Any]]

      // group the data to collect all the timestamps by key and headStart
      // uniqSort to produce one row per key, otherwise the mega-join will produce square number of rows.
      val queriesByHeadStartsDs: Dataset[(CKey, TimeTupleArray)] = {
        implicit val tupleEncoder: Encoder[(CKey, java.util.ArrayList[Any])] =
          Encoders.kryo[(CKey, java.util.ArrayList[Any])]
        implicit val resultEncoder: Encoder[(CKey, TimeTupleArray)] =
          Encoders.kryo[(CKey, TimeTupleArray)]

        queriesDf
          .map { row =>
            val tsVal = row.get(queryTsIndex)
            assert(tsVal != null, "ts column cannot be null in left source or query df")
            val ts = tsVal.asInstanceOf[Long]
            val partition = row.getString(partitionIndex)
            val key: CKey = (queriesKeyGen(row), headStart(ts))
            (key, TimeTuple.make(ts, partition))
          }(tupleEncoder)
          .groupByKey(_._1)(ckeyEncoder)
          .mapGroups { (key, iter) =>
            (key, iter.map(_._2).toArray.uniqSort(TimeTuple))
          }(resultEncoder)
      }

      val sawtoothAggregator =
        new SawtoothAggregator(aggregations, selectedSchema, resolution)

      // create the IRs up to minHop accuracy
      val headStartsWithIrsDs: Dataset[(CKey, Array[Any])] = {
        implicit val resultEncoder: Encoder[(CKey, Array[Any])] = Encoders.kryo[(CKey, Array[Any])]

        val headStartsByKey = queriesByHeadStartsDs
          .map(_._1)(ckeyEncoder)
          .groupByKey(_._1)(kwHashEncoder)

        headStartsByKey
          .cogroup(hopsDs.groupByKey(_._1)(kwHashEncoder)) {
            (keyWithHash: KeyWithHash,
             headStartIter: Iterator[CKey],
             hopsIter: Iterator[(KeyWithHash, HopsAggregator.OutputArrayType)]) =>
              val headStartsArray = headStartIter.map(_._2).toArray
              util.Arrays.sort(headStartsArray)
              val hopsOpt = hopsIter.toSeq.headOption.map(_._2)
              val headStartIrs = sawtoothAggregator.computeWindows(hopsOpt.orNull, headStartsArray)
              headStartsArray.indices.iterator.map { i =>
                ((keyWithHash, headStartsArray(i)), headStartIrs(i))
              }
          }(resultEncoder)
      }

      // events by headStart
      val inputKeyGen = FastHashing.generateKeyBuilder(keyColumns.toArray, inputDf.schema)
      val minHeadStart = headStart(minQueryTs)
      val localTsIndex = tsIndex

      val eventsDs: Dataset[(CKey, api.Row)] = {
        implicit val tupleEncoder: Encoder[(CKey, api.Row)] = Encoders.kryo[(CKey, api.Row)]

        inputDf
          .filter(s"${Constants.TimeColumn} between $minHeadStart and $maxQueryTs")
          .map { row =>
            val key: CKey = (inputKeyGen(row), headStart(row.getLong(localTsIndex)))
            (key, SparkConversions.toChrononRow(row, localTsIndex): api.Row)
          }(tupleEncoder)
      }

      // Three-way join using two sequential cogroups
      // First: queriesByHeadStarts LEFT JOIN headStartsWithIrs
      type FirstJoinResult = (CKey, TimeTupleArray, Option[Array[Any]])
      implicit val firstJoinEncoder: Encoder[FirstJoinResult] = Encoders.kryo[FirstJoinResult]

      val firstJoinDs = queriesByHeadStartsDs
        .groupByKey(_._1)(ckeyEncoder)
        .cogroup(headStartsWithIrsDs.groupByKey(_._1)(ckeyEncoder)) {
          (key: CKey, queryIter: Iterator[(CKey, TimeTupleArray)], irIter: Iterator[(CKey, Array[Any])]) =>
            val queriesOpt = queryIter.toSeq.headOption.map(_._2)
            val irOpt = irIter.toSeq.headOption.map(_._2)
            queriesOpt match {
              case Some(queries) => Iterator((key, queries, irOpt))
              case None          => Iterator.empty
            }
        }(firstJoinEncoder)

      // Second: firstJoin LEFT JOIN eventsByHeadStart
      val outputDs = firstJoinDs
        .groupByKey(_._1)(ckeyEncoder)
        .cogroup(eventsDs.groupByKey(_._1)(ckeyEncoder)) {
          (compositeKey: CKey, firstIter: Iterator[FirstJoinResult], eventsIter: Iterator[(CKey, api.Row)]) =>
            val keys = compositeKey._1
            firstIter.toSeq.headOption match {
              case Some((_, queriesWithPartition, headStartIrOpt)) =>
                val events = eventsIter.map(_._2)
                val inputsIt: Iterator[api.Row] = if (events.hasNext) events else null
                val queries = queriesWithPartition.map { TimeTuple.getTs }
                val irs = sawtoothAggregator.cumulate(inputsIt, queries, headStartIrOpt.orNull)
                queries.indices.iterator.map { i =>
                  (keys.data ++ queriesWithPartition(i).toArray, normalizeOrFinalize(irs(i)))
                }
              case None => Iterator.empty
            }
        }(outputPairEncoder)

      toDf(outputDs, Seq(Constants.TimeColumn -> LongType, tableUtils.partitionColumn -> StringType))
    }

  // convert raw data into IRs, collected by hopSizes
  // TODO cache this into a table: interface below
  // Class HopsCacher(keySchema, irSchema, resolution) extends RddCacher[(KeyWithHash, HopsOutput)]
  //  buildTableRow((keyWithHash, hopsOutput)) -> GenericRowWithSchema
  //  buildRddRow(GenericRowWithSchema) -> (keyWithHash, hopsOutput)
  private def hopsAggregate(minQueryTs: Long,
                            resolution: Resolution): Dataset[(KeyWithHash, HopsAggregator.OutputArrayType)] =
    tableUtils.withJobDescription(s"hopsAggregate(${keyColumns.mkString(",")})") {
      val hopsAggregator =
        new HopsAggregator(minQueryTs, aggregations, selectedSchema, resolution)
      val keyBuilder = FastHashing.generateKeyBuilder(keyColumns.toArray, inputDf.schema)
      val localTsIndex = tsIndex

      val hopsAgg = new HopsAggregatorWrapper(hopsAggregator).toColumn.name("agg")
      implicit val tupleEncoder: Encoder[(KeyWithHash, api.Row)] = Encoders.kryo[(KeyWithHash, api.Row)]
      implicit val keyEncoder: Encoder[KeyWithHash] = Encoders.kryo[KeyWithHash]
      implicit val rowEncoder: Encoder[api.Row] = Encoders.kryo[api.Row]

      inputDf
        .map { row => (keyBuilder(row), SparkConversions.toChrononRow(row, localTsIndex): api.Row) }(tupleEncoder)
        .groupByKey(_._1)(keyEncoder)
        .mapValues(_._2)(rowEncoder)
        .agg(hopsAgg)
    }

  protected[spark] def toDf(aggregateDs: Dataset[(Array[Any], Array[Any])],
                            additionalFields: Seq[(String, DataType)]): DataFrame = {
    val finalKeySchema = StructType(keySchema ++ additionalFields.map { case (name, typ) => StructField(name, typ) })
    val flatSchema = StructType(finalKeySchema ++ postAggSchema)
    val flatZSchema = flatSchema.toChrononSchema("Flat")
    val rowEncoder = ExpressionEncoder(flatSchema)
    aggregateDs
      .map { case (keys, values) =>
        val result = new Array[Any](keys.length + values.length)
        System.arraycopy(keys, 0, result, 0, keys.length)
        System.arraycopy(values, 0, result, keys.length, values.length)
        SparkConversions.toSparkRow(result, flatZSchema, GenericRowHandler.func).asInstanceOf[GenericRow]: Row
      }(rowEncoder)
      .toDF()
  }

  private def normalizeOrFinalize(ir: Array[Any]): Array[Any] =
    if (finalize) {
      windowAggregator.finalize(ir)
    } else {
      windowAggregator.normalize(ir)
    }

}

// TODO: truncate queryRange for caching
object GroupBy {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  // Need to use a case class here to allow null matching
  private case class SourceDataProfile(earliestRequired: String, earliestPresent: String, latestAllowed: String)

  // if the source is a join - we are going to materialize the underlying table
  // and replace the joinSource with Event or Entity Source
  def replaceJoinSource(groupByConf: api.GroupBy,
                        queryRange: PartitionRange,
                        tableUtils: TableUtils,
                        computeDependency: Boolean = true,
                        showDf: Boolean = false): api.GroupBy = {

    val result = groupByConf.deepCopy()

    val newSources: java.util.List[api.Source] = groupByConf.sources.toScala.map { source =>
      if (!source.isSetJoinSource) source
      else {
        logger.info("Join source detected. Materializing the join.")
        val joinSource = source.getJoinSource
        val joinConf = joinSource.join

        val joinOutputTable =
          if (computeDependency) {
            JoinUtils.materializeJoin(joinConf, Option(queryRange.start), queryRange.end, tableUtils, showDf)
          } else {
            joinConf.finalOutputTable
          }

        if (computeDependency && showDf) {
          val df = tableUtils.scanDf(query = null, table = joinOutputTable, range = None)
          logger.info(
            s"printing output data from groupby::join_source: ${groupByConf.metaData.name}::${joinConf.metaData.name}")
          df.prettyPrint()
        }

        val topic = joinConf.left.topic
        val newSource = joinSource.toDirectSource(joinOutputTable)

        if (newSource.isSetEvents) {

          val events = newSource.getEvents
          events.setQuery(joinSource.query)
          // set invalid topic to make sure inferAccuracy works as expected
          events.setTopic(topic + Constants.TopicInvalidSuffix)

        } else if (newSource.isSetEntities) {

          val entities = newSource.getEntities
          entities.setQuery(joinSource.query)
          // TODO: PITC backfill of temporal entity tables require mutations to be set & enriched
          // Do note that mutations can only be backfilled if the aggregations are all deletable
          // It is very unlikely that we will ever need to PITC backfill
          // we don't need mutation enrichment for serving
          entities.setMutationTopic(joinConf.left.topic + Constants.TopicInvalidSuffix)

        }

        newSource
      }
    }.toJava

    result.setSources(newSources)
  }

  def inputDf(groupByConfOld: api.GroupBy,
              queryRange: PartitionRange,
              tableUtils: TableUtils,
              computeDependency: Boolean = false): DataFrame = {

    logger.info(s"\n----[Processing GroupBy: ${groupByConfOld.metaData.name}]----")

    val groupByConf = replaceJoinSource(groupByConfOld, queryRange, tableUtils, computeDependency)

    val inputDf = groupByConf.sources.toScala
      .map { source =>
        sourceDf(groupByConf,
                 source,
                 groupByConf.getKeyColumns.toScala,
                 queryRange,
                 tableUtils,
                 groupByConf.maxWindow,
                 groupByConf.inferredAccuracy)

      }
      .reduce { (df1, df2) =>
        // align the columns by name - when one source has select * the ordering might not be aligned
        val columns1 = df1.schema.fields.map(_.name)
        df1.union(df2.selectExpr(columns1: _*))
      }

    def doesNotNeedTime = !Option(groupByConf.getAggregations).exists(_.toScala.needsTimestamp)
    def hasValidTimeColumn = inputDf.schema.find(_.name == Constants.TimeColumn).exists(_.dataType == LongType)

    require(
      doesNotNeedTime || hasValidTimeColumn,
      s"Time column, ts doesn't exists (or is not a LONG type) for groupBy ${groupByConf.metaData.name}, but you either have windowed aggregation(s) or time based aggregation(s) like: " +
        "first, last, firstK, lastK. \n" +
        "Please note that for the entities case, \"ts\" needs to be explicitly specified in the selects."
    )

    // at-least one of the keys should be present in the row.
    val nullFilterClause = groupByConf.keyColumns.toScala.map(key => s"($key IS NOT NULL)").mkString(" OR ")
    inputDf.filter(nullFilterClause)
  }

  def from(groupByConfOld: api.GroupBy,
           queryRange: PartitionRange,
           tableUtils: TableUtils,
           computeDependency: Boolean,
           bloomMapOpt: Option[util.Map[String, BloomFilter]] = None,
           skewFilter: Option[String] = None,
           finalize: Boolean = true,
           showDf: Boolean = false): GroupBy = {
    logger.info(s"\n----[Processing GroupBy: ${groupByConfOld.metaData.name}]----")
    val groupByConf = replaceJoinSource(groupByConfOld, queryRange, tableUtils, computeDependency, showDf)
    val inputDf = groupByConf.sources.toScala
      .map { source =>
        sourceDf(groupByConf,
                 source,
                 groupByConf.getKeyColumns.toScala,
                 queryRange,
                 tableUtils,
                 groupByConf.maxWindow,
                 groupByConf.inferredAccuracy)

      }
      .reduce { (df1, df2) =>
        // align the columns by name - when one source has select * the ordering might not be aligned
        val columns1 = df1.schema.fields.map(_.name)
        df1.union(df2.selectExpr(columns1: _*))
      }

    def doesNotNeedTime = !Option(groupByConf.getAggregations).exists(_.toScala.needsTimestamp)
    def hasValidTimeColumn = inputDf.schema.find(_.name == Constants.TimeColumn).exists(_.dataType == LongType)
    assert(
      doesNotNeedTime || hasValidTimeColumn,
      s"Time column, ts doesn't exists (or is not a LONG type) for groupBy ${groupByConf.metaData.name}, but you either have windowed aggregation(s) or time based aggregation(s) like: " +
        "first, last, firstK, lastK. \n" +
        "Please note that for the entities case, \"ts\" needs to be explicitly specified in the selects."
    )
    val logPrefix = s"gb:{${groupByConf.metaData.name}}:"
    val keyColumns = groupByConf.getKeyColumns.toScala
    val skewFilteredDf = skewFilter
      .map { sf =>
        logger.info(s"$logPrefix filtering using skew filter:\n    $sf")
        val filtered = inputDf.filter(sf)
        filtered
      }
      .getOrElse(inputDf)

    val processedInputDf = bloomMapOpt.map { skewFilteredDf.filterBloom }.getOrElse { skewFilteredDf }

    // at-least one of the keys should be present in the row.
    val nullFilterClause = groupByConf.keyColumns.toScala.map(key => s"($key IS NOT NULL)").mkString(" OR ")
    val nullFiltered = processedInputDf.filter(nullFilterClause)
    if (showDf) {
      logger.info(s"printing input date for groupBy: ${groupByConf.metaData.name}")
      nullFiltered.prettyPrint()
    }

    // Generate mutation Df if required, align the columns with inputDf so no additional schema is needed by aggregator.
    val mutationSources = groupByConf.sources.toScala.filter { source =>
      source.isSetEntities && source.getEntities.isSetMutationTable
    }
    val mutationsColumnOrder = inputDf.columns ++ Constants.MutationFields.map(_.name)

    def mutationDfFn(): DataFrame = {
      val df: DataFrame = if (groupByConf.inferredAccuracy == api.Accuracy.TEMPORAL && mutationSources.nonEmpty) {
        val mutationDf = mutationSources
          .map(ms =>
            sourceDf(
              groupByConf,
              ms,
              groupByConf.getKeyColumns.toScala,
              queryRange.shiftPartitions(1),
              tableUtils,
              groupByConf.maxWindow,
              groupByConf.inferredAccuracy,
              mutations = true
            ))
          .reduce { (df1, df2) =>
            val columns1 = df1.schema.fields.map(_.name)
            df1.union(df2.selectExpr(columns1: _*))
          }
          .selectExpr(mutationsColumnOrder: _*)
        bloomMapOpt.map { mutationDf.filterBloom }.getOrElse { mutationDf }
      } else null

      if (showDf && df != null) {
        logger.info(s"printing mutation data for groupBy: ${groupByConf.metaData.name}")
        df.prettyPrint()
      }

      df
    }

    new GroupBy(
      Option(groupByConf.getAggregations).map(_.toScala).orNull,
      keyColumns,
      nullFiltered,
      mutationDfFn,
      skewFilter = None,
      finalize = finalize,
      tableUtilsOverride = tableUtils
    )
  }

  private def getIntersectedRange(source: api.Source,
                                  queryRange: PartitionRange,
                                  tableUtils: TableUtils,
                                  window: Option[api.Window]): PartitionRange = {

    implicit val tu: TableUtils = tableUtils
    implicit val sourcePartitionSpec: PartitionSpec = source.query.partitionSpec(tableUtils.partitionSpec)
    // intersectingRange so a coarser source still supplies the query range's full time interval
    val effectiveQueryRange = queryRange.intersectingRange(sourcePartitionSpec)
    val sourceStartPartition = sourcePartitionSpec.normalizeStart(source.query.startPartition, tableUtils.partitionSpec)
    val sourceEndPartition = sourcePartitionSpec.normalizeEnd(source.query.endPartition, tableUtils.partitionSpec)

    // from here on down - the math is based entirely on source partition spec
    val PartitionRange(queryStart, queryEnd) = effectiveQueryRange
    val effectiveEnd = (Option(effectiveQueryRange.end) ++ Option(sourceEndPartition))
      .reduceLeftOption(Ordering[String].min)
      .orNull

    val dataProfile: SourceDataProfile = source.dataModel match {
      case ENTITIES => SourceDataProfile(queryStart, sourceStartPartition, effectiveEnd)
      case EVENTS =>
        if (Option(source.getEvents.isCumulative).getOrElse(false)) {
          // lastAvailablePartition normalizes grid-matching ds values to the global format;
          // translate back so all ds values in this method stay in the source's spec
          lazy val latestAvailable: Option[String] =
            tableUtils
              .lastAvailablePartition(source.table,
                                      subPartitionFilters = source.subPartitionFilters,
                                      tablePartitionSpec = Option(sourcePartitionSpec))
              .map { p =>
                if (
                  sourcePartitionSpec.hasSameGrid(tableUtils.partitionSpec) &&
                  sourcePartitionSpec != tableUtils.partitionSpec
                )
                  tableUtils.partitionSpec.translate(p, sourcePartitionSpec)
                else p
              }
          val latestValid: String = Option(sourceEndPartition).getOrElse(latestAvailable.orNull)
          SourceDataProfile(latestValid, latestValid, latestValid)
        } else {
          val minQuery = sourcePartitionSpec.before(queryStart)
          // sawtooth windows round their start down to the tail hop grid (e.g. daily hops for
          // a 14d window), so the scan must reach back to the tail-hop boundary - not just the
          // raw minQuery - window instant. A no-op for daily grids, where minQuery - window
          // already sits on a day boundary; under sub-daily grids it extends the scan back by
          // at most one tail hop.
          val windowStart: String =
            window
              .filterNot(_.isUnboundedSentinel)
              .map { w =>
                val raw = sourcePartitionSpec.partitionStartMillis(minQuery) - w.millis
                sourcePartitionSpec.at(TsUtils.round(raw, FiveMinuteResolution.calculateTailHop(w)))
              }
              .orNull
          lazy val sourceStart = Option(sourceStartPartition).orNull
          SourceDataProfile(windowStart, sourceStart, effectiveEnd)
        }
    }

    val sourceRange = PartitionRange(dataProfile.earliestPresent, dataProfile.latestAllowed)
    val queryableDataRange =
      PartitionRange(dataProfile.earliestRequired, Seq(queryEnd, dataProfile.latestAllowed).max)
    val intersectedRange = sourceRange.intersect(queryableDataRange)
    logger.info(s"""
               |Computing intersected range as:
               |   query range: $queryRange
               |   query window: $window
               |   source table: ${source.table}
               |   source data range: $sourceRange
               |   source start/end: ${source.query.startPartition}/${source.query.endPartition}
               |   normalized source start/end: $sourceStartPartition/$sourceEndPartition
               |   source data model: ${source.dataModel}
               |   queryable data range: $queryableDataRange
               |   intersected range: $intersectedRange
               |""".stripMargin)
    intersectedRange
  }

  private def sourceDf(groupByConf: api.GroupBy,
                       source: api.Source,
                       keys: Seq[String],
                       queryRange: PartitionRange,
                       tableUtils: TableUtils,
                       window: Option[api.Window],
                       accuracy: api.Accuracy,
                       mutations: Boolean = false): DataFrame = {

    val intersectedRange: PartitionRange = getIntersectedRange(source, queryRange, tableUtils, window)
    implicit val tu: TableUtils = tableUtils

    var metaColumns: Map[String, String] = Map(tableUtils.partitionColumn -> source.query.partitionColumn)
    if (mutations) {
      metaColumns ++= Map(
        Constants.ReversalColumn -> source.query.reversalColumn,
        Constants.MutationTimeColumn -> source.query.mutationTimeColumn
      )
    }

    val sourcePartitionSpec = source.query.partitionSpec(tableUtils.partitionSpec)

    val timeMapping = if (source.dataModel == ENTITIES) {
      Option(source.query.timeColumn).map(Constants.TimeColumn -> _)
    } else {
      if (accuracy == api.Accuracy.TEMPORAL) {
        Some(Constants.TimeColumn -> source.query.timeColumn)
      } else {
        val dsBasedTimestamp = // 1 millisecond before ds + 1
          s"(((UNIX_TIMESTAMP(${sourcePartitionSpec.column}, '${sourcePartitionSpec.format}') * 1000) + ${sourcePartitionSpec.spanMillis}) - 1)"

        Some(Constants.TimeColumn -> Option(source.query.timeColumn).getOrElse(dsBasedTimestamp))
      }
    }

    logger.info(s"""
         |Time Mapping: $timeMapping
         |""".stripMargin)
    metaColumns ++= timeMapping

    // typed predicates: against a real timestamp/date column (time_partitioned sources),
    // ds literals cast to NULL and silently empty the scan - epoch bounds are emitted instead
    val scanTable = if (mutations) source.getEntities.mutationTable.cleanSpec else source.table
    val partitionConditions = tableUtils.rangeWheresFor(
      intersectedRange,
      scanTable,
      Option(source.query.partitionColumn).getOrElse(tableUtils.partitionColumn)
    )

    logger.info(s"""
         |Rendering source query:
         |   intersected/effective scan range: $intersectedRange
         |   partitionConditions: $partitionConditions
         |   metaColumns: $metaColumns
         |""".stripMargin)

    if (mutations && !source.getEntities.isSetMutationTable) {
      throw new Exception(s"mutationTopic is not set for groupby ${groupByConf.metaData.name} with Accuracy.TEMPORAL")
    }

    // TODO: maybe drop this
    // chronon run ds macro is only supported for group bys
    val selects = Option(source.query.selects)
      .map(_.toScala.map(keyValue => {
        if (keyValue._2.contains(Constants.ChrononRunDs)) {
          val parametricMacro = ParametricMacro(Constants.ChrononRunDs, _ => queryRange.start)
          (keyValue._1, parametricMacro.replace(keyValue._2))
        } else {
          keyValue
        }
      }))
      .orNull

    tableUtils
      .scanDfBase(
        selects,
        scanTable,
        Option(source.query.wheres).map(_.toScala).getOrElse(Seq.empty[String]),
        partitionConditions,
        Some(metaColumns ++ keys.map(_ -> null))
      )
      .translatePartitionSpec(sourcePartitionSpec, tableUtils.partitionSpec)
  }

  def computeBackfill(groupByConf: api.GroupBy,
                      startPartition: String,
                      endPartition: String,
                      tableUtils: TableUtils,
                      stepDays: Option[Int] = None,
                      skipFirstHole: Boolean = true): Unit = {
    Option(groupByConf.setups).foreach(_.foreach(tableUtils.sql))
    val outputTable = groupByConf.metaData.outputTable
    val tableProps = Option(groupByConf.metaData.tableProperties)
      .map(_.toScala)
      .orNull
    val groupByUnfilledRangesOpt = Option(
      Seq(PartitionRange(startPartition, endPartition)(tableUtils.partitionSpec))
    ) // TODO(tchow): possilbly revert if orchestrator is not yet available.

    if (groupByUnfilledRangesOpt.isEmpty) {
      logger.info(s"""Nothing to backfill for $outputTable - given
           |endPartition of $endPartition
           |backfill start of $startPartition
           |Exiting...""".stripMargin)
      return
    }
    val groupByUnfilledRanges = groupByUnfilledRangesOpt.get
    logger.info(s"group by unfilled ranges: $groupByUnfilledRanges")
    val exceptions = mutable.Buffer.empty[String]
    groupByUnfilledRanges.foreach { groupByUnfilledRange =>
      try {
        val stepRanges = stepDays.map(groupByUnfilledRange.steps).getOrElse(Seq(groupByUnfilledRange))
        logger.info(s"Group By ranges to compute: ${stepRanges.map {
            _.toString()
          }.pretty}")
        stepRanges.zipWithIndex.foreach { case (range, index) =>
          tableUtils.withJobDescription(
            s"GroupBy.backfill(${groupByConf.metaData.name}) $range [${index + 1}/${stepRanges.size}]") {
            logger.info(s"Computing group by for range: $range [${index + 1}/${stepRanges.size}]")
            val groupByBackfill = from(groupByConf, range, tableUtils, computeDependency = true)
            val outputDf = groupByConf.dataModel match {
              // group by backfills have to be snapshot only
              case ENTITIES => groupByBackfill.snapshotEntities
              case EVENTS   => groupByBackfill.snapshotEvents(range)
            }
            if (!groupByConf.hasDerivations) {
              outputDf.save(outputTable, tableProps)
            } else {
              val finalOutputColumns = groupByConf.derivationsScala.finalOutputColumn(outputDf.columns)
              val result = outputDf.select(finalOutputColumns.toSeq: _*)
              result.save(outputTable, tableProps)
            }
            logger.info(s"Wrote to table $outputTable, into partitions: $range")
          }
        }
        logger.info(s"Wrote to table $outputTable for range: $groupByUnfilledRange")

      } catch {
        case err: Throwable =>
          exceptions += s"Error handling range $groupByUnfilledRange : ${err.getMessage}\n${err.traceString}"
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
}

private[spark] class HopsAggregatorWrapper(hopsAggregator: HopsAggregator)
    extends Aggregator[api.Row, HopsAggregator.IrMapType, HopsAggregator.OutputArrayType]
    with Serializable {

  override def zero: HopsAggregator.IrMapType = hopsAggregator.init()
  override def reduce(buf: HopsAggregator.IrMapType, input: api.Row): HopsAggregator.IrMapType =
    hopsAggregator.update(buf, input)
  override def merge(b1: HopsAggregator.IrMapType, b2: HopsAggregator.IrMapType): HopsAggregator.IrMapType =
    hopsAggregator.merge(b1, b2)
  override def finish(buf: HopsAggregator.IrMapType): HopsAggregator.OutputArrayType =
    hopsAggregator.toTimeSortedArray(buf)
  override def bufferEncoder: Encoder[HopsAggregator.IrMapType] = Encoders.kryo[HopsAggregator.IrMapType]
  override def outputEncoder: Encoder[HopsAggregator.OutputArrayType] = Encoders.kryo[HopsAggregator.OutputArrayType]
}

private[spark] class SnapshotEntityAggregator(selectedSchema: Array[(String, api.DataType)],
                                              aggregationParts: Seq[api.AggregationPart],
                                              hasWindows: Boolean,
                                              spanMillis: Long)
    extends Aggregator[(api.Row, Long), Array[Any], Array[Any]]
    with Serializable {

  // RowAggregator contains non-serializable internals (TimedDispatcher) so recreate lazily
  @transient private lazy val windowAggregator = new RowAggregator(selectedSchema, aggregationParts)

  override def zero: Array[Any] = windowAggregator.init
  override def reduce(ir: Array[Any], input: (api.Row, Long)): Array[Any] = {
    val (chrononRow, partitionTs) = input
    if (hasWindows)
      windowAggregator.updateWindowed(ir, chrononRow, partitionTs + spanMillis)
    else
      windowAggregator.update(ir, chrononRow)
    ir
  }
  override def merge(b1: Array[Any], b2: Array[Any]): Array[Any] = windowAggregator.merge(b1, b2)
  override def finish(ir: Array[Any]): Array[Any] = ir
  override def bufferEncoder: Encoder[Array[Any]] = Encoders.kryo[Array[Any]]
  override def outputEncoder: Encoder[Array[Any]] = Encoders.kryo[Array[Any]]
}

private[spark] class TemporalSnapshotAggregator(sawtoothAggregator: SawtoothMutationAggregator)
    extends Aggregator[(api.Row, Long), BatchIr, FinalBatchIr]
    with Serializable {

  override def zero: BatchIr = sawtoothAggregator.init
  override def reduce(batchIr: BatchIr, input: (api.Row, Long)): BatchIr = {
    val (chrononRow, shiftedTs) = input
    sawtoothAggregator.update(shiftedTs, batchIr, chrononRow)
    batchIr
  }
  override def merge(b1: BatchIr, b2: BatchIr): BatchIr = sawtoothAggregator.merge(b1, b2)
  override def finish(batchIr: BatchIr): FinalBatchIr = sawtoothAggregator.finalizeSnapshot(batchIr)
  override def bufferEncoder: Encoder[BatchIr] = Encoders.kryo[BatchIr]
  override def outputEncoder: Encoder[FinalBatchIr] = Encoders.kryo[FinalBatchIr]
}
