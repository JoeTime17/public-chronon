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

package ai.chronon.online

import ai.chronon.aggregator.windowing.{ResolutionUtils, SawtoothOnlineAggregator}
import ai.chronon.api.Constants.{ReversalField, TimeField}
import ai.chronon.api.Extensions.{GroupByOps, MetadataOps, WindowOps, WindowUtils}
import ai.chronon.api.ScalaJavaConversions.ListOps
import ai.chronon.api._
import ai.chronon.online.OnlineDerivationUtil.{DerivationFunc, buildDerivationFunction, buildDerivedFields}
import ai.chronon.online.serde._
import org.apache.avro.Schema

// mixin class - with schema
class GroupByServingInfoParsed(val groupByServingInfo: GroupByServingInfo)
    extends GroupByServingInfo(groupByServingInfo)
    with Serializable {

  // only consulted when the explicit batchEndTs watermark is absent (uploads from older versions);
  // lazy so a missing/invalid dateFormat only fails when the fallback is actually exercised
  private lazy val partitionSpec = {
    val intervalMillis = Option(groupByServingInfo.partitionInterval).map(_.millis).getOrElse(WindowUtils.Day.millis)
    val offset = Option(groupByServingInfo.partitionOffset).map(_.millis).getOrElse(0L)
    PartitionSpec("ds", groupByServingInfo.dateFormat, intervalMillis, offset)
  }

  // streaming starts scanning after batchEnd
  lazy val batchEndTsMillis: Long =
    if (groupByServingInfo.isSetBatchEndTs) groupByServingInfo.getBatchEndTs
    else partitionSpec.epochMillis(batchEndDate)
  private def parser = new Schema.Parser()

  val MutationAvroFields: Seq[StructField] = Seq(TimeField, ReversalField)
  val MutationAvroColumns: Seq[String] = MutationAvroFields.map(_.name)

  lazy val aggregator: SawtoothOnlineAggregator = {
    new SawtoothOnlineAggregator(batchEndTsMillis,
                                 groupByServingInfo.groupBy.aggregations.toScala,
                                 valueChrononSchema.fields.map(sf => (sf.name, sf.fieldType)))
  }

  // caching groupBy helper to avoid re-computing batchDataSet,streamingDataset & inferred accuracy
  lazy val groupByOps = new GroupByOps(groupByServingInfo.groupBy)

  lazy val irChrononSchema: StructType =
    StructType.from(s"${groupBy.metaData.cleanName}_IR", aggregator.batchIrSchema)

  @transient lazy val deriveFunc: DerivationFunc = {
    val keySchema = keyCodec.chrononSchema.asInstanceOf[StructType]
    val baseValueSchema = if (groupBy.aggregations == null) {
      selectedChrononSchema
    } else {
      outputChrononSchema
    }
    buildDerivationFunction(groupBy.derivationsScala, keySchema, baseValueSchema)
  }

  val smallestTailHopMillis: Long = ResolutionUtils.getSmallestTailHopMillis(groupByServingInfo.groupBy)

  def keyCodec: AvroCodec = AvroCodec.of(keyAvroSchema)
  @transient lazy val keyChrononSchema: StructType =
    AvroConversions.toChrononSchema(keyCodec.schema).asInstanceOf[StructType]

  lazy val valueChrononSchema: StructType = {
    val valueFields = groupBy.aggregationInputs
      .flatMap(inp => selectedChrononSchema.fields.find(_.name == inp))
    StructType(s"${groupBy.metaData.cleanName}_INPUT_COLS", valueFields)
  }

  lazy val valueAvroSchema: String = {
    AvroConversions.fromChrononSchema(valueChrononSchema).toString()
  }

  def valueAvroCodec: serde.AvroCodec = serde.AvroCodec.of(valueAvroSchema)
  def selectedCodec: serde.AvroCodec = serde.AvroCodec.of(selectedAvroSchema)

  @transient lazy val irAvroToChrononRowConverter: Any => Array[Any] =
    AvroConversions.genericRecordToChrononRowConverter(irChrononSchema)
  lazy val irAvroSchema: String = AvroConversions.fromChrononSchema(irChrononSchema).toString()

  def irCodec: serde.AvroCodec = serde.AvroCodec.of(irAvroSchema)
  def outputCodec: serde.AvroCodec = serde.AvroCodec.of(outputAvroSchema)

  // Start tiling specific variables

  lazy val tiledCodec: TileCodec = new TileCodec(groupBy, valueChrononSchema.fields.map(sf => (sf.name, sf.fieldType)))

  // End tiling specific variables

  def outputChrononSchema: StructType =
    if (groupByServingInfo.groupBy.aggregations == null) {
      selectedChrononSchema
    } else {
      StructType.from(s"${groupBy.metaData.cleanName}_OUTPUT", aggregator.windowedAggregator.outputSchema)
    }

  lazy val outputAvroSchema: String =
    AvroConversions.fromChrononSchema(outputChrononSchema).toString()

  lazy val responseChrononSchema: StructType = {
    val baseValueSchema =
      if (groupByServingInfo.groupBy.aggregations == null) selectedChrononSchema
      else outputChrononSchema
    if (groupBy.hasDerivations) {
      StructType(s"${groupBy.metaData.cleanName}_RESPONSE",
                 buildDerivedFields(groupBy.derivationsScala, keyChrononSchema, baseValueSchema).toArray)
    } else {
      baseValueSchema
    }
  }

  lazy val responseAvroSchema: String =
    AvroConversions.fromChrononSchema(responseChrononSchema).toString()

  def inputChrononSchema: StructType = {
    AvroConversions.toChrononSchema(parser.parse(inputAvroSchema)).asInstanceOf[StructType]
  }

  def selectedChrononSchema: StructType = {
    AvroConversions.toChrononSchema(parser.parse(selectedAvroSchema)).asInstanceOf[StructType]
  }

  // Schema associated to the stored KV value for streaming data.
  // Mutations require reversal column and timestamp for proper computation and the effective timestamp of mutations is
  // the MutationTime. Therefore, the schema in the value now includes timestamp and reversal column.}
  lazy val mutationValueAvroSchema: String = {
    AvroConversions
      .fromChrononSchema(
        StructType(s"${groupBy.metaData.cleanName}_MUTATION_COLS", (valueChrononSchema ++ MutationAvroFields).toArray))
      .toString
  }

  def mutationValueChrononSchema: StructType = {
    AvroConversions.toChrononSchema(parser.parse(mutationValueAvroSchema)).asInstanceOf[StructType]
  }

  def mutationValueAvroCodec: serde.AvroCodec = serde.AvroCodec.of(mutationValueAvroSchema)

  // Schema for data consumed by the streaming job.
  // Needs consistency with mutationDf Schema for backfill group by. (Shared queries)
  // Additional columns used for mutations are stored
  def mutationChrononSchema: StructType = {
    val fields: Seq[StructField] = inputChrononSchema ++ Constants.MutationFields
    StructType("MUTATION_SCHEMA", fields.toArray)
  }

  def streamChrononSchema: StructType = {
    groupByOps.dataModel match {
      case DataModel.EVENTS   => inputChrononSchema
      case DataModel.ENTITIES => mutationChrononSchema
    }
  }
}
