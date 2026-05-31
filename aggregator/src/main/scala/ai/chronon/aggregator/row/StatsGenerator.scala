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

package ai.chronon.aggregator.row

import ai.chronon.api
import ai.chronon.api.Extensions._
import ai.chronon.api.ScalaJavaConversions._
import org.apache.datasketches.kll.KllFloatsSketch
import org.apache.datasketches.memory.Memory

import java.util

/** Module managing FeatureStats Schema, Aggregations to be used by type and aggregator construction.
  *
  * Stats Aggregation has an offline/ batch component and an online component.
  * The metrics defined for stats depend on the schema of the join. The dataTypes and column names.
  * For the online side, we obtain this information from the JoinCodec/valueSchema
  * For the offline side, we obtain this information directly from the outputTable.
  * To keep the schemas consistent we sort the metrics in the schema by name. (one column can have multiple metrics).
  */
object StatsGenerator {

  val nullSuffix = "__null"
  val nullRateSuffix = "__null_rate"
  val zeroSuffix = "__zero"
  val strSuffix = "__str"
  val totalColumn = "total"
  // Leveraged to build a CDF. Consider using native KLLSketch CDF/PMF methods.
  val finalizedPercentilesMerged: Array[Double] = Array(0.01) ++ (5 until 100 by 5).map(_.toDouble / 100) ++ Array(0.99)
  // Leveraged to build candlestick time series.
  val finalizedPercentilesSeries: Array[Double] = Array(0.05, 0.25, 0.5, 0.75, 0.95)
  val ignoreColumns: Seq[String] = Seq(api.Constants.TimeColumn, "ds", "date_key", "date", "datestamp")

  /** InputTransform acts as a signal of how to process the metric.
    *
    * IsNull: Check if the input is null.
    *
    * IsZero: Check if the input equals zero (for numeric columns).
    *
    * Raw: Operate in the input column.
    *
    * One: lit(true) in spark. Used for row counts leveraged to obtain null rate values.
    */
  object InputTransform extends Enumeration {
    type InputTransform = Value
    val IsNull, IsZero, Raw, RawToString, One = Value
  }
  import InputTransform._

  /** MetricTransform represents a single statistic built on top of an input column.
    */
  case class MetricTransform(name: String,
                             expression: InputTransform,
                             operation: api.Operation,
                             suffix: String = "",
                             argMap: util.Map[String, String] = null)

  /** Post processing for finalized values or IRs when generating a time series of stats.
    * In the case of percentiles for examples we reduce to 5 values in order to generate candlesticks.
    */
  def SeriesFinalizer(key: String, value: AnyRef): AnyRef = {
    (key, value) match {
      case (k, _: Array[Byte]) if k.endsWith("percentile") =>
        val sketch = KllFloatsSketch.heapify(Memory.wrap(value.asInstanceOf[Array[Byte]]))
        sketch.getQuantiles(finalizedPercentilesSeries).asInstanceOf[AnyRef]
      case _ => value
    }
  }

  def buildAggPart(m: MetricTransform): api.AggregationPart = {
    val aggPart = new api.AggregationPart()
    aggPart.setInputColumn(s"${m.name}${m.suffix}")
    aggPart.setOperation(m.operation)
    if (m.argMap != null)
      aggPart.setArgMap(m.argMap)
    aggPart.setWindow(WindowUtils.Unbounded)
    aggPart
  }

  /** Build RowAggregator to use for computing stats on a dataframe based on metrics */
  def buildAggregator(metrics: Seq[MetricTransform], selectedSchema: api.StructType): RowAggregator = {
    val aggParts = metrics.flatMap { m => Seq(buildAggPart(m)) }
    new RowAggregator(selectedSchema.unpack, aggParts)
  }

  /** Stats applied to any column */
  def anyTransforms(column: String): Seq[MetricTransform] =
    Seq(MetricTransform(column, InputTransform.IsNull, operation = api.Operation.SUM, suffix = nullSuffix))

  /** Stats applied to numeric columns */
  def numericTransforms(column: String): Seq[MetricTransform] =
    anyTransforms(column) ++ Seq(
      MetricTransform(column, InputTransform.IsZero, operation = api.Operation.SUM, suffix = zeroSuffix),
      MetricTransform(column, InputTransform.Raw, operation = api.Operation.MAX),
      MetricTransform(column, InputTransform.Raw, operation = api.Operation.MIN),
      MetricTransform(column, InputTransform.Raw, operation = api.Operation.AVERAGE),
      MetricTransform(column, InputTransform.Raw, operation = api.Operation.VARIANCE)
    )

  /** For the schema of the data define metrics to be aggregated */
  def buildMetrics(fields: Seq[(String, api.DataType)]): Seq[MetricTransform] = {
    val metrics = fields
      .flatMap { case (name, dataType) =>
        if (ignoreColumns.contains(name)) {
          Seq.empty
        } else if (api.DataType.isNumeric(dataType) && dataType != api.ByteType) {
          // ByteTypes are not supported due to Avro Encodings and limited support on aggregators.
          // Needs to be casted on source if required.
          numericTransforms(name)
        } else {
          anyTransforms(name)
        }
      }
      .sortBy(_.name)
    metrics :+ MetricTransform(totalColumn, InputTransform.One, api.Operation.COUNT)
  }

  /** Build enhanced metrics with cardinality awareness.
    * @param fields Schema fields as (name, dataType) pairs
    * @param cardinalityMap Map of column names to their cardinality ratio (distinct / total_rows).
    *                       Normalizing by row count keeps classification stable across step sizes.
    * @param cardinalityThreshold Ratio threshold: columns with ratio <= threshold are "low cardinality" (default 0.01 = 1%)
    */
  def buildEnhancedMetrics(fields: Seq[(String, api.DataType)],
                           cardinalityMap: Map[String, Double],
                           cardinalityThreshold: Double = 0.01): Seq[MetricTransform] = {
    val metrics = fields
      .flatMap { case (name, dataType) =>
        if (ignoreColumns.contains(name)) {
          Seq.empty
        } else {
          // Skip complex types (List, Map, Struct) for now - they don't support standard aggregations
          dataType match {
            case _: api.ListType | _: api.MapType | _: api.StructType =>
              Seq.empty
            case api.DateType | api.TimestampType =>
              anyTransforms(name)
            case _ =>
              val cardinality = cardinalityMap.getOrElse(name, 0.0)
              val isLowCardinality = cardinality <= cardinalityThreshold
              val isNumeric = api.DataType.isNumeric(dataType) && dataType != api.ByteType

              // Special handling for boolean columns
              if (dataType == api.BooleanType) {
                anyTransforms(name) ++ Seq(
                  // Count true values by summing the raw boolean column (true=1, false=0)
                  MetricTransform(name, InputTransform.Raw, operation = api.Operation.SUM, suffix = "_true")
                )
              } else if (isLowCardinality && isNumeric) {
                // Low-cardinality numeric: get full numeric stats PLUS histogram (as categorical)
                numericTransforms(name) ++ Seq(
                  MetricTransform(name,
                                  InputTransform.RawToString,
                                  operation = api.Operation.UNIQUE_COUNT,
                                  suffix = strSuffix),
                  MetricTransform(name,
                                  InputTransform.RawToString,
                                  operation = api.Operation.HISTOGRAM,
                                  suffix = strSuffix)
                )
              } else if (isLowCardinality) {
                // Low-cardinality string: use histogram with categorical transforms
                anyTransforms(name) ++ Seq(
                  MetricTransform(name, InputTransform.Raw, operation = api.Operation.UNIQUE_COUNT),
                  MetricTransform(name, InputTransform.Raw, operation = api.Operation.HISTOGRAM)
                )
              } else if (isNumeric) {
                // High-cardinality numeric: use percentiles, no histogram
                numericTransforms(name) ++ Seq(
                  MetricTransform(name, InputTransform.Raw, operation = api.Operation.APPROX_UNIQUE_COUNT),
                  MetricTransform(
                    name,
                    InputTransform.Raw,
                    operation = api.Operation.APPROX_PERCENTILE,
                    argMap = Map("percentiles" -> s"[${finalizedPercentilesMerged.mkString(", ")}]").toJava
                  )
                )
              } else {
                // Non-numeric high-cardinality: basic transforms only
                anyTransforms(name) ++ Seq(
                  MetricTransform(name, InputTransform.Raw, operation = api.Operation.APPROX_UNIQUE_COUNT)
                )
              }
          }
        }
      }
      .sortBy(_.name)
    metrics :+ MetricTransform(totalColumn, InputTransform.One, api.Operation.COUNT)
  }

  def lInfKllSketch(sketch1: AnyRef, sketch2: AnyRef, bins: Int = 20): AnyRef = {
    if (sketch1 == null || sketch2 == null) return None
    val sketchIr1 = KllFloatsSketch.heapify(Memory.wrap(sketch1.asInstanceOf[Array[Byte]]))
    val sketchIr2 = KllFloatsSketch.heapify(Memory.wrap(sketch2.asInstanceOf[Array[Byte]]))
    val binsToDoubles = (0 to bins).map(_.toDouble / bins).toArray
    val keySet = sketchIr1.getQuantiles(binsToDoubles).union(sketchIr2.getQuantiles(binsToDoubles))
    var linfSimple = 0.0
    keySet.foreach { key =>
      val cdf1 = sketchIr1.getRank(key)
      val cdf2 = sketchIr2.getRank(key)
      val cdfDiff = Math.abs(cdf1 - cdf2)
      linfSimple = Math.max(linfSimple, cdfDiff)
    }
    linfSimple.asInstanceOf[AnyRef]
  }

  /** PSI is a measure of the difference between two probability distributions.
    * However, it's not defined for cases where a bin can have zero elements in either distribution
    * (meant for continuous measures). In order to support PSI for discrete measures we add a small eps value to
    * perturb the distribution in bins.
    *
    * Existing rules of thumb are: PSI < 0.10 means "little shift", .10<PSI<.25 means "moderate shift",
    * and PSI>0.25 means "significant shift, action required"
    * https://scholarworks.wmich.edu/dissertations/3208
    */
  def PSIKllSketch(reference: AnyRef, comparison: AnyRef, bins: Int = 20, eps: Double = 0.000001): AnyRef = {
    if (reference == null || comparison == null) return None
    val referenceSketch = KllFloatsSketch.heapify(Memory.wrap(reference.asInstanceOf[Array[Byte]]))
    val comparisonSketch = KllFloatsSketch.heapify(Memory.wrap(comparison.asInstanceOf[Array[Byte]]))
    val binsToDoubles = (0 to bins).map(_.toDouble / bins).toArray
    val keySet =
      referenceSketch
        .getQuantiles(binsToDoubles)
        .union(comparisonSketch.getQuantiles(binsToDoubles))
        .distinct
        .sorted
        .toArray
    val referencePMF = regularize(referenceSketch.getPMF(keySet), eps)
    val comparisonPMF = regularize(comparisonSketch.getPMF(keySet), eps)
    var psi = 0.0
    for (i <- referencePMF.indices) {
      psi += (referencePMF(i) - comparisonPMF(i)) * Math.log(referencePMF(i) / comparisonPMF(i))
    }
    psi.asInstanceOf[AnyRef]
  }

  /** Given a PMF add and substract small values to keep a valid probability distribution without zeros */
  def regularize(doubles: Array[Double], eps: Double): Array[Double] = {
    val countZeroes = doubles.count(_ == 0.0)
    if (countZeroes == 0) {
      doubles // If there are no zeros, return the original array
    } else {
      val nonZeroCount = doubles.length - countZeroes
      val replacement = eps * nonZeroCount / countZeroes
      doubles.map {
        case 0.0 => replacement
        case x   => x - eps
      }
    }
  }
}
