package ai.chronon.spark.stats

import ai.chronon.api.ColorPrinter.ColorString
import ai.chronon.api.PartitionRange.{collapseToRange, collapsedPrint, rangesToString}
import ai.chronon.api.{PartitionRange, PartitionSpec, Window}
import ai.chronon.spark.Extensions._
import ai.chronon.spark.catalog.TableUtils
import org.apache.spark.sql.DataFrame

import scala.collection.mutable

// general purpose utility to fill missing partitions
class PartitionRunner[T](verb: String,
                         endDs: String,
                         inputTable: String,
                         outputTable: String,
                         computeFunc: DataFrame => (DataFrame, T),
                         postFunc: Option[T => Unit] = None,
                         // how many days to run at a time
                         stepSize: Int = 30,
                         // how many days to look back in input table
                         inputLookBack: Option[Window] = None,
                         // for a given output partition how far behind is the latest necessary input partition
                         // the earliest necessary input partition would be `output - lag - inputLookBack`
                         lag: Option[Window] = None,
                         // won't compute an output partition if more than this many relevant input partitions are missing
                         maxMissingInputPartitions: Int = 0)(implicit tu: TableUtils) {

  private implicit val partitionSpec: PartitionSpec = tu.partitionSpec

  // returns map of affected partition -> seq of source partitions
  // accounting for lag and input_lookback params
  private[spark] def affectedPartitions(inputPartitions: Seq[String]): Map[String, Seq[String]] = {
    // output partition -> # of missing inputPartitions
    val affectedMap = mutable.Map.empty[String, Seq[String]]
    inputPartitions.sorted.distinct.foreach { hole =>
      val holeMinusLag = partitionSpec.minus(hole, lag)
      val affectedOutputPartitions = inputLookBack
        .map { lookBack =>
          partitionSpec.partitionsFrom(holeMinusLag, lookBack).filter(_ <= endDs)
        }
        .getOrElse(Seq(holeMinusLag))
      affectedOutputPartitions.foreach { outputPartition =>
        affectedMap(outputPartition) = affectedMap.getOrElse(outputPartition, Seq.empty) :+ hole
      }
    }
    affectedMap.toMap
  }

  private def computeInputRange(outputRange: PartitionRange): PartitionRange = {
    val end = partitionSpec.minus(outputRange.end, lag)
    val start = partitionSpec.minus(partitionSpec.minus(outputRange.start, inputLookBack), lag)
    PartitionRange(start, end)
  }

  // takes into account three things - lag, lookback & step size
  // figures out
  //   1. missing output ranges
  //      i. excluding output partitions not computable due to holes in input, based on `maxMissingInputPartitions`
  //      ii. chunking with step size
  //   2. corresponding input ranges - taking into account lag & look_back
  // returns: sequence of tuples of inputRanges and corresponding outputRanges
  private def computeRanges: Seq[(PartitionRange, PartitionRange)] = {
    // find partitions to fill
    tu.partitions(inputTable)
    val inputPartitions = tu.partitions(inputTable).filter(_ <= endDs)
    val inputRange = partitionRange(inputTable)
    val inputHoles = inputRange.partitions.toSet -- inputPartitions.toSet

    // output partition -> # of missing inputPartitions
    val missingHistogram = affectedPartitions(inputHoles.toSeq)
    val outputPartitionsToIgnore = missingHistogram.filter(_._2.length > maxMissingInputPartitions).keys

    val existingOutputPartitions = tu.partitions(outputTable)
    val outputPartitionsToCompute = (inputPartitions.toSet -- existingOutputPartitions) -- outputPartitionsToIgnore

    // TODO: break at this point for multiple input case
    val outputSteps = collapseToRange(outputPartitionsToCompute).flatMap(_.stepsByDays(stepSize))
    val inputSteps = outputSteps.map(computeInputRange)

    println(s"""
         |Table to $verb(input): $inputTable, ${partitionRange(inputTable)}
         |Holes/Missing Partitions in $inputTable: ${collapsedPrint(inputHoles)}
         |
         |Output table: $outputTable, ${partitionRange(outputTable)}
         |
         |Output partitions with # of missing input partitions: [${missingHistogram}]
         |Output partitions to ignore: ${collapsedPrint(outputPartitionsToIgnore)}
         |Pre-existing output partitions: ${collapsedPrint(existingOutputPartitions)}
         |
         |Output Partitions to compute: ${collapsedPrint(outputPartitionsToCompute)}
         |Output Steps:\n  ${rangesToString(outputSteps)}
         |Corresponding Input Steps:\n  ${rangesToString(inputSteps)}
         |""".stripMargin.yellow)

    inputSteps.zip(outputSteps)
  }

  // TODO: hand this over to the control plane once we build it to run in parallel
  //       and to merge racing jobs
  def runInSequence: Option[T] = {
    val ranges = computeRanges
    val n = ranges.length
    var side: Option[T] = None
    ranges.zipWithIndex.foreach { case ((inputRange, outputRange), i) =>
      println(s"""
           |Computing range ${i + 1}/$n
           |input: $inputTable (${inputRange.start} -> ${inputRange.end})
           |output: $outputTable (${outputRange.start} -> ${outputRange.end})
           |""".stripMargin.yellow)
      val inputFilter = inputRange.whereClauses.mkString(" AND ")
      val inputDf = tu.loadTable(inputTable).filter(inputFilter)
      val (outputDf, sideVal) = computeFunc(inputDf)
      side = Option(sideVal)
      if (outputDf.columns.contains(tu.partitionColumn)) {
        outputDf.save(outputTable)
      } else {
        outputDf.save(outputTable, partitionColumns = List.empty)
      }
      println(s"""
           |Finished computing range ${i + 1}/$n
           |input: $inputTable (${inputRange.start} -> ${inputRange.end})
           |output: $outputTable (${outputRange.start} -> ${outputRange.end})
           |""".stripMargin.green)
      postFunc.foreach(_(sideVal))
    }
    side
  }

  def partitionRange(table: String): PartitionRange = {
    val minPartition = tu.firstAvailablePartition(table).orNull
    val maxPartition = tu.lastAvailablePartition(table).orNull
    PartitionRange(minPartition, maxPartition)(partitionSpec)
  }
}
