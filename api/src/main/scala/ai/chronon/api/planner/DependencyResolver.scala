package ai.chronon.api.planner

import ai.chronon.api.Extensions._
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.api.{PartitionRange, PartitionSpec, TableDependency, Window}

object DependencyResolver {

  private def minus(partition: String, offset: Window)(implicit partitionSpec: PartitionSpec): String = {
    if (partition == null) return null
    if (offset == null) return null
    partitionSpec.minusFast(partition, offset)
  }

  private def max(partition: String, cutOff: String): String = {
    if (partition == null) return cutOff
    if (cutOff == null) return partition
    Ordering[String].max(partition, cutOff)
  }

  private def min(partition: String, cutOff: String): String = {
    if (partition == null) return cutOff
    if (cutOff == null) return partition
    Ordering[String].min(partition, cutOff)
  }

  def computeOutputRange(parentRange: PartitionRange, tableDep: TableDependency): Option[PartitionRange] = {
    require(parentRange != null, "Parent range cannot be null")
    require(parentRange.start != null, "Parent range start cannot be null")
    require(parentRange.end != null, "Parent range end cannot be null")
    require(parentRange.start <= parentRange.end, "Parent range start must be <= end")

    implicit val partitionSpec: PartitionSpec = parentRange.partitionSpec

    val start =
      if (tableDep.isSetEndOffset) partitionSpec.plus(parentRange.start, tableDep.getEndOffset)
      else parentRange.start
    val end =
      if (tableDep.isSetStartOffset) partitionSpec.plus(parentRange.end, tableDep.getStartOffset)
      else parentRange.end

    if (start != null && end != null && start > end) {
      return None
    }

    Some(PartitionRange(start, end))
  }

  def computeInputRange(queryRange: PartitionRange, tableDep: TableDependency): Option[PartitionRange] = {

    implicit val partitionSpec: PartitionSpec = queryRange.partitionSpec

    require(queryRange != null, "Query range cannot be null")
    require(queryRange.start != null, "Query range start cannot be null")
    require(queryRange.end != null, "Query range end cannot be null")

    val offsetStart = minus(queryRange.start, tableDep.getStartOffset)
    val offsetEnd = minus(queryRange.end, tableDep.getEndOffset)
    val start = max(offsetStart, tableDep.getStartCutOff)
    val end = min(offsetEnd, tableDep.getEndCutOff)

    if (start != null && end != null && start > end) {
      return None
    }

    if (tableDep.tableInfo.isCumulative) {

      // we should always compute the latest possible partition when end_cutoff is not set
      val latestValidInput = Option(tableDep.getEndCutOff).getOrElse(partitionSpec.now)
      val latestValidInputWithOffset = minus(latestValidInput, tableDep.getEndOffset)

      return Some(PartitionRange(latestValidInputWithOffset, latestValidInputWithOffset))

    }

    Some(PartitionRange(start, end))
  }

  def getMissingSteps(requiredPartitionRange: PartitionRange,
                      existingPartitions: Seq[String],
                      stepDays: Int = 1): Seq[PartitionRange] = {
    val requiredPartitions = requiredPartitionRange.partitions

    val missingPartitions = requiredPartitions.filterNot(existingPartitions.contains)
    val missingPartitionRanges = PartitionRange.collapseToRange(missingPartitions)(requiredPartitionRange.partitionSpec)

    val missingSteps = missingPartitionRanges.flatMap(_.steps(stepDays))
    missingSteps
  }
}
