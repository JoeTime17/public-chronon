package ai.chronon.api.planner

import ai.chronon.api.Extensions._
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.api.{PartitionInterval, PartitionRange, PartitionSpec, TableDependency, Window}

object DependencyResolver {

  private def minus(partition: String, offset: Window)(implicit partitionSpec: PartitionSpec): String = {
    if (partition == null) return null
    if (offset == null) return null
    // defense in depth: unbounded offsets mean "no bound", not BCE-era partition arithmetic
    if (offset.isUnboundedSentinel) return null
    partitionSpec.minus(partition, offset)
  }

  private def max(partition: String, cutOff: String)(implicit partitionSpec: PartitionSpec): String = {
    if (partition == null) return cutOff
    if (cutOff == null) return partition
    if (partitionSpec.partitionStartMillis(partition) >= partitionSpec.partitionStartMillis(cutOff)) partition
    else cutOff
  }

  private def min(partition: String, cutOff: String)(implicit partitionSpec: PartitionSpec): String = {
    if (partition == null) return cutOff
    if (cutOff == null) return partition
    if (partitionSpec.partitionStartMillis(partition) <= partitionSpec.partitionStartMillis(cutOff)) partition
    else cutOff
  }

  def computeOutputRange(parentRange: PartitionRange, tableDep: TableDependency): Option[PartitionRange] =
    computeOutputRange(parentRange, tableDep, parentRange.partitionSpec)

  def computeOutputRange(parentRange: PartitionRange,
                         tableDep: TableDependency,
                         outputPartitionSpec: PartitionSpec): Option[PartitionRange] = {
    require(parentRange != null, "Parent range cannot be null")
    require(parentRange.start != null, "Parent range start cannot be null")
    require(parentRange.end != null, "Parent range end cannot be null")
    require(parentRange.wellDefined, "Parent range start must be <= end")

    val childStartMillis = parentRange.startMillis + Option(tableDep.getEndOffset).map(_.millis).getOrElse(0L)
    val childEndMillis = parentRange.endMillis + Option(tableDep.getStartOffset).map(_.millis).getOrElse(0L)
    if (childStartMillis >= childEndMillis) None
    else outputPartitionSpec.rangeIntersecting(PartitionInterval(childStartMillis, childEndMillis))
  }

  def computeInputRange(queryRange: PartitionRange, tableDep: TableDependency): Option[PartitionRange] = {

    require(queryRange != null, "Query range cannot be null")
    require(queryRange.start != null, "Query range start cannot be null")
    require(queryRange.end != null, "Query range end cannot be null")
    require(tableDep.tableInfo != null, "TableDependency.tableInfo cannot be null")

    implicit val inputPartitionSpec: PartitionSpec = tableDep.tableInfo.partitionSpec(queryRange.partitionSpec)
    val startCutOff = inputPartitionSpec.normalizeStart(tableDep.getStartCutOff, queryRange.partitionSpec)
    val endCutOff = inputPartitionSpec.normalizeEnd(tableDep.getEndCutOff, queryRange.partitionSpec)

    val inputEndMillis = queryRange.endMillis - Option(tableDep.getEndOffset).map(_.millis).getOrElse(0L)

    val offsetStart = Option(tableDep.getStartOffset)
      .filterNot(_.isUnboundedSentinel)
      .map(offset => inputPartitionSpec.at(queryRange.startMillis - offset.millis))
      .orNull
    val offsetEnd = inputPartitionSpec.at(inputEndMillis - 1)
    val start = max(offsetStart, startCutOff)
    val end = min(offsetEnd, endCutOff)

    if (
      start != null && end != null &&
      inputPartitionSpec.partitionStartMillis(start) > inputPartitionSpec.partitionStartMillis(end)
    ) {
      return None
    }

    if (tableDep.tableInfo.isCumulative) {

      // we should always compute the latest possible partition when end_cutoff is not set
      val latestValidInput = Option(endCutOff).getOrElse(inputPartitionSpec.now)
      val latestValidInputWithOffset = minus(latestValidInput, tableDep.getEndOffset)

      return Some(PartitionRange(latestValidInputWithOffset, latestValidInputWithOffset)(inputPartitionSpec))

    }

    Some(PartitionRange(start, end)(inputPartitionSpec))
  }

  def getMissingSteps(requiredPartitionRange: PartitionRange,
                      existingPartitions: Seq[String],
                      stepDays: Int = 1): Seq[PartitionRange] = {
    val requiredPartitions = requiredPartitionRange.partitions

    val missingPartitions = requiredPartitions.filterNot(existingPartitions.contains)
    val missingPartitionRanges = PartitionRange.collapseToRange(missingPartitions)(requiredPartitionRange.partitionSpec)

    val missingSteps = missingPartitionRanges.flatMap(_.stepsByDays(stepDays))
    missingSteps
  }
}
