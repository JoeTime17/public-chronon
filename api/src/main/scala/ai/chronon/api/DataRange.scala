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

package ai.chronon.api

sealed trait DataRange {
  def toTimePoints: Array[Long]
}

case class PartitionInterval(startMillis: Long, endMillis: Long) {
  require(startMillis <= endMillis, s"Invalid interval [$startMillis, $endMillis)")

  def isEmpty: Boolean = startMillis == endMillis

  def contains(timestampMillis: Long): Boolean =
    startMillis <= timestampMillis && timestampMillis < endMillis

  def overlaps(other: PartitionInterval): Boolean =
    startMillis < other.endMillis && other.startMillis < endMillis

  def shift(startDeltaMillis: Long, endDeltaMillis: Long): PartitionInterval =
    PartitionInterval(startMillis + startDeltaMillis, endMillis + endDeltaMillis)
}

case class TimeRange(start: Long, end: Long)(implicit partitionSpec: PartitionSpec) extends DataRange {
  def toTimePoints: Array[Long] = {
    Stream
      .iterate(partitionSpec.grid.leftBound(start))(_ + partitionSpec.spanMillis)
      .takeWhile(_ <= end)
      .toArray
  }

  def toPartitionRange: PartitionRange = {
    PartitionRange(partitionSpec.at(start), partitionSpec.at(end))
  }

  def pretty: String = s"start:[${TsUtils.toStr(start)}]-end:[${TsUtils.toStr(end)}]"
  override def toString: String = s"[${TsUtils.toStr(start)}-${TsUtils.toStr(end)}]"
}
// start and end can be null - signifies unbounded-ness
case class PartitionRange(start: String, end: String)(implicit val partitionSpec: PartitionSpec)
    extends DataRange
    with Ordered[PartitionRange] {

  def valid: Boolean = {
    (Option(start), Option(end)) match {
      case (Some(s), Some(e)) => partitionSpec.partitionStartMillis(s) <= partitionSpec.partitionStartMillis(e)
      case _                  => true
    }
  }

  def isSinglePartition: Boolean = {
    start == end
  }

  def intersect(other: PartitionRange): PartitionRange = {
    val newStart = (Option(start).map(partitionSpec.partitionStartMillis) ++
      Option(other.start).map(other.partitionSpec.partitionStartMillis))
      .reduceLeftOption(Ordering[Long].max)
      .map(partitionSpec.at)
      .orNull
    val newEnd = (Option(end).map(partitionSpec.partitionEndMillis) ++
      Option(other.end).map(other.partitionSpec.partitionEndMillis))
      .reduceLeftOption(Ordering[Long].min)
      .map(endMillis => partitionSpec.at(endMillis - 1))
      .orNull
    // could be invalid
    PartitionRange(newStart, newEnd)
  }

  override def toTimePoints: Array[Long] = {
    assert(start != null && end != null, "Can't request timePoint conversion when PartitionRange is unbounded")
    partitions.map(partitionSpec.partitionStartMillis).toArray
  }

  def startMillis: Long = {
    require(start != null, "Can't request startMillis when PartitionRange start is unbounded")
    partitionSpec.partitionStartMillis(start)
  }

  def endMillis: Long = {
    require(end != null, "Can't request endMillis when PartitionRange end is unbounded")
    partitionSpec.partitionEndMillis(end)
  }

  def interval: PartitionInterval = {
    require(start != null && end != null, "Can't request interval when PartitionRange is unbounded")
    PartitionInterval(startMillis, endMillis)
  }

  def betweenClauses: String = {
    s"${partitionSpec.column} BETWEEN '$start' AND '$end'"
  }

  def whereClauses: Seq[String] = {
    (Option(start).map(s => s"${partitionSpec.column} >= '$s'") ++ Option(end).map(e =>
      s"${partitionSpec.column} <= '$e'")).toSeq
  }

  def steps(partitionCount: Int): Seq[PartitionRange] = {
    partitions
      .sliding(partitionCount, partitionCount) // sliding(x, x) => tumbling(x)
      .map { step => PartitionRange(step.head, step.last) }
      .toSeq
  }

  // thrift ExecutionInfo.stepDays stays day-denominated; sub-daily specs get a day's worth
  // of partitions per step, so existing daily configs behave identically
  def stepsByDays(stepDays: Int): Seq[PartitionRange] =
    steps(math.max(1, stepDays * partitionSpec.grid.partitionsPerDay))

  def partitions: Seq[String] = {
    require(wellDefined, s"Invalid partition range $this")
    val endStartMillis = partitionSpec.partitionStartMillis(end)
    Stream
      .iterate(start)(partitionSpec.after)
      .takeWhile(partition => partitionSpec.partitionStartMillis(partition) <= endStartMillis)
  }

  // no nulls in start or end and start <= end - used as a pre-check before the `partitions` function
  def wellDefined: Boolean =
    start != null && end != null && partitionSpec.partitionStartMillis(start) <= partitionSpec.partitionStartMillis(end)

  def shiftPartitions(partitionCount: Int): PartitionRange = {
    if (partitionCount == 0) {
      this
    } else {
      PartitionRange(partitionSpec.shiftPartitions(start, partitionCount),
                     partitionSpec.shiftPartitions(end, partitionCount))
    }
  }

  def toDateRange: DateRange = {
    new DateRange()
      .setStartDate(start)
      .setEndDate(end)
  }

  def shiftMillis(millis: Long): PartitionRange = {
    if (millis == 0) {
      this
    } else {
      val newStart = if (start == null) {
        null
      } else {
        partitionSpec.at(partitionSpec.partitionStartMillis(start) + millis)
      }

      val newEnd = if (end == null) {
        null
      } else {
        partitionSpec.at(partitionSpec.partitionEndMillis(end) + millis - 1)
      }

      PartitionRange(newStart, newEnd)
    }
  }

  override def compare(that: PartitionRange): Int = {
    def compareDate(left: String, right: String): Int = {
      if (left == right) {
        0
      } else if (left == null) {
        -1
      } else if (right == null) {
        1
      } else {
        java.lang.Long.compare(partitionSpec.partitionStartMillis(left), that.partitionSpec.partitionStartMillis(right))
      }
    }

    val compareStart = compareDate(this.start, that.start)
    if (compareStart != 0) {
      compareStart
    } else {
      compareDate(this.end, that.end)
    }
  }

  /** The range of `otherSpec` partitions whose time intervals intersect this range's time
    * interval. The end derives from the *end* of this range's last interval, so translating a
    * coarse range into a finer spec keeps the whole time interval instead of just the first
    * sub-partition of the last ds.
    */
  def intersectingRange(otherSpec: PartitionSpec): PartitionRange = {
    if (otherSpec == partitionSpec) return this
    val newStart = Option(start).map(d => otherSpec.at(partitionSpec.partitionStartMillis(d))).orNull
    val newEnd = Option(end).map(d => otherSpec.at(partitionSpec.partitionEndMillis(d) - 1)).orNull

    PartitionRange(newStart, newEnd)(otherSpec)
  }

  def translate(otherSpec: PartitionSpec): PartitionRange = {
    if (otherSpec == partitionSpec) this
    else {
      val newStart = Option(start).map(partitionSpec.translate(_, otherSpec)).orNull
      val newEnd = Option(end).map(partitionSpec.translate(_, otherSpec)).orNull
      PartitionRange(newStart, newEnd)(otherSpec)
    }
  }

  /** The last millisecond this range's partitions hold; tolerates an unbounded start (used for
    * readiness checks where only "data through when?" matters)
    */
  def maxMillis: Long = {
    require(end != null, s"max millis undefined for end-unbounded range $this")
    partitionSpec.partitionEndMillis(end) - 1
  }

  override def toString: String = s"[$start...$end]"
}

object PartitionRange {
  def rangesToString(ranges: Iterable[PartitionRange]): String = {
    val tuples = ranges.map(r => s"(${r.start} -> ${r.end})").mkString(", ")
    s"$tuples"
  }

  /** Which `targetSpec` partitions hold data that is fully present in `partitions` (in
    * `sourceSpec`)? The rule: a target partition counts iff its whole time interval is contained
    * in the union of the source partitions' time intervals. Same grid: 1:1 translation. Across
    * grids, candidates are every target partition intersecting a source partition's interval,
    * kept only when ALL the source partitions overlapping them exist. Example: a 3h@01:00
    * partition straddling midnight needs BOTH surrounding daily partitions, and a daily
    * partition needs all 24 hourly partitions.
    */
  def fullyContainedPartitions(partitions: Seq[String],
                               sourceSpec: PartitionSpec,
                               targetSpec: PartitionSpec): Seq[String] = {
    if (sourceSpec.hasSameGrid(targetSpec)) {
      if (sourceSpec == targetSpec) partitions else partitions.map(sourceSpec.translate(_, targetSpec))
    } else {
      val partitionSet = partitions.toSet
      partitions
        .flatMap(p => PartitionRange(p, p)(sourceSpec).intersectingRange(targetSpec).partitions)
        .distinct
        .filter { t => PartitionRange(t, t)(targetSpec).intersectingRange(sourceSpec).partitions.forall(partitionSet) }
    }
  }

  // takes a list of partitions and collapses them into ranges
  // eg: ["2020-01-01", "2020-01-02", "2020-01-03", "2020-01-05", "2020-01-07", "2020-01-08"]
  // will return: [
  //    PartitionRange("2020-01-01", "2020-01-03"),
  //    PartitionRange("2020-01-05", "2020-01-05"),
  //    PartitionRange("2020-01-07", "2020-01-08")
  // ]
  def collapseToRange(partitions: Iterable[String])(implicit partitionSpec: PartitionSpec): Seq[PartitionRange] = {
    if (partitions == null) return null
    var result = Seq.empty[PartitionRange]
    val sortedPartitions = partitions.toSeq.distinct.sortBy(partitionSpec.partitionStartMillis)
    if (sortedPartitions.isEmpty) return result
    var start = sortedPartitions.head
    var end = start
    sortedPartitions.tail.foreach { p =>
      if (partitionSpec.after(end) == p) {
        end = p
      } else {
        val range = PartitionRange(start, end)
        result = result :+ range
        start = p
        end = p
      }
    }
    result :+ PartitionRange(start, end)
  }

  def collapsedPrint(parts: Iterable[String])(implicit partitionSpec: PartitionSpec): String = {
    val ranges = collapseToRange(parts)
    if (ranges == null) return ""
    rangesToString(ranges)
  }

  // takes a collapsed string and expands it back to individual partitions
  // eg: "(2020-01-01 -> 2020-01-03), (2020-01-05 -> 2020-01-05), (2020-01-07 -> 2020-01-08)"
  // will return: ["2020-01-01", "2020-01-02", "2020-01-03", "2020-01-05", "2020-01-07", "2020-01-08"]
  def expandDates(collapsedString: String)(implicit partitionSpec: PartitionSpec): Seq[String] = {
    if (collapsedString == null || collapsedString.trim.isEmpty) return Seq.empty

    try {
      // Parse the collapsed string format: "(start -> end), (start -> end), ..."
      val rangePattern = """\(([^)]+)\)""".r
      val ranges = rangePattern.findAllIn(collapsedString).map(_.stripPrefix("(").stripSuffix(")")).toSeq

      ranges.flatMap { rangeStr =>
        val parts = rangeStr.split(" -> ").map(_.trim)
        if (parts.length == 2) {
          val start = parts(0)
          val end = parts(1)
          PartitionRange(start, end).partitions
        } else {
          Seq.empty
        }
      }
    } catch {
      case _: Exception => Seq.empty
    }
  }

  def toTimeRange(partitionRange: PartitionRange): TimeRange = {
    val spec = partitionRange.partitionSpec
    // PartitionRange.endMillis is exclusive; TimeRange end is inclusive.
    TimeRange(partitionRange.startMillis, partitionRange.endMillis - 1)(spec)
  }
}
