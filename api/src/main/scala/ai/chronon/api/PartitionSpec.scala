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

import ai.chronon.api.Extensions._
import org.apache.commons.lang3.time.FastDateFormat

import java.text.{ParseException, ParsePosition, SimpleDateFormat}
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.{Calendar, Locale, TimeZone}
import scala.collection.mutable.ListBuffer
import scala.util.Try

/** Regular partition boundaries: `{ k * spanMillis + offsetMillis }`, with each partition holding
  * `[start, start + spanMillis)`. Independent of partition format and column.
  */
case class PartitionGrid(spanMillis: Long, offsetMillis: Long = 0L) {
  require(spanMillis > 0, s"Partition interval must be positive, found $spanMillis")
  // day-denominated reasoning (partitionsPerDay, stepsByDays, snapshot/orchestration math) relies
  // on partitions tiling the UTC day; week/month-sized partitions are deliberately unrepresentable
  // (7d boundaries would start on Thursday - epoch day zero; 30d boundaries drift off calendar months)
  require(
    spanMillis == WindowUtils.Day.millis || (spanMillis < WindowUtils.Day.millis && WindowUtils.Day.millis % spanMillis == 0),
    s"Partition interval must divide a UTC day evenly or equal one day, found ${spanMillis}ms. " +
      s"Weekly/monthly cadences are expressed as schedules over daily partitions, not as partition intervals."
  )
  require(
    offsetMillis >= 0 && offsetMillis < spanMillis,
    s"Partition offset must be in [0, interval), found ${offsetMillis}ms for interval ${spanMillis}ms. " +
      s"Declare the canonical offset instead of relying on modular normalization."
  )
  require(
    spanMillis < WindowUtils.Day.millis || offsetMillis == 0,
    s"Daily partitions keep their boundaries at midnight UTC: offsets are only supported on sub-daily grids, " +
      s"found offset ${offsetMillis}ms on a ${spanMillis}ms interval."
  )

  def isDaily: Boolean = spanMillis == WindowUtils.Day.millis && offsetMillis == 0

  /** 1 for daily intervals; used to convert day-denominated configs like stepDays. */
  def partitionsPerDay: Int = math.max(1, (WindowUtils.Day.millis / spanMillis).toInt)

  /** Left boundary of the grid interval containing `millis`. */
  def leftBound(millis: Long): Long =
    millis - Math.floorMod(millis - offsetMillis, spanMillis)

  def floor(millis: Long): Long = leftBound(millis)

  /** True when this grid's interval is an exact multiple of the upstream's interval. */
  def isExactMultipleOf(upstream: PartitionGrid): Boolean =
    spanMillis >= upstream.spanMillis && spanMillis % upstream.spanMillis == 0

  /** Directional: true when this grid's boundaries land on the upstream grid. Example: a 6h@4h
    * grid lines up with a 3h@1h grid (04:00, 10:00, ... are all 3h@1h boundaries); 6h@2h does not.
    */
  def linesUpWith(upstream: PartitionGrid): Boolean =
    Math.floorMod(offsetMillis - upstream.offsetMillis, upstream.spanMillis) == 0L

  /** True when every boundary of this grid is also a boundary of `upstream`. */
  def isBoundarySubsetOf(upstream: PartitionGrid): Boolean =
    isExactMultipleOf(upstream) && linesUpWith(upstream)

  /** Conditional semantic-hash token: the historical daily-at-midnight grid contributes nothing. */
  def semanticToken: Option[String] =
    if (isDaily) None else Some(s"grid:interval_ms=$spanMillis,offset_ms=$offsetMillis")

  def show: String = {
    val offsetMinutes = offsetMillis / WindowUtils.MinuteMillis
    f"${WindowUtils.millisToString(spanMillis)} starting ${offsetMinutes / 60}%02d:${offsetMinutes % 60}%02d"
  }
}

/** Table-facing partition spec: a grid plus the column/format used to parse and render ds values.
  * A ds value names the start of its interval, and `format` must be sortable and precise enough
  * for the grid.
  */
case class PartitionSpec(column: String, format: String, spanMillis: Long, offsetMillis: Long = 0L) {

  private val partitionGrid = PartitionGrid(spanMillis, offsetMillis)
  def grid: PartitionGrid = partitionGrid

  validateFormat()

  private def partitionFormatter =
    DateTimeFormatter
      .ofPattern(format, Locale.US)
      .withZone(ZoneOffset.UTC)

  def expandRange(startDateStr: String, endDateStr: String): List[String] = {
    // Parse/format in UTC with a stable locale
    val tz = TimeZone.getTimeZone("UTC")
    val dateFormat = FastDateFormat.getInstance(format, tz, Locale.US)

    // Parse start and end dates
    val startDate = dateFormat.parse(startDateStr)
    val endDate = dateFormat.parse(endDateStr)

    if (startDate.after(endDate)) return List.empty

    val dates = ListBuffer[String]()

    val calendar = Calendar.getInstance(tz, Locale.US)
    calendar.setTime(startDate)

    while (!calendar.getTime.after(endDate)) {
      dates += dateFormat.format(calendar.getTime)
      calendar.add(calendarGrain(intervalWindow), intervalWindow.length)
    }

    dates.toList
  }

  // SimpleDateFormat is not thread-safe, so cache one strict instance per thread instead of
  // re-building the formatter on every parse; @transient so Spark closures stay serializable
  @transient private lazy val strictParser: ThreadLocal[SimpleDateFormat] = new ThreadLocal[SimpleDateFormat] {
    override def initialValue(): SimpleDateFormat = {
      val formatter = new SimpleDateFormat(format)
      formatter.setTimeZone(TimeZone.getTimeZone("UTC"))
      formatter.setLenient(false)
      formatter
    }
  }

  def epochMillis(partition: String): Long = {
    val formatter = strictParser.get()
    val position = new ParsePosition(0)
    val parsed = formatter.parse(partition, position)
    if (parsed == null || position.getIndex != partition.length) {
      val errorOffset = if (position.getErrorIndex >= 0) position.getErrorIndex else position.getIndex
      throw new ParseException(s"Unparseable date: '$partition'", errorOffset)
    }
    parsed.getTime
  }

  def isDaily: Boolean = grid.isDaily

  /** Same partitionInterval and partitionOffset; ds values translate 1:1 even if column or
    * format differs.
    */
  def hasSameGrid(other: PartitionSpec): Boolean = grid == other.grid

  def at(millis: Long): String = partitionFormatter.format(Instant.ofEpochMilli(grid.leftBound(millis)))

  def partitionStartMillis(partitionValue: String): Long = epochMillis(partitionValue)

  def partitionEndMillis(partitionValue: String): Long = partitionStartMillis(partitionValue) + spanMillis

  def rangeIntersecting(interval: PartitionInterval): Option[PartitionRange] = {
    if (interval.isEmpty) {
      None
    } else {
      Some(PartitionRange(at(interval.startMillis), at(interval.endMillis - 1))(this))
    }
  }

  def before(s: String): String = shiftPartitions(s, -1)

  def calendarGrain(window: Window): Int = window.timeUnit match {
    case TimeUnit.DAYS    => Calendar.DAY_OF_MONTH
    case TimeUnit.HOURS   => Calendar.HOUR_OF_DAY
    case TimeUnit.MINUTES => Calendar.MINUTE
  }

  def minus(s: String, window: Window): String = at(epochMillis(s) - window.millis)

  def plus(s: String, window: Window): String = at(epochMillis(s) + window.millis)

  def minusFast(s: String, window: Window): String = minus(s, window)

  def plusFast(s: String, window: Window): String = plus(s, window)

  def minus(partition: String, window: Option[Window]): String = {
    if (partition == null) return null
    window.map(minus(partition, _)).getOrElse(partition)
  }

  def plus(partition: String, window: Option[Window]): String = {
    if (partition == null) return null
    window.map(plus(partition, _)).getOrElse(partition)
  }

  def after(s: String): String = shiftPartitions(s, 1)

  def afterFast(s: String): String = after(s)

  // all partitions `count` ahead of `s` including `s` - result size will be count + 1
  // used to compute effected output partitions for a given partition
  def partitionsFrom(s: String, count: Int): Seq[String] = s +: (1 to count).map(shiftPartitions(s, _))

  def partitionsFrom(s: String, window: Window): Seq[String] = {
    val count = math.ceil(window.millis.toDouble / spanMillis).toInt
    partitionsFrom(s, count)
  }

  def before(millis: Long): String = at(millis - spanMillis)

  def shiftPartitions(partition: String, partitionCount: Int): String =
    at(epochMillis(partition) + partitionCount * spanMillis)

  def shift(partition: String, partitionCount: Int): String = shiftPartitions(partition, partitionCount)

  def now: String = at(System.currentTimeMillis())

  def shiftPartitionsBackFromNow(partitionCount: Int): String = shiftPartitions(now, 0 - partitionCount)

  def intervalWindow: Window = {
    if (spanMillis == WindowUtils.Day.millis) WindowUtils.Day
    else if (spanMillis == WindowUtils.Hour.millis) WindowUtils.Hour
    else if (spanMillis % WindowUtils.MinuteMillis == 0) {
      new Window((spanMillis / WindowUtils.MinuteMillis).toInt, TimeUnit.MINUTES)
    } else
      throw new UnsupportedOperationException(s"Partition intervals should be whole minutes - found ${spanMillis}ms")
  }

  /** Converts a partition value from this spec into the equivalent value in `targetSpec`.
    * The value is parsed with this spec, then rendered with `targetSpec`, including flooring
    * to the target grid when the target has a coarser or differently-offset interval.
    */
  def translate(partitionValue: String, targetSpec: PartitionSpec): String = {
    val millis = epochMillis(partitionValue)
    targetSpec.at(millis)
  }

  /** This ds value re-rendered canonically when it parses fully and starts a grid interval; None
    * otherwise. Lookup paths use it to admit externally-stored partition values (normalizing
    * padding quirks) without ever letting an off-boundary value into range math.
    */
  def canonical(value: String): Option[String] =
    Try(epochMillis(value)).toOption.filter(ms => grid.leftBound(ms) == ms).map(at)

  /** Normalizes a start value by translating from the fallback spec's interval start when needed. */
  def normalizeStart(partition: String, fallbackSpec: PartitionSpec): String = {
    if (partition == null) return null
    val startMillis =
      Try(epochMillis(partition)).toOption
        .getOrElse(fallbackSpec.partitionStartMillis(partition))
    at(startMillis)
  }

  /** Normalizes an end value by translating from the fallback spec's interval end when needed. */
  def normalizeEnd(partition: String, fallbackSpec: PartitionSpec): String = {
    if (partition == null) return null
    val endMillis =
      Try(partitionEndMillis(partition)).toOption
        .getOrElse(fallbackSpec.partitionEndMillis(partition))
    at(endMillis - 1)
  }

  private def validateFormat(): Unit =
    PartitionSpec.validateFormat(format, spanMillis, offsetMillis, grid, partitionFormatter, epochMillis)
}

object PartitionSpec {
  private val logger = org.slf4j.LoggerFactory.getLogger(getClass)

  // mid-month, mid-day reference so truncating formats are exposed; fixed for repeatability
  private val ProbeInstantMillis: Long = 1710510443000L // 2024-03-15T13:47:23Z

  private val SortabilityProbeBoundaries: Seq[Long] = Seq(
    1710507600000L, // 2024-03-15T13:00:00Z - crosses hour 12 on 12-hour clocks
    1710547200000L, // 2024-03-16T00:00:00Z - day rollover
    1706745600000L, // 2024-02-01T00:00:00Z - month rollover
    1735689600000L // 2025-01-01T00:00:00Z - year rollover
  )

  def validate(column: String, format: String, spanMillis: Long, offsetMillis: Long = 0L): Unit = {
    val grid = PartitionGrid(spanMillis, offsetMillis)
    val formatter = DateTimeFormatter
      .ofPattern(format, Locale.US)
      .withZone(ZoneOffset.UTC)
    validateFormat(format, spanMillis, offsetMillis, grid, formatter, epochMillis(format, _))
  }

  private def epochMillis(format: String, partition: String): Long = {
    val formatter = new SimpleDateFormat(format)
    formatter.setTimeZone(TimeZone.getTimeZone("UTC"))
    formatter.setLenient(false)

    val position = new ParsePosition(0)
    val parsed = formatter.parse(partition, position)
    if (parsed == null || position.getIndex != partition.length) {
      val errorOffset = if (position.getErrorIndex >= 0) position.getErrorIndex else position.getIndex
      throw new ParseException(s"Unparseable date: '$partition'", errorOffset)
    }
    parsed.getTime
  }

  private def validateFormat(format: String,
                             spanMillis: Long,
                             offsetMillis: Long,
                             grid: PartitionGrid,
                             partitionFormatter: DateTimeFormatter,
                             epochMillis: String => Long): Unit = {
    // resolution: every grid instant must round-trip exactly, else range arithmetic on ds values
    // would drift (e.g. a 3h partitionInterval with a date-only format collapses 8 partitions
    // into one ds value)
    val t0 = grid.leftBound(ProbeInstantMillis)
    Seq(t0, t0 + spanMillis).foreach { t =>
      val value = partitionFormatter.format(Instant.ofEpochMilli(t))
      require(
        !value.contains("'"),
        s"partition format '$format' produces ds values containing quotes; ds values are embedded in SQL unescaped"
      )
      val parsed = epochMillis(value)
      require(
        parsed == t,
        s"partition format '$format' cannot represent partition boundaries for partitionInterval=${spanMillis}ms " +
          s"partitionOffset=${offsetMillis}ms: '$value' parses back to ${TsUtils.toStr(parsed)} instead of " +
          s"${TsUtils.toStr(t)}. Include time fields, e.g. 'yyyy-MM-dd-HH-mm'."
      )
    }
    // sortability heuristic: consecutive ds values straddling hour-12, day, month and year
    // rollovers must order lexicographically (catches MM-dd-yyyy, 12-hour clocks, etc.)
    SortabilityProbeBoundaries.foreach { boundary =>
      val prev = grid.leftBound(boundary - 1)
      val prevValue = partitionFormatter.format(Instant.ofEpochMilli(prev))
      val nextValue = partitionFormatter.format(Instant.ofEpochMilli(prev + spanMillis))
      require(
        prevValue < nextValue,
        s"partition format '$format' is not lexicographically sortable: " +
          s"'$prevValue' !< '$nextValue' (instants ${TsUtils.toStr(prev)} -> ${TsUtils.toStr(prev + spanMillis)})"
      )
    }
    if (format.exists(c => c == ' ' || c == ':')) {
      logger.warn(
        s"partition format '$format' contains spaces or colons which get URL-escaped in object-store " +
          s"paths; prefer dash-separated formats like 'yyyy-MM-dd-HH'")
    }
  }

  val daily: PartitionSpec = PartitionSpec("ds", "yyyy-MM-dd", 24 * 60 * 60 * 1000)

  def hourly(column: String = "ds", format: String = "yyyy-MM-dd-HH-mm"): PartitionSpec =
    PartitionSpec(column, format, 60 * 60 * 1000)
}
