package ai.chronon.api.test

import ai.chronon.api.Extensions.WindowUtils
import ai.chronon.api.{PartitionGrid, PartitionRange, PartitionSpec, TimeUnit, Window}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.text.ParseException

class PartitionSpecTest extends AnyFlatSpec with Matchers {

  private val dailySpec = PartitionSpec.daily
  private val compactSpec = PartitionSpec("ds", "yyyyMMdd", 24 * 60 * 60 * 1000)
  private val threeHourSpec = PartitionSpec("ds", "yyyy-MM-dd-HH-mm", 3 * 60 * 60 * 1000)
  private val offsetThreeHourSpec = PartitionSpec("ds", "yyyy-MM-dd-HH-mm", 3 * 60 * 60 * 1000, 60 * 60 * 1000)
  private val fifteenMinuteSpec = PartitionSpec("ds", "yyyy-MM-dd-HH-mm", 15 * 60 * 1000)

  "PartitionGrid" should "own grid invariants independent of ds format" in {
    an[IllegalArgumentException] should be thrownBy PartitionGrid(0)
    an[IllegalArgumentException] should be thrownBy PartitionGrid(5 * 60 * 60 * 1000L)
    an[IllegalArgumentException] should be thrownBy PartitionGrid(24 * 60 * 60 * 1000L, 60 * 60 * 1000L)
    an[IllegalArgumentException] should be thrownBy PartitionGrid(3 * 60 * 60 * 1000L, -1L)
    an[IllegalArgumentException] should be thrownBy PartitionGrid(3 * 60 * 60 * 1000L, 3 * 60 * 60 * 1000L)

    noException should be thrownBy PartitionGrid(24 * 60 * 60 * 1000L)
    noException should be thrownBy PartitionGrid(90 * 60 * 1000L)
    noException should be thrownBy PartitionGrid(3 * 60 * 60 * 1000L, 60 * 60 * 1000L)
  }

  it should "emit a semantic token only for non-daily grids" in {
    PartitionGrid(24 * 60 * 60 * 1000L).semanticToken should be(None)
    PartitionGrid(3 * 60 * 60 * 1000L).semanticToken should be(Some("grid:interval_ms=10800000,offset_ms=0"))
    PartitionGrid(3 * 60 * 60 * 1000L, 60 * 60 * 1000L).semanticToken should be(
      Some("grid:interval_ms=10800000,offset_ms=3600000"))
  }

  it should "make the boundary-subset check directional" in {
    val upstream = PartitionGrid(3 * 60 * 60 * 1000L, 60 * 60 * 1000L)
    val linedUpDownstream = PartitionGrid(6 * 60 * 60 * 1000L, 4 * 60 * 60 * 1000L)
    val offBoundaryDownstream = PartitionGrid(6 * 60 * 60 * 1000L, 2 * 60 * 60 * 1000L)

    linedUpDownstream.isExactMultipleOf(upstream) should be(true)
    linedUpDownstream.linesUpWith(upstream) should be(true)
    linedUpDownstream.isBoundarySubsetOf(upstream) should be(true)

    upstream.isExactMultipleOf(linedUpDownstream) should be(false)
    upstream.linesUpWith(linedUpDownstream) should be(false)
    upstream.isBoundarySubsetOf(linedUpDownstream) should be(false)

    offBoundaryDownstream.isExactMultipleOf(upstream) should be(true)
    offBoundaryDownstream.linesUpWith(upstream) should be(false)
    offBoundaryDownstream.isBoundarySubsetOf(upstream) should be(false)
  }

  "PartitionSpec.expandRange" should "expand date range into individual dates" in {
    val result = dailySpec.expandRange("2024-01-01", "2024-01-05")
    val expected = List("2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05")
    result should be(expected)
  }

  it should "handle single day range" in {
    val result = dailySpec.expandRange("2024-01-15", "2024-01-15")
    result should be(List("2024-01-15"))
  }

  it should "expand sub-daily ranges by the partition interval" in {
    val result = threeHourSpec.expandRange("2024-01-15-00-00", "2024-01-15-09-00")
    result should be(List("2024-01-15-00-00", "2024-01-15-03-00", "2024-01-15-06-00", "2024-01-15-09-00"))
  }

  it should "expand offset sub-daily ranges by the partition interval" in {
    val result = offsetThreeHourSpec.expandRange("2024-01-01-22-00", "2024-01-02-04-00")
    result should be(List("2024-01-01-22-00", "2024-01-02-01-00", "2024-01-02-04-00"))
  }

  it should "expand range across month boundaries" in {
    val result = dailySpec.expandRange("2024-01-30", "2024-02-02")
    val expected = List("2024-01-30", "2024-01-31", "2024-02-01", "2024-02-02")
    result should be(expected)
  }

  "PartitionSpec.plus" should "add one day correctly" in {
    val result = dailySpec.plus("2024-01-01", WindowUtils.Day)
    result should be("2024-01-02")
  }

  it should "add multiple days correctly" in {
    val window = new Window(3, TimeUnit.DAYS)
    val result = dailySpec.plus("2024-01-01", window)
    result should be("2024-01-04")
  }

  it should "handle month boundary correctly when adding days" in {
    val result = dailySpec.plus("2024-01-31", WindowUtils.Day)
    result should be("2024-02-01")
  }

  "PartitionSpec.after" should "return the next day" in {
    val result = dailySpec.after("2024-03-15")
    result should be("2024-03-16")
  }

  it should "return the next sub-daily partition" in {
    threeHourSpec.after("2024-03-15-21-00") should be("2024-03-16-00-00")
    fifteenMinuteSpec.after("2024-03-15-00-45") should be("2024-03-15-01-00")
  }

  "PartitionSpec.minus" should "subtract days correctly" in {
    val window = new Window(2, TimeUnit.DAYS)
    val result = dailySpec.minus("2024-01-03", window)
    result should be("2024-01-01")
  }

  it should "handle month boundary correctly when subtracting days" in {
    val result = dailySpec.minus("2024-02-01", WindowUtils.Day)
    result should be("2024-01-31")
  }

  "PartitionSpec.calendarGrain" should "return HOUR_OF_DAY for HOURS" in {
    val window = new Window(1, TimeUnit.HOURS)
    val result = dailySpec.calendarGrain(window)
    result should be(java.util.Calendar.HOUR_OF_DAY)
  }
  
  it should "return DAY_OF_MONTH for DAYS" in {
    val window = new Window(1, TimeUnit.DAYS)
    val result = dailySpec.calendarGrain(window)
    result should be(java.util.Calendar.DAY_OF_MONTH)
  }
  
  it should "return MINUTE for MINUTES" in {
    val window = new Window(1, TimeUnit.MINUTES)
    val result = dailySpec.calendarGrain(window)
    result should be(java.util.Calendar.MINUTE)
  }

  "PartitionSpec.translate" should "convert from yyyy-MM-dd to yyyyMMdd" in {
    dailySpec.translate("2025-11-25", compactSpec) should be("20251125")
    dailySpec.translate("2025-01-01", compactSpec) should be("20250101")
    dailySpec.translate("1970-01-01", compactSpec) should be("19700101")
  }

  "PartitionSpec.epochMillis" should "reject partitions that do not match its format" in {
    intercept[ParseException] {
      dailySpec.epochMillis("20251125")
    }
  }

  it should "convert from yyyyMMdd to yyyy-MM-dd" in {
    compactSpec.translate("20251125", dailySpec) should be("2025-11-25")
    compactSpec.translate("20250101", dailySpec) should be("2025-01-01")
    compactSpec.translate("19700101", dailySpec) should be("1970-01-01")
  }

  it should "convert between daily and sub-daily formats at interval starts" in {
    dailySpec.translate("2025-11-25", threeHourSpec) should be("2025-11-25-00-00")
    threeHourSpec.translate("2025-11-25-18-00", dailySpec) should be("2025-11-25")
  }

  it should "floor timestamps using partition offset" in {
    val jan2Midnight = dailySpec.epochMillis("2024-01-02")
    offsetThreeHourSpec.at(jan2Midnight + 30 * 60 * 1000) should be("2024-01-01-22-00")
    offsetThreeHourSpec.at(jan2Midnight + 60 * 60 * 1000) should be("2024-01-02-01-00")
  }

  it should "round-trip between formats" in {
    val date = "2025-12-01"
    val roundTripped = compactSpec.translate(dailySpec.translate(date, compactSpec), dailySpec)
    roundTripped should be(date)
  }

  "PartitionSpec.canonical" should "admit only fully-parsing on-boundary ds values, normalizing padding" in {
    val spec = PartitionSpec("ds", "yyyy-MM-dd-HH", 3 * 60 * 60 * 1000, 60 * 60 * 1000)
    spec.canonical("2024-01-02-04") should be(Some("2024-01-02-04"))
    spec.canonical("2024-01-02-4") should be(Some("2024-01-02-04")) // unpadded but on-grid
    spec.canonical("2024-01-02-03") should be(None) // off the 3h@01:00 grid
    spec.canonical("2024-01-02") should be(None) // under-specified for the format
    spec.canonical("garbage") should be(None)
    dailySpec.canonical("2024-01-02") should be(Some("2024-01-02"))
  }

  "PartitionSpec date arithmetic" should "work correctly with yyyyMMdd format" in {
    compactSpec.after("20251125") should be("20251126")
    compactSpec.before("20251201") should be("20251130")
    compactSpec.shiftPartitions("20251231", 1) should be("20260101")
  }

  it should "support minus with window in yyyyMMdd format" in {
    val twoDays = new Window(2, TimeUnit.DAYS)
    compactSpec.minus("20251125", twoDays) should be("20251123")
  }

  "PartitionRange.intersectingRange" should "convert CLI dates (yyyy-MM-dd) to yyyyMMdd" in {
    val cliRange = PartitionRange("2025-11-25", "2025-12-01")(dailySpec)
    val intersecting = cliRange.intersectingRange(compactSpec)

    intersecting.start should be("20251125")
    intersecting.end should be("20251201")
    intersecting.partitionSpec should be(compactSpec)
  }

  it should "preserve range validity after cross-format conversion" in {
    val cliRange = PartitionRange("2025-11-25", "2025-12-01")(dailySpec)
    val intersecting = cliRange.intersectingRange(compactSpec)

    intersecting.wellDefined should be(true)
    intersecting.partitions.size should be(7)
    intersecting.partitions.head should be("20251125")
    intersecting.partitions.last should be("20251201")
  }

  it should "be a no-op when source and target formats match" in {
    val range = PartitionRange("2025-11-25", "2025-12-01")(dailySpec)
    val intersecting = range.intersectingRange(dailySpec)

    intersecting.start should be("2025-11-25")
    intersecting.end should be("2025-12-01")
  }

  it should "expand a daily range into every overlapping offset sub-daily partition" in {
    val range = PartitionRange("2024-01-02", "2024-01-02")(dailySpec)
    val intersecting = range.intersectingRange(offsetThreeHourSpec)

    intersecting.start should be("2024-01-01-22-00")
    intersecting.end should be("2024-01-02-22-00")
    intersecting.partitions should contain theSameElementsInOrderAs Seq(
      "2024-01-01-22-00",
      "2024-01-02-01-00",
      "2024-01-02-04-00",
      "2024-01-02-07-00",
      "2024-01-02-10-00",
      "2024-01-02-13-00",
      "2024-01-02-16-00",
      "2024-01-02-19-00",
      "2024-01-02-22-00"
    )
  }

  it should "expand a midnight-straddling partition into all impacted daily partitions" in {
    val range = PartitionRange("2024-01-02-22-00", "2024-01-02-22-00")(offsetThreeHourSpec)
    val intersecting = range.intersectingRange(dailySpec)

    intersecting should be(PartitionRange("2024-01-02", "2024-01-03")(dailySpec))
  }

  it should "convert a sub-daily range to daily without underflowing" in {
    // [21:00, 24:00) is contained in 2024-01-02: the daily end must come from the partition
    // containing (endMillis - 1ms), not from subtracting a full daily interval (which would
    // underflow to 2024-01-01)
    val lastInterval = PartitionRange("2024-01-02-21-00", "2024-01-02-21-00")(threeHourSpec)
    lastInterval.intersectingRange(dailySpec) should be(PartitionRange("2024-01-02", "2024-01-02")(dailySpec))

    val firstInterval = PartitionRange("2024-01-02-00-00", "2024-01-02-00-00")(threeHourSpec)
    firstInterval.intersectingRange(dailySpec) should be(PartitionRange("2024-01-02", "2024-01-02")(dailySpec))
  }

  it should "convert between specs with different offsets by time-interval intersection" in {
    val sixHourOffsetSpec = PartitionSpec("ds", "yyyy-MM-dd-HH-mm", 6 * 60 * 60 * 1000, 60 * 60 * 1000)

    // boundaries line up (offsets differ by a whole number of upstream intervals): the 6h@1h
    // partition [01:00, 07:00) maps exactly onto the 3h@1h partitions
    val downstream = PartitionRange("2024-01-02-01-00", "2024-01-02-01-00")(sixHourOffsetSpec)
    downstream.intersectingRange(offsetThreeHourSpec).partitions should contain theSameElementsInOrderAs Seq(
      "2024-01-02-01-00",
      "2024-01-02-04-00"
    )

    // grids whose boundaries don't line up over-fetch: every midnight-boundary 3h partition
    // overlapping [01:00, 07:00)
    downstream.intersectingRange(threeHourSpec).partitions should contain theSameElementsInOrderAs Seq(
      "2024-01-02-00-00",
      "2024-01-02-03-00",
      "2024-01-02-06-00"
    )
  }

  "PartitionSpec construction" should "validate format resolution and sortability" in {
    // formats must resolve the grid: a date-only format collapses sub-daily partitions
    an[IllegalArgumentException] should be thrownBy PartitionSpec("ds", "yyyy-MM-dd", 60 * 60 * 1000L)
    // non-sortable orderings and 12-hour clocks are rejected
    an[IllegalArgumentException] should be thrownBy PartitionSpec("ds", "MM-dd-yyyy", 24 * 60 * 60 * 1000L)
    an[IllegalArgumentException] should be thrownBy PartitionSpec("ds", "yyyy-MM-dd-hh", 60 * 60 * 1000L)
    // ds values containing literal quotes would embed quotes in SQL unescaped
    an[IllegalArgumentException] should be thrownBy PartitionSpec("ds", "yyyy-MM-dd''HH", 60 * 60 * 1000L)
    // a 30m offset is not expressible in an hour-resolution format
    an[IllegalArgumentException] should be thrownBy PartitionSpec("ds", "yyyy-MM-dd-HH", 60 * 60 * 1000L, 30 * 60 * 1000L)

    // common formats pass (space/colon formats stay expressible but warn; the default is dash-separated)
    noException should be thrownBy PartitionSpec("ds", "yyyy-MM-dd", 24 * 60 * 60 * 1000L)
    noException should be thrownBy PartitionSpec("ds", "yyyyMMdd", 24 * 60 * 60 * 1000L)
    noException should be thrownBy PartitionSpec("ds", "yyyy-MM-dd-HH", 60 * 60 * 1000L)
    noException should be thrownBy PartitionSpec("ds", "yyyy-MM-dd-HH-mm", 3 * 60 * 60 * 1000L)
    noException should be thrownBy PartitionSpec("ds", "yyyy-MM-dd-HH-mm", 3 * 60 * 60 * 1000L, 60 * 60 * 1000L)
  }

  it should "delegate grid invariant validation to PartitionGrid" in {
    an[IllegalArgumentException] should be thrownBy PartitionSpec("ds", "yyyy-MM-dd-HH-mm", 5 * 60 * 60 * 1000L)
    an[IllegalArgumentException] should be thrownBy
      PartitionSpec("ds", "yyyy-MM-dd-HH-mm", 3 * 60 * 60 * 1000L, 3 * 60 * 60 * 1000L)
  }

  it should "validate raw partition spec fields without constructing a PartitionSpec" in {
    noException should be thrownBy
      PartitionSpec.validate("ds", "yyyy-MM-dd-HH-mm", 3 * 60 * 60 * 1000L, 60 * 60 * 1000L)

    an[IllegalArgumentException] should be thrownBy
      PartitionSpec.validate("ds", "yyyy-MM-dd", 60 * 60 * 1000L)
    an[IllegalArgumentException] should be thrownBy
      PartitionSpec.validate("ds", "yyyy-MM-dd-HH", 60 * 60 * 1000L, 30 * 60 * 1000L)
  }

  "PartitionRange.maxMillis" should "derive the last millisecond the range holds" in {
    val daily = PartitionRange("2024-01-01", "2024-01-03")(dailySpec)
    daily.maxMillis should be(dailySpec.epochMillis("2024-01-04") - 1)

    val slice = PartitionRange("2024-01-02-21-00", "2024-01-02-21-00")(threeHourSpec)
    slice.maxMillis should be(dailySpec.epochMillis("2024-01-03") - 1)

    // tolerates an unbounded start
    PartitionRange(null, "2024-01-03")(dailySpec).maxMillis should be(dailySpec.epochMillis("2024-01-04") - 1)
  }

  "PartitionRange.intersectingRange" should "compute intersecting ranges across grids" in {
    val day = PartitionRange("2024-01-05", "2024-01-05")(dailySpec)
    day.intersectingRange(dailySpec) should be theSameInstanceAs day

    // [00:00, 24:00) intersects the 3h@01:00 partitions from yesterday 22:00 through today 22:00
    val intersecting = day.intersectingRange(offsetThreeHourSpec)
    intersecting.start should be("2024-01-04-22-00")
    intersecting.end should be("2024-01-05-22-00")
    intersecting.partitions.size should be(9)

    val slice = PartitionRange("2024-01-05-00-00", "2024-01-05-03-00")(threeHourSpec)
    slice.intersectingRange(dailySpec) should be(PartitionRange("2024-01-05", "2024-01-05")(dailySpec))

    // a partition straddling midnight needs both days
    val straddling = PartitionRange("2024-01-05-22-00", "2024-01-05-22-00")(offsetThreeHourSpec)
    straddling.intersectingRange(dailySpec).start should be("2024-01-05")
    straddling.intersectingRange(dailySpec).end should be("2024-01-06")
  }

  "PartitionRange.fullyContainedPartitions" should "translate partitions across grids" in {
    // same grid: 1:1 translation
    PartitionRange.fullyContainedPartitions(Seq("2024-01-05", "2024-01-06"), dailySpec, compactSpec) should be(
      Seq("20240105", "20240106"))

    // coarser partitions expand into finer ones, but only FULLY contained ones count: the
    // day-straddling 22:00 partitions need both surrounding daily partitions
    val covered = PartitionRange.fullyContainedPartitions(Seq("2024-01-05"), dailySpec, offsetThreeHourSpec)
    covered should contain("2024-01-05-01-00")
    covered should not contain "2024-01-04-22-00" // [22:00, 01:00) also needs daily 2024-01-04
    covered should not contain "2024-01-05-22-00" // also needs daily 2024-01-06
    covered.size should be(7)

    val coveredTwoDays =
      PartitionRange.fullyContainedPartitions(Seq("2024-01-05", "2024-01-06"), dailySpec, offsetThreeHourSpec)
    coveredTwoDays should contain("2024-01-05-22-00") // both surrounding days present now
    coveredTwoDays.size should be(15)

    // a coarser partition counts only when ALL its finer partitions exist
    val allSlices = Seq("2024-01-05-00-00", "2024-01-05-03-00", "2024-01-05-06-00", "2024-01-05-09-00",
      "2024-01-05-12-00", "2024-01-05-15-00", "2024-01-05-18-00", "2024-01-05-21-00")
    PartitionRange.fullyContainedPartitions(allSlices, threeHourSpec, dailySpec) should be(Seq("2024-01-05"))
    PartitionRange.fullyContainedPartitions(allSlices.drop(1), threeHourSpec, dailySpec) should be(Seq.empty)
  }

  "PartitionRange.steps" should "tumble by partition count and by days" in {
    val dailyRange = PartitionRange("2024-01-01", "2024-01-10")(dailySpec)
    dailyRange.stepsByDays(3) should be(dailyRange.steps(3))

    val subDaily = PartitionRange("2024-01-01-00-00", "2024-01-02-06-00")(threeHourSpec).stepsByDays(1)
    subDaily.head.partitions.size should be(8)
    subDaily.last.partitions.size should be(3) // ragged tail
  }

  "TimeRange.toTimePoints" should "stride on the offset grid" in {
    implicit val spec: PartitionSpec = offsetThreeHourSpec
    def utc(s: String): Long = java.time.Instant.parse(s).toEpochMilli
    val tr = ai.chronon.api.TimeRange(utc("2024-01-05T02:00:00Z"), utc("2024-01-05T08:00:00Z"))
    tr.toTimePoints should be(
      Array(utc("2024-01-05T01:00:00Z"), utc("2024-01-05T04:00:00Z"), utc("2024-01-05T07:00:00Z")))
  }
}
