package ai.chronon.api.test.planner

import ai.chronon.api.Builders.dep
import ai.chronon.api.Extensions.WindowUtils
import ai.chronon.api.{PartitionRange, PartitionSpec, TableInfo}
import ai.chronon.api.planner.DependencyResolver
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DependencyResolverTest extends AnyFlatSpec with Matchers {

  implicit val partitionSpec: PartitionSpec = PartitionSpec.daily
  private val threeHourSpec = PartitionSpec("ds", "yyyy-MM-dd-HH-mm", 3 * 60 * 60 * 1000)
  private val offsetThreeHourSpec = PartitionSpec("ds", "yyyy-MM-dd-HH-mm", 3 * 60 * 60 * 1000, 60 * 60 * 1000)

  "computeOutputRange" should "return same range when no offsets are set" in {
    val parentRange = PartitionRange("2024-01-01", "2024-01-05")
    val tableDep = dep("test.table")

    val result = DependencyResolver.computeOutputRange(parentRange, tableDep)

    result shouldBe Some(PartitionRange("2024-01-01", "2024-01-05"))
  }

  it should "expand end by startOffset days" in {
    val parentRange = PartitionRange("2024-01-01", "2024-01-05")
    val tableDep = dep("test.table", startOffsetDays = 7)

    val result = DependencyResolver.computeOutputRange(parentRange, tableDep)

    result shouldBe Some(PartitionRange("2024-01-01", "2024-01-12"))
  }

  it should "expand start by endOffset days" in {
    val parentRange = PartitionRange("2024-01-05", "2024-01-10")
    val tableDep = dep("test.table", endOffsetDays = 3)

    val result = DependencyResolver.computeOutputRange(parentRange, tableDep)

    result shouldBe Some(PartitionRange("2024-01-08", "2024-01-10"))
  }

  it should "expand both start and end with both offsets" in {
    val parentRange = PartitionRange("2024-01-10", "2024-01-15")
    val tableDep = dep("test.table", startOffsetDays = 30, endOffsetDays = 7)

    val result = DependencyResolver.computeOutputRange(parentRange, tableDep)

    result shouldBe Some(PartitionRange("2024-01-17", "2024-02-14"))
  }

  it should "return None when expanded range is invalid (start > end)" in {
    val parentRange = PartitionRange("2024-01-01", "2024-01-02")
    val tableDep = dep("test.table", endOffsetDays = 10)

    val result = DependencyResolver.computeOutputRange(parentRange, tableDep)

    result shouldBe None
  }

  it should "map a daily downstream range to all required three-hour input partitions" in {
    val queryRange = PartitionRange("2024-01-02", "2024-01-02")
    val tableDep = dep("test.hourly_table")
    tableDep.setTableInfo(
      new TableInfo()
        .setTable("test.hourly_table")
        .setPartitionColumn(threeHourSpec.column)
        .setPartitionFormat(threeHourSpec.format)
        .setPartitionInterval(WindowUtils.fromMillis(threeHourSpec.spanMillis))
    )

    val result = DependencyResolver.computeInputRange(queryRange, tableDep)

    result shouldBe Some(PartitionRange("2024-01-02-00-00", "2024-01-02-21-00")(threeHourSpec))
  }

  it should "normalize end cutoffs using the end of the fallback partition" in {
    val queryRange = PartitionRange("2024-01-05", "2024-01-05")
    val tableDep = dep("test.hourly_table")
    tableDep.setEndCutOff("2024-01-05")
    tableDep.setTableInfo(
      new TableInfo()
        .setTable("test.hourly_table")
        .setPartitionColumn(threeHourSpec.column)
        .setPartitionFormat(threeHourSpec.format)
        .setPartitionInterval(WindowUtils.fromMillis(threeHourSpec.spanMillis))
    )

    val result = DependencyResolver.computeInputRange(queryRange, tableDep)

    result shouldBe Some(PartitionRange("2024-01-05-00-00", "2024-01-05-21-00")(threeHourSpec))
  }

  it should "map a daily downstream range to boundary-crossing offset input partitions" in {
    val queryRange = PartitionRange("2024-01-02", "2024-01-02")
    val tableDep = dep("test.hourly_table")
    tableDep.setTableInfo(
      new TableInfo()
        .setTable("test.hourly_table")
        .setPartitionColumn(offsetThreeHourSpec.column)
        .setPartitionFormat(offsetThreeHourSpec.format)
        .setPartitionInterval(WindowUtils.fromMillis(offsetThreeHourSpec.spanMillis))
        .setPartitionOffset(WindowUtils.fromMillis(offsetThreeHourSpec.offsetMillis))
    )

    val result = DependencyResolver.computeInputRange(queryRange, tableDep)

    result shouldBe Some(PartitionRange("2024-01-01-22-00", "2024-01-02-22-00")(offsetThreeHourSpec))
  }

  it should "keep the input start unbounded for unbounded start offsets" in {
    val queryRange = PartitionRange("2024-01-02", "2024-01-02")
    val tableDep = dep("test.events")
    tableDep.setStartOffset(WindowUtils.Unbounded)

    val result = DependencyResolver.computeInputRange(queryRange, tableDep)

    result shouldBe Some(PartitionRange(null, "2024-01-02"))
  }

  it should "map a partial three-hour upstream range to the impacted daily output partition" in {
    val parentRange = PartitionRange("2024-01-02-06-00", "2024-01-02-06-00")(threeHourSpec)
    val tableDep = dep("test.hourly_table")

    val result = DependencyResolver.computeOutputRange(parentRange, tableDep, PartitionSpec.daily)

    result shouldBe Some(PartitionRange("2024-01-02", "2024-01-02"))
  }

  it should "map a midnight-straddling upstream partition to both impacted daily output partitions" in {
    val parentRange = PartitionRange("2024-01-02-22-00", "2024-01-02-22-00")(offsetThreeHourSpec)
    val tableDep = dep("test.hourly_table")

    val result = DependencyResolver.computeOutputRange(parentRange, tableDep, PartitionSpec.daily)

    result shouldBe Some(PartitionRange("2024-01-02", "2024-01-03"))
  }

}
