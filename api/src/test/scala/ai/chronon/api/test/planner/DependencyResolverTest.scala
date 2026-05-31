package ai.chronon.api.test.planner

import ai.chronon.api.Builders.dep
import ai.chronon.api.{PartitionRange, PartitionSpec}
import ai.chronon.api.planner.DependencyResolver
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DependencyResolverTest extends AnyFlatSpec with Matchers {

  implicit val partitionSpec: PartitionSpec = PartitionSpec.daily

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

}
