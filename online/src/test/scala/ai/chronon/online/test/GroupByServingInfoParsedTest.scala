package ai.chronon.online.test

import ai.chronon.api.{GroupBy, GroupByServingInfo, TimeUnit, Window}
import ai.chronon.online.GroupByServingInfoParsed
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class GroupByServingInfoParsedTest extends AnyFlatSpec with Matchers {

  private def utc(s: String): Long = java.time.Instant.parse(s).toEpochMilli

  // eager members of the parsed class only need a groupBy to exist, not a full conf
  private def servingInfo: GroupByServingInfo = new GroupByServingInfo().setGroupBy(new GroupBy())

  "batchEndTsMillis" should "prefer the explicit watermark and fall back to ds parsing" in {
    // explicit thrift watermark wins, even over a deliberately inconsistent ds
    val explicit = servingInfo
      .setBatchEndTs(utc("2026-06-03T04:00:00Z"))
      .setBatchEndDate("1999-01-01")
      .setDateFormat("yyyy-MM-dd")
    new GroupByServingInfoParsed(explicit).batchEndTsMillis should be(utc("2026-06-03T04:00:00Z"))

    // uploads from older versions: daily ds parse
    val daily = servingInfo
      .setBatchEndDate("2026-06-03")
      .setDateFormat("yyyy-MM-dd")
    new GroupByServingInfoParsed(daily).batchEndTsMillis should be(utc("2026-06-03T00:00:00Z"))

    // sub-daily ds values parse via the partition interval and offset fields
    val subDaily = servingInfo
      .setBatchEndDate("2026-06-03-04-00")
      .setDateFormat("yyyy-MM-dd-HH-mm")
      .setPartitionInterval(new Window(3, TimeUnit.HOURS))
      .setPartitionOffset(new Window(1, TimeUnit.HOURS))
    new GroupByServingInfoParsed(subDaily).batchEndTsMillis should be(utc("2026-06-03T04:00:00Z"))
  }
}
