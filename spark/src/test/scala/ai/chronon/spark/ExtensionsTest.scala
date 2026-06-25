package ai.chronon.spark

import ai.chronon.api.PartitionSpec
import ai.chronon.spark.Extensions._
import ai.chronon.spark.utils.SparkTestBase
import org.scalatest.matchers.should.Matchers

class ExtensionsTest extends SparkTestBase with Matchers {

  "translatePartitionSpec" should "floor same-format partition values to the target grid" in {
    import spark.implicits._

    val fifteenMinuteSpec = PartitionSpec("ds", "yyyy-MM-dd-HH-mm", 15 * 60 * 1000)
    val hourlySpec = PartitionSpec("ds", "yyyy-MM-dd-HH-mm", 60 * 60 * 1000)

    val result = Seq("2024-01-01-00-00", "2024-01-01-00-15", "2024-01-01-00-45")
      .toDF("ds")
      .translatePartitionSpec(fifteenMinuteSpec, hourlySpec)
      .as[String]
      .collect()
      .toSeq

    result should contain theSameElementsInOrderAs Seq(
      "2024-01-01-00-00",
      "2024-01-01-00-00",
      "2024-01-01-00-00"
    )
  }
}
