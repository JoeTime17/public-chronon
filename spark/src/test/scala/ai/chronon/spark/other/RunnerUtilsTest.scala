package ai.chronon.spark.other

import ai.chronon.api.{ExecutionInfo, MetaData, PartitionSpec, TableInfo}
import ai.chronon.api.Extensions._
import ai.chronon.spark.RunnerUtils
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.utils.SparkTestBase
import org.scalatest.matchers.should.Matchers

class RunnerUtilsTest extends SparkTestBase with Matchers {

  override protected def sparkConfs: Map[String, String] = Map(
    "spark.chronon.partition.column" -> "partition_date",
    "spark.chronon.partition.format" -> "yyyyMMdd"
  )

  it should "resolve unstamped runtime metadata from Spark session partition defaults" in {
    val expectedSessionSpec = PartitionSpec("partition_date", "yyyyMMdd", WindowUtils.Day.millis)
    val metadata = new MetaData().setName("legacy_unstamped")

    metadata.partitionSpec(TableUtils(spark).partitionSpec) shouldEqual expectedSessionSpec
    RunnerUtils.tableUtilsForMetadata(spark, metadata).partitionSpec shouldEqual expectedSessionSpec
  }

  it should "prefer stamped metadata over Spark session partition defaults" in {
    val stampedSpec = PartitionSpec("event_hour", "yyyy-MM-dd-HH", WindowUtils.Hour.millis)
    val tableInfo = new TableInfo().withSpec(stampedSpec)
    val metadata = new MetaData()
      .setName("stamped")
      .setExecutionInfo(new ExecutionInfo().setOutputTableInfo(tableInfo))

    metadata.partitionSpec(TableUtils(spark).partitionSpec) shouldEqual stampedSpec
    RunnerUtils.tableUtilsForMetadata(spark, metadata).partitionSpec shouldEqual stampedSpec
  }
}
