package ai.chronon.integrations.cloud_azure

import ai.chronon.api.Extensions.{WindowOps, WindowUtils}
import ai.chronon.api.PartitionSpec
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class Spark2CosmosLoaderTest extends AnyFlatSpec with Matchers {

  private def baseArgs(endDs: String): Seq[String] =
    Seq(
      "--table-name",
      "test_upload_table",
      "--dataset",
      "test_group_by_BATCH",
      "--end-ds",
      endDs,
      "--cosmos-endpoint",
      "https://example.documents.azure.com:443/",
      "--cosmos-key",
      "fake-key",
      "--cosmos-database",
      "chronon",
      "--cosmos-container",
      "test-container"
    )

  "Spark2CosmosLoader" should "build a sub-daily partition spec from upload args" in {
    val expected =
      PartitionSpec("ds", "yyyy-MM-dd-HH-mm", 3 * WindowUtils.Hour.millis, WindowUtils.Hour.millis)
    val config = new Spark2CosmosLoader.Conf(
      baseArgs("2023-08-13-22-00") ++ Seq(
        "--partition-column",
        expected.column,
        "--partition-format",
        expected.format,
        "--partition-span-millis",
        expected.spanMillis.toString,
        "--partition-offset-millis",
        expected.offsetMillis.toString
      ))

    val actual = Spark2CosmosLoader.partitionSpecFromConfig(config, PartitionSpec.daily)

    actual shouldBe expected
    actual.partitionEndMillis("2023-08-13-22-00") shouldBe expected.partitionEndMillis("2023-08-13-22-00")
  }

  it should "default the upload partition column to ds while falling back for the rest of the spec" in {
    val fallback = PartitionSpec("dt", "yyyyMMdd", WindowUtils.Day.millis)
    val config = new Spark2CosmosLoader.Conf(baseArgs("20230813"))

    Spark2CosmosLoader.partitionSpecFromConfig(config, fallback) shouldBe
      PartitionSpec("ds", fallback.format, fallback.spanMillis, fallback.offsetMillis)
  }
}
