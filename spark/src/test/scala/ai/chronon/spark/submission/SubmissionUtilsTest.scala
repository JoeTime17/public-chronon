package ai.chronon.spark.submission

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File

class SubmissionUtilsTest extends AnyFlatSpec with Matchers {

  "CrucibleSubmitter.appendClasspath" should "append local jars without clobbering existing classpaths" in {
    CrucibleSubmitter.appendClasspath("/opt/spark/jars/a.jar", "/opt/chronon/app.jar") shouldBe
      s"/opt/spark/jars/a.jar${File.pathSeparator}/opt/chronon/app.jar"
  }

  it should "use the local jar path when the existing classpath is empty" in {
    CrucibleSubmitter.appendClasspath("", "/opt/chronon/app.jar") shouldBe "/opt/chronon/app.jar"
    CrucibleSubmitter.appendClasspath(null, "/opt/chronon/app.jar") shouldBe "/opt/chronon/app.jar"
  }

  "NodeConfReader.isCloudUri" should "detect supported schemes case-insensitively" in {
    Seq("GS://bucket/conf.json",
        "S3://bucket/conf.json",
        "S3A://bucket/conf.json",
        "ABFS://container@account.dfs.core.windows.net/conf.json",
        "ABFSS://container@account.dfs.core.windows.net/conf.json").foreach { uri =>
      NodeConfReader.isCloudUri(uri) shouldBe true
    }
  }

  it should "leave local paths as non-cloud URIs" in {
    NodeConfReader.isCloudUri("/tmp/conf.json") shouldBe false
    NodeConfReader.isCloudUri("file:///tmp/conf.json") shouldBe false
  }
}
