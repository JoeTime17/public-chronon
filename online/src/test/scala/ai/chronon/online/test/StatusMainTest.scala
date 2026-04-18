package ai.chronon.online.test

import ai.chronon.api.Builders
import ai.chronon.online.status.StatusMain
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class StatusMainTest extends AnyFlatSpec with Matchers {

  // --- Argument parsing ---

  it should "parse upload-to-kv mode with all flags" in {
    val args = new StatusMain.Args(Array(
      "--mode", "upload-to-kv",
      "--conf-path", "compiled/group_bys/team/my_gb",
      "--online-class", "com.example.MyApi",
      "--online-jar", "/tmp/my.jar",
      "--repo", "/my/repo",
      "--enable-debug"
    ))
    args.mode() shouldEqual "upload-to-kv"
    args.confPath() shouldEqual "compiled/group_bys/team/my_gb"
    args.onlineClass() shouldEqual "com.example.MyApi"
    args.onlineJar() shouldEqual "/tmp/my.jar"
    args.repo() shouldEqual "/my/repo"
    args.enableDebug() shouldEqual true
  }

  it should "parse streaming mode with minimal flags and verify defaults" in {
    val args = new StatusMain.Args(Array(
      "--mode", "streaming",
      "--conf-path", "compiled/group_bys/team/my_gb"
    ))
    args.mode() shouldEqual "streaming"
    args.repo() shouldEqual "."
    args.enableDebug() shouldEqual false
    args.onlineJar.isDefined shouldEqual false
    args.onlineClass.isDefined shouldEqual false
  }

  it should "parse -Z extra props" in {
    val args = new StatusMain.Args(Array(
      "--mode", "streaming",
      "--conf-path", "compiled/group_bys/team/my_gb",
      "-Zfoo=bar",
      "-Zbaz=qux"
    ))
    args.extraProps shouldEqual Map("foo" -> "bar", "baz" -> "qux")
  }

  it should "parse --enable-debug flag" in {
    val args = new StatusMain.Args(Array(
      "--mode", "streaming",
      "--conf-path", "compiled/group_bys/team/my_gb",
      "--enable-debug"
    ))
    args.enableDebug() shouldEqual true
  }

  // --- streamingStatus validation ---

  it should "throw when GroupBy has no streaming source" in {
    val groupBy = Builders.GroupBy(
      metaData = Builders.MetaData(name = "test.no_stream_gb"),
      sources = Seq(
        Builders.Source.events(
          query = Builders.Query(selects = Map("col" -> "col")),
          table = "db.table"
        )
      )
    )
    val ex = the[IllegalArgumentException] thrownBy {
      StatusMain.streamingStatus(groupBy)
    }
    ex.getMessage should include("has no streaming source")
  }

  it should "throw when streaming source has non-kafka topic" in {
    val groupBy = Builders.GroupBy(
      metaData = Builders.MetaData(name = "test.pubsub_gb"),
      sources = Seq(
        Builders.Source.events(
          query = Builders.Query(selects = Map("col" -> "col")),
          table = "db.table",
          topic = "pubsub://my-topic"
        )
      )
    )
    val ex = the[IllegalArgumentException] thrownBy {
      StatusMain.streamingStatus(groupBy)
    }
    ex.getMessage should include("only supports kafka")
  }

  it should "throw when kafka topic has no bootstrap server" in {
    val groupBy = Builders.GroupBy(
      metaData = Builders.MetaData(name = "test.no_bootstrap_gb"),
      sources = Seq(
        Builders.Source.events(
          query = Builders.Query(selects = Map("col" -> "col")),
          table = "db.table",
          topic = "kafka://my-topic"
        )
      )
    )
    val ex = the[IllegalArgumentException] thrownBy {
      StatusMain.streamingStatus(groupBy)
    }
    ex.getMessage should include("No bootstrap server")
  }

  // --- uploadToKvStatus validation ---

  it should "throw when upload-to-kv is called without --online-class" in {
    val groupBy = Builders.GroupBy(
      metaData = Builders.MetaData(name = "test.no_class_gb")
    )
    val args = new StatusMain.Args(Array(
      "--mode", "upload-to-kv",
      "--conf-path", "compiled/group_bys/team/my_gb"
    ))
    val ex = the[IllegalArgumentException] thrownBy {
      StatusMain.uploadToKvStatus(groupBy, args)
    }
    ex.getMessage should include("--online-class is required")
  }
}
