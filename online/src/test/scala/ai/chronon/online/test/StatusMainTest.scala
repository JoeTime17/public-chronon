package ai.chronon.online.test

import ai.chronon.api.Builders
import ai.chronon.online.status.{FlinkAccess, StatusMain}
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

  it should "parse streaming mode with flink-url" in {
    val args = new StatusMain.Args(Array(
      "--mode", "streaming",
      "--conf-path", "compiled/group_bys/team/my_gb",
      "--flink-url", "http://flink-jm:8081"
    ))
    args.mode() shouldEqual "streaming"
    args.flinkUrl() shouldEqual "http://flink-jm:8081"
    args.repo() shouldEqual "."
    args.enableDebug() shouldEqual false
    args.onlineJar.isDefined shouldEqual false
    args.onlineClass.isDefined shouldEqual false
  }

  it should "parse streaming mode without flink-url (optional at parse time)" in {
    val args = new StatusMain.Args(Array(
      "--mode", "streaming",
      "--conf-path", "compiled/group_bys/team/my_gb"
    ))
    args.mode() shouldEqual "streaming"
    args.flinkUrl.isDefined shouldEqual false
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

  it should "throw when streaming mode is called without --flink-url or --online-class" in {
    val args = new StatusMain.Args(Array(
      "--mode", "streaming",
      "--conf-path", "compiled/group_bys/team/my_gb"
    ))
    val ex = the[IllegalArgumentException] thrownBy {
      StatusMain.resolveFlinkAccess("test.no_flink_url_gb", args)
    }
    ex.getMessage should include("--flink-url or --online-class is required")
  }

  it should "use --flink-url directly when provided" in {
    val args = new StatusMain.Args(Array(
      "--mode", "streaming",
      "--conf-path", "compiled/group_bys/team/my_gb",
      "--flink-url", "http://flink-jm:8081/"
    ))
    val access = StatusMain.resolveFlinkAccess("test.gb", args)
    access.url shouldEqual "http://flink-jm:8081"
    access.authHeaders shouldBe empty
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

  // --- Flink REST API JSON parsing ---

  it should "parse running job ID from Flink jobs response" in {
    val json = """{"jobs":[{"id":"abc123","status":"RUNNING"},{"id":"def456","status":"CANCELED"}]}"""
    StatusMain.parseRunningJobId(json) shouldEqual "abc123"
  }

  it should "throw when no running Flink job exists" in {
    val json = """{"jobs":[{"id":"def456","status":"CANCELED"}]}"""
    the[RuntimeException] thrownBy {
      StatusMain.parseRunningJobId(json)
    } should have message "No running Flink job found"
  }

  it should "throw when jobs list is empty" in {
    val json = """{"jobs":[]}"""
    the[RuntimeException] thrownBy {
      StatusMain.parseRunningJobId(json)
    } should have message "No Flink jobs found"
  }

  it should "parse checkpoint counts" in {
    val json = """{"counts":{"restored":1,"total":20,"in_progress":0,"completed":15,"failed":4}}"""
    StatusMain.parseCheckpointCounts(json) shouldEqual 15
  }

  it should "return 0 when checkpoint counts are missing" in {
    val json = """{}"""
    StatusMain.parseCheckpointCounts(json) shouldEqual 0
  }

  it should "parse vertex IDs from job detail" in {
    val json = """{
      "jid":"abc123",
      "vertices":[
        {"id":"v1","name":"Source"},
        {"id":"v2","name":"Sink"}
      ]
    }"""
    StatusMain.parseVertexIds(json) shouldEqual Seq("v1", "v2")
  }

  it should "return empty seq when no vertices" in {
    val json = """{"jid":"abc123"}"""
    StatusMain.parseVertexIds(json) shouldEqual Seq.empty
  }

  it should "parse metric IDs from metrics list" in {
    val json = """[{"id":"0.chronon.feature_group.test_gb.event_created_to_sink_time.Mean"},{"id":"other_metric"}]"""
    val ids = StatusMain.parseMetricIds(json)
    ids should have size 2
    ids.head should include("event_created_to_sink_time")
  }

  it should "parse metric value" in {
    val json = """[{"id":"some.metric.Mean","value":"1234.5"}]"""
    StatusMain.parseMetricValue(json) shouldEqual Some(1234.5)
  }

  it should "return None for empty metric value response" in {
    val json = """[]"""
    StatusMain.parseMetricValue(json) shouldEqual None
  }

  it should "return None for null metric value" in {
    val json = """[{"id":"some.metric","value":null}]"""
    StatusMain.parseMetricValue(json) shouldEqual None
  }

  it should "parse all metric values from multiple subtasks" in {
    val json = """[{"id":"0.Sink.metric_p99","value":"100.0"},{"id":"1.Sink.metric_p99","value":"200.0"}]"""
    StatusMain.parseAllMetricValues(json) shouldEqual Seq(100.0, 200.0)
  }

  it should "skip null values in multi-subtask response" in {
    val json = """[{"id":"0.Sink.metric_p99","value":"100.0"},{"id":"1.Sink.metric_p99","value":null}]"""
    StatusMain.parseAllMetricValues(json) shouldEqual Seq(100.0)
  }
}
