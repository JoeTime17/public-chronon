package ai.chronon.integrations.cloud_k8s

import ai.chronon.api.JobStatusType
import ai.chronon.spark.submission.JobSubmitterConstants.MaxRetainedCheckpoints
import K8sFlinkSubmitter.{DeploymentPendingTimeout, InitContainerSpec}
import org.junit.Assert.{assertEquals, assertFalse, assertNull, assertTrue}
import org.scalatest.flatspec.AnyFlatSpec

import java.time.Instant
import java.util.Collections
import scala.jdk.CollectionConverters._

class K8sFlinkSubmitterTest extends AnyFlatSpec {

  private val checkpointUri = "gs://zipline-warehouse/flink-checkpoints"

  // Minimal no-op init container spec builder for tests that don't exercise the init container
  private val noopInitContainerSpec: Array[String] => InitContainerSpec = _ =>
    InitContainerSpec(
      Collections.emptyList(),
      Collections.emptyList(),
      Collections.emptyList(),
      Collections.emptyList()
    )

  private def submitterWithExtra(extraConfig: Map[String, String] = Map.empty,
                                  extraJars: Array[String] = Array.empty,
                                  podLabels: Map[String, String] = Map.empty): K8sFlinkSubmitter =
    new K8sFlinkSubmitter(
      flinkImage = "test-image:latest",
      buildInitContainerSpec = noopInitContainerSpec,
      extraFlinkConfig = extraConfig,
      extraJarNames = extraJars,
      defaultJarsBasePath = "gs://test-bucket/libs/",
      podTemplateLabels = podLabels
    )

  private def config(jobProperties: Map[String, String] = Map.empty,
                     extraConfig: Map[String, String] = Map.empty): Map[String, String] =
    submitterWithExtra(extraConfig).buildFlinkConfiguration(
      flinkCheckpointUri = checkpointUri,
      jobProperties = jobProperties
    )

  // --- buildFlinkConfiguration: base defaults ---

  "buildFlinkConfiguration" should "set checkpoint and backend defaults" in {
    val cfg = config()
    assertEquals(checkpointUri, cfg("state.savepoints.dir"))
    assertEquals(checkpointUri, cfg("state.checkpoints.dir"))
    assertEquals("rocksdb", cfg("state.backend.type"))
    assertEquals("true", cfg("state.backend.incremental"))
    assertEquals("filesystem", cfg("state.checkpoint-storage"))
    assertEquals("/tmp/flink-state", cfg("state.backend.rocksdb.localdir"))
    assertEquals(MaxRetainedCheckpoints, cfg("state.checkpoints.num-retained"))
  }

  it should "not set pipeline.jars — jars are on the classpath via FLINK_CLASSPATH env var" in {
    assertFalse(config().contains("pipeline.jars"))
  }

  it should "not include cloud-specific keys by default (no extraFlinkConfig)" in {
    val cfg = config()
    assertFalse(cfg.contains("s3.access.key"))
    assertFalse(cfg.contains("fs.s3a.aws.credentials.provider"))
  }

  // --- buildFlinkConfiguration: extraFlinkConfig injection ---

  it should "include keys from extraFlinkConfig" in {
    val cfg = config(extraConfig = Map("custom.key" -> "custom.val"))
    assertEquals("custom.val", cfg("custom.key"))
  }

  it should "allow jobProperties to override extraFlinkConfig" in {
    val cfg = config(
      jobProperties = Map("state.backend.type" -> "hashmap"),
      extraConfig = Map("state.backend.type" -> "rocksdb")
    )
    assertEquals("hashmap", cfg("state.backend.type"))
  }

  it should "allow jobProperties to override base defaults" in {
    val cfg = config(Map("state.checkpoints.num-retained" -> "5"))
    assertEquals("5", cfg("state.checkpoints.num-retained"))
  }

  it should "pass through all jobProperties keys into the Flink configuration" in {
    val cfg = config(Map(
      "custom.flink.key" -> "custom-value",
      "state.checkpoints.num-retained" -> "7"
    ))
    assertEquals("custom-value", cfg("custom.flink.key"))
    assertEquals("7", cfg("state.checkpoints.num-retained"))
  }

  it should "allow jobProperties to override JM memory" in {
    val cfg = config(Map("jobmanager.memory.process.size" -> "2G"))
    assertEquals("2G", cfg("jobmanager.memory.process.size"))
  }

  // --- buildFlinkConfiguration: memory tier dispatch ---

  it should "default to 16G tier when no TM memory is specified" in {
    val cfg = config()
    assertEquals("16G", cfg("taskmanager.memory.process.size"))
    assertEquals("1", cfg("taskmanager.numberOfTaskSlots"))
    assertEquals("4G", cfg("jobmanager.memory.process.size"))
    assertEquals("256m", cfg("taskmanager.memory.network.min"))
    assertEquals("512m", cfg("taskmanager.memory.network.max"))
    assertEquals("0.5f", cfg("taskmanager.memory.managed.fraction"))
    assertEquals("512m", cfg("taskmanager.memory.jvm-metaspace.size"))
    assertEquals("256m", cfg("taskmanager.memory.task.off-heap.size"))
  }

  it should "apply 32G tier settings" in {
    val cfg = config(Map("taskmanager.memory.process.size" -> "32G"))
    assertEquals("32G", cfg("taskmanager.memory.process.size"))
    assertEquals("2", cfg("taskmanager.numberOfTaskSlots"))
    assertEquals("512m", cfg("taskmanager.memory.network.min"))
    assertEquals("1G", cfg("taskmanager.memory.network.max"))
    assertEquals("0.5f", cfg("taskmanager.memory.managed.fraction"))
    assertEquals("512m", cfg("taskmanager.memory.jvm-metaspace.size"))
    assertEquals("512m", cfg("taskmanager.memory.task.off-heap.size"))
  }

  it should "apply 64G tier settings" in {
    val cfg = config(Map("taskmanager.memory.process.size" -> "64G"))
    assertEquals("64G", cfg("taskmanager.memory.process.size"))
    assertEquals("4", cfg("taskmanager.numberOfTaskSlots"))
    assertEquals("1G", cfg("taskmanager.memory.network.min"))
    assertEquals("2G", cfg("taskmanager.memory.network.max"))
    assertEquals("0.5f", cfg("taskmanager.memory.managed.fraction"))
    assertEquals("512m", cfg("taskmanager.memory.jvm-metaspace.size"))
    assertEquals("1G", cfg("taskmanager.memory.task.off-heap.size"))
  }

  it should "use SmallTaskManager defaults for sub-16G TM memory (e.g. 8G)" in {
    val cfg = config(Map("taskmanager.memory.process.size" -> "8G"))
    assertEquals("1", cfg("taskmanager.numberOfTaskSlots"))
    assertEquals("8G", cfg("taskmanager.memory.process.size"))
    assertFalse(cfg.contains("taskmanager.memory.network.min"))
  }

  // --- sanitizeDeploymentName ---

  "sanitizeDeploymentName" should "lowercase and replace invalid chars with dashes" in {
    assertEquals("flink-my-job-123", K8sFlinkSubmitter.sanitizeDeploymentName("flink-My_Job.123"))
  }

  it should "truncate to 45 characters" in {
    val long = "flink-" + "a" * 50
    val result = K8sFlinkSubmitter.sanitizeDeploymentName(long)
    assertTrue(s"Expected <= 45 chars, got ${result.length}", result.length <= 45)
  }

  it should "strip trailing dashes after truncation" in {
    val name = "a" * 44 + "-extra"
    val result = K8sFlinkSubmitter.sanitizeDeploymentName(name)
    assertFalse(s"Expected no trailing dash, got: $result", result.endsWith("-"))
  }

  it should "strip leading and trailing dashes" in {
    assertEquals("foo-bar", K8sFlinkSubmitter.sanitizeDeploymentName("--foo-bar--"))
  }

  // --- resolveStatus ---

  private def resolveStatus(submitter: K8sFlinkSubmitter,
                             lifecycle: String,
                             jmStatus: String,
                             ts: Option[Instant]): JobStatusType =
    submitter.resolveStatus("test-deployment", lifecycle, jmStatus, ts)

  private val s = submitterWithExtra()
  private val now = Some(Instant.now())
  private val old = Some(Instant.now().minusSeconds(DeploymentPendingTimeout.toSeconds + 60))

  "resolveStatus" should "return RUNNING when lifecycleState is STABLE" in {
    assertEquals(JobStatusType.RUNNING, resolveStatus(s, "STABLE", "READY", now))
  }

  it should "return FAILED when lifecycleState is FAILED" in {
    assertEquals(JobStatusType.FAILED, resolveStatus(s, "FAILED", "MISSING", now))
  }

  it should "return FAILED when lifecycleState is SUSPENDED" in {
    assertEquals(JobStatusType.FAILED, resolveStatus(s, "SUSPENDED", "READY", now))
  }

  it should "return UNKNOWN for an unrecognised lifecycleState" in {
    assertEquals(JobStatusType.UNKNOWN, resolveStatus(s, "SOMETHING_NEW", "READY", now))
  }

  it should "return PENDING when DEPLOYED with READY jmStatus and within timeout" in {
    assertEquals(JobStatusType.PENDING, resolveStatus(s, "DEPLOYED", "READY", now))
  }

  it should "return PENDING when UPGRADING with READY jmStatus and within timeout" in {
    assertEquals(JobStatusType.PENDING, resolveStatus(s, "UPGRADING", "READY", now))
  }

  it should "return FAILED immediately when jmDeploymentStatus is ERROR regardless of age" in {
    assertEquals(JobStatusType.FAILED, resolveStatus(s, "DEPLOYED", "ERROR", now))
  }

  it should "return PENDING when jmDeploymentStatus is MISSING and deployment is new" in {
    assertEquals(JobStatusType.PENDING, resolveStatus(s, "DEPLOYED", "MISSING", now))
  }

  it should "return FAILED when jmDeploymentStatus is MISSING and deployment has timed out" in {
    assertEquals(JobStatusType.FAILED, resolveStatus(s, "DEPLOYED", "MISSING", old))
  }

  it should "return FAILED when DEPLOYED with READY jmStatus but deployment has timed out" in {
    assertEquals(JobStatusType.FAILED, resolveStatus(s, "DEPLOYED", "READY", old))
  }

  it should "return FAILED when UPGRADING with READY jmStatus but deployment has timed out" in {
    assertEquals(JobStatusType.FAILED, resolveStatus(s, "UPGRADING", "READY", old))
  }

  it should "return PENDING when no creationTimestamp is available" in {
    assertEquals(JobStatusType.PENDING, resolveStatus(s, "DEPLOYED", "READY", None))
  }

  // --- buildComponentSpec: podTemplateLabels ---

  private def podMeta(submitter: K8sFlinkSubmitter): java.util.Map[String, Object] = {
    val componentSpec = submitter.buildComponentSpec(
      memory = "4G",
      cpu = 1.0,
      replicas = Some(1),
      Collections.emptyList(),
      Collections.emptyList(),
      Collections.emptyList(),
      Collections.emptyList()
    )
    componentSpec.get("podTemplate")
      .asInstanceOf[java.util.Map[String, Object]]
      .get("metadata")
      .asInstanceOf[java.util.Map[String, Object]]
  }

  "buildComponentSpec" should "not include labels in podTemplate metadata when podTemplateLabels is empty" in {
    val meta = podMeta(submitterWithExtra())
    assertNull(meta.get("labels"))
  }

  it should "include labels in podTemplate metadata when podTemplateLabels is set" in {
    val meta = podMeta(submitterWithExtra(podLabels = Map("azure.workload.identity/use" -> "true")))
    val labels = meta.get("labels").asInstanceOf[java.util.Map[String, String]]
    assertEquals("true", labels.get("azure.workload.identity/use"))
  }

  it should "include all provided podTemplateLabels" in {
    val meta = podMeta(submitterWithExtra(podLabels = Map("foo" -> "bar", "baz" -> "qux")))
    val labels = meta.get("labels").asInstanceOf[java.util.Map[String, String]]
    assertEquals("bar", labels.get("foo"))
    assertEquals("qux", labels.get("baz"))
  }

  it should "always include prometheus annotations regardless of podTemplateLabels" in {
    val meta = podMeta(submitterWithExtra(podLabels = Map("some-label" -> "val")))
    val annotations = meta.get("annotations").asInstanceOf[java.util.Map[String, String]]
    assertEquals("true", annotations.get("prometheus.io/scrape"))
  }

  // --- flinkEnvVars injection into JM/TM containers ---

  private def containerEnvVars(submitter: K8sFlinkSubmitter,
                                envVars: java.util.List[java.util.Map[String, String]]
                               ): java.util.List[java.util.Map[String, String]] = {
    val componentSpec = submitter.buildComponentSpec(
      memory = "4G",
      cpu = 1.0,
      replicas = Some(1),
      Collections.emptyList(),
      envVars,
      Collections.emptyList(),
      Collections.emptyList()
    )
    val podSpec = componentSpec.get("podTemplate")
      .asInstanceOf[java.util.Map[String, Object]]
      .get("spec")
      .asInstanceOf[java.util.Map[String, Object]]
    val containers = podSpec.get("containers")
      .asInstanceOf[java.util.List[java.util.Map[String, Object]]]
    containers.get(0).get("env")
      .asInstanceOf[java.util.List[java.util.Map[String, String]]]
  }

  it should "inject flinkEnvVars as env vars on JM and TM containers with FLINK_ prefix stripped" in {
    val submitter = submitterWithExtra()
    // buildComponentSpec receives already-processed env var maps (FLINK_ prefix already stripped by submit()).
    // Here we test that buildComponentSpec faithfully passes them through to the container spec.
    val podEnvVars: java.util.List[java.util.Map[String, String]] =
      List("SASL_JAAS_CFG" -> "sasl-value", "BOOTSTRAP_SERVERS" -> "kafka:9092").map {
        case (key, value) =>
          val m = new java.util.HashMap[String, String]()
          m.put("name", key)
          m.put("value", value)
          m: java.util.Map[String, String]
      }.asJava

    val envs = containerEnvVars(submitter, podEnvVars)

    val envMap = envs.asScala.map(e => e.get("name") -> e.get("value")).toMap
    assertEquals("sasl-value", envMap("SASL_JAAS_CFG"))
    assertEquals("kafka:9092", envMap("BOOTSTRAP_SERVERS"))
  }

  it should "preserve existing container env vars (e.g. FLINK_CLASSPATH) alongside injected pod env vars" in {
    val submitter = submitterWithExtra()
    val classpathVar = new java.util.HashMap[String, String]()
    classpathVar.put("name", "FLINK_CLASSPATH")
    classpathVar.put("value", "/opt/flink/usrlib/*")
    val saslVar = new java.util.HashMap[String, String]()
    saslVar.put("name", "SASL_JAAS_CFG")
    saslVar.put("value", "secret-value")
    val allVars = new java.util.ArrayList[java.util.Map[String, String]]()
    allVars.add(classpathVar)
    allVars.add(saslVar)

    val envs = containerEnvVars(submitter, allVars)

    val envMap = envs.asScala.map(e => e.get("name") -> e.get("value")).toMap
    assertEquals("/opt/flink/usrlib/*", envMap("FLINK_CLASSPATH"))
    assertEquals("secret-value", envMap("SASL_JAAS_CFG"))
  }

  // --- createFlinkIngress ---

  "createFlinkIngress" should "throw IllegalArgumentException when host cannot be extracted" in {
    val submitterWithBadUrl = new K8sFlinkSubmitter(
      flinkImage = "test-image:latest",
      buildInitContainerSpec = noopInitContainerSpec,
      defaultJarsBasePath = "gs://test-bucket/libs/",
      ingressBaseUrl = Some("not-a-valid-url")
    )
    assertThrows[IllegalArgumentException] {
      submitterWithBadUrl.createFlinkIngress(null, "my-deployment", "default", "some-uid")
    }
  }
}
