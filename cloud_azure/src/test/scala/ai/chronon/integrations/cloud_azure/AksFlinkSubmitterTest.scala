package ai.chronon.integrations.cloud_azure

import org.junit.Assert._
import org.scalatest.flatspec.AnyFlatSpec

import scala.jdk.CollectionConverters._

class AksFlinkSubmitterTest extends AnyFlatSpec {

  // --- parseAbfssUri ---

  "parseAbfssUri" should "parse abfss URI into (accountName, fileSystem, path)" in {
    val (account, fs, path) = AksFlinkSubmitter.parseAbfssUri(
      "abfss://mycontainer@myaccount.dfs.core.windows.net/path/to/jar.jar")
    assertEquals("myaccount", account)
    assertEquals("mycontainer", fs)
    assertEquals("path/to/jar.jar", path)
  }

  it should "handle deeply nested paths" in {
    val (account, fs, path) = AksFlinkSubmitter.parseAbfssUri(
      "abfss://libs@account.dfs.core.windows.net/flink/1.0/jars/flink_assembly.jar")
    assertEquals("account", account)
    assertEquals("libs", fs)
    assertEquals("flink/1.0/jars/flink_assembly.jar", path)
  }

  it should "throw IllegalArgumentException for URI missing container (no @ delimiter)" in {
    intercept[IllegalArgumentException] {
      AksFlinkSubmitter.parseAbfssUri("abfss://account.dfs.core.windows.net/path/jar.jar")
    }
  }

  it should "throw IllegalArgumentException for URI missing blob path" in {
    intercept[IllegalArgumentException] {
      AksFlinkSubmitter.parseAbfssUri("abfss://container@account.dfs.core.windows.net")
    }
  }

  // --- buildInitContainerSpec: command construction ---

  "buildInitContainerSpec" should "produce az login with workload identity before download commands" in {
    val spec =
      AksFlinkSubmitter.buildInitContainerSpec(
        Array("abfss://libs@account.dfs.core.windows.net/jars/my.jar"))
    val container = spec.initContainers.asScala.head
    val commands = container.get("command").asInstanceOf[java.util.List[String]].asScala
    assertEquals("sh", commands(0))
    assertEquals("-c", commands(1))
    val shellCmd = commands(2)
    assertTrue(shellCmd.startsWith("az login --service-principal"))
    assertTrue(shellCmd.contains("$AZURE_CLIENT_ID"))
    assertTrue(shellCmd.contains("$AZURE_TENANT_ID"))
    assertTrue(shellCmd.contains("$AZURE_FEDERATED_TOKEN_FILE"))
  }

  it should "produce az storage fs file download command with --auth-mode login" in {
    val spec =
      AksFlinkSubmitter.buildInitContainerSpec(
        Array("abfss://libs@account.dfs.core.windows.net/jars/my.jar"))
    val shellCmd = spec.initContainers.asScala.head.get("command")
      .asInstanceOf[java.util.List[String]].asScala.apply(2)
    assertTrue(shellCmd.contains("az storage fs file download"))
    assertTrue(shellCmd.contains("--account-name 'account'"))
    assertTrue(shellCmd.contains("--file-system 'libs'"))
    assertTrue(shellCmd.contains("--path 'jars/my.jar'"))
    assertTrue(shellCmd.contains("--dest '/opt/flink/usrlib/my.jar'"))
    assertTrue(shellCmd.contains("--auth-mode login"))
  }

  it should "join multiple JARs with ' && '" in {
    val spec = AksFlinkSubmitter.buildInitContainerSpec(
      Array(
        "abfss://libs@account.dfs.core.windows.net/jars/a.jar",
        "abfss://libs@account.dfs.core.windows.net/jars/b.jar"
      ))
    val shellCmd = spec.initContainers.asScala.head.get("command")
      .asInstanceOf[java.util.List[String]].asScala.apply(2)
    assertTrue(shellCmd.contains(" && "))
    assertTrue(shellCmd.contains("a.jar"))
    assertTrue(shellCmd.contains("b.jar"))
  }

  it should "only run az login when jar array is empty" in {
    val spec = AksFlinkSubmitter.buildInitContainerSpec(Array.empty)
    val shellCmd = spec.initContainers.asScala.head.get("command")
      .asInstanceOf[java.util.List[String]].asScala.apply(2)
    assertTrue(shellCmd.startsWith("az login --service-principal"))
    assertFalse(shellCmd.contains("az storage fs file download"))
  }

  // --- buildInitContainerSpec: pod spec structure ---

  it should "use the Azure CLI image for the init container" in {
    val spec =
      AksFlinkSubmitter.buildInitContainerSpec(
        Array("abfss://c@a.dfs.core.windows.net/jars/x.jar"))
    val initContainer = spec.initContainers.asScala.head
    assertEquals("mcr.microsoft.com/azure-cli:latest", initContainer.get("image"))
  }

  it should "mount flink-usrlib volume at /opt/flink/usrlib in the init container" in {
    val spec =
      AksFlinkSubmitter.buildInitContainerSpec(
        Array("abfss://c@a.dfs.core.windows.net/jars/x.jar"))
    val mounts = spec.initContainers.asScala.head
      .get("volumeMounts")
      .asInstanceOf[java.util.List[java.util.Map[String, String]]]
      .asScala
    assertTrue(mounts.exists { m =>
      m.get("mountPath") == "/opt/flink/usrlib" && m.get("name") == "flink-usrlib"
    })
  }

  it should "set FLINK_CLASSPATH=/opt/flink/usrlib/* env var on the main container" in {
    val spec =
      AksFlinkSubmitter.buildInitContainerSpec(
        Array("abfss://c@a.dfs.core.windows.net/jars/x.jar"))
    val envVars = spec.envVars.asScala
    assertTrue(envVars.exists { e =>
      e.get("name") == "FLINK_CLASSPATH" && e.get("value") == "/opt/flink/usrlib/*"
    })
  }

  it should "include an emptyDir volume named flink-usrlib" in {
    val spec =
      AksFlinkSubmitter.buildInitContainerSpec(
        Array("abfss://c@a.dfs.core.windows.net/jars/x.jar"))
    val volumes = spec.volumes.asScala
    assertTrue(volumes.exists(_.get("name") == "flink-usrlib"))
  }

  // --- apply factory ---

  "AksFlinkSubmitter.apply" should "set the default Flink image when no image is specified" in {
    // Pass testEnv so the factory doesn't attempt to read AZURE_* from the test process environment
    val submitter = AksFlinkSubmitter(env = testEnv)
    assertNotNull(submitter)
    assertEquals("ziplineai/flink:1.20.3", AksFlinkSubmitter.FlinkImage)
  }

  it should "have empty AksOnlyAdditionalJarNames" in {
    assertEquals(0, AksFlinkSubmitter.AksOnlyAdditionalJarNames.length)
  }

  private val testEnv = Map(
    "FLINK_AZURE_CLIENT_ID" -> "test-flink-client-id",
    "FLINK_AZURE_TENANT_ID" -> "test-flink-tenant-id"
  )

  it should "include Azure OAuth and Prometheus keys in extraAzureFlinkConfig" in {
    val config = AksFlinkSubmitter.extraAzureFlinkConfig(testEnv)
    assertTrue(config.contains("fs.azure.account.auth.type"))
    assertEquals("OAuth", config("fs.azure.account.auth.type"))
    // Must be the Hadoop 3.4.x class — WorkloadIdentityCallbackHandler (3.3.x) does not exist
    // in the flink-azure-fs-hadoop plugin which shades Hadoop 3.4.1.
    assertEquals("org.apache.hadoop.fs.azurebfs.oauth2.WorkloadIdentityTokenProvider",
                 config("fs.azure.account.oauth.provider.type"))
    assertEquals("test-flink-client-id", config("fs.azure.account.oauth2.client.id"))
    assertEquals("test-flink-tenant-id", config("fs.azure.account.oauth2.msi.tenant"))
    // Token file path is the well-known webhook-injected constant — never configurable
    assertEquals(AksFlinkSubmitter.WorkloadIdentityTokenFilePath,
                 config("fs.azure.account.oauth2.token.file"))
    assertTrue(config.contains("metrics.reporters"))
    assertEquals("prom", config("metrics.reporters"))
    assertTrue(config.contains("metrics.reporter.prom.factory.class"))
    assertTrue(config.contains("metrics.reporter.prom.port"))
  }

  it should "throw when FLINK_AZURE_CLIENT_ID is missing" in {
    intercept[IllegalArgumentException] {
      AksFlinkSubmitter.extraAzureFlinkConfig(testEnv - "FLINK_AZURE_CLIENT_ID")
    }
  }

  it should "throw when FLINK_AZURE_TENANT_ID is missing" in {
    intercept[IllegalArgumentException] {
      AksFlinkSubmitter.extraAzureFlinkConfig(testEnv - "FLINK_AZURE_TENANT_ID")
    }
  }

  // The Azure Workload Identity webhook injects AZURE_CLIENT_ID / AZURE_TENANT_ID /
  // AZURE_FEDERATED_TOKEN_FILE only when this label is present on the pod — without it
  // `az login` fails in the download-jars init container.
  it should "set azure.workload.identity/use=true in WorkloadIdentityPodLabels" in {
    assertEquals("true", AksFlinkSubmitter.WorkloadIdentityPodLabels("azure.workload.identity/use"))
  }

  // The hub on AKS without streaming workflows shouldn't fail to start when these env vars
  // aren't set; the resolution happens lazily on first Flink submission.
  it should "construct AksFlinkSubmitter even when FLINK_AZURE_* env vars are missing" in {
    // Should NOT throw — extraAzureFlinkConfig is captured by-name and not evaluated until
    // the first call to K8sFlinkSubmitter.buildFlinkConfiguration / .submit.
    val submitter = AksFlinkSubmitter(env = Map.empty)
    assertTrue(submitter != null)
  }

  it should "fail at first Flink config build when FLINK_AZURE_CLIENT_ID is unset" in {
    val submitter = AksFlinkSubmitter(env = Map.empty)
    // The lazy by-name extraFlinkConfig is forced when buildFlinkConfiguration is called —
    // that's when we want the IllegalArgumentException, not at hub startup.
    intercept[IllegalArgumentException] {
      submitter.buildFlinkConfiguration("abfss://state-uri/checkpoints", Map.empty)
    }
  }
}
