package ai.chronon.integrations.cloud_azure

import ai.chronon.integrations.cloud_k8s.K8sFlinkSubmitter
import ai.chronon.integrations.cloud_k8s.K8sFlinkSubmitter.InitContainerSpec
import io.fabric8.kubernetes.client.Config

import scala.jdk.CollectionConverters._

/** Azure-specific constants and factory for [[K8sFlinkSubmitter]].
  *
  * Provides the Chronon Flink image, an Azure Blob Storage-backed init container spec builder
  * (using the Azure CLI with Workload Identity auth to stage JARs into /opt/flink/usrlib/),
  * and Azure OAuth + Prometheus metrics config injected into the cloud-agnostic
  * [[K8sFlinkSubmitter]].
  */
object AksFlinkSubmitter {

  val FlinkImage = "ziplineai/flink:1.20.3"

  val DefaultAzureFlinkJarsBasePath = ""

  // the ziplineai/flink image already bundles the necessary runtime JARs
  val AksOnlyAdditionalJarNames: Array[String] = Array.empty

  // The AKS Workload Identity webhook always mounts the federated token at this path.
  // It is a well-known constant — not configurable per-deployment.
  val WorkloadIdentityTokenFilePath = "/var/run/secrets/azure/tokens/azure-identity-token"

  // Azure Workload Identity (pod-level managed identity) + Prometheus metrics config.
  //
  // WorkloadIdentityTokenProvider (Hadoop 3.4.x, HADOOP-18890) reads tenant/client/token-file
  // from Flink config. We bake the values in at submission time so the Flink JM/TM pods have them at startup.
  //
  // Note: The hub may run on AKS (workload identity) or locally (service principal). In both cases
  // the Flink pods always run on AKS and always use workload identity via zipline-flink-sa.
  // We therefore use dedicated FLINK_AZURE_CLIENT_ID / FLINK_AZURE_TENANT_ID env vars that
  // refer to that managed identity — independent of however the hub itself authenticates.
  // The token file path is a webhook constant and never needs to be configured.
  def extraAzureFlinkConfig(env: Map[String, String] = sys.env): Map[String, String] = {
    val clientId = env.getOrElse(
      "FLINK_AZURE_CLIENT_ID",
      throw new IllegalArgumentException(
        "FLINK_AZURE_CLIENT_ID must be set to the client ID of the zipline-flink-sa managed identity")
    )
    val tenantId = env.getOrElse(
      "FLINK_AZURE_TENANT_ID",
      throw new IllegalArgumentException(
        "FLINK_AZURE_TENANT_ID must be set to the Azure tenant ID for the Flink workload identity"))
    Map(
      // Use AKS Workload Identity (OIDC federation)
      "fs.azure.account.auth.type" -> "OAuth",
      // WorkloadIdentityTokenProvider is the Hadoop 3.4.x class (HADOOP-18890).
      // We pull this in via the flink-azure-fs-hadoop plugin that is built into the Zipline flink image.
      "fs.azure.account.oauth.provider.type" -> "org.apache.hadoop.fs.azurebfs.oauth2.WorkloadIdentityTokenProvider",
      "fs.azure.account.oauth2.client.id" -> clientId,
      "fs.azure.account.oauth2.msi.tenant" -> tenantId,
      "fs.azure.account.oauth2.token.file" -> WorkloadIdentityTokenFilePath,
      // Expose Flink metrics via Prometheus HTTP endpoint so the Azure managed scraper can collect them.
      "metrics.reporters" -> "prom",
      "metrics.reporter.prom.factory.class" -> "org.apache.flink.metrics.prometheus.PrometheusReporterFactory",
      "metrics.reporter.prom.port" -> "9250-9260"
    )
  }

  /** Parses an abfss:// URI into (accountName, fileSystem, path) components for use with
    * `az storage fs file download`.
    *
    * abfss://container@account.dfs.core.windows.net/path/to/file
    *   → ("account", "container", "path/to/file")
    */
  private[cloud_azure] def parseAbfssUri(abfssUri: String): (String, String, String) = {
    val uri = java.net.URI.create(abfssUri)
    val fileSystem = uri.getUserInfo
    val host = uri.getHost
    val path = uri.getPath.stripPrefix("/")
    require(fileSystem != null && fileSystem.nonEmpty,
            s"Expected abfss://container@account.dfs.core.windows.net/path, got: $abfssUri")
    require(path.nonEmpty, s"Expected abfss://container@account.dfs.core.windows.net/path, got: $abfssUri")
    // host is account.dfs.core.windows.net — extract the account name
    val accountName = host
      .split("\\.")
      .headOption
      .getOrElse(throw new IllegalArgumentException(s"Could not extract account name from host: $host"))
    (accountName, fileSystem, path)
  }

  /** Builds the init container spec for downloading JARs from Azure Data Lake Storage Gen2.
    *
    * Uses `az storage fs file download` with `--auth-mode login` so that authentication is
    * handled by AKS Workload Identity (OIDC federation) — no credentials appear in the pod spec.
    * Mirrors buildInitContainerSpec in EksFlinkSubmitter.
    */
  def buildInitContainerSpec(jarUris: Array[String]): InitContainerSpec = {
    val downloadCommands = jarUris
      .map { jar =>
        val (accountName, fileSystem, path) = parseAbfssUri(jar)
        val dest = s"/opt/flink/usrlib/${jar.split("/").last}"
        s"az storage fs file download --account-name '$accountName' --file-system '$fileSystem' --path '$path' --dest '$dest' --auth-mode login --overwrite true"
      }
      .mkString(" && ")

    // Workload Identity injects AZURE_CLIENT_ID, AZURE_TENANT_ID, and AZURE_FEDERATED_TOKEN_FILE
    // via the mutating webhook. We must explicitly call `az login` with the projected token before any storage commands.
    val loginCommand =
      "az login --service-principal -u $AZURE_CLIENT_ID --tenant $AZURE_TENANT_ID --federated-token $(cat $AZURE_FEDERATED_TOKEN_FILE)"
    val shellScript = if (downloadCommands.isEmpty) loginCommand else s"$loginCommand && $downloadCommands"

    val initContainer = new java.util.HashMap[String, Object]()
    initContainer.put("name", "download-jars")
    initContainer.put("image", "mcr.microsoft.com/azure-cli:latest")
    val commands = new java.util.ArrayList[String]()
    commands.add("sh"); commands.add("-c"); commands.add(shellScript)
    initContainer.put("command", commands)
    val initVolumeMount = new java.util.HashMap[String, String]()
    initVolumeMount.put("name", "flink-usrlib")
    initVolumeMount.put("mountPath", "/opt/flink/usrlib")
    val initVolumeMounts = new java.util.ArrayList[java.util.Map[String, String]]()
    initVolumeMounts.add(initVolumeMount)
    initContainer.put("volumeMounts", initVolumeMounts)
    val initContainers = new java.util.ArrayList[java.util.Map[String, Object]]()
    initContainers.add(initContainer)

    val usrlibVolumeMount = new java.util.HashMap[String, String]()
    usrlibVolumeMount.put("name", "flink-usrlib")
    usrlibVolumeMount.put("mountPath", "/opt/flink/usrlib")
    val usrlibVolumeMounts = new java.util.ArrayList[java.util.Map[String, String]]()
    usrlibVolumeMounts.add(usrlibVolumeMount)

    val classpathEnvVar = new java.util.HashMap[String, String]()
    classpathEnvVar.put("name", "FLINK_CLASSPATH")
    classpathEnvVar.put("value", "/opt/flink/usrlib/*")
    val envVars = new java.util.ArrayList[java.util.Map[String, String]]()
    envVars.add(classpathEnvVar)

    val usrlibVolume = new java.util.HashMap[String, Object]()
    usrlibVolume.put("name", "flink-usrlib")
    usrlibVolume.put("emptyDir", new java.util.HashMap[String, Object]())
    val volumes = new java.util.ArrayList[java.util.Map[String, Object]]()
    volumes.add(usrlibVolume)

    InitContainerSpec(initContainers, envVars, usrlibVolumeMounts, volumes)
  }

  // Required for the Azure Workload Identity mutating webhook to inject AZURE_CLIENT_ID,
  // AZURE_TENANT_ID, and AZURE_FEDERATED_TOKEN_FILE into pods — without this label the
  // webhook skips injection and `az login` fails in the init container.
  val WorkloadIdentityPodLabels: Map[String, String] = Map("azure.workload.identity/use" -> "true")

  /** Creates a [[K8sFlinkSubmitter]] configured for Azure AKS. */
  def apply(
      flinkImage: String = FlinkImage,
      defaultJarsBasePath: String = DefaultAzureFlinkJarsBasePath,
      k8sConfig: Option[Config] = None,
      ingressBaseUrl: Option[String] = None,
      env: Map[String, String] = sys.env
  ): K8sFlinkSubmitter =
    new K8sFlinkSubmitter(
      flinkImage = flinkImage,
      buildInitContainerSpec = buildInitContainerSpec,
      extraFlinkConfig = extraAzureFlinkConfig(env),
      extraJarNames = AksOnlyAdditionalJarNames,
      defaultJarsBasePath = defaultJarsBasePath,
      k8sConfig = k8sConfig,
      ingressBaseUrl = ingressBaseUrl,
      podTemplateLabels = WorkloadIdentityPodLabels
    )
}
