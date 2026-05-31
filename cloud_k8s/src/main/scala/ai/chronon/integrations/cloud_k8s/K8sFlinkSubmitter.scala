package ai.chronon.integrations.cloud_k8s

import ai.chronon.api.JobStatusType
import ai.chronon.spark.submission.JobSubmitterConstants.{MaxRetainedCheckpoints, additionalFlinkJars}
import io.fabric8.kubernetes.api.model.GenericKubernetesResource
import io.fabric8.kubernetes.api.model.networking.v1.IngressBuilder
import io.fabric8.kubernetes.client.{Config, KubernetesClientBuilder}
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext
import org.slf4j.LoggerFactory

import java.time.Instant
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._

/** Cloud-agnostic Flink job submitter for Kubernetes clusters running the Flink Kubernetes Operator.
  *
  * Cloud-specific concerns (container image, JAR staging init container, credential config) are
  * injected via constructor parameters so that AWS (EKS), GCP (GKE), and Azure (AKS) can each
  * provide their own implementations without duplicating the K8s CRD management logic.
  *
  * @param flinkImage         Base Flink Docker image
  * @param buildInitContainerSpec  Builds the pod spec additions needed to stage JARs into
  *                           /opt/flink/usrlib/ at pod startup (init containers, env vars,
  *                           volume mounts, volumes). Cloud-specific: AWS uses aws s3 cp,
  *                           GCP would use gsutil, etc.
  * @param extraFlinkConfig   Additional Flink config key-values to inject that are not part of the standard Flink configuration
  *                            (e.g. IRSA credentials for AWS, Prometheus scraping configs, ..).
  * @param extraJarNames      Additional JARs to include in the image (typically cloud specific jars that need to be added)
  * @param k8sConfig          Optional fabric8 K8s client config (defaults to in-cluster / kubeconfig).
  * @param ingressBaseUrl     Optional base URL for nginx ingress creation (enables Flink UI proxying).
  */
class K8sFlinkSubmitter(
    flinkImage: String,
    buildInitContainerSpec: Array[String] => K8sFlinkSubmitter.InitContainerSpec,
    // By-name so cloud-specific config that depends on env vars (e.g. AKS workload-identity
    // tenant/client IDs) only resolves on first Flink submission, not at hub startup. Memoized
    // below via `resolvedExtraFlinkConfig` so the resolution still happens once per instance.
    extraFlinkConfig: => Map[String, String] = Map.empty,
    extraJarNames: Array[String] = Array.empty,
    defaultJarsBasePath: String,
    k8sConfig: Option[Config] = None,
    ingressBaseUrl: Option[String] = None,
    podTemplateLabels: Map[String, String] = Map.empty
) {
  import K8sFlinkSubmitter._

  private val logger = LoggerFactory.getLogger(getClass)

  private lazy val resolvedExtraFlinkConfig: Map[String, String] = extraFlinkConfig

  // TM memory tiers. TM sizing follows the following conventions:
  // 64G: 4 slots, tuned network/managed/metaspace settings matching prior load testing on Dataproc
  // 32G: 2 slots, halved from 64G settings
  // 16G: 1 slot, tuned settings for a smaller footprint
  // Small (anything else, e.g. 8G): 1 slot, no explicit memory tuning — Flink defaults apply,
  // and the pod resource size is taken directly from jobProperties (or Flink's own default if absent)
  private sealed trait MemoryTier {
    // None means Flink defaults
    def tmProcessMemory: Option[String]
    def jmProcessMemory: String = "4G"
    def taskSlots: Int
    def networkMin: Option[String]
    def networkMax: Option[String]
    def managedFraction: Option[String]
    def metaspaceSize: Option[String]
    def taskOffHeap: Option[String]
  }

  private case object TaskManager64G extends MemoryTier {
    val tmProcessMemory = Some("64G")
    val taskSlots = 4
    val networkMin = Some("1G")
    val networkMax = Some("2G")
    val managedFraction = Some("0.5f")
    val metaspaceSize = Some("512m")
    val taskOffHeap = Some("1G")
  }

  private case object TaskManager32G extends MemoryTier {
    val tmProcessMemory = Some("32G")
    val taskSlots = 2
    val networkMin = Some("512m")
    val networkMax = Some("1G")
    val managedFraction = Some("0.5f")
    val metaspaceSize = Some("512m")
    val taskOffHeap = Some("512m")
  }

  private case object TaskManager16G extends MemoryTier {
    val tmProcessMemory = Some("16G")
    val taskSlots = 1
    val networkMin = Some("256m")
    val networkMax = Some("512m")
    val managedFraction = Some("0.5f")
    val metaspaceSize = Some("512m")
    val taskOffHeap = Some("256m")
  }

  private case object SmallTaskManager extends MemoryTier {
    val tmProcessMemory = None
    val taskSlots = 1
    val networkMin = None
    val networkMax = None
    val managedFraction = None
    val metaspaceSize = None
    val taskOffHeap = None
  }

  private def parseTmMemoryTier(jobProperties: Map[String, String]): MemoryTier = {
    jobProperties.get("taskmanager.memory.process.size") match {
      case Some(v) if v.toUpperCase.startsWith("64") => TaskManager64G
      case Some(v) if v.toUpperCase.startsWith("32") => TaskManager32G
      case Some(v) if v.toUpperCase.startsWith("16") => TaskManager16G
      case None                                      => TaskManager16G
      case _                                         => SmallTaskManager
    }
  }

  private def flinkMemoryConfig(tier: MemoryTier): Map[String, String] = {
    val base = Map(
      "jobmanager.memory.process.size" -> tier.jmProcessMemory,
      "taskmanager.numberOfTaskSlots" -> tier.taskSlots.toString
    )
    val optional = List(
      tier.tmProcessMemory.map("taskmanager.memory.process.size" -> _),
      tier.networkMin.map("taskmanager.memory.network.min" -> _),
      tier.networkMax.map("taskmanager.memory.network.max" -> _),
      tier.managedFraction.map("taskmanager.memory.managed.fraction" -> _),
      tier.metaspaceSize.map("taskmanager.memory.jvm-metaspace.size" -> _),
      tier.taskOffHeap.map("taskmanager.memory.task.off-heap.size" -> _)
    ).flatten.toMap
    base ++ optional
  }

  def buildFlinkConfiguration(flinkCheckpointUri: String, jobProperties: Map[String, String]): Map[String, String] = {
    val tier = parseTmMemoryTier(jobProperties)

    // extraFlinkConfig (cloud-specific, e.g. IRSA credentials) merged after base;
    // jobProperties applied last so callers can override anything.
    // All jars are available via FLINK_CLASSPATH=/opt/flink/usrlib/*,
    // which Flink surfaces as pipeline.classpaths.
    Map(
      "state.savepoints.dir" -> flinkCheckpointUri,
      "state.checkpoints.dir" -> flinkCheckpointUri,
      "state.backend.type" -> "rocksdb",
      "state.backend.incremental" -> "true",
      // override the local dir as the default path can exceed filesystem name length limits
      "state.backend.rocksdb.localdir" -> "/tmp/flink-state",
      "state.checkpoint-storage" -> "filesystem",
      "rest.profiling.enabled" -> "true",
      "state.checkpoints.num-retained" -> MaxRetainedCheckpoints
    ) ++ flinkMemoryConfig(tier) ++ resolvedExtraFlinkConfig ++ jobProperties
  }

  private def flinkDeploymentCrdContext: CustomResourceDefinitionContext =
    new CustomResourceDefinitionContext.Builder()
      .withGroup("flink.apache.org")
      .withVersion("v1beta1")
      .withScope("Namespaced")
      .withPlural("flinkdeployments")
      .withKind("FlinkDeployment")
      .build()

  private def k8sClient = new KubernetesClientBuilder()
    .withConfig(k8sConfig.getOrElse(Config.autoConfigure(null)))
    .build()

  def status(deploymentName: String, namespace: String): JobStatusType =
    statusWithCreationTime(deploymentName, namespace)._1

  /** Returns the job status alongside the CRD creation timestamp in a single K8s call.
    * Callers that need the creation time for a submission grace-period check should use
    * this method to avoid a second round-trip.
    */
  def statusWithCreationTime(deploymentName: String, namespace: String): (JobStatusType, Option[Instant]) = {
    val client = k8sClient
    try {
      val resource = client
        .genericKubernetesResources(flinkDeploymentCrdContext)
        .inNamespace(namespace)
        .withName(deploymentName)
        .get()

      if (resource == null) return (JobStatusType.UNKNOWN, None)

      val statusMap = Option(resource.getAdditionalProperties.get("status"))
        .collect { case m: java.util.Map[_, _] => m }

      val lifecycleState = statusMap
        .flatMap(s => Option(s.get("lifecycleState")))
        .map(_.toString)
        .getOrElse("")

      val jmDeploymentStatus = statusMap
        .flatMap(s => Option(s.get("jobManagerDeploymentStatus")))
        .map(_.toString)
        .getOrElse("")

      val creationTimestamp = Option(resource.getMetadata.getCreationTimestamp)
        .map(ts => Instant.from(DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(ts)))

      (resolveStatus(deploymentName, lifecycleState, jmDeploymentStatus, creationTimestamp), creationTimestamp)
    } finally {
      client.close()
    }
  }

  def resolveStatus(deploymentName: String,
                    lifecycleState: String,
                    jmDeploymentStatus: String,
                    creationTimestamp: Option[Instant]): JobStatusType = {
    lifecycleState match {
      case "STABLE"                                              => JobStatusType.RUNNING
      case "SUSPENDED" | "FAILED"                                => JobStatusType.FAILED
      case "DEPLOYED" | "CREATED" | "UPGRADING" | "ROLLING_BACK" =>
        // The operator sets jobManagerDeploymentStatus=ERROR for detected pod failures (e.g.
        // CrashLoopBackOff on the init container), but lifecycleState stays DEPLOYED. Similarly, unschedulable TMs
        // leave the deployment stuck in DEPLOYED indefinitely. In these cases we want to fail relatively
        // quickly so the orchestrator can surface the error and avoid long waits, but we also want to give
        // the operator some time to react to transient issues (e.g. scheduling delays due to cluster
        // autoscaling or temporary capacity shortages) before we declare failure.
        val timedOut = creationTimestamp.exists { created =>
          Instant.now().isAfter(created.plusSeconds(DeploymentPendingTimeout.toSeconds))
        }
        if (jmDeploymentStatus == "ERROR") {
          // ERROR means the operator has positively detected a pod failure (e.g. CrashLoopBackOff)
          // — safe to fail immediately regardless of age.
          logger.warn(s"FlinkDeployment $deploymentName has jobManagerDeploymentStatus=ERROR, treating as FAILED")
          JobStatusType.FAILED
        } else if (timedOut && jmDeploymentStatus == "MISSING") {
          // MISSING is the normal state immediately after creation before the JM pod starts.
          // Only treat it as a failure once the deployment has been around long enough that
          // the JM should have started by now.
          logger.warn(
            s"FlinkDeployment $deploymentName has jobManagerDeploymentStatus=MISSING after $DeploymentPendingTimeout, treating as FAILED")
          JobStatusType.FAILED
        } else if (timedOut) {
          logger.warn(
            s"FlinkDeployment $deploymentName has been in $lifecycleState for more than $DeploymentPendingTimeout without reaching STABLE, treating as FAILED")
          JobStatusType.FAILED
        } else {
          JobStatusType.PENDING
        }
      case _ =>
        logger.warn(
          s"Flink deployment $deploymentName has unrecognized lifecycleState=$lifecycleState, treating as UNKNOWN")
        JobStatusType.UNKNOWN
    }
  }

  def delete(deploymentName: String, namespace: String): Unit = {
    val client = k8sClient
    try {
      client
        .genericKubernetesResources(flinkDeploymentCrdContext)
        .inNamespace(namespace)
        .withName(deploymentName)
        .delete()
      logger.info(s"Deleted FlinkDeployment: $deploymentName in namespace: $namespace")
    } finally {
      client.close()
    }
  }

  def submit(jobId: String,
             mainClass: String,
             mainJarUri: String,
             jarUris: Array[String],
             flinkCheckpointUri: String,
             maybeSavepointUri: Option[String],
             maybeFlinkJarsUri: Option[String],
             jobProperties: Map[String, String],
             args: Seq[String],
             serviceAccount: String,
             namespace: String,
             envVars: Map[String, String] = Map.empty,
             nodeSelector: Map[String, String] = Map.empty): String = {

    val deploymentName = sanitizeDeploymentName(s"flink-$jobId")
    val basePath = maybeFlinkJarsUri.getOrElse(defaultJarsBasePath)
    val flinkJars = additionalFlinkJars(basePath) ++ extraJarNames.map { name =>
      val base = if (basePath.endsWith("/")) basePath else basePath + "/"
      base + name
    }
    val allJarUris = (jarUris ++ flinkJars).distinct
    val tier = parseTmMemoryTier(jobProperties)

    val flinkConfiguration = buildFlinkConfiguration(flinkCheckpointUri, jobProperties)

    val spec = new java.util.HashMap[String, Object]()
    spec.put("image", flinkImage)
    spec.put("flinkVersion", "v1_20")
    spec.put("serviceAccount", serviceAccount)
    spec.put("flinkConfiguration", flinkConfiguration.asJava)

    val allJars = (mainJarUri +: allJarUris).distinct
    val containerSpec = buildInitContainerSpec(allJars)

    // Inject FLINK_-prefixed keys from flinkEnvVars as pod env vars on JM/TM containers,
    // stripping the FLINK_ routing prefix so underlying libs see the name they expect
    // (e.g. FLINK_SASL_JAAS_CONFIG -> SASL_JAAS_CONFIG).
    // These are kept out of flinkConfiguration (and therefore out of Flink's startup config logging).
    val podEnvVars = envVars
      .collect {
        case (k, v) if k.startsWith("FLINK_") =>
          val envVarMap = new java.util.HashMap[String, String]()
          envVarMap.put("name", k.stripPrefix("FLINK_"))
          envVarMap.put("value", v)
          envVarMap
      }
      .toList
      .asJava

    val allEnvVars = new java.util.ArrayList[java.util.Map[String, String]]()
    allEnvVars.addAll(containerSpec.envVars)
    allEnvVars.addAll(podEnvVars)

    // Pod resource memory must match what Flink is configured to use.
    // For sized tiers (64G/32G) we set it explicitly; for small tiers we use whatever jobProperties
    // supplied, falling back to a conservative 4G default so the pod request is never left unset.
    val tmPodMemory = flinkConfiguration.getOrElse("taskmanager.memory.process.size", "4G")
    val jmPodMemory = flinkConfiguration.getOrElse("jobmanager.memory.process.size", "4G")

    spec.put(
      "jobManager",
      buildComponentSpec(
        memory = jmPodMemory,
        cpu = 1.0,
        replicas = Some(1),
        containerSpec.initContainers,
        allEnvVars,
        containerSpec.volumeMounts,
        containerSpec.volumes,
        nodeSelector = nodeSelector
      )
    )
    spec.put(
      "taskManager",
      buildComponentSpec(
        memory = tmPodMemory,
        cpu = tier.taskSlots.toDouble,
        replicas = None,
        containerSpec.initContainers,
        allEnvVars,
        containerSpec.volumeMounts,
        containerSpec.volumes,
        nodeSelector = nodeSelector
      )
    )

    val localJarUri = s"local:///opt/flink/usrlib/${mainJarUri.split("/").last}"
    val job = new java.util.HashMap[String, Object]()
    job.put("jarURI", localJarUri)
    job.put("entryClass", mainClass)
    job.put("args", args.toArray)
    // Operator validates parallelism > 0; actual job-level parallelism is set inside the Flink job itself
    job.put("parallelism", Integer.valueOf(1))
    job.put("state", "running")
    maybeSavepointUri match {
      case Some(savepointUri) =>
        job.put("upgradeMode", "savepoint")
        job.put("initialSavepointPath", savepointUri)
      case None =>
        // stateless allows cold restarts without HA; last-state requires HA to be enabled
        job.put("upgradeMode", "stateless")
    }
    spec.put("job", job)

    val client = k8sClient

    try {
      val resource = new GenericKubernetesResource()
      resource.setApiVersion("flink.apache.org/v1beta1")
      resource.setKind("FlinkDeployment")
      resource.setMetadata(
        new io.fabric8.kubernetes.api.model.ObjectMetaBuilder()
          .withName(deploymentName)
          .withNamespace(namespace)
          .build()
      )
      resource.setAdditionalProperty("spec", spec)

      val created = client
        .genericKubernetesResources(flinkDeploymentCrdContext)
        .inNamespace(namespace)
        .resource(resource)
        .create()

      logger.info(s"Created FlinkDeployment: $deploymentName in namespace: $namespace")

      if (ingressBaseUrl.isDefined) {
        try {
          createFlinkIngress(client, deploymentName, namespace, created.getMetadata.getUid)
        } catch {
          case e: Exception =>
            logger.warn(
              s"FlinkDeployment '$deploymentName' (namespace=$namespace, uid=${created.getMetadata.getUid}) " +
                s"was created successfully but ingress setup failed — Flink UI may be unavailable: ${e.getMessage}",
              e
            )
        }
      }

      deploymentName
    } finally {
      client.close()
    }
  }

  // Create an Ingress resource for the Flink REST UI, with rules specific to this deployment. This allows
  // nginx-ingress to route /flink/{deploymentName} to the correct Flink
  // REST service, and ensures the Ingress is automatically deleted when the FlinkDeployment is removed.
  def createFlinkIngress(client: io.fabric8.kubernetes.client.KubernetesClient,
                         deploymentName: String,
                         namespace: String,
                         ownerUid: String): Unit = {
    // Extract host from ingressBaseUrl so the ingress rule is host-specific, matching at the
    // same specificity level as the hub catch-all ingress. Without a host, nginx-ingress prefers
    // host-specific rules (even with path: /) over wildcard-host rules with longer paths.
    val host = ingressBaseUrl
      .flatMap { url =>
        scala.util.Try(new java.net.URI(url).getHost).toOption.filter(_ != null)
      }
      .getOrElse(throw new IllegalArgumentException(
        s"Could not extract host from ingressBaseUrl: $ingressBaseUrl — cannot create host-specific ingress rule"
      ))

    val ingress = new IngressBuilder()
      .withNewMetadata()
      .withName(deploymentName)
      .withNamespace(namespace)
      // Owner reference ensures Kubernetes GC deletes the Ingress when the FlinkDeployment
      // is removed for any reason (crash, operator cleanup, manual delete).
      .addNewOwnerReference()
      .withApiVersion("flink.apache.org/v1beta1")
      .withKind("FlinkDeployment")
      .withName(deploymentName)
      .withUid(ownerUid)
      .withController(true)
      .withBlockOwnerDeletion(true)
      .endOwnerReference()
      .addToAnnotations("nginx.ingress.kubernetes.io/use-regex", "true")
      .addToAnnotations("nginx.ingress.kubernetes.io/rewrite-target", "/$2")
      .addToAnnotations("nginx.ingress.kubernetes.io/proxy-read-timeout", "3600")
      .addToAnnotations("nginx.ingress.kubernetes.io/proxy-send-timeout", "3600")
      .addToAnnotations("nginx.ingress.kubernetes.io/proxy-http-version", "1.1")
      .endMetadata()
      .withNewSpec()
      .withIngressClassName("nginx-hub")
      .addNewRule()
      .withHost(host)
      .withNewHttp()
      .addNewPath()
      .withPath(s"/flink/$deploymentName(/|$$)(.*)")
      .withPathType("ImplementationSpecific")
      .withNewBackend()
      .withNewService()
      .withName(s"$deploymentName-rest")
      .withNewPort()
      .withNumber(8081)
      .endPort()
      .endService()
      .endBackend()
      .endPath()
      .endHttp()
      .endRule()
      .endSpec()
      .build()

    client.network().v1().ingresses().inNamespace(namespace).resource(ingress).create()
    logger.info(s"Created Ingress: $deploymentName in namespace: $namespace")
  }

  private[cloud_k8s] def buildComponentSpec(
      memory: String,
      cpu: Double,
      replicas: Option[Int],
      initContainers: java.util.List[java.util.Map[String, Object]],
      envVars: java.util.List[java.util.Map[String, String]],
      volumeMounts: java.util.List[java.util.Map[String, String]],
      volumes: java.util.List[java.util.Map[String, Object]],
      nodeSelector: Map[String, String] = Map.empty): java.util.Map[String, Object] = {
    val component = new java.util.HashMap[String, Object]()

    val resource = new java.util.HashMap[String, Object]()
    resource.put("memory", memory)
    resource.put("cpu", Double.box(cpu))
    component.put("resource", resource)

    replicas.foreach(r => component.put("replicas", Integer.valueOf(r)))

    val mainContainer = new java.util.HashMap[String, Object]()
    mainContainer.put("name", "flink-main-container")
    mainContainer.put("env", envVars)
    mainContainer.put("volumeMounts", volumeMounts)

    val podSpec = new java.util.HashMap[String, Object]()
    podSpec.put("initContainers", initContainers)
    podSpec.put("containers", {
                  val list = new java.util.ArrayList[java.util.Map[String, Object]]()
                  list.add(mainContainer)
                  list
                })
    podSpec.put("volumes", volumes)
    if (nodeSelector.nonEmpty) {
      podSpec.put("nodeSelector", nodeSelector.asJava)
    }

    val podMeta = new java.util.HashMap[String, Object]()
    podMeta.put(
      "annotations", {
        val m = new java.util.HashMap[String, String]()
        m.put("prometheus.io/scrape", "true")
        m.put("prometheus.io/port", "9250")
        m.put("prometheus.io/path", "/metrics")
        m
      }
    )
    if (podTemplateLabels.nonEmpty) {
      val labelsMap = new java.util.HashMap[String, String]()
      podTemplateLabels.foreach { case (k, v) => labelsMap.put(k, v) }
      podMeta.put("labels", labelsMap)
    }

    val podTemplate = new java.util.HashMap[String, Object]()
    podTemplate.put("metadata", podMeta)
    podTemplate.put("spec", podSpec)
    component.put("podTemplate", podTemplate)

    component
  }

}

object K8sFlinkSubmitter {

  /** Holds the pod-level additions produced by a cloud-specific init container builder.
    * Fields map directly to the Flink Kubernetes Operator's pod template structure.
    */
  case class InitContainerSpec(
      initContainers: java.util.List[java.util.Map[String, Object]],
      envVars: java.util.List[java.util.Map[String, String]],
      volumeMounts: java.util.List[java.util.Map[String, String]],
      volumes: java.util.List[java.util.Map[String, Object]]
  )

  // How long a deployment may stay in a non-STABLE pending state before we declare it failed.
  val DeploymentPendingTimeout: Duration = Duration(10, TimeUnit.MINUTES)

  // Flink Kubernetes Operator enforces a 45-char limit on FlinkDeployment names.
  def sanitizeDeploymentName(raw: String): String = {
    val cleaned = raw.toLowerCase
      .replaceAll("[^a-z0-9-]", "-")
      .replaceAll("^-+|-+$", "")
      .take(45)
      .replaceAll("-+$", "")
    require(cleaned.nonEmpty, "jobId must produce a valid Kubernetes name")
    cleaned
  }
}
