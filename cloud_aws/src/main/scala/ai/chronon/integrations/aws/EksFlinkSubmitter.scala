package ai.chronon.integrations.aws

import ai.chronon.integrations.cloud_k8s.K8sFlinkSubmitter
import ai.chronon.integrations.cloud_k8s.K8sFlinkSubmitter.InitContainerSpec
import io.fabric8.kubernetes.client.Config

import scala.jdk.CollectionConverters._

/** AWS-specific constants and factory for [[K8sFlinkSubmitter]].
  *
  * Provides the EMR-on-EKS Flink image, an S3-backed init container spec builder (using the AWS
  * CLI to stage JARs into /opt/flink/usrlib/), and IRSA credential + Prometheus metrics config
  * that are injected into the cloud-agnostic [[K8sFlinkSubmitter]].
  */
object EksFlinkSubmitter {
  // EMR on EKS Flink image — pinned by digest so AWS pushing a new `:latest` doesn't
  // change /opt/flink/lib/ between job restarts (which surfaces as
  // IllegalStateException: "The library registration references a different set of library BLOBs").
  val FlinkImage =
    "public.ecr.aws/emr-on-eks/flink/emr-7.12.0-flink@sha256:13efc2b96c4b2385553bf5423dd2f5ba3467cef2711cafa78502a1d64b4eafe6"

  // Default path for the libs we need for Spark expression eval in Flink
  val DefaultS3FlinkJarsBasePath = "s3://zipline-spark-libs/spark-3.5.3/libs/"

  // Jars required on EKS that other engines like Dataproc provide via their pre-installed Hadoop/YARN host classpath.
  val EksOnlyAdditionalJarNames: Array[String] = Array(
    "hadoop-client-runtime-3.3.6.jar",
    "jakarta.servlet-api-4.0.3.jar"
  )

  private[aws] def eksAdditionalFlinkJars(flinkJarsBasePath: String): Array[String] = {
    val base = if (flinkJarsBasePath.endsWith("/")) flinkJarsBasePath else flinkJarsBasePath + "/"
    EksOnlyAdditionalJarNames.map(base + _)
  }

  def shellQuote(s: String): String = "'" + s.replace("'", "'\"'\"'") + "'"

  // AWS IRSA credential config and Prometheus metrics reporter — injected into K8sFlinkSubmitter
  // so the base class stays cloud-agnostic.
  val extraAwsFlinkConfig: Map[String, String] = Map(
    // Use Web Identity Token credentials (IRSA) instead of EC2 instance metadata
    "s3.access.key" -> "",
    "s3.secret.key" -> "",
    "fs.s3a.aws.credentials.provider" -> "com.amazonaws.auth.WebIdentityTokenCredentialsProvider",
    // Expose Flink metrics via Prometheus HTTP endpoint so the AWS managed scraper can collect them.
    // Port range handles multiple TM pods co-scheduled on the same node.
    "metrics.reporters" -> "prom",
    "metrics.reporter.prom.factory.class" -> "org.apache.flink.metrics.prometheus.PrometheusReporterFactory",
    "metrics.reporter.prom.port" -> "9250-9260"
  )

  /** Builds the init container spec for downloading JARs from S3 using the AWS CLI. */
  def buildInitContainerSpec(jarUris: Array[String]): InitContainerSpec = {
    val downloadCommands = jarUris
      .map { jar =>
        val dest = s"/opt/flink/usrlib/${jar.split("/").last}"
        s"aws s3 cp ${shellQuote(jar)} ${shellQuote(dest)}"
      }
      .mkString(" && ")

    val initContainer = new java.util.HashMap[String, Object]()
    initContainer.put("name", "download-jars")
    initContainer.put("image", "amazon/aws-cli:2.34.45")
    val commands = new java.util.ArrayList[String]()
    commands.add("sh"); commands.add("-c"); commands.add(downloadCommands)
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

  /** Creates a [[K8sFlinkSubmitter]] configured for AWS EKS. */
  def apply(k8sConfig: Option[Config] = None, ingressBaseUrl: Option[String] = None): K8sFlinkSubmitter =
    new K8sFlinkSubmitter(
      flinkImage = FlinkImage,
      buildInitContainerSpec = buildInitContainerSpec,
      extraFlinkConfig = extraAwsFlinkConfig,
      extraJarNames = EksOnlyAdditionalJarNames,
      defaultJarsBasePath = DefaultS3FlinkJarsBasePath,
      k8sConfig = k8sConfig,
      ingressBaseUrl = ingressBaseUrl
    )
}
