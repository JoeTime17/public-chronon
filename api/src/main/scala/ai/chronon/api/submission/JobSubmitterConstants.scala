package ai.chronon.api.submission

object JobSubmitterConstants {
  val MainClass = "mainClass"
  val JarURI = "jarUri"
  val FlinkMainJarURI = "flinkMainJarUri"
  val FlinkPubSubConnectorJarURI = "flinkPubSubConnectorJarUri"
  val FlinkKinesisConnectorJarURI = "flinkKinesisConnectorJarUri"
  val FlinkJarsUri = "flinkJarsUri"
  val AdditionalJars = "additionalJars"
  val SavepointUri = "savepointUri"
  val FlinkCheckpointUri = "flinkCheckpointUri"

  val JobId = "jobId"

  // Only lowercase, numbers, and dashes allowed for key labels in Dataproc
  val JobType = "job-type"
  val MetadataName = "metadata-name"
  val ZiplineVersion = "zipline-version"

  val SparkJobType = "spark"
  val FlinkJobType = "flink"

  // Main class constants
  val BatchRunnerClass = "ai.chronon.spark.batch.BatchNodeRunner"
  val KVUploadRunnerClass = "ai.chronon.spark.kv_store.KVUploadNodeRunner"
  val ModelRunnerClass = "ai.chronon.spark.model.ModelNodeRunner"
  val FlinkMainClass = "ai.chronon.flink.FlinkJob"

  // EMR specific properties
  val ClusterInstanceCount = "clusterInstanceCount"
  val ClusterInstanceType = "clusterInstanceType"
  val ClusterIdleTimeout = "clusterIdleTimeout"
  val ClusterId = "jobFlowId"
  val ClusterName = "clusterName"
  val SubnetId = "subnetId"
  val SecurityGroupId = "securityGroupId"
  val EmrReleaseLabel = "emrReleaseLabel"

  // EKS (Flink on EKS) specific properties
  val EksServiceAccount = "eksServiceAccount"
  val EksNamespace = "eksNamespace"
  val EksNodeSelector = "eksNodeSelector"

  val JarUriArgKeyword = "--jar-uri"
  val JobTypeArgKeyword = "--job-type"
  val MainClassKeyword = "--main-class"
  val FlinkMainJarUriArgKeyword = "--flink-main-jar-uri"
  val FlinkPubSubJarUriArgKeyword = "--flink-pubsub-jar-uri"
  val FlinkKinesisJarUriArgKeyword = "--flink-kinesis-jar-uri"
  val FlinkJarsUriArgKeyword = "--flink-jars-uri"
  val AdditionalJarsUriArgKeyword = "--additional-jars"
  val FilesArgKeyword = "--files"
  val ConfTypeArgKeyword = "--conf-type"
  val ConfPathArgKeyword = "--conf-path"
  val LocalConfPathArgKeyword = "--local-conf-path"
  val OriginalModeArgKeyword = "--original-mode"
  val ZiplineVersionArgKeyword = "--zipline-version"
  val GroupByNameArgKeyword = "--groupby-name"
  val OnlineClassArgKeyword = "--online-class"
  val ApiPropsArgPrefix = "-Z"
  val LocalZiplineVersionArgKeyword = "--local-zipline-version"
  val StreamingManifestPathArgKeyword = "--streaming-manifest-path"
  val StreamingCheckpointPathArgKeyword = "--streaming-checkpoint-path"
  val StreamingModeArgKeyword = "--streaming-mode"

  val StreamingVersionCheckDeploy = "--version-check"
  val StreamingTopicOverrideArgKeyword = "--topic-override"

  val StreamingLatestSavepointArgKeyword = "--latest-savepoint"
  val StreamingCustomSavepointArgKeyword = "--custom-savepoint"
  val StreamingNoSavepointArgKeyword = "--no-savepoint"

  val JobIdArgKeyword = "--job-id"
  val EksServiceAccountArgKeyword = "--eks-service-account"
  val EksNamespaceArgKeyword = "--eks-namespace"
  val EksNodeSelectorArgKeyword = "--eks-node-selector"

  val FlinkDeploymentModeArgKeyword = "--flink-deployment-mode"
  val FlinkDeploymentModeDefault = "default"
  val FlinkDeploymentModeApplication = "application"

  val SharedInternalArgs: Set[String] = Set(
    JarUriArgKeyword,
    JobTypeArgKeyword,
    MainClassKeyword,
    FlinkMainJarUriArgKeyword,
    FlinkPubSubJarUriArgKeyword,
    FlinkKinesisJarUriArgKeyword,
    FlinkJarsUriArgKeyword,
    AdditionalJarsUriArgKeyword,
    LocalConfPathArgKeyword,
    OriginalModeArgKeyword,
    FilesArgKeyword,
    ZiplineVersionArgKeyword,
    LocalZiplineVersionArgKeyword,
    StreamingModeArgKeyword,
    StreamingLatestSavepointArgKeyword,
    StreamingCustomSavepointArgKeyword,
    StreamingNoSavepointArgKeyword,
    StreamingCheckpointPathArgKeyword,
    StreamingVersionCheckDeploy,
    JobIdArgKeyword,
    EksServiceAccountArgKeyword,
    EksNamespaceArgKeyword,
    EksNodeSelectorArgKeyword,
    FlinkDeploymentModeArgKeyword
  )

  // Generic spark cluster name environment variable - works across all cloud providers
  val SparkClusterNameEnvVar = "SPARK_CLUSTER_NAME"

  val GcpBigtableInstanceIdEnvVar = "GCP_BIGTABLE_INSTANCE_ID"
  val GcpProjectIdEnvVar = "GCP_PROJECT_ID"
  val GcpRegionEnvVar = "GCP_REGION"
  val GcpLocationEnvVar = "GCP_LOCATION"
  // Deprecated: Use ClusterNameEnvVar instead
  val GcpDataprocClusterNameEnvVar = "GCP_DATAPROC_CLUSTER_NAME"
  val EnableUploadKVClientEnvVar = "ENABLE_UPLOAD_CLIENTS"

  // AWS-specific environment variables
  // Deprecated: Use ClusterNameEnvVar instead
  val EmrClusterNameEnvVar = "EMR_CLUSTER_NAME"

  val TableStatsDatasetNameArgKeyword = "--table-stats-dataset"

  val CheckIfJobIsRunning = "check-if-job-is-running"
  val StreamingDeploy = "deploy"

  // We use incremental checkpoints and we cap how many we keep around
  val MaxRetainedCheckpoints: String = "10"

  // Flink jobs built with thin jars need Spark catalyst/SQL deps on the classpath.
  // When FlinkJarsUri is provided, these filenames are resolved against that base path.
  val FlinkAdditionalJarNames: Array[String] = Array(
    "commons-collections4-4.4.jar",
    "commons-compiler-3.1.9.jar",
    "janino-3.1.9.jar",
    "json4s-ast_2.12-3.7.0-M11.jar",
    "json4s-core_2.12-3.7.0-M11.jar",
    "kryo-shaded-4.0.2.jar",
    "metrics-core-4.2.19.jar",
    "metrics-json-4.2.19.jar",
    "spark-catalyst_2.12-3.5.3.jar",
    "spark-common-utils_2.12-3.5.3.jar",
    "spark-core_2.12-3.5.3.jar",
    "spark-kvstore_2.12-3.5.3.jar",
    "spark-launcher_2.12-3.5.3.jar",
    "spark-hive_2.12-3.5.3.jar",
    "spark-network-common_2.12-3.5.3.jar",
    "spark-network-shuffle_2.12-3.5.3.jar",
    "spark-sql-api_2.12-3.5.3.jar",
    "spark-sql_2.12-3.5.3.jar",
    "spark-unsafe_2.12-3.5.3.jar",
    "xbean-asm9-shaded-4.23.jar"
  )

  def additionalFlinkJars(flinkJarsBasePath: String): Array[String] = {
    val base = if (flinkJarsBasePath.endsWith("/")) flinkJarsBasePath else flinkJarsBasePath + "/"
    FlinkAdditionalJarNames.map(base + _)
  }
}
