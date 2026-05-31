from ai.chronon.repo.cluster import generate_dataproc_cluster_config, generate_emr_cluster_config
from ai.chronon.repo.constants import RunMode
from ai.chronon.repo.spark_catalog_confs import *
from ai.chronon.types import ClusterConfigProperties, ConfigProperties, EnvironmentVariables, Team

default = Team(
    description="Default team",
    email="ml-infra@<customer>.com",  # TODO: Infra team email
    outputNamespace="default",
    conf=ConfigProperties(
        common={
            "spark.chronon.partition.column": "ds",
        }
    ),
    env=EnvironmentVariables(
        common={
            "VERSION": "latest",
            "CUSTOMER_ID": "dev",
            "FRONTEND_URL": "http://localhost:3000",
            "HUB_URL": "http://localhost:3903",
            "EVAL_URL": "http://localhost:3904",
            "FETCHER_URL": "http://localhost:9000",
        },
    ),
)

test = Team(
    outputNamespace="test",
    env=EnvironmentVariables(
        common={
            "GCP_BIGTABLE_INSTANCE_ID": "test-instance"  # example, custom bigtable instance
        },
        modeEnvironments={
            RunMode.BACKFILL: {
            },
            RunMode.UPLOAD: {
            }
        }
    ),
)

gcp = Team(
    outputNamespace="data",
    env=EnvironmentVariables(
        common={
            "CLOUD_PROVIDER": "gcp",
            "CUSTOMER_ID": "canary",
            "VERSION": "latest",
            "GCP_PROJECT_ID": "canary-443022",
            "GCP_REGION": "us-central1",
            "SPARK_CLUSTER_NAME": "zipline-canary-cluster",
            "GCP_BIGTABLE_INSTANCE_ID": "zipline-canary-instance",
            "ENABLE_PUBSUB": "true",
            "ARTIFACT_PREFIX": "gs://zipline-artifacts-canary",
            "WAREHOUSE_PREFIX": "gs://zipline-warehouse-canary",
            "FLINK_STATE_URI": "gs://zipline-warehouse-canary/flink-state",
            "CHRONON_ONLINE_ARGS": " -Ztasks=4",
            "FRONTEND_URL": "http://localhost:3000",
            "HUB_URL": "http://localhost:3903",
            "EVAL_URL": "http://localhost:3904",
            # Sentinel — referenced by test_canary_compile.py to verify that
            # the prod compile pass uses only this `teams.py` file (never
            # `teams.canary.py`). Must not appear in any file under
            # compiled_canary/.
            "PROD_ONLY_SENTINEL_GCP": "prod-only-sentinel-value-9b8a7c",
        },
        modeEnvironments={
            RunMode.UPLOAD: {
                "SPARK_CLUSTER_NAME": "zipline-transient-upload-cluster"
            }
        }
    ),
    conf=ConfigProperties(
        common={
            **BigQueryConfiguration({
                "spark.sql.catalog.spark_catalog.warehouse": "gs://zipline-warehouse-canary/data/tables/",
                "spark.sql.catalog.spark_catalog.gcp.bigquery.location": "us-central1",
                "spark.sql.catalog.spark_catalog.gcp.bigquery.project-id": "canary-443022",
            }),

            "spark.chronon.table.format_provider.class": "ai.chronon.integrations.cloud_gcp.GcpFormatProvider",
            "spark.chronon.table_write.format": "iceberg",

            "spark.chronon.partition.format": "yyyy-MM-dd",
            "spark.chronon.partition.column": "ds",

            "spark.chronon.coalesce.factor": "10",
            "spark.default.parallelism": "10",
            "spark.sql.shuffle.partitions": "10",
            "spark.driver.memory": "512m",
            "spark.driver.cores": "1",
            "spark.executor.memory": "512m",
            "spark.executor.cores": "1",

            # Chronon OTel metrics. Default off + HTTP to localhost:4318. The canary cluster's
            # Ops Agent listens on the gRPC port (4317), not 4318, so steer the SDK accordingly.
            "spark.driver.extraJavaOptions": " ".join([
                "-Dai.chronon.metrics.enabled=true",
                "-Dai.chronon.metrics.reader=grpc",
                "-Dai.chronon.metrics.exporter.url=http://localhost:4317",
            ]),
            # Sentinel — prod-only conf marker for the test.
            "spark.chronon.test.prod_only_sentinel": "prod-only-conf-sentinel-4f5a6b",
        },
        modeConfigs={
        }
    ),
    clusterConf=ClusterConfigProperties(
        modeClusterConfigs={
            RunMode.UPLOAD: {
                "dataproc.config": generate_dataproc_cluster_config(2, "canary-443022", "gs://zipline-artifacts-canary",
                                                                    idle_timeout="7200s",
                                                                    worker_host_type="n2-highmem-4",
                                                                    master_host_type="n2-highmem-8"),
                # Sentinel — prod-only cluster-conf marker for the test.
                "prod_only_sentinel_cluster": "prod-only-cluster-sentinel-2e3f4a",
            }
        }
    ),
)

aws = Team(
    outputNamespace="data",
    env=EnvironmentVariables(
        common={
            "CLOUD_PROVIDER": "aws",
            "CUSTOMER_ID": "canary",
            "VERSION": "latest",
            "AWS_REGION": "us-west-2",
            "SPARK_CLUSTER_NAME": "zipline-emr-canary",
            "ARTIFACT_PREFIX": "s3://zipline-artifacts-canary",
            "WAREHOUSE_PREFIX": "s3://zipline-warehouse-canary",
            "FLINK_STATE_URI": "s3://zipline-warehouse-canary/flink-state",
            "CHRONON_ONLINE_ARGS": " -Ztasks=1",
            "FRONTEND_URL": "https://canary-aws.zipline.ai",
            "HUB_URL": "https://canary-orch-aws.zipline.ai",
            "EVAL_URL": "https://canary-eval-aws.zipline.ai",
            "ENABLE_KINESIS": "true",
            "FLINK_JARS_URI": "s3://zipline-artifacts-canary/spark-3.5.3/libs/",
        },
        modeEnvironments={
            RunMode.UPLOAD: {
            }
        }
    ),
    conf=ConfigProperties(
        common={
            **GlueConfiguration({
                "spark.sql.catalog.spark_catalog.warehouse": "s3://zipline-warehouse-canary/data/tables/",
            }),
            "spark.chronon.partition.format": "yyyy-MM-dd",
            "spark.chronon.partition.column": "ds",
            "spark.chronon.table_write.format": "iceberg",
            "spark.chronon.table_write.upload.format": "ion",
            "spark.chronon.table_write.upload.location": "s3://zipline-warehouse-canary/data/ion_uploads/",

            "spark.chronon.coalesce.factor": "10",
            "spark.default.parallelism": "10",
            "spark.sql.shuffle.partitions": "10",
            "spark.driver.memory": "1g",
            "spark.driver.cores": "1",
            "spark.executor.memory": "1g",
            "spark.executor.cores": "1",
            "taskmanager.memory.process.size": "4G",
        },
        modeConfigs={
            RunMode.BACKFILL: {
            }
        }
    ),
    clusterConf=ClusterConfigProperties(
        common={
            "emr.config": generate_emr_cluster_config(
                instance_count=3,
                subnet_name="zipline-canary-subnet-main",
                security_group_name="zipline-canary-sg",
                instance_type="m5.xlarge",
                idle_timeout=7200,
                release_label="emr-7.12.0"
            )
        }
    ),
)

quickstart = Team(
    description="K8s team for local Kind cluster development (cloud_k8s/local)",
    email="dev@example.com",
    outputNamespace="quickstart",
    conf=ConfigProperties(
        common={
            "spark.chronon.partition.column": "ds",
            # Iceberg catalog via JDBC (PostgreSQL in-cluster)
            "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.iceberg.type": "jdbc",
            "spark.sql.catalog.iceberg.uri": "jdbc:postgresql://postgres.chronon.svc.cluster.local:5432/iceberg_catalog",
            "spark.sql.catalog.iceberg.jdbc.user": "chronon",
            "spark.sql.catalog.iceberg.jdbc.password": "chronon",
            "spark.sql.catalog.iceberg.warehouse": "s3a://warehouse/",
            # S3A configuration for MinIO
            "spark.hadoop.fs.s3a.endpoint": "http://minio.chronon.svc.cluster.local:9000",
            "spark.hadoop.fs.s3a.access.key": "minioadmin",
            "spark.hadoop.fs.s3a.secret.key": "minioadmin",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "spark.sql.defaultCatalog": "iceberg",
            "spark.chronon.coalesce.factor": "10",
            "spark.default.parallelism": "10",
            "spark.sql.shuffle.partitions": "10",
        },
        modeConfigs={
            RunMode.BACKFILL: {
                "spark.driver.memory": "512m",
                "spark.executor.memory": "1g",
                "spark.executor.cores": "1",
                "spark.executor.instances": "1",
            },
            RunMode.UPLOAD: {
                "spark.driver.memory": "512m",
                "spark.executor.memory": "512m",
            },
        },
    ),
    env=EnvironmentVariables(
        common={
            "VERSION": "latest",
            "K8S_NAMESPACE": "chronon",
            "SPARK_IMAGE": "chronon-spark-k8s:latest",
            "SPARK_SERVICE_ACCOUNT": "spark",
            "SPARK_EVENT_LOG_ENABLED": "true",
            "SPARK_EVENT_LOG_DIR": "s3a://chronon-spark-logs/event-logs",
            "ARTIFACT_PREFIX": "s3a://warehouse",
            "FLINK_STATE_URI": "s3a://warehouse",
            "WAREHOUSE_PREFIX": "s3a://warehouse",
            "SPARK_HISTORY_SERVER_URL": "http://spark-history-server:18080",
            "HUB_URL": "http://localhost:3903",
            "FRONTEND_URL": "http://localhost:3000",
            "EVAL_URL": "http://localhost:3904",
            "ONLINE_CLASS": "ai.chronon.integrations.cloud_k8s.LocalRedisApiImpl",
        },
        modeEnvironments={
            RunMode.BACKFILL: {},
            RunMode.UPLOAD: {},
            RunMode.STREAMING: {},
        },
    ),
)

azure = Team(
    outputNamespace="data",
    env=EnvironmentVariables(
        common={
            "CLOUD_PROVIDER": "azure",
            "CUSTOMER_ID": "dev",
            "VERSION": "latest",
            "SPARK_CLUSTER_NAME": "http://dev-zipline-kyuubi.westus.cloudapp.azure.com:10099",
            "ARTIFACT_PREFIX": "abfss://dev-zipline-artifacts@ziplineai2.dfs.core.windows.net",
            "WAREHOUSE_PREFIX": "abfss://dev-zipline-warehouse@ziplineai2.dfs.core.windows.net",
            "CHRONON_ONLINE_ARGS": " -Ztasks=4",
            "FRONTEND_URL": "https://dev-azure.zipline.ai",
            "HUB_URL": "https://dev-orch-azure.zipline.ai",
            "CHRONON_ONLINE_ARGS": " -Ztasks=1 -Zbootstrap=zipline-demo-events.servicebus.windows.net:9093",
            "SNOWFLAKE_JDBC_URL": "jdbc:snowflake://VEJLULX-AZURE.snowflakecomputing.com/?user=demo_batch_service&db=Demo&schema=public&warehouse=demo_wh",
            "SNOWFLAKE_PRIVATE_KEY_VAULT_URI": "https://demo-service-writer-pkey.vault.azure.net/secrets/snowflake-private-key",
            "FLINK_SASL_JAAS_CONFIG_VAULT_URI": "https://eventhub-sasl-jaas.vault.azure.net/secrets/sasl-jass-config",
            "OC_CREDENTIAL_VAULT_URI": "https://dev-zipline-secrets.vault.azure.net/secrets/oc-catalog-credential",
            "EVAL_URL": "https://dev-azure.zipline.ai/services/eval",
            "AUTH_SCOPE": "api://dev-zipline-auth",
            "FLINK_JARS_URI": "abfss://dev-zipline-artifacts@ziplineai2.dfs.core.windows.net/spark-3.5.3/libs/",
            "FLINK_STATE_URI": "abfss://dev-zipline-warehouse@ziplineai2.dfs.core.windows.net/flink-state",
        },
    ),
    conf=ConfigProperties(
        common={
            **OpenCatalogConfiguration({
                "spark.sql.catalog.spark_catalog.uri": "https://vejlulx-azure-oc.snowflakecomputing.com/polaris/api/catalog",
                "spark.sql.catalog.spark_catalog.credential": "{OC_CREDENTIAL}",
                "spark.sql.catalog.spark_catalog.warehouse": "demo-v2",
                "spark.sql.catalog.spark_catalog.scope": "PRINCIPAL_ROLE:engine",

            }),
            "spark.chronon.table_write.format": "iceberg",
            "spark.chronon.table.format_provider.class": "ai.chronon.integrations.cloud_azure.AzureFormatProvider",
            "spark.chronon.partition.format": "yyyy-MM-dd",
            "spark.chronon.partition.column": "ds",
            "spark.chronon.coalesce.factor": "10",
            "spark.default.parallelism": "10",
            "spark.sql.shuffle.partitions": "10",
            "spark.driver.memory": "4g",
            "spark.driver.memoryOverhead": "1g",
            "spark.driver.cores": "1",
            "spark.executor.memory": "512m",
            "spark.executor.cores": "1",
            "taskmanager.memory.process.size": "4G",
        },
    ),
)

aws_databricks = Team(
    outputNamespace="data",
    env=EnvironmentVariables(
        common={
            "CLOUD_PROVIDER": "aws",
            "CUSTOMER_ID": "dev",
            "VERSION": "latest",
            "AWS_REGION": "us-west-2",
            "SPARK_CLUSTER_NAME": "zipline-emr-canary",
            "ARTIFACT_PREFIX": "s3://zipline-artifacts-canary",
            # TODO: To move warehouse location to canary instead of dev blocked by databricks setup
            "WAREHOUSE_PREFIX": "s3://zipline-warehouse-dev",
            "FRONTEND_URL": "http://localhost:3000",
            "HUB_URL": "http://localhost:3903",
        },
    ),
    conf=ConfigProperties(
        common={
            # Delta + Iceberg extensions in same session
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension,ai.chronon.spark.extensions.ChrononDeltaFixExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            # DeltaCatalog required by UCSingleCatalog for Delta reads
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            # UC read catalog (Delta via UCSingleCatalog with OAuth auto-refresh)
            "spark.sql.catalog.workspace": "io.unitycatalog.spark.UCSingleCatalog",
            "spark.sql.catalog.workspace.uri": "https://dbc-050d6f00-dcb3.cloud.databricks.com",
            "spark.sql.catalog.workspace.auth.type": "oauth",
            "spark.sql.catalog.workspace.auth.oauth.uri": "https://dbc-050d6f00-dcb3.cloud.databricks.com/oidc/v1/token",
            "spark.sql.catalog.workspace.auth.oauth.clientId": "{DATABRICKS_CLIENT_ID}",
            "spark.sql.catalog.workspace.auth.oauth.clientSecret": "{DATABRICKS_CLIENT_SECRET}",
            # UC 0.4.0 defaults renewCredential.enabled=true which tries to set AwsVendedTokenProvider
            # on Hadoop's S3A — causes classloader issues on EMR. Disable to use EMR's own credentials.
            "spark.sql.catalog.workspace.renewCredential.enabled": "false",

            # UC write catalog (Iceberg REST with OAuth)
            "spark.sql.catalog.workspace_iceberg": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.workspace_iceberg.type": "rest",
            "spark.sql.catalog.workspace_iceberg.uri": "https://dbc-050d6f00-dcb3.cloud.databricks.com/api/2.1/unity-catalog/iceberg-rest",
            "spark.sql.catalog.workspace_iceberg.credential": "{DATABRICKS_CREDENTIAL}",
            "spark.sql.catalog.workspace_iceberg.oauth2-server-uri": "https://dbc-050d6f00-dcb3.cloud.databricks.com/oidc/v1/token",
            "spark.sql.catalog.workspace_iceberg.scope": "all-apis",
            "spark.sql.catalog.workspace_iceberg.warehouse": "workspace",

            # Chronon write config
            "spark.chronon.table_write.format": "iceberg",
            "spark.chronon.partition.column": "ds",
            "spark.chronon.partition.format": "yyyy-MM-dd",

            "spark.sql.warehouse.dir": "s3://zipline-warehouse-dev/data/uc-poc/warehouse/",
            "spark.chronon.coalesce.factor": "10",
            "spark.default.parallelism": "10",
            "spark.sql.shuffle.partitions": "10",
            "spark.driver.memory": "1g",
            "spark.driver.cores": "1",
            "spark.executor.memory": "1g",
            "spark.executor.cores": "1",
        },
    ),
    clusterConf=ClusterConfigProperties(
        common={
            "emr.config": generate_emr_cluster_config(
                instance_count=3,
                subnet_name="zipline-canary-subnet-main",
                security_group_name="zipline-canary-sg",
                instance_type="m5.xlarge",
                idle_timeout=7200,
                release_label="emr-7.12.0"
            )
        }
    ),
)
