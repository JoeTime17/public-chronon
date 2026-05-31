"""Canary-env teams definitions for `python/test/canary/`. Loaded by the compile
pipeline (alongside `teams.py`) to produce `compiled_canary/`. Strict isolation:
every team referenced by any config in this tree must appear here — there's no
fallback to `teams.py`."""

from ai.chronon.repo.cluster import generate_dataproc_cluster_config, generate_emr_cluster_config
from ai.chronon.repo.constants import RunMode
from ai.chronon.repo.spark_catalog_confs import *
from ai.chronon.types import ClusterConfigProperties, ConfigProperties, EnvironmentVariables, Team

default = Team(
    description="Default team (canary)",
    email="ml-infra@<customer>.com",
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
        },
    ),
)

# Defined for inheritance-from-default coverage: a team that's present in
# teams.canary.py but has no env/conf of its own must still pick up `default`'s
# fields during the canary compile.
test = Team(
    outputNamespace="test",
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
            # the canary compile pass uses only this `teams.canary.py` file
            # (never `teams.py`). Must not appear in any file under compiled/.
            "CANARY_ONLY_SENTINEL_GCP": "canary-only-sentinel-value-1d2e3f",
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

            "spark.driver.extraJavaOptions": " ".join([
                "-Dai.chronon.metrics.enabled=true",
                "-Dai.chronon.metrics.reader=grpc",
                "-Dai.chronon.metrics.exporter.url=http://localhost:4317",
            ]),
            # Sentinel — canary-only conf marker for the test.
            "spark.chronon.test.canary_only_sentinel": "canary-only-conf-sentinel-7c8d9e",
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
                # Sentinel — canary-only cluster-conf marker for the test.
                "canary_only_sentinel_cluster": "canary-only-cluster-sentinel-5b6c7d",
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


# Show that teams can be imported from other teams.py files
from teams import aws_databricks, azure, quickstart, aws

# only affects the files in compiled_canary/ to verify that the canary compile uses this teams.canary.py file and not teams.py
aws_databricks_env: EnvironmentVariables = aws_databricks.env
aws_databricks_env.common['DATABRICKS_EXTRA'] = "DATABRICKS_EXTRA_1"

aws.env.common['ARTIFACT_PREFIX'] = "s3://zipline-artifacts-canary-aws"