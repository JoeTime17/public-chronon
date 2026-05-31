---
title: "Project Setup"
order: 0
---

# Project Setup

Before you can test, deploy, or serve Chronon entities, you need to configure your project's `teams.py` file. This file defines teams, cloud configurations, catalog settings, and execution environments.

## Overview

Every Chronon project has a `teams.py` file at its root. Each top-level variable in this file defines a **team** — a named configuration scope that controls how Chronon jobs run. A team bundles together:

- **Catalog configuration** — how Chronon reads and writes tables (Iceberg via Glue, BigQuery, Open Catalog, etc.)
- **Environment variables** — cloud provider settings, service URLs, storage paths
- **Mode-specific overrides** — different settings for backfill vs. upload vs. streaming

The `default` team provides base settings that all other teams inherit from.

```python
from ai.chronon.repo.spark_catalog_confs import *
from ai.chronon.types import ConfigProperties, EnvironmentVariables, Team

default = Team(
    description="Default team",
    email="ml-infra@company.com",
    outputNamespace="default",
    conf=ConfigProperties(
        common={
            "spark.chronon.partition.column": "ds",
        }
    ),
    env=EnvironmentVariables(
        common={
            "VERSION": "latest",
            "FRONTEND_URL": "http://localhost:3000",
            "HUB_URL": "http://localhost:3903",
        },
    ),
)
```

## Team Configuration

A `Team` has the following fields:

| Field | Type | Description |
|---|---|---|
| `description` | `str` | Human-readable description |
| `email` | `str` | Team contact email |
| `outputNamespace` | `str` | Namespace for output tables (e.g. `"data"`) |
| `conf` | `ConfigProperties` | Spark/Flink job configuration |
| `env` | `EnvironmentVariables` | Environment variables for job execution |

Each of `conf` and `env` support a `common` dict (applies to all modes) and a mode-specific dict for per-mode overrides. Configurations are layered in priority order: `default` team -> team-specific -> entity-specific.

## Catalog Configuration

Chronon writes Iceberg tables by default. The catalog determines _where_ table metadata is stored. Chronon provides helper configurations for common setups.

### AWS Glue Catalog

Uses AWS Glue as the Iceberg metastore. This is the standard setup for AWS deployments. See the [Iceberg AWS Glue documentation](https://iceberg.apache.org/docs/latest/aws/) and [AWS Glue Iceberg guide](https://docs.aws.amazon.com/prescriptive-guidance/latest/apache-iceberg-on-aws/iceberg-glue.html) for more details.

```python
from ai.chronon.repo.spark_catalog_confs import GlueConfiguration

aws_team = Team(
    outputNamespace="data",
    conf=ConfigProperties(
        common={
            **GlueConfiguration({
                "spark.sql.catalog.spark_catalog.warehouse": "s3://your-warehouse-bucket/data/tables/",
            }),
            "spark.chronon.partition.format": "yyyy-MM-dd",
            "spark.chronon.partition.column": "ds",
            "spark.chronon.table_write.format": "iceberg",
        },
    ),
)
```

`GlueConfiguration` sets up `SparkSessionCatalog` with `GlueCatalog` as the backing implementation and configures the necessary Hive metastore and Iceberg extensions.

**Required values:**
- `spark.sql.catalog.spark_catalog.warehouse` — S3 path for the Iceberg warehouse

### GCP BigQuery Metastore Catalog

Uses BigQuery Metastore as the Iceberg catalog for GCP deployments. See the [BigLake Metastore Iceberg REST catalog documentation](https://docs.cloud.google.com/biglake/docs/blms-rest-catalog) and [Dataproc Iceberg with BigQuery Metastore guide](https://cloud.google.com/dataproc/docs/guides/iceberg-metadata-bigquery-metastore) for more details.

```python
from ai.chronon.repo.spark_catalog_confs import BigQueryConfiguration

gcp_team = Team(
    outputNamespace="data",
    conf=ConfigProperties(
        common={
            **BigQueryConfiguration({
                "spark.sql.catalog.spark_catalog.warehouse": "gs://your-warehouse-bucket/data/tables/",
                "spark.sql.catalog.spark_catalog.gcp.bigquery.location": "us-central1",
                "spark.sql.catalog.spark_catalog.gcp.bigquery.project-id": "your-gcp-project",
            }),
            "spark.chronon.partition.format": "yyyy-MM-dd",
            "spark.chronon.partition.column": "ds",
            "spark.chronon.table_write.format": "iceberg",
        },
    ),
)
```

**Required values:**
- `spark.sql.catalog.spark_catalog.warehouse` — GCS path for the Iceberg warehouse
- `spark.sql.catalog.spark_catalog.gcp.bigquery.location` — BigQuery region
- `spark.sql.catalog.spark_catalog.gcp.bigquery.project-id` — GCP project ID

### Open Catalog (Snowflake Polaris / REST)

Uses the Iceberg REST catalog protocol, compatible with Snowflake's Open Catalog (Polaris) and other REST-based catalog implementations. This is commonly used with Azure deployments. See the [Snowflake Open Catalog documentation](https://docs.snowflake.com/en/user-guide/opencatalog/overview) for more details.

```python
from ai.chronon.repo.spark_catalog_confs import OpenCatalogConfiguration

azure_team = Team(
    outputNamespace="data",
    conf=ConfigProperties(
        common={
            **OpenCatalogConfiguration({
                "spark.sql.catalog.spark_catalog.uri": "https://your-account.snowflakecomputing.com/polaris/api/catalog",
                "spark.sql.catalog.spark_catalog.credential": "your-credential",
                "spark.sql.catalog.spark_catalog.warehouse": "your-warehouse",
                "spark.sql.catalog.spark_catalog.scope": "PRINCIPAL_ROLE:engine",
            }),
            "spark.chronon.table_write.format": "iceberg",
            "spark.chronon.partition.format": "yyyy-MM-dd",
            "spark.chronon.partition.column": "ds",
        },
    ),
)
```

**Required values:**
- `spark.sql.catalog.spark_catalog.uri` — REST catalog endpoint
- `spark.sql.catalog.spark_catalog.credential` — Authentication credential
- `spark.sql.catalog.spark_catalog.warehouse` — Warehouse name in the catalog
- `spark.sql.catalog.spark_catalog.scope` — Authorization scope

### Databricks Unity Catalog

For Databricks environments, you can configure a dual-catalog setup: Unity Catalog for reading existing Delta tables, and Iceberg REST for writing Chronon output. See the [Databricks Iceberg REST catalog documentation](https://docs.databricks.com/aws/en/external-access/iceberg) and the [Unity Catalog integrations guide](https://docs.databricks.com/aws/en/external-access/integrations) for more details.

```python
databricks_team = Team(
    outputNamespace="data",
    conf=ConfigProperties(
        common={
            # Delta + Iceberg extensions in same session
            "spark.sql.extensions": (
                "io.delta.sql.DeltaSparkSessionExtension,"
                "ai.chronon.spark.extensions.ChrononDeltaFixExtension,"
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
            ),
            # DeltaCatalog for reading existing Delta tables
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",

            # UC read catalog (Delta via UCSingleCatalog)
            "spark.sql.catalog.workspace": "io.unitycatalog.spark.UCSingleCatalog",
            "spark.sql.catalog.workspace.uri": "https://your-workspace.cloud.databricks.com",
            "spark.sql.catalog.workspace.auth.type": "oauth",
            "spark.sql.catalog.workspace.auth.oauth.uri": "https://your-workspace.cloud.databricks.com/oidc/v1/token",
            "spark.sql.catalog.workspace.auth.oauth.clientId": "{DATABRICKS_CLIENT_ID}",
            "spark.sql.catalog.workspace.auth.oauth.clientSecret": "{DATABRICKS_CLIENT_SECRET}",

            # UC write catalog (Iceberg REST)
            "spark.sql.catalog.workspace_iceberg": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.workspace_iceberg.type": "rest",
            "spark.sql.catalog.workspace_iceberg.uri": "https://your-workspace.cloud.databricks.com/api/2.1/unity-catalog/iceberg-rest",
            "spark.sql.catalog.workspace_iceberg.credential": "{DATABRICKS_CREDENTIAL}",
            "spark.sql.catalog.workspace_iceberg.oauth2-server-uri": "https://your-workspace.cloud.databricks.com/oidc/v1/token",
            "spark.sql.catalog.workspace_iceberg.scope": "all-apis",
            "spark.sql.catalog.workspace_iceberg.warehouse": "workspace",

            "spark.chronon.table_write.format": "iceberg",
            "spark.chronon.partition.column": "ds",
            "spark.chronon.partition.format": "yyyy-MM-dd",
        },
    ),
)
```

This uses two named catalogs:
- `workspace` — reads existing Delta tables via Unity Catalog's `UCSingleCatalog`
- `workspace_iceberg` — writes Iceberg tables via Unity Catalog's Iceberg REST API

## Environment Variables

Use `EnvironmentVariables` to configure cloud-specific settings, service URLs, and storage paths. These are passed to job executors at runtime.

```python
from ai.chronon.repo.constants import RunMode

my_team = Team(
    outputNamespace="data",
    env=EnvironmentVariables(
        common={
            "CLOUD_PROVIDER": "gcp",               # "gcp", "aws", or "azure"
            "CUSTOMER_ID": "my-org",
            "VERSION": "latest",
            "ARTIFACT_PREFIX": "gs://my-artifacts",  # where Chronon JARs and scripts live
            "WAREHOUSE_PREFIX": "gs://my-warehouse", # root path for data storage
            "FRONTEND_URL": "https://zipline.my-org.com",
            "HUB_URL": "https://zipline-hub.my-org.com",
        },
        modeEnvironments={
            RunMode.UPLOAD: {
                "SPARK_CLUSTER_NAME": "upload-cluster",  # use a different cluster for uploads
            },
        },
    ),
)
```

`modeEnvironments` lets you override variables for specific run modes (e.g., `RunMode.BACKFILL`, `RunMode.UPLOAD`, `RunMode.STREAMING`). The full set of modes is defined in `ai.chronon.repo.constants.RunMode`.

## Mode-Specific Overrides

Both `conf` and `env` support per-mode overrides via `modeConfigs` and `modeEnvironments`. This lets you tune resource allocation or behavior for different job types.

```python
from ai.chronon.repo.constants import RunMode

my_team = Team(
    outputNamespace="data",
    conf=ConfigProperties(
        common={
            # ... catalog and common settings ...
            "spark.driver.memory": "1g",
            "spark.executor.memory": "1g",
        },
        modeConfigs={
            RunMode.BACKFILL: {
                "spark.executor.memory": "4g",   # backfills need more memory
                "spark.executor.instances": "10",
            },
            RunMode.UPLOAD: {
                "spark.executor.memory": "2g",
            },
        },
    ),
)
```

## Common Chronon Properties

These Spark properties control Chronon-specific behavior and should be set in `conf.common`:

| Property | Description | Example |
|---|---|---|
| `spark.chronon.partition.column` | Partition column name | `"ds"` |
| `spark.chronon.partition.format` | Partition date format | `"yyyy-MM-dd"` |
| `spark.chronon.table_write.format` | Output table format | `"iceberg"` |
| `spark.chronon.coalesce.factor` | Output coalesce factor | `"10"` |
| `spark.chronon.table.format_provider.class` | Cloud-specific format provider | `"ai.chronon.integrations.cloud_gcp.GcpFormatProvider"` |

## Full Example

Here is a complete example of a `teams.py` with a default team and a AWS team:

```python
from ai.chronon.repo.constants import RunMode
from ai.chronon.repo.spark_catalog_confs import *
from ai.chronon.types import ConfigProperties, EnvironmentVariables, Team

default = Team(
    description="Default team",
    email="ml-infra@company.com",
    outputNamespace="default",
    conf=ConfigProperties(
        common={
            "spark.chronon.partition.column": "ds",
        }
    ),
    env=EnvironmentVariables(
        common={
            "VERSION": "latest",
            "FRONTEND_URL": "https://zipline.company.com",
            "HUB_URL": "https://zipline-hub.company.com",
        },
    ),
)

recommendations = Team(
    outputNamespace="data",
    env=EnvironmentVariables(
        common={
            "CLOUD_PROVIDER": "aws",
            "CUSTOMER_ID": "my-org",
            "VERSION": "latest",
            "AWS_REGION": "us-west-2",
            "SPARK_CLUSTER_NAME": "chronon-emr-cluster",
            "ARTIFACT_PREFIX": "s3://my-org-artifacts",
            "WAREHOUSE_PREFIX": "s3://my-org-warehouse",
            "FLINK_STATE_URI": "s3://my-org-warehouse/flink-state",
        },
        modeEnvironments={
            RunMode.UPLOAD: {
                "SPARK_CLUSTER_NAME": "chronon-transient-upload-cluster",
            }
        }
    ),
    conf=ConfigProperties(
        common={
            **GlueConfiguration({
                "spark.sql.catalog.spark_catalog.warehouse": "s3://my-org-warehouse/data/tables/",
            }),
            "spark.chronon.partition.format": "yyyy-MM-dd",
            "spark.chronon.partition.column": "ds",
            "spark.chronon.table_write.format": "iceberg",
            "spark.chronon.coalesce.factor": "10",
            "spark.default.parallelism": "10",
            "spark.sql.shuffle.partitions": "10",
            "spark.driver.memory": "1g",
            "spark.executor.memory": "1g",
        },
        modeConfigs={
            RunMode.BACKFILL: {
                "spark.executor.memory": "4g",
            }
        }
    ),
)
```

Entities defined under the `recommendations/` directory (e.g. `joins/recommendations/user_features.py`) will use the `recommendations` team's configuration. The team name is determined by the subdirectory name under each entity type.

## Multi-Environment Setup

To run the same configs against a separate canary deployment (smaller cluster, non-prod cloud project, different upstream data, etc.), drop a sibling `teams.canary.py` next to `teams.py`:

```
<repo-root>/
├── teams.py           # → compiles to compiled/         (prod)
├── teams.canary.py    # → compiles to compiled_canary/  (canary, optional)
├── group_bys/
├── joins/
└── staging_queries/
```

`zipline compile` discovers `teams.canary.py` automatically and runs a second compile pass. Only `canary` is supported as a non-prod env today.

See **[Multi-Environment Compile & Deploy](MultiEnvironment.md)** for the full guide — the recommended `from teams import …` authoring pattern, per-entity `environments=[...]` opt-in, the `--env` deploy contract, and the CI workflow.
