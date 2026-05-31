#!/usr/bin/env bash
# Launches a local spark-shell connected to the Azure Iceberg warehouse (Polaris/Snowflake Open Catalog).
# Requires: az CLI logged in, CHRONON_REPO set (defaults to git root).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="${CHRONON_REPO:-$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel)}"
JAR="${CHRONON_SPARK_JAR:-$REPO_ROOT/out/cloud_azure/assembly.dest/out.jar}"

if [ ! -f "$JAR" ]; then
  echo "Assembly JAR not found at $JAR — run: ./mill cloud_azure.assembly"
  exit 1
fi

echo "Fetching OC_CREDENTIAL from Azure Key Vault..."
OC_CREDENTIAL="$(az keyvault secret show \
  --vault-name dev-zipline-secrets \
  --name oc-catalog-credential \
  --query value -o tsv)"

export OC_CREDENTIAL

spark-shell \
  --master "local[*]" \
  --jars "$JAR" \
  --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkCatalog" \
  --conf "spark.sql.catalog.spark_catalog.type=rest" \
  --conf "spark.sql.catalog.spark_catalog.uri=https://vejlulx-azure-oc.snowflakecomputing.com/polaris/api/catalog" \
  --conf "spark.sql.catalog.spark_catalog.credential=$OC_CREDENTIAL" \
  --conf "spark.sql.catalog.spark_catalog.warehouse=demo-v2" \
  --conf "spark.sql.catalog.spark_catalog.scope=PRINCIPAL_ROLE:engine" \
  --conf "spark.sql.catalog.spark_catalog.header.X-Iceberg-Access-Delegation=vended-credentials" \
  --conf "spark.sql.defaultCatalog=spark_catalog" \
  --conf "spark.chronon.table_write.format=iceberg" \
  --conf "spark.chronon.table.format_provider.class=ai.chronon.integrations.cloud_azure.AzureFormatProvider" \
  --conf "spark.chronon.partition.format=yyyy-MM-dd" \
  --conf "spark.chronon.partition.column=ds" \
  --conf "spark.chronon.coalesce.factor=10" \
  --conf "spark.default.parallelism=10" \
  --conf "spark.sql.shuffle.partitions=10" \
  --conf "spark.driver.memory=1g"
