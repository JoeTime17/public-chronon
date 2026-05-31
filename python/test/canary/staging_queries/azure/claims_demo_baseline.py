from ai.chronon.types import ConfigProperties, EngineType, EnvironmentVariables, StagingQuery, TableDependency


CLAIMS_DEMO_ENV = EnvironmentVariables(
    common={
        "CLOUD_PROVIDER": "azure",
        "CUSTOMER_ID": "claims-demo",
        "VERSION": "latest",
        "SPARK_CLUSTER_NAME": "http://crucible.crucible-system.svc.cluster.local:8080",
        "K8S_NAMESPACE": "claims-demo-hub",
        "SPARK_IMAGE": "ziplinecanary.azurecr.io/chronon-spark-azure:latest",
        "SPARK_SERVICE_ACCOUNT": "spark",
        "SPARK_EVENT_LOG_ENABLED": "true",
        "SPARK_EVENT_LOG_DIR": "abfss://crucible@ziplineai2.dfs.core.windows.net/spark-events",
        "SPARK_HISTORY_SERVER_URL": "http://spark-history-server.crucible-system.svc.cluster.local:18080",
        "ARTIFACT_PREFIX": "abfss://crucible@ziplineai2.dfs.core.windows.net/claims-demo/artifacts",
        "WAREHOUSE_PREFIX": "abfss://crucible@ziplineai2.dfs.core.windows.net/claims-demo/warehouse",
        "FLINK_STATE_URI": "abfss://crucible@ziplineai2.dfs.core.windows.net/claims-demo/flink-state",
        "FRONTEND_URL": "http://hub.claims-demo-hub.svc.cluster.local:3903",
        "HUB_URL": "http://hub.claims-demo-hub.svc.cluster.local:3903",
        "EVAL_URL": "http://hub.claims-demo-hub.svc.cluster.local:3903",
        "SNOWFLAKE_JDBC_URL": "jdbc:snowflake://VEJLULX-AZURE.snowflakecomputing.com/?user=demo_batch_service&db=Demo&schema=CLAIMS_DEMO&warehouse=demo_wh",
        "SNOWFLAKE_PRIVATE_KEY_VAULT_URI": "https://demo-service-writer-pkey.vault.azure.net/secrets/snowflake-private-key",
        "POSTGRES_HOST": "crucible-claims-pg-westus.postgres.database.azure.com",
        "POSTGRES_PASSWORD_VAULT_URI": "https://crucible-azure-kv.vault.azure.net/secrets/postgres-password",
        "OC_CREDENTIAL_VAULT_URI": "",
    }
)

CLAIMS_DEMO_CONF = ConfigProperties(
    common={
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
        "spark.driver.extraJavaOptions": "-Dlog4j.configurationFile=/opt/chronon/log4j2.properties",
        "spark.executor.memory": "512m",
        "spark.executor.cores": "1",
        "spark.executor.extraJavaOptions": "-Dlog4j.configurationFile=/opt/chronon/log4j2.properties",
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.defaultCatalog": "spark_catalog",
        "spark.sql.catalog.spark_catalog": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.spark_catalog.type": "jdbc",
        "spark.sql.catalog.spark_catalog.uri": "jdbc:postgresql://{POSTGRES_HOST}:5432/iceberg_catalog?sslmode=require",
        "spark.sql.catalog.spark_catalog.jdbc.user": "chronon",
        "spark.sql.catalog.spark_catalog.jdbc.password": "{POSTGRES_PASSWORD}",
        "spark.sql.catalog.spark_catalog.warehouse": "abfss://crucible@ziplineai2.dfs.core.windows.net/claims-demo/warehouse",
        "spark.sql.catalog.spark_catalog.credential": "",
        "spark.sql.catalog.spark_catalog.header.X-Iceberg-Access-Delegation": "",
        "spark.sql.catalog.spark_catalog.scope": "",
    }
)


baseline_source = StagingQuery(
    query="""
    SELECT
        * EXCLUDE (DS),
        DATE_TRUNC('DAY', DS)::DATE AS ds
    FROM DEMO.CLAIMS_DEMO.CLAIMS_DEMO_BASELINE_SOURCE
    WHERE DATE_TRUNC('DAY', DS)::DATE BETWEEN {{ start_date }} AND {{ end_date }}
    """,
    output_namespace="claims_demo",
    engine_type=EngineType.SNOWFLAKE,
    conf=CLAIMS_DEMO_CONF,
    env_vars=CLAIMS_DEMO_ENV,
    dependencies=[
        TableDependency(
            table="DEMO.CLAIMS_DEMO.CLAIMS_DEMO_BASELINE_SOURCE",
            partition_column="DS",
            start_offset=0,
            end_offset=0,
        )
    ],
    version=0,
    step_days=30,
)
