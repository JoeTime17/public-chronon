# Batch Node Runner manual submission requires some credentials for open catalog.
# This is for Azure + Polaris // Snow_OC
set -x
spark-submit \
      --class ai.chronon.spark.batch.BatchNodeRunner \
      --master "local[1]" \
      --driver-memory 1g \
      --executor-memory 1g \
      --conf "spark.driver.extraJavaOptions=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005" \
      --conf spark.app.name=spark-azure_exports_dim_listings__0__staging-local-test \
      --conf spark.chronon.coalesce.factor=10 \
      --conf spark.chronon.partition.column=ds \
      --conf spark.chronon.partition.format=yyyy-MM-dd \
      --conf spark.chronon.table_write.format=iceberg \
      --conf spark.default.parallelism=10 \
      --conf spark.executor.cores=1 \
      --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
      --conf spark.kryo.registrator=ai.chronon.spark.submission.ChrononKryoRegistrator \
      --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkCatalog \
      --conf spark.chronon.table.format_provider.class=ai.chronon.integrations.cloud_azure.AzureFormatProvider \
      --conf spark.sql.catalog.spark_catalog.credential=${OC_CREDENTIAL-client_id:client_secret} \
      --conf spark.sql.catalog.spark_catalog.header.X-Iceberg-Access-Delegation=vended-credentials \
      --conf spark.sql.catalog.spark_catalog.scope=PRINCIPAL_ROLE:${OC_ROLE-all} \
      --conf spark.sql.catalog.spark_catalog.type=rest \
      --conf spark.sql.catalog.spark_catalog.uri=https://${OC_ACCOUNT-account}.snowflakecomputing.com/polaris/api/catalog \
      --conf spark.sql.catalog.spark_catalog.warehouse=${OC_CATALOG-catalog_name} \
      --conf spark.sql.shuffle.partitions=10 \
      --conf spark.zipline.label.branch=main \
      --conf spark.zipline.label.job-type=spark \
      --conf spark.zipline.label.metadata-name=azure.exports.dim_listings__0__staging \
      --conf spark.zipline.label.user=sean@zipline.ai \
      --conf spark.zipline.label.zipline-version=latest \
      --conf spark.zipline.label.zipline_workflow_id=69138b3a-4027-416a-8dcb-b656029e7609 \
      out/cloud_azure/assembly.dest/out.jar \
      --conf-path=azure_exports_dim_listings__0__staging \
	  --start-ds=2026-01-18 \
 	  --end-ds=2026-01-18 \
	  --table-stats-dataset=DATA_QUALITY_METRICS \
	  --online-class=ai.chronon.integrations.cloud_azure.AzureApiImpl \
	  -ZAZURE_STORAGE_ACCOUNT_NAME=$AZ_STORAGE \
	  -ZAZURE_REGION=$AZ_REGION \
	  -ZENABLE_UPLOAD_CLIENTS=true \
      -ZCOSMOS_KEY=$COSMOS_KEY \
      -ZCOSMOS_ENDPOINT=$COSMOS_ENDPOINT \
      -ZCOSMOS_DATABASE=$COSMOS_DATABASE
