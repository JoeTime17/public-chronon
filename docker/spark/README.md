# Spark on Kubernetes Docker Images

This directory contains Dockerfiles for building Spark images to run Chronon on Kubernetes across different cloud providers.

## Overview

These Docker images extend the official Apache Spark images with cloud-specific dependencies and the Chronon application JAR, enabling Spark workloads to run on managed Kubernetes services like Azure AKS, GCP GKE, and AWS EKS.

## Available Images

### Azure (azure.Dockerfile)

Based on `apache/spark:3.5.3-java17`, this image includes:
- **Hadoop Azure libraries** (3.4.2) for Azure Data Lake Storage Gen2 (ADLS) support
- **Azure Cosmos Spark connector** (4.42.0) for Cosmos DB integration
- **PostgreSQL JDBC driver** (42.7.3) for Iceberg JDBC catalogs
- Cloud-specific JARs placed in `/opt/spark/jars` for automatic classpath inclusion
- A Log4j2 policy at `/opt/chronon/log4j2.properties` and `/opt/spark/conf/log4j2.properties` that keeps Spark internals at WARN during startup

**Key Dependencies:**
- `hadoop-azure-3.4.2.jar` - Azure storage filesystem support
- `hadoop-common-3.4.2.jar` - Core Hadoop utilities
- `azure-cosmos-spark_3-5_2-12-4.42.0.jar` - Cosmos DB connector
- `postgresql-42.7.3.jar` - JDBC driver for Iceberg JDBC catalogs

## Building Images

### Azure
```bash
cd docker/spark
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -f azure.Dockerfile \
  -t chronon-spark-azure:latest \
  .
```

### Pushing to Registry
```bash
# Tag for your container registry
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -f azure.Dockerfile \
  -t <your-registry>/chronon-spark-azure:<version> \
  --push \
  .
```

## Using the Images

### On Azure Kubernetes Service (AKS)

1. Push the image to Azure Container Registry:
```bash
az acr login --name <your-acr-name>
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -f azure.Dockerfile \
  -t <your-acr-name>.azurecr.io/chronon-spark:latest \
  --push \
  .
```

2. Configure your Spark application to use the image:
```yaml
spec:
  image: <your-acr-name>.azurecr.io/chronon-spark:latest
  sparkConf:
    "spark.kubernetes.container.image.pullPolicy": "Always"
```

3. Ensure your AKS cluster has appropriate service account permissions to access ADLS and other Azure resources.

## Application JAR

The `out.jar` file contains the compiled Chronon application code. To update it:

1. Build the Chronon project:
```bash
./mill cloud_azure.assembly  # or appropriate module for your cloud
```

2. Copy the assembled JAR:
```bash
cp out/cloud_azure/assembly.dest/out.jar docker/spark/out.jar
```

3. Rebuild the Docker image with the updated JAR.

## Adding New Cloud Providers

To add support for additional cloud providers (GCP, AWS, etc.):

1. Create a new Dockerfile: `<cloud>.Dockerfile`
2. Base it on `apache/spark:3.5.3-java17`
3. Add cloud-specific connector JARs:
   - **GCP**: `gcs-connector`, `bigquery-connector`
   - **AWS**: `hadoop-aws`, `aws-java-sdk-bundle`
4. Follow the same pattern as `azure.Dockerfile`
5. Update this README with build and usage instructions

## Version Alignment

When updating dependencies, ensure version compatibility:
- Spark version must match the base image
- Hadoop version should align with cloud provider SDK requirements
- Scala version (2.12) must match Spark's Scala version
- Keep dependency versions in sync with those in the Chronon build configuration

## Troubleshooting

### ClassNotFoundException for cloud connectors
- Verify JARs are in `/opt/spark/jars` directory
- Check JAR versions match runtime Spark/Scala versions

### Authentication Issues
- Ensure Kubernetes service accounts have appropriate cloud IAM roles
- For Azure: verify managed identity or service principal configuration
- For GCP: check workload identity setup
- For AWS: verify IRSA (IAM Roles for Service Accounts)

### Snowflake JDBC logs written to $HOME
The Snowflake JDBC driver writes logs to `$HOME/snowflake_jdbc.log` by default. The `spark` user in these images does not have a writable home directory, which causes noisy warnings or failures.

A Snowflake client config file is written to `/etc/sf_client_config.json` in the image and `SF_CLIENT_CONFIG_FILE` is set to point to it, redirecting logs to `/tmp`.

### Spark SQL generated-code logs
Spark initializes Log4j2 before Chronon code can call `SparkContext.setLogLevel`. The Azure image ships a Log4j2 config that sets `org.apache.spark` to WARN so Catalyst generated Java, Jetty, and Hadoop DEBUG logs do not flood driver logs before Chronon starts. `JAVA_TOOL_OPTIONS` points the driver and executor JVMs at this file with `-Dlog4j.configurationFile=...` because Spark-on-Kubernetes creates those JVMs before Spark can honor in-app logging changes.

### Out of Memory Errors
- Adjust Spark driver/executor memory settings in your Spark configuration
- Consider the overhead of cloud connector libraries when sizing pods

## References

- [Apache Spark on Kubernetes](https://spark.apache.org/docs/latest/running-on-kubernetes.html)
- [Azure Cosmos Spark Connector](https://github.com/Azure/azure-sdk-for-java/tree/main/sdk/cosmos/azure-cosmos-spark_3_2-12)
- [Hadoop Azure ADLS Documentation](https://hadoop.apache.org/docs/stable/hadoop-azure/index.html)
