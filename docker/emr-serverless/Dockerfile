# https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/using-custom-images.html

# Pinned by digest (not :latest tag) so rebuilds are reproducible — the public
# ECR :latest tag is mutable and AWS updates it on new EMR releases. To bump:
#   curl -sI -H "Authorization: Bearer $(curl -s 'https://public.ecr.aws/token/?scope=repository:emr-serverless/spark/emr-<release>:pull' | jq -r .token)" \
#     -H 'Accept: application/vnd.docker.distribution.manifest.list.v2+json' \
#     https://public.ecr.aws/v2/emr-serverless/spark/emr-<release>/manifests/latest \
#     | grep -i docker-content-digest
ARG EMR_BASE_IMAGE=public.ecr.aws/emr-serverless/spark/emr-7.12.0@sha256:7e8008524607d800884d2d6b5f71073b3f9189fd04fbc124b420654c5eb8d39c
FROM ${EMR_BASE_IMAGE}

LABEL io.zipline.delta-spark.source="https://github.com/zipline-ai/delta/tree/v3.3.2-spark-3.5.3"

ARG DELTA_SPARK_JAR=delta-spark_2.12-3.3.2.jar
ARG DELTA_LIB_DIR=/usr/share/aws/delta/lib

USER root

COPY ${DELTA_SPARK_JAR} /tmp/delta-spark-custom.jar

# Replace only delta-spark; delta-storage and other base jars stay as-is.
# spark-defaults.conf classpath references the stable symlink (delta-spark.jar),
# so we must repoint it after swapping the underlying versioned jar.
RUN set -eux; \
    rm -f ${DELTA_LIB_DIR}/delta-spark_2.12-*.jar; \
    mv /tmp/delta-spark-custom.jar ${DELTA_LIB_DIR}/${DELTA_SPARK_JAR}; \
    ln -sfn ${DELTA_SPARK_JAR} ${DELTA_LIB_DIR}/delta-spark.jar; \
    chown hadoop:hadoop ${DELTA_LIB_DIR}/${DELTA_SPARK_JAR}; \
    chown -h hadoop:hadoop ${DELTA_LIB_DIR}/delta-spark.jar; \
    chmod 0644 ${DELTA_LIB_DIR}/${DELTA_SPARK_JAR}

USER hadoop:hadoop
