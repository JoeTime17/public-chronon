#!/bin/bash
set -e

# Required environment variables
required_vars=("FETCHER_JAR" "FETCHER_PORT")
for var in "${required_vars[@]}"; do
  if [ -z "${!var}" ]; then
    echo "Error: Required environment variable $var is not set"
    exit 1
  fi
done

# Default to GCP if PROVIDER is not set
PROVIDER=${PROVIDER:-GCP}

# Convert to uppercase for case-insensitive comparison
PROVIDER_UPPER=$(echo "$PROVIDER" | tr '[:lower:]' '[:upper:]')

if [[ $PROVIDER_UPPER == "AWS" ]]; then
  ONLINE_JAR=$CLOUD_AWS_JAR
  ONLINE_CLASS=$AWS_ONLINE_CLASS
elif [[ $PROVIDER_UPPER == "GCP" ]]; then
  ONLINE_JAR=$CLOUD_GCP_JAR
  ONLINE_CLASS=$GCP_ONLINE_CLASS
elif [[ $PROVIDER_UPPER == "AZURE" ]]; then
  ONLINE_JAR=$CLOUD_AZURE_JAR
  ONLINE_CLASS=$AZURE_ONLINE_CLASS
else
  echo "Error: Invalid PROVIDER value '$PROVIDER'. Must be 'AWS', 'GCP', or 'AZURE'"
  exit 1
fi

METRICS_OPTS=""

if [ -n "$CHRONON_METRICS_PREFIX" ]; then
  METRICS_OPTS="$METRICS_OPTS -Dai.chronon.metrics.prefix=$CHRONON_METRICS_PREFIX"
fi

if [ -n "$CHRONON_METRICS_READER" ]; then
  METRICS_OPTS="$METRICS_OPTS -Dai.chronon.metrics.enabled=true"
  METRICS_OPTS="$METRICS_OPTS -Dai.chronon.metrics.reader=$CHRONON_METRICS_READER"

  if [ "$CHRONON_METRICS_READER" = "prometheus" ]; then
    if [ -n "$CHRONON_PROMETHEUS_SERVER_PORT" ]; then
      METRICS_OPTS="$METRICS_OPTS -Dai.chronon.metrics.exporter.port=$CHRONON_PROMETHEUS_SERVER_PORT"
    fi
    if [ -n "$VERTX_PROMETHEUS_SERVER_PORT" ]; then
      METRICS_OPTS="$METRICS_OPTS -Dai.chronon.vertx.metrics.exporter.port=$VERTX_PROMETHEUS_SERVER_PORT"
    fi
  else
    # For http and grpc metrics readers
    if [ -n "$EXPORTER_OTLP_ENDPOINT" ]; then
      METRICS_OPTS="$METRICS_OPTS -Dai.chronon.metrics.exporter.url=$EXPORTER_OTLP_ENDPOINT"
    fi
    if [ -n "$CHRONON_METRICS_EXPORTER_INTERVAL" ]; then
      METRICS_OPTS="$METRICS_OPTS -Dai.chronon.metrics.exporter.interval=$CHRONON_METRICS_EXPORTER_INTERVAL"
    fi
  fi
else
  echo "No CHRONON_METRICS_READER configured. Disabling metrics reporting"
  METRICS_OPTS="-Dai.chronon.metrics.enabled=false"
fi

# Configure Google Cloud Profiler if enabled
if [ "$ENABLE_GCLOUD_PROFILER" = true ]; then
  JVM_OPTS="$JVM_OPTS -agentpath:/opt/cprof/profiler_java_agent.so=-cprof_service=chronon-fetcher,-logtostderr,-minloglevel=1,-cprof_enable_heap_sampling"
fi

# Configure garbage collector based on USE_ZGC environment variable
if [ "$USE_ZGC" = true ]; then
  echo "Enabling ZGC garbage collector in generational mode"
  GC_OPTS="-XX:+UseZGC -XX:+ZGenerational"
else
  echo "Using default garbage collector (G1)"
  GC_OPTS=""
fi

ADD_OPENS_OPTS="
--add-opens=java.base/java.lang=ALL-UNNAMED
--add-opens=java.base/java.lang.invoke=ALL-UNNAMED
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED
--add-opens=java.base/java.io=ALL-UNNAMED
--add-opens=java.base/java.net=ALL-UNNAMED
--add-opens=java.base/java.nio=ALL-UNNAMED
--add-opens=java.base/java.util=ALL-UNNAMED
--add-opens=java.base/java.util.concurrent=ALL-UNNAMED
--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED
--add-opens=java.base/sun.nio.cs=ALL-UNNAMED
--add-opens=java.base/sun.security.action=ALL-UNNAMED
--add-opens=java.base/sun.util.calendar=ALL-UNNAMED
--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED
"

JVM_OPTS="$JVM_OPTS $GC_OPTS $ADD_OPENS_OPTS -XX:MaxMetaspaceSize=1g -XX:MaxRAMPercentage=70.0 -XX:MinRAMPercentage=70.0 -XX:InitialRAMPercentage=70.0 -XX:MaxHeapFreeRatio=100 -XX:MinHeapFreeRatio=0"

# TTL cache configuration
TTL_OPTS=""
if [ -n "$CHRONON_JOIN_CONF_TTL_MILLIS" ]; then
  TTL_OPTS="$TTL_OPTS -Dai.chronon.join.conf.ttl.millis=$CHRONON_JOIN_CONF_TTL_MILLIS"
fi
if [ -n "$CHRONON_JOIN_CODEC_TTL_MILLIS" ]; then
  TTL_OPTS="$TTL_OPTS -Dai.chronon.join.codec.ttl.millis=$CHRONON_JOIN_CODEC_TTL_MILLIS"
fi

echo "Starting Fetcher service with online jar $ONLINE_JAR and online class $ONLINE_CLASS"
if ! java $JVM_OPTS $METRICS_OPTS -cp $FETCHER_JAR:$ONLINE_JAR ai.chronon.service.ChrononServiceLauncher \
  run ai.chronon.service.FetcherVerticle \
  -Dserver.port=$FETCHER_PORT \
  -Donline.jar=$ONLINE_JAR \
  -Donline.api.props='{"kv.tablePrefix":"'"$KV_TABLE_PREFIX"'"}' \
  $TTL_OPTS \
  -Donline.class=$ONLINE_CLASS; then
  echo "Error: Fetcher service failed to start"
  exit 1
fi