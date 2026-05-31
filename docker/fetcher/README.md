# Chronon Feature Fetching Service

Docker image that allows you to run the Chronon Feature Service.

## Launching the Service

To fetch the latest image, run:
```bash
docker pull ziplineai/chronon-fetcher:latest
```

To run the service, you can use the following command:
```bash
docker run \
 -p 9000:9000 \
 ziplineai/chronon-fetcher:latest
```

### Additional Configuration & Environment Variables

There's some additional environment variables that the Docker container can be started up with (using the `-e "VAR=VALUE"` syntax)

| Environment Variable              | Description                                                                                                                                                                                                                                                                                                                   | Default Value           |
|-----------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------|
| **AWS Configuration**
| KV_TABLE_PREFIX                   | Prefix to prepend to DynamoDB table names when fetching features. Allows the service to fetch from prefixed DynamoDB tables (e.g., if set to "dev_", table "my_table" becomes "dev_my_table")                                                                                                                                | ``                      |
| **Google Cloud Configuration**
| GCP_PROJECT_ID                    | GCloud Zipline BigTable project                                                                                                                                                                                                                                                                                               | ``                      |
| GOOGLE_CLOUD_PROJECT              | GCloud Zipline BigTable project                                                                                                                                                                                                                                                                                               | ``                      |
| GCP_BIGTABLE_INSTANCE_ID          | GCloud Zipline BigTable instance id                                                                                                                                                                                                                                                                                           | ``                      |
| GOOGLE_APPLICATION_CREDENTIALS    | Path to [GCloud application credentials json file](https://cloud.google.com/docs/authentication/application-default-credentials#GAC)                                                                                                                                                                                          | ``                      |
| GCP_BIGTABLE_INSTANCE_ID          | GCloud Zipline BigTable instance id                                                                                                                                                                                                                                                                                           | ``                      |
| **Profiler Configuration**        
| ENABLE_GCLOUD_PROFILER            | If set to 'true', enables profiling service using [cloud profiler](https://cloud.google.com/profiler/docs/about-profiler)                                                                                                                                                                                                     | `false`                 |
| **Service Metrics Configuration** 
| CHRONON_METRICS_PREFIX            | Prefix to add to all Chronon metrics                                                                                                                                                                                                                                                                                          | ``                      |
| CHRONON_METRICS_READER            | Either of 'http', 'prometheus' or 'grpc' (**Note**: grpc will result in only Chronon fetcher library metrics and no Vert.x HTTP metrics being reported)                                                                                                                                                                       | ``                      |
| EXPORTER_OTLP_ENDPOINT            | If using the 'http' or 'grpc' metrics reader, exports metrics to the configured endpoint using [OTLP over http](https://github.com/open-telemetry/opentelemetry-collector/tree/main/exporter/otlphttpexporter) or [OTLP over grpc](https://github.com/open-telemetry/opentelemetry-collector/tree/main/exporter/otlpexporter) | `http://localhost:4318` |
| CHRONON_METRICS_EXPORTER_INTERVAL | If using the 'http' or 'grpc' metrics reader, configures the interval between metric exports using ISO-8601 duration format (e.g., PT30S for 30 seconds)                                                                                                                                                                      | `PT15S`                 |
| CHRONON_PROMETHEUS_SERVER_PORT    | If using the 'prometheus' metrics reader, exposes a [Prometheus service endpoint](https://opentelemetry.io/docs/specs/otel/metrics/sdk_exporters/prometheus/) on the configured port to export Chronon library metrics                                                                                                        | `8905`                  |
| VERTX_PROMETHEUS_SERVER_PORT      | If using the 'prometheus' metrics reader, exposes a [Prometheus service endpoint](https://opentelemetry.io/docs/specs/otel/metrics/sdk_exporters/prometheus/) on the configured port to export Vert.x webservice metrics                                                                                                      | `8906`                  |
| **Monitoring**                    
| FETCHER_OOC_TOPIC_INFO            | Topic string in the format of: `kafka://my-topic-name/key1=value1/key2=value2`. If configured, the service emits [online offline consistency feature logging](https://chronon.ai/test_deploy_serve/Online_Offline_Consistency.html) data to the given topic                                                                   | ``                      |
| **JVM Configuration**             
| JVM_OPTS                          | Additional JVM options to pass to the service | ``                      |
| USE_ZGC                           | Use the [Z Garbage Collector](https://docs.oracle.com/en/java/javase/21/gctuning/z-garbage-collector.html) | `false`|

## Exporting Metrics

The Chronon library and [Vert.x](https://vertx.io/) fetcher web service can be collect metrics using the [OpenTelemetry](https://opentelemetry.io/) standard. 
Currently, the service supports two ways of exporting metrics:
1. **OTLP over HTTP**: Metrics can be exported (push) to an OTLP endpoint using the `EXPORTER_OTLP_ENDPOINT` environment variable.
2. **Prometheus**: Metrics can be exposed on a Prometheus endpoint (pull) using the `CHRONON_PROMETHEUS_SERVER_PORT` and `VERTX_PROMETHEUS_SERVER_PORT` environment variables.

## Service profiling
The service can be profiled using the [Google Cloud Profiler](https://cloud.google.com/profiler/docs/about-profiler) by setting the `ENABLE_GCLOUD_PROFILER` environment variable to `true`. 
The profiles are exported to the cloud profiler under the service name 'chronon-fetcher'. 

## Service Endpoints

The service exposes the following HTTP endpoints:

|Verb                 |Endpoint                       |Description                  |
|---------------------|-------------------------------|-----------------------------|
|GET |`/ping` | Health check endpoint            |
|GET |`/config` | Fetcher service configuration            |
|GET |`/v1/joins` | List of Chronon joins marked online            |
|GET|`/v1/join/:name/schema` | Returns a join schema payload |
|GET|`/v1/groupby/:name/schema` | Returns an online GroupBy schema payload |
|GET|`/v1/groupby/:name/status` | Returns online GroupBy upload status |
|POST|`/v1/fetch/groupby/:name`| Retrieve features for a specific GroupBy
|POST|`/v1/fetch/join/:name`| Retrieve features for a specific Join
|POST|`/v2/fetch/join/:name`| Retrieve features for a specific Join with the features response as an Avro binary string (base64 encoded)

### Example Usage

Some example curl calls to the service are shown below. The fetch join and GroupBy endpoints support bulk lookups, so you can pass a list of entity keys to fetch features for multiple entities in a single request.
```bash
$ curl http://localhost:9000/v1/joins                                                            
{"joinNames":["search.ranking_listing","search.recommendation_v1"]}

$ curl http://localhost:9000/v1/join/search.ranking_listing/schema | jq
{
  "joinName": "search.ranking_listing",
  "keySchema": "...",
  "valueSchema": "...",
  "schemaHash": "c72b6d",
  ...
}

$ curl -X POST http://localhost:9000/v1/fetch/join/ranking_listing -H 'Content-Type: application/json' -d '[{"listing_id":"12345"}]' | jq
{
  "results": [
    {
      "status": "Success",
      "entityKeys": {
        "listing_id": "12345"
      },
      "features": {
        "my_feature_1": 1,
        "my_feature_2": 0.5
        ...
      }
    }
  ]
}
```

#### V2 fetch join with Avro string response
Get the Avro schema from `valueSchema`
```bash
curl -X POST 'http://localhost:9000/v1/join/gcp.demo.v1__1/schema'  -H 'Content-Type: application/json'
{
  "joinName": "gcp.demo.v1__1",
  "keySchema": "...",
  "valueSchema": "...",
  "schemaHash": "...",
  ...
}
```

Query the v2 fetch join.
```bash
$ curl -X POST 'http://localhost:9000/v2/fetch/join/gcp.demo.v1__1' -H 'Content-Type: application/json' \
 -d '[{"listing_id":"1","user_id":"user_7"}]'
{
  "status": "Success",
  "entityKeys": {
    "listing_id": "1",
    "user_id": "user_7"
  },
  "featureAvroString": "AgICCHZpZXcCFgK85YW2sGYAAgICAAIAAgAAAAAAAAAAAgICCHZpZXcCFgK85YW2sGYAAgAAAAAAAAAAAgACigYCAAAAAAAAAAACAAIQY2xvdGhpbmcCAAIuL2ltYWdlcy90b3lzLzEvbWFpbi5qcGcCQEJ1aWxkaW5nIEtpdCAtIEhpZ2ggUXVhbGl0eSBUb3lzAgAAAAAAAAAAAgAAAAAAAAAAAgAAAAAAAAAAAi4vaW1hZ2VzL3RveXMvMS9zaWRlLmpwZwIAAAAAAAAAAAIAAAAAAADwPwIIdG95cwIAAAAAAAAAAAICAgACAAIAAAAAAAAAAAIAAgAAAAAAAAAAAip0cmVuZGluZyxoYW5kbWFkZSxuZXcCAAIOAgECAAAAAAAA8D8CAAIAAgAAAAAAAAAAAgICAAIAAAAAAADwPwICAgACAAAAAAAAAAACAAIAAgZFVVICAAIAAgICAAIAAgAAAAAAAAAAAgAAAAAAAAAAApoEVGhpcyBidWlsZGluZyBraXQgaXMgY3JhZnRlZCB3aXRoIGF0dGVudGlvbiB0byBkZXRhaWwgYW5kIGRlc2lnbmVkIGZvciBkdXJhYmlsaXR5LiBGZWF0dXJlcyBwcmVtaXVtIG1hdGVyaWFscyBhbmQgZXhjZWxsZW50IGNyYWZ0c21hbnNoaXAuIFBlcmZlY3QgZm9yIGJvdGggcGVyc29uYWwgdXNlIGFuZCBhcyBhIGdpZnQuIENvbWVzIHdpdGggbWFudWZhY3R1cmVyIHdhcnJhbnR5IGFuZCBzYXRpc2ZhY3Rpb24gZ3VhcmFudGVlLiBGYXN0IHNoaXBwaW5nIGF2YWlsYWJsZS4CAAICAgICCHZpZXcCFgK85YW2sGYAAgACiAFQcmVtaXVtIGJ1aWxkaW5nIGtpdCBwZXJmZWN0IGZvciBkYWlseSB1c2UuIEdyZWF0IHF1YWxpdHkgYW5kIHZhbHVlLgIAAAAAAADwPwIAAAAAAAAAAAICAgh2aWV3AhYCvOWFtrBmAALOXAICAqAFAgICAAICAgACAAAAAAAAAAACAAIA",
  "featuresErrors": {
    "listing_id_exception": "java.lang.RuntimeException: Couldn't fetch group by serving info for GCP_DIM_LISTINGS_V1__0_BATCH, please make sure a batch upload was successful"
  }
}
```

And then b64 decode the `featureAvroString` and use the Avro schema from `valueSchema` to decode the binary Avro data.

Full example at: [scripts/fetcher_tests/avro_encoded_string_test.py](../../scripts/fetcher_tests/avro_encoded_string_test.py)

### Local Telemetry Docker Setup

For local development with full observability stack, use the included Docker Compose configuration that provides:

* **OTEL Collector**: Receives and transforms Chronon metrics
* **Prometheus**: Time-series database for metrics storage and querying

#### Setup Steps

1. **Configure environment** (copy and edit with your values):
   ```shell
   cp .env.example .env
   # Edit .env with your GCP project details
   ```

2. **Build image** (optional - skip to use latest):
   ```shell
   ./scripts/distribution/publish_docker_images.sh --local
   ```

3. **Start services**:
   ```shell
   docker-compose -f chronon-service-with-local-telemetry.yml up -d
   ```

#### Viewing Metrics

After making API calls per [Example Usage](#example-usage):

```bash
# View raw metrics from OTEL Collector
curl http://localhost:9464/metrics

# List available metrics in Prometheus
curl -s "http://localhost:9091/api/v1/label/__name__/values" | jq '.data[]'

# Query metrics in Prometheus UI
open http://localhost:9091
```
