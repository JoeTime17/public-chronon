---
title: "Metrics Reference"
order: 5
---

# Metrics Reference

Chronon emits metrics across its online serving and offline batch components using an [OpenTelemetry](https://opentelemetry.io/)-based framework. This page is a reference for all metrics outside the Flink streaming pipeline. For Flink streaming metrics, see [Streaming Metrics](./streaming_metrics.md).

## Framework Overview

### Metric Name Format

Metric names are composed as:

```
[global_prefix.]<environment>[.<suffix>].<metric_name>
```

For example, a GroupBy fetch latency metric is: `group_by.fetch.multi_get.latency.millis`

When exported to **Prometheus**, dots are converted to underscores and histogram metrics get `_bucket`, `_count`, and `_sum` suffixes:

```
group_by_fetch_multi_get_latency_millis_bucket
```

### Metric Types

| Type | OTel Instrument | Description |
|------|----------------|-------------|
| Counter | `LongCounter` | Monotonically increasing count; use `.increment()` or `.count()` |
| Histogram | `LongHistogram` | Distribution of values (latencies, sizes); use `.distribution()` |
| Gauge (long) | `LongGauge` | Point-in-time value (batch job outputs); use `.gauge(long)` |
| Gauge (double) | `DoubleGauge` | Point-in-time decimal value; use `.gauge(double)` |

### Standard Tags

Every metric is automatically tagged with the following dimensions from its `Metrics.Context`:

| Tag | Description |
|-----|-------------|
| `environment` | The subsystem emitting the metric (e.g. `group_by.fetch`, `kv_store`) |
| `group_by` | GroupBy name (when applicable) |
| `join` | Join name (when applicable) |
| `team` | Team owning the entity, from metadata |
| `production` | `"true"` or `"false"`, from metadata |
| `accuracy` | `"TEMPORAL"` or `"SNAPSHOT"` (for GroupBy contexts) |
| `join_part_prefix` | Prefix for a specific join part |
| `staging_query` | StagingQuery name (when applicable) |
| `dataset` | Dataset/table name (KV store contexts) |
| `model` | Model name (model transform contexts) |
| `model_transforms` | ModelTransforms name (model transform contexts) |

Some metrics carry additional per-call tags (e.g. `feature=<name>`, `exception=<class>`).

---

## Fetcher Service Metrics

The Chronon fetcher serves online feature lookups. A Join fetch internally fans out to multiple GroupBy fetches. GroupBy-level metrics fire for each GroupBy resolved within a Join.

### GroupBy Fetch and Join Fetch — Common Metrics

These metrics are emitted under both `group_by.fetch.*` and `join.fetch.*` environments:

| Metric | Type | Description |
|--------|------|-------------|
| `{group_by\|join}.fetch.fetch.count` | Histogram | Total feature values in the response |
| `{group_by\|join}.fetch.fetch.null_count` | Histogram | Feature values that are null in the response |
| `{group_by\|join}.fetch.fetch.exception_count` | Histogram | Feature values that resulted in exceptions |
| `{group_by\|join}.fetch.feature.count` | Counter | Per-feature occurrence count (tag: `feature=<name>`) |
| `{group_by\|join}.fetch.feature.null_count` | Counter | Per-feature null count (tag: `feature=<name>`) |
| `{group_by\|join}.fetch.exception` | Counter | Exceptions during fetch (tag: `exception=<signature>`) |

### GroupBy Fetch Metrics

Emitted under environment `group_by.fetch`:

| Metric | Type | Description |
|--------|------|-------------|
| `group_by.fetch.group_by_request.count` | Counter | Incremented per GroupBy fetch request |
| `group_by.fetch.group_by_serving_info_failure.count` | Counter | Failures loading GroupBy serving info |
| `group_by.fetch.multi_get.batch.size` | Counter | Batch size sent to KV store multi-get |
| `group_by.fetch.multi_get.bytes` | Histogram | Total bytes returned by KV store multi-get |
| `group_by.fetch.multi_get.response.length` | Histogram | Number of values returned by multi-get |
| `group_by.fetch.multi_get.latency.millis` | Histogram | KV store multi-get latency |
| `group_by.fetch.group_by.batchir_decode.latency.millis` | Histogram | Time to decode the batch intermediate representation |
| `group_by.fetch.group_by.all_streamingir_decode.latency.millis` | Histogram | Time to decode all streaming intermediate representations |
| `group_by.fetch.group_by.degraded_counter.count` | Counter | Detections of late/stale batch data that may degrade accuracy |
| `group_by.fetch.group_by.aggregator.latency.millis` | Histogram | Time to run the final aggregation |
| `group_by.fetch.group_by.latency.millis` | Histogram | Total GroupBy fetch latency |
| `group_by.fetch.batch.response.row.count` | Histogram | Row count in the batch KV response |
| `group_by.fetch.batch.response.bytes` | Histogram | Bytes in the batch KV response |
| `group_by.fetch.batch.response.freshness.millis` | Histogram | Age of the most recent batch value (ms) |
| `group_by.fetch.batch.response.freshness.minutes` | Histogram | Age of the most recent batch value (minutes) |
| `group_by.fetch.streaming.response.row.count` | Histogram | Row count in the streaming KV response |
| `group_by.fetch.streaming.response.bytes` | Histogram | Bytes in the streaming KV response |
| `group_by.fetch.streaming.response.freshness.millis` | Histogram | Age of the most recent streaming value (ms) |
| `group_by.fetch.streaming.response.freshness.minutes` | Histogram | Age of the most recent streaming value (minutes) |

### Join Fetch Metrics

Emitted under environment `join.fetch`:

| Metric | Type | Description |
|--------|------|-------------|
| `join.fetch.join_request.count` | Counter | Incremented per Join fetch request |
| `join.fetch.internal.request.count` | Counter | Internal GroupBy request count within the join |
| `join.fetch.internal.latency.millis` | Histogram | Latency of the internal GroupBy fetch fan-out |
| `join.fetch.response.keys.count` | Histogram | Number of response keys returned |
| `join.fetch.overall.latency.millis` | Histogram | End-to-end join fetch latency |
| `join.fetch.derivation_codec.latency.millis` | Histogram | Time to fetch the derivation codec |
| `join.fetch.derivation.latency.millis` | Histogram | Time to compute derived features |
| `join.fetch.request.latency.millis` | Histogram | Full join request latency (including derivations) |
| `join.fetch.logging_request.count` | Counter | Number of logging requests emitted |
| `join.fetch.logging_request.latency.millis` | Histogram | Time to encode the logging payload |
| `join.fetch.logging_request.overall.latency.millis` | Histogram | End-to-end logging latency |
| `join.fetch.response.external_pre_processing.latency` | Histogram | Time for external response pre-processing |
| `join.fetch.response.external_invalid_joins.count` | Counter | Invalid external join responses |
| `join.fetch.external.latency.millis` | Histogram | External fetch latency |

### Schema Fetch Metrics

Emitted under environment `join.schema.fetch`:

| Metric | Type | Description |
|--------|------|-------------|
| `join.schema.fetch.avroconversionbytes.latency.millis` | Histogram | Time to convert schema to Avro bytes |
| `join.schema.fetch.avroconversionstring.latency.millis` | Histogram | Time to convert schema to Avro string |

### Metadata Fetch Metrics

Emitted under environment `metadata.fetch`:

| Metric | Type | Description |
|--------|------|-------------|
| `metadata.fetch.latency.millis` | Histogram | Metadata fetch latency |
| `metadata.fetch.exception` | Counter | Exceptions during metadata fetch |

### Model Transform Metrics

Model transform metrics are split across two environments. `model.predict` covers per-model inference stages; `modeltransforms.fetch` covers the overall transform fetch.

| Metric | Type | Description |
|--------|------|-------------|
| `model.predict.model_preprocess.latency.millis` | Histogram | Input preprocessing time |
| `model.predict.model_inference.latency.millis` | Histogram | Model inference time |
| `model.predict.model_inference.bulk_requests.count` | Counter | Number of unique inputs batched for inference |
| `model.predict.model_postprocess.latency.millis` | Histogram | Output postprocessing time |
| `modeltransforms.fetch.model_transform.overall.latency.millis` | Histogram | End-to-end model transform fetch latency |
| `modeltransforms.fetch.model_transform.feature_count` | Histogram | Feature count returned from model transforms |

---

## Spark Batch Job Metrics

### GroupBy Upload (`group_by.upload`)

Emitted at the end of a GroupBy upload job. All metrics are gauges since they represent terminal job-level measurements.

| Metric | Type | Description |
|--------|------|-------------|
| `group_by.upload.upload.null_count` | Gauge | Null count per field (tags: `field=<name>`, `endDs=<date>`) |
| `group_by.upload.key.bytes` | Gauge | Total key bytes uploaded to the KV store |
| `group_by.upload.value.bytes` | Gauge | Total value bytes uploaded to the KV store |
| `group_by.upload.row.count` | Gauge | Total rows uploaded |
| `group_by.upload.latency.minutes` | Gauge | Job duration in minutes |

---

## KV Store Metrics

All KV store backends emit metrics under environment `kv_store` with a suffix identifying the backend (e.g. `kv_store.dynamodb`). The `dataset` tag identifies the specific table or container being accessed.

### Common Metrics (All Backends)

| Metric | Type | Description |
|--------|------|-------------|
| `<backend>.create.successes` | Counter | Table/container created successfully |
| `<backend>.create.failures` | Counter | Table/container creation failed (tag: `exception=<class>`) |
| `<backend>.multiGet.latency` | Histogram | Multi-get (read) latency |
| `<backend>.multiGet.successes` | Counter | Successful multi-get operations |
| `<backend>.multiGet.<backend>_errors` | Counter | Multi-get errors (tag: `exception=<class>`) |
| `<backend>.multiPut.latency` | Histogram | Multi-put (write) latency |
| `<backend>.multiPut.successes` | Counter | Successful multi-put operations |
| `<backend>.multiPut.failures` | Counter | Failed multi-put operations (tag: `exception=<class>`) |
| `<backend>.list.latency` | Histogram | List/scan operation latency |
| `<backend>.list.successes` | Counter | Successful list operations |
| `<backend>.list.<backend>_errors` | Counter | List errors (tag: `exception=<class>`) |
| `<backend>.bulkPut.latency` | Histogram | Bulk load latency |
| `<backend>.bulkPut.successes` | Counter | Successful bulk loads |
| `<backend>.bulkPut.failures` | Counter | Failed bulk loads (tag: `exception=<class>`) |

Where `<backend>` is one of: `kv_store.dynamodb`, `kv_store.bigtable`, `kv_store.redis`, `kv_store.cosmos`, `kv_store.data_quality_metrics`.

### DynamoDB-Specific Metrics (`kv_store.dynamodb`)

In addition to the common metrics above:

| Metric | Type | Description |
|--------|------|-------------|
| `kv_store.dynamodb.iops_error` | Counter | Provisioned throughput exceeded errors |
| `kv_store.dynamodb.missing_table` | Counter | Table not found errors |
| `kv_store.dynamodb.dynamodb_error` | Counter | Generic DynamoDB errors |

---

## Configuration

Metrics collection is **disabled by default**. Set the following JVM system properties to enable and configure it:

| Property | Default | Description |
|----------|---------|-------------|
| `ai.chronon.metrics.enabled` | `false` | Set to `true` to enable metrics collection |
| `ai.chronon.metrics.reporter` | `otel` | Reporter backend; only `otel` (OpenTelemetry) is supported |
| `ai.chronon.metrics.prefix` | _(none)_ | Optional global prefix prepended to all metric names |
| `ai.chronon.metrics.reader` | `http` | Export mode: `http` (OTLP HTTP), `grpc` (OTLP gRPC), or `prometheus` |
| `ai.chronon.metrics.exporter.url` | `http://localhost:4318` | OTLP endpoint URL (for `http` and `grpc` modes) |
| `ai.chronon.metrics.exporter.port` | `8905` | Port for the Prometheus HTTP server (for `prometheus` mode) |
| `ai.chronon.metrics.exporter.interval` | `PT15s` | Export interval as an ISO-8601 duration |
| `ai.chronon.metrics.exporter.resources` | _(none)_ | Comma-separated `key=value` pairs added as OTel resource attributes |

### Export Modes

- **`http`** — Pushes metrics via OTLP HTTP to an OpenTelemetry Collector or compatible backend (e.g. Datadog, Grafana Cloud)
- **`grpc`** — Pushes metrics via OTLP gRPC
- **`prometheus`** — Starts an embedded Prometheus HTTP server that scrapes can be pulled from

---

## Streaming Metrics

For metrics emitted by the Flink-based streaming pipeline (event ingestion, windowed aggregation, KV store writes), see [Streaming Metrics](./streaming_metrics.md).
