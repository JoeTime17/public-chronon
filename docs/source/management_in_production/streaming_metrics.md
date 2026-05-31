---
title: "Streaming Metrics"
order: 4
---

# Streaming Metrics

Chronon's streaming pipeline provides comprehensive metrics for monitoring real-time feature computation performance, health, and data quality. The streaming infrastructure is built on Apache Flink and emits detailed metrics at every stage of the pipeline to enable proactive monitoring and troubleshooting.

## Overview

Chronon's streaming pipeline processes real-time events through a series of stages to compute and persist features:

1. **Event Ingestion** - Read events from Kafka/PubSub topics
2. **Event Processing** - Apply Spark SQL expressions for filtering/projection  
3. **Windowing & Aggregation** - Compute [tiled aggregations](../Tiled_Architecture.md) over event windows
4. **Serialization** - Serialize tiled features to Avro
5. **KV Store Writing** - Asynchronously persist features to key-value store

Each stage emits detailed performance and health metrics to provide end-to-end observability of the streaming feature computation pipeline.

## Metrics Categories

### 1. Feature Freshness Metrics

These metrics track the time it takes for events to flow through the entire pipeline.

**Metric: `event_created_to_sink_time`**
- **Type**: Histogram
- **Description**: Time from when an event was originally created to when it's successfully written to the KV store
- **Tags**: `groupby`, `team`, `production`, `environment`
- **Use Case**: Monitor end-to-end feature freshness and detect pipeline bottlenecks

**Metric: `flink_processing_time`**
- **Type**: Histogram  
- **Description**: Time from when an event is received by the Flink application to when it's successfully written to the KV store
- **Tags**: `groupby`, `team`, `production`, `environment`
- **Use Case**: Identify processing bottlenecks within the Flink job itself

### 2. Event Processing Metrics

Track the reliability of event processing operations.

**Metric: `deserialization_errors`**
- **Type**: Counter
- **Description**: Errors deserializing incoming events
- **Tags**: `group_by`, `team`, `production`, `environment`
- **Use Case**: Monitor overall pipeline health - track dropped events due to deserialization issues

**Metric: `sql_exec_errors`**
- **Type**: Counter
- **Description**: Errors performing Spark SQL eval on incoming events
- **Tags**: `feature_group`, `team`, `production`, `environment`
- **Use Case**: Monitor overall pipeline health - track dropped events due to SQL execution issues

**Metric: `event_processing_error`**
- **Type**: Counter
- **Description**: General event processing errors across all operators
- **Tags**: `feature_group`, `team`, `production`, `environment`
- **Use Case**: Monitor overall pipeline health - track dropped events due to processing issues

### 3. Operator Performance Metrics

Monitor the performance of various Flink operators in the pipeline.

**Metric: `event_deser_time`**
- **Type**: Histogram
- **Description**: Time spent deserializing incoming events
- **Tags**: `group_by`, `team`, `production`, `environment`
- **Use Case**: Identify performance bottlenecks

**Metric: `spark_expr_eval_time`**
- **Type**: Histogram
- **Description**: Time spent on Spark expression eval per event
- **Tags**: `feature_group`, `team`, `production`, `environment`
- **Use Case**: Identify performance bottlenecks

**Metric: `spark_row_ser_time`**
- **Type**: Histogram
- **Description**: Time spent serializing a Spark row after expression evaluation
- **Tags**: `feature_group`, `team`, `production`, `environment`
- **Use Case**: Identify serialization bottlenecks in the expression evaluation stage

**Metric: `row_aggregation_time`**
- **Type**: Histogram
- **Description**: Time spent aggregating individual rows within windows
- **Tags**: `feature_group`, `team`, `production`, `environment`
- **Use Case**: Optimize aggregation logic and identify performance bottlenecks

**Metric: `row_tile_conversion_time`**
- **Type**: Histogram
- **Description**: Time spent converting aggregated rows to tile format
- **Tags**: `feature_group`, `team`, `production`, `environment`  
- **Use Case**: Monitor serialization performance

**Metric: `tile_avro_codec_time`**
- **Type**: Histogram
- **Description**: Time spent converting tiles to Avro format
- **Tags**: `feature_group`, `team`, `production`, `environment`
- **Use Case**: Monitor serialization performance

### 4. KV Store Write Metrics

Track the performance and reliability of writing features to the key-value store.

**Metric: `kvstore_writer.successes`**
- **Type**: Counter
- **Description**: Number of successful writes to the KV store
- **Tags**: `feature_group`, `team`, `production`, `environment`
- **Use Case**: Monitor write throughput and success rates

**Metric: `kvstore_writer.errors`**
- **Type**: Counter
- **Description**: Number of failed writes to the KV store
- **Tags**: `feature_group`, `team`, `production`, `environment`
- **Use Case**: Track write failures and identify KV store issues

**Metric: `multiput_time`**
- **Type**: Histogram
- **Description**: Time taken for multi-put operations to the KV store
- **Tags**: `feature_group`, `team`, `production`, `environment`
- **Use Case**: Monitor KV store write latency and identify performance issues

## Implementation Details

### Metrics Framework

Chronon uses Flink's [built-in metrics system](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/ops/metrics/) to track metrics. 
We use the [Flink Prometheus Reporter](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/deployment/metric_reporters/#prometheus) to export metrics to Prometheus-compatible backends.

- **Histogram Metrics**: Use exponentially decaying reservoirs for efficient percentile calculation
- **Counter Metrics**: Thread-safe counters for reliable event counting
- **Tags/Labels**: Comprehensive labeling with groupby name, team, production flag, and environment
- **Export Configuration**: Configurable export to Prometheus, Datadog, CloudWatch, or other OTLP-compatible backends

### Error Handling Philosophy

The streaming pipeline follows a "fail-safe" approach:
- Individual event processing errors are counted but don't fail the entire job
- Late events are tracked via side outputs but processed if possible
- KV store write failures are tracked but don't block processing of subsequent events
- Comprehensive error metrics enable proactive monitoring without pipeline instability

## Configuration

### Enabling Streaming Metrics

To enable streaming metrics collection, set the following Flink configuration parameters (already set in the existing DataprocSubmitter):

* "metrics.reporters" = "prom",
* "metrics.reporter.prom.factory.class" = "org.apache.flink.metrics.prometheus.PrometheusReporterFactory",

## Monitoring & Alerting

### Key Metrics to Monitor

**Job Health:**
1. `job_uptime` - Exposed by the Flink Job Manager per job. Indicates if the job is running
2. `job_numberOfCompletedCheckpoints` - Exposed by the Flink Job Manager per job. Indicates if checkpoints are succeeding

**Performance:**
1. `flink_processing_time` (p95, p99) - Processing latency trends
2. `multiput_time` (median, p95) - KV store performance
3. `event_created_to_sink_time` - End-to-end freshness

**Data Quality:**
1. `tiling.late_events` - Watermark effectiveness and upstream delays
2. `deserialization_errors` - Event deserialization errors (resulting in dropped events)
3. `sql_exec_errors` - Spark SQL execution errors (resulting in dropped events)
4. `kvstore_writer.errors` - KV store write failures (resulting in dropped feature updates)

## Native Support in Zipline Cloud

For users on the **Zipline paid offering**, streaming metrics are fully integrated with:

- **Real-time Dashboards**: Pre-built visualizations for all streaming metrics

## Limitations & Future Enhancements

### Current Limitations

- Metrics are emitted at the Flink operator level, not per-feature granularity
- Historical metric retention depends on your metrics backend configuration  
- Some metrics require manual correlation to identify root causes
- Limited built-in anomaly detection capabilities

### Future Enhancements

- Per-feature granularity metrics for more detailed monitoring
- Built-in anomaly detection for automatic alerting
- Integration with distributed tracing for end-to-end request tracking
- Enhanced schema evolution metrics and compatibility checking
- Automated performance tuning based on metrics trends
- Integration with data lineage systems for impact analysis
