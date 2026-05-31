---
title: "Observability Overview"
order: 1
---

# Observability Overview

Chronon is a complex system that has various components each with their own observability requirements. The most crucial of these are:

* Fetcher observability: The Chronon fetcher is responsible for serving feature values for online inference. It emits performance metrics, outlined in more detail in the [Fetcher Documentation](../../../docker/fetcher/README.md).
* Data quality: Chronon produces data in batch, stored in Iceberg tables, and online data produced in realtime stored in the KV store. Data quality metrics are computed and exposed on both of these. See [Data Quality documentation](./data_quality.md).
* Metrics: Chronon emits a broad set of metrics from the fetcher service, batch jobs, and KV store backends. See the [Metrics Reference](./metrics.md) for a full listing.
* Streaming metrics: Metrics specific to the Flink-based streaming pipeline (event ingestion, aggregation, KV store writes). See [Streaming Metrics](./streaming_metrics.md) for more details.
* Online/offline consistency: Chronon measures the consistency between the data it produces to power online serving and what it backfills for training. See [Online/offline consistency documentation](./Online_Offline_Consistency.md).
* Orchestration: Using open source Chronon requires orchestrating jobs via an external system such as Airflow. Because these jobs are critical to compute production data, observability on those systems is strongly suggested. You can see a sample implementation of Airflow integration with Datadog metrics [here](https://github.com/airbnb/chronon/tree/main/airflow). Note this is not required for Zipline users, as the Zipline Hub has built in orchestration.

**Note:** All of the above observability is provided out of the box for Zipline users. For open-source users, the computation and serving of the metrics is all in the open source project, however integration into existing systems is required for dashboarding, visualization, etc.

# Management Overview

Beyond the basic [Setup](../setup/Overview.md) of Chronon and Observability (outlined above), the main thing for Chronon teams to be aware of in production are the various [Failure scenarios and how to handle them](./failure_handling.md).
