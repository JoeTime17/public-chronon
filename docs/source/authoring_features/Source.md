---
title: "Sources"
order: 4
---

# Sources

Sources in Chronon define data inputs to feature computation pipelines. It's often the first thing that users will write when defining a new entity in Chronon, and getting it right is one of the most important steps in the authoring process. Defining it correctly will make everything fall into place correctly.

There are five base source types in Chronon that can be used as inputs to feature pipelines. They differ primarily in the shape of data that they ingest, and where they ingest it from.

All features are created with one of these sources as input, except for `ChainedFeatures` which use existing features as inputs.

## Overview

All sources are basically composed of the following pieces*:

1. Table that represents an offline warehouse such as hive or a topic that represents an event stream in a messaging system such as kafka
2. A `Query` component, which tells chronon which fields to extract for the computation pipeline

*External sources are the exception to the above, those are explained more below.


## Streaming EventSource

Taken from the [returns.py](https://github.com/zipline-ai/chronon/blob/main/api/python/test/sample/group_bys/quickstart/returns.py) example GroupBy in the quickstart tutorial.

```python
source = Source(
    events=EventSource(
        table="data.returns", # This points to the log table with historical return events
        topic="events.returns", # Streaming event
        query=Query(
            selects=select("user_id","refund_amt"), # Select the fields we care about
            time_column="ts") # The event time
    ))
```

Key points:

* Contains a table that has historical data for the input event, and a topic that can be listened to for realtime updates
* The query specifies a few columns that we care about in our pipeline
* A time column is provided that corresponds to the event times with millisecond accuracy


## Batch EventSource

Modified from the above example.

```python
source = Source(
    events=EventSource(
        table="data.returns",
        topic=None, # This makes it a batch source
        query=Query(
            selects=select("user_id","refund_amt"),
        )
    ))
```

Key points:
* Omitting the topic turns a streaming event source into a batch event source, as the streaming input is not specified
* Features built on this source will be computed daily (as batch jobs) as new data lands in the source table
* Time column can also be omitted, since Chronon already knows the timeline along with feature change (batch updates everytime data lands daily)
* A time column could be included if you wanted offline computation to be intra-day accurate, however this should be done carefully, as online features will only be getting daily batch updates

## Streaming EntitySource

Here is an example of a streaming EntitySource, modeled after a hypothetical "users" table.

```python
user_activity = Source(entities=EntitySource(
  snapshotTable="db_snapshots.users",
  mutationTable="db_mutations.users",
  mutationTopic="events.users_mutations",
  query=Query(
            selects=select("user_id","account_created_ds","email_verified"), # Select the fields we care about
        )
)
```

In this case there would be:

1. A production users table that powers the application
2. An offline table `db_snapshots.users` that contains daily snapshots of the production table
3. A change data capture system that writes changes to the `events.users_mutations` topic, and has corresponding historical events in the `db_mutations.users` table.

As you can see, a pre-requisite to using the streaming `EntitySource` is a change capture system. [Debezium](https://debezium.io/) is one suitable solution for this piece of upstream infrastructure.

For a more detailed complete guide see [Entity Source](./EntitySource.md).

## Batch EntitySource

Taken from the [users.py](https://github.com/zipline-ai/chronon/blob/main/api/python/test/sample/group_bys/quickstart/users.py) example GroupBy in the quickstart tutorial.

```python
source = Source(
    entities=EntitySource(
        snapshotTable="data.users", # This points to a table that contains daily snapshots of the entire product catalog
        query=Query(
            selects=select("user_id","account_created_ds","email_verified"), # Select the fields we care about
        )
    ))
```

This is similar to the above, however, it only contains the `snapshotTable`, and not the batch and streaming mutations sources. See [Entity Source](./EntitySource.md) for the full reference on all EntitySource parameters.

## Time-Partitioned Sources

Many modern data warehouses (BigQuery, Snowflake, Delta Lake) don't use Hive-style string partitions like `ds=2024-01-15`. Instead, tables have a timestamp or date column that serves as the time axis. Chronon supports these tables via the `time_partitioned` flag on `Query`.

```python
source = Source(
    events=EventSource(
        table="warehouse.user_events",
        query=Query(
            selects=select("user_id", "event_value"),
            time_column="UNIX_TIMESTAMP(event_ts) * 1000",  # millis for event-time logic
            partition_column="event_ts",  # the timestamp/date column to filter by
            time_partitioned=True,        # signals this is not a Hive-partitioned table
        )
    ))
```

Key points:

* `partition_column` is set to the timestamp or date column in the source table (e.g. `event_ts`, `created_at`). Chronon uses this column for range filtering (`WHERE event_ts >= '2024-01-01' AND event_ts < '2024-01-02'`).
* `time_column` remains the expression that produces milliseconds-since-epoch for Chronon's event-time windowing and temporal join logic. It is independent of `partition_column`.
* `time_partitioned=True` tells Chronon that the table does not have discrete Hive-style partitions. Instead of running `SHOW PARTITIONS`, Chronon computes `MIN` and `MAX` of the `partition_column` and expands that range into a contiguous list of dates (one per day by default, or on the consumer's sub-daily grid). This is a **gap-filling** approach: every partition between the earliest and latest observed timestamp is treated as present, even if no rows exist for some partitions in between. The logic assumes continuous coverage across the observed range rather than enumerating only partitions that actually contain data.
* For sub-daily outputs, external `time_partitioned=True` sources may omit `partition_interval`/`partition_offset`; Chronon slices them on the consumer grid. Physical Hive-style sources still need an explicit grid unless they come from another Chronon config's `.table`/`.derived_table` reference.
* After scanning, the partition column values are formatted into date strings (e.g. `yyyy-MM-dd`) and mapped to the standard `ds` column. This means downstream output tables remain physically date-partitioned exactly as they would be with Hive-style sources — consumers see no difference.
* The orchestration sensor uses the format provider's `maxTimestampDate` method (e.g. `SELECT DATE(MAX(partition_column))`) to check whether data has landed for the required date range, replacing the standard partition-existence check. Each engine (Spark/BigQuery/Snowflake) implements this via its native connector.
