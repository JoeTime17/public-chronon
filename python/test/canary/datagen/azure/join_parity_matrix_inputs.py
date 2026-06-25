#!/usr/bin/env python3
"""Generate Azure join parity matrix source tables.

This is an external input producer for the canary join matrix. It writes raw
Iceberg tables under join_matrix_input so the orchestrator sensors can observe
real partitions before the groupBy uploads run.
"""

from __future__ import annotations

import argparse
import hashlib
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable, List, Sequence, Tuple

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
)


NAMESPACE = "join_matrix_input"
USERS = ("u1", "u2", "u3")
DEFAULT_START = "2023-07-31-04-00"


@dataclass(frozen=True)
class PartitionWindow:
    start: datetime
    end: datetime


def parse_time(value: str) -> datetime:
    normalized = value.strip().replace("T", " ").removesuffix("Z")
    formats = (
        "%Y-%m-%d-%H-%M",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
    )
    for fmt in formats:
        try:
            parsed = datetime.strptime(normalized, fmt)
            return parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    raise ValueError(f"Unsupported time value: {value}")


def fmt_hour(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d-%H-%M")


def fmt_day(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")


def epoch_millis(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def floor_to_hour(dt: datetime) -> datetime:
    return dt.replace(minute=0, second=0, microsecond=0)


def floor_to_day(dt: datetime) -> datetime:
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


def floor_to_grid(dt: datetime, interval_hours: int, offset_hours: int = 0) -> datetime:
    origin = datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(hours=offset_hours)
    delta_hours = int((dt - origin).total_seconds() // 3600)
    floored = delta_hours - (delta_hours % interval_hours)
    return origin + timedelta(hours=floored)


def hourly_partitions(start: datetime, end: datetime) -> List[PartitionWindow]:
    current = floor_to_hour(start)
    last = floor_to_hour(end)
    windows = []
    while current <= last:
        windows.append(PartitionWindow(current, current + timedelta(hours=1)))
        current += timedelta(hours=1)
    return windows


def offset_3h_partitions(start: datetime, end: datetime) -> List[PartitionWindow]:
    current = floor_to_grid(start, interval_hours=3, offset_hours=1)
    last = floor_to_grid(end, interval_hours=3, offset_hours=1)
    windows = []
    while current <= last:
        windows.append(PartitionWindow(current, current + timedelta(hours=3)))
        current += timedelta(hours=3)
    return windows


def daily_partitions(start: datetime, end: datetime) -> List[PartitionWindow]:
    current = floor_to_day(start)
    last = floor_to_day(end)
    windows = []
    while current <= last:
        windows.append(PartitionWindow(current, current + timedelta(days=1)))
        current += timedelta(days=1)
    return windows


def stable_value(*parts: object, modulo: int = 1000, minimum: int = 1) -> int:
    payload = "|".join(str(part) for part in parts).encode("utf-8")
    digest = hashlib.sha256(payload).hexdigest()
    return int(digest[:12], 16) % modulo + minimum


def rows_for_users(partition: str, value_name: str) -> Iterable[Tuple[str, int]]:
    for user in USERS:
        yield user, stable_value(value_name, partition, user, modulo=500, minimum=1)


def create_table(spark: SparkSession, table: str, fields: Sequence[StructField]) -> None:
    columns = ",\n  ".join(f"{field.name} {spark_type(field.dataType)}" for field in fields)
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
          {columns}
        )
        USING iceberg
        PARTITIONED BY (ds)
        """
    )


def spark_type(data_type) -> str:
    if isinstance(data_type, StringType):
        return "STRING"
    if isinstance(data_type, LongType):
        return "BIGINT"
    if isinstance(data_type, BooleanType):
        return "BOOLEAN"
    raise TypeError(f"Unsupported type: {data_type}")


def write_rows(
    spark: SparkSession,
    table: str,
    schema: StructType,
    rows: Sequence[tuple],
    chunk_partitions: int,
) -> None:
    if not rows:
        return
    create_table(spark, table, schema.fields)
    partition_index = schema.fieldNames().index("ds")
    rows_by_partition = {}
    for row in rows:
        rows_by_partition.setdefault(row[partition_index], []).append(row)

    partitions = sorted(rows_by_partition)
    for index in range(0, len(partitions), chunk_partitions):
        chunk = partitions[index : index + chunk_partitions]
        chunk_rows = [row for partition in chunk for row in rows_by_partition[partition]]
        df = spark.createDataFrame(chunk_rows, schema=schema)
        # Bound the number of partitions handled by each writer task. Iceberg's
        # fanout writer keeps one file writer open per output partition.
        df = df.repartition(min(8, len(chunk)), "ds")
        df.writeTo(table).overwritePartitions()
        print(f"Wrote {len(chunk_rows)} rows to {table} across {len(chunk)} partitions")

    print(f"Finished {table}: {len(rows)} rows across {len(partitions)} partitions")


def build_rows(start: datetime, end: datetime):
    hourly = hourly_partitions(start, end)
    offset = offset_3h_partitions(start, end)
    daily = daily_partitions(start, end)

    left_rows = []
    for window in daily:
        ds = fmt_day(window.start)
        for user in USERS:
            left_rows.append((user, epoch_millis(window.start + timedelta(hours=12)), ds))

    txn_rows = []
    for window in offset:
        ds = fmt_hour(window.start)
        event_ts = epoch_millis(window.start + timedelta(minutes=45))
        for user, amount in rows_for_users(ds, "txn"):
            txn_rows.append((user, amount, event_ts, ds))

    hourly_rows = []
    for window in hourly:
        ds = fmt_hour(window.start)
        event_ts = epoch_millis(window.start + timedelta(minutes=30))
        for user, amount in rows_for_users(ds, "hourly"):
            hourly_rows.append((user, amount, event_ts, ds))

    daily_event_rows = []
    daily_balance_rows = []
    ratings_snapshot_rows = []
    ratings_mutation_rows = []
    for window in daily:
        ds = fmt_day(window.start)
        snapshot_ts = epoch_millis(window.start + timedelta(hours=23, minutes=59))
        mutation_ts = epoch_millis(window.start + timedelta(hours=12))
        for user, amount in rows_for_users(ds, "daily-event"):
            daily_event_rows.append((user, amount, ds))
        for user, balance in rows_for_users(ds, "daily-balance"):
            daily_balance_rows.append((user, balance + 1000, ds))
        for user, rating in rows_for_users(ds, "ratings-snapshot"):
            ratings_snapshot_rows.append((user, snapshot_ts, rating % 10 + 1, ds))
        for user, rating in rows_for_users(ds, "ratings-mutation"):
            ratings_mutation_rows.append((user, mutation_ts, rating % 5 + 1, ds, mutation_ts, False))

    offset_event_rows = []
    offset_balance_rows = []
    ratings_snapshot_3h_rows = []
    ratings_mutation_3h_rows = []
    for window in offset:
        ds = fmt_hour(window.start)
        event_ts = epoch_millis(window.start + timedelta(minutes=75))
        mutation_ts = epoch_millis(window.start + timedelta(minutes=90))
        for user, amount in rows_for_users(ds, "offset-event"):
            offset_event_rows.append((user, amount, event_ts, ds))
        for user, balance in rows_for_users(ds, "offset-balance"):
            offset_balance_rows.append((user, balance + 2000, ds))
        for user, rating in rows_for_users(ds, "ratings-3h-snapshot"):
            ratings_snapshot_3h_rows.append((user, event_ts, rating % 10 + 1, ds))
        for user, rating in rows_for_users(ds, "ratings-3h-mutation"):
            ratings_mutation_3h_rows.append((user, event_ts, rating % 5 + 1, ds, mutation_ts, False))

    return {
        "left_events": left_rows,
        "txn_events": txn_rows,
        "hourly_events": hourly_rows,
        "daily_events": daily_event_rows,
        "offset_grid_events": offset_event_rows,
        "offset_grid_balance": offset_balance_rows,
        "ratings_snapshot": ratings_snapshot_rows,
        "ratings_mutations": ratings_mutation_rows,
        "ratings_snapshot_3h": ratings_snapshot_3h_rows,
        "ratings_mutations_3h": ratings_mutation_3h_rows,
        "daily_balance": daily_balance_rows,
    }


SCHEMAS = {
    "left_events": StructType(
        [StructField("user_id", StringType()), StructField("ts", LongType()), StructField("ds", StringType())]
    ),
    "txn_events": StructType(
        [
            StructField("user_id", StringType()),
            StructField("txn_amount", LongType()),
            StructField("ts", LongType()),
            StructField("ds", StringType()),
        ]
    ),
    "hourly_events": StructType(
        [
            StructField("user_id", StringType()),
            StructField("amount_1h", LongType()),
            StructField("ts", LongType()),
            StructField("ds", StringType()),
        ]
    ),
    "daily_events": StructType(
        [StructField("user_id", StringType()), StructField("amount_d", LongType()), StructField("ds", StringType())]
    ),
    "offset_grid_events": StructType(
        [
            StructField("user_id", StringType()),
            StructField("amount_3h", LongType()),
            StructField("ts", LongType()),
            StructField("ds", StringType()),
        ]
    ),
    "offset_grid_balance": StructType(
        [StructField("user_id", StringType()), StructField("balance_3h", LongType()), StructField("ds", StringType())]
    ),
    "ratings_snapshot": StructType(
        [
            StructField("user_id", StringType()),
            StructField("ts", LongType()),
            StructField("rating", LongType()),
            StructField("ds", StringType()),
        ]
    ),
    "ratings_mutations": StructType(
        [
            StructField("user_id", StringType()),
            StructField("ts", LongType()),
            StructField("rating", LongType()),
            StructField("ds", StringType()),
            StructField("mutation_ts", LongType()),
            StructField("is_before", BooleanType()),
        ]
    ),
    "ratings_snapshot_3h": StructType(
        [
            StructField("user_id", StringType()),
            StructField("ts", LongType()),
            StructField("rating", LongType()),
            StructField("ds", StringType()),
        ]
    ),
    "ratings_mutations_3h": StructType(
        [
            StructField("user_id", StringType()),
            StructField("ts", LongType()),
            StructField("rating", LongType()),
            StructField("ds", StringType()),
            StructField("mutation_ts", LongType()),
            StructField("is_before", BooleanType()),
        ]
    ),
    "daily_balance": StructType(
        [StructField("user_id", StringType()), StructField("balance_d", LongType()), StructField("ds", StringType())]
    ),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", default="spark_catalog")
    parser.add_argument("--namespace", default=NAMESPACE)
    parser.add_argument("--start", default=DEFAULT_START)
    parser.add_argument("--end", default=None)
    parser.add_argument("--lookback-days", type=int, default=None)
    parser.add_argument("--lookahead-hours", type=int, default=6)
    parser.add_argument("--chunk-partitions", type=int, default=128)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    end = parse_time(args.end) if args.end else now + timedelta(hours=args.lookahead_hours)
    if args.lookback_days is not None:
        start = end - timedelta(days=args.lookback_days)
    else:
        start = parse_time(args.start)

    if start > end:
        raise ValueError(f"start must be <= end: {start.isoformat()} > {end.isoformat()}")

    spark = SparkSession.builder.appName("join-parity-matrix-input-generator").getOrCreate()
    spark.sparkContext.setLogLevel("INFO")

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {args.catalog}.{args.namespace}")
    rows_by_table = build_rows(start, end)
    print(f"Generating join matrix input partitions from {start.isoformat()} to {end.isoformat()}")

    for table_name, rows in rows_by_table.items():
        full_table_name = f"{args.catalog}.{args.namespace}.{table_name}"
        write_rows(spark, full_table_name, SCHEMAS[table_name], rows, args.chunk_partitions)

    spark.stop()


if __name__ == "__main__":
    main()
