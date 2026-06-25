from copy import deepcopy

from staging_queries.azure.claims_demo_baseline import CLAIMS_DEMO_CONF, CLAIMS_DEMO_ENV

from ai.chronon.types import (
    Accuracy,
    Aggregation,
    EntitySource,
    EventSource,
    GroupBy,
    Operation,
    Query,
    TimeUnit,
    Window,
    selects,
)


INPUT_NAMESPACE = "join_matrix_input"
OUTPUT_NAMESPACE = "join_matrix"

HOURLY_INTERVAL = "1h"
SUBDAILY_INTERVAL = "3h"
SUBDAILY_OFFSET = "1h"
SUBDAILY_FORMAT = "yyyy-MM-dd-HH-mm"
HOURLY_SCHEDULE = "0 * * * *"
SUBDAILY_SCHEDULE = "0 1-22/3 * * *"
DAILY_INTERVAL = "1d"
DAILY_FORMAT = "yyyy-MM-dd"
ZERO_HOURS = Window(length=0, time_unit=TimeUnit.HOURS)


def _claims_demo_conf():
    return deepcopy(CLAIMS_DEMO_CONF)


def _claims_demo_env():
    return deepcopy(CLAIMS_DEMO_ENV)


def _subdaily_query(select_map, start_partition, time_column=None, **kwargs):
    return Query(
        selects=select_map,
        start_partition=start_partition,
        time_column=time_column,
        partition_column="ds",
        partition_format=SUBDAILY_FORMAT,
        partition_interval=SUBDAILY_INTERVAL,
        partition_offset=SUBDAILY_OFFSET,
        **kwargs,
    )


def _hourly_query(select_map, start_partition, time_column=None, **kwargs):
    return Query(
        selects=select_map,
        start_partition=start_partition,
        time_column=time_column,
        partition_column="ds",
        partition_format=SUBDAILY_FORMAT,
        partition_interval=HOURLY_INTERVAL,
        **kwargs,
    )


def _daily_query(select_map, start_partition, time_column=None, **kwargs):
    return Query(
        selects=select_map,
        start_partition=start_partition,
        time_column=time_column,
        partition_column="ds",
        partition_format=DAILY_FORMAT,
        partition_interval=DAILY_INTERVAL,
        partition_offset=ZERO_HOURS,
        **kwargs,
    )


parity_txn_sum = GroupBy(
    sources=[
        EventSource(
            table=f"{INPUT_NAMESPACE}.txn_events",
            query=_subdaily_query(
                selects("user_id", "txn_amount"),
                start_partition="2023-07-31-04-00",
                time_column="ts",
            ),
        )
    ],
    keys=["user_id"],
    aggregations=[
        Aggregation(
            input_column="txn_amount",
            operation=Operation.SUM,
            windows=[
                Window(length=1, time_unit=TimeUnit.DAYS),
                Window(length=14, time_unit=TimeUnit.DAYS),
            ],
        )
    ],
    accuracy=Accuracy.TEMPORAL,
    output_namespace=OUTPUT_NAMESPACE,
    conf=_claims_demo_conf(),
    env_vars=_claims_demo_env(),
    partition_interval=SUBDAILY_INTERVAL,
    partition_offset=SUBDAILY_OFFSET,
    offline_schedule=SUBDAILY_SCHEDULE,
)


parity_hourly_amount = GroupBy(
    sources=[
        EventSource(
            table=f"{INPUT_NAMESPACE}.hourly_events",
            query=_hourly_query(
                selects("user_id", "amount_1h"),
                start_partition="2023-08-13-19-00",
                time_column="ts",
            ),
        )
    ],
    keys=["user_id"],
    aggregations=[
        Aggregation(input_column="amount_1h", operation=Operation.SUM),
        Aggregation(
            input_column="amount_1h",
            operation=Operation.SUM,
            windows=[Window(length=6, time_unit=TimeUnit.HOURS)],
        ),
    ],
    accuracy=Accuracy.SNAPSHOT,
    output_namespace=OUTPUT_NAMESPACE,
    conf=_claims_demo_conf(),
    env_vars=_claims_demo_env(),
    partition_interval=HOURLY_INTERVAL,
    offline_schedule=HOURLY_SCHEDULE,
)


parity_daily_amount = GroupBy(
    sources=[
        EventSource(
            table=f"{INPUT_NAMESPACE}.daily_events",
            query=_daily_query(
                selects("user_id", "amount_d"),
                start_partition="2023-08-12",
            ),
        )
    ],
    keys=["user_id"],
    aggregations=[
        Aggregation(
            input_column="amount_d",
            operation=Operation.SUM,
        )
    ],
    accuracy=Accuracy.SNAPSHOT,
    output_namespace=OUTPUT_NAMESPACE,
    conf=_claims_demo_conf(),
    env_vars=_claims_demo_env(),
    partition_interval=DAILY_INTERVAL,
)


parity_offset_amount = GroupBy(
    sources=[
        EventSource(
            table=f"{INPUT_NAMESPACE}.offset_grid_events",
            query=_subdaily_query(
                selects("user_id", "amount_3h"),
                start_partition="2023-08-13-19-00",
                time_column="ts",
            ),
        )
    ],
    keys=["user_id"],
    aggregations=[
        Aggregation(input_column="amount_3h", operation=Operation.SUM),
        Aggregation(
            input_column="amount_3h",
            operation=Operation.SUM,
            windows=[Window(length=6, time_unit=TimeUnit.HOURS)],
        ),
    ],
    accuracy=Accuracy.SNAPSHOT,
    output_namespace=OUTPUT_NAMESPACE,
    conf=_claims_demo_conf(),
    env_vars=_claims_demo_env(),
    partition_interval=SUBDAILY_INTERVAL,
    partition_offset=SUBDAILY_OFFSET,
    offline_schedule=SUBDAILY_SCHEDULE,
)


parity_offset_balance = GroupBy(
    sources=[
        EntitySource(
            snapshot_table=f"{INPUT_NAMESPACE}.offset_grid_balance",
            query=_subdaily_query(
                selects("user_id", "balance_3h"),
                start_partition="2023-08-13-19-00",
            ),
        )
    ],
    keys=["user_id"],
    aggregations=None,
    accuracy=Accuracy.SNAPSHOT,
    output_namespace=OUTPUT_NAMESPACE,
    conf=_claims_demo_conf(),
    env_vars=_claims_demo_env(),
    partition_interval=SUBDAILY_INTERVAL,
    partition_offset=SUBDAILY_OFFSET,
    offline_schedule=SUBDAILY_SCHEDULE,
)


parity_ratings_sum = GroupBy(
    sources=[
        EntitySource(
            snapshot_table=f"{INPUT_NAMESPACE}.ratings_snapshot",
            mutation_table=f"{INPUT_NAMESPACE}.ratings_mutations",
            query=_daily_query(
                selects("user_id", "ts", "rating"),
                start_partition="2023-08-12",
                mutation_time_column="mutation_ts",
                reversal_column="is_before",
            ),
        )
    ],
    keys=["user_id"],
    aggregations=[Aggregation(input_column="rating", operation=Operation.SUM)],
    accuracy=Accuracy.TEMPORAL,
    output_namespace=OUTPUT_NAMESPACE,
    conf=_claims_demo_conf(),
    env_vars=_claims_demo_env(),
    partition_interval=DAILY_INTERVAL,
)


parity_offset_ratings_sum = GroupBy(
    sources=[
        EntitySource(
            snapshot_table=f"{INPUT_NAMESPACE}.ratings_snapshot_3h",
            mutation_table=f"{INPUT_NAMESPACE}.ratings_mutations_3h",
            query=_subdaily_query(
                selects("user_id", "ts", "rating"),
                start_partition="2023-08-13-19-00",
                mutation_time_column="mutation_ts",
                reversal_column="is_before",
            ),
        )
    ],
    keys=["user_id"],
    aggregations=[Aggregation(input_column="rating", operation=Operation.SUM)],
    accuracy=Accuracy.TEMPORAL,
    output_namespace=OUTPUT_NAMESPACE,
    conf=_claims_demo_conf(),
    env_vars=_claims_demo_env(),
    partition_interval=SUBDAILY_INTERVAL,
    partition_offset=SUBDAILY_OFFSET,
    offline_schedule=SUBDAILY_SCHEDULE,
)


parity_daily_balance = GroupBy(
    sources=[
        EntitySource(
            snapshot_table=f"{INPUT_NAMESPACE}.daily_balance",
            query=_daily_query(
                selects("user_id", "balance_d"),
                start_partition="2023-08-12",
            ),
        )
    ],
    keys=["user_id"],
    aggregations=None,
    accuracy=Accuracy.SNAPSHOT,
    output_namespace=OUTPUT_NAMESPACE,
    conf=_claims_demo_conf(),
    env_vars=_claims_demo_env(),
    partition_interval=DAILY_INTERVAL,
)
