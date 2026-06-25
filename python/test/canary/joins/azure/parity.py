from copy import deepcopy

from group_bys.azure import join_parity_matrix as group_bys
from staging_queries.azure.claims_demo_baseline import CLAIMS_DEMO_CONF, CLAIMS_DEMO_ENV

from ai.chronon.types import EventSource, Join, JoinPart, Query, selects


def _claims_demo_conf():
    return deepcopy(CLAIMS_DEMO_CONF)


def _claims_demo_env():
    return deepcopy(CLAIMS_DEMO_ENV)


left = EventSource(
    table=f"{group_bys.INPUT_NAMESPACE}.left_events",
    query=Query(
        selects=selects("user_id"),
        start_partition="2023-08-13",
        time_column="ts",
        partition_column="ds",
        partition_format=group_bys.DAILY_FORMAT,
        partition_interval=group_bys.DAILY_INTERVAL,
    ),
)


matrix = Join(
    left=left,
    right_parts=[
        JoinPart(group_by=group_bys.parity_txn_sum, prefix="tmp"),
        JoinPart(group_by=group_bys.parity_hourly_amount, prefix="snap1"),
        JoinPart(group_by=group_bys.parity_daily_amount, prefix="snapd"),
        JoinPart(group_by=group_bys.parity_offset_amount, prefix="snap3"),
        JoinPart(group_by=group_bys.parity_offset_balance, prefix="ent3"),
        JoinPart(group_by=group_bys.parity_ratings_sum, prefix="mut"),
        JoinPart(group_by=group_bys.parity_offset_ratings_sum, prefix="mut3"),
        JoinPart(group_by=group_bys.parity_daily_balance, prefix="entd"),
    ],
    output_namespace=group_bys.OUTPUT_NAMESPACE,
    online=True,
    use_long_names=False,
    conf=_claims_demo_conf(),
    env_vars=_claims_demo_env(),
    partition_interval=group_bys.DAILY_INTERVAL,
    modular_execution=True,
)
