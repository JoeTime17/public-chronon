from staging_queries.azure import crucible_stress_sources
from staging_queries.azure.claims_demo_baseline import CLAIMS_DEMO_CONF, CLAIMS_DEMO_ENV

from ai.chronon.types import Aggregation, EventSource, GroupBy, Operation, Query, TimeUnit, Window, selects


purchase_source = EventSource(
    table=crucible_stress_sources.purchases.table,
    query=Query(
        selects=selects("user_id", "purchase_price"),
        start_partition="2026-05-01",
        time_column="ts",
    ),
)

purchase_windows = [Window(length=days, time_unit=TimeUnit.DAYS) for days in [1, 3, 7]]

user_purchase_features = GroupBy(
    sources=[purchase_source],
    keys=["user_id"],
    online=False,
    version=0,
    aggregations=[
        Aggregation(
            input_column="purchase_price",
            operation=Operation.SUM,
            windows=purchase_windows,
        ),
        Aggregation(
            input_column="purchase_price",
            operation=Operation.COUNT,
            windows=purchase_windows,
        ),
        Aggregation(
            input_column="purchase_price",
            operation=Operation.AVERAGE,
            windows=purchase_windows,
        ),
        Aggregation(
            input_column="purchase_price",
            operation=Operation.LAST_K(10),
        ),
    ],
    output_namespace="claims_demo",
    conf=CLAIMS_DEMO_CONF,
    env_vars=CLAIMS_DEMO_ENV,
    step_days=1,
)
