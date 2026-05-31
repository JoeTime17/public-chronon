from group_bys.azure import crucible_stress
from staging_queries.azure import crucible_stress_sources
from staging_queries.azure.claims_demo_baseline import CLAIMS_DEMO_CONF, CLAIMS_DEMO_ENV

from ai.chronon.types import EventSource, Join, JoinPart, Query, selects


checkout_source = EventSource(
    table=crucible_stress_sources.checkouts.table,
    query=Query(
        selects=selects(
            "checkout_id",
            "user_id",
        ),
        time_column="ts",
    ),
)

training_set = Join(
    left=checkout_source,
    row_ids=["checkout_id"],
    right_parts=[
        JoinPart(group_by=crucible_stress.user_purchase_features),
    ],
    online=False,
    output_namespace="claims_demo",
    version=0,
    conf=CLAIMS_DEMO_CONF,
    env_vars=CLAIMS_DEMO_ENV,
    step_days=1,
    enable_stats_compute=False,
)
