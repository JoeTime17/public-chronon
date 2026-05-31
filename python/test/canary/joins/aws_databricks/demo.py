from group_bys.aws_databricks import dim_listings

from ai.chronon.types import Join, JoinPart

v1 = Join(
    left=dim_listings.source,
    row_ids=[],
    right_parts=[
        JoinPart(
            group_by=dim_listings.v1,
        ),
    ],
    online=False,
    output_namespace="workspace_iceberg.poc",
    step_days=30,
    enable_stats_compute=True,
)
