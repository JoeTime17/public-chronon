from group_bys.aws import dim_listings, dim_merchants, user_activities
from staging_queries.aws import exports

from ai.chronon.types import (
    Derivation,
    EventSource,
    GroupBy,
    Join,
    JoinPart,
    Query,
    selects,
)

"""
This Join combines user activity events with:
1. User-level behavioral features (from user_activities GroupBy)
2. Listing-level attributes (from dim_listings GroupBy)

Left side: Raw user activity events
Right parts:
- User behavioral aggregations (keyed by user_id)
- Listing dimension attributes (keyed by listing_id)
"""

# Left side: Raw user activity events from Kinesis export
source = EventSource(
    # This will be the Glue table that receives the Kinesis data
    table=exports.user_activities.table,
    query=Query(
        selects=selects(user_id="user_id", listing_id="listing_id", row_id="event_id"),
        time_column="event_time_ms",
    ),
)

# Join with user behavioral features and listing attributes
v1 = Join(
    left=source,
    row_ids=["event_id"],  # TODO -- kill this once the SPJ API change goes through
    right_parts=[
        # User behavioral features (aggregated over time windows)
        JoinPart(
            group_by=user_activities.v1,
        ),
        # Listing dimension attributes (point-in-time lookup)
        JoinPart(
            group_by=dim_listings.v1,
        ),
        # Listing dimension attributes (point-in-time lookup)
        JoinPart(group_by=dim_merchants.v1, prefix="merchant_"),
    ],
    version=1,
    online=True,
    output_namespace="data",
    enable_stats_compute=True,
    step_days=10,
)

# Example join with some derivations
derivations_v1 = Join(
    left=source,
    row_ids=["event_id"],  # TODO -- kill this once the SPJ API change goes through
    right_parts=[
        JoinPart(
            group_by=dim_listings.v1,
        ),
        JoinPart(
            group_by=user_activities.v1,
        ),
    ],
    derivations=[
        Derivation(
            name="is_listing_heavy",
            expression="IF(listing_id_weight_grams > 1000, 1, 0)",
        ),
        # with a built-in Spark fn
        Derivation(
            name="is_item_handmade",
            expression="array_contains(split(listing_id_tags, ','), 'handmade')",
        ),
        Derivation(name="price_log", expression="log1p(listing_id_price_cents)"),
        Derivation(
            name="price_bucket",
            expression="""
                CASE
                    WHEN listing_id_price_cents < 1000 THEN 0
                    WHEN listing_id_price_cents < 5000 THEN 1
                    WHEN listing_id_price_cents < 10000 THEN 2
                    WHEN listing_id_price_cents < 50000 THEN 3
                    ELSE 4
                END
            """,
        ),
        Derivation(name="*", expression="*"),
    ],
    version=2,
    online=True,
    output_namespace="data",
    enable_stats_compute=True,
    step_days=10,
)

from ai.chronon.types import StagingQuery, TableDependency

v1_s = StagingQuery(
    query=f"""
SELECT
    *,
    case when rand() < 0.5 then 0 else 1 end as label
FROM {derivations_v1.table}
WHERE ds BETWEEN {{{{ start_date }}}} AND {{{{ end_date }}}}
""",
    output_namespace="data",
    dependencies=[
        TableDependency(table=derivations_v1.table, partition_column="ds", start_offset=0, end_offset=0),
    ],
    version=0,
)
