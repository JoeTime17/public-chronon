from group_bys.gcp import dim_listings, latest_listing_by_user
from staging_queries.gcp import exports

from ai.chronon.types import Derivation, EventSource, Join, JoinPart, JoinSource, Query, selects

"""
Two-hop join chaining example for GCP canary.

Join1:
- left side only carries user_id and ts from user activities
- right side adds the latest listing_id observed for that user in the trailing 7 days

Join2:
- left side is a JoinSource over Join1 that projects user_id + listing_id
- right side joins listing-level features keyed by listing_id
"""

source = EventSource(
    table=exports.user_activities.table,
    query=Query(
        selects=selects(
            "user_id",
        ),
        time_column="event_time_ms",
    ),
)

join1_v1 = Join(
    left=source,
    row_ids=["user_id"],
    right_parts=[
        JoinPart(
            group_by=latest_listing_by_user.v1,
        ),
    ],
    version=1,
)

join2_left = JoinSource(
    join=join1_v1,
    query=Query(
        selects=selects(
            user_id="user_id",
            listing_id="user_id_listing_id_last_7d",
        ),
        time_column="ts",
    ),
)

join2_v1 = Join(
    left=join2_left,
    row_ids=["user_id"],
    right_parts=[
        JoinPart(
            group_by=dim_listings.v1,
        ),
    ],
    version=1,
)

join1_modular_derived_v1 = Join(
    left=source,
    row_ids=["user_id"],
    right_parts=[
        JoinPart(
            group_by=latest_listing_by_user.v1,
        ),
    ],
    derivations=[
        Derivation(
            name="derived_listing_id",
            expression="user_id_listing_id_last_7d",
        ),
    ],
    modular_execution=True,
    version=3,
)

join2_modular_derived_v1 = Join(
    left=JoinSource(
        join=join1_modular_derived_v1,
        query=Query(
            selects=selects(
                user_id="user_id",
                listing_id="derived_listing_id",
            ),
            time_column="ts",
        ),
    ),
    row_ids=["user_id"],
    right_parts=[
        JoinPart(
            group_by=dim_listings.v1,
        ),
    ],
    version=3,
)
