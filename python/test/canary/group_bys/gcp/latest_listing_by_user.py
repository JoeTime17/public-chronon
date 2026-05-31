from staging_queries.gcp import exports

from ai.chronon.types import Accuracy, Aggregation, EventSource, GroupBy, Operation, Query, TimeUnit, Window, selects
from ai.chronon.types import EnvironmentVariables

"""
Point-in-time lookup from user_id to the most recent listing_id the user interacted
with in the trailing 7-day window.

This is intentionally simple so it can be used as the first hop in a join-to-join
chaining example where Join1 adds listing_id and Join2 consumes that listing_id as
part of its left source.
"""

source = EventSource(
    table=exports.user_activities.table,
    query=Query(
        selects=selects(
            user_id="user_id",
            listing_id="listing_id",
        ),
        start_partition="2025-01-01",
        time_column="event_time_ms",
    ),
)

v1 = GroupBy(
    sources=[source],
    keys=["user_id"],
    aggregations=[
        Aggregation(
            input_column="listing_id",
            operation=Operation.LAST,
            windows=[Window(length=7, time_unit=TimeUnit.DAYS)],
        ),
    ],
    accuracy=Accuracy.TEMPORAL,
    online=True,
    version=1,
    env_vars=EnvironmentVariables(
        common={
            "CHRONON_ONLINE_ARGS": "-Ztasks=1",
        }
    ),
)
