"""Canary example: a Join whose left source pulls its table name via `.table`
from a StagingQuery that does NOT set `output_namespace=`.

`team_default_ns_example.v1.table` must resolve to `<team_ns>.<clean_name>` at
compile time — via the enclosing `gcp` team's `outputNamespace="data"` default
— without requiring a filesystem lookup at Python authoring time.
"""

from group_bys.gcp import purchases
from staging_queries.gcp import team_default_ns_example

from ai.chronon.types import EventSource, Join, JoinPart, Query, selects

source = EventSource(
    table=team_default_ns_example.v1.table,
    query=Query(
        selects=selects("user_id"),
        time_column="ts",
    ),
)

v1 = Join(
    left=source,
    row_ids="user_id",
    right_parts=[JoinPart(group_by=purchases.v1_test)],
    version=0,
)
