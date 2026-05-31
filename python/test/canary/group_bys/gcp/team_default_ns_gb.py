"""Canary fixture: GroupBy with NO explicit `output_namespace=`.

Relies on the enclosing `gcp` team's `outputNamespace="data"` default. Compile
must stamp that namespace onto the compiled Thrift and resolve any placeholder
tokens that appeared at Python authoring time.
"""

from staging_queries.gcp import purchases_import

from ai.chronon.types import Aggregation, EventSource, GroupBy, Operation, Query, TimeUnit, Window, selects

source = EventSource(
    table=purchases_import.v1.table,
    query=Query(
        selects=selects("user_id", "purchase_price"),
        start_partition="2023-11-01",
        time_column="ts",
    ),
)

v1 = GroupBy(
    sources=[source],
    keys=["user_id"],
    version=0,
    aggregations=[
        Aggregation(
            input_column="purchase_price",
            operation=Operation.SUM,
            windows=[Window(length=1, time_unit=TimeUnit.DAYS)],
        ),
    ],
)
