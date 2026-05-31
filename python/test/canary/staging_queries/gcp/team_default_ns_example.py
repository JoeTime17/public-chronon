"""Canary example: a StagingQuery that does NOT set `output_namespace=`.

Relies on the enclosing team's (`gcp`, `outputNamespace="data"`) default for
namespace resolution at compile time. Referenced by `joins/gcp/team_default_ns_join.py`
via `.table` at authoring time — that access path must not require reaching into
`teams.py` (no filesystem I/O, no side effects) and must not fail when the
underlying object hasn't had its namespace populated yet.

Exists purely to exercise the placeholder-resolution path; without a conf like
this, every canary `.table` reference points at a producer with explicit
`output_namespace=` and the fallback path is never hit.
"""

from ai.chronon.types import EngineType, StagingQuery, TableDependency

v1 = StagingQuery(
    query="SELECT * FROM data.checkouts WHERE ds BETWEEN {{ start_date }} AND {{ end_date }}",
    engine_type=EngineType.BIGQUERY,
    dependencies=[
        TableDependency(table="data.checkouts", partition_column="ds", start_offset=0, end_offset=0)
    ],
    version=0,
)
