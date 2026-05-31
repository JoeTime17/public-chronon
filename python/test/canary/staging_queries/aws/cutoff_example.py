"""AWS mirror of gcp/cutoff_example.py.

Two upstream export-style StagingQueries plus a downstream StagingQuery that
depends on both. One upstream dep uses plain offset=0 (wait for today's
upstream partition). The other uses start_cutoff so the platform orchestrator
requires every upstream partition from the cutoff date through the downstream's
query end to be Filled before the downstream step can run.

Source tables are the same demo.* tables refreshed daily for this cloud's
canary warehouse. See gcp/cutoff_example.py for the original design notes.
"""

from ai.chronon.types import EngineType, StagingQuery, TableDependency


def _passthrough_export(source_table: str):
    """Produces a ds-partitioned copy of an already-ds-partitioned source."""
    query = f"""
    SELECT *
    FROM {source_table}
    WHERE ds BETWEEN {{{{ start_date }}}} AND {{{{ end_date }}}}
    """
    return StagingQuery(
        query=query,
        output_namespace="data",
        engine_type=EngineType.SPARK,
        dependencies=[
            TableDependency(table=source_table, partition_column="ds", start_offset=0, end_offset=0)
        ],
        version=0,
        step_days=30,
    )


export_a = _passthrough_export("demo.dim_listings")
export_b = _passthrough_export("demo.dim_merchants")


downstream = StagingQuery(
    query=f"""
    SELECT
        a.ds AS ds,
        a.listing_id AS listing_id,
        a.merchant_id AS merchant_id
    FROM {export_a.table} a
    JOIN {export_b.table} b USING (ds, merchant_id)
    WHERE a.ds BETWEEN {{{{ start_date }}}} AND {{{{ end_date }}}}
    """,
    output_namespace="data",
    engine_type=EngineType.SPARK,
    dependencies=[
        TableDependency(table=export_a.table, partition_column="ds", start_offset=0, end_offset=0),
        TableDependency(
            table=export_b.table,
            partition_column="ds",
            start_cutoff="2026-02-25",
        ),
    ],
    version=0,
    step_days=30,
)
