"""Replicates Daniele's setup: two upstream export-style StagingQueries plus a
downstream StagingQuery that depends on both. One upstream dep uses plain
offset=0 (wait for today's upstream partition). The other uses start_cutoff
so the platform orchestrator requires every upstream partition from the
cutoff date through the downstream's query end to be Filled before the
downstream step can run.

Note: the TableDependency controls *scheduling* only. The downstream SQ's
{{ start_date }} / {{ end_date }} still come from its own startPartition +
unfilled-ranges logic — if the downstream SQL needs to scan all upstream
partitions, write it without a date filter on the upstream table.
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
        engine_type=EngineType.BIGQUERY,
        dependencies=[
            TableDependency(table=source_table, partition_column="ds", start_offset=0, end_offset=0)
        ],
        version=0,
        step_days=30,
    )


# Upstream A: rolling export — the downstream just needs today's partition.
# demo.* tables are refreshed daily in canary; the data.* seed tables stopped
# refreshing mid-2025 and the 2026 dates below were unreachable.
export_a = _passthrough_export("demo.dim_listings")

# Upstream B: export whose history the downstream wants to verify in full.
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
    engine_type=EngineType.BIGQUERY,
    dependencies=[
        # Plain daily dep on A — wait for today's A partition.
        TableDependency(table=export_a.table, partition_column="ds", start_offset=0, end_offset=0),
        # start_cutoff on B — the platform orchestrator requires every B partition
        # from the cutoff through the downstream's query end to be Filled before
        # this SQ can run. Omitting `offset` signals intent of "from the cutoff
        # onward, no rolling window."
        #
        # The chosen date aligns with the hub-backfill integration test's
        # [2026-03-01, 2026-03-03] window — this gives a tight, deterministic
        # contiguity check (5 days back from the backfill start) that remains
        # feasible in canary. Real customers would pick a cutoff that matches
        # their upstream's actual historical coverage.
        TableDependency(
            table=export_b.table,
            partition_column="ds",
            start_cutoff="2026-02-25",
        ),
    ],
    version=0,
    step_days=30,
)
