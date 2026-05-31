from joins.azure import demo

from ai.chronon.types import StagingQuery, TableDependency

# For modular join backfills
v1 = StagingQuery(
    query=f"""
SELECT
    *,
    case when rand() < 0.5 then 0 else 1 end as label
FROM {demo.derivations_v3.table}__derived
WHERE ds BETWEEN {{{{ start_date }}}} AND {{{{ end_date }}}}
""",
    output_namespace="data",
    dependencies=[
        TableDependency(table=f"{demo.derivations_v3.table}__derived", partition_column="ds", start_offset=0, end_offset=0),
    ],
    version=0,
)
