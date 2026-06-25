from joins.gcp import demo

from ai.chronon.types import StagingQuery, TableDependency

# For modular join backfills
v1 = StagingQuery(
    query=f"""
SELECT 
    *,
    case when rand() < 0.5 then 0 else 1 end as label
FROM {demo.derivations_v1.derived_table}
WHERE ds BETWEEN {{{{ start_date }}}} AND {{{{ end_date }}}}
""",
    output_namespace="data",
    dependencies=[
        TableDependency(table=demo.derivations_v1.derived_table, partition_column="ds", start_offset=0, end_offset=0),
    ],
    version=0,
)
