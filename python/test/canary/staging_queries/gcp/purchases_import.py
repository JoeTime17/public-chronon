from ai.chronon.types import EngineType, StagingQuery, TableDependency

v1 = StagingQuery(
    query="SELECT * FROM data.purchases WHERE ds BETWEEN {{ start_date }} AND {{ end_date }}",
    engine_type=EngineType.BIGQUERY,
    output_namespace="data",
    dependencies=[
        TableDependency(table="data.purchases", partition_column="ds", start_offset=0, end_offset=0)
    ],
    version=0,
)
