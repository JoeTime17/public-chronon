from ai.chronon.types import EngineType, StagingQuery, TableDependency

table_name = "test_table"
partition_column = "ds"  # Your table must be partitioned by a time-based column.

snowflake_export_sql = f"""
    SELECT
      *
    FROM {table_name}
    WHERE
    DATE_TRUNC('DAY', {partition_column}) BETWEEN {{{{ start_date }}}} AND {{{{ end_date }}}}
"""

v1 = StagingQuery(
    query=snowflake_export_sql,
    output_namespace="data",
    engine_type=EngineType.SNOWFLAKE,
    dependencies=[
        # Your table must be partitioned by a time-based column.
        TableDependency(table=table_name, partition_column=partition_column, start_offset=0, end_offset=0),
    ],
)
