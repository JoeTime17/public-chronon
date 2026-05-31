from ai.chronon.types import EngineType, StagingQuery, TableDependency


def get_select_star_export(table: str, partition_column: str = "ds"):
    """Export a dimension table that already has a ds partition column."""
    spark_export_sql = f"""
    SELECT
        *
    FROM demo.{table}
    WHERE
    {partition_column} BETWEEN {{{{ start_date }}}} AND {{{{ end_date }}}}
    """
    return StagingQuery(
        query=spark_export_sql,
        output_namespace="quickstart",
        engine_type=EngineType.SPARK,
        dependencies=[
            TableDependency(table=f"demo.{table}", partition_column=partition_column, start_offset=0, end_offset=0)
        ],
        version=0,
        step_days=20,
    )


user_activities = get_select_star_export("user_activities", "ds")
dim_listings = get_select_star_export("dim_listings", "ds")
dim_merchants = get_select_star_export("dim_merchants", "ds")
dim_users = get_select_star_export("dim_users", "ds")
