from ai.chronon.types import EngineType, StagingQuery, TableDependency


def get_select_star_export(table: str, partition_column: str = "ds"):
    spark_export_sql = f"""
    SELECT
        *
    FROM demo.{table}
    WHERE
    {partition_column} BETWEEN {{{{ start_date }}}} AND {{{{ end_date }}}}
    """

    return StagingQuery(
        query=spark_export_sql,
        output_namespace="data",
        engine_type=EngineType.SPARK,
        dependencies=[
            TableDependency(table=f"demo.{table}", partition_column=partition_column, start_offset=0, end_offset=0)
        ],
        version=0,
        step_days=10,
    )


def get_native_partition_export(table: str, partition_column: str):
    native_partition_sql = f"""
    SELECT
        *,
        DATE_FORMAT({partition_column}, 'yyyy-MM-dd') as ds
    FROM demo.{table}
    WHERE
    {partition_column} BETWEEN {{{{ start_date }}}} AND {{{{ end_date }}}}
    """
    return StagingQuery(
        query=native_partition_sql,
        output_namespace="data",
        engine_type=EngineType.SPARK,
        dependencies=[
            TableDependency(table=f"demo.{table}", partition_column=partition_column, start_offset=0, end_offset=0)
        ],
        version=0,
        step_days=10,
    )


user_activities = get_native_partition_export("user_activities", "event_time")
checkouts = get_native_partition_export("checkouts", "ts")
dim_listings = get_select_star_export("dim_listings", "ds")
dim_merchants = get_select_star_export("dim_merchants", "ds")
dim_users = get_select_star_export("dim_users", "ds")
