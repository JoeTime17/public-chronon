from ai.chronon.types import EngineType, StagingQuery, TableDependency


def get_select_star_export(table: str, partition_column: str = "ds"):
    snowflake_export_sql = f"""
    SELECT
      * EXCLUDE ({partition_column}),
       DATE_TRUNC('DAY', {partition_column})::DATE as ds
    FROM {table}
    WHERE
    DATE_TRUNC('DAY', {partition_column}) BETWEEN {{{{ start_date }}}} AND {{{{ end_date }}}}
    """

    return StagingQuery(
        query=snowflake_export_sql,
        output_namespace="data",
        engine_type=EngineType.SNOWFLAKE,
        dependencies=[
            TableDependency(table=f"{table}", partition_column=partition_column, start_offset=0, end_offset=0)
        ],
        version=0,
        step_days=30,
    )


def get_native_partition_export(table: str, partition_column: str, time_partitioned: bool = None, version: int = 0):
    snowflake_partition_sql = f"""
    SELECT
        *,
        DATE_TRUNC('DAY', {partition_column})::DATE as ds
    FROM {table}
    WHERE
    {partition_column} BETWEEN {{{{ start_date }}}} AND {{{{ end_date }}}}
    """
    return StagingQuery(
        query=snowflake_partition_sql,
        output_namespace="data",
        engine_type=EngineType.SNOWFLAKE,
        dependencies=[
            TableDependency(table=f"{table}", partition_column=partition_column, start_offset=0, end_offset=0, time_partitioned=time_partitioned)
        ],
        version=version,
        step_days=30,
    )

def user_activities_export(version: int = 0):
    """ User activities is generated from the avro packages published by the azure event hub"""
    sql = f"""
    SELECT
        parsed.event_id,
        parsed.event_time_ms,
        parsed.ingested_time_ms,
        parsed.user_id,
        parsed.session_id,
        parsed.device_type,
        parsed.country_code,
        parsed.listing_id,
        parsed.event_type,
        to_date(from_unixtime(parsed.event_time_ms / 1000)) AS ds
    FROM (
        SELECT *
          , from_json(CAST(Body AS STRING)
          , 'event_id STRING, event_time_ms BIGINT, ingested_time_ms BIGINT,
             user_id STRING, session_id STRING, device_type STRING,
             country_code STRING, listing_id BIGINT, event_type STRING') AS parsed
        FROM raw_activities
        WHERE Body IS NOT NULL
    )
    WHERE parsed.event_time_ms >= CAST(to_unix_timestamp({{{{start_date}}}}, 'yyyy-MM-dd') * 1000 AS BIGINT)
      AND parsed.event_time_ms <  CAST(to_unix_timestamp({{{{end_date}}}}, 'yyyy-MM-dd') * 1000 AS BIGINT);
    """
    return StagingQuery(
            setups=[
                "ADD JAR 'abfss://demo@ziplineai2.dfs.core.windows.net/jars/spark-avro_2.12-3.5.3.jar'",
                """
                CREATE OR REPLACE TEMPORARY VIEW raw_activities USING avro
                OPTIONS (
                  path 'abfss://demo@ziplineai2.dfs.core.windows.net/zipline-demo-events/user-activities/',
                  recursiveFileLookup 'true'
                );""",
            ],
            query=sql,
            output_namespace="data",
            dependencies=[],
            version=version,
            step_days=30,
            )


user_activities = user_activities_export(version=0)
checkouts = get_native_partition_export("checkouts", "ds")
dim_listings_pc = get_native_partition_export("dim_listings_custom_part", "datest", time_partitioned=True, version=1)
dim_listings = get_select_star_export("dim_listings", "ds")
dim_merchants = get_select_star_export("dim_merchants", "ds")
dim_users = get_select_star_export("dim_users", "ds")
