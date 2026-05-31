from staging_queries.azure.claims_demo_baseline import CLAIMS_DEMO_CONF, CLAIMS_DEMO_ENV

from ai.chronon.types import EngineType, StagingQuery, TableDependency


SNOWFLAKE_SCHEMA = "DEMO.CLAIMS_DEMO"


def _snowflake_export(source_table: str, select_clause: str) -> StagingQuery:
    return StagingQuery(
        query=f"""
        SELECT
            {select_clause},
            DATE_TRUNC('DAY', DS)::DATE AS ds
        FROM {SNOWFLAKE_SCHEMA}.{source_table}
        WHERE DATE_TRUNC('DAY', DS)::DATE BETWEEN {{{{ start_date }}}} AND {{{{ end_date }}}}
        """,
        output_namespace="claims_demo",
        engine_type=EngineType.SNOWFLAKE,
        conf=CLAIMS_DEMO_CONF,
        env_vars=CLAIMS_DEMO_ENV,
        dependencies=[
            TableDependency(
                table=f"{SNOWFLAKE_SCHEMA}.{source_table}",
                partition_column="DS",
                start_offset=0,
                end_offset=0,
            )
        ],
        version=0,
        step_days=1,
    )


purchases = _snowflake_export(
    "CRUCIBLE_STRESS_PURCHASES",
    """
            USER_ID AS user_id,
            PURCHASE_PRICE AS purchase_price,
            TS AS ts
    """,
)

checkouts = _snowflake_export(
    "CRUCIBLE_STRESS_CHECKOUTS",
    """
            CHECKOUT_ID AS checkout_id,
            USER_ID AS user_id,
            TS AS ts
    """,
)
