from joins.aws import training_set

from ai.chronon.types import EngineType, StagingQuery, TableDependency


def get_staging_query(category_name):
    query = f"""
        SELECT
            *,
            '{category_name}' as category_name
        FROM {training_set.v1_test.table}
        WHERE ds BETWEEN {{{{ start_date }}}} AND {{{{ end_date }}}}
    """
    return StagingQuery(
        query=query,
        output_namespace="data",
        table_properties={"sample_config_json": """{"sample_key": "sample value"}"""},
        dependencies=[
            TableDependency(table=training_set.v1_test.table, partition_column="ds", start_offset=1, end_offset=1)
        ],
        version=0,
        step_days=10,
    )

cart = get_staging_query("cart")
user = get_staging_query("user")
item = get_staging_query("item")
order = get_staging_query("order")
payment = get_staging_query("payment")
shipping = get_staging_query("shipping")


def terminal_query(staging_queries):
    full_query =  "\nUNION ALL\n".join([f"""SELECT
                *
            FROM {staging_query.table}
            WHERE ds BETWEEN {{{{ start_date }}}} AND {{{{ end_date }}}}""" for staging_query in staging_queries])
    return full_query


terminal = StagingQuery(
    query=terminal_query([cart, user, item, order, payment, shipping]),
    table_properties={"sample_config_json": """{"sample_key": "sample value"}"""},
    output_namespace="data",
    dependencies=[
        TableDependency(table=cart.table, partition_column="ds", start_offset=1, end_offset=1),
        TableDependency(table=user.table, partition_column="ds", start_offset=1, end_offset=1),
        TableDependency(table=item.table, partition_column="ds", start_offset=1, end_offset=1),
        TableDependency(table=order.table, partition_column="ds", start_offset=1, end_offset=1),
        TableDependency(table=payment.table, partition_column="ds", start_offset=1, end_offset=1),
        TableDependency(table=shipping.table, partition_column="ds", start_offset=1, end_offset=1),
    ],
    version=0,
    step_days=10,
)

purchases_labels = StagingQuery(
    query=f"""
SELECT
    *,
    case when rand() < 0.5 then 0 else 1 end as label
FROM {training_set.v1_test.table}
WHERE ds BETWEEN {{{{ start_date }}}} AND {{{{ end_date }}}}
""",
    table_properties={"sample_config_json": """{"sample_key": "sample value"}"""},
    output_namespace="data",
    dependencies=[
        TableDependency(table=training_set.v1_test.table, partition_column="ds", start_offset=0, end_offset=0),
    ],
    version=0,
    step_days=10,
)

query_hub = f"""
SELECT
    *
FROM {training_set.v1_hub.table}
WHERE ds BETWEEN {{{{ start_date }}}} AND {{{{ end_date }}}}
"""

v1_hub = StagingQuery(
    query=query_hub,
    output_namespace="data",
    table_properties={"sample_config_json": """{"sample_key": "sample value"}"""},
    dependencies=[
        TableDependency(table=training_set.v1_hub.table, partition_column="ds", start_offset=1, end_offset=1)
    ],
    version=0,
    step_days=10,
)

bigquery_import_query = f"""
SELECT
    *
FROM {training_set.v1_hub.table}
WHERE ds BETWEEN {{{{ start_date }}}} AND {{{{ end_date }}}}
"""

v1_bigquery_import = StagingQuery(
    query=bigquery_import_query,
    engine_type=EngineType.BIGQUERY,
    output_namespace="data",
    dependencies=[
        TableDependency(table=training_set.v1_hub.table, partition_column="ds", start_offset=0, end_offset=0)
    ],
    version=0,
    step_days=10,
)
