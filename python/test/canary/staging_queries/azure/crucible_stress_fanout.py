from joins.azure import crucible_stress
from staging_queries.azure.claims_demo_baseline import CLAIMS_DEMO_CONF, CLAIMS_DEMO_ENV

from ai.chronon.types import EngineType, StagingQuery, TableDependency


FANOUT = 4


def _segment(segment_id: int) -> StagingQuery:
    return StagingQuery(
        query=f"""
        SELECT
            *,
            {segment_id} AS stress_segment
        FROM {crucible_stress.training_set.table}
        WHERE ds BETWEEN {{{{ start_date }}}} AND {{{{ end_date }}}}
          AND pmod(abs(hash(CAST(checkout_id AS STRING))), {FANOUT}) = {segment_id}
        """,
        output_namespace="claims_demo",
        engine_type=EngineType.SPARK,
        conf=CLAIMS_DEMO_CONF,
        env_vars=CLAIMS_DEMO_ENV,
        dependencies=[
            TableDependency(
                table=crucible_stress.training_set.table,
                partition_column="ds",
                start_offset=0,
                end_offset=0,
            )
        ],
        version=0,
        step_days=1,
    )


for _segment_id in range(FANOUT):
    globals()[f"segment_{_segment_id:03d}"] = _segment(_segment_id)


def _terminal_query() -> str:
    return "\nUNION ALL\n".join(
        f"""
        SELECT
            *
        FROM {globals()[f"segment_{segment_id:03d}"].table}
        WHERE ds BETWEEN {{{{ start_date }}}} AND {{{{ end_date }}}}
        """
        for segment_id in range(FANOUT)
    )


terminal = StagingQuery(
    query=_terminal_query(),
    output_namespace="claims_demo",
    engine_type=EngineType.SPARK,
    conf=CLAIMS_DEMO_CONF,
    env_vars=CLAIMS_DEMO_ENV,
    dependencies=[
        TableDependency(
            table=globals()[f"segment_{segment_id:03d}"].table,
            partition_column="ds",
            start_offset=0,
            end_offset=0,
        )
        for segment_id in range(FANOUT)
    ],
    version=0,
    step_days=1,
)
