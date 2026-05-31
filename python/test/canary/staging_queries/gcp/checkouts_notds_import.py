from ai.chronon.types import EngineType, StagingQuery, TableDependency
from ai.chronon.types import ConfigProperties

v1 = StagingQuery(
    query="SELECT * FROM data.checkouts_notds WHERE notds BETWEEN {{ start_date }} AND {{ end_date }}",
    engine_type=EngineType.BIGQUERY,
    output_namespace="data",
    conf=ConfigProperties(common={"spark.chronon.partition.column": "notds"}),
    dependencies=[
        TableDependency(table="data.checkouts_notds", partition_column="notds", start_offset=0, end_offset=0)
    ],
    version=0,
)
