### Chronon Python API


#### Overview

Chronon Python API for materializing configs to be run by the Chronon Engine. Contains python helpers to help managed a repo of feature and join definitions to be executed by the chronon scala engine.


#### User API Overview

##### Sources

Most fields are self explanatory. Time columns are expected to be in milliseconds (unixtime).

```python
# File <repo>/sources/sample_sources.py
from ai.chronon.query import (
  Query,
  select,
)
from ai.chronon.source import EventSource, EntitySource

# Sample query
Query(
  selects=select(
      user="user_id",
      created_at="created_at",
  ),
  wheres=["has_availability = 1"],
  start_partition="2021-01-01",  # Defines the beginning of time for computations related to the source.
  setups=["...UDF..."],
  time_column="ts",
  end_partition=None,
  mutation_time_column="mutation_timestamp",
  reversal_column="CASE WHEN mutation_type IN ('DELETE', 'UPDATE_BEFORE') THEN true ELSE false END"
)

user_activity = EntitySource(
  snapshot_table="db_exports.table",
  mutation_table="mutations_namespace.table_mutations",
  mutation_topic="mutationsKafkaTopic",
  query=Query(...)
)

website__views = EventSource(
  table="namespace.table",
  topic="kafkaTopicForEvents",
)
```


##### Group By (Features)

Group Bys are aggregations over sources that define features. For example:

```python
# File <repo>/group_bys/example_team/example_group_by.py
from ai.chronon.group_by import (
  GroupBy,
  Window,
  TimeUnit,
  Accuracy,
  Operation,
  Aggregations,
  Aggregation,
  DefaultAggregation,
)
from sources import sample_sources

sum_cols = [f"active_{x}_days" for x in [30, 90, 120]]


v0 = GroupBy(
  sources=test_source.user_activity,
  keys=["user"],
  aggregations=Aggregations(
    user_active_1_day=Aggregation(operation=Operation.LAST),
    second_feature=Aggregation(
      input_column="active_7_days",
      operation=Operation.SUM,
      windows=[
        Window(n, TimeUnit.DAYS) for n in [3, 5, 9]
      ]
    ),
  ) + [
    Aggregation(
      input_column=col,
      operation=Operation.SUM
    ) for col in sum_columns           # Alternative syntax for defining aggregations.
  ] + [
    Aggregation(
      input_column="device",
      operation=LAST_K(10)
    )
  ],
  dependencies=[
    "db_exports.table/ds={{ ds }}"      # If not defined will be derived from the Source info.
  ],
  accuracy=Accuracy.SNAPSHOT,          # This could be TEMPORAL for point in time correctness.
  env={
    "backfill": {                      # Execution environment variables for each of the modes for `run.py`
      "EXECUTOR_MEMORY": "4G"
     },
  },
  online=True,                         # True if this group by needs to be uploaded to a KV Store.
  production=False                     # True if this group by is production level.
)
```

##### Join

A Join is a collection of feature values for the keys and (times if applicable) defined on the left (source). Example:

```python
# File <repo>/joins/example_team/example_join.py
from ai.chronon.join import Join, JoinPart
from sources import sample_sources
from group_bys.example_team import example_group_by

v1 = Join(
    left=sample_sources.website__views,
    right_parts=[
        JoinPart(group_by=example_group_by.v0),
    ],
    online=True,       # True if this join will be fetched in production.
    production=False,  # True if this join should not use non-production group bys.
    env={"backfill": {"PARALLELISM": "10"}, "streaming": {"STREAMING_ENV_VAR": "VALUE"}},
)
```

##### Compile

`zipline compile` turns authored configs (under `group_bys/`, `joins/`, `staging_queries/`, `models/`, `model_transforms/`) into thriftjson under `compiled/`. The compiler reads team-level defaults (output namespace, env vars, conf, cluster config) from `teams.py` and merges them onto each config's `metaData.executionInfo`.

```bash
zipline compile --chronon-root <repo-root>
```

###### Multi-environment compile via `teams.canary.py`

To compile the same configs against a canary deployment, drop a sibling `teams.canary.py` next to `teams.py`. `zipline compile` will run a second pass and write to `compiled_canary/`. Only `canary` is supported as a non-prod env today.

See the **[Multi-Environment Compile & Deploy guide](https://docs.zipline.ai/docs/running_on_zipline_hub/MultiEnvironment)** for the full guide — directory layout, the `from teams import …` authoring pattern, per-entity `environments=[...]` opt-in, the `--env` deploy contract, the compile summary, and the CI workflow.

##### Pre-commit Setup

1. Install pre-commit and other dev libraries:
```
pip install -r requirements/dev.txt
```
2. Run the following command under `api/python` to install the git hook scripts:
```
pre-commit install
```

To support more pre-commit hooks, add them to the `.pre-commit-config.yaml` file.