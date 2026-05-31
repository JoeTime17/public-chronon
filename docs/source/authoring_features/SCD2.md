---
title: "SCD2"
order: 2
---

# SCD2 GroupBys

Slowly changing dimension type 2, usually shortened to SCD2, is a warehouse modeling pattern for data whose values
change over time while preserving history. Instead of updating a row in place, the table keeps one row per version and
records the interval when that version was valid.

A typical SCD2 table looks like:

| user_id | contract_id | valid_from_ts | valid_to_ts | amount | status |
|---------|-------------|---------------|-------------|--------|--------|
| u1      | c1          | 1704067200000 | 1704153600000 | 10.0 | basic  |
| u1      | c1          | 1704153600000 | null          | 20.0 | pro    |

At query time, the active rows are the rows where:

```sql
valid_from_ts <= ts AND ts < coalesce(valid_to_ts, infinity)
```

Chronon exposes SCD2 support as a small Python helper that generates a normal `GroupBy`. Use it when the feature should
aggregate the set of rows active at the request or training row timestamp.

## Example

```python
from ai.chronon import scd2
from ai.chronon.types import EventSource, Query

contracts_source = EventSource(
    table="warehouse.contracts_scd2",
    query=Query(
        selects={
            "user_id": "user_id",
            "contract_id": "contract_id",
            "valid_from_ts": "valid_from_ts",
            "valid_to_ts": "valid_to_ts",
            "amount": "amount",
            "status": "status",
        },
        time_column="valid_from_ts",
    ),
)

contracts = scd2.GroupBy(
    sources=[contracts_source],
    keys=["user_id"],
    row_id_column="contract_id",
    valid_from_column="valid_from_ts",
    valid_to_column="valid_to_ts",
    aggregations=[
        scd2.Aggregation(
            input_column="amount",
            operation=scd2.Operation.SUM,
            windows=["7d", "30d"],
        ),
        scd2.Aggregation(
            input_column="amount",
            operation=scd2.Operation.AVG,
        ),
        scd2.Aggregation(
            operation=scd2.Operation.COUNT,
        ),
        scd2.Aggregation(
            operation=scd2.Operation.EXISTS,
        ),
        scd2.Aggregation(
            input_column="status",
            operation=scd2.Operation.COUNT_DISTINCT,
        ),
        scd2.Aggregation(
            operation=scd2.Operation.COUNT,
            windows=["10950d"],
            window_column="birth_ts",
        ),
    ],
)
```

The SCD2 aggregation operations are intentionally narrower than the normal Chronon `Operation` enum:

| operation | meaning |
|-----------|---------|
| `COUNT` | count active SCD2 rows |
| `EXISTS` | return whether at least one SCD2 row is active |
| `SUM` | sum a numeric field across active rows |
| `AVG` | average a numeric field across active rows |
| `MIN` | minimum field value across active rows |
| `MAX` | maximum field value across active rows |
| `LATEST` | field value from the active row with the latest `valid_from` |
| `EARLIEST` | field value from the active row with the earliest `valid_from` |
| `COUNT_DISTINCT` | exact distinct count of a field across active rows |

Names are generated the same way as regular GroupBy aggregations:

```text
amount_sum_7d
amount_sum_30d
amount_avg
count
exists
status_count_distinct
count_by_birth_ts_10950d
```

## How It Works

`scd2.GroupBy` is Python authoring sugar. It returns a regular Chronon `GroupBy` so the Spark and serving engines do not
need a new thrift object.

The helper casts `row_id_column` into a hidden bucket column and creates one hidden struct containing the validity
interval, timestamp, and all fields referenced by the SCD2 aggregations:

```sql
named_struct(
  '__chronon_scd2_valid_from', cast(valid_from_ts as bigint),
  '__chronon_scd2_valid_to', coalesce(cast(valid_to_ts as bigint), cast(9223372036854775807 as bigint)),
  '__chronon_scd2_ts', cast(valid_from_ts as bigint),
  'amount', amount,
  'status', status
)
```

It then creates one hidden bucketed aggregation:

```python
Aggregation(
    input_column="__chronon_scd2_row",
    operation=Operation.LAST,
    buckets=["__chronon_scd2_row_id"],
)
```

That produces a map from `row_id` to the latest known version of that row. Each requested SCD2 aggregation is generated
as a `Derivation` over that map. For example, `SUM(amount)` filters active rows at the join timestamp and folds them:

```sql
aggregate(
  filter(
    map_values(__chronon_scd2_row_last_by___chronon_scd2_row_id),
    x -> x.__chronon_scd2_valid_from <= ts AND ts < x.__chronon_scd2_valid_to
  ),
  named_struct('sum', cast(0 as double), 'cnt', cast(0 as bigint)),
  (acc, x) -> named_struct(
    'sum', acc.sum + IF(x.amount IS NULL, cast(0 as double), cast(x.amount as double)),
    'cnt', acc.cnt + IF(x.amount IS NULL, cast(0 as bigint), cast(1 as bigint))
  ),
  acc -> IF(acc.cnt = 0, cast(NULL as double), acc.sum)
)
```

Windows are applied in the generated derivation, not on the hidden `LAST` aggregation. This matters because a row can
become valid before the window but still be active at the request timestamp. The hidden map must keep the latest version
per `row_id`; the user-facing window decides which active rows participate in a particular feature.

By default, a window filters on the SCD2 version timestamp (`valid_from_column`). An aggregation can instead set
`window_column` to filter active rows using a different timestamp stored on the row. For example, to count active users
whose current birthdate value is within the last 30 years by zipcode, key the GroupBy by `zipcode` and use:

```python
scd2.Aggregation(
    operation=scd2.Operation.COUNT,
    windows=["10950d"],
    window_column="birth_ts",
)
```

If a user later corrects their birthdate, the hidden `LAST` bucket keeps only the latest active SCD2 version for that
user, and the derivation applies the 30-year filter to that latest `birth_ts` value.

## Notes

- `row_id_column` is cast to string because Chronon bucketed aggregations produce map keys.
- `valid_to_column` may be null; null means the row is open-ended.
- SCD2 GroupBys always use `TEMPORAL` accuracy because the generated features depend on the left row's timestamp.
- The helper includes only fields referenced by SCD2 aggregations in the hidden struct.
- Deletions are represented by closing the interval with `valid_to`; expired rows are filtered out before aggregation.
- If an SCD2 warehouse table mutates old partitions when intervals close, make sure the Chronon backfill range covers
  those corrected partitions.
