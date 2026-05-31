import pytest
from gen_thrift.api import ttypes
from gen_thrift.common import ttypes as common

from ai.chronon import query, scd2, source


def event_source():
    return source.EventSource(
        table="data.contracts_scd2",
        query=query.Query(
            selects={
                "user_id": "user_id",
                "contract_id": "contract_id",
                "valid_from_ts": "valid_from_ms",
                "valid_to_ts": "valid_to_ms",
                "amount": "cast(amount_cents as double) / 100",
                "status": "status",
            },
            time_column="valid_from_ms",
        ),
    )


def test_scd2_group_by_lowers_to_single_hidden_bucketed_last():
    gb = scd2.GroupBy(
        sources=[event_source()],
        keys=["user_id"],
        row_id_column="contract_id",
        valid_from_column="valid_from_ts",
        valid_to_column="valid_to_ts",
        aggregations=[
            scd2.Aggregation(input_column="amount", operation=scd2.Operation.SUM, windows=["7d", "30d"]),
            scd2.Aggregation(input_column="amount", operation=scd2.Operation.AVG),
            scd2.Aggregation(
                input_column="amount",
                operation=scd2.Operation.MAX,
                windows=[common.Window(15, common.TimeUnit.MINUTES)],
            ),
            scd2.Aggregation(
                operation=scd2.Operation.COUNT,
                windows=["1d"],
                window_column="birth_ts",
            ),
            scd2.Aggregation(operation=scd2.Operation.COUNT),
            scd2.Aggregation(input_column="status", operation=scd2.Operation.LATEST),
        ],
    )

    assert gb.accuracy == ttypes.Accuracy.TEMPORAL
    assert len(gb.aggregations) == 1
    hidden_agg = gb.aggregations[0]
    assert hidden_agg.inputColumn == "__chronon_scd2_row"
    assert hidden_agg.operation == ttypes.Operation.LAST
    assert hidden_agg.buckets == ["__chronon_scd2_row_id"]
    assert hidden_agg.windows is None

    selects = gb.sources[0].events.query.selects
    assert selects["__chronon_scd2_row_id"] == "cast(contract_id as string)"
    assert selects["__chronon_scd2_row"].startswith("named_struct(")
    assert "'__chronon_scd2_valid_from', cast(valid_from_ms as bigint)" in selects["__chronon_scd2_row"]
    assert (
        "'__chronon_scd2_valid_to', coalesce(cast(valid_to_ms as bigint), "
        "cast(9223372036854775807 as bigint))"
    ) in selects["__chronon_scd2_row"]
    assert "'amount', cast(amount_cents as double) / 100" in selects["__chronon_scd2_row"]
    assert "'birth_ts', birth_ts" in selects["__chronon_scd2_row"]
    assert "'status', status" in selects["__chronon_scd2_row"]

    derivation_names = [derivation.name for derivation in gb.derivations]
    assert derivation_names == [
        "amount_sum_7d",
        "amount_sum_30d",
        "amount_avg",
        "amount_max_15m",
        "count_by_birth_ts_1d",
        "count",
        "status_latest",
    ]

    expressions = {derivation.name: derivation.expression for derivation in gb.derivations}
    assert "__chronon_scd2_row_last_by___chronon_scd2_row_id" in expressions["amount_sum_7d"]
    assert "x.__chronon_scd2_valid_from <= ts" in expressions["amount_sum_7d"]
    assert "ts < x.__chronon_scd2_valid_to" in expressions["amount_sum_7d"]
    assert "x.__chronon_scd2_ts > ts - 604800000" in expressions["amount_sum_7d"]
    assert "acc.sum / acc.cnt" in expressions["amount_avg"]
    assert "x.__chronon_scd2_ts > ts - 900000" in expressions["amount_max_15m"]
    assert "x.birth_ts > ts - 86400000" in expressions["count_by_birth_ts_1d"]
    assert "cast(size(filter(map_values" in expressions["count"]
    assert "array_sort(filter(map_values" in expressions["status_latest"]


def test_scd2_group_by_sets_time_column_from_valid_from_when_missing():
    src = source.EventSource(
        table="data.contracts_scd2",
        query=query.Query(
            selects={
                "user_id": "user_id",
                "contract_id": "contract_id",
                "valid_from_ts": "valid_from_ms",
                "valid_to_ts": "valid_to_ms",
                "amount": "amount",
            },
        ),
    )

    gb = scd2.GroupBy(
        sources=src,
        keys=["user_id"],
        row_id_column="contract_id",
        valid_from_column="valid_from_ts",
        valid_to_column="valid_to_ts",
        aggregations=[scd2.Aggregation(input_column="amount", operation=scd2.Operation.SUM)],
    )

    assert gb.sources[0].events.query.timeColumn == "valid_from_ms"


def test_scd2_aggregation_validation():
    with pytest.raises(ValueError, match="input_column is required"):
        scd2.Aggregation(operation=scd2.Operation.SUM)

    with pytest.raises(ValueError, match="input_column is not supported"):
        scd2.Aggregation(input_column="amount", operation=scd2.Operation.COUNT)

    with pytest.raises(ValueError, match="Unsupported SCD2 operation"):
        scd2.Aggregation(input_column="amount", operation="last_k")

    with pytest.raises(ValueError, match="window_column requires windows"):
        scd2.Aggregation(operation=scd2.Operation.COUNT, window_column="birth_ts")

    with pytest.raises(TypeError, match="scd2.Aggregation"):
        scd2.GroupBy(
            sources=event_source(),
            keys=["user_id"],
            row_id_column="contract_id",
            valid_from_column="valid_from_ts",
            valid_to_column="valid_to_ts",
            aggregations=[ttypes.Aggregation(inputColumn="amount", operation=ttypes.Operation.SUM)],
        )

    with pytest.raises(ValueError, match="TEMPORAL accuracy"):
        scd2.GroupBy(
            sources=event_source(),
            keys=["user_id"],
            row_id_column="contract_id",
            valid_from_column="valid_from_ts",
            valid_to_column="valid_to_ts",
            aggregations=[scd2.Aggregation(input_column="amount", operation=scd2.Operation.SUM)],
            accuracy=ttypes.Accuracy.SNAPSHOT,
        )
