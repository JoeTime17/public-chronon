#     Copyright (C) 2023 The Chronon Authors.
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

from collections.abc import Sequence
from copy import deepcopy
from dataclasses import dataclass
from typing import Dict, List, Optional, Union

import ai.chronon.group_by as group_by
import ai.chronon.utils as utils
import ai.chronon.windows as window_utils
import gen_thrift.api.ttypes as ttypes
import gen_thrift.common.ttypes as common
from ai.chronon.derivation import Derivation


class Operation:
    COUNT = "count"
    EXISTS = "exists"
    SUM = "sum"
    AVG = "avg"
    AVERAGE = AVG
    MIN = "min"
    MAX = "max"
    LATEST = "latest"
    EARLIEST = "earliest"
    COUNT_DISTINCT = "count_distinct"


_OPERATIONS = {
    Operation.COUNT,
    Operation.EXISTS,
    Operation.SUM,
    Operation.AVG,
    Operation.MIN,
    Operation.MAX,
    Operation.LATEST,
    Operation.EARLIEST,
    Operation.COUNT_DISTINCT,
}

_INPUT_REQUIRED = {
    Operation.SUM,
    Operation.AVG,
    Operation.MIN,
    Operation.MAX,
    Operation.LATEST,
    Operation.EARLIEST,
    Operation.COUNT_DISTINCT,
}

_HIDDEN_ROW_ID_COLUMN = "__chronon_scd2_row_id"
_HIDDEN_ROW_COLUMN = "__chronon_scd2_row"
_VALID_FROM_FIELD = "__chronon_scd2_valid_from"
_VALID_TO_FIELD = "__chronon_scd2_valid_to"
_ROW_TS_FIELD = "__chronon_scd2_ts"
_INFINITY_TS = "cast(9223372036854775807 as bigint)"


@dataclass(frozen=True)
class Aggregation:
    input_column: Optional[str] = None
    operation: str = Operation.COUNT
    windows: Optional[List[Union[common.Window, str]]] = None
    window_column: Optional[str] = None

    def __post_init__(self):
        if self.operation == Operation.AVERAGE:
            object.__setattr__(self, "operation", Operation.AVG)
        if self.operation not in _OPERATIONS:
            raise ValueError(
                f"Unsupported SCD2 operation {self.operation}. "
                f"Supported operations: {sorted(_OPERATIONS)}"
            )
        if self.operation in _INPUT_REQUIRED and not self.input_column:
            raise ValueError(f"input_column is required for SCD2 operation {self.operation}")
        if self.operation in (Operation.COUNT, Operation.EXISTS) and self.input_column:
            raise ValueError(f"input_column is not supported for SCD2 operation {self.operation}")
        if self.window_column and not self.windows:
            raise ValueError("window_column requires windows")

    @property
    def normalized_windows(self) -> Optional[List[common.Window]]:
        if not self.windows:
            return None
        return [window_utils.normalize_window(w) for w in self.windows]


def _window_to_suffix(window: common.Window) -> str:
    unit = common.TimeUnit._VALUES_TO_NAMES[window.timeUnit].lower()[0]
    return f"{window.length}{unit}"


def _window_to_millis(window: common.Window) -> int:
    if window.timeUnit == common.TimeUnit.MINUTES:
        return window.length * 60 * 1000
    if window.timeUnit == common.TimeUnit.HOURS:
        return window.length * 60 * 60 * 1000
    if window.timeUnit == common.TimeUnit.DAYS:
        return window.length * 24 * 60 * 60 * 1000
    raise ValueError(f"Unsupported SCD2 window time unit: {window.timeUnit}")


def _output_name(aggregation: Aggregation, window: Optional[common.Window]) -> str:
    if aggregation.input_column:
        base = f"{aggregation.input_column}_{aggregation.operation}"
    else:
        base = aggregation.operation
    if aggregation.window_column:
        base = f"{base}_by_{aggregation.window_column}"
    if window:
        return f"{base}_{_window_to_suffix(window)}"
    return base


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "\\'") + "'"


def _source_query(source: ttypes.Source) -> ttypes.Query:
    if source.events is not None:
        return source.events.query
    if source.entities is not None:
        return source.entities.query
    if source.joinSource is not None:
        return source.joinSource.query
    raise ValueError("scd2.GroupBy only supports EventSource, EntitySource, and JoinSource")


def _select_expression(query: ttypes.Query, column: str) -> str:
    selects = query.selects or {}
    return selects.get(column, column)


def _cast_ts(expr: str) -> str:
    return f"cast({expr} as bigint)"


def _struct_expression(
    query: ttypes.Query,
    input_columns: List[str],
    valid_from_column: str,
    valid_to_column: str,
) -> str:
    valid_from_expr = _select_expression(query, valid_from_column)
    valid_to_expr = _select_expression(query, valid_to_column)
    fields = [
        f"{_sql_string(_VALID_FROM_FIELD)}, {_cast_ts(valid_from_expr)}",
        f"{_sql_string(_VALID_TO_FIELD)}, coalesce({_cast_ts(valid_to_expr)}, {_INFINITY_TS})",
        f"{_sql_string(_ROW_TS_FIELD)}, {_cast_ts(valid_from_expr)}",
    ]
    for column in input_columns:
        fields.append(f"{_sql_string(column)}, {_select_expression(query, column)}")
    return "named_struct(" + ", ".join(fields) + ")"


def _prepare_sources(
    sources: Union[Sequence[utils.ANY_SOURCE_TYPE], utils.ANY_SOURCE_TYPE],
    output_namespace: Optional[str],
    row_id_column: str,
    valid_from_column: str,
    valid_to_column: str,
    input_columns: List[str],
) -> List[ttypes.Source]:
    normalized_sources = deepcopy(utils.normalize_sources(sources, output_namespace))
    for source in normalized_sources:
        query = _source_query(source)
        if query is None:
            raise ValueError("SCD2 sources must have a query")
        if query.selects is None:
            query.selects = {}
        row_id_expr = _select_expression(query, row_id_column)
        query.selects[_HIDDEN_ROW_ID_COLUMN] = f"cast({row_id_expr} as string)"
        query.selects[_HIDDEN_ROW_COLUMN] = _struct_expression(
            query, input_columns, valid_from_column, valid_to_column
        )
        if query.timeColumn is None:
            query.timeColumn = _select_expression(query, valid_from_column)
    return normalized_sources


def _hidden_aggregation() -> ttypes.Aggregation:
    return group_by.Aggregation(
        input_column=_HIDDEN_ROW_COLUMN,
        operation=group_by.Operation.LAST,
        buckets=[_HIDDEN_ROW_ID_COLUMN],
    )


def _active_rows_expression(
    map_column: str,
    as_of_column: str,
    window: Optional[common.Window],
    window_column: Optional[str],
) -> str:
    predicates = [
        f"x.{_VALID_FROM_FIELD} <= {as_of_column}",
        f"{as_of_column} < x.{_VALID_TO_FIELD}",
    ]
    if window:
        window_field = window_column or _ROW_TS_FIELD
        predicates.append(f"x.{window_field} > {as_of_column} - {_window_to_millis(window)}")
    return (
        f"filter(map_values({map_column}), "
        f"x -> {' AND '.join(predicates)})"
    )


def _stateful_numeric_aggregate(
    active_rows: str,
    field: str,
    finish_expression: str,
) -> str:
    return f"""
aggregate(
  {active_rows},
  named_struct('sum', cast(0 as double), 'cnt', cast(0 as bigint)),
  (acc, x) -> named_struct(
    'sum', acc.sum + IF(x.{field} IS NULL, cast(0 as double), cast(x.{field} as double)),
    'cnt', acc.cnt + IF(x.{field} IS NULL, cast(0 as bigint), cast(1 as bigint))
  ),
  acc -> {finish_expression}
)
""".strip()


def _empty_rows_predicate(map_column: str, active_rows: str) -> str:
    return f"{map_column} IS NULL OR size({active_rows}) = 0"


def _derivation_expression(
    aggregation: Aggregation,
    map_column: str,
    as_of_column: str,
    window: Optional[common.Window],
) -> str:
    active_rows = _active_rows_expression(map_column, as_of_column, window, aggregation.window_column)
    empty_rows = _empty_rows_predicate(map_column, active_rows)
    op = aggregation.operation
    field = aggregation.input_column

    if op == Operation.COUNT:
        return f"IF({map_column} IS NULL, cast(0 as bigint), cast(size({active_rows}) as bigint))"

    if op == Operation.EXISTS:
        return f"IF({map_column} IS NULL, false, size({active_rows}) > 0)"

    if op == Operation.SUM:
        return _stateful_numeric_aggregate(
            active_rows,
            field,
            "IF(acc.cnt = 0, cast(NULL as double), acc.sum)",
        )

    if op == Operation.AVG:
        return _stateful_numeric_aggregate(
            active_rows,
            field,
            "IF(acc.cnt = 0, cast(NULL as double), acc.sum / acc.cnt)",
        )

    if op == Operation.MIN:
        return f"array_min(transform(filter({active_rows}, x -> x.{field} IS NOT NULL), x -> x.{field}))"

    if op == Operation.MAX:
        return f"array_max(transform(filter({active_rows}, x -> x.{field} IS NOT NULL), x -> x.{field}))"

    if op == Operation.COUNT_DISTINCT:
        return (
            f"IF({map_column} IS NULL, cast(0 as bigint), "
            f"cast(size(array_distinct(transform(filter({active_rows}, "
            f"x -> x.{field} IS NOT NULL), x -> x.{field}))) as bigint))"
        )

    if op in (Operation.LATEST, Operation.EARLIEST):
        comparator = "l.{0} > r.{0}".format(_ROW_TS_FIELD)
        reverse = "l.{0} < r.{0}".format(_ROW_TS_FIELD)
        if op == Operation.EARLIEST:
            comparator, reverse = reverse, comparator
        sorted_rows = (
            f"array_sort({active_rows}, "
            f"(l, r) -> CASE WHEN {comparator} THEN -1 "
            f"WHEN {reverse} THEN 1 ELSE 0 END)"
        )
        return f"IF({empty_rows}, NULL, element_at({sorted_rows}, 1).{field})"

    raise ValueError(f"Unsupported SCD2 operation {op}")


def _derivations(
    aggregations: List[Aggregation],
    map_column: str,
    as_of_column: str,
) -> List[ttypes.Derivation]:
    result = []
    for aggregation in aggregations:
        windows = aggregation.normalized_windows
        if windows:
            for window in windows:
                result.append(
                    Derivation(
                        name=_output_name(aggregation, window),
                        expression=_derivation_expression(aggregation, map_column, as_of_column, window),
                    )
                )
        else:
            result.append(
                Derivation(
                    name=_output_name(aggregation, None),
                    expression=_derivation_expression(aggregation, map_column, as_of_column, None),
                )
            )
    return result


def _input_columns(aggregations: List[Aggregation]) -> List[str]:
    result = []
    seen = set()
    for aggregation in aggregations:
        for column in (aggregation.input_column, aggregation.window_column):
            if column and column not in seen:
                result.append(column)
                seen.add(column)
    return result


def GroupBy(
    sources: Union[Sequence[utils.ANY_SOURCE_TYPE], utils.ANY_SOURCE_TYPE],
    keys: List[str],
    aggregations: List[Aggregation],
    row_id_column: str,
    valid_from_column: str,
    valid_to_column: str,
    version: Optional[int] = None,
    accuracy: ttypes.Accuracy = ttypes.Accuracy.TEMPORAL,
    output_namespace: str = None,
    table_properties: Dict[str, str] = None,
    tags: Dict[str, str] = None,
    online: bool = group_by.DEFAULT_ONLINE,
    production: bool = group_by.DEFAULT_PRODUCTION,
    offline_schedule: str = None,
    online_schedule: Optional[str] = None,
    conf: common.ConfigProperties = None,
    env_vars: common.EnvironmentVariables = None,
    cluster_conf: common.ClusterConfigProperties = None,
    step_days: int = None,
    disable_historical_backfill: bool = False,
    as_of_column: str = "ts",
) -> ttypes.GroupBy:
    assert aggregations, "SCD2 aggregations are not specified"
    for aggregation in aggregations:
        if not isinstance(aggregation, Aggregation):
            raise TypeError(
                "scd2.GroupBy expects aggregations created with ai.chronon.scd2.Aggregation"
            )
    if accuracy != ttypes.Accuracy.TEMPORAL:
        raise ValueError("scd2.GroupBy only supports TEMPORAL accuracy")

    source_input_columns = _input_columns(aggregations)
    prepared_sources = _prepare_sources(
        sources,
        output_namespace,
        row_id_column,
        valid_from_column,
        valid_to_column,
        source_input_columns,
    )
    hidden_aggregation = _hidden_aggregation()
    map_column = group_by.get_output_col_names(hidden_aggregation)[0]

    return group_by.GroupBy(
        sources=prepared_sources,
        keys=keys,
        aggregations=[hidden_aggregation],
        version=version,
        derivations=_derivations(aggregations, map_column, as_of_column),
        accuracy=ttypes.Accuracy.TEMPORAL,
        output_namespace=output_namespace,
        table_properties=table_properties,
        tags=tags,
        online=online,
        production=production,
        offline_schedule=offline_schedule,
        online_schedule=online_schedule,
        conf=conf,
        env_vars=env_vars,
        cluster_conf=cluster_conf,
        step_days=step_days,
        disable_historical_backfill=disable_historical_backfill,
    )
