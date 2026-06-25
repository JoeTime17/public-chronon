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

import warnings

import pytest

import gen_thrift.common.ttypes as common
from ai.chronon import query
from ai.chronon.staging_query import StagingQuery, TableDependency


def _days(n):
    return common.Window(length=n, timeUnit=common.TimeUnit.DAYS)


def _hours(n):
    return common.Window(length=n, timeUnit=common.TimeUnit.HOURS)


def test_nothing_set_defaults_both_sides_to_zero():
    td = TableDependency(table="ns.upstream").to_thrift()
    assert td.startOffset == _days(0)
    assert td.endOffset == _days(0)
    assert td.startCutOff is None
    assert td.endCutOff is None
    assert td.tableInfo.partitionInterval == _days(1)


def test_partition_column_without_offsets_no_longer_raises():
    # Previously validated; now defaults to [query.start, query.end].
    td = TableDependency(table="ns.upstream", partition_column="ds").to_thrift()
    assert td.startOffset == _days(0)
    assert td.endOffset == _days(0)


def test_query_partition_interval_defaults_to_subdaily_format():
    q = query.Query(partition_column="ds", partition_interval="15m")

    assert q.partitionColumn == "ds"
    assert q.partitionFormat == "yyyy-MM-dd-HH-mm"
    assert q.partitionInterval == common.Window(length=15, timeUnit=common.TimeUnit.MINUTES)


def test_query_partition_offset_is_serialized():
    q = query.Query(partition_column="ds", partition_interval="3h", partition_offset="1h")

    assert q.partitionOffset == _hours(1)


def test_table_dependency_partition_interval_is_serialized():
    td = TableDependency(table="ns.upstream", partition_column="ds", partition_interval="3h").to_thrift()

    assert td.tableInfo.partitionColumn == "ds"
    assert td.tableInfo.partitionFormat is None
    assert td.tableInfo.partitionInterval == _hours(3)


def test_time_partitioned_table_dependency_without_interval_omits_partition_interval():
    td = TableDependency(
        table="ns.upstream",
        partition_column="event_ts",
        time_partitioned=True,
    ).to_thrift()

    assert td.tableInfo.timePartitioned is True
    assert td.tableInfo.partitionInterval is None


def test_table_dependency_partition_offset_is_serialized():
    td = TableDependency(
        table="ns.upstream",
        partition_column="ds",
        partition_interval="3h",
        partition_offset="1h",
    ).to_thrift()

    assert td.tableInfo.partitionOffset == _hours(1)


def test_table_dependency_inherits_non_daily_grid_from_table_reference():
    producer = StagingQuery(
        query="SELECT 1 as user_id, 1 as event_id, 0L as ts",
        output_namespace="data",
        offline_schedule="0 1-22/3 * * *",
        partition_interval="3h",
        partition_offset="1h",
    )
    producer.metaData.name = "team.producer__1"

    td = TableDependency(table=producer.table).to_thrift()

    assert td.tableInfo.table == "data.team_producer__1"
    assert td.tableInfo.partitionColumn == "ds"
    assert td.tableInfo.partitionFormat == "yyyy-MM-dd-HH-mm"
    assert td.tableInfo.partitionInterval == _hours(3)
    assert td.tableInfo.partitionOffset == _hours(1)


def test_table_dependency_does_not_propagate_daily_table_reference():
    producer = StagingQuery(
        query="SELECT 1 as user_id, 1 as event_id, 0L as ts",
        output_namespace="data",
    )
    producer.metaData.name = "team.producer__1"

    td = TableDependency(table=producer.table).to_thrift()

    assert td.tableInfo.table == "data.team_producer__1"
    assert td.tableInfo.partitionInterval == _days(1)
    assert td.tableInfo.partitionOffset is None


def test_staging_query_partition_interval_sets_output_table_info():
    sq = StagingQuery(
        query="SELECT * FROM ns.upstream",
        dependencies=[TableDependency(table="ns.upstream", partition_interval="3h")],
        offline_schedule="0 */3 * * *",
        partition_interval="3h",
    )

    table_info = sq.metaData.executionInfo.outputTableInfo
    assert table_info.partitionColumn == "ds"
    assert table_info.partitionFormat == "yyyy-MM-dd-HH-mm"
    assert table_info.partitionInterval == _hours(3)


def test_staging_query_infers_interval_but_never_offset_from_schedule():
    # The cron fire phase (1h) is a derived processing delay over the midnight-boundary
    # grid — it is never inferred as a partition offset.
    sq = StagingQuery(
        query="SELECT * FROM ns.upstream WHERE ds BETWEEN '{{ start_date }}' AND '{{ end_date }}'",
        dependencies=[TableDependency(table="ns.upstream", partition_interval="3h")],
        offline_schedule="0 1-22/3 * * *",
    )

    table_info = sq.metaData.executionInfo.outputTableInfo
    assert table_info.partitionColumn == "ds"
    assert table_info.partitionFormat == "yyyy-MM-dd-HH-mm"
    assert table_info.partitionInterval == _hours(3)
    assert table_info.partitionOffset is None


def test_staging_query_explicit_offset_with_matching_cron_has_zero_delay():
    # Declaring partition_offset="1h" makes the grid 01:00, 04:00, ... and the
    # `0 1-22/3 * * *` fires land exactly on it (zero derived delay).
    sq = StagingQuery(
        query="SELECT * FROM ns.upstream WHERE ds BETWEEN '{{ start_date }}' AND '{{ end_date }}'",
        dependencies=[TableDependency(table="ns.upstream", partition_interval="3h")],
        offline_schedule="0 1-22/3 * * *",
        partition_offset="1h",
    )

    table_info = sq.metaData.executionInfo.outputTableInfo
    assert table_info.partitionInterval == _hours(3)
    assert table_info.partitionOffset == _hours(1)


def test_staging_query_explicit_offset_with_mismatched_cron_is_rejected():
    with pytest.raises(ValueError):
        StagingQuery(
            query="SELECT * FROM ns.upstream WHERE ds BETWEEN '{{ start_date }}' AND '{{ end_date }}'",
            dependencies=[TableDependency(table="ns.upstream", partition_interval="3h")],
            offline_schedule="0 9,17 * * *",
            partition_offset="1h",
        )


def test_staging_query_partition_interval_must_match_cron_interval():
    with pytest.raises(ValueError, match="must match"):
        StagingQuery(
            query="SELECT * FROM ns.upstream WHERE ds BETWEEN '{{ start_date }}' AND '{{ end_date }}'",
            dependencies=[TableDependency(table="ns.upstream", partition_interval="2h")],
            offline_schedule="0 */3 * * *",
            partition_interval="2h",
        )


def test_staging_query_partition_interval_matching_cron_interval_is_accepted():
    sq = StagingQuery(
        query="SELECT * FROM ns.upstream WHERE ds BETWEEN '{{ start_date }}' AND '{{ end_date }}'",
        dependencies=[TableDependency(table="ns.upstream", partition_interval="1h")],
        offline_schedule="0 * * * *",
        partition_interval="1h",
    )

    table_info = sq.metaData.executionInfo.outputTableInfo
    assert table_info.partitionInterval == _hours(1)
    assert table_info.partitionOffset is None


def test_staging_query_partition_interval_finer_than_cron_interval_is_rejected():
    with pytest.raises(ValueError, match="must match"):
        StagingQuery(
            query="SELECT * FROM ns.upstream WHERE ds BETWEEN '{{ start_date }}' AND '{{ end_date }}'",
            dependencies=[TableDependency(table="ns.upstream", partition_interval="1h")],
            offline_schedule="0 */3 * * *",
            partition_interval="1h",
        )


def test_staging_query_subdaily_rejects_dependency_without_interval():
    with pytest.raises(ValueError, match="partition_interval"):
        StagingQuery(
            query="SELECT * FROM ns.upstream WHERE ds BETWEEN '{{ start_date }}' AND '{{ end_date }}'",
            dependencies=[TableDependency(table="ns.upstream")],
            partition_interval="3h",
        )


def test_staging_query_subdaily_propagates_output_grid_to_time_partitioned_dependency_without_interval():
    sq = StagingQuery(
        query="SELECT * FROM ns.upstream WHERE ds BETWEEN '{{ start_date }}' AND '{{ end_date }}'",
        dependencies=[TableDependency(table="ns.upstream", time_partitioned=True)],
        offline_schedule="0 1-22/3 * * *",
        partition_interval="3h",
        partition_offset="1h",
    )

    dep = sq.tableDependencies[0].tableInfo
    assert dep.timePartitioned is True
    assert dep.partitionColumn == "ds"
    assert dep.partitionFormat == "yyyy-MM-dd-HH-mm"
    assert dep.partitionInterval == _hours(3)
    assert dep.partitionOffset == _hours(1)


def test_staging_query_subdaily_allows_time_partitioned_dependency_with_interval():
    sq = StagingQuery(
        query="SELECT * FROM ns.upstream WHERE ds BETWEEN '{{ start_date }}' AND '{{ end_date }}'",
        dependencies=[
            TableDependency(
                table="ns.upstream",
                time_partitioned=True,
                partition_interval="3h",
                partition_offset="1h",
            )
        ],
        offline_schedule="0 */3 * * *",
        partition_interval="3h",
    )

    assert sq.tableDependencies[0].tableInfo.timePartitioned is True
    assert sq.tableDependencies[0].tableInfo.partitionInterval == _hours(3)
    assert sq.tableDependencies[0].tableInfo.partitionOffset == _hours(1)


def test_staging_query_offset_must_be_smaller_than_interval():
    with pytest.raises(ValueError, match="strictly less"):
        StagingQuery(
            query="SELECT * FROM ns.upstream WHERE ds BETWEEN '{{ start_date }}' AND '{{ end_date }}'",
            dependencies=[TableDependency(table="ns.upstream", partition_interval="3h")],
            offline_schedule="0 */3 * * *",
            partition_offset="3h",
        )


def test_start_offset_only_sets_start_and_defaults_end_to_zero():
    td = TableDependency(table="ns.upstream", start_offset=7).to_thrift()
    assert td.startOffset == _days(7)
    assert td.endOffset == _days(0)


def test_end_offset_only_sets_end_and_defaults_start_to_zero():
    td = TableDependency(table="ns.upstream", end_offset=1).to_thrift()
    assert td.startOffset == _days(0)
    assert td.endOffset == _days(1)


def test_asymmetric_start_and_end_offsets():
    td = TableDependency(table="ns.upstream", start_offset=30, end_offset=1).to_thrift()
    assert td.startOffset == _days(30)
    assert td.endOffset == _days(1)


def test_start_cutoff_alone_pins_start_and_defaults_end_to_zero():
    # startOffset is None so the planner resolves start = max(null, cutoff) = cutoff.
    td = TableDependency(table="ns.upstream", start_cutoff="2024-01-01").to_thrift()
    assert td.startOffset is None
    assert td.endOffset == _days(0)
    assert td.startCutOff == "2024-01-01"
    assert td.endCutOff is None


def test_end_cutoff_alone_clamps_end_with_zero_offset():
    # No pin-on-end: null endOffset breaks downstream planners, so end always clamps.
    td = TableDependency(table="ns.upstream", end_cutoff="2024-12-31").to_thrift()
    assert td.startOffset == _days(0)
    assert td.endOffset == _days(0)
    assert td.endCutOff == "2024-12-31"


def test_start_offset_combines_with_start_cutoff_for_clamp_with_floor():
    td = TableDependency(
        table="ns.upstream", start_offset=7, start_cutoff="2024-01-01"
    ).to_thrift()
    assert td.startOffset == _days(7)
    assert td.startCutOff == "2024-01-01"


def test_end_offset_combines_with_end_cutoff():
    td = TableDependency(
        table="ns.upstream", end_offset=1, end_cutoff="2024-12-31"
    ).to_thrift()
    assert td.endOffset == _days(1)
    assert td.endCutOff == "2024-12-31"


def test_deprecated_offset_applies_to_both_sides():
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        td = TableDependency(table="ns.upstream", offset=3).to_thrift()
    assert td.startOffset == _days(3)
    assert td.endOffset == _days(3)
    assert any(issubclass(w.category, DeprecationWarning) for w in caught)


def test_start_offset_overrides_deprecated_offset_on_start_side():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        td = TableDependency(
            table="ns.upstream", offset=5, start_offset=1
        ).to_thrift()
    assert td.startOffset == _days(1)
    assert td.endOffset == _days(5)


def test_end_offset_overrides_deprecated_offset_on_end_side():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        td = TableDependency(
            table="ns.upstream", offset=5, end_offset=0
        ).to_thrift()
    assert td.startOffset == _days(5)
    assert td.endOffset == _days(0)


def test_deprecated_offset_with_start_cutoff_clamps_not_pins():
    # offset wins the start-side fallback over the cutoff-implied pin default.
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        td = TableDependency(
            table="ns.upstream", offset=7, start_cutoff="2024-01-01"
        ).to_thrift()
    assert td.startOffset == _days(7)
    assert td.startCutOff == "2024-01-01"


def test_deprecated_offset_zero_still_warns():
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        TableDependency(table="ns.upstream", offset=0).to_thrift()
    assert any(issubclass(w.category, DeprecationWarning) for w in caught)


def test_end_cutoff_passthrough_with_zero_end_offset():
    td = TableDependency(
        table="ns.upstream", end_offset=0, end_cutoff="2024-12-31"
    ).to_thrift()
    assert td.endCutOff == "2024-12-31"
    assert td.endOffset == _days(0)
