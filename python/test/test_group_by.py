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


import pytest

import gen_thrift.common.ttypes as common
from ai.chronon import group_by, query, source
from ai.chronon.staging_query import StagingQuery
from gen_thrift.api import ttypes


@pytest.fixture
def sum_op():
    return ttypes.Operation.SUM


@pytest.fixture
def min_op():
    return ttypes.Operation.MIN


@pytest.fixture
def days_unit():
    return common.TimeUnit.DAYS


@pytest.fixture
def hours_unit():
    return common.TimeUnit.HOURS


def event_source(table):
    """
    Sample left join
    """
    return ttypes.EventSource(
        table=table,
        query=ttypes.Query(
            startPartition="2020-04-09",
            selects={"subject": "subject_sql", "event_id": "event_sql", "cnt": 1},
            timeColumn="CAST(ts AS DOUBLE)",
        ),
    )


def entity_source(snapshotTable, mutationTable):
    """
    Sample source
    """
    return ttypes.EntitySource(
        snapshotTable=snapshotTable,
        mutationTable=mutationTable,
        query=ttypes.Query(
            startPartition="2020-04-09",
            selects={"subject": "subject_sql", "event_id": "event_sql", "cnt": 1},
            timeColumn="CAST(ts AS DOUBLE)",
            mutationTimeColumn="__mutationTs",
            reversalColumn="is_reverse",
        ),
    )


def test_pretty_window_str(days_unit, hours_unit):
    """
    Test pretty window utils.
    """
    window = common.Window(length=7, timeUnit=days_unit)
    assert group_by.window_to_str_pretty(window) == "7 days"
    window = common.Window(length=2, timeUnit=hours_unit)
    assert group_by.window_to_str_pretty(window) == "2 hours"


def test_pretty_operation_str(sum_op, min_op):
    """
    Test pretty operation util.
    """
    assert group_by.op_to_str(sum_op) == "sum"
    assert group_by.op_to_str(min_op) == "min"


def test_select():
    """
    Test select builder
    """
    assert query.selects("subject", event="event_expr") == {
        "subject": "subject",
        "event": "event_expr",
    }


def test_contains_windowed_aggregation(sum_op, min_op, days_unit):
    """
    Test checker for windowed aggregations
    """
    assert not group_by.contains_windowed_aggregation([])
    aggregations = [
        ttypes.Aggregation(inputColumn="event", operation=sum_op),
        ttypes.Aggregation(inputColumn="event", operation=min_op),
    ]
    assert not group_by.contains_windowed_aggregation(aggregations)
    aggregations.append(
        ttypes.Aggregation(
            inputColumn="event",
            operation=sum_op,
            windows=[common.Window(length=7, timeUnit=days_unit)],
        )
    )
    assert group_by.contains_windowed_aggregation(aggregations)


def test_validator_ok():
    gb = group_by.GroupBy(
        sources=event_source("table"),
        keys=["subject"],
        aggregations=group_by.Aggregations(
            random=ttypes.Aggregation(
                inputColumn="event_id", operation=ttypes.Operation.SUM
            ),
            event_id=ttypes.Aggregation(operation=ttypes.Operation.LAST),
            cnt=ttypes.Aggregation(operation=ttypes.Operation.COUNT),
            percentile=group_by.Aggregation(
                input_column="event_id",
                operation=group_by.Operation.APPROX_PERCENTILE([0.5, 0.75]),
            ),
        ),
        version=0,
    )
    assert all(
        [
            agg.inputColumn
            for agg in gb.aggregations
            if agg.operation != ttypes.Operation.COUNT
        ]
    )
    group_by.validate_group_by(gb)
    with pytest.raises(ValueError):
        group_by.GroupBy(
            sources=event_source("table"),
            keys=["subject"],
            aggregations=group_by.Aggregations(
                percentile=group_by.Aggregation(
                    input_column="event_id",
                    operation=group_by.Operation.APPROX_PERCENTILE([1.5]),
                ),
            ),
            version=0
        )
    with pytest.raises(AssertionError):
        group_by.GroupBy(
            sources=event_source("table"),
            keys=["subject"],
            aggregations=None,
            version=0,
        )
    with pytest.raises(AssertionError):
        group_by.GroupBy(
            sources=entity_source("table", "mutationTable"),
            keys=["subject"],
            aggregations=None,
            version=0
        )
    group_by.GroupBy(
        sources=entity_source("table", None),
        keys=["subject"],
        aggregations=None,
        version=0,
    )


def subdaily_event_source(table):
    """Source declaring a 3h grid - sub-daily groupBys need sources whose boundaries the
    output grid sits on."""
    return ttypes.EventSource(
        table=table,
        query=query.Query(
            selects={"subject": "subject_sql", "event_id": "event_sql"},
            time_column="ts",
            partition_interval="3h",
        ),
    )


def test_partition_interval_sets_output_table_info():
    gb = group_by.GroupBy(
        sources=subdaily_event_source("table"),
        keys=["subject"],
        aggregations=group_by.Aggregations(
            cnt=ttypes.Aggregation(operation=ttypes.Operation.COUNT),
        ),
        version=0,
        partition_interval="3h",
    )

    table_info = gb.metaData.executionInfo.outputTableInfo
    assert table_info.partitionColumn == "ds"
    assert table_info.partitionFormat == "yyyy-MM-dd-HH-mm"
    assert table_info.partitionInterval.length == 3
    assert table_info.partitionInterval.timeUnit == common.TimeUnit.HOURS


def test_event_source_inherits_non_daily_grid_from_table_reference():
    producer = StagingQuery(
        query="SELECT 1 as subject, 1 as event_id, 0L as ts",
        output_namespace="data",
        offline_schedule="0 1-22/3 * * *",
        partition_interval="3h",
        partition_offset="1h",
    )
    producer.metaData.name = "team.producer__1"

    src = source.EventSource(
        table=producer.table,
        query=query.Query(
            selects={"subject": "subject_sql", "event_id": "event_sql"},
            time_column="ts",
        ),
    )

    assert src.events.table == "data.team_producer__1"
    assert src.events.query.partitionColumn == "ds"
    assert src.events.query.partitionFormat == "yyyy-MM-dd-HH-mm"
    assert src.events.query.partitionInterval == common.Window(length=3, timeUnit=common.TimeUnit.HOURS)
    assert src.events.query.partitionOffset == common.Window(length=1, timeUnit=common.TimeUnit.HOURS)


def test_event_source_explicit_grid_wins_over_table_reference():
    producer = StagingQuery(
        query="SELECT 1 as subject, 1 as event_id, 0L as ts",
        output_namespace="data",
        offline_schedule="0 1-22/3 * * *",
        partition_interval="3h",
        partition_offset="1h",
    )
    producer.metaData.name = "team.producer__1"

    src = source.EventSource(
        table=producer.table,
        query=query.Query(
            selects={"subject": "subject_sql", "event_id": "event_sql"},
            time_column="ts",
            partition_interval="6h",
        ),
    )

    assert src.events.query.partitionInterval == common.Window(length=6, timeUnit=common.TimeUnit.HOURS)
    assert src.events.query.partitionOffset is None


def test_generic_collector():
    aggregation = group_by.Aggregation(
        input_column="test", operation=group_by.Operation.APPROX_PERCENTILE([0.4, 0.2])
    )
    assert aggregation.argMap == {"k": "20", "percentiles": "[0.4, 0.2]"}


def test_select_sanitization():
    gb = group_by.GroupBy(
        sources=[
            ttypes.EventSource(  # No selects are spcified
                table="event_table1", query=query.Query(selects=None, time_column="ts")
            ),
            ttypes.EntitySource(  # Some selects are specified
                snapshotTable="entity_table1",
                query=query.Query(
                    selects={"key1": "key1_sql", "event_id": "event_sql"}
                ),
            ),
        ],
        keys=["key1", "key2"],
        aggregations=group_by.Aggregations(
            random=ttypes.Aggregation(
                inputColumn="event_id", operation=ttypes.Operation.SUM
            ),
            event_id=ttypes.Aggregation(operation=ttypes.Operation.LAST),
            cnt=ttypes.Aggregation(operation=ttypes.Operation.COUNT),
        ),
        version=0,
    )
    required_selects = set(["key1", "key2", "event_id", "cnt"])
    assert set(gb.sources[0].events.query.selects.keys()) == required_selects
    assert set(gb.sources[0].events.query.selects.values()) == required_selects
    assert set(gb.sources[1].entities.query.selects.keys()) == required_selects
    assert set(gb.sources[1].entities.query.selects.values()) == set(
        ["key1_sql", "key2", "event_sql", "cnt"]
    )


def test_tuple_sources_are_normalized():
    gb = group_by.GroupBy(
        sources=(event_source("event_table1"), event_source("event_table2")),
        keys=["subject"],
        aggregations=group_by.Aggregations(
            event_id=ttypes.Aggregation(operation=ttypes.Operation.LAST),
            cnt=ttypes.Aggregation(operation=ttypes.Operation.COUNT),
        ),
        version=0,
    )

    assert [source.events.table for source in gb.sources] == [
        "event_table1",
        "event_table2",
    ]


def test_snapshot_with_hour_aggregation():
    with pytest.raises(AssertionError):
        group_by.GroupBy(
            sources=[
                ttypes.EntitySource(  # Some selects are specified
                    snapshotTable="entity_table1",
                    query=query.Query(
                        selects={"key1": "key1_sql", "event_id": "event_sql"},
                        time_column="ts",
                    ),
                )
            ],
            keys=["key1"],
            aggregations=group_by.Aggregations(
                random=ttypes.Aggregation(
                    inputColumn="event_id",
                    operation=ttypes.Operation.SUM,
                    windows=[
                        common.Window(1, common.TimeUnit.HOURS),
                    ],
                ),
            ),
            accuracy=group_by.Accuracy.SNAPSHOT,
            version=0,
        )


def _subdaily_entity_source():
    # source declares the same 3h grid as the groupBy so source-grid validation stays satisfied
    return ttypes.EntitySource(
        snapshotTable="entity_table1",
        query=query.Query(
            selects={"key1": "key1_sql", "event_id": "event_sql"},
            time_column="ts",
            partition_interval="3h",
        ),
    )


def test_snapshot_subdaily_window_allowed_on_subdaily_grid():
    # mirrors the scala parity fixture (SNAPSHOT accuracy, 6h window, 3h grid): snapshot
    # accuracy on a sub-daily grid IS fine enough for windows that are multiples of the grid
    gb = group_by.GroupBy(
        sources=[_subdaily_entity_source()],
        keys=["key1"],
        aggregations=group_by.Aggregations(
            random=ttypes.Aggregation(
                inputColumn="event_id",
                operation=ttypes.Operation.SUM,
                windows=[
                    common.Window(6, common.TimeUnit.HOURS),
                    common.Window(1, common.TimeUnit.DAYS),
                ],
            ),
        ),
        accuracy=group_by.Accuracy.SNAPSHOT,
        partition_interval="3h",
        offline_schedule="0 */3 * * *",
        version=0,
    )
    assert gb is not None


def test_snapshot_window_must_be_multiple_of_grid():
    # a 4h window on a 3h grid cannot be resolved by on-boundary snapshots
    with pytest.raises(AssertionError, match="multiple"):
        group_by.GroupBy(
            sources=[_subdaily_entity_source()],
            keys=["key1"],
            aggregations=group_by.Aggregations(
                random=ttypes.Aggregation(
                    inputColumn="event_id",
                    operation=ttypes.Operation.SUM,
                    windows=[
                        common.Window(4, common.TimeUnit.HOURS),
                    ],
                ),
            ),
            accuracy=group_by.Accuracy.SNAPSHOT,
            partition_interval="3h",
            offline_schedule="0 */3 * * *",
            version=0,
        )


def test_subdaily_group_by_rejects_undeclared_sources():
    # an undeclared source is implicitly daily: a sub-daily groupBy over it would land a day
    # late, permanently - the coarse-source-under-fine-output trap, caught at authoring time
    with pytest.raises(ValueError, match="partition_interval"):
        group_by.GroupBy(
            sources=[event_source("table")],
            keys=["subject"],
            aggregations=group_by.Aggregations(
                random=ttypes.Aggregation(
                    inputColumn="event_id", operation=ttypes.Operation.SUM
                ),
            ),
            partition_interval="3h",
            offline_schedule="0 */3 * * *",
            version=0,
        )


def test_subdaily_group_by_allows_time_partitioned_source_without_interval():
    src = ttypes.EventSource(
        table="table",
        query=query.Query(
            selects={"subject": "subject_sql", "event_id": "event_sql"},
            time_column="ts",
            time_partitioned=True,
        ),
    )
    gb = group_by.GroupBy(
        sources=[src],
        keys=["subject"],
        aggregations=group_by.Aggregations(
            random=ttypes.Aggregation(inputColumn="event_id", operation=ttypes.Operation.SUM),
        ),
        partition_interval="3h",
        offline_schedule="0 */3 * * *",
        version=0,
    )
    assert gb.sources[0].events.query.timePartitioned is True
    assert gb.sources[0].events.query.partitionInterval is None


def test_subdaily_group_by_allows_time_partitioned_source_with_interval():
    src = ttypes.EventSource(
        table="table",
        query=query.Query(
            selects={"subject": "subject_sql", "event_id": "event_sql"},
            time_column="ts",
            time_partitioned=True,
            partition_interval="3h",
            partition_offset="1h",
        ),
    )
    gb = group_by.GroupBy(
        sources=[src],
        keys=["subject"],
        aggregations=group_by.Aggregations(
            random=ttypes.Aggregation(inputColumn="event_id", operation=ttypes.Operation.SUM),
        ),
        partition_interval="3h",
        offline_schedule="0 */3 * * *",
        version=0,
    )
    assert gb is not None
    assert gb.sources[0].events.query.partitionOffset == common.Window(
        length=1, timeUnit=common.TimeUnit.HOURS
    )


def test_additional_metadata():
    gb = group_by.GroupBy(
        sources=[
            ttypes.EventSource(
                table="event_table1", query=query.Query(selects=None, time_column="ts")
            )
        ],
        keys=["key1", "key2"],
        aggregations=[
            group_by.Aggregation(
                input_column="event_id", operation=ttypes.Operation.SUM
            )
        ],
        tags={"to_deprecate": "true"},
        version=0,
    )
    assert gb.metaData.tags["to_deprecate"]


def test_windows_as_strings():
    gb = group_by.GroupBy(
        sources=[
            ttypes.EventSource(
                table="event_table1", query=query.Query(selects=None, time_column="ts")
            )
        ],
        keys=["key1", "key2"],
        aggregations=[
            group_by.Aggregation(
                input_column="event_id",
                operation=ttypes.Operation.SUM,
                windows=["1h", "30d"],
            )
        ],
        tags={"to_deprecate": "true"},
        version=0
    )

    windows = gb.aggregations[0].windows

    assert len(windows) == 2
    assert windows[0] == common.Window(1, common.TimeUnit.HOURS)
    assert windows[1] == common.Window(30, common.TimeUnit.DAYS)

    assert gb.metaData.tags["to_deprecate"]

def test_query_api_obj():
    selects_map = {
        "key1": "key1",
        "event_id": "event_id",
        "ts": "ts",
        "mutationTs": "mutationTs",
        "ds": "ds",
    }

    wheres = ["key1 = 1"]
    start_partition = "2020-04-09"
    end_partition = "2020-04-10"
    time_column = "ts"
    mutation_time_column = "mutationTs"
    partition_column = "ds"
    partition_format = "yyyy-MM-dd"
    sub_partitions_to_wait_for = ["hr=23:00"]

    query_obj = query.Query(
        selects=selects_map,
        wheres=wheres,
        start_partition=start_partition,
        end_partition=end_partition,
        time_column=time_column,
        mutation_time_column=mutation_time_column,
        partition_column=partition_column,
        partition_format=partition_format,
        sub_partitions_to_wait_for=sub_partitions_to_wait_for
    )

    assert query_obj.selects == selects_map
    assert query_obj.wheres == wheres
    assert query_obj.startPartition == start_partition
    assert query_obj.endPartition == end_partition
    assert query_obj.timeColumn == time_column
    assert query_obj.mutationTimeColumn == mutation_time_column
    assert query_obj.partitionColumn == partition_column
    assert query_obj.partitionFormat == partition_format
    assert query_obj.subPartitionsToWaitFor == sub_partitions_to_wait_for


def test_query_with_time_partitioned_flag():
    """
    Test Query with time_partitioned flag for tables using timestamp/date columns
    instead of traditional Hive-style string partitioning.
    """
    query_obj = query.Query(
        selects={"key1": "key1", "value": "value"},
        time_column="UNIX_TIMESTAMP(created_at) * 1000",
        partition_column="created_at",
        time_partitioned=True,
    )

    assert query_obj.timePartitioned is True
    assert query_obj.partitionColumn == "created_at"


def test_query_without_time_partitioned_flag():
    """
    Test Query without time_partitioned flag (default Hive-style partitioning).
    """
    query_obj = query.Query(
        selects={"key1": "key1"},
        partition_column="ds",
    )

    assert query_obj.partitionColumn == "ds"
    assert query_obj.timePartitioned is None  # Not set

def test_online_schedule_validation():
    """Test that online_schedule validation works correctly."""
    # Test that online_schedule cannot be set when online=False
    with pytest.raises(ValueError, match="online_schedule cannot be set when online=False"):
        group_by.GroupBy(
            sources=event_source("table"),
            keys=["subject"],
            aggregations=group_by.Aggregations(
                count=group_by.Aggregation(
                    input_column="event_id",
                    operation=group_by.Operation.COUNT
                ),
            ),
            version=1,
            online=False,
            online_schedule="@daily"  # This should raise an error
        )

    # Test that online_schedule can be None when online=False
    gb = group_by.GroupBy(
        sources=event_source("table"),
        keys=["subject"],
        aggregations=group_by.Aggregations(
            count=group_by.Aggregation(
                input_column="event_id",
                operation=group_by.Operation.COUNT
            ),
        ),
        version=1,
        online=False,
        online_schedule=None  # This should be fine
    )
    assert gb.metaData.executionInfo.onlineSchedule is None

    # Test that online_schedule defaults to @daily when online=True and not specified
    gb = group_by.GroupBy(
        sources=event_source("table"),
        keys=["subject"],
        aggregations=group_by.Aggregations(
            count=group_by.Aggregation(
                input_column="event_id",
                operation=group_by.Operation.COUNT
            ),
        ),
        version=1,
        online=True,
        online_schedule=None  # Should default to @daily
    )
    assert gb.metaData.executionInfo.onlineSchedule == "@daily"

    # offline @never disables only offline scheduling; online=True still gets the normal default.
    gb = group_by.GroupBy(
        sources=event_source("table"),
        keys=["subject"],
        aggregations=group_by.Aggregations(
            count=group_by.Aggregation(
                input_column="event_id",
                operation=group_by.Operation.COUNT
            ),
        ),
        version=1,
        online=True,
        offline_schedule="@never",
        online_schedule=None,
    )
    assert gb.metaData.executionInfo.offlineSchedule == "@never"
    assert gb.metaData.executionInfo.onlineSchedule == "@daily"

    # Test that online_schedule can be explicitly set when online=True
    gb = group_by.GroupBy(
        sources=event_source("table"),
        keys=["subject"],
        aggregations=group_by.Aggregations(
            count=group_by.Aggregation(
                input_column="event_id",
                operation=group_by.Operation.COUNT
            ),
        ),
        version=1,
        online=True,
        online_schedule="0 2 * * *"  # Custom schedule
    )
    assert gb.metaData.executionInfo.onlineSchedule == "0 2 * * *"

    # Existing daily workflows may use different cron offsets for offline and online jobs.
    gb = group_by.GroupBy(
        sources=event_source("table"),
        keys=["subject"],
        aggregations=group_by.Aggregations(
            count=group_by.Aggregation(
                input_column="event_id",
                operation=group_by.Operation.COUNT
            ),
        ),
        version=1,
        online=True,
        offline_schedule="0 4 * * *",
        online_schedule="0 3 * * *",
    )
    assert gb.metaData.executionInfo.onlineSchedule == "0 3 * * *"

    with pytest.raises(ValueError, match="sub-daily partition_interval"):
        group_by.GroupBy(
            sources=event_source("table"),
            keys=["subject"],
            aggregations=group_by.Aggregations(
                count=group_by.Aggregation(
                    input_column="event_id",
                    operation=group_by.Operation.COUNT
                ),
            ),
            version=1,
            online=True,
            offline_schedule="0 */3 * * *",
            online_schedule="30 */3 * * *",
            partition_interval="3h",
        )

    # Test that @never disables online scheduling even when online=True
    gb = group_by.GroupBy(
        sources=event_source("table"),
        keys=["subject"],
        aggregations=group_by.Aggregations(
            count=group_by.Aggregation(
                input_column="event_id",
                operation=group_by.Operation.COUNT
            ),
        ),
        version=1,
        online=True,
        online_schedule="@never"
    )
    assert gb.metaData.executionInfo.onlineSchedule is None

    # Test that @never is accepted even when online=False
    gb = group_by.GroupBy(
        sources=event_source("table"),
        keys=["subject"],
        aggregations=group_by.Aggregations(
            count=group_by.Aggregation(
                input_column="event_id",
                operation=group_by.Operation.COUNT
            ),
        ),
        version=1,
        online=False,
        online_schedule="@never"
    )
    assert gb.metaData.executionInfo.onlineSchedule is None


def _online_group_by(sources=None, **kwargs):
    return group_by.GroupBy(
        sources=sources if sources is not None else event_source("table"),
        keys=["subject"],
        aggregations=group_by.Aggregations(
            count=group_by.Aggregation(
                input_column="event_id",
                operation=group_by.Operation.COUNT
            ),
        ),
        version=1,
        online=True,
        **kwargs,
    )


def test_online_schedule_keeps_daily_default_for_custom_daily_offline():
    # A custom daily offline cron must NOT be inherited by online_schedule —
    # that would churn semantic hashes for every existing conf.
    gb = _online_group_by(offline_schedule="0 4 * * *")
    assert gb.metaData.executionInfo.onlineSchedule == "@daily"


def test_online_schedule_inherits_subdaily_offline():
    gb = _online_group_by(sources=subdaily_event_source("table"), offline_schedule="0 */3 * * *")
    assert gb.metaData.executionInfo.onlineSchedule == "0 */3 * * *"


def test_online_schedule_mismatch_rejected_for_inferred_subdaily():
    # The online-must-match-offline check fires even without an explicit
    # partition_interval, when the sub-daily interval is inferred from the schedule.
    with pytest.raises(ValueError, match="online_schedule must match offline_schedule"):
        _online_group_by(
            offline_schedule="0 */3 * * *",
            online_schedule="30 */3 * * *",
        )
