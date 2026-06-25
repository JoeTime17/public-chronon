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
from ai.chronon import join
from gen_thrift.api import ttypes as api


def event_source(table):
    """
    Sample left join
    """
    return api.Source(
        events=api.EventSource(
            table=table,
            query=api.Query(
                startPartition="2020-04-09",
                selects={
                    "subject": "subject_sql",
                    "event_id": "event_sql",
                },
                timeColumn="CAST(ts AS DOUBLE)",
            ),
        ),
    )


def subdaily_event_source(table):
    """Left source declaring a 3h grid - sub-daily joins need a left on whose boundaries the
    join grid sits (the left feeds the join's own partitions)."""
    return api.Source(
        events=api.EventSource(
            table=table,
            query=api.Query(
                startPartition="2020-04-09",
                selects={
                    "subject": "subject_sql",
                    "event_id": "event_sql",
                },
                timeColumn="CAST(ts AS DOUBLE)",
                partitionInterval=common.Window(length=3, timeUnit=common.TimeUnit.HOURS),
            ),
        ),
    )


def right_part(source):
    """
    Sample Agg
    """
    return api.JoinPart(
        groupBy=api.GroupBy(
            sources=[source],
            keyColumns=["subject"],
            aggregations=[],
            accuracy=api.Accuracy.SNAPSHOT,
        ),
    )


def scheduled_subdaily_right_part(schedule=None):
    execution_info = common.ExecutionInfo(
        offlineSchedule=schedule,
        outputTableInfo=common.TableInfo(
            table="data.subdaily_right",
            partitionColumn="ds",
            partitionFormat="yyyy-MM-dd-HH-mm",
            partitionInterval=common.Window(length=3, timeUnit=common.TimeUnit.HOURS),
        ),
    )
    return api.JoinPart(
        groupBy=api.GroupBy(
            sources=[subdaily_event_source("table")],
            keyColumns=["subject"],
            aggregations=[],
            accuracy=api.Accuracy.SNAPSHOT,
            metaData=api.MetaData(
                name="group_bys.test.subdaily_right",
                executionInfo=execution_info,
            ),
        ),
    )


def test_online_schedule_validation():
    """Test that online_schedule validation works correctly for joins."""
    # Test that online_schedule cannot be set when online=False
    with pytest.raises(ValueError, match="online_schedule cannot be set when online=False"):
        join.Join(
            left=event_source("table"),
            right_parts=[right_part(event_source("table"))],
            version=1,
            row_ids=["id"],
            online=False,
            online_schedule="@daily"  # This should raise an error
        )

    # Test that online_schedule can be None when online=False
    j = join.Join(
        left=event_source("table"),
        right_parts=[right_part(event_source("table"))],
        version=1,
        row_ids=["id"],
        online=False,
        online_schedule=None  # This should be fine
    )
    assert j.metaData.executionInfo.onlineSchedule is None

    # Test that online_schedule defaults to @daily when online=True and not specified
    j = join.Join(
        left=event_source("table"),
        right_parts=[right_part(event_source("table"))],
        version=1,
        row_ids=["id"],
        online=True,
        online_schedule=None  # Should default to @daily
    )
    assert j.metaData.executionInfo.onlineSchedule == "@daily"

    # offline @never disables only offline scheduling; online=True still gets the normal default.
    j = join.Join(
        left=event_source("table"),
        right_parts=[right_part(event_source("table"))],
        version=1,
        row_ids=["id"],
        online=True,
        offline_schedule="@never",
        online_schedule=None,
    )
    assert j.metaData.executionInfo.offlineSchedule == "@never"
    assert j.metaData.executionInfo.onlineSchedule == "@daily"

    # Test that online_schedule can be explicitly set when online=True
    j = join.Join(
        left=event_source("table"),
        right_parts=[right_part(event_source("table"))],
        version=1,
        row_ids=["id"],
        online=True,
        offline_schedule="0 2 * * *",
        online_schedule="0 2 * * *"  # Custom schedule
    )
    assert j.metaData.executionInfo.onlineSchedule == "0 2 * * *"

    # Existing daily workflows may use different cron offsets for offline and online jobs.
    j = join.Join(
        left=event_source("table"),
        right_parts=[right_part(event_source("table"))],
        version=1,
        row_ids=["id"],
        online=True,
        offline_schedule="0 4 * * *",
        online_schedule="0 3 * * *",
    )
    assert j.metaData.executionInfo.onlineSchedule == "0 3 * * *"

    with pytest.raises(ValueError, match="sub-daily partition_interval"):
        join.Join(
            left=event_source("table"),
            right_parts=[right_part(event_source("table"))],
            version=1,
            row_ids=["id"],
            online=True,
            offline_schedule="0 */3 * * *",
            online_schedule="30 */3 * * *",
            partition_interval="3h",
        )

    # Test that @never disables online scheduling even when online=True
    j = join.Join(
        left=event_source("table"),
        right_parts=[right_part(event_source("table"))],
        version=1,
        row_ids=["id"],
        online=True,
        online_schedule="@never"
    )
    assert j.metaData.executionInfo.onlineSchedule is None

    # Test that @never is accepted even when online=False
    j = join.Join(
        left=event_source("table"),
        right_parts=[right_part(event_source("table"))],
        version=1,
        row_ids=["id"],
        online=False,
        online_schedule="@never"
    )
    assert j.metaData.executionInfo.onlineSchedule is None


def _online_join(left=None, **kwargs):
    return join.Join(
        left=left if left is not None else event_source("table"),
        right_parts=[right_part(event_source("table"))],
        version=1,
        row_ids=["id"],
        online=True,
        **kwargs,
    )


def test_online_schedule_keeps_daily_default_for_custom_daily_offline():
    # A custom daily offline cron must NOT be inherited by online_schedule —
    # that would churn semantic hashes for every existing conf.
    j = _online_join(offline_schedule="0 4 * * *")
    assert j.metaData.executionInfo.onlineSchedule == "@daily"


def test_online_schedule_inherits_subdaily_offline():
    j = _online_join(left=subdaily_event_source("table"), offline_schedule="0 */3 * * *")
    assert j.metaData.executionInfo.onlineSchedule == "0 */3 * * *"


def test_online_schedule_mismatch_rejected_for_inferred_subdaily():
    # The online-must-match-offline check fires even without an explicit
    # partition_interval, when the sub-daily interval is inferred from the schedule.
    with pytest.raises(ValueError, match="online_schedule must match offline_schedule"):
        _online_join(
            offline_schedule="0 */3 * * *",
            online_schedule="30 */3 * * *",
        )


def test_partition_interval_sets_output_table_info():
    j = join.Join(
        left=subdaily_event_source("table"),
        right_parts=[right_part(event_source("table"))],
        version=1,
        row_ids=["id"],
        offline_schedule="0 */3 * * *",
        partition_interval="3h",
    )

    table_info = j.metaData.executionInfo.outputTableInfo
    assert table_info.partitionColumn == "ds"
    assert table_info.partitionFormat == "yyyy-MM-dd-HH-mm"
    assert table_info.partitionInterval.length == 3
    assert table_info.partitionInterval.timeUnit == common.TimeUnit.HOURS


def test_subdaily_join_rejects_undeclared_left():
    # an undeclared left is implicitly daily: a sub-daily join over it lands a day late
    with pytest.raises(ValueError, match="partition_interval"):
        join.Join(
            left=event_source("table"),
            right_parts=[right_part(event_source("table"))],
            version=1,
            row_ids=["id"],
            partition_interval="3h",
        )


def test_subdaily_join_allows_time_partitioned_left_without_interval():
    left = event_source("table")
    left.events.query.timePartitioned = True

    j = join.Join(
        left=left,
        right_parts=[right_part(event_source("table"))],
        version=1,
        row_ids=["id"],
        offline_schedule="0 */3 * * *",
        partition_interval="3h",
    )

    assert j.left.events.query.timePartitioned is True
    assert j.left.events.query.partitionInterval is None


def test_subdaily_join_allows_time_partitioned_left_with_interval_and_offset():
    left = event_source("table")
    left.events.query.timePartitioned = True
    left.events.query.partitionInterval = common.Window(
        length=3, timeUnit=common.TimeUnit.HOURS
    )
    left.events.query.partitionOffset = common.Window(length=1, timeUnit=common.TimeUnit.HOURS)

    j = join.Join(
        left=left,
        right_parts=[right_part(event_source("table"))],
        version=1,
        row_ids=["id"],
        offline_schedule="0 */3 * * *",
        partition_interval="3h",
    )

    assert j.left.events.query.partitionOffset == common.Window(
        length=1, timeUnit=common.TimeUnit.HOURS
    )


def test_subdaily_join_right_parts_stay_unvalidated():
    # right parts pick the latest snapshot per left-row ts on their own grid: a daily-cadence part under
    # a sub-daily join is the product (mixed hourly/daily/realtime features), never an error
    j = join.Join(
        left=subdaily_event_source("table"),
        right_parts=[right_part(event_source("table"))],
        version=1,
        row_ids=["id"],
        offline_schedule="0 */3 * * *",
        partition_interval="3h",
    )
    assert j is not None


def test_online_join_rejects_subdaily_right_part_without_schedule():
    with pytest.raises(ValueError, match="regular sub-daily offline_schedule or online_schedule"):
        join.Join(
            left=event_source("table"),
            right_parts=[scheduled_subdaily_right_part()],
            version=1,
            row_ids=["id"],
            online=True,
        )


def test_online_join_accepts_scheduled_subdaily_right_part():
    j = join.Join(
        left=event_source("table"),
        right_parts=[scheduled_subdaily_right_part(schedule="0 */3 * * *")],
        version=1,
        row_ids=["id"],
        online=True,
    )

    assert j is not None
