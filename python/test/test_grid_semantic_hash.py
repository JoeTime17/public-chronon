"""
Tests that the output partition grid (interval + offset) participates in compile-time
in-place change detection, while the schedule (delay) and everything else in metaData
stays excluded -- so existing daily confs never churn.
"""

import gen_thrift.api.ttypes as ttypes
import gen_thrift.common.ttypes as common
from ai.chronon.cli.compile.conf_validator import ConfValidator


def _group_by(interval=None, offset=None, schedule="@daily"):
    table_info = None
    if interval is not None:
        table_info = common.TableInfo(partitionInterval=interval, partitionOffset=offset)
    exec_info = common.ExecutionInfo(offlineSchedule=schedule, outputTableInfo=table_info)
    meta = ttypes.MetaData(name="test_team.test_gb", executionInfo=exec_info)
    return ttypes.GroupBy(metaData=meta, keyColumns=["k"])


def _has_diff(obj, old_obj):
    # _has_diff only touches its arguments, so an unbound call keeps the test free of
    # repo-scanning fixture setup
    return ConfValidator._has_diff(None, obj, old_obj)


def _hours(n):
    return common.Window(length=n, timeUnit=common.TimeUnit.HOURS)


def _minutes(n):
    return common.Window(length=n, timeUnit=common.TimeUnit.MINUTES)


def _days(n):
    return common.Window(length=n, timeUnit=common.TimeUnit.DAYS)


class TestGridInDiffDetection:
    def test_daily_and_unset_grids_are_identical(self):
        # Window(1, DAYS), Window(24, HOURS) and no declaration all mean the same daily
        # grid: none of them may read as an in-place change against the others
        assert not _has_diff(_group_by(), _group_by(interval=_days(1)))
        assert not _has_diff(_group_by(), _group_by(interval=_hours(24)))

    def test_grid_change_is_a_diff(self):
        assert _has_diff(_group_by(interval=_hours(3)), _group_by())
        assert _has_diff(_group_by(interval=_hours(3)), _group_by(interval=_hours(6)))

    def test_offset_change_is_a_diff(self):
        assert _has_diff(
            _group_by(interval=_hours(3), offset=_hours(1)),
            _group_by(interval=_hours(3)),
        )

    def test_equal_millis_units_are_not_a_diff(self):
        assert not _has_diff(
            _group_by(interval=_hours(3)), _group_by(interval=_minutes(180))
        )

    def test_schedule_change_is_not_a_diff(self):
        # delay is execution-domain: rescheduling must never force a version bump
        assert not _has_diff(
            _group_by(interval=_hours(3), schedule="0 */3 * * *"),
            _group_by(interval=_hours(3), schedule="20 */3 * * *"),
        )
        assert not _has_diff(
            _group_by(schedule="0 0 * * *"), _group_by(schedule="0 6 * * *")
        )
