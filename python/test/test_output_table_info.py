"""
Tests for output_table_info partition grid validation (interval/offset constraints).
"""

import pytest

import gen_thrift.common.ttypes as common
from ai.chronon.windows import (
    DAILY_PARTITION_FORMAT,
    SUB_DAILY_PARTITION_FORMAT,
    output_table_info,
)


class TestIntervalConstraints:
    """Partition intervals must divide a UTC day evenly or equal exactly one day."""

    def test_rejects_interval_that_does_not_divide_a_day(self):
        with pytest.raises(ValueError, match="divide"):
            output_table_info(partition_interval="5h")

    def test_rejects_multi_day_intervals(self):
        # week/month-sized partitions are unrepresentable: 7d boundaries start on Thursday
        # (epoch day zero), 30d grids drift off calendar months. Weekly/monthly SCHEDULES
        # still work over daily partitions.
        with pytest.raises(ValueError, match="divide|day"):
            output_table_info(partition_interval="7d")
        with pytest.raises(ValueError, match="divide|day"):
            output_table_info(partition_interval="30d")
        with pytest.raises(ValueError, match="divide|day"):
            output_table_info(partition_interval="25h")

    def test_accepts_day_dividing_intervals(self):
        for interval in ["1m", "15m", "30m", "1h", "3h", "8h", "12h"]:
            info = output_table_info(partition_interval=interval)
            assert info.partitionFormat == SUB_DAILY_PARTITION_FORMAT

    def test_rejects_irregular_simple_cron_intervals(self):
        for interval in ["45m", "90m"]:
            with pytest.raises(ValueError, match="irregular"):
                output_table_info(partition_interval=interval)

    def test_rejects_coarser_schedule_over_subdaily_interval(self):
        with pytest.raises(ValueError, match="coarser"):
            output_table_info(partition_interval="1h", schedule="@daily")
        with pytest.raises(ValueError, match="coarser"):
            output_table_info(partition_interval="1h", schedule="0 2 * * *")

    def test_rejects_subdaily_cron_interval_mismatch(self):
        with pytest.raises(ValueError, match="must match"):
            output_table_info(partition_interval="1h", schedule="0 */3 * * *")

    def test_accepts_explicit_daily_interval(self):
        info = output_table_info(partition_interval="1d")
        assert info.partitionFormat == DAILY_PARTITION_FORMAT

    def test_rejects_object_form_multi_day_interval(self):
        # the Window object path must hit the same validation as the string path
        with pytest.raises(ValueError, match="divide|day"):
            output_table_info(
                partition_interval=common.Window(length=7, timeUnit=common.TimeUnit.DAYS)
            )


class TestOffsetConstraints:
    """Offsets only exist on sub-daily grids and must be canonical: 0 <= offset < interval."""

    def test_rejects_offset_on_daily_grid(self):
        # daily grids keep their boundaries at midnight
        with pytest.raises(ValueError, match="daily|sub-daily"):
            output_table_info(partition_interval="1d", partition_offset="1h")

    def test_rejects_negative_offset_object_form(self):
        # string "-1h" already fails _from_str positivity; the Window object path must
        # reject too instead of letting scala silently floorMod it to interval-1h
        with pytest.raises(ValueError):
            output_table_info(
                partition_interval="3h",
                partition_offset=common.Window(length=-1, timeUnit=common.TimeUnit.HOURS),
            )

    def test_rejects_offset_equal_to_interval(self):
        with pytest.raises(ValueError, match="less than"):
            output_table_info(partition_interval="3h", partition_offset="3h")

    def test_zero_offset_object_form_is_dropped(self):
        # explicit zero offset and absent offset must serialize identically -- the planner
        # convention is offset-only-when-nonzero, and divergent bytes churn semantic hashes
        info = output_table_info(
            partition_interval="3h",
            partition_offset=common.Window(length=0, timeUnit=common.TimeUnit.HOURS),
        )
        assert info.partitionOffset is None

    def test_accepts_canonical_sub_daily_offset(self):
        info = output_table_info(partition_interval="3h", partition_offset="1h")
        assert info.partitionInterval == common.Window(length=3, timeUnit=common.TimeUnit.HOURS)
        assert info.partitionOffset == common.Window(length=1, timeUnit=common.TimeUnit.HOURS)

    def test_offset_without_interval_or_schedule_rejected(self):
        with pytest.raises(ValueError, match="partition_offset requires"):
            output_table_info(partition_offset="1h")


class TestFormatOverrideWarning:
    def test_custom_output_format_warns(self):
        # the knob stays (input tables need it) but custom OUTPUT formats are discouraged:
        # compact/composed formats can silently mismatch downstream readers
        with pytest.warns(UserWarning, match="Custom output partition_format"):
            info = output_table_info(partition_interval="3h", partition_format="yyyyMMddHH")
        assert info.partitionFormat == "yyyyMMddHH"

    def test_default_formats_stay_quiet(self):
        import warnings

        with warnings.catch_warnings():
            warnings.simplefilter("error")
            output_table_info(partition_interval="3h")
            output_table_info(partition_interval="3h", partition_format=SUB_DAILY_PARTITION_FORMAT)
            output_table_info(partition_interval="1d", partition_format=DAILY_PARTITION_FORMAT)
