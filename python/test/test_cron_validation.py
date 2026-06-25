"""
Tests for the structural cron validation rule in windows.regular_subdaily_schedule.

The rule (no probing, pure cron-field inspection):
- cron interval >= 24h (fires at most once per day) -> daily: returns None; any
  day-of-month / month / weekday restrictions are fine (weekly/monthly reports).
- cron interval < 24h (fires more than once per day) -> day-of-month, month, and
  weekday fields must all be '*', and the minute/hour pattern must be regular with
  the implied interval dividing 24h.
"""

import pytest

from ai.chronon.windows import HOUR_MILLIS, MINUTE_MILLIS, regular_subdaily_schedule


class TestDailyOrCoarserSchedules:
    """Fires <= once/day -> None (daily interval), day restrictions allowed."""

    def test_daily_and_coarser_schedules_return_none(self):
        for schedule in [
            None,
            "",
            "none",
            "@daily",
            "@never",
            "0 0 * * *",  # midnight daily
            "30 14 * * *",  # daily with delay baked into the fire time
            "0 0 * * MON",  # weekly
            "0 6 1 * *",  # monthly report
            "0 6 1,15 * *",  # twice a month
            "0 9 * * MON-FRI",  # weekdays only
            "45 16 * * 6",  # Saturdays
            "0 9 1 1 *",  # yearly
        ]:
            assert regular_subdaily_schedule(schedule) is None, f"expected daily for '{schedule}'"


class TestRegularSubDailySchedules:
    """Fires > once/day with clean day fields and a regular pattern -> interval millis."""

    def test_regular_subdaily_intervals(self):
        cases = {
            "0 * * * *": HOUR_MILLIS,
            "0 */2 * * *": 2 * HOUR_MILLIS,
            "*/30 * * * *": 30 * MINUTE_MILLIS,
            "*/15 * * * *": 15 * MINUTE_MILLIS,
            "5/15 * * * *": 15 * MINUTE_MILLIS,  # constant cron phase, 15m grid
            "0 0,12 * * *": 12 * HOUR_MILLIS,
            "20 */3 * * *": 3 * HOUR_MILLIS,  # 20m delay over a 3h grid
            "0 1-22/3 * * *": 3 * HOUR_MILLIS,  # 1h phase over a 3h grid
        }
        for schedule, expected in cases.items():
            offset_ms = HOUR_MILLIS if schedule == "0 1-22/3 * * *" else 0
            assert regular_subdaily_schedule(schedule, offset_ms) == expected, schedule


class TestDayRestrictedSubDailyRejected:
    """Sub-daily firing on a subset of days is incoherent under interval inference:
    the declared grid would have most of its partitions never computed."""

    def test_weekday_restricted_subdaily_rejected(self):
        for schedule in [
            "0 */2 * * MON",  # every 2h, Mondays only
            "*/30 * * * 0",  # every 30m, Sundays only
            "* * * * 1-5",  # every minute, weekdays only
            "0 0,12 * * MON",  # twice a day, Mondays only
        ]:
            with pytest.raises(ValueError, match="every day|day-restricted|restrict"):
                regular_subdaily_schedule(schedule)

    def test_dom_and_month_restricted_subdaily_rejected(self):
        for schedule in [
            "0 */3 1 * *",  # every 3h, 1st of month only
            "0 */3 * 1 *",  # every 3h, January only (defeats fixed-window probes)
            "0 */3 1-7 * *",  # every 3h, first week of month only
        ]:
            with pytest.raises(ValueError, match="every day|day-restricted|restrict"):
                regular_subdaily_schedule(schedule)


class TestIrregularSubDailyRejected:
    """Sub-daily but uneven spacing across the day."""

    def test_irregular_patterns_rejected(self):
        for schedule in [
            "0 */5 * * *",  # 5h cadence leaves a 4h gap at the day boundary
            "0 9,14 * * *",  # two uneven fires
            "30 8-17 * * *",  # hourly only within business hours
            "0 1,4,9 * * *",  # arbitrary hours
            "*/7 * * * *",  # 7m doesn't divide the hour pattern evenly
        ]:
            with pytest.raises(ValueError, match="regular|constant|divide"):
                regular_subdaily_schedule(schedule)


class TestAliasHandling:
    def test_non_daily_aliases_rejected(self):
        with pytest.raises(ValueError, match="@daily"):
            regular_subdaily_schedule("@hourly")
