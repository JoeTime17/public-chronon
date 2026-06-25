from dataclasses import dataclass
from typing import Optional, Union

import gen_thrift.common.ttypes as common


def _days(length: int) -> common.Window:
    return common.Window(length=length, timeUnit=common.TimeUnit.DAYS)


def _hours(length: int) -> common.Window:
    return common.Window(length=length, timeUnit=common.TimeUnit.HOURS)


def _minutes(length: int) -> common.Window:
    return common.Window(length=length, timeUnit=common.TimeUnit.MINUTES)


def _from_str(s: str) -> common.Window:
    """
    converts strings like "30d", "2h", "15m" etc into common.Window

    Args:
        s (str): Duration string in format "<number>(d|h|m)" where d=days, h=hours, m=minutes

    Returns:
        common.Window: Window object with specified duration

    Raises:
        ValueError: If string format is invalid
    """

    if not s or len(s) < 2:
        raise ValueError(f"Invalid duration format: {s}")

    # Get the numeric value and unit
    value = s[:-1]
    unit = s[-1].lower()

    try:
        length = int(value)
        if length <= 0:
            raise ValueError(f"Duration must be positive: {s}")

        if unit == "d":
            return _days(length)
        elif unit == "h":
            return _hours(length)
        elif unit == "m":
            return _minutes(length)
        else:
            raise ValueError(
                f"Invalid time unit '{unit}'. Must be 'd' for days, 'h' for hours, or 'm' for minutes"
            )

    except ValueError as e:
        if "invalid literal for int()" in str(e):
            raise ValueError(f"Invalid numeric value in duration: {value}") from e
        raise e from None


def normalize_window(w: Union[common.Window, str]) -> common.Window:
    """
    Normalizes a window specification to a common.Window object.

    Accepts either a Window object directly or a string like "30d", "24h", or "15m".
    This is used across the codebase (e.g., in GroupBy aggregations and TrainingSpec).

    Args:
        w: Either a common.Window object or a string like "7d", "24h", "15m"

    Returns:
        common.Window: The normalized window object

    Raises:
        TypeError: If the input is neither a string nor a Window object
    """
    if isinstance(w, str):
        return _from_str(w)
    elif isinstance(w, common.Window):
        return w
    else:
        raise TypeError(
            f"Window should be either a string like '7d', '24h', '15m', or a Window type, "
            f"got {type(w).__name__}"
        )


DAY_MILLIS = 24 * 60 * 60 * 1000
HOUR_MILLIS = 60 * 60 * 1000
MINUTE_MILLIS = 60 * 1000
DAILY_PARTITION_FORMAT = "yyyy-MM-dd"
# dash-separated: partition values become object-store directory names, where spaces and
# colons get URL-escaped (Hive percent-escapes colons). Space/colon formats remain
# expressible by setting an explicit partition format.
SUB_DAILY_PARTITION_FORMAT = "yyyy-MM-dd-HH-mm"


@dataclass(frozen=True)
class PartitionSpec:
    column: Optional[str] = None
    format: Optional[str] = None
    interval: Optional[Union[common.Window, str]] = None
    offset: Optional[Union[common.Window, str]] = None
    time_partitioned: Optional[bool] = None

    @classmethod
    def from_table_info(cls, table_info) -> "PartitionSpec":
        if table_info is None:
            return cls()
        return cls(
            column=table_info.partitionColumn,
            format=table_info.partitionFormat,
            interval=table_info.partitionInterval,
            offset=table_info.partitionOffset,
            time_partitioned=getattr(table_info, "timePartitioned", None),
        )

    @classmethod
    def from_table_reference(cls, table_reference) -> "PartitionSpec":
        partition_spec = getattr(table_reference, "partition_spec", None)
        if partition_spec is not None:
            return partition_spec
        return cls(
            column=getattr(table_reference, "partition_column", None),
            format=getattr(table_reference, "partition_format", None),
            interval=getattr(table_reference, "partition_interval", None),
            offset=getattr(table_reference, "partition_offset", None),
        )

    def has_interval(self) -> bool:
        return self.interval is not None

    def with_missing_from(self, fallback: "PartitionSpec") -> "PartitionSpec":
        return PartitionSpec(
            column=self.column if self.column is not None else fallback.column,
            format=self.format if self.format is not None else fallback.format,
            interval=self.interval if self.interval is not None else fallback.interval,
            offset=self.offset if self.offset is not None else fallback.offset,
            time_partitioned=(
                self.time_partitioned
                if self.time_partitioned is not None
                else fallback.time_partitioned
            ),
        )

    def normalized_interval(self) -> Optional[common.Window]:
        return normalize_window(self.interval) if self.interval is not None else None

    def normalized_offset(self) -> Optional[common.Window]:
        return normalize_window(self.offset) if self.offset is not None else None

    def defaulted_format(self) -> Optional[str]:
        if self.format is not None:
            return self.format
        if self.interval is None:
            return None
        return default_partition_format(self.interval)

    def interval_millis(self) -> Optional[int]:
        return window_millis(self.interval) if self.interval is not None else None

    def offset_millis(self) -> int:
        return window_millis(self.offset) if self.offset is not None else 0

    def is_daily_grid(self) -> bool:
        return self.interval_millis() == DAY_MILLIS and self.offset_millis() == 0

    def is_subdaily_grid(self) -> bool:
        interval_ms = self.interval_millis()
        return interval_ms is not None and interval_ms < DAY_MILLIS

    def query_kwargs(self):
        return {
            "partitionColumn": self.column,
            "partitionFormat": self.defaulted_format(),
            "partitionInterval": self.normalized_interval(),
            "partitionOffset": self.normalized_offset(),
            "timePartitioned": self.time_partitioned,
        }

    def table_info(self, table: str = None, use_default_format: bool = False) -> common.TableInfo:
        return common.TableInfo(
            table=table,
            partitionColumn=self.column,
            partitionFormat=self.defaulted_format() if use_default_format else self.format,
            partitionInterval=self.normalized_interval(),
            partitionOffset=self.normalized_offset(),
            timePartitioned=self.time_partitioned,
        )


def from_millis(millis: int) -> common.Window:
    if millis % DAY_MILLIS == 0:
        return _days(millis // DAY_MILLIS)
    if millis % HOUR_MILLIS == 0:
        return _hours(millis // HOUR_MILLIS)
    if millis % MINUTE_MILLIS == 0:
        return _minutes(millis // MINUTE_MILLIS)
    raise ValueError(f"Window duration must be whole minutes, found {millis}ms")


def window_millis(w: Union[common.Window, str]) -> int:
    window = normalize_window(w)
    if window.timeUnit == common.TimeUnit.DAYS:
        return window.length * DAY_MILLIS
    if window.timeUnit == common.TimeUnit.HOURS:
        return window.length * HOUR_MILLIS
    if window.timeUnit == common.TimeUnit.MINUTES:
        return window.length * MINUTE_MILLIS
    raise ValueError(f"Unsupported TimeUnit for partition interval: {window.timeUnit}")


def default_partition_format(partition_interval: Union[common.Window, str]) -> str:
    return (
        SUB_DAILY_PARTITION_FORMAT
        if window_millis(partition_interval) < DAY_MILLIS
        else DAILY_PARTITION_FORMAT
    )


def _is_unrestricted(field_values, full_range) -> bool:
    """True when an expanded cron day-field doesn't restrict which days the cron fires."""
    if field_values == ["*"]:
        return True
    try:
        values = {int(v) for v in field_values}
    except (TypeError, ValueError):
        return False
    # croniter accepts 7 as an alias for Sunday (0) in the weekday field
    return {v % 7 for v in values} >= full_range if full_range == set(range(7)) else values >= full_range


def regular_subdaily_schedule(schedule_expression: str, partition_offset_ms: int = 0) -> Optional[int]:
    """Validate a cron expression and return its data interval in millis for regular sub-daily
    schedules, or None for daily-or-coarser (or absent) schedules.

    The rule is structural (pure cron-field inspection, no probing — fixed probe windows can
    be defeated by month/day-of-month crons that coincide with the window):

    - fires at most once per day → daily interval: day-of-month / month / weekday
      restrictions are fine (weekly or monthly reports over daily partitions).
    - fires more than once per day → the day fields must all be unrestricted and the
      minute/hour pattern must be evenly spaced, including across the midnight wrap-around
      (which forces the interval to divide a UTC day exactly).

    Grids are declared, never inferred from the cron fire phase: the phase only contributes
    a derived processing delay relative to the declared grid. For a structurally regular
    cron that delay is constant and strictly less than the interval by construction, so no
    separate phase validation is needed; ``partition_offset_ms`` is kept for callers that
    want to log or surface the derived delay.
    """
    from croniter import croniter

    if not schedule_expression or schedule_expression.strip().lower() in (
        "",
        "none",
        "null",
        "@daily",
        "@never",
    ):
        return None

    schedule_expression = schedule_expression.strip()
    if schedule_expression.startswith("@"):
        raise ValueError(
            "Only @daily and @never aliases are supported; use a 5-field cron expression otherwise."
        )

    minutes, hours, dom, month, dow = croniter.expand(schedule_expression)[0][:5]
    minute_values = list(range(60)) if minutes == ["*"] else sorted({int(m) for m in minutes})
    hour_values = list(range(24)) if hours == ["*"] else sorted({int(h) for h in hours})

    fires = sorted(h * 60 + m for h in hour_values for m in minute_values)
    if len(fires) <= 1:
        # at most once per day: the 24h interval ceiling applies and skipped days are just
        # normal "this job doesn't run every day" (weekly/monthly schedules)
        return None

    day_unrestricted = (
        _is_unrestricted(dom, set(range(1, 32)))
        and _is_unrestricted(month, set(range(1, 13)))
        and _is_unrestricted(dow, set(range(7)))
    )
    if not day_unrestricted:
        raise ValueError(
            "Sub-daily schedules must fire every day: day-restricted sub-daily crons are not "
            f"supported (day-of-month, month, and weekday fields must be '*'), got '{schedule_expression}'. "
            "A grid inferred from a day-restricted cron would have most of its partitions never computed."
        )

    deltas = {fires[i + 1] - fires[i] for i in range(len(fires) - 1)}
    deltas.add(fires[0] + 24 * 60 - fires[-1])  # midnight wrap-around closes the day boundary gap
    if len(deltas) != 1:
        raise ValueError(
            "Sub-daily schedules must be regular: fire times must be evenly spaced across the "
            f"whole UTC day including the midnight wrap-around, got '{schedule_expression}'."
        )
    interval_ms = deltas.pop() * MINUTE_MILLIS
    # constant spacing that wraps the day always tiles 24h exactly; assert the invariant
    assert DAY_MILLIS % interval_ms == 0, f"regular cron interval {interval_ms}ms must divide a day"
    return interval_ms


def regular_simple_cron_interval(interval_ms: int) -> bool:
    """True when a sub-daily interval can be represented by a simple repeating cron field."""
    if interval_ms <= 0 or interval_ms >= DAY_MILLIS or interval_ms % MINUTE_MILLIS != 0:
        return False
    interval_minutes = interval_ms // MINUTE_MILLIS
    if interval_minutes < 60:
        return 60 % interval_minutes == 0
    return interval_minutes % 60 == 0


def validate_source_grid(conf_desc: str, query, source_desc: str) -> None:
    """GroupBy/model sources and the join LEFT must hold the output partition's whole time
    range: every output boundary must also be a source boundary. Example: a 3h@1h output over
    a daily source can't fill [13:00, 16:00) until the day closes. Call only when the conf's
    output grid is sub-daily: physical sources must declare a partition_interval unless the
    table is Chronon-produced and the TableReference propagated it. time_partitioned sources
    use timestamp/date slicing and may inherit the consumer grid at planning time. Join RIGHT
    parts pick the latest snapshot at or before each left row's ts on their own grid - mixed
    cadence is the product - and must never be validated through this."""
    if query is None or query.partitionInterval is not None:
        return
    if query.timePartitioned:
        return
    raise ValueError(
        f"{conf_desc} has a sub-daily output grid over {source_desc} with no declared "
        "partition_interval - implicitly daily. Every intraday run would wait for the full "
        "day's partition and land a day late. Declare the source's partition_interval."
    )


def validate_table_dependency_grid(conf_desc: str, dependency, source_desc: str) -> None:
    """Validate a Python StagingQuery TableDependency for a sub-daily output grid.

    Physical dependencies must declare the upstream cadence explicitly. Chronon-produced
    tables propagate their non-daily grid through TableReference, and time_partitioned
    dependencies may inherit the StagingQuery output grid.
    """
    if dependency is None or dependency.partition_interval is not None:
        return
    if dependency.time_partitioned:
        return
    raise ValueError(
        f"{conf_desc} has a sub-daily output grid over {source_desc} with no declared "
        "partition_interval - implicitly daily. Declare the dependency's partition_interval."
    )


def source_query(source):
    """The inner query of a thrift Source union (events / entities / joinSource)."""
    if source is None:
        return None
    inner = source.events or source.entities or source.joinSource
    return inner.query if inner is not None else None


def is_subdaily(
    partition_interval: Union[common.Window, str] = None, schedule: str = None
) -> bool:
    """True when the output partitionInterval is sub-daily — either via an explicit partition
    interval below one day or a regular sub-daily schedule it would be inferred from."""
    if partition_interval is not None:
        return window_millis(partition_interval) < DAY_MILLIS
    if schedule:
        return regular_subdaily_schedule(schedule) is not None
    return False


def output_table_info(
    partition_interval: Union[common.Window, str] = None,
    partition_offset: Union[common.Window, str] = None,
    partition_column: str = "ds",
    partition_format: str = None,
    schedule: str = None,
) -> common.TableInfo:
    # The offset is never inferred from the cron fire phase; it defaults to zero (midnight
    # boundaries) and only an explicit partition_offset moves the grid. The cron fire phase
    # is treated as a derived processing delay relative to the declared grid.
    offset_ms = window_millis(partition_offset) if partition_offset is not None else 0
    cron_interval_ms = regular_subdaily_schedule(schedule, offset_ms) if schedule else None
    normalized_schedule = schedule.strip().lower() if isinstance(schedule, str) else schedule
    has_enabled_schedule = normalized_schedule not in (None, "", "none", "null", "@never")
    if partition_interval is None and cron_interval_ms is None:
        if partition_offset is not None:
            raise ValueError(
                "partition_offset requires a partition_interval or a regular sub-daily schedule."
            )
        return None
    interval = (
        normalize_window(partition_interval)
        if partition_interval is not None
        else from_millis(cron_interval_ms)
    )
    interval_ms = window_millis(interval)
    # day-denominated reasoning relies on partitions tiling the UTC day; week/month-sized
    # partitions are deliberately unrepresentable (weekly/monthly cadences are schedules over
    # daily partitions, not partition intervals)
    if interval_ms != DAY_MILLIS and (interval_ms > DAY_MILLIS or DAY_MILLIS % interval_ms != 0):
        raise ValueError(
            f"partition_interval ({interval_ms}ms) must divide a UTC day evenly or equal one day. "
            "Weekly/monthly cadences are expressed as schedules over daily partitions."
        )
    if interval_ms < DAY_MILLIS and not regular_simple_cron_interval(interval_ms):
        raise ValueError(
            f"irregular partition_interval ({interval_ms}ms) is not supported for scheduled jobs. "
            "Use a minute interval that divides an hour or an hour interval that divides a day."
        )
    if has_enabled_schedule and cron_interval_ms is None and interval_ms < DAY_MILLIS:
        raise ValueError(
            f"schedule '{schedule}' is coarser than sub-daily partition_interval ({interval_ms}ms). "
            "Use a regular sub-daily schedule at the partition cadence."
        )
    if cron_interval_ms is not None and cron_interval_ms != interval_ms:
        raise ValueError(
            f"partition_interval ({interval_ms}ms) must match the cron data interval "
            f"({cron_interval_ms}ms) of schedule '{schedule}'."
        )
    # validate on computed millis so the Window object path hits the same checks as strings
    if offset_ms != 0:
        if interval_ms >= DAY_MILLIS:
            raise ValueError(
                "Daily partitions keep their boundaries at midnight: partition_offset is only "
                "supported on sub-daily grids."
            )
        if offset_ms < 0 or offset_ms >= interval_ms:
            raise ValueError(
                f"partition_offset ({offset_ms}ms) must be non-negative and strictly less than "
                f"the partition interval ({interval_ms}ms)."
            )
    # zero offset serializes identically to no offset: the planner convention is
    # offset-only-when-nonzero, and divergent bytes would churn semantic hashes
    offset = normalize_window(partition_offset) if offset_ms != 0 else None
    default_format = default_partition_format(interval)
    if partition_format is not None and partition_format != default_format:
        import warnings

        warnings.warn(
            f"Custom output partition_format '{partition_format}' (default for this interval is "
            f"'{default_format}'). Custom output formats are discouraged: compact or composed "
            "formats can silently mismatch downstream readers. "
            "Prefer the default; input tables can keep declaring their actual format.",
            UserWarning,
            stacklevel=2,
        )
    return PartitionSpec(
        column=partition_column,
        format=partition_format or default_format,
        interval=interval,
        offset=offset,
    ).table_info(use_default_format=True)
