import datetime
import json
import math
from typing import Optional, OrderedDict

import ai.chronon.utils as utils
from gen_thrift.api.ttypes import GroupBy, Join
from gen_thrift.common.ttypes import TimeUnit

AIRFLOW_DEPENDENCIES_KEY = "airflowDependencies"

DEFAULT_PARTITION_FORMAT = "yyyy-MM-dd"

# Java SimpleDateFormat -> Python strftime for the tokens that appear in
# Chronon partition formats. Ordered longest-first so `yyyy` isn't partially
# eaten by `yy` during replace().
_JAVA_TO_PY_FORMAT = [
    ("yyyy", "%Y"),
    ("yy", "%y"),
    ("MM", "%m"),
    ("dd", "%d"),
    ("HH", "%H"),
    ("mm", "%M"),
    ("ss", "%S"),
]


def _java_format_to_python(fmt: str) -> str:
    """Translate a Chronon partition_format (Java SimpleDateFormat subset) to a
    Python strftime format. Covers yyyy/yy/MM/dd/HH/mm/ss — every pattern used
    in practice. Literals (dashes, slashes, etc.) pass through unchanged."""
    out = fmt
    for java, py in _JAVA_TO_PY_FORMAT:
        out = out.replace(java, py)
    return out


def _cutoff_already_binds(
    offset: int,
    end_cutoff: str,
    partition_format: str = DEFAULT_PARTITION_FORMAT,
    today: Optional[str] = None,
) -> bool:
    """Return True iff ``end_cutoff`` already caps the sensor partition at compile
    time — i.e. the partition for ``today - offset`` (formatted in the same
    partition_format as ``end_cutoff``) already sorts past it. Every future run
    would then resolve the upstream end partition to the cutoff literal."""
    today_date = datetime.date.fromisoformat(today) if today else datetime.date.today()
    effective = today_date - datetime.timedelta(days=offset)
    py_fmt = _java_format_to_python(partition_format)
    return effective.strftime(py_fmt) > end_cutoff


def create_airflow_dependency(
    table,
    partition_column,
    additional_partitions=None,
    offset=0,
    end_cutoff: Optional[str] = None,
    partition_format: Optional[str] = None,
):
    """
    Create an Airflow dependency object for a table.

    The sensor waits on ``ds - offset`` (via ``macros.ds_add(ds, -offset)``) to
    match Chronon's semantics where ``offset=N`` means an ``N``-day lag. When
    ``end_cutoff`` is set and already past at compile time, the spec uses the
    cutoff literal instead — every future run resolves to the same fixed
    partition, so there's nothing to template on ``ds``.

    Args:
        table: The table name (with namespace).
        partition_column: The partition column.
        additional_partitions: Sub-partitions to wait on alongside the date.
        offset: Day lag (positive = past). Resolved by ``TableDependency.resolved_offsets``
            from ``end_offset`` / ``offset`` / ``0``.
        end_cutoff: Optional fixed ceiling on the upstream end partition.
        partition_format: Chronon partition_format for this dependency (defaults
            to ``yyyy-MM-dd``). Used to render ``today - offset`` in the same
            format as ``end_cutoff`` for the cutoff-bound check.

    Returns:
        A dictionary with name and spec for the Airflow dependency.
    """
    assert (
        partition_column is not None
    ), """Partition column must be provided via the spark.chronon.partition.column
        config. This can be set as a default in teams.py, or at the individual config level. For example:
        ```
        Team(
            conf=ConfigProperties(
                common={
                    "spark.chronon.partition.column": "_test_column",
                }
            )
        )
        ```
        """

    additional_partitions_str = ""
    if additional_partitions:
        additional_partitions_str = "/" + "/".join(additional_partitions)

    effective_partition_format = partition_format or DEFAULT_PARTITION_FORMAT
    if end_cutoff is not None and _cutoff_already_binds(
        offset, end_cutoff, effective_partition_format
    ):
        partition_value = end_cutoff
        name_suffix = f"at_{utils.sanitize(end_cutoff)}"
    else:
        # Airflow has no `ds_sub` — subtract by passing a negated offset to ds_add.
        partition_value = f"{{{{ macros.ds_add(ds, {-offset}) }}}}"
        name_suffix = f"with_offset_{offset}"

    return {
        "name": f"wf_{utils.sanitize(table)}_{name_suffix}",
        "spec": f"{table}/{partition_column}={partition_value}{additional_partitions_str}",
    }


def _get_partition_col_from_query(query):
    """Gets partition column from query if available"""
    if query:
        return query.partitionColumn
    return None


def _get_additional_subPartitionsToWaitFor_from_query(query):
    """Gets additional subPartitionsToWaitFor from query if available"""
    if query:
        return query.subPartitionsToWaitFor
    return None


def _get_airflow_deps_from_source(source, partition_column=None):
    """
    Given a source, return a list of Airflow dependencies.

    Args:
        source: The source object (events, entities, or joinSource)
        partition_column: The partition column to use

    Returns:
        A list of Airflow dependency objects
    """
    tables = []
    additional_partitions = None
    # Assumes source has already been normalized
    if source.events:
        tables = [source.events.table]
        # Use partition column from query if available, otherwise use the provided one
        source_partition_column, additional_partitions = (
            _get_partition_col_from_query(source.events.query) or partition_column,
            _get_additional_subPartitionsToWaitFor_from_query(source.events.query),
        )

    elif source.entities:
        # Given the setup of Query, we currently mandate the same partition column for snapshot and mutations tables
        tables = [source.entities.snapshotTable]
        if source.entities.mutationTable:
            tables.append(source.entities.mutationTable)
        source_partition_column, additional_partitions = (
            _get_partition_col_from_query(source.entities.query) or partition_column,
            _get_additional_subPartitionsToWaitFor_from_query(source.entities.query),
        )
    elif source.joinSource:
        # TODO: Handle joinSource -- it doesn't work right now because the metadata isn't set on joinSource at this point
        return []
    else:
        # Unknown source type
        return []

    # Without a partition column we can't construct a meaningful dependency
    # spec — skip rather than fail. Happens whenever a team's conf doesn't set
    # `spark.chronon.partition.column` and the source's query doesn't specify
    # one either (e.g. a per-env teams file with minimal conf). Failing later
    # (at runtime / scheduling) is preferred over an obscure assertion deep in
    # compile-time dep generation.
    if source_partition_column is None:
        return []

    return [
        create_airflow_dependency(table, source_partition_column, additional_partitions)
        for table in tables
    ]


def extract_default_partition_column(obj):
    try:
        return obj.metaData.executionInfo.conf.common.get("spark.chronon.partition.column")
    except Exception:
        # Error handling occurs in `create_airflow_dependency`
        return None


def _get_distinct_day_windows(group_by):
    windows = []
    aggs = group_by.aggregations
    if aggs:
        for agg in aggs:
            for window in agg.windows:
                time_unit = window.timeUnit
                length = window.length
                if time_unit == TimeUnit.DAYS:
                    windows.append(length)
                elif time_unit == TimeUnit.HOURS:
                    windows.append(math.ceil(length / 24))
                elif time_unit == TimeUnit.MINUTES:
                    windows.append(math.ceil(length / (24 * 60)))
    return set(windows)


def _set_join_deps(join):
    default_partition_col = extract_default_partition_column(join)

    deps = []

    # Handle left source
    left_query = utils.get_query(join.left)
    left_partition_column = _get_partition_col_from_query(left_query) or default_partition_col
    deps.extend(_get_airflow_deps_from_source(join.left, left_partition_column))

    # Handle right parts (join parts)
    if join.joinParts:
        for join_part in join.joinParts:
            if join_part.groupBy and join_part.groupBy.sources:
                for source in join_part.groupBy.sources:
                    source_query = utils.get_query(source)
                    source_partition_column = (
                        _get_partition_col_from_query(source_query) or default_partition_col
                    )
                    deps.extend(_get_airflow_deps_from_source(source, source_partition_column))

    # Update the metadata customJson with dependencies
    _dedupe_and_set_airflow_deps_json(join, deps, AIRFLOW_DEPENDENCIES_KEY)


def _set_group_by_deps(group_by):
    if not group_by.sources:
        return

    default_partition_col = extract_default_partition_column(group_by)

    deps = []

    # Process each source in the group_by
    for source in group_by.sources:
        source_query = utils.get_query(source)
        source_partition_column = (
            _get_partition_col_from_query(source_query) or default_partition_col
        )
        deps.extend(_get_airflow_deps_from_source(source, source_partition_column))

    # Update the metadata customJson with dependencies
    _dedupe_and_set_airflow_deps_json(group_by, deps, AIRFLOW_DEPENDENCIES_KEY)


def _dedupe_and_set_airflow_deps_json(obj, deps, custom_json_key):
    sorted_items = [tuple(sorted(d.items())) for d in deps]
    # Deduplicate while preserving order, then use OrderedDict for reproducible key ordering
    seen = set()
    unique = []
    for t in sorted_items:
        if t not in seen:
            seen.add(t)
            unique.append(OrderedDict(t))
    existing_json = obj.metaData.customJson or "{}"
    json_map = json.loads(existing_json)
    json_map[custom_json_key] = unique
    obj.metaData.customJson = json.dumps(json_map)


def set_airflow_deps(obj):
    """
    Set Airflow dependencies for a Chronon object.

    Args:
        obj: A Join, GroupBy
    """
    # StagingQuery dependency setting is handled directly in object init
    if isinstance(obj, Join):
        _set_join_deps(obj)
    elif isinstance(obj, GroupBy):
        _set_group_by_deps(obj)
