import json
import logging
import re
import warnings
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Union

from sqlglot import exp, parse_one

import ai.chronon.airflow_helpers as airflow_helpers
import gen_thrift.api.ttypes as ttypes
import gen_thrift.common.ttypes as common
from ai.chronon import utils
from ai.chronon.airflow_helpers import AIRFLOW_DEPENDENCIES_KEY
from ai.chronon.cli.compile.config_origin import mark_factory_created_config

logger = logging.getLogger(__name__)


def _strip_jinja_templates(query: str) -> str:
    """Replace Jinja-style template variables with placeholder values so sqlglot can parse the query."""
    return re.sub(r"\{\{[^}]*\}\}", "2000-01-01", query)


def _tables_in_query(query: str, dialect: str = "spark") -> List[str]:
    """Get the tables referenced in a SQL query."""
    clean_query = _strip_jinja_templates(query)
    try:
        parsed_query = parse_one(clean_query, dialect=dialect)
    except Exception as e:
        logger.warning("Failed to parse SQL for table extraction: %s", e)
        return []
    return [t.sql() for t in parsed_query.find_all(exp.Table)]


def _normalize_table_name(table_name: str) -> str:
    """Normalize a table name by stripping backticks and double quotes."""
    return table_name.replace("`", "").replace('"', "")


def _get_output_table_name(staging_query: ttypes.StagingQuery, full_name: bool = False):
    """generate output table name for staging query job"""
    return utils._ensure_name_and_get_output_table(
        staging_query, ttypes.StagingQuery, "staging_queries", full_name
    )


# Wrapper for EngineType
class EngineType:
    SPARK = ttypes.EngineType.SPARK
    BIGQUERY = ttypes.EngineType.BIGQUERY
    SNOWFLAKE = ttypes.EngineType.SNOWFLAKE


@dataclass
class TableDependency:
    """
    Declares an upstream table dependency for a StagingQuery. The orchestrator
    resolves the required upstream partition range from the query range and the
    offset / cutoff fields, then requires every partition in that range to be
    Filled before the step runs.

    Resolved range:
        ``[max(query.start - start_offset, start_cutoff),
           min(query.end - end_offset, end_cutoff)]``

    :param offset:
        DEPRECATED — prefer ``start_offset`` / ``end_offset``. When set, applies
        to both sides unless a specific ``start_offset`` / ``end_offset`` overrides
        that side. Emits a ``DeprecationWarning``.
    :type offset: Optional[int]
    :param start_offset:
        Days subtracted from ``query.start`` to compute the upstream range start.
        Combined with ``start_cutoff`` via ``max(query.start - start_offset,
        start_cutoff)`` — the cutoff acts as a floor. Defaults to ``0`` when unset,
        except when only ``start_cutoff`` is set (no ``offset``, no ``start_offset``),
        in which case the resolved ``startOffset`` is left ``None`` so the start
        pins at ``start_cutoff`` regardless of the query range.
    :type start_offset: Optional[int]
    :param end_offset:
        Days subtracted from ``query.end`` to compute the upstream range end.
        Combined with ``end_cutoff`` via ``min(query.end - end_offset, end_cutoff)``
        — the cutoff acts as a ceiling. Defaults to ``0`` when unset. Always
        non-null in the resolved thrift: when ``end_cutoff`` is also unset,
        ``DependencyResolver.computeInputRange`` returns ``PartitionRange(_, null)``
        and ``BatchNodeRunner.requiredEnd`` / the cumulative branch NPE downstream.
        So ``end_cutoff`` can only clamp — it cannot pin the end the way
        ``start_cutoff`` can pin the start.
    :type end_offset: Optional[int]
    :param start_cutoff:
        Fixed floor on the resolved range start (inclusive), as a partition
        string (e.g. ``"2024-01-01"``). Combined with ``start_offset`` as above.
    :type start_cutoff: Optional[str]
    :param end_cutoff:
        Fixed ceiling on the resolved range end (inclusive), as a partition
        string. Combined with ``end_offset`` as above.
    :type end_cutoff: Optional[str]
    """

    table: str
    partition_column: Optional[str] = None
    partition_format: Optional[str] = None
    additional_partitions: Optional[List[str]] = None
    offset: Optional[int] = None
    start_offset: Optional[int] = None
    end_offset: Optional[int] = None
    start_cutoff: Optional[str] = None
    end_cutoff: Optional[str] = None
    time_partitioned: Optional[bool] = None

    def resolved_offsets(self) -> Tuple[Optional[int], int]:
        """Resolve ``(start_offset, end_offset)`` from the dataclass fields using
        the same precedence as ``to_thrift``. ``start`` may be ``None`` (pin at
        ``start_cutoff``); ``end`` is always an ``int``."""
        if self.start_offset is not None:
            start = self.start_offset
        elif self.offset is not None:
            start = self.offset
        elif self.start_cutoff is not None:
            # Null startOffset + startCutOff pins the resolved start at the cutoff
            # (DependencyResolver: max(null, cutoff) = cutoff).
            start = None
        else:
            start = 0

        if self.end_offset is not None:
            end = self.end_offset
        elif self.offset is not None:
            end = self.offset
        else:
            # Keep endOffset non-null: with no end_cutoff either, the resolver
            # returns PartitionRange(_, null) and BatchNodeRunner.requiredEnd /
            # the cumulative branch NPE downstream.
            end = 0

        return start, end

    def to_thrift(self):
        if self.offset is not None:
            warnings.warn(
                f"TableDependency `offset` is deprecated; use `start_offset` and/or `end_offset` "
                f"(table={self.table}).",
                DeprecationWarning,
                stacklevel=2,
            )

        resolved_start_offset, resolved_end_offset = self.resolved_offsets()

        return common.TableDependency(
            tableInfo=common.TableInfo(
                table=self.table,
                partitionColumn=self.partition_column,
                partitionFormat=self.partition_format,
                partitionInterval=common.Window(1, common.TimeUnit.DAYS),
                timePartitioned=self.time_partitioned,
            ),
            startOffset=(
                None
                if resolved_start_offset is None
                else common.Window(length=resolved_start_offset, timeUnit=common.TimeUnit.DAYS)
            ),
            endOffset=common.Window(length=resolved_end_offset, timeUnit=common.TimeUnit.DAYS),
            startCutOff=self.start_cutoff,
            endCutOff=self.end_cutoff,
        )


def StagingQuery(
    query: str,
    version: Optional[int] = None,
    output_namespace: Optional[str] = None,
    table_properties: Optional[Dict[str, str]] = None,
    setups: Optional[List[str]] = None,
    engine_type: Optional[EngineType] = None,
    dependencies: Optional[List[Union[TableDependency, Dict]]] = None,
    tags: Optional[Dict[str, str]] = None,
    # execution params
    offline_schedule: str = "@daily",
    conf: Optional[common.ConfigProperties] = None,
    env_vars: Optional[common.EnvironmentVariables] = None,
    cluster_conf: common.ClusterConfigProperties = None,
    step_days: Optional[int] = None,
    recompute_days: Optional[int] = None,
    additional_partitions: List[str] = None,
    environments: Optional[List[str]] = None,
) -> ttypes.StagingQuery:
    """
    Creates a StagingQuery object for executing arbitrary SQL queries with templated date parameters.

    :param query:
        Arbitrary spark query that should be written with template parameters:
        - `{{ start_date }}`: Initial run uses start_date, future runs use latest partition + 1 day
        - `{{ end_date }}`: The end partition of the computing range
        - `{{ latest_date }}`: End partition independent of the computing range (for cumulative sources)
        - `{{ max_date(table=namespace.my_table) }}`: Max partition available for a given table
        These parameters can be modified with offset and bounds:
        - `{{ start_date(offset=-10, lower_bound='2023-01-01', upper_bound='2024-01-01') }}`
    :type query: str
    :param setups:
        Spark SQL setup statements. Used typically to register UDFs.
    :type setups: List[str]
    :type partition_column: str
    :param engine_type:
        By default, spark is the compute engine. You can specify an override (eg. bigquery, etc.)
        Use the EngineType class constants: EngineType.SPARK, EngineType.BIGQUERY, etc.
    :type engine_type: int
    :param tags:
        Additional metadata that does not directly affect computation, but is useful for management.
    :type tags: Dict[str, str]
    :param offline_schedule:
        The offline schedule interval for batch jobs. Supports standard cron expressions
        that run at most once per day. Examples:
        '@daily': Legacy format for midnight daily execution
        '0 2 * * *': Daily at 2:00 AM
        '30 14 * * MON-FRI': Weekdays at 2:30 PM
        '0 9 * * 1': Mondays at 9:00 AM
        '15 23 * * SUN': Sundays at 11:15 PM
        '@never': Explicitly disable offline scheduling
        Note: Hourly, sub-hourly, or multi-daily schedules are not supported.
    :type offline_schedule: str
    :param conf:
        Configuration properties for the StagingQuery.
    :type conf: common.ConfigProperties
    :param env_vars:
        Environment variables for the StagingQuery.
    :type env_vars: common.EnvironmentVariables
    :param cluster_conf:
        Cluster configuration properties for the join.
    :param step_days:
        The maximum number of days to process at once
    :type step_days: int
    :param dependencies:
        List of dependencies for the StagingQuery. Each dependency can be either a TableDependency object
        or a dictionary with 'name' and 'spec' keys.
    :type dependencies: List[Union[TableDependency, Dict]]
    :param recompute_days:
        Used by orchestrator to determine how many days are recomputed on each incremental scheduled run. Should be
        set when the source data is changed in-place (i.e. existing partitions overwritten with new data each day up to
        X days later) or when you want partially mature aggregations (i.e. a 7 day window, but start computing it from
        day 1, and refresh it for the next 6 days)
    :type recompute_days: int
    :param environments:
        List of environments where this StagingQuery should be deployed/available.
        Defaults to ['prod']. Valid values: 'prod', 'canary' (case-insensitive).
    :type environments: List[str]
    :return:
        A StagingQuery object
    """
    # `environments` is left unset (None) when the author doesn't specify it.
    # Downstream consumers (e.g. hub schedule-all) default the missing/empty
    # case to ['prod']. Keeping it unset on disk avoids baking a default into
    # every compiled conf.
    if environments:
        environments = utils.convert_environments_to_enum(environments)

    # Get caller's filename to assign team
    team = utils._get_team_from_caller()

    assert version is None or isinstance(version, int), (
        f"Version must be an integer or None, but found {type(version).__name__}"
    )
    tables_in_query = [
        _normalize_table_name(t)
        for t in _tables_in_query(
            query,
            dialect=ttypes.EngineType._VALUES_TO_NAMES[engine_type].lower()
            if engine_type
            else "spark",
        )
    ]
    if tables_in_query:
        assert dependencies is not None, "Dependencies must be specified if tables are in the query"
        spec_names = set()
        for d in dependencies:
            if isinstance(d, TableDependency):
                spec_names.add(_normalize_table_name(d.table))
            elif isinstance(d, dict):
                spec_names.add(_normalize_table_name(d["spec"].split("/")[0]))
        mismatch = set(tables_in_query) - set(spec_names)
        if mismatch:
            # There can be staging queries that run on top of views which are not handled as dependencies right now.
            # So it's better to avoid this validation until views are handled for both Eval and / or Hub
            # raise ValueError(f"Tables in query but not in dependencies: {mismatch}\nDependencies: {spec_names}\nTables in query: {tables_in_query}")
            pass

    # Create execution info
    exec_info = common.ExecutionInfo(
        offlineSchedule=offline_schedule,
        conf=conf,
        env=env_vars,
        stepDays=step_days,
        clusterConf=cluster_conf,
    )

    airflow_dependencies = []

    if dependencies:
        for d in dependencies:
            if isinstance(d, TableDependency) and d.partition_column is not None:
                _, end_offset = d.resolved_offsets()
                airflow_dependency = airflow_helpers.create_airflow_dependency(
                    d.table,
                    d.partition_column,
                    d.additional_partitions,
                    offset=end_offset,
                    end_cutoff=d.end_cutoff,
                    partition_format=d.partition_format,
                )
                airflow_dependencies.append(airflow_dependency)
            elif isinstance(d, dict):
                # If it's already a dictionary, just append it
                airflow_dependencies.append(d)
            elif isinstance(d, TableDependency) and d.partition_column is None:
                # NoOp
                pass
            else:
                raise ValueError(
                    "Dependencies must be either TableDependency instances or dictionaries."
                )

    custom_json = json.dumps({AIRFLOW_DEPENDENCIES_KEY: airflow_dependencies})

    # Create metadata
    meta_data = ttypes.MetaData(
        outputNamespace=output_namespace,
        team=team,
        executionInfo=exec_info,
        tags=tags,
        customJson=custom_json,
        tableProperties=table_properties,
        version=str(version) if version is not None else None,
        additionalOutputPartitionColumns=additional_partitions,
        environments=environments,
    )

    thrift_deps = []
    if dependencies and len(dependencies) > 0:
        for d in dependencies:
            if d and isinstance(d, TableDependency):
                thrift_deps.append(d.to_thrift())

    # Create and return the StagingQuery object with camelCase parameter names
    staging_query = ttypes.StagingQuery(
        metaData=meta_data,
        query=query,
        setups=setups,
        engineType=engine_type,
        tableDependencies=thrift_deps,
        recomputeDays=recompute_days,
    )

    # Add the table property that calls the private function
    staging_query.__class__.table = property(
        lambda self: _get_output_table_name(self, full_name=True)
    )

    return mark_factory_created_config(staging_query)
