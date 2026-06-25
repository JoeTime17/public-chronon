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

import copy
import gc
import importlib
import logging
from collections import Counter
from typing import Dict, List, Optional, Union

import ai.chronon.utils as utils
import ai.chronon.windows as window_utils
import gen_thrift.api.ttypes as api
import gen_thrift.common.ttypes as common
from ai.chronon.cli.compile.config_origin import mark_factory_created_config
from ai.chronon.data_types import DataType, FieldsType

logging.basicConfig(level=logging.INFO)


def _get_output_table_name(join: api.Join, full_name: bool = False):
    """generate output table name for join backfill job"""
    return utils._ensure_name_and_get_output_table_reference(join, api.Join, "joins", full_name)


def _get_derived_output_table_name(join: api.Join, full_name: bool = False):
    """Generate output table name for modular join derivation output."""
    return utils._ensure_name_and_get_output_table_reference(
        join, api.Join, "joins", full_name, suffix="__derived"
    )


def JoinPart(
    group_by: api.GroupBy,
    key_mapping: Dict[str, str] = None,
    prefix: str = None,
    tags: Dict[str, str] = None,
) -> api.JoinPart:
    """
    Specifies HOW to join the `left` of a Join with GroupBy's.

    :param group_by:
        The GroupBy object to join with. Keys on left are used to equi join with keys on right.
        When left is entities all GroupBy's are computed as of midnight.
        When left is events, we do a point-in-time join when right.accuracy == TEMPORAL OR right.source.topic != null
    :type group_by: ai.chronon.api.GroupBy
    :param key_mapping:
        Names of keys don't always match on left and right, this mapping tells us how to map when they don't.
    :type key_mapping: Dict[str, str]
    :param prefix:
        All the output columns of the groupBy will be prefixed with this string. This is used when you need to join
        the same groupBy more than once with `left`. Say on the left you have seller and buyer, on the group you have
        a user's avg_price, and you want to join the left (seller, buyer) with (seller_avg_price, buyer_avg_price) you
        would use key_mapping and prefix parameters.
    :param tags:
        Additional metadata about the JoinPart that you wish to track. Does not effect computation.
    :type tags: Dict[str, str]
    :return:
        JoinPart specifies how the left side of a join, or the query in online setting, would join with the right side
        components like GroupBys.
    """

    assert isinstance(group_by, api.GroupBy), (
        f"Expecting GroupBy. But found {type(group_by).__name__}"
    )

    # Resolve group_by's module via GC referrers and set metadata names
    gc.collect()

    group_by_module_name = None
    for ref in gc.get_referrers(group_by):
        if (
            isinstance(
                ref, dict
            )  # Attaching methods to GroupBy adds references in GC, need to filter out
            and "__name__" in ref
            and ref["__name__"].startswith("group_bys")
        ):
            group_by_module_name = ref["__name__"]
            break

    if group_by_module_name:
        logging.debug(
            "group_by's module info from garbage collector {}".format(group_by_module_name)
        )
        group_by_module = importlib.import_module(group_by_module_name)
        utils._import_module_set_name(group_by_module, api.GroupBy)
    else:
        if not group_by.metaData.name:
            logging.error("No group_by file or custom group_by name found")
            raise ValueError(
                "[GroupBy] Must specify a group_by name if group_by is not defined in separate file. "
                "You may pass it in via GroupBy.name. \n"
            )

    if key_mapping:
        try:
            utils.check_contains(
                key_mapping.values(), group_by.keyColumns, "key", group_by.metaData.name
            )
        except AssertionError as e:
            raise ValueError(
                f"Invalid key_mapping for JoinPart on GroupBy '{group_by.metaData.name}'.\n"
                f"key_mapping format is {{left_column: group_by_key}}.\n"
                f"Valid GroupBy keys: {group_by.keyColumns}\n"
                f"Provided key_mapping: {key_mapping}\n"
                f"Bad value(s): {e}"
            ) from None

    join_part = api.JoinPart(groupBy=group_by, keyMapping=key_mapping, prefix=prefix)
    join_part.tags = tags
    return join_part


def _window_millis_or_none(window) -> Optional[int]:
    return window_utils.window_millis(window) if window is not None else None


def _format_millis(millis: int) -> str:
    if millis % window_utils.DAY_MILLIS == 0:
        return f"{millis // window_utils.DAY_MILLIS}d"
    if millis % window_utils.HOUR_MILLIS == 0:
        return f"{millis // window_utils.HOUR_MILLIS}h"
    return f"{millis // window_utils.MINUTE_MILLIS}m"


def _validate_online_join_subdaily_group_by_schedules(right_parts: List[api.JoinPart]) -> None:
    """Online joins schedule sub-daily groupBy uploads independently."""
    for part in right_parts or []:
        group_by = part.groupBy
        metadata = getattr(group_by, "metaData", None)
        execution_info = getattr(metadata, "executionInfo", None)
        output_info = getattr(execution_info, "outputTableInfo", None)
        output_interval_ms = _window_millis_or_none(
            getattr(output_info, "partitionInterval", None)
        )
        if output_interval_ms is None or output_interval_ms >= window_utils.DAY_MILLIS:
            continue

        group_by_name = getattr(metadata, "name", None) or "<unnamed>"
        schedule = (
            getattr(execution_info, "onlineSchedule", None)
            or getattr(execution_info, "offlineSchedule", None)
        )
        normalized_schedule = schedule.strip().lower() if isinstance(schedule, str) else schedule
        if normalized_schedule in (None, "", "none", "null", "@daily", "@never"):
            raise ValueError(
                f"Online Join includes sub-daily GroupBy '{group_by_name}' "
                f"(partition_interval={_format_millis(output_interval_ms)}) without a regular "
                "sub-daily offline_schedule or online_schedule. Declare the GroupBy schedule so "
                "the orchestrator can schedule its upload independently."
            )

        try:
            schedule_interval_ms = window_utils.regular_subdaily_schedule(schedule)
        except ValueError as e:
            raise ValueError(
                f"Online Join includes sub-daily GroupBy '{group_by_name}' with invalid "
                f"schedule '{schedule}': {e}"
            ) from None

        if schedule_interval_ms is None:
            raise ValueError(
                f"Online Join includes sub-daily GroupBy '{group_by_name}' "
                f"(partition_interval={_format_millis(output_interval_ms)}) without a regular "
                "sub-daily offline_schedule or online_schedule. Declare the GroupBy schedule so "
                "the orchestrator can schedule its upload independently."
            )

        if schedule_interval_ms != output_interval_ms:
            raise ValueError(
                f"Online Join includes sub-daily GroupBy '{group_by_name}' with schedule "
                f"'{schedule}' ({_format_millis(schedule_interval_ms)}) but output "
                f"partition_interval={_format_millis(output_interval_ms)}. Sub-daily groupBy "
                "uploads that feed online joins must be scheduled at their own partition cadence."
            )


def ExternalSource(
    name: str,
    team: str,
    key_fields: FieldsType,
    value_fields: FieldsType,
) -> api.ExternalSource:
    """
    External sources are online only data sources. During fetching, using
    chronon java client, they consume a Request containing a key map
    (name string to value). And produce a Response containing a value map.

    This is primarily used in Joins. We also expose a fetchExternal method in
    java client library that can be used to fetch a batch of External source
    requests efficiently.

    Internally Chronon will batch these requests to the service and parallelize
    fetching from different services, while de-duplicating given a batch of
    join requests.

    The implementation of how to fetch is an `ExternalSourceHandler` in
    scala/java api that needs to be registered while implementing
    ai.chronon.online.Api with the name used in the ExternalSource. This is
    meant for re-usability of external source definitions.

    :param name: name of the external source to fetch from. Should match
                 the name in the registry.
    :param key_fields: List of tuples of string and DataType. This is what
        will be given to ExternalSource handler registered in Java API.
        Eg., `[('key1', DataType.INT, 'key2', DataType.STRING)]`
    :param value_fields: List of tuples of string and DataType. This is what
        the ExternalSource handler will respond with::

            [
                ('value0', DataType.INT),
                ('value1', DataType.MAP(DataType.STRING, DataType.LONG),
                ('value2', DataType.STRUCT(
                    name = 'Context',
                    ('field1', DataType.INT),
                    ('field2', DataType.DOUBLE)
                ))
            ]

    """
    assert name != "contextual", "Please use `ContextualSource`"
    return api.ExternalSource(
        metadata=api.MetaData(name=name, team=team),
        keySchema=DataType.STRUCT(f"ext_{name}_keys", *key_fields),
        valueSchema=DataType.STRUCT(f"ext_{name}_values", *value_fields),
    )


def ContextualSource(fields: FieldsType, team="default") -> api.ExternalSource:
    """
    Contextual source values are passed along for logging. No external request is
    actually made.
    """
    return api.ExternalSource(
        metadata=api.MetaData(name="contextual", team=team),
        keySchema=DataType.STRUCT("contextual_keys", *fields),
        valueSchema=DataType.STRUCT("contextual_values", *fields),
    )


def ExternalPart(
    source: api.ExternalSource, key_mapping: Dict[str, str] = None, prefix: str = None
) -> api.ExternalPart:
    """
    Used to describe which ExternalSources to pull features from while fetching
    online. This data also goes into logs based on sample percent.

    Just as in JoinPart, key_mapping is used to map the join left's keys to
    external source's keys. "vendor" and "buyer" on left side (query map)
    could both map to a "user" in an account data external source. You would
    create one ExternalPart for vendor with params:
    `(key_mapping={vendor: user}, prefix=vendor)`
    another for buyer.

    This doesn't have any implications offline besides logging. "right_parts"
    can be both backfilled and logged. Whereas, "external_parts" can only be
    logged. If you need the ability to backfill an external source, look into
    creating an EntitySource with mutation data for point-in-time-correctness.

    :param source: External source to join with
    :param key_mapping: How to map the keys from the query/left side to the
                        source
    :param prefix: Sometime you want to use the same source to fetch data for
                   different entities in the query. Eg., A transaction
                   between a buyer and a seller might query "user information"
                   serivce/source that has information about both buyer &
                   seller
    """
    return api.ExternalPart(source=source, keyMapping=key_mapping, prefix=prefix)


def Derivation(name: str, expression: str) -> api.Derivation:
    """
    .. deprecated::
        Use ``from ai.chronon.types import Derivation`` instead.
    """
    import warnings

    warnings.warn(
        "Importing Derivation from ai.chronon.join is deprecated. "
        "Use 'from ai.chronon.types import Derivation' instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    from ai.chronon.derivation import Derivation as _Derivation

    return _Derivation(name=name, expression=expression)


def BootstrapPart(
    table: str, key_columns: List[str] = None, query: api.Query = None
) -> api.BootstrapPart:
    """
    Bootstrap is the concept of using pre-computed feature values and skipping backfill computation during the
    training data generation phase. Bootstrap can be used for many purposes:
    - Generating ongoing feature values from logs
    - Backfilling feature values for external features (in which case Chronon is unable to run backfill)
    - Initializing a new Join by migrating old data from an older Join and reusing data

    One can bootstrap against any of these:

    - join part fields:
        Bootstrap can happen at individual field level within a join part.
        If all fields within a group by are bootstrapped, then we skip computation for group by. Otherwise, the whole
        thing will be re-run but only the values for the non-bootstrapped fields will be retained in the final table.
    - external part fields:
        Bootstrap can happen at individual field level within an external part.
        Since there is no backfill logic in chronon for external part, all non-bootstrapped fields in external parts
        are left as NULLs.
    - derivation fields:
        Derived fields can also be bootstrapped. Since derived fields depend on "base" fields (either join part or
        external part), chronon will try to trigger the least amount of computation possible. For example,
        if there is a join part where all derived fields that depend on the join part have been bootstrapped,
        then we skip the computation for this join part.
    - keys:
        Keys of both join parts and external parts can be bootstrapped. During offline table generation, we will first
        try to utilize key's data from left table; if it's not there, then we utilize bootstrap.
        For contextual features, we also support propagating the key bootstrap to the values.


    :param table: Name of hive table that contains feature values where rows are 1:1 mapped to left table
    :param key_columns: Keys to join bootstrap table to left table
    :param query: Selected columns (features & keys) and filtering conditions of the bootstrap tables.
    """
    return api.BootstrapPart(table=table, query=query, keyColumns=key_columns)


def Join(
    left: api.Source,
    right_parts: List[api.JoinPart],
    row_ids: Optional[Union[str, List[str]]] = None,
    version: Optional[int] = None,
    online_external_parts: List[api.ExternalPart] = None,
    bootstrap_parts: List[api.BootstrapPart] = None,
    bootstrap_from_log: bool = False,
    skew_keys: Dict[str, List[str]] = None,
    derivations: List[api.Derivation] = None,
    output_namespace: str = None,
    table_properties: Dict[str, str] = None,
    online: bool = False,
    production: bool = False,
    sample_percent: float = 100.0,
    check_consistency: bool = None,
    consistency_sample_percent: float = 5.0,
    use_long_names: bool = False,
    # execution params
    offline_schedule: str = "@daily",
    online_schedule: str = None,
    historical_backfill: bool = None,
    conf: common.ConfigProperties = None,
    env_vars: common.EnvironmentVariables = None,
    cluster_conf: common.ClusterConfigProperties = None,
    step_days: int = None,
    enable_stats_compute: bool = None,
    modular_execution: bool = False,
    environments: Optional[List[str]] = None,
    partition_interval: Optional[Union[common.Window, str]] = None,
    partition_offset: Optional[Union[common.Window, str]] = None,
) -> api.Join:
    """
    Construct a join object. A join can pull together data from various GroupBy's both offline and online. This is also
    the focal point for logging, data quality computation and monitoring. A join maps 1:1 to models in ML usage.

    :param left:
        The source on the left side, when Entities, all GroupBys are join with SNAPSHOT accuracy (midnight values).
        When left is events, if on the right, either when GroupBy's are TEMPORAL, or when topic is specified, we perform
        a TEMPORAL / point-in-time join.
    :type left: ai.chronon.api.Source
    :param right_parts:
        The list of groupBy's to join with. GroupBy's are wrapped in a JoinPart, which contains additional information
        on how to join the left side with the GroupBy.
    :type right_parts: List[ai.chronon.api.JoinPart]
    :param check_consistency:
        If online serving data should be compared with backfill data - as online-offline-consistency metrics.
        The metrics go into hive and your configured kv store for further visualization and monitoring.
    :type check_consistency: bool
    :param additional_args:
        Additional args go into `customJson` of `ai.chronon.api.MetaData` within the `ai.chronon.api.Join` object.
        This is a place for arbitrary information you want to tag your conf with.
    :type additional_args: List[str]
    :param additional_env:
        Deprecated, see env
    :type additional_env: List[str]
    :param online:
        Should we upload this conf into kv store so that we can fetch/serve this join online.
        Once Online is set to True, you ideally should not change the conf.
    :type online: bool
    :param production:
        This when set can be integrated to trigger alerts. You will have to integrate this flag into your alerting
        system yourself.
    :type production: bool
    :param output_namespace:
        In backfill mode, we will produce data into hive. This represents the hive namespace that the data will be
        written into. You can set this at the teams.json level.
    :type output_namespace: str
    :param table_properties:
        Specifies the properties on output hive tables. Can be specified in teams.json.
    :param lag:
        Param that goes into customJson. You can pull this out of the json at path "metaData.customJson.lag"
        This is used by airflow integration to pick an older hive partition to wait on.
    :param skew_keys:
        While back-filling, if there are known irrelevant keys - like user_id = 0 / NULL etc. You can specify them here.
        This is used to blacklist crawlers etc
    :param sample_percent:
        Online only parameter. What percent of online serving requests to this join should be logged into warehouse.
    :param consistency_sample_percent:
        Online only parameter. What percent of online serving requests to this join should be sampled to compute
        online offline consistency metrics.
        if sample_percent=50.0 and consistency_sample_percent=10.0, then basically the consistency job runs on
        5% of total traffic.
    :param online_external_parts:
        users can register external sources into Api implementation. Chronon fetcher can invoke the implementation.
        This is applicable only for online fetching. Offline this will not be produce any values.
    :param offline_schedule:
        Schedule expression for offline join compute tasks. Supports standard cron expressions,
        including regular sub-daily schedules. Examples: '@daily' (midnight), '0 2 * * *' (2am daily),
        '0 */3 * * *' (every 3 hours), '5/15 * * * *' (every 15 minutes with a 5 minute processing offset).
        Use '@never' to explicitly disable offline scheduling.
    :param online_schedule:
        Schedule expression for online/deploy tasks. When online=True and online_schedule is not specified,
        defaults to offline_schedule. Set to "@never" to explicitly disable online scheduling even when online=True.
        Supports the same format as offline_schedule.
    :param partition_interval:
        Output partition interval for this Join. Examples: "1d", "3h", "15m".
        When set below daily, Chronon uses "yyyy-MM-dd-HH-mm" ds values.
    :param partition_offset:
        Offset from UTC midnight/epoch for the output partition grid. Defaults to zero
        (boundaries at midnight) and must be declared explicitly to move the grid; the cron
        fire phase is treated as a processing delay relative to the declared grid, never as
        a grid offset.
    :param row_ids:
        Columns of the left table that uniquely define a training record. Used as default keys during bootstrap.
        Optional.
    :param bootstrap_parts:
        A list of BootstrapPart used for the Join. See BootstrapPart doc for more details
    :param bootstrap_from_log:
        If set to True, will use logging table to generate training data by default and skip continuous backfill.
        Logging will be treated as another bootstrap source, but other bootstrap_parts will take precedence.
    :param historical_backfill:
        Flag to indicate whether join backfill should backfill previous holes.
        Setting to false will only backfill latest single partition
    :type historical_backfill: bool
    :return:
        A join object that can be used to backfill or serve data. For ML use-cases this should map 1:1 to model.
    :param conf:
        Configuration properties for the join. Depending on the mode we layer confs with the following priority:
        1. conf set in the join.conf.<mode>
        2. conf set in the join.conf.common
        3. conf set in the team.conf.<mode>
        4. conf set in the team.conf.common
        5. conf set in the default.conf.<mode>
        6. conf set in the default.conf.common
    :param env_vars:
        Environment variables for the join. Depending on the mode we layer envs with the following priority:
        1. env vars set in the join.env.<mode>
        2. env vars set in the join.env.common
        3. env vars set in the team.env.<mode>
        4. env vars set in the team.env.common
        5. env vars set in the default.env.<mode>
        6. env vars set in the default.env.common
    :param cluster_conf:
        Cluster configuration properties for the join.
    :param step_days:
        The maximum number of days to output at once
    :param enable_stats_compute:
        Whether to enable enhanced statistics computation and upload for this join.
        When True, stats compute and upload nodes will be added to the workflow.
    :type enable_stats_compute: bool
    :param modular_execution:
        When True, uses modular join planning (JoinPlanner) instead of the default
        monolith planner (MonolithJoinPlanner).
    :type modular_execution: bool
    :param environments:
        List of environments where this join should be deployed/available.
        Defaults to ['prod']. Valid values: 'prod', 'canary' (case-insensitive).
    :type environments: List[str]
    """
    # Normalize row_ids
    if isinstance(row_ids, str):
        row_ids = [row_ids]

    # `environments` is left unset (None) when the author doesn't specify it.
    # Downstream consumers (e.g. hub schedule-all) default the missing/empty
    # case to ['prod']. Keeping it unset on disk avoids baking a default into
    # every compiled conf.
    if environments:
        environments = utils.convert_environments_to_enum(environments)

    assert version is None or isinstance(version, int), (
        f"Version must be an integer or None, but found {type(version).__name__}"
    )

    # Normalize before deepcopy so a left JoinSource can resolve the nested
    # join's metadata name from its defining module. Without this, the copied
    # inner join can keep a null name and later resolve to "<namespace>.null".
    normalized_left = utils.normalize_source(left, output_namespace)

    # create a deep copy for case: multiple LeftOuterJoin use the same left,
    # validation will fail after the first iteration
    updated_left = copy.deepcopy(normalized_left)
    if left.events and left.events.query.selects:
        assert "ts" not in left.events.query.selects.keys(), (
            "'ts' is a reserved key word for Chronon, please specify the expression in timeColumn"
        )
        # mapping ts to query.timeColumn to events only
        updated_left.events.query.selects.update({"ts": updated_left.events.query.timeColumn})

    consistency_sample_percent = consistency_sample_percent if check_consistency else None

    # external parts need to be unique on (prefix, part.source.metaData.name)
    if online_external_parts:
        count_map = Counter(
            [(part.prefix, part.source.metadata.name) for part in online_external_parts]
        )
        has_duplicates = False
        for key, count in count_map.items():
            if count > 1:
                has_duplicates = True
                print(f"Found {count - 1} duplicate(s) for external part {key}")
        assert has_duplicates is False, "Please address all the above mentioned duplicates."

    if bootstrap_from_log:
        has_logging = sample_percent > 0 and online
        assert has_logging, (
            "Join must be online with sample_percent set in order to use bootstrap_from_log option"
        )
        bootstrap_parts = (bootstrap_parts or []) + [
            api.BootstrapPart(
                # templated values will be replaced when metaData.name is set at the end
                table="{{ logged_table }}"
            )
        ]

    # Validate online_schedule based on online flag
    if not online and online_schedule is not None and online_schedule != "@never":
        raise ValueError(
            "online_schedule cannot be set when online=False. "
            "Either set online=True or remove the online_schedule parameter."
        )

    subdaily_output = window_utils.is_subdaily(partition_interval, offline_schedule)

    # "@never" explicitly disables online scheduling even when online=True
    if online_schedule == "@never":
        online_schedule = None
    # Set default online_schedule if online is True and online_schedule is not specified.
    # Inherit the offline schedule only when it is sub-daily; daily confs keep the
    # historical "@daily" default so existing semantic hashes don't churn.
    elif online and online_schedule is None:
        if subdaily_output and offline_schedule not in (None, "@never"):
            online_schedule = offline_schedule
        else:
            online_schedule = "@daily"
    elif (
        online
        and subdaily_output
        and online_schedule is not None
        and offline_schedule not in (None, "@never")
        and online_schedule != offline_schedule
    ):
        raise ValueError(
            "online_schedule must match offline_schedule for sub-daily partition_interval when both are set. "
            "Leave online_schedule empty to inherit the offline schedule."
        )

    if online:
        _validate_online_join_subdaily_group_by_schedules(right_parts)

    if modular_execution:
        if conf is None:
            conf = common.ConfigProperties(common={"modular_execution": "true"})
        else:
            if conf.common is None:
                conf.common = {"modular_execution": "true"}
            else:
                conf.common["modular_execution"] = "true"

    exec_info = common.ExecutionInfo(
        offlineSchedule=offline_schedule,
        onlineSchedule=online_schedule,
        conf=conf,
        env=env_vars,
        stepDays=step_days,
        historicalBackfill=historical_backfill,
        clusterConf=cluster_conf,
        enableStatsCompute=enable_stats_compute,
        outputTableInfo=window_utils.output_table_info(
            partition_interval,
            partition_offset=partition_offset,
            schedule=offline_schedule,
        ),
    )

    output_info = exec_info.outputTableInfo
    if (
        output_info is not None
        and window_utils.window_millis(output_info.partitionInterval) < window_utils.DAY_MILLIS
    ):
        # the left feeds the join's own partitions; right parts pick the latest snapshot at
        # or before each row's ts on their own grid
        # (mixed hourly/daily/realtime cadence is the product) and are deliberately NOT checked
        left_inner = updated_left.events or updated_left.entities or updated_left.joinSource
        left_table = getattr(left_inner, "table", None) or getattr(left_inner, "snapshotTable", None)
        window_utils.validate_source_grid(
            "This Join", window_utils.source_query(updated_left), f"left source {left_table}"
        )

    metadata = api.MetaData(
        online=online,
        production=production,
        outputNamespace=output_namespace,
        tableProperties=table_properties,
        samplePercent=sample_percent,
        consistencyCheck=check_consistency,
        consistencySamplePercent=consistency_sample_percent,
        executionInfo=exec_info,
        version=str(version) if version is not None else None,
        environments=environments,
    )

    join = api.Join(
        left=updated_left,
        joinParts=right_parts,
        metaData=metadata,
        skewKeys=skew_keys,
        onlineExternalParts=online_external_parts,
        bootstrapParts=bootstrap_parts,
        rowIds=row_ids,
        derivations=derivations,
        useLongNames=use_long_names,
    )

    # Add the table property that calls the private function
    join.__class__.table = property(lambda self: _get_output_table_name(self, full_name=True))
    join.__class__.derived_table = property(
        lambda self: _get_derived_output_table_name(self, full_name=True)
    )

    return mark_factory_created_config(join)
