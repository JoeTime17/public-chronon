"""Object for checking whether a Chronon API thrift object is consistent with other"""

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

import json
import logging
import re
import sys
import textwrap
from collections import defaultdict
from dataclasses import dataclass
from typing import List, Optional, Tuple

import gen_thrift.common.ttypes as common
from ai.chronon.cli.compile.column_hashing import (
    compute_group_by_columns_hashes,
    get_pre_derived_group_by_columns,
    get_pre_derived_group_by_features,
    get_pre_derived_join_features,
    get_pre_derived_source_keys,
)
from ai.chronon.cli.compile.version_utils import is_version_change
from ai.chronon.cli.theme import console
from ai.chronon.logger import get_logger
from ai.chronon.repo.serializer import thrift_simple_json
from ai.chronon.utils import get_query, get_root_source
from gen_thrift.api.ttypes import (
    Accuracy,
    Aggregation,
    Derivation,
    EventSource,
    GroupBy,
    Join,
    JoinPart,
    Query,
    Source,
)

# Fields that indicate status of the entities.
SKIPPED_FIELDS = frozenset(["metaData", "onlineExternalParts"])


@dataclass
class ConfigChange:
    """Represents a change to a compiled config object."""

    name: str
    obj_type: str
    online: bool = False
    production: bool = False
    base_name: str = None
    old_version: int = None
    new_version: int = None
    is_version_change: bool = False


def _output_grid_token(obj) -> Optional[str]:
    """The output partition grid is a data-layout property (changing it renames every ds in
    the output table), so unlike the rest of metaData it participates in in-place change
    detection --
    via a conditional token so daily/unset grids contribute nothing and existing confs never
    churn. The schedule (and hence the derived delay) stays excluded. Canonicalized to millis
    so Window(1, DAYS) == Window(24, HOURS) == unset. Mirrors MetadataOps.outputGridToken in
    scala Extensions."""
    from ai.chronon.windows import DAY_MILLIS, window_millis

    meta = getattr(obj, "metaData", None)
    exec_info = getattr(meta, "executionInfo", None) if meta else None
    info = getattr(exec_info, "outputTableInfo", None) if exec_info else None
    if info is None or info.partitionInterval is None:
        return None
    interval_ms = window_millis(info.partitionInterval)
    offset_ms = window_millis(info.partitionOffset) if info.partitionOffset else 0
    if interval_ms == DAY_MILLIS and offset_ms == 0:
        return None
    return f"grid:interval_ms={interval_ms},offset_ms={offset_ms}"


def _strip_fields_recursive(obj, fields_to_strip):
    """Recursively remove specified fields from all dicts in a JSON tree."""
    if isinstance(obj, dict):
        for field in fields_to_strip:
            obj.pop(field, None)
        for value in obj.values():
            _strip_fields_recursive(value, fields_to_strip)
    elif isinstance(obj, list):
        for item in obj:
            _strip_fields_recursive(item, fields_to_strip)


def _is_batch_upload_needed(group_by: GroupBy) -> bool:
    if group_by.metaData.online:
        return True
    else:
        return False


def is_identifier(s: str) -> bool:
    identifier_regex = re.compile(r"[a-zA-Z_][a-zA-Z0-9_]*")
    return re.fullmatch(identifier_regex, s) is not None


def _source_has_topic(source: Source) -> bool:
    if source.events:
        return source.events.topic is not None
    elif source.entities:
        return source.entities.mutationTopic is not None
    elif source.joinSource:
        return _source_has_topic(source.joinSource.join.left)
    return False


def _group_by_has_topic(groupBy: GroupBy) -> bool:
    return any(_source_has_topic(source) for source in groupBy.sources)


def _group_by_has_hourly_windows(groupBy: GroupBy) -> bool:
    aggs: List[Aggregation] = groupBy.aggregations

    if not aggs:
        return False

    for agg in aggs:
        if not agg.windows:
            return False

        for window in agg.windows:
            if window.timeUnit == common.TimeUnit.HOURS:
                return True

    return False


def detect_feature_name_collisions(
    group_bys: List[Tuple[GroupBy, str]], entity_set_type: str, name: str
) -> BaseException | None:
    # Build a map of output_column -> set of group_by name
    output_col_to_gbs = {}
    for gb, prefix in group_bys:
        jp_prefix_str = f"{prefix}_" if prefix else ""
        key_str = "_".join(gb.keyColumns)
        prefix_str = jp_prefix_str + key_str + "_"
        cols = compute_group_by_columns_hashes(gb, exclude_keys=True)
        if not cols:
            print("HERE")
        cols = {
            f"{prefix_str}{base_col}"
            for base_col in list(compute_group_by_columns_hashes(gb, exclude_keys=True).keys())
        }
        gb_name = gb.metaData.name
        for col in cols:
            if col not in output_col_to_gbs:
                output_col_to_gbs[col] = set()
            output_col_to_gbs[col].add(gb_name)

    # Find output columns that are produced by more than one group_by
    collisions = {col: gb_names for col, gb_names in output_col_to_gbs.items() if len(gb_names) > 1}

    if not collisions:
        return None  # no collisions

    # Assemble an error message listing the conflicting GroupBys by name
    lines = [f"{entity_set_type} for Join: {name} has the following output name collisions:\n"]
    for col, gb_names in collisions.items():
        names_str = ", ".join(sorted(gb_names))
        lines.append(f"  - [{col}] has collisions from: [{names_str}]")

    lines.append(
        "\nConsider assigning distinct `prefix` values to the conflicting parts to avoid collisions."
    )
    return ValueError("\n".join(lines))


class ConfValidator(object):
    """
    applies repo wide validation rules
    """

    def __init__(
        self,
        input_root,
        output_root,
        existing_gbs,
        existing_joins,
        existing_staging_queries,
        log_level=logging.INFO,
    ):
        self.chronon_root_path = input_root
        self.output_root = output_root

        self.log_level = log_level
        self.logger = get_logger(log_level)

        # we keep the objs in the list not in  a set since thrift does not
        # implement __hash__ for ttypes object.

        self.old_objs = defaultdict(dict)
        self.old_group_bys = existing_gbs
        self.old_joins = existing_joins
        self.old_staging_queries = existing_staging_queries
        self.old_objs["GroupBy"] = self.old_group_bys
        self.old_objs["Join"] = self.old_joins
        self.old_objs["StagingQuery"] = self.old_staging_queries

    def _get_old_obj(self, obj_class: type, obj_name: str) -> object:
        """
        returns:
           materialized version of the obj given the object's name.
        """
        class_name = obj_class.__name__

        if class_name not in self.old_objs:
            return None
        obj_map = self.old_objs[class_name]

        if obj_name not in obj_map:
            return None
        return obj_map[obj_name]

    def _get_old_joins_with_group_by(self, group_by: GroupBy) -> List[Join]:
        """
        returns:
            materialized joins including the group_by as dicts.
        """
        joins = []
        for join in self.old_joins.values():
            if join.joinParts is not None and group_by.metaData.name in [
                rp.groupBy.metaData.name for rp in join.joinParts
            ]:
                joins.append(join)
        return joins

    def can_skip_materialize(self, obj: object) -> List[str]:
        """
        Check if the object can be skipped to be materialized and return reasons
        if it can be.
        """
        reasons = []
        if isinstance(obj, GroupBy):
            if not _is_batch_upload_needed(obj):
                reasons.append(
                    "GroupBys should not be materialized if batch upload job is not needed"
                )
            # Otherwise group_bys included in online join or are marked explicitly
            # online itself are materialized.
            elif not any(
                join.metaData.online for join in self._get_old_joins_with_group_by(obj)
            ) and not _is_batch_upload_needed(obj):
                reasons.append("is not marked online/production nor is included in any online join")
        return reasons

    def validate_obj(self, obj: object) -> List[BaseException]:
        """
        Validate Chronon API obj against other entities in the repo.

        returns:
          list of errors.
        """
        errors = []
        # Online configs are deployed via `zipline hub schedule` which schedules them to run at a regular cadence.
        # If the semantic hash changes after deployment, subsequent runs will fail. So we block in-place
        # changes to online configs at compile time itself to prevent this.
        if not self.safe_to_overwrite(obj):
            obj_type = type(obj).__name__
            errors.append(
                ValueError(
                    f"{obj_type} '{obj.metaData.name}' is online and cannot be changed in-place as there can be downstream readers"
                    f"To make changes to an online config, create a new copy of it"
                )
            )
        if isinstance(obj, GroupBy):
            errors.extend(self._validate_group_by(obj))
        elif isinstance(obj, Join):
            errors.extend(self._validate_join(obj))
        return errors

    def _has_diff(self, obj: object, old_obj: object, skipped_fields=SKIPPED_FIELDS) -> bool:
        new_json = json.loads(thrift_simple_json(obj))
        old_json = json.loads(thrift_simple_json(old_obj))
        _strip_fields_recursive(new_json, skipped_fields)
        _strip_fields_recursive(old_json, skipped_fields)
        # stripping metaData would hide a grid change; reinject the grid as a synthetic field
        if "metaData" in skipped_fields:
            for stripped_json, source_obj in ((new_json, obj), (old_json, old_obj)):
                token = _output_grid_token(source_obj)
                if token:
                    stripped_json["__outputGrid"] = token
        return new_json != old_json

    def safe_to_overwrite(self, obj: object) -> bool:
        """When an object is already materialized as online, it is no more safe
        to materialize and overwrite the old conf.
        """
        old_obj = self._get_old_obj(type(obj), obj.metaData.name)
        return not old_obj or not self._has_diff(obj, old_obj) or not old_obj.metaData.online

    def _validate_time_partitioned_query(
        self, query: Optional[Query], source_context: str
    ) -> BaseException | None:
        # timePartitioned flag is deprecated — partition column type is detected automatically.
        # Kept for backwards compatibility but no longer enforced.
        return None

    def _validate_derivations(
        self, pre_derived_cols: List[str], derivations: List[Derivation]
    ) -> List[BaseException]:
        """
        Validate join/groupBy's derivation is defined correctly.

        Returns:
          list of validation errors.
        """
        errors = []
        derived_columns = set(pre_derived_cols)

        wild_card_derivation_included = any(
            derivation.expression == "*" for derivation in derivations
        )
        if not wild_card_derivation_included:
            derived_columns.clear()
        for derivation in derivations:
            # if the derivation is a renaming derivation, check whether the expression is in pre-derived schema
            if is_identifier(derivation.expression):
                # for wildcard derivation we want to remove the original column if there is a renaming operation
                # applied on it
                if wild_card_derivation_included:
                    if derivation.expression in derived_columns:
                        derived_columns.remove(derivation.expression)
                if derivation.expression not in pre_derived_cols and derivation.expression not in (
                    "ds",
                    "ts",
                ):
                    errors.append(
                        ValueError(
                            "Incorrect derivation expression {}, expression not found in pre-derived columns {}".format(
                                derivation.expression, pre_derived_cols
                            )
                        )
                    )
            if derivation.name != "*":
                if derivation.name in derived_columns:
                    errors.append(
                        ValueError(
                            "Incorrect derivation name {} due to output column name conflict".format(
                                derivation.name
                            )
                        )
                    )
                else:
                    derived_columns.add(derivation.name)
        return errors

    def _validate_join_part_keys(self, join_part: JoinPart, left_cols: List[str]) -> BaseException:
        keys = []

        key_mapping = join_part.keyMapping if join_part.keyMapping else {}
        # key_mapping is {left_col: gb_key}, so invert to resolve gb_key -> left_col
        inverted_key_mapping = {v: k for k, v in key_mapping.items()}
        for key in join_part.groupBy.keyColumns:
            keys.append(inverted_key_mapping.get(key, key))

        missing = [k for k in keys if k not in left_cols]

        err_string = ""
        left_cols_as_str = ", ".join(left_cols)
        group_by_name = join_part.groupBy.metaData.name
        if missing:
            key_mapping_str = f"Key Mapping: {key_mapping}" if key_mapping else ""
            err_string += textwrap.dedent(f"""
                - Join is missing keys {missing} on left side. Required for JoinPart: {group_by_name}.
                Existing columns on left side: {left_cols_as_str}
                All required Keys: {join_part.groupBy.keyColumns}
                {key_mapping_str}
                Consider renaming a column on the left, or including the key_mapping argument to your join_part.""")

        if key_mapping:
            # Left side of key mapping should include columns on the left
            key_map_keys_missing_from_left = [k for k in key_mapping.keys() if k not in left_cols]
            if key_map_keys_missing_from_left:
                err_string += f"\n- The following keys in your key_mapping: {str(key_map_keys_missing_from_left)} for JoinPart {group_by_name} are not included in the left side of the join: {left_cols_as_str}"

            # Right side of key mapping should only include keys in GroupBy
            keys_missing_from_key_map_values = [
                v for v in key_mapping.values() if v not in join_part.groupBy.keyColumns
            ]
            if keys_missing_from_key_map_values:
                err_string += f"\n- The following values in your key_mapping: {str(keys_missing_from_key_map_values)}  for JoinPart {group_by_name} do not cover any group by key columns: {join_part.groupBy.keyColumns}"

            if key_map_keys_missing_from_left or keys_missing_from_key_map_values:
                err_string += (
                    "\n(Key Mapping should be formatted as column_from_left -> group_by_key)"
                )

        if err_string:
            return ValueError(err_string)

    def _validate_keys(self, join: Join) -> List[BaseException]:
        left = join.left

        left_selects = None
        if left.events:
            left_selects = left.events.query.selects
        elif left.entities:
            left_selects = left.entities.query.selects
        elif left.joinSource:
            left_selects = left.joinSource.query.selects
            # TODO -- if selects are not selected here, get output cols from join

        left_cols = []

        if left_selects:
            left_cols = left_selects.keys()

        errors = []

        if left_cols:
            join_parts = list(join.joinParts)  # Create a copy to avoid modifying the original

            # Validate join_parts
            for join_part in join_parts:
                join_part_err = self._validate_join_part_keys(join_part, left_cols)
                if join_part_err:
                    errors.append(join_part_err)

        return errors

    def _validate_join(self, join: Join) -> List[BaseException]:
        """
        Validate join's status with materialized versions of group_bys
        included by the join.

        Returns:
          list of validation errors.
        """
        included_group_bys_and_prefixes = [(rp.groupBy, rp.prefix) for rp in join.joinParts]
        included_group_bys = [tup[0] for tup in included_group_bys_and_prefixes]

        offline_included_group_bys = [
            gb.metaData.name
            for gb in included_group_bys
            if not gb.metaData or gb.metaData.online is False
        ]
        errors = []

        if join.left and (left_query := get_query(join.left)) is not None:
            left_query_err = self._validate_time_partitioned_query(
                left_query, f"join {join.metaData.name} left"
            )
            if left_query_err:
                errors.append(left_query_err)

        for index, bootstrap_part in enumerate(join.bootstrapParts or []):
            bootstrap_query_err = self._validate_time_partitioned_query(
                bootstrap_part.query, f"join {join.metaData.name} bootstrapParts[{index}]"
            )
            if bootstrap_query_err:
                errors.append(bootstrap_query_err)

        old_group_bys = [
            group_by
            for group_by in included_group_bys
            if self._get_old_obj(GroupBy, group_by.metaData.name)
        ]
        non_prod_old_group_bys = [
            group_by.metaData.name
            for group_by in old_group_bys
            if group_by.metaData.production is False
        ]
        # Check if the underlying groupBy is valid
        group_by_errors = [self._validate_group_by(group_by) for group_by in included_group_bys]
        errors += [
            ValueError(f"join {join.metaData.name}'s underlying {error}")
            for errors in group_by_errors
            for error in errors
        ]
        # Check if the production join is using non production groupBy
        if join.metaData.production and non_prod_old_group_bys:
            errors.append(
                ValueError(
                    "join {} is production but includes the following non production group_bys: {}".format(
                        join.metaData.name, ", ".join(non_prod_old_group_bys)
                    )
                )
            )
        # Check if the online join is using the offline groupBy
        if join.metaData.online:
            if offline_included_group_bys:
                errors.append(
                    ValueError(
                        "join {} is online but includes the following offline group_bys: {}".format(
                            join.metaData.name, ", ".join(offline_included_group_bys)
                        )
                    )
                )
        # Only validate the join derivation when the underlying groupBy is valid
        group_by_correct = all(not errors for errors in group_by_errors)
        if join.derivations and group_by_correct:
            features = list(get_pre_derived_join_features(join).keys())
            # For online joins keys are not included in output schema
            if join.metaData.online:
                columns = features
            else:
                keys = list(get_pre_derived_source_keys(join.left).keys())
                columns = features + keys
            errors.extend(self._validate_derivations(columns, join.derivations))

        errors.extend(self._validate_keys(join))

        # If the join is using "short" names, ensure that there are no collisions
        if join.useLongNames is False:
            right_part_collisions = detect_feature_name_collisions(
                included_group_bys_and_prefixes, "right parts", join.metaData.name
            )
            if right_part_collisions:
                errors.append(right_part_collisions)

        return errors

    def _validate_group_by(self, group_by: GroupBy) -> List[BaseException]:
        """
        Validate group_by's status with materialized versions of joins
        including the group_by.

        Return:
          List of validation errors.
        """
        joins = self._get_old_joins_with_group_by(group_by)
        online_joins = [join.metaData.name for join in joins if join.metaData.online is True]
        prod_joins = [join.metaData.name for join in joins if join.metaData.production is True]
        errors = []

        non_temporal = group_by.accuracy is None or group_by.accuracy == Accuracy.SNAPSHOT

        no_topic = not _group_by_has_topic(group_by)
        has_hourly_windows = _group_by_has_hourly_windows(group_by)

        # batch features cannot contain hourly windows
        if (no_topic and non_temporal) and has_hourly_windows:
            errors.append(
                ValueError(
                    f"group_by {group_by.metaData.name} is defined to be daily refreshed but contains "
                    f"hourly windows. "
                )
            )

        def _validate_bounded_event_source():
            if group_by.aggregations is None:
                return

            unbounded_event_sources = [
                str(src)
                for src in group_by.sources
                if isinstance(get_root_source(src), EventSource)
                and get_query(src).startPartition is None
            ]

            if not unbounded_event_sources:
                return

            unwindowed_aggregations = [
                str(agg) for agg in group_by.aggregations if agg.windows is None
            ]

            if not unwindowed_aggregations:
                return

            nln = "\n"

            errors.append(
                ValueError(
                    f"""group_by {group_by.metaData.name} uses unwindowed aggregations [{nln}{f",{nln}".join(unwindowed_aggregations)}{nln}]
                    on unbounded event sources: [{nln}{f",{nln}".join(unbounded_event_sources)}{nln}].
                    Please set a start_partition on the source, or a window on the aggregation."""
                )
            )

        _validate_bounded_event_source()

        # group by that are marked explicitly offline should not be present in
        # materialized online joins.
        if group_by.metaData.online is False and online_joins:
            errors.append(
                ValueError(
                    "group_by {} is explicitly marked offline but included in "
                    "the following online joins: {}".format(
                        group_by.metaData.name, ", ".join(online_joins)
                    )
                )
            )
        # group by that are marked explicitly non-production should not be
        # present in materialized production joins.
        if prod_joins:
            if group_by.metaData.production is False:
                errors.append(
                    ValueError(
                        "group_by {} is explicitly marked as non-production but included in the following production "
                        "joins: {}".format(group_by.metaData.name, ", ".join(prod_joins))
                    )
                )
            # if the group by is included in any of materialized production join,
            # set it to production in the materialized output.
            else:
                group_by.metaData.production = True

        # validate the derivations are defined correctly
        if group_by.derivations:
            # For online group_by keys are not included in output schema
            if group_by.metaData.online:
                columns = list(get_pre_derived_group_by_features(group_by).keys())
            else:
                columns = list(get_pre_derived_group_by_columns(group_by).keys())
            errors.extend(self._validate_derivations(columns, group_by.derivations))

        for index, source in enumerate(group_by.sources):
            src: Source = source
            query_error = self._validate_time_partitioned_query(
                get_query(src), f"group_by {group_by.metaData.name} source[{index}]"
            )
            if query_error:
                errors.append(query_error)

            if src.events and src.events.isCumulative and (src.events.query.timeColumn is None):
                errors.append(
                    ValueError(
                        "Please set query.timeColumn for Cumulative Events Table: {}".format(
                            src.events.table
                        )
                    )
                )
            elif src.joinSource:
                join_obj = src.joinSource.join
                if join_obj.metaData.name is None or join_obj.metaData.team is None:
                    errors.append(
                        ValueError(f"Join must be defined with team and name: {join_obj}")
                    )
        return errors

    def validate_changes(self, results, validate_all=False):
        """
        Validate changes in compiled objects against existing materialized objects.

        Args:
            results: List of CompiledObj from parse_configs

        Returns:
            None (exits on user cancellation)
        """

        # Filter out results with errors and only process GroupBy/Join
        valid_results = []
        for result in results:
            if result.errors:
                continue  # Skip results with errors
            if result.obj_type not in ["GroupBy", "Join", "StagingQuery"]:
                continue  # Skip non-GroupBy/Join objects
            valid_results.append(result)

        # Categorize changes
        changed_objects = {"changed": [], "deleted": [], "added": []}

        # Process each valid result
        for result in valid_results:
            obj_name = result.obj.metaData.name
            obj_type = result.obj_type

            # Check if object exists in old objects
            old_obj = self._get_old_obj(type(result.obj), obj_name)

            if old_obj is None:
                # New object
                change = self._create_config_change(result.obj, obj_type)
                changed_objects["added"].append(change)
            elif self._has_diff(result.obj, old_obj, skipped_fields=[] if validate_all else SKIPPED_FIELDS):
                # Modified object
                change = self._create_config_change(result.obj, obj_type)
                changed_objects["changed"].append(change)

        # Check for deleted objects
        # Build set of current objects from valid_results for lookup
        current_objects = {(result.obj_type, result.obj.metaData.name) for result in valid_results}

        for obj_type in ["GroupBy", "Join", "StagingQuery"]:
            old_objs = self.old_objs.get(obj_type, {})
            for obj_name, old_obj in old_objs.items():
                if (obj_type, obj_name) not in current_objects:
                    # Object was deleted
                    change = self._create_config_change(old_obj, obj_type)
                    changed_objects["deleted"].append(change)

        # Store changes for later confirmation check
        self.pending_changes = {
            "changed": changed_objects["changed"],
            "deleted": changed_objects["deleted"],
            "added": changed_objects["added"],
        }

    def _filter_non_version_changes(self, existing_changes, added_changes):
        """Filter out version changes from existing changes.

        Returns list of changes that are NOT version bumps and require confirmation.
        """
        added_names = {change.name for change in added_changes}
        non_version_changes = []

        for change in existing_changes:
            # Check if this deleted config has a corresponding added config that represents a version bump
            is_version_bump = any(
                is_version_change(change.name, added_name) for added_name in added_names
            )

            if not is_version_bump:
                non_version_changes.append(change)

        return non_version_changes

    def check_pending_changes_confirmation(self, compile_status):
        """Check if user confirmation is needed for pending changes after display."""
        # Skip confirmation if there are compilation errors
        if self._has_compilation_errors(compile_status):
            return  # Don't prompt when there are errors

        if not hasattr(self, "pending_changes"):
            return  # No pending changes

        # Check if we need user confirmation (only for non-version-bump changes)
        if self._non_version_changes():
            if not self._prompt_user_confirmation():
                console.print("❌ Compilation cancelled by user.")
                sys.exit(1)

    def _non_version_changes(self):
        """Return list of changes that are NOT version bumps and require confirmation."""
        return self._filter_non_version_changes(
            self.pending_changes["changed"] + self.pending_changes["deleted"],
            self.pending_changes["added"],
        )

    def _has_compilation_errors(self, compile_status):
        """Check if there are any compilation errors across all class trackers."""
        for tracker in compile_status.cls_to_tracker.values():
            if tracker.files_to_errors:
                return True
        return False

    def _create_config_change(self, obj, obj_type):
        """Create a ConfigChange object from a thrift object."""
        return ConfigChange(
            name=obj.metaData.name,
            obj_type=obj_type,
            online=obj.metaData.online if obj.metaData.online else False,
            production=obj.metaData.production if obj.metaData.production else False,
        )

    def _prompt_user_confirmation(self) -> bool:
        """
        Prompt user for Y/N confirmation to proceed with overwriting existing configs.
        Returns True if user confirms, False otherwise.
        """
        console.print(
            "\n❗ [bold red3]Some configs are changing in-place (changing semantics without changing the version).[/bold red3]"
        )
        console.print(
            "[dim]Note that changes can be caused by directly modifying a config, or by changing the version of an upstream object which changes the input source.[/dim]"
        )
        console.print("")
        console.print(
            "[dim]This can be a safe operation if you are sure that the configs that are unaffected by the change (i.e. upstream config is versioning up in a way that does not effect the downstream), or if the objects in question are not yet used by critical workloads.[/dim]"
        )
        console.print("")
        console.print("❓ [bold]Do you want to proceed? (y/N):[/bold]", end=" ")

        try:
            response = input().strip().lower()
            return response in ["y", "yes"]
        except (EOFError, KeyboardInterrupt):
            console.print("\n❌ Compilation cancelled.")
            return False
