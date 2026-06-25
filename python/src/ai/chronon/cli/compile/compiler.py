import os
import shutil
import traceback
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import ai.chronon.cli.compile.display.compiled_obj
import ai.chronon.cli.compile.parse_configs as parser
import ai.chronon.cli.logger as logger
from ai.chronon import utils
from ai.chronon.cli.compile import serializer
from ai.chronon.cli.compile.compile_context import CompileContext, ConfigInfo
from ai.chronon.cli.compile.display.compiled_obj import CompiledObj
from ai.chronon.cli.compile.parse_teams import merge_team_execution_info
from ai.chronon.cli.formatter import Format, PromptException
from ai.chronon.cli.theme import console
from ai.chronon.types import MetaData
from gen_thrift.api.ttypes import ConfType, GroupBy, Join, ModelTransforms, StagingQuery

logger = logger.get_logger()


@dataclass
class CompileResult:
    config_info: ConfigInfo
    obj_dict: Dict[str, Any]
    error_dict: Dict[str, List[BaseException]]


class Compiler:
    def __init__(self, compile_context: CompileContext):
        self.compile_context = compile_context

    def compile(self, dry_run=False, validate_all=False) -> Dict[ConfType, CompileResult]:
        # Clean staging directory at the start to ensure fresh compilation
        staging_dir = self.compile_context.staging_output_dir()
        if os.path.exists(staging_dir):
            shutil.rmtree(staging_dir)

        config_infos = self.compile_context.config_infos

        compile_results = {}
        all_compiled_objects = []  # Collect all compiled objects for change validation
        processed_dirs = set()  # Track processed directories
        # Track config name to source file mapping to detect duplicates across directories
        config_name_to_file: Dict[Tuple[ConfType, str], str] = {}

        for config_info in config_infos:
            input_dir = self.compile_context.input_dir(config_info.cls)

            # Only process each directory once
            if input_dir not in processed_dirs and os.path.exists(input_dir):
                # Scan for ALL config types in this directory to support cross-type discovery
                # (e.g., finding Join objects in staging_queries/ directories)
                multi_results = self._compile_multi_class_configs(
                    config_infos, input_dir, compile_results, config_name_to_file
                )

                # Merge results for each type (duplicates already filtered in _compile_multi_class_configs)
                for ci, result, compiled_objs in multi_results:
                    if ci.config_type not in compile_results:
                        compile_results[ci.config_type] = result
                    else:
                        # Merge results (duplicates already prevented from being written)
                        compile_results[ci.config_type].obj_dict.update(result.obj_dict)
                        # Extend error lists instead of replacing to preserve errors from multiple directories
                        for key, error_list in result.error_dict.items():
                            if key in compile_results[ci.config_type].error_dict:
                                compile_results[ci.config_type].error_dict[key].extend(error_list)
                            else:
                                compile_results[ci.config_type].error_dict[key] = error_list.copy()

                    # Track source files for all successfully compiled configs
                    for co in compiled_objs:
                        if co.name and co.obj:
                            config_name_to_file[(ci.config_type, co.name)] = co.file

                    all_compiled_objects.extend(compiled_objs)

                processed_dirs.add(input_dir)

        # Close class trackers after all directories have been processed
        for config_info in config_infos:
            if config_info.config_type is not None:
                self.compile_context.compile_status.close_cls(config_info.cls.__name__)

        self._validate_chronon_table_reference_grid_propagation(all_compiled_objects)

        # Validate changes once after all classes have been processed
        self.compile_context.validator.validate_changes(all_compiled_objects, validate_all)

        # Show the nice display first
        if self.compile_context.format != Format.JSON:
            console.print(
                self.compile_context.compile_status.render(
                    self.compile_context.ignore_python_errors
                )
            )

        # Check for confirmation before finalizing files
        if not self.compile_context.force:
            if self.compile_context.format != Format.JSON:
                self.compile_context.validator.check_pending_changes_confirmation(
                    self.compile_context.compile_status
                )
            else:
                # In case of JSON format we need to prompt the user for confirmation if changes are not versioned.
                non_version_changes = self.compile_context.validator._non_version_changes()
                if non_version_changes:
                    raise PromptException(
                        prompt=f"The following configs are changing in-place (changing semantics without changing the version)."
                        f" {', '.join([v.name for v in non_version_changes])} Do you want to proceed?",
                        options=["yes", "no"],
                        instructions="If 'yes' run with --force to proceed with the compilation.",
                    )

        # Only proceed with file operations if there are no compilation errors
        if dry_run:
            staging_dir = self.compile_context.staging_output_dir()
            if os.path.exists(staging_dir):
                shutil.rmtree(staging_dir)
        elif not self.has_compilation_errors() or self.compile_context.ignore_python_errors:
            self._compile_team_metadata()

            # check if staging_output_dir exists
            staging_dir = self.compile_context.staging_output_dir()
            if os.path.exists(staging_dir):
                # replace staging_output_dir to output_dir
                output_dir = self.compile_context.output_dir()
                if os.path.exists(output_dir):
                    shutil.rmtree(output_dir)
                shutil.move(staging_dir, output_dir)
                if self.compile_context.format != Format.JSON:
                    env_label = self.compile_context.env.upper()
                    console.print(
                        f"[bold green]{env_label} compilation successful.[/] "
                        f"Compiled files saved to {output_dir}"
                    )
            else:
                if self.compile_context.format != Format.JSON:
                    console.print(
                        f"Staging directory {staging_dir} does not exist. "
                        "Happens when every chronon config fails to compile or when no chronon configs exist."
                    )
        else:
            # Clean up staging directory when there are errors (don't move to output)
            if self.compile_context.format != Format.JSON:
                console.print("Compilation errors detected. Not saving compiled files to output directory.")
            staging_dir = self.compile_context.staging_output_dir()
            if os.path.exists(staging_dir):
                shutil.rmtree(staging_dir)

        return compile_results

    def has_compilation_errors(self) -> bool:
        """Check if there are any compilation errors across all class trackers."""
        for tracker in self.compile_context.compile_status.cls_to_tracker.values():
            if tracker.files_to_errors:
                return True
        return False

    def _validate_chronon_table_reference_grid_propagation(self, compiled_objects: List[CompiledObj]) -> None:
        known_non_daily_tables = self._known_non_daily_chronon_tables(compiled_objects)
        if not known_non_daily_tables:
            return

        for co in compiled_objects:
            if not co.obj:
                continue
            errors = [
                error
                for table, grid_declared, field in self._table_uses(co.obj)
                for error in [self._chronon_table_reference_error(table, grid_declared, field, known_non_daily_tables)]
                if error is not None
            ]
            if not errors:
                continue
            error_co = CompiledObj(
                name=co.name,
                obj=None,
                file=co.file,
                errors=errors,
                obj_type=co.obj_type,
                tjson=co.tjson,
            )
            self.compile_context.compile_status.add_object_update_display(error_co, co.obj_type)

    def _known_non_daily_chronon_tables(self, compiled_objects: List[CompiledObj]) -> Dict[str, Tuple[str, str]]:
        result = {}
        for co in compiled_objects:
            obj = co.obj
            if not obj or not isinstance(obj, (GroupBy, Join, ModelTransforms, StagingQuery)):
                continue
            if not utils._non_daily_output_grid(obj):
                continue
            table = utils.output_table_name(obj, full_name=True)
            result[table] = (co.name, "table")
            if isinstance(obj, Join) and obj.derivations:
                result[f"{table}__derived"] = (co.name, "derived_table")
        return result

    def _table_uses(self, obj: Any) -> List[Tuple[str, bool, str]]:
        uses = []
        seen = set()

        def field(prefix: str, suffix: str) -> str:
            return f"{prefix} {suffix}".strip()

        def add_source(source, field_prefix: str):
            if source is None:
                return
            if source.events:
                query = source.events.query
                uses.append(
                    (
                        source.events.table,
                        query is not None and query.partitionInterval is not None,
                        field_prefix,
                    )
                )
            elif source.entities:
                query = source.entities.query
                uses.append(
                    (
                        source.entities.snapshotTable,
                        query is not None and query.partitionInterval is not None,
                        field_prefix,
                    )
                )
            elif source.joinSource:
                add_join(source.joinSource.join, field(field_prefix, "joinSource"))

        def add_group_by(group_by, field_prefix: str):
            if group_by is None:
                return
            key = ("group_by", id(group_by))
            if key in seen:
                return
            seen.add(key)
            for source in group_by.sources or []:
                add_source(source, field(field_prefix, "source table"))

        def add_join(join, field_prefix: str):
            if join is None:
                return
            key = ("join", id(join))
            if key in seen:
                return
            seen.add(key)
            add_source(join.left, field(field_prefix, "left source table"))
            for part in join.joinParts or []:
                group_by = part.groupBy
                group_by_name = getattr(getattr(group_by, "metaData", None), "name", None)
                part_prefix = f"{field_prefix} join part"
                if group_by_name:
                    part_prefix = f"{part_prefix} {group_by_name}"
                add_group_by(group_by, part_prefix)
            for bootstrap in join.bootstrapParts or []:
                query = bootstrap.query
                uses.append(
                    (
                        bootstrap.table,
                        query is not None and query.partitionInterval is not None,
                        field(field_prefix, "bootstrap table"),
                    )
                )

        if isinstance(obj, (GroupBy, ModelTransforms)):
            add_group_by(obj, "")
        elif isinstance(obj, Join):
            add_join(obj, "")
        elif isinstance(obj, StagingQuery):
            for dep in obj.tableDependencies or []:
                table_info = dep.tableInfo
                if table_info is not None:
                    uses.append(
                        (
                            table_info.table,
                            table_info.partitionInterval is not None,
                            "table dependency",
                        )
                    )

        return [(table, grid_declared, field) for table, grid_declared, field in uses if table]

    def _chronon_table_reference_error(
        self,
        table: str,
        grid_declared: bool,
        field: str,
        known_non_daily_tables: Dict[str, Tuple[str, str]],
    ) -> Optional[ValueError]:
        if grid_declared or table not in known_non_daily_tables:
            return None

        producer_name, accessor = known_non_daily_tables[table]
        return ValueError(
            f"{field} '{table}' is produced by Chronon config '{producer_name}' with a non-daily grid, "
            f"but the consumer did not receive partition_interval metadata. Use the producer's .{accessor} "
            "property directly, or explicitly set partition_interval/partition_offset on the consumer."
        )

    def _compile_team_metadata(self):
        """
        Compile the team metadata and return the compiled object.
        """
        teams_dict = self.compile_context.teams_dict
        for team in teams_dict:
            m = MetaData()
            merge_team_execution_info(m, teams_dict, team)

            tjson = serializer.thrift_simple_json(m)
            name = f"{team}.{team}_team_metadata"
            result = CompiledObj(
                name=name,
                obj=m,
                file=name,
                errors=None,
                obj_type=MetaData.__name__,
                tjson=tjson,
            )
            self._write_object(result)
            self.compile_context.compile_status.add_object_update_display(result, MetaData.__name__)

        # Done writing team metadata, close the class
        self.compile_context.compile_status.close_cls(MetaData.__name__)

    def _compile_class_configs(
        self, config_info: ConfigInfo
    ) -> Tuple[CompileResult, List[CompiledObj]]:
        """
        Compile configs for a single class type (backward compatibility wrapper).
        Calls the multi-class version with a single class.
        """
        compile_result = CompileResult(config_info=config_info, obj_dict={}, error_dict={})

        input_dir = self.compile_context.input_dir(config_info.cls)

        # Use multi-class version with single class for consistency
        multi_type_results = parser.from_folder([config_info.cls], input_dir, self.compile_context)
        compiled_objects = multi_type_results.get(config_info.cls, [])

        objects, errors = self._write_objects_in_folder(compiled_objects)

        if objects:
            compile_result.obj_dict.update(objects)

        if errors:
            compile_result.error_dict.update(errors)

        self.compile_context.compile_status.close_cls(config_info.cls.__name__)

        return compile_result, compiled_objects

    def _compile_multi_class_configs(
        self,
        config_infos: List[ConfigInfo],
        input_dir: str,
        existing_compile_results: Optional[Dict[ConfType, CompileResult]] = None,
        config_name_to_file: Optional[Dict[Tuple[ConfType, str], str]] = None,
    ) -> List[Tuple[ConfigInfo, CompileResult, List[CompiledObj]]]:
        """
        Compile multiple config types from a single directory.

        Args:
            config_infos: List of ConfigInfo objects to search for in this directory
            input_dir: The directory to scan
            existing_compile_results: Already compiled results to check for duplicates
            config_name_to_file: Mapping of (config_type, name) to source file for duplicate detection

        Returns:
            List of tuples: [(ConfigInfo, CompileResult, List[CompiledObj]), ...]
        """
        if existing_compile_results is None:
            existing_compile_results = {}
        if config_name_to_file is None:
            config_name_to_file = {}

        # Extract all target classes
        target_classes = [ci.cls for ci in config_infos]

        # Scan directory for all types at once
        multi_type_results = parser.from_folder(target_classes, input_dir, self.compile_context)

        results = []

        # Process results for each config type
        for config_info in config_infos:
            compiled_objects = multi_type_results.get(config_info.cls, [])

            compile_result = CompileResult(
                config_info=config_info,
                obj_dict={},
                error_dict={}
            )

            # Check for duplicates BEFORE writing
            objects_to_write = []
            for co in compiled_objects:
                if co.name and co.obj:
                    # Check if this config name already exists for this type
                    if config_info.config_type in existing_compile_results:
                        if co.name in existing_compile_results[config_info.config_type].obj_dict:
                            # Duplicate detected!
                            existing_file = config_name_to_file.get((config_info.config_type, co.name), "unknown")
                            error = ValueError(
                                f"Duplicate config name '{co.name}' of type {config_info.folder_name} found:\n"
                                f"  - First defined in: {existing_file}\n"
                                f"  - Also found in: {co.file}\n"
                                f"Config names must be unique across all directories. "
                                f"Please rename one of these configs."
                            )

                            # Add error instead of writing the object
                            compile_result.error_dict[co.name] = [error]

                            if self.compile_context.format != Format.JSON:
                                self.compile_context.compile_status.print_live_console(str(error))

                            # Mark as error in display (create error version of CompiledObj)
                            error_co = CompiledObj(
                                name=co.name,
                                obj=None,
                                file=co.file,
                                errors=[error],
                                obj_type=co.obj_type,
                                tjson=co.tjson
                            )
                            self.compile_context.compile_status.add_object_update_display(error_co, config_info.cls.__name__)
                            continue

                    # No duplicate, safe to write
                    objects_to_write.append(co)
                else:
                    # Objects with parsing errors should still be processed for error recording
                    objects_to_write.append(co)

            # Write non-duplicate objects and collect results
            objects, errors = self._write_objects_in_folder(objects_to_write)

            if objects:
                compile_result.obj_dict.update(objects)
            if errors:
                compile_result.error_dict.update(errors)

            results.append((config_info, compile_result, objects_to_write))

        return results

    def _write_objects_in_folder(
        self,
        compiled_objects: List[ai.chronon.cli.compile.display.compiled_obj.CompiledObj],
    ) -> Tuple[Dict[str, Any], Dict[str, List[BaseException]]]:
        error_dict = {}
        object_dict = {}

        for co in compiled_objects:
            if co.obj:
                if co.errors:
                    error_dict[co.name] = co.errors

                    if self.compile_context.format != Format.JSON:
                        for error in co.errors:
                            self.compile_context.compile_status.print_live_console(
                                f"Error processing conf {co.name}: {error}"
                            )
                            traceback.print_exception(type(error), error, error.__traceback__)

                else:
                    self._write_object(co)
                    object_dict[co.name] = co.obj
            else:
                error_dict[co.file] = co.errors

                if self.compile_context.format != Format.JSON:
                    self.compile_context.compile_status.print_live_console(
                        f"Error processing file {co.file}: {co.errors}"
                    )
                    for error in co.errors:
                        traceback.print_exception(type(error), error, error.__traceback__)

        return object_dict, error_dict

    def _write_object(self, compiled_obj: CompiledObj) -> Optional[List[BaseException]]:
        output_path = self.compile_context.staging_output_path(compiled_obj)

        folder = os.path.dirname(output_path)

        if not os.path.exists(folder):
            os.makedirs(folder)

        with open(output_path, "w") as f:
            f.write(compiled_obj.tjson)
