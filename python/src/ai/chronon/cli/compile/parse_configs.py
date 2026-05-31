import copy
import glob
import importlib
import os
import sys
from typing import Any, Dict, List, Optional, Set

from ai.chronon import airflow_helpers
from ai.chronon.cli.compile import parse_teams, serializer
from ai.chronon.cli.compile.compile_context import CompileContext
from ai.chronon.cli.compile.config_origin import get_factory_origin_file
from ai.chronon.cli.compile.display.compiled_obj import CompiledObj
from ai.chronon.cli.logger import get_logger
from gen_thrift.api.ttypes import GroupBy, Join

logger = get_logger()


def from_folder(target_classes: List[type], input_dir: str, compile_context: CompileContext) -> Dict[type, List[CompiledObj]]:
    """
    Recursively consumes a folder, and constructs a map of
    object qualifier to StagingQuery, GroupBy, or Join.
    Supports multiple target classes in a single scan.
    """

    python_files = glob.glob(os.path.join(input_dir, "**/*.py"), recursive=True)
    # Visit shallowest paths first, then alphabetical within depth, so utility
    # modules are loaded before team-specific files that may depend on them.
    python_files.sort(key=lambda p: (p.count(os.sep), p))

    # Results keyed by class type
    results = {cls: [] for cls in target_classes}
    authoring_folders = {
        config_info.folder_name
        for config_info in compile_context.config_infos
        if config_info.config_type is not None
    }
    factory_enforced_classes = {
        config_info.cls
        for config_info in compile_context.config_infos
        if config_info.config_type is not None
    }

    for f in python_files:
        try:
            # Get objects of all target types from this file
            multi_type_results = from_file(
                f,
                target_classes,
                input_dir,
                compile_context.seen_obj_ids,
                authoring_folders,
                factory_enforced_classes,
            )

            # Process each type's results
            for target_cls, objects_dict in multi_type_results.items():
                for name, obj in objects_dict.items():
                    parse_teams.update_metadata(
                        obj,
                        compile_context.teams_dict,
                        teams_file_name=compile_context.teams_file_name,
                    )
                    # Populate columnHashes field with semantic hashes
                    populate_column_hashes(obj)

                    # Airflow deps must be set AFTER updating metadata
                    airflow_helpers.set_airflow_deps(obj)

                    obj.metaData.sourceFile = os.path.relpath(f, compile_context.chronon_root)

                    tjson = serializer.thrift_simple_json(obj)

                    # Perform validation
                    errors = compile_context.validator.validate_obj(obj)

                    # Use actual object type, not target class
                    actual_type = type(obj).__name__

                    result = CompiledObj(
                        name=name,
                        obj=obj,
                        file=f,
                        errors=errors if len(errors) > 0 else None,
                        obj_type=actual_type,
                        tjson=tjson,
                    )
                    results[target_cls].append(result)

                    compile_context.compile_status.add_object_update_display(result, actual_type)

        except Exception as e:
            # Attribute import errors to first target class
            result = CompiledObj(
                name=None,
                obj=None,
                file=f,
                errors=[e],
                obj_type=target_classes[0].__name__,
                tjson=None,
            )

            results[target_classes[0]].append(result)

            compile_context.compile_status.add_object_update_display(result, target_classes[0].__name__)

    return results


def from_file(
    file_path: str,
    target_classes: List[type],
    input_dir: str,
    seen_obj_ids: Set[int],
    authoring_folders: Set[str],
    factory_enforced_classes: Set[type],
) -> Dict[type, Dict[str, Any]]:
    """
    Extract config objects from a Python file.
    Supports extracting multiple config types from a single file.

    Imported config objects appear in the importing module's `__dict__`, just
    like local objects. We skip objects whose identity is already owned by a
    different authoring module so a dependency import cannot change a config's
    canonical module path. Local objects still use `seen_obj_ids` to dedup
    aliases and repeated sightings.

    Args:
        file_path: Path to the Python file to parse
        target_classes: List of config classes to search for (e.g., [GroupBy, Join])
        input_dir: Root directory for the config type
        seen_obj_ids: Mutable set of id()s for objects already claimed by an
            earlier file in this compile run. Updated in place.
        authoring_folders: User-authored config folders under chronon_root.
        factory_enforced_classes: Config classes that must be constructed via
            the ai.chronon.types factory layer.

    Returns:
        Nested dict: {GroupBy: {name: obj}, Join: {name: obj}, ...}
    """
    # this is where the python path should have been set to
    chronon_root = os.path.dirname(input_dir)
    rel_path = os.path.relpath(file_path, chronon_root)

    rel_path_without_extension = os.path.splitext(rel_path)[0]

    module_name = rel_path_without_extension.replace("/", ".")

    conf_type, team_name_with_path = module_name.split(".", 1)
    mod_path = team_name_with_path.replace("/", ".")

    modules_before = set(sys.modules.keys())
    try:
        module = importlib.import_module(module_name)
    except Exception as e:
        # Remove any partially-loaded modules from the cache so that downstream
        # files importing this one don't get cascading import errors.
        for mod in set(sys.modules.keys()) - modules_before:
            del sys.modules[mod]
        # Python removes the failed module from sys.modules but leaves it as an
        # attribute on the parent package. Clean that up too to prevent cascade.
        sys.modules.pop(module_name, None)
        parent_name, _, child_name = module_name.rpartition(".")
        parent = sys.modules.get(parent_name)
        if parent is not None and hasattr(parent, child_name):
            delattr(parent, child_name)
        raise ValueError(f"Error parsing {os.path.relpath(file_path)}: {e}") from None

    # Results keyed by class type
    result = {cls: {} for cls in target_classes}

    for var_name, obj in list(module.__dict__.items()):
        # Check if object is an instance of any target class
        for target_cls in target_classes:
            if isinstance(obj, target_cls):
                origin_file = get_factory_origin_file(obj)
                if target_cls in factory_enforced_classes and origin_file is None:
                    raise ValueError(
                        _missing_factory_origin_error(
                            obj,
                            target_cls,
                            var_name,
                            file_path,
                            chronon_root,
                        )
                    )
                if _factory_origin_owned_elsewhere(
                    obj,
                    origin_file,
                    file_path,
                    chronon_root,
                    authoring_folders,
                ):
                    break

                # Identity dedup keeps aliases to the same local object from
                # compiling multiple times in one run.
                if id(obj) in seen_obj_ids:
                    break
                seen_obj_ids.add(id(obj))

                copied_obj = copy.deepcopy(obj)

                name = f"{mod_path}.{var_name}"

                # Add version suffix if version is set
                if copied_obj.metaData.version is not None:
                    name = name + "__" + str(copied_obj.metaData.version)

                copied_obj.metaData.name = name
                copied_obj.metaData.team = mod_path.split(".")[0]

                result[target_cls][name] = copied_obj
                break  # Each object belongs to only one type

    return result


def _factory_origin_owned_elsewhere(
    obj: Any,
    origin_file: Optional[str],
    file_path: str,
    chronon_root: str,
    authoring_folders: Set[str],
) -> bool:
    if origin_file is None:
        return False
    if not _is_authoring_path(origin_file, chronon_root, authoring_folders):
        return False

    origin_file = os.path.abspath(origin_file)
    if origin_file == os.path.abspath(file_path):
        return False

    return any(
        _module_file(module) == origin_file and _module_binds_obj(module, obj)
        for module in list(sys.modules.values())
    )


def _module_file(module: Any) -> str:
    module_file = getattr(module, "__file__", None)
    return os.path.abspath(module_file) if module_file else ""


def _module_binds_obj(module: Any, obj: Any) -> bool:
    return any(value is obj for value in vars(module).values())


def _missing_factory_origin_error(
    obj: Any,
    target_cls: type,
    var_name: str,
    file_path: str,
    chronon_root: str,
) -> str:
    rel_path = os.path.relpath(file_path, chronon_root)
    type_name = type(obj).__name__
    factory_name = target_cls.__name__
    return (
        f"{rel_path}:{var_name} is a {type_name} config object without the "
        "Chronon factory origin marker. "
        "Chronon configs must be constructed through the ai.chronon.types "
        f"factory layer, e.g. `from ai.chronon.types import {factory_name}`."
    )


def _is_authoring_path(file_path: str, chronon_root: str, authoring_folders: Set[str]) -> bool:
    rel_path = os.path.relpath(os.path.abspath(file_path), os.path.abspath(chronon_root))
    if rel_path == os.pardir or rel_path.startswith(os.pardir + os.sep):
        return False

    first_component = rel_path.split(os.sep, 1)[0]
    return first_component in authoring_folders


def populate_column_hashes(obj: Any):
    """
    Populate the columnHashes field in the object's metadata with semantic hashes
    for each output column.
    """
    # Import here to avoid circular imports
    from ai.chronon.cli.compile.column_hashing import (
        compute_group_by_columns_hashes,
        compute_join_column_hashes,
    )

    if isinstance(obj, GroupBy):
        # For GroupBy objects, get column hashes
        column_hashes = compute_group_by_columns_hashes(obj, exclude_keys=False)
        obj.metaData.columnHashes = column_hashes

    elif isinstance(obj, Join):
        # For Join objects, get column hashes
        column_hashes = compute_join_column_hashes(obj)
        obj.metaData.columnHashes = column_hashes

        if obj.joinParts:
            for jp in obj.joinParts or []:
                group_by = jp.groupBy
                group_by_hashes = compute_group_by_columns_hashes(group_by)
                group_by.metaData.columnHashes = group_by_hashes
