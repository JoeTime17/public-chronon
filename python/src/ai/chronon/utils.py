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

import gc
import importlib
import inspect
import json
import os
import re
import shutil
import subprocess
import tempfile
from collections.abc import Iterable, Sequence
from typing import List, Optional, Union, get_args

import gen_thrift.api.ttypes as api

# Common type definition for any source type used across the codebase
ANY_SOURCE_TYPE = Union[
    api.Source, api.EventSource, api.EntitySource, api.JoinSource, api.ModelTransforms
]

chronon_root_path = ""  # passed from compile.py


def convert_environments_to_enum(environments: List[str]) -> List[int]:
    """Convert environment strings to enum values with validation.

    Args:
        environments: List of environment strings ('prod', 'canary', case-insensitive)

    Returns:
        List of Environment enum integer values

    Raises:
        ValueError: If any environment string is not 'prod' or 'canary'
    """
    env_map = {
        'prod': api.Environment.PROD,
        'canary': api.Environment.CANARY,
    }
    result = []
    for env in environments:
        env_lower = env.lower()
        if env_lower not in env_map:
            raise ValueError(
                f"Invalid environment '{env}'. Must be one of: {list(env_map.keys())}"
            )
        result.append(env_map[env_lower])
    return result


def normalize_source(source: ANY_SOURCE_TYPE, output_namespace: str = None) -> api.Source:
    """Convert any source type to a properly wrapped api.Source"""
    if isinstance(source, api.EventSource):
        return api.Source(events=source)
    elif isinstance(source, api.EntitySource):
        return api.Source(entities=source)
    elif isinstance(source, api.JoinSource):
        __set_name(source.join, api.Join, "joins")
        if output_namespace and not source.join.metaData.outputNamespace:
            source.join.metaData.outputNamespace = output_namespace
        return api.Source(joinSource=source)
    elif isinstance(source, api.ModelTransforms):
        return api.Source(modelTransforms=source)
    elif isinstance(source, api.Source):
        if source.entities:
            return normalize_source(source.entities, output_namespace)
        elif source.events:
            return normalize_source(source.events, output_namespace)
        elif source.joinSource:
            return normalize_source(source.joinSource, output_namespace)
        elif source.modelTransforms:
            return normalize_source(source.modelTransforms, output_namespace)
        else:
            return source
    else:
        print("unrecognized " + str(source))


def normalize_sources(
    sources: Union[Sequence[ANY_SOURCE_TYPE], ANY_SOURCE_TYPE], output_namespace: str = None
) -> List[api.Source]:
    """Convert a source or source sequence into wrapped api.Source objects."""
    source_types = get_args(ANY_SOURCE_TYPE)

    if isinstance(sources, source_types):
        sources = [sources]
    elif not isinstance(sources, Sequence):
        raise TypeError("sources must be a source or a sequence of sources")

    for index, source in enumerate(sources):
        if not isinstance(source, source_types):
            raise TypeError(
                f"sources[{index}] must be a supported source type, got {type(source).__name__}"
            )

    return [normalize_source(source, output_namespace) for source in sources]


def edit_distance(str1, str2):
    m = len(str1) + 1
    n = len(str2) + 1
    dp = [[0 for _ in range(n)] for _ in range(m)]
    for i in range(m):
        for j in range(n):
            if i == 0:
                dp[i][j] = j
            elif j == 0:
                dp[i][j] = i
            elif str1[i - 1] == str2[j - 1]:
                dp[i][j] = dp[i - 1][j - 1]
            else:
                dp[i][j] = 1 + min(dp[i][j - 1], dp[i - 1][j], dp[i - 1][j - 1])
    return dp[m - 1][n - 1]


class JsonDiffer:
    def __init__(self):
        self.temp_dir = tempfile.mkdtemp()
        self.new_name = "new.json"
        self.old_name = "old.json"

    def diff(self, new_json_str: object, old_json_str: object, skipped_keys=None) -> str:
        if skipped_keys is None:
            skipped_keys = []
        new_json = {k: v for k, v in json.loads(new_json_str).items() if k not in skipped_keys}
        old_json = {k: v for k, v in json.loads(old_json_str).items() if k not in skipped_keys}

        with (
            open(os.path.join(self.temp_dir, self.old_name), mode="w") as old,
            open(os.path.join(self.temp_dir, self.new_name), mode="w") as new,
        ):
            old.write(json.dumps(old_json, sort_keys=True, indent=2))
            new.write(json.dumps(new_json, sort_keys=True, indent=2))
        diff_str = subprocess.run(
            ["diff", old.name, new.name], stdout=subprocess.PIPE
        ).stdout.decode("utf-8")
        return diff_str

    def clean(self):
        shutil.rmtree(self.temp_dir)


def check_contains_single(candidate, valid_items, type_name, name, print_function=repr):
    name_suffix = f"for {name}" if name else ""
    candidate_str = print_function(candidate)
    if not valid_items:
        assert f"{candidate_str}, is not a valid {type_name} because no {type_name}s are specified {name_suffix}"
    elif candidate not in valid_items:
        sorted_items = sorted(
            map(print_function, valid_items),
            key=lambda item: edit_distance(candidate_str, item),
        )
        printed_items = "\n    ".join(sorted_items)
        assert (
            candidate in valid_items
        ), f"""{candidate_str}, is not a valid {type_name} {name_suffix}
Please pick one from:
    {printed_items}
"""


def check_contains(candidates, *args):
    if isinstance(candidates, Iterable) and not isinstance(candidates, str):
        for candidate in candidates:
            check_contains_single(candidate, *args)
    else:
        check_contains_single(candidates, *args)


def get_streaming_sources(group_by: api.GroupBy) -> List[api.Source]:
    """Checks if the group by has a source with streaming enabled."""
    return [source for source in group_by.sources if is_streaming(source)]


def is_streaming(source: api.Source) -> bool:
    """Checks if the source has streaming enabled."""
    return (source.entities and source.entities.mutationTopic is not None) or (
        source.events and source.events.topic is not None
    )


def _get_underlying_source(
    source: api.Source,
) -> Union[api.EventSource, api.EntitySource, api.JoinSource]:
    if source.entities:
        return source.entities
    elif source.events:
        return source.events
    else:
        return source.joinSource


def get_root_source(
    source: api.Source,
) -> Union[api.EventSource, api.EntitySource]:
    if source.entities:
        return source.entities
    elif source.events:
        return source.events
    else:
        return get_root_source(source.joinSource.join.left)


def get_query(source: api.Source) -> api.Query:
    return _get_underlying_source(source).query


def get_table(source: api.Source) -> str:
    if source.entities:
        table = source.entities.snapshotTable
    elif source.events:
        table = source.events.table
    else:
        from ai.chronon.join import _get_output_table_name

        table = _get_output_table_name(source.joinSource.join, True)
    return table.split("/")[0]




def get_mod_name_from_gc(obj, mod_prefix):
    """get an object's module information from garbage collector"""
    mod_name = None
    # get obj's module info from garbage collector
    gc.collect()

    referrers = gc.get_referrers(obj)

    valid_referrers = [
        ref for ref in referrers if (isinstance(ref, Iterable) and "__name__" in ref)
    ]

    if len(valid_referrers) == 1:
        return valid_referrers[0]["__name__"]

    for ref in valid_referrers:
        if ref["__name__"].startswith(mod_prefix):
            mod_name = ref["__name__"]
            break

    return mod_name



def _import_module_set_name(module, cls):
    """Evaluate imported modules to assign object name."""
    for name, obj in list(module.__dict__.items()):
        if isinstance(obj, cls):
            base_name = module.__name__.partition(".")[2] + "." + name
            if hasattr(obj.metaData, "version") and obj.metaData.version is not None:
                base_name = base_name + "__" + str(obj.metaData.version)
            obj.metaData.name = base_name
            obj.metaData.team = module.__name__.split(".")[1]
    return module


def _import_module_set_name(module, cls):
    """Evaluate imported modules to assign object name."""
    for name, obj in list(module.__dict__.items()):
        if isinstance(obj, cls):
            base_name = module.__name__.partition(".")[2] + "." + name
            if hasattr(obj.metaData, "version") and obj.metaData.version is not None:
                base_name = base_name + "__" + str(obj.metaData.version)
            obj.metaData.name = base_name
            obj.metaData.team = module.__name__.split(".")[1]
    return module


def __set_name(obj, cls, mod_prefix):
    module_qualifier = get_mod_name_from_gc(obj, mod_prefix)

    module = importlib.import_module(module_qualifier)
    _import_module_set_name(module, cls)


def _get_team_from_caller(stack_depth=2):
    """Extract team name from the calling file's parent directory."""
    caller_path = inspect.stack()[stack_depth].filename
    return os.path.basename(os.path.dirname(caller_path))


def _ensure_name_and_get_output_table(obj, cls, mod_prefix, full_name=False):
    """Set name via GC if missing, then return the output table name. Used by `.table`
    property accessors on GroupBy, Join, StagingQuery, and ModelTransforms."""
    if not obj.metaData.name:
        __set_name(obj, cls, mod_prefix)
    return output_table_name(obj, full_name)


def sanitize(name):
    """
    From api.Extensions.scala
    Option(name).map(_.replaceAll("[^a-zA-Z0-9_]", "_")).orNull
    """
    if name is not None:
        return re.sub("[^a-zA-Z0-9_]", "_", name)
    return None


# Internal compile-time placeholder emitted by `output_table_name` when `outputNamespace`
# isn't populated yet (i.e., during Python authoring time, before the compile pass runs
# `_propagate_namespace`). The compile pass walks every string field and substitutes this
# token using each object's own post-propagation namespace — so consumers that capture a
# producer's `.table` name at import time still land on the correct fully-qualified name
# in the compiled Thrift. Purely internal plumbing; users should never type it themselves.
#
# The token shape is deliberately alphanumeric+underscore so `utils.sanitize` (used by
# `airflow_helpers.create_airflow_dependency` when building the `name` field from a table)
# passes it through unchanged. A `{{ db }}`-style token would be mangled to `____db____`
# by sanitize and become unfindable at compile-time substitution.
OUTPUT_NAMESPACE_PLACEHOLDER = "_chronon_namespace_placeholder_"


def output_table_name(obj, full_name: bool):
    table_name = sanitize(obj.metaData.name)
    if not full_name:
        return table_name
    db = obj.metaData.outputNamespace or OUTPUT_NAMESPACE_PLACEHOLDER
    return db + "." + table_name


def join_part_name(jp):
    if jp.groupBy is None:
        raise NotImplementedError("Join Part names for non group bys is not implemented.")
    if not jp.groupBy.metaData.name and isinstance(jp.groupBy, api.GroupBy):
        __set_name(jp.groupBy, api.GroupBy, "group_bys")
    return "_".join(
        [
            component
            for component in [jp.prefix, sanitize(jp.groupBy.metaData.name)]
            if component is not None
        ]
    )


class DotDict(dict):
    def __getattr__(self, attr):
        if attr in self:
            value = self[attr]
            return DotDict(value) if isinstance(value, dict) else value
        return None


def convert_json_to_obj(d):
    if isinstance(d, dict):
        return DotDict({k: convert_json_to_obj(v) for k, v in d.items()})
    elif isinstance(d, list):
        return [convert_json_to_obj(item) for item in d]
    else:
        return d


def compose(arg, *methods):
    """
    Allows composing deeply nested method calls - typically used in selects & derivations
    The first arg is what is threaded into methods, methods can have more than one arg.

    Example:

    .. code-block:: python
        compose(
            "user_id_approx_distinct_count_by_query",
            "map_entries",
            "array_sort (x, y) -> IF(y.value > x.value, -1, IF(y.value < x.value, 1, 0))",
            "transform entry -> entry.key"
        )

    would produce (without the new lines or indents):

    .. code-block:: text

        transform(
            array_sort(
                map_entries(
                    user_id_approx_distinct_count_by_query
                ),
                (x, y) -> IF(y.value > x.value, -1, IF(y.value < x.value, 1, 0))
            ),
            entry -> entry.key
        )
    """

    indent = "    " * (len(methods))

    result = [indent + arg]

    for method in methods:
        method_parts = method.split(" ", 1)
        method = method_parts[0]

        if len(method_parts) > 1:
            remaining_args = method_parts[1]
            last = result.pop()
            result = result + [last + ",", indent + remaining_args]

        indent = indent[:-4]
        result = [f"{indent}{method}("] + result + [f"{indent})"]

    return "\n".join(result)


def resolve_online_schedule(online: bool, online_schedule: Optional[str]) -> Optional[str]:
    """Validate and normalize the online_schedule parameter."""
    if not online and online_schedule is not None and online_schedule != "@never":
        raise ValueError(
            "online_schedule cannot be set when online=False. "
            "Either set online=True or remove the online_schedule parameter."
        )
    if online_schedule == "@never":
        return None
    elif online and online_schedule is None:
        return "@daily"
    return online_schedule


def clean_expression(expr):
    """
    Cleans up an expression by removing leading and trailing whitespace and newlines.
    """
    return re.sub(r"\s+", " ", expr).strip()
