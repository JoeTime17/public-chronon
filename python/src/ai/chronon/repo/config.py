"""Zipline config CLI - inspect values defined in the nearest teams.py.

walks up from cwd to locate teams.py, merges the default team with an
optionally selected team (and an optional compiled conf), and exposes
get/list subcommands.
"""

import json
import os
import sys
from typing import Dict, List, Optional, Tuple

import click
from rich.table import Table

import ai.chronon.cli.compile.parse_teams as parse_teams
from ai.chronon.cli.formatter import Format, jsonify_exceptions_if_json_format
from ai.chronon.cli.options import chronon_root_option, env_option, format_option
from ai.chronon.cli.theme import console
from gen_thrift.api.ttypes import MetaData, Team

# Section labels used as key prefixes (e.g. `env.CUSTOMER_ID`, `conf.spark.x`).
ENV_SECTION = "env"
CONF_SECTION = "conf"
CLUSTER_SECTION = "cluster"


def _find_chronon_root(start: Optional[str] = None) -> Optional[str]:
    """Walk up from `start` (default cwd) looking for the nearest teams.py.

    Returns the directory containing teams.py, or None if none is found.
    """
    current = os.path.abspath(start or os.getcwd())
    while True:
        if os.path.exists(os.path.join(current, "teams.py")):
            return current
        parent = os.path.dirname(current)
        if parent == current:
            return None
        current = parent


def _resolve_chronon_root(chronon_root: Optional[str]) -> str:
    """Validate an explicit chronon-root or auto-discover one by walking up."""
    if chronon_root:
        expanded = os.path.expanduser(chronon_root)
        if not os.path.isdir(expanded):
            raise click.ClickException(f"--chronon-root '{chronon_root}' is not a directory.")
        if not os.path.exists(os.path.join(expanded, "teams.py")):
            raise click.ClickException(f"teams.py not found in '{chronon_root}'.")
        return os.path.abspath(expanded)
    found = _find_chronon_root()
    if found is None:
        raise click.ClickException(
            "Could not find teams.py in the current directory or any parent. "
            "Run from within a Zipline repo or pass --chronon-root."
        )
    return found


def _teams_file_for_env(env: str) -> str:
    """Map an env name to the teams file basename. Mirrors the convention used
    by `parse_teams.discover_compile_envs`: prod → teams.py, canary →
    teams.canary.py. Any other env raises since only those two are supported
    end-to-end (Thrift `Environment` enum)."""
    e = (env or parse_teams.PROD_ENV).lower()
    if e == parse_teams.PROD_ENV:
        return parse_teams.PROD_TEAMS_FILE
    if e == parse_teams.CANARY_ENV:
        return f"teams.{parse_teams.CANARY_ENV}.py"
    raise click.ClickException(
        f"Unsupported env '{env}'. Allowed: 'prod', 'canary'."
    )


def _load_teams_silent(chronon_root: str, env: str = "prod") -> Dict[str, Team]:
    """Load teams.<env>.py (default prod → teams.py) without the parse_teams
    console message. Caller must have validated chronon_root."""
    if chronon_root not in sys.path:
        sys.path.insert(0, chronon_root)
    teams_file_name = _teams_file_for_env(env)
    teams_path = os.path.join(chronon_root, teams_file_name)
    if not os.path.exists(teams_path):
        raise click.ClickException(
            f"{teams_file_name} not found at {chronon_root}. "
            f"Drop a sibling teams.canary.py next to teams.py to enable "
            f"--env canary."
        )
    return parse_teams.load_teams(chronon_root, teams_file_name=teams_file_name, print=False)


def _flatten_section(
    section: str, common: Optional[Dict[str, str]], modes: Optional[Dict[str, Dict[str, str]]]
) -> Dict[str, str]:
    """Flatten a `(common, modes)` pair into `{prefixed_key: value}` entries.

    `common` values produce `<section>.<key>`; `modes` values produce
    `<section>.<mode>.<key>`. Only mode entries that *differ* from the merged
    common map are emitted, since `merge_team_execution_info` already pushes
    common into each mode and surfacing duplicates would just be noise.
    """
    out: Dict[str, str] = {}
    common = common or {}
    for key, value in common.items():
        out[f"{section}.{key}"] = value
    if modes:
        for mode, mode_map in modes.items():
            if not mode_map:
                continue
            for key, value in mode_map.items():
                if common.get(key) == value:
                    continue
                out[f"{section}.{mode}.{key}"] = value
    return out


def _merged_execution_info_from_team(
    team_dict: Dict[str, Team], team_name: str, teams_file_name: str = "teams.py"
) -> MetaData:
    """Run the standard team-merge pipeline and return a MetaData with the
    fully-merged ExecutionInfo (default team + the named team)."""
    if team_name not in team_dict:
        raise click.ClickException(
            f"Team '{team_name}' not found in {teams_file_name}. "
            f"Available teams: {sorted(team_dict.keys())}"
        )
    metadata = MetaData()
    parse_teams.merge_team_execution_info(metadata, team_dict, team_name)
    return metadata


def _exec_info_from_conf(repo: str, conf: str) -> Tuple[Dict, str]:
    """Read a compiled conf file and return its `metaData.executionInfo` dict
    and the team name embedded in its metadata (or 'default' if absent)."""
    conf_path = os.path.join(repo, conf) if not os.path.isabs(conf) else conf
    if not os.path.isfile(conf_path):
        raise click.ClickException(f"Conf file not found: {conf_path}")
    with open(conf_path) as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError as e:
            raise click.ClickException(
                f"Failed to parse conf '{conf_path}' as JSON. Compiled confs are "
                f"expected to be JSON. ({e})"
            ) from e
    metadata = data.get("metaData", {}) or {}
    execution_info = metadata.get("executionInfo", {}) or {}
    team = metadata.get("team") or parse_teams._DEFAULT_CONF_TEAM
    return execution_info, team


def _flatten_from_dict(execution_info: Dict) -> Dict[str, str]:
    """Flatten a JSON-shaped `executionInfo` (from a compiled conf) into the
    same prefixed-key dict shape as `_flatten_section`."""
    out: Dict[str, str] = {}
    env = execution_info.get("env") or {}
    out.update(_flatten_section(ENV_SECTION, env.get("common"), env.get("modeEnvironments")))
    conf = execution_info.get("conf") or {}
    out.update(_flatten_section(CONF_SECTION, conf.get("common"), conf.get("modeConfigs")))
    cluster = execution_info.get("clusterConf") or {}
    out.update(
        _flatten_section(CLUSTER_SECTION, cluster.get("common"), cluster.get("modeClusterConfigs"))
    )
    return out


def _flatten_from_metadata(metadata: MetaData) -> Dict[str, str]:
    """Flatten an in-memory MetaData's ExecutionInfo into prefixed-key entries."""
    out: Dict[str, str] = {}
    execution_info = metadata.executionInfo
    if execution_info is None:
        return out
    env = execution_info.env
    if env is not None:
        out.update(_flatten_section(ENV_SECTION, env.common, env.modeEnvironments))
    conf = execution_info.conf
    if conf is not None:
        out.update(_flatten_section(CONF_SECTION, conf.common, conf.modeConfigs))
    cluster = execution_info.clusterConf
    if cluster is not None:
        out.update(
            _flatten_section(CLUSTER_SECTION, cluster.common, cluster.modeClusterConfigs)
        )
    return out


def collect_values(
    chronon_root: str,
    team: Optional[str] = None,
    conf: Optional[str] = None,
    env: str = "prod",
) -> Tuple[Dict[str, str], str]:
    """Return `(values, source_team)` where `values` is the prefixed-key map
    of resolved settings and `source_team` is the team they were resolved
    against.

    `env` picks which teams file to load (prod → teams.py, canary →
    teams.canary.py), so the same conf-aware inspection works against the
    canary compile output.

    When `--conf` is passed, the compiled conf's executionInfo is layered on
    *top* of the current teams.<env>.py merge (default + the conf's team).
    This way, new keys added to the teams file after compile time still
    surface, while any value baked into the conf wins (matching what the
    runtime sees)."""
    teams_file_name = _teams_file_for_env(env)
    team_dict = _load_teams_silent(chronon_root, env=env)

    if conf:
        execution_info, conf_team = _exec_info_from_conf(chronon_root, conf)
        # `--team` overrides the conf-embedded team if explicitly passed.
        target_team = team or conf_team
        base: Dict[str, str] = {}
        if target_team in team_dict:
            metadata = _merged_execution_info_from_team(team_dict, target_team, teams_file_name)
            base = _flatten_from_metadata(metadata)
        # Conf snapshot wins over current teams.<env>.py for keys in both.
        return {**base, **_flatten_from_dict(execution_info)}, target_team

    target = team or parse_teams._DEFAULT_CONF_TEAM
    metadata = _merged_execution_info_from_team(team_dict, target, teams_file_name)
    return _flatten_from_metadata(metadata), target


def collect_values_for_all_teams(
    chronon_root: str, env: str = "prod"
) -> Dict[str, Dict[str, str]]:
    """Return `{team_name: prefixed_values}` for every team in teams.<env>.py.
    Each team's values are merged with the `default` team from that same
    teams file, matching what the compiler would emit for that env."""
    teams_file_name = _teams_file_for_env(env)
    team_dict = _load_teams_silent(chronon_root, env=env)
    result: Dict[str, Dict[str, str]] = {}
    for team_name in team_dict:
        metadata = _merged_execution_info_from_team(team_dict, team_name, teams_file_name)
        result[team_name] = _flatten_from_metadata(metadata)
    return result


def _matches_key(stored_key: str, query: str) -> bool:
    """Case-insensitive key match. The stored key is fully-qualified
    (e.g. `env.CUSTOMER_ID`); a query may match either the full key or the
    bare name (the part after the section prefix and any mode segment)."""
    stored_lower = stored_key.lower()
    query_lower = query.lower()
    if stored_lower == query_lower:
        return True
    # Allow lookup by bare name (everything after the section/mode prefix).
    parts = stored_key.split(".", 1)
    if len(parts) == 2 and parts[1].lower() == query_lower:
        return True
    return False


def _print_table(values: Dict[str, str], source_team: str, format: Format):
    if format == Format.JSON:
        print(json.dumps({"team": source_team, "values": values}, indent=2, sort_keys=True))
        return
    if not values:
        console.print(f"[dim]No config values found for team '{source_team}'.[/dim]")
        return
    table = Table(title=f"Zipline config (team: {source_team})", show_lines=False)
    table.add_column("Key", style="cyan", overflow="fold")
    table.add_column("Value", style="white", overflow="fold")
    for key in sorted(values.keys()):
        table.add_row(key, values[key])
    console.print(table)


def _ordered_team_names(team_names) -> List[str]:
    """Sort team names with `default` pinned first, then alphabetical."""
    others = sorted(name for name in team_names if name != parse_teams._DEFAULT_CONF_TEAM)
    if parse_teams._DEFAULT_CONF_TEAM in team_names:
        return [parse_teams._DEFAULT_CONF_TEAM] + others
    return others


def _print_all_teams(values_by_team: Dict[str, Dict[str, str]], format: Format):
    if format == Format.JSON:
        # Order keys with `default` first via an explicit dict (Python preserves insertion order).
        ordered = {name: values_by_team[name] for name in _ordered_team_names(values_by_team)}
        print(json.dumps({"teams": ordered}, indent=2))
        return
    if not values_by_team:
        console.print("[dim]No teams found in teams.py.[/dim]")
        return
    for team_name in _ordered_team_names(values_by_team):
        _print_table(values_by_team[team_name], team_name, format)


# ── CLI ───────────────────────────────────────────────────────────────


_team_option = click.option(
    "--team",
    default=None,
    help="Team name from teams.py (e.g. 'gcp', 'aws'). Defaults to the 'default' team.",
)
_conf_option = click.option(
    "--conf",
    default=None,
    help=(
        "Path to a compiled conf file (e.g. compiled/joins/team/my_join). "
        "When set, values come from the conf's merged executionInfo."
    ),
)
_chronon_root_option = chronon_root_option(
    help="Path to the Chronon root (containing teams.py). Auto-discovered if omitted.",
)
_env_option = env_option(
    help="Which teams file to inspect: 'prod' (teams.py) or 'canary' (teams.canary.py).",
)
_format_option = format_option()


@click.group(name="config", help="Inspect values defined in the nearest teams.py.")
def config():
    pass


@config.command("list")
@click.option(
    "--all",
    "all_teams",
    is_flag=True,
    default=False,
    help="List config for every team in the selected teams file instead of a single team.",
)
@_team_option
@_conf_option
@_chronon_root_option
@_env_option
@_format_option
@jsonify_exceptions_if_json_format
def list_cmd(all_teams, team, conf, chronon_root, env, format):
    """List all config values for the resolved team (and optional conf)."""
    root = _resolve_chronon_root(chronon_root)
    if all_teams:
        if team or conf:
            raise click.ClickException("--all cannot be combined with --team or --conf.")
        values_by_team = collect_values_for_all_teams(root, env=env)
        _print_all_teams(values_by_team, format)
        return
    values, source_team = collect_values(root, team=team, conf=conf, env=env)
    _print_table(values, source_team, format)


@config.command("get")
@click.argument("key")
@_team_option
@_conf_option
@_chronon_root_option
@_env_option
@_format_option
@jsonify_exceptions_if_json_format
def get_cmd(key, team, conf, chronon_root, env, format):
    """Get a config value by KEY (case-insensitive).

    KEY may be the fully-qualified name (e.g. 'env.CUSTOMER_ID') or the bare
    name (e.g. 'CUSTOMER_ID'). When multiple sections define the same bare
    key, all matches are returned.
    """
    root = _resolve_chronon_root(chronon_root)
    values, source_team = collect_values(root, team=team, conf=conf, env=env)
    matches = {k: v for k, v in values.items() if _matches_key(k, key)}
    if format == Format.JSON:
        print(json.dumps({"team": source_team, "key": key, "values": matches}, indent=2, sort_keys=True))
        return
    if not matches:
        raise click.ClickException(
            f"Key '{key}' not found for team '{source_team}'. "
            f"Run `zipline config list{f' --team {team}' if team else ''}` to see available keys."
        )
    if len(matches) == 1:
        console.print(next(iter(matches.values())))
        return
    for k in sorted(matches.keys()):
        console.print(f"{k}={matches[k]}")


# Public entry-point list for tests.
__all__ = [
    "config",
    "collect_values",
    "_find_chronon_root",
    "_matches_key",
    "_flatten_section",
]
