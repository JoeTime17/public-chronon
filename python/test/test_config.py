"""Tests for the `zipline config` CLI."""
import json
import os
import re
import textwrap

import pytest
from click.testing import CliRunner

from ai.chronon.repo.config import (
    _find_chronon_root,
    _flatten_section,
    _matches_key,
    collect_values,
    collect_values_for_all_teams,
    config,
)


def strip_ansi(text):
    return re.sub(r"\x1b\[[0-9;]*[mGJKHF]", "", text)


@pytest.fixture
def temp_repo(tmp_path):
    """Create a minimal Zipline-style repo with a teams.py for tests."""
    teams_py = tmp_path / "teams.py"
    teams_py.write_text(textwrap.dedent('''
        from gen_thrift.api.ttypes import Team
        from ai.chronon.repo.constants import RunMode
        from ai.chronon.types import ConfigProperties, EnvironmentVariables

        default = Team(
            outputNamespace="default_ns",
            conf=ConfigProperties(
                common={
                    "spark.chronon.partition.column": "ds",
                },
            ),
            env=EnvironmentVariables(
                common={
                    "VERSION": "latest",
                    "CUSTOMER_ID": "default-customer",
                },
            ),
        )

        gcp = Team(
            outputNamespace="gcp_ns",
            conf=ConfigProperties(
                common={
                    "spark.chronon.partition.column": "_DATE",
                    "spark.driver.memory": "4g",
                },
            ),
            env=EnvironmentVariables(
                common={
                    "CUSTOMER_ID": "canary",
                    "GCP_PROJECT_ID": "canary-443022",
                    "GCP_REGION": "us-central1",
                },
                modeEnvironments={
                    RunMode.UPLOAD: {
                        "GCP_REGION": "us-central1",
                        "SPARK_CLUSTER_NAME": "transient-upload",
                    },
                },
            ),
        )

        aws = Team(
            outputNamespace="aws_ns",
            env=EnvironmentVariables(
                common={
                    "CUSTOMER_ID": "aws-canary",
                    "AWS_REGION": "us-west-2",
                },
            ),
        )
    ''').lstrip())
    return tmp_path


@pytest.fixture
def sample_compiled_conf(tmp_path):
    """A compiled conf file with a representative metaData.executionInfo."""
    conf_path = tmp_path / "compiled" / "joins" / "sample_team" / "demo.v1__0"
    conf_path.parent.mkdir(parents=True)
    conf_path.write_text(json.dumps({
        "metaData": {
            "name": "sample_team.demo.v1",
            "team": "sample_team",
            "executionInfo": {
                "env": {
                    "common": {
                        "CUSTOMER_ID": "from-conf",
                        "EXTRA": "conf-only",
                    },
                    "modeEnvironments": {
                        "backfill": {
                            "EXECUTOR_CORES": "4",
                        },
                    },
                },
                "conf": {
                    "common": {
                        "spark.chronon.partition.column": "ds_conf",
                    },
                },
            },
        },
    }))
    return conf_path


# --- _find_chronon_root --------------------------------------------------


def test_find_chronon_root_walks_up(tmp_path):
    repo = tmp_path / "repo"
    nested = repo / "group_bys" / "team"
    nested.mkdir(parents=True)
    (repo / "teams.py").write_text("default = None\n")
    assert _find_chronon_root(str(nested)) == str(repo)


def test_find_chronon_root_returns_none_when_missing(tmp_path):
    nested = tmp_path / "no_repo_here"
    nested.mkdir()
    assert _find_chronon_root(str(nested)) is None


# --- _flatten_section ----------------------------------------------------


def test_flatten_section_dedupes_mode_values_matching_common():
    out = _flatten_section(
        "env",
        common={"FOO": "1", "BAR": "2"},
        modes={"backfill": {"FOO": "1", "BAR": "2"}, "upload": {"FOO": "override"}},
    )
    # Mode values that match common are not duplicated.
    assert out == {
        "env.FOO": "1",
        "env.BAR": "2",
        "env.upload.FOO": "override",
    }


def test_flatten_section_handles_none_inputs():
    assert _flatten_section("conf", None, None) == {}


# --- _matches_key --------------------------------------------------------


def test_matches_key_full_qualified_match_is_case_insensitive():
    assert _matches_key("env.CUSTOMER_ID", "ENV.customer_id")


def test_matches_key_bare_name_matches_after_section():
    assert _matches_key("env.CUSTOMER_ID", "customer_id")
    assert _matches_key("conf.spark.chronon.partition.column", "spark.chronon.partition.column")


def test_matches_key_does_not_match_unrelated_keys():
    assert not _matches_key("env.CUSTOMER_ID", "GCP_PROJECT_ID")


# --- collect_values ------------------------------------------------------


def test_collect_values_default_team(temp_repo):
    values, source = collect_values(str(temp_repo))
    assert source == "default"
    assert values["env.VERSION"] == "latest"
    assert values["env.CUSTOMER_ID"] == "default-customer"
    assert values["conf.spark.chronon.partition.column"] == "ds"


def test_collect_values_specific_team_merges_with_default(temp_repo):
    values, source = collect_values(str(temp_repo), team="gcp")
    assert source == "gcp"
    # gcp overrides default's CUSTOMER_ID
    assert values["env.CUSTOMER_ID"] == "canary"
    # default's VERSION is inherited
    assert values["env.VERSION"] == "latest"
    # gcp-specific value present
    assert values["env.GCP_PROJECT_ID"] == "canary-443022"
    # gcp overrides default's partition column
    assert values["conf.spark.chronon.partition.column"] == "_DATE"
    # gcp-only conf
    assert values["conf.spark.driver.memory"] == "4g"


def test_collect_values_includes_mode_overrides_only_when_different(temp_repo):
    values, _ = collect_values(str(temp_repo), team="gcp")
    # GCP_REGION matches common in upload mode → not duplicated
    assert "env.upload.GCP_REGION" not in values
    # SPARK_CLUSTER_NAME only exists in upload mode → emitted
    assert values["env.upload.SPARK_CLUSTER_NAME"] == "transient-upload"


def test_collect_values_unknown_team_raises(temp_repo):
    import click
    with pytest.raises(click.ClickException) as exc:
        collect_values(str(temp_repo), team="nope")
    assert "nope" in str(exc.value.message)


def test_collect_values_canary_env_loads_canary_teams_file(temp_repo):
    """`--env canary` must load teams.canary.py, not teams.py. The canary file
    can declare totally different env values for the same team name; the
    inspector should surface those instead of the prod ones."""
    canary_py = temp_repo / "teams.canary.py"
    canary_py.write_text(textwrap.dedent('''
        from gen_thrift.api.ttypes import Team
        from ai.chronon.types import EnvironmentVariables

        default = Team(
            outputNamespace="default_ns_canary",
            env=EnvironmentVariables(
                common={
                    "VERSION": "canary-build",
                    "CUSTOMER_ID": "canary-customer",
                },
            ),
        )

        gcp = Team(
            outputNamespace="gcp_ns_canary",
            env=EnvironmentVariables(
                common={
                    "CUSTOMER_ID": "gcp-canary",
                    "GCP_PROJECT_ID": "canary-project",
                },
            ),
        )
    ''').lstrip())

    # prod path still works and surfaces prod values.
    prod_values, _ = collect_values(str(temp_repo), team="gcp", env="prod")
    assert prod_values["env.CUSTOMER_ID"] == "canary"  # prod's gcp uses this
    assert prod_values["env.GCP_PROJECT_ID"] == "canary-443022"

    # canary path surfaces values from teams.canary.py.
    canary_values, source = collect_values(str(temp_repo), team="gcp", env="canary")
    assert source == "gcp"
    assert canary_values["env.CUSTOMER_ID"] == "gcp-canary"
    assert canary_values["env.GCP_PROJECT_ID"] == "canary-project"
    # default-team inheritance from teams.canary.py (not teams.py).
    assert canary_values["env.VERSION"] == "canary-build"


def test_collect_values_canary_env_without_teams_canary_py_raises(temp_repo):
    """`--env canary` against a repo with no teams.canary.py should fail with
    an actionable error pointing the user at the missing file."""
    import click
    with pytest.raises(click.ClickException) as exc:
        collect_values(str(temp_repo), env="canary")
    assert "teams.canary.py" in str(exc.value.message)


def test_collect_values_unsupported_env_raises(temp_repo):
    """Only prod/canary are supported (Thrift Environment enum). Anything else
    must error rather than silently fall back."""
    import click
    with pytest.raises(click.ClickException) as exc:
        collect_values(str(temp_repo), env="staging")
    assert "staging" in str(exc.value.message)


def test_collect_values_with_conf_path(temp_repo, sample_compiled_conf):
    rel_conf = os.path.relpath(sample_compiled_conf, temp_repo)
    values, source = collect_values(str(temp_repo), conf=rel_conf)
    assert source == "sample_team"
    assert values["env.CUSTOMER_ID"] == "from-conf"
    assert values["env.EXTRA"] == "conf-only"
    assert values["env.backfill.EXECUTOR_CORES"] == "4"
    assert values["conf.spark.chronon.partition.column"] == "ds_conf"


def test_collect_values_with_absolute_conf_path(temp_repo, sample_compiled_conf):
    values, _ = collect_values(str(temp_repo), conf=str(sample_compiled_conf))
    assert values["env.EXTRA"] == "conf-only"


def test_collect_values_with_conf_layers_current_teams_py(temp_repo, tmp_path):
    """A compiled conf is a snapshot — so values added to teams.py *after*
    compile time should still surface via --conf, while values baked into the
    conf snapshot win over current teams.py for keys present in both."""
    # Conf was compiled when default-team didn't have NEW_DEFAULT_KEY, but it
    # had its own opinion on CUSTOMER_ID.
    conf_path = tmp_path / "compiled" / "joins" / "gcp" / "stale.v1__0"
    conf_path.parent.mkdir(parents=True)
    conf_path.write_text(json.dumps({
        "metaData": {
            "name": "gcp.stale.v1",
            "team": "gcp",
            "executionInfo": {
                "env": {
                    "common": {
                        "CUSTOMER_ID": "snapshot-customer",
                    },
                },
            },
        },
    }))
    # Add a key to the temp_repo's default team that wasn't in the snapshot.
    (temp_repo / "teams.py").write_text(
        (temp_repo / "teams.py").read_text().replace(
            '"CUSTOMER_ID": "default-customer",',
            '"CUSTOMER_ID": "default-customer",\n                    "NEW_DEFAULT_KEY": "appeared-after-compile",',
        )
    )
    values, source = collect_values(str(temp_repo), conf=str(conf_path))
    assert source == "gcp"
    # New default-team key surfaces even though it's not in the conf snapshot.
    assert values["env.NEW_DEFAULT_KEY"] == "appeared-after-compile"
    # Conf snapshot wins over teams.py when both define the key.
    assert values["env.CUSTOMER_ID"] == "snapshot-customer"


def test_collect_values_with_conf_falls_back_when_team_missing(temp_repo, sample_compiled_conf):
    """If the conf's team isn't in the current teams.py, fall back to just
    the snapshot — don't blow up."""
    # sample_compiled_conf has team="sample_team", which isn't in temp_repo.
    values, source = collect_values(str(temp_repo), conf=str(sample_compiled_conf))
    assert source == "sample_team"
    assert values["env.CUSTOMER_ID"] == "from-conf"


def test_collect_values_missing_conf_raises(temp_repo):
    import click
    with pytest.raises(click.ClickException) as exc:
        collect_values(str(temp_repo), conf="compiled/missing/conf")
    assert "not found" in str(exc.value.message)


# --- collect_values_for_all_teams ---------------------------------------


def test_collect_values_for_all_teams_returns_every_team(temp_repo):
    by_team = collect_values_for_all_teams(str(temp_repo))
    assert set(by_team.keys()) == {"default", "gcp", "aws"}
    # Each team should be merged with default's env
    assert by_team["aws"]["env.VERSION"] == "latest"
    # gcp's CUSTOMER_ID overrides default's
    assert by_team["gcp"]["env.CUSTOMER_ID"] == "canary"
    assert by_team["aws"]["env.CUSTOMER_ID"] == "aws-canary"
    assert by_team["default"]["env.CUSTOMER_ID"] == "default-customer"


# --- CLI: list -----------------------------------------------------------


def _invoke(args, cwd):
    """Run the config CLI with cwd set, isolated from the parent's env."""
    runner = CliRunner()
    prev_cwd = os.getcwd()
    prev_env = os.environ.pop("CHRONON_ROOT", None)
    try:
        os.chdir(cwd)
        return runner.invoke(config, args)
    finally:
        os.chdir(prev_cwd)
        if prev_env is not None:
            os.environ["CHRONON_ROOT"] = prev_env


def test_list_default_team_text_output(temp_repo):
    result = _invoke(["list"], cwd=str(temp_repo))
    assert result.exit_code == 0, result.output
    out = strip_ansi(result.output)
    assert "default-customer" in out
    assert "env.VERSION" in out


def test_list_with_team_option(temp_repo):
    result = _invoke(["list", "--team", "gcp"], cwd=str(temp_repo))
    assert result.exit_code == 0, result.output
    out = strip_ansi(result.output)
    assert "canary" in out
    assert "GCP_PROJECT_ID" in out


def test_list_json_output(temp_repo):
    result = _invoke(["list", "--team", "gcp", "-f", "json"], cwd=str(temp_repo))
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["team"] == "gcp"
    assert payload["values"]["env.CUSTOMER_ID"] == "canary"


def test_list_walks_up_to_find_teams_py(temp_repo):
    nested = temp_repo / "group_bys" / "some_team"
    nested.mkdir(parents=True)
    result = _invoke(["list"], cwd=str(nested))
    assert result.exit_code == 0, result.output
    assert "default-customer" in strip_ansi(result.output)


def test_list_errors_when_no_teams_py(tmp_path):
    result = _invoke(["list"], cwd=str(tmp_path))
    assert result.exit_code != 0
    assert "Could not find teams.py" in strip_ansi(result.output)


def test_list_with_conf(temp_repo, sample_compiled_conf):
    rel = os.path.relpath(sample_compiled_conf, temp_repo)
    result = _invoke(["list", "--conf", rel], cwd=str(temp_repo))
    assert result.exit_code == 0, result.output
    out = strip_ansi(result.output)
    assert "from-conf" in out
    assert "conf-only" in out


def test_list_all_text_output(temp_repo):
    result = _invoke(["list", "--all"], cwd=str(temp_repo))
    assert result.exit_code == 0, result.output
    out = strip_ansi(result.output)
    # All three team headers should be present
    assert "team: default" in out
    assert "team: gcp" in out
    assert "team: aws" in out
    # Team-specific values should appear
    assert "aws-canary" in out
    # `default` should render first, before any other team
    default_idx = out.index("team: default")
    assert default_idx < out.index("team: aws")
    assert default_idx < out.index("team: gcp")


def test_list_all_json_output(temp_repo):
    result = _invoke(["list", "--all", "-f", "json"], cwd=str(temp_repo))
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    keys = list(payload["teams"].keys())
    assert keys[0] == "default"  # `default` pinned first
    assert set(keys) == {"default", "gcp", "aws"}
    assert payload["teams"]["gcp"]["env.CUSTOMER_ID"] == "canary"


def test_list_all_rejects_team_flag(temp_repo):
    result = _invoke(["list", "--all", "--team", "gcp"], cwd=str(temp_repo))
    assert result.exit_code != 0
    assert "cannot be combined" in strip_ansi(result.output)


def test_list_all_rejects_conf_flag(temp_repo, sample_compiled_conf):
    rel = os.path.relpath(sample_compiled_conf, temp_repo)
    result = _invoke(["list", "--all", "--conf", rel], cwd=str(temp_repo))
    assert result.exit_code != 0
    assert "cannot be combined" in strip_ansi(result.output)


def test_list_with_chronon_root_option(temp_repo):
    nested = temp_repo / "deep" / "place"
    nested.mkdir(parents=True)
    result = _invoke(
        ["list", "--chronon-root", str(temp_repo)],
        cwd=str(nested),
    )
    assert result.exit_code == 0, result.output


# --- CLI: get ------------------------------------------------------------


def test_get_returns_value_for_full_qualified_key(temp_repo):
    result = _invoke(["get", "env.CUSTOMER_ID"], cwd=str(temp_repo))
    assert result.exit_code == 0, result.output
    assert strip_ansi(result.output).strip() == "default-customer"


def test_get_is_case_insensitive(temp_repo):
    result = _invoke(["get", "ENV.customer_id"], cwd=str(temp_repo))
    assert result.exit_code == 0, result.output
    assert strip_ansi(result.output).strip() == "default-customer"


def test_get_bare_key_works(temp_repo):
    result = _invoke(["get", "VERSION"], cwd=str(temp_repo))
    assert result.exit_code == 0, result.output
    assert strip_ansi(result.output).strip() == "latest"


def test_get_with_team_option(temp_repo):
    result = _invoke(["get", "CUSTOMER_ID", "--team", "gcp"], cwd=str(temp_repo))
    assert result.exit_code == 0, result.output
    assert strip_ansi(result.output).strip() == "canary"


def test_get_missing_key_errors(temp_repo):
    result = _invoke(["get", "NOT_A_REAL_KEY"], cwd=str(temp_repo))
    assert result.exit_code != 0
    assert "not found" in strip_ansi(result.output)


def test_get_json_output(temp_repo):
    result = _invoke(
        ["get", "CUSTOMER_ID", "--team", "gcp", "-f", "json"],
        cwd=str(temp_repo),
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["team"] == "gcp"
    assert payload["values"] == {"env.CUSTOMER_ID": "canary"}


def test_get_with_conf_uses_conf_value(temp_repo, sample_compiled_conf):
    rel = os.path.relpath(sample_compiled_conf, temp_repo)
    result = _invoke(
        ["get", "CUSTOMER_ID", "--conf", rel],
        cwd=str(temp_repo),
    )
    assert result.exit_code == 0, result.output
    assert strip_ansi(result.output).strip() == "from-conf"
