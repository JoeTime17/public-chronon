"""Tests for the `zipline compare-tables` CLI helpers.

These cover the pure helpers — argument building, metric aggregation,
hostname extraction, env-var interpolation, and the teams.py loader. The
network-touching paths (OAuth token fetch, SQL warehouse query, EMR submit)
are exercised end-to-end manually rather than in unit tests.
"""
import json
import os
import textwrap

import pytest

from click.testing import CliRunner

from ai.chronon.repo.compare_tables import (
    _aggregate_metrics,
    _build_spark_args,
    _create_temp_conf,
    _default_output_tables,
    _derive_emr_log_uri,
    _derive_emr_role,
    _extract_databricks_hostname,
    _interpolate_env_vars,
    _list_arg,
    _load_team_config,
    _parse_customer_name,
    _parse_left_table,
    _quote_table,
    _spark_configs_use_databricks,
    _split_metric_column,
    compare_tables,
)


# --- _split_metric_column ------------------------------------------------


@pytest.mark.parametrize(
    "col, expected",
    [
        ("merchant_id_mismatch_sum", ("merchant_id", "mismatch_sum")),
        ("price_cents_total_count", ("price_cents", "total_count")),
        ("price_smape_average", ("price", "smape_average")),
        ("foo_left_null_sum", ("foo", "left_null_sum")),
        # Longest suffix wins so "left_minus_right_approx_percentile" beats
        # the shorter "_approx_percentile" suffixes.
        (
            "x_left_minus_right_approx_percentile",
            ("x", "left_minus_right_approx_percentile"),
        ),
    ],
)
def test_split_metric_column_recognizes_known_suffixes(col, expected):
    assert _split_metric_column(col) == expected


@pytest.mark.parametrize("col", ["JoinPath", "ts", "random_column"])
def test_split_metric_column_returns_none_for_non_metric_columns(col):
    assert _split_metric_column(col) is None


# --- _aggregate_metrics --------------------------------------------------


def test_aggregate_metrics_sums_counts_and_averages_smape():
    cols = [
        "JoinPath",
        "price_mismatch_sum",
        "price_total_count",
        "price_smape_average",
        "price_left_null_sum",
    ]
    rows = [
        ("v1_vs_v2", 2, 10, 0.10, 1),
        ("v1_vs_v2", 3, 15, 0.20, 0),
    ]
    out = _aggregate_metrics(cols, rows)
    assert out["price"]["mismatch_sum"] == 5
    assert out["price"]["total_count"] == 25
    assert out["price"]["left_null_sum"] == 1
    assert out["price"]["smape_average"] == pytest.approx(0.15)


def test_aggregate_metrics_drops_smape_when_all_null():
    cols = ["price_mismatch_sum", "price_total_count", "price_smape_average"]
    rows = [(0, 5, None), (1, 5, None)]
    out = _aggregate_metrics(cols, rows)
    assert "smape_average" not in out["price"]
    assert out["price"]["mismatch_sum"] == 1
    assert out["price"]["total_count"] == 10


def test_aggregate_metrics_handles_empty_input():
    assert _aggregate_metrics([], []) == {}


def test_aggregate_metrics_ignores_non_metric_columns():
    cols = ["JoinPath", "ts", "price_mismatch_sum", "price_total_count"]
    rows = [("v1", 12345, 1, 4)]
    out = _aggregate_metrics(cols, rows)
    assert set(out) == {"price"}


# --- _extract_databricks_hostname ----------------------------------------


def test_extract_databricks_hostname_from_catalog_uri():
    configs = {
        "spark.sql.catalog.workspace.uri": "https://dbc-050d6f00-dcb3.cloud.databricks.com",
        "spark.master": "yarn",
    }
    assert _extract_databricks_hostname(configs) == "dbc-050d6f00-dcb3.cloud.databricks.com"


def test_extract_databricks_hostname_from_oauth_uri():
    configs = {
        "spark.sql.catalog.workspace.auth.oauth.uri": (
            "https://dbc-x.cloud.databricks.com/oidc/v1/token"
        ),
    }
    assert _extract_databricks_hostname(configs) == "dbc-x.cloud.databricks.com"


def test_extract_databricks_hostname_returns_none_when_no_databricks_url():
    configs = {
        "spark.sql.catalog.glue.uri": "https://glue.us-west-2.amazonaws.com",
        "spark.master": "yarn",
    }
    assert _extract_databricks_hostname(configs) is None


def test_extract_databricks_hostname_skips_non_string_values():
    # Defensive: configs are nominally string→string, but we don't want to
    # crash if a value somehow comes through as something else.
    assert _extract_databricks_hostname({"spark.sql.catalog.workspace.uri": 42}) is None


# --- _interpolate_env_vars ----------------------------------------------


def test_interpolate_env_vars_substitutes_known_vars(monkeypatch):
    monkeypatch.setenv("CID", "client123")
    monkeypatch.setenv("CSEC", "secretXYZ")
    out = _interpolate_env_vars({
        "auth": "{CID}:{CSEC}",
        "literal": "no_placeholder",
    })
    assert out == {"auth": "client123:secretXYZ", "literal": "no_placeholder"}


def test_interpolate_env_vars_leaves_unresolvable_placeholders():
    out = _interpolate_env_vars({"k": "{DEFINITELY_NOT_SET_PLEASE}"})
    assert out == {"k": "{DEFINITELY_NOT_SET_PLEASE}"}


def test_interpolate_env_vars_ignores_non_matching_braces():
    # Lowercase and digits-first don't match the regex.
    assert _interpolate_env_vars({"k": "{lowercase}"}) == {"k": "{lowercase}"}
    assert _interpolate_env_vars({"k": "{1ABC}"}) == {"k": "{1ABC}"}


def test_interpolate_env_vars_substitutes_multiple_in_one_value(monkeypatch):
    """Matches `NodeSubmitter.interpolateEnvVars` on the orchestration side,
    which uses `replaceAllIn` so multi-placeholder values like
    `jdbc://{HOST}:{PORT}/db` resolve correctly."""
    monkeypatch.setenv("HOST", "my-host")
    monkeypatch.setenv("PORT", "5432")
    out = _interpolate_env_vars({"url": "jdbc://{HOST}:{PORT}/db"})
    assert out["url"] == "jdbc://my-host:5432/db"


# --- _list_arg ----------------------------------------------------------


def test_list_arg_splits_on_commas():
    assert _list_arg("--keys", "a,b,c") == ["--keys", "a", "b", "c"]


def test_list_arg_strips_whitespace():
    assert _list_arg("--keys", " a , b , c ") == ["--keys", "a", "b", "c"]


def test_list_arg_single_value():
    assert _list_arg("--keys", "only") == ["--keys", "only"]


@pytest.mark.parametrize("bad", ["", ",", " , ", ",,", "  "])
def test_list_arg_rejects_empty_value(bad):
    """Empty / comma-only / whitespace-only values would silently send a
    malformed argv (`['--keys']` with no following values) to the Driver.
    Fail locally with a usage error instead.
    """
    import click as _click
    with pytest.raises(_click.UsageError):
        _list_arg("--keys", bad)


# --- _build_spark_args --------------------------------------------------


def test_build_spark_args_minimum_required():
    out = _build_spark_args(
        left_table="lt",
        right_table="rt",
        keys="k1,k2",
        side_by_side_table="sbs",
        metrics_table="met",
    )
    assert isinstance(out, list)
    assert out[0] == "compare-tables"
    assert "--left-table=lt" in out
    assert "--right-table=rt" in out
    # --keys is a Scallop List[String] flag: expanded into separate argv elements.
    keys_idx = out.index("--keys")
    assert out[keys_idx + 1] == "k1" and out[keys_idx + 2] == "k2"
    assert "--side-by-side-table=sbs" in out
    assert "--metrics-table=met" in out
    # Optional flags omitted when not set
    assert not any(a.startswith("--mapping") for a in out)
    assert "--migration-check" not in out


def test_build_spark_args_includes_all_optional_flags():
    out = _build_spark_args(
        left_table="lt",
        right_table="rt",
        keys="k",
        side_by_side_table="sbs",
        metrics_table="met",
        mapping='{"a":"b"}',
        timestamp_millis_left="ts_l * 1000",
        timestamp_millis_right="ts_r * 1000",
        left_columns="c1,c2",
        migration_check=True,
    )
    assert '--mapping={"a":"b"}' in out
    assert "--timestamp-millis-left=ts_l * 1000" in out
    assert "--timestamp-millis-right=ts_r * 1000" in out
    cols_idx = out.index("--left-columns")
    assert out[cols_idx + 1] == "c1" and out[cols_idx + 2] == "c2"
    assert "--migration-check" in out


# --- _quote_table -------------------------------------------------------


@pytest.mark.parametrize(
    "ident, expected",
    [
        ("workspace.poc.metrics", "`workspace`.`poc`.`metrics`"),
        ("workspace_iceberg.poc.compare_metrics", "`workspace_iceberg`.`poc`.`compare_metrics`"),
        ("dev-canary.poc.t", "`dev-canary`.`poc`.`t`"),  # hyphens allowed
        ("table_only", "`table_only`"),
    ],
)
def test_quote_table_accepts_valid_identifiers(ident, expected):
    assert _quote_table(ident) == expected


@pytest.mark.parametrize(
    "bad",
    [
        "workspace.poc.metrics; DROP TABLE x",  # semicolon + space — cannot live inside backticks
        "workspace.poc.metrics' OR 1=1",        # quote chars
        "workspace.poc.`metrics`",              # embedded backticks — would break our quoting
        "workspace.123starts_with_digit.t",     # bad identifier (starts with digit)
        ".leading.dot",                         # empty leading segment
        "",                                     # empty
    ],
)
def test_quote_table_rejects_unsafe_or_malformed_identifiers(bad):
    """Anything that would break out of `\\`...\\`` backticking, or doesn't look
    like an identifier, must be rejected before reaching SQL.
    """
    import click
    with pytest.raises(click.ClickException):
        _quote_table(bad)


def test_build_spark_args_keeps_space_containing_values_atomic():
    """Regression: values with spaces (mapping JSON, timestamp SQL exprs) must
    survive subprocess argv tokenization. If anything in the pipeline `.split()`s
    the result back into a string this test catches it.
    """
    out = _build_spark_args(
        left_table="lt",
        right_table="rt",
        keys="k",
        side_by_side_table="sbs",
        metrics_table="met",
        mapping='{"old_col": "new_col", "amount": "CAST(value AS DOUBLE)"}',
        timestamp_millis_left="cast(ts_l as long) * 1000",
        timestamp_millis_right="cast(ts_r as long) * 1000",
    )
    # Each space-containing value must occupy exactly one slot in the argv list.
    assert '--mapping={"old_col": "new_col", "amount": "CAST(value AS DOUBLE)"}' in out
    assert "--timestamp-millis-left=cast(ts_l as long) * 1000" in out
    assert "--timestamp-millis-right=cast(ts_r as long) * 1000" in out


# --- _parse_customer_name / _derive_emr_* --------------------------------


@pytest.mark.parametrize(
    "artifact_prefix, expected",
    [
        ("s3://zipline-artifacts-canary", "canary"),
        ("s3://zipline-artifacts-acme-corp", "acme-corp"),
        ("s3://zipline-artifacts-foo/bar/baz", "foo"),  # trailing path is fine
        ("s3://zipline-artifacts-x_y_z", "x_y_z"),
        ("s3://some-other-bucket", None),
        ("", None),
        (None, None),
    ],
)
def test_parse_customer_name(artifact_prefix, expected):
    assert _parse_customer_name(artifact_prefix) == expected


def test_derive_emr_role_uses_tf_convention():
    arn = _derive_emr_role("345594603419", "canary")
    assert arn == "arn:aws:iam::345594603419:role/zipline_canary_emr_serverless_role"


def test_derive_emr_role_returns_none_on_missing_input():
    assert _derive_emr_role(None, "canary") is None
    assert _derive_emr_role("345594603419", None) is None


def test_derive_emr_log_uri():
    assert _derive_emr_log_uri("canary") == "s3://zipline-logs-canary/emr/"
    assert _derive_emr_log_uri(None) is None


# --- _parse_left_table + _default_output_tables -------------------------


@pytest.mark.parametrize(
    "left, expected",
    [
        ("workspace.poc.dim_listings", ("workspace", "poc", "dim_listings")),
        ("a.b.c", ("a", "b", "c")),
        # Malformed: not catalog.schema.table → (None, None, None) → caller errors.
        ("workspace.dim_listings", (None, None, None)),
        ("dim_listings", (None, None, None)),
        ("a.b.c.d", (None, None, None)),
        ("", (None, None, None)),
        (None, (None, None, None)),
    ],
)
def test_parse_left_table(left, expected):
    assert _parse_left_table(left) == expected


def test_default_output_tables_fall_back_to_left_table():
    sbs, met = _default_output_tables(
        "workspace.poc.dim_listings", None, None, "20260513_120000"
    )
    assert sbs == "workspace.poc.compare_sbs_20260513_120000"
    assert met == "workspace.poc.compare_metrics_20260513_120000"


def test_default_output_tables_respects_overrides():
    # `--output-catalog` wins over left's catalog (e.g. write to iceberg catalog).
    sbs, met = _default_output_tables(
        "workspace.poc.dim_listings", "workspace_iceberg", None, "20260513_120000"
    )
    assert sbs == "workspace_iceberg.poc.compare_sbs_20260513_120000"
    # `--output-schema` wins over left's schema.
    sbs2, _ = _default_output_tables(
        "workspace.poc.dim_listings", None, "staging", "20260513_120000"
    )
    assert sbs2 == "workspace.staging.compare_sbs_20260513_120000"
    # Both pieces overridden.
    sbs3, _ = _default_output_tables(
        "workspace.poc.dim_listings", "iceberg_cat", "qa", "20260513_120000"
    )
    assert sbs3 == "iceberg_cat.qa.compare_sbs_20260513_120000"


def test_default_output_tables_errors_when_unresolvable():
    """Malformed left + missing overrides → clear ClickException."""
    import click as _click
    with pytest.raises(_click.ClickException, match="output catalog"):
        _default_output_tables("malformed", None, None, "20260513_120000")
    # Overriding catalog clears the catalog error → next missing piece (schema).
    with pytest.raises(_click.ClickException, match="output schema"):
        _default_output_tables("malformed", "cat", None, "20260513_120000")
    # Both overridden → works even with malformed left.
    sbs, _ = _default_output_tables("malformed", "cat", "schema", "20260513_120000")
    assert sbs == "cat.schema.compare_sbs_20260513_120000"


# --- _spark_configs_use_databricks --------------------------------------


def test_spark_configs_use_databricks_detects_placeholder():
    assert _spark_configs_use_databricks({"k": "{DATABRICKS_CREDENTIAL}"})
    assert _spark_configs_use_databricks({"k": "prefix-{DATABRICKS_CLIENT_ID}-suffix"})
    # Multiple values, any one match is enough.
    assert _spark_configs_use_databricks(
        {"a": "plain", "b": "{DATABRICKS_CLIENT_SECRET}"}
    )


def test_spark_configs_use_databricks_skips_when_no_placeholder():
    """AWS+Glue customer with no Databricks setup — no creds should be required."""
    assert not _spark_configs_use_databricks({"k": "plain-value"})
    assert not _spark_configs_use_databricks({"k": "{OTHER_VAR}"})
    assert not _spark_configs_use_databricks({})


# --- _create_temp_conf --------------------------------------------------


def test_create_temp_conf_writes_metadata_shape():
    path = _create_temp_conf({"spark.foo": "bar", "spark.baz": "1"})
    try:
        with open(path) as f:
            data = json.load(f)
        # The Driver-side JobSubmitter expects this exact structure when parsing
        # the MetaData object out of a local conf file.
        assert data["executionInfo"]["conf"]["common"] == {
            "spark.foo": "bar",
            "spark.baz": "1",
        }
        assert data["executionInfo"]["conf"]["modeConfigs"] == {}
    finally:
        os.unlink(path)


# --- _load_team_config --------------------------------------------------


@pytest.fixture
def team_repo(tmp_path):
    """Minimal Zipline-style repo with a teams.py exercising common + mode configs."""
    teams_py = tmp_path / "teams.py"
    teams_py.write_text(textwrap.dedent('''
        from gen_thrift.api.ttypes import Team
        from ai.chronon.repo.constants import RunMode
        from ai.chronon.types import ConfigProperties, EnvironmentVariables

        default = Team(
            outputNamespace="default_ns",
            env=EnvironmentVariables(common={"VERSION": "latest"}),
        )

        aws_databricks = Team(
            outputNamespace="aws_ns",
            conf=ConfigProperties(
                common={
                    "spark.sql.catalog.workspace.uri": "https://dbc-test.cloud.databricks.com",
                    "spark.driver.memory": "4g",
                },
                modeConfigs={
                    RunMode.BACKFILL: {
                        "spark.driver.memory": "8g",
                    },
                },
            ),
            env=EnvironmentVariables(
                common={
                    "CUSTOMER_ID": "test-canary",
                    "DATABRICKS_CLIENT_ID": "{CID_PLACEHOLDER}",
                    "DATABRICKS_WAREHOUSE_ID": "wh-abc-123",
                },
            ),
        )
    ''').lstrip())
    return tmp_path


def test_load_team_config_returns_common_env_and_conf(team_repo):
    env, conf = _load_team_config("aws_databricks", str(team_repo))
    # env_common comes through as-is (no interpolation on env).
    assert env["CUSTOMER_ID"] == "test-canary"
    # default-team values are inherited.
    assert env["VERSION"] == "latest"
    # The CLI uses this entry to default --warehouse-id when neither flag nor
    # shell env var is set, so it must survive the loader unchanged.
    assert env["DATABRICKS_WAREHOUSE_ID"] == "wh-abc-123"
    # conf_common comes through; mode-scoped overrides are intentionally dropped.
    assert conf["spark.sql.catalog.workspace.uri"] == "https://dbc-test.cloud.databricks.com"
    assert conf["spark.driver.memory"] == "4g"  # common, not the backfill 8g override


def test_load_team_config_returns_raw_values_without_interpolation(team_repo):
    """Loader hands back raw placeholders; interpolation is the submit-step's job
    so values resolved late (AWS_ACCOUNT_ID from STS, etc.) can feed placeholders.
    """
    env, _ = _load_team_config("aws_databricks", str(team_repo))
    assert env["DATABRICKS_CLIENT_ID"] == "{CID_PLACEHOLDER}"


# --- timestamp flag pair validation (symmetric) --------------------------


@pytest.fixture
def cli_args_common(team_repo):
    """Minimum required CLI args + --chronon-root pointing at the test fixture.
    --yes skips the confirmation prompt, --version skips the env-var fetch.
    """
    return [
        "--team", "aws_databricks",
        "--left-table", "workspace.poc.dim_listings",
        "--right-table", "workspace.poc.dim_listings_v2",
        "--keys", "listing_id",
        "--chronon-root", str(team_repo),
        "--version", "test-version",
        "--yes",
    ]


@pytest.mark.parametrize(
    "ts_flags",
    [
        ["--timestamp-millis-left", "ts * 1000"],   # left without right
        ["--timestamp-millis-right", "ts * 1000"],  # right without left
    ],
)
def test_timestamp_pair_required_both_directions(cli_args_common, ts_flags):
    """Setting only one of --timestamp-millis-left/right must fail locally — the
    Driver requires either both or neither.
    """
    result = CliRunner().invoke(compare_tables, cli_args_common + ts_flags)
    assert result.exit_code != 0
    assert "must be set together" in result.output


def test_timestamp_pair_neither_is_fine(cli_args_common, monkeypatch):
    """Both unset should pass the pair check (it'll fail later on Databricks
    creds, but past the pair check). We don't care about the eventual exit
    code — we just want to confirm we didn't fail on the pair check.
    """
    # Make sure DATABRICKS_CLIENT_ID/SECRET aren't set so we exit at the
    # next-stage check, not the pair check.
    monkeypatch.delenv("DATABRICKS_CLIENT_ID", raising=False)
    monkeypatch.delenv("DATABRICKS_CLIENT_SECRET", raising=False)
    result = CliRunner().invoke(compare_tables, cli_args_common)
    assert "must be set together" not in result.output
