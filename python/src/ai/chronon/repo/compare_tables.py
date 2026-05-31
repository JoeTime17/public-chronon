import base64
import contextlib
import io
import json
import logging
import os
import re
import subprocess
import tempfile
import time
import urllib.parse
import urllib.request

import boto3
import click

from ai.chronon.cli.options import chronon_root_option
from ai.chronon.cli.theme import console
from ai.chronon.logger import get_logger
from ai.chronon.repo.aws import (
    EMR_ENTRY,
    ZIPLINE_AWS_JAR_DEFAULT,
    AwsRunner,
)
from ai.chronon.repo.config import (
    _load_teams_silent,
    _merged_execution_info_from_team,
    _resolve_chronon_root,
)
from ai.chronon.repo.constants import ZIPLINE_DIRECTORY
from ai.chronon.repo.utils import (
    extract_filename_from_path,
    get_customer_id,
    upload_to_blob_store,
)

LOG = get_logger()


def _quiet_noisy_loggers():
    """Mute third-party loggers that flood the user's terminal with INFO/DEBUG.

    `ai.chronon.join` calls `logging.basicConfig(level=INFO)` at import time,
    which turns on INFO globally for everything that propagates to root. We
    don't want that for an interactive command — only WARN+ for libraries,
    and our own user-facing status goes through `console.print` (not logging).
    `ai.chronon.logger` is named explicitly because `get_logger()` sets its level
    to INFO at import time, so muting the `ai.chronon` parent doesn't cascade down.
    """
    # ERROR-only for libraries with chatty credential-refresh / version warnings.
    for name in (
        "boto3", "botocore", "botocore.credentials",
        "urllib3", "databricks", "databricks.sql.client",
    ):
        logging.getLogger(name).setLevel(logging.ERROR)
    # WARNING for chronon — we use console.print for user-facing output instead.
    for name in ("ai.chronon", "ai.chronon.logger"):
        logging.getLogger(name).setLevel(logging.WARNING)

SPARK_DRIVER_CLASS = "ai.chronon.spark.Driver"
COMPARE_TABLES_SUBCOMMAND = "compare-tables"

# CLI is currently only wired for the AWS path (EMR Serverless submitter). The
# guard in submit_compare_tables fails fast with a clear message for any other
# value, so users on GCP/Azure see "supported soon" not a cryptic crash.
_SUPPORTED_CLOUD_PROVIDERS = {"aws"}

# Catches both `{DATABRICKS_FOO}` and `{DATABRICKS}` style placeholders inside
# spark config values — used to decide whether DATABRICKS_CLIENT_ID/SECRET are
# actually required for this run (vs. an AWS+Glue customer who never touches DB).
_DATABRICKS_PLACEHOLDER_RE = re.compile(r"\{DATABRICKS[A-Z0-9_]*\}")

# Logback config we drop on disk and point java at via -Dlogback.configurationFile
# to override the noisy DEBUG default baked into the EMR submitter jar. Root WARN
# kills the AWS SDK and Apache HTTP DEBUG firehose; we keep ai.chronon at INFO so
# we still see "Started EMR Serverless job run: ..." which we parse for the job id.
_LOGBACK_QUIET_XML = """<configuration>
  <appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
    <encoder><pattern>%-5level %logger{0} - %msg%n</pattern></encoder>
  </appender>
  <root level="WARN"><appender-ref ref="STDOUT" /></root>
  <logger name="ai.chronon" level="INFO" />
</configuration>
"""

# Matches the same pattern NodeSubmitter.interpolateEnvVars uses on the platform
# side, so secret placeholders in teams.py spark configs resolve identically.
_ENV_VAR_PLACEHOLDER = re.compile(r"\{([A-Z_][A-Z0-9_]*)\}")

# Suffixes that CompareBaseJob appends to each compared column. Listed
# longest-first so prefix matching against a column name like
# "price_left_minus_right_approx_percentile" finds the right boundary.
_METRIC_SUFFIXES = [
    "_left_minus_right_approx_percentile",
    "_left_approx_percentile",
    "_right_approx_percentile",
    "_smape_average",
    "_both_null_sum",
    "_left_null_sum",
    "_right_null_sum",
    "_mismatch_sum",
    "_total_count",
]

# Matches the Databricks workspace hostname inside any URL value.
_DBX_HOSTNAME_RE = re.compile(r"https?://([A-Za-z0-9.-]+\.databricks\.com)")


def _interpolate_env_vars(conf):
    """Replace {VAR_NAME} placeholders in config values with environment variable values.

    Matches the behavior of NodeSubmitter.interpolateEnvVars in the orchestration layer.
    Unresolvable placeholders are left as-is.
    """
    result = {}
    for k, v in conf.items():
        result[k] = _ENV_VAR_PLACEHOLDER.sub(
            lambda m: os.environ.get(m.group(1), m.group(0)), v
        )
    return result


def _load_team_config(team, chronon_root):
    """Pull `(env_common, conf_common)` out of the merged ExecutionInfo for `team`.

    Reads directly off the merged MetaData (default-team ∪ named-team) rather
    than the flattened `collect_values` namespace, so we don't have to guess
    which prefixed keys are spark configs vs mode-scoped overrides. Mode-scoped
    overrides are intentionally dropped: this is a one-shot ad-hoc job, not a
    multi-mode pipeline run.

    Both dicts are returned RAW — interpolation is deferred to the submit
    function so that values resolved later (AWS_ACCOUNT_ID from STS, etc.) can
    still feed `{VAR_NAME}` placeholders.
    """
    team_dict = _load_teams_silent(chronon_root)
    metadata = _merged_execution_info_from_team(team_dict, team)
    LOG.info(f"Loaded config for team '{team}' from {chronon_root}/teams.py")

    exec_info = metadata.executionInfo
    env_common = {}
    conf_common = {}
    if exec_info is not None:
        if exec_info.env is not None and exec_info.env.common:
            env_common = dict(exec_info.env.common)
        if exec_info.conf is not None and exec_info.conf.common:
            conf_common = dict(exec_info.conf.common)

    return env_common, conf_common


def _create_temp_conf(spark_configs):
    """Create a temporary compiled conf JSON file as a MetaData object with spark configs
    embedded in executionInfo, matching the format that JobSubmitter.getModeConfigProperties expects."""
    conf = {
        "executionInfo": {
            "conf": {
                "common": spark_configs,
                "modeConfigs": {},
            }
        }
    }
    fd, path = tempfile.mkstemp(suffix=".json", prefix="compare_tables_conf_")
    with os.fdopen(fd, "w") as f:
        json.dump(conf, f)
    return path


def _split_metric_column(col_name):
    """Return (base_column, suffix) for a metric column, or None if it isn't one.

    Suffixes are matched longest-first so `foo_left_minus_right_approx_percentile`
    correctly resolves to base="foo".
    """
    for suffix in _METRIC_SUFFIXES:
        if col_name.endswith(suffix):
            return col_name[: -len(suffix)], suffix[1:]
    return None


def _aggregate_metrics(columns, rows):
    """Collapse per-(JoinPath, ts) metric rows into a single per-column summary.

    Sums count-like metrics across rows; averages smape across non-null values.
    """
    by_col = {}
    for col_name in columns:
        split = _split_metric_column(col_name)
        if split is None:
            continue
        base, suffix = split
        by_col.setdefault(base, {})[suffix] = 0
        by_col[base].setdefault(f"_{suffix}_count", 0)

    for row in rows:
        for i, col_name in enumerate(columns):
            split = _split_metric_column(col_name)
            if split is None:
                continue
            base, suffix = split
            val = row[i]
            if val is None:
                continue
            if suffix == "smape_average":
                by_col[base][suffix] += float(val)
                by_col[base][f"_{suffix}_count"] += 1
            elif suffix.endswith("_sum") or suffix == "total_count":
                by_col[base][suffix] += int(val)

    # Finalize smape averages
    for _base, metrics in by_col.items():
        cnt = metrics.pop("_smape_average_count", 0)
        if cnt and "smape_average" in metrics:
            metrics["smape_average"] = metrics["smape_average"] / cnt
        else:
            metrics.pop("smape_average", None)
        # Drop the bookkeeping keys we used during accumulation
        for k in list(metrics):
            if k.startswith("_") and k.endswith("_count"):
                metrics.pop(k)

    return by_col


def _extract_databricks_hostname(spark_configs):
    """Find a Databricks workspace hostname embedded in spark catalog configs."""
    for k, v in spark_configs.items():
        if not isinstance(v, str):
            continue
        # The catalog/auth URIs are the canonical place a workspace URL appears.
        if k.startswith("spark.sql.catalog.") and (
            k.endswith(".uri")
            or k.endswith(".auth.oauth.uri")
            or k.endswith(".oauth2-server-uri")
        ):
            m = _DBX_HOSTNAME_RE.match(v)
            if m:
                return m.group(1)
    return None


def _fetch_databricks_oauth_token(hostname, client_id, client_secret):
    """Get an M2M access token via the workspace's OIDC client_credentials endpoint."""
    token_url = f"https://{hostname}/oidc/v1/token"
    body = urllib.parse.urlencode(
        {"grant_type": "client_credentials", "scope": "all-apis"}
    ).encode("utf-8")
    basic_auth = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    req = urllib.request.Request(token_url, data=body, method="POST")
    req.add_header("Authorization", f"Basic {basic_auth}")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    with urllib.request.urlopen(req, timeout=30) as resp:  # noqa: S310 (constructed URL)
        payload = json.loads(resp.read())
    return payload["access_token"]


# Strict allow-list for the table identifiers we plug into a SELECT. The CLI is
# normally run interactively by an operator who supplies their own table names,
# but f-string interpolation into SQL is still a foot-gun — validate before use
# and backtick-quote each part so unusual but legal identifiers also work.
_SQL_IDENT_PART_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_-]*$")


def _quote_table(name):
    """Validate `<catalog>.<schema>.<table>` and return its backtick-quoted form.

    Raises `click.ClickException` on anything that doesn't look like an identifier
    so we never f-string raw user input into SQL.
    """
    parts = name.split(".")
    if not parts or not all(_SQL_IDENT_PART_RE.match(p) for p in parts):
        raise click.ClickException(
            f"Invalid SQL identifier '{name}'. Table names must consist of "
            "dotted identifiers matching [A-Za-z_][A-Za-z0-9_-]*."
        )
    return ".".join(f"`{p}`" for p in parts)


def _candidate_read_paths(metrics_table):
    """Yield the names to try when reading the metrics table over the SQL warehouse.

    Databricks-specific quirk: customers write Iceberg via a `*_iceberg` REST catalog
    binding (e.g. `workspace_iceberg`) but the SQL warehouse exposes the same physical
    Unity Catalog without that suffix (`workspace`). We try the user-provided name
    first; if that resolves to TABLE_OR_VIEW_NOT_FOUND we fall back to the de-suffixed
    catalog so users don't have to know about the binding mismatch.
    """
    yield metrics_table
    parts = metrics_table.split(".")
    if parts and parts[0].endswith("_iceberg"):
        parts[0] = parts[0][: -len("_iceberg")]
        yield ".".join(parts)


def _query_metrics_table(hostname, warehouse_id, access_token, metrics_table):
    """Run `SELECT * FROM <metrics_table>` via the SQL warehouse and return (cols, rows).

    Tries the user-provided table name first, then a `_iceberg`-suffix-stripped
    variant if Databricks reports TABLE_OR_VIEW_NOT_FOUND. Each candidate is
    validated and backtick-quoted before it lands in SQL.
    """
    try:
        from databricks import sql as dbsql
    except ImportError as e:
        raise RuntimeError(
            "databricks-sql-connector is not installed. "
            "Install it (`pip install databricks-sql-connector`) or rebuild the wheel "
            "after running `requirements upgrade` to enable metrics summary."
        ) from e

    http_path = f"/sql/1.0/warehouses/{warehouse_id}"
    last_exc = None
    with dbsql.connect(
        server_hostname=hostname,
        http_path=http_path,
        access_token=access_token,
    ) as conn:
        for candidate in _candidate_read_paths(metrics_table):
            quoted = _quote_table(candidate)
            try:
                with conn.cursor() as cursor:
                    cursor.execute(f"SELECT * FROM {quoted} LIMIT 10000")
                    cols = [d[0] for d in cursor.description]
                    rows = cursor.fetchall()
                return cols, rows
            except Exception as e:  # noqa: BLE001 — try the fallback before giving up
                if "TABLE_OR_VIEW_NOT_FOUND" not in str(e):
                    raise
                last_exc = e
    raise last_exc


def _render_summary_table(summary, metrics_table):
    """Pretty-print the per-column metric summary using rich."""
    from rich.table import Table

    if not summary:
        console.print(
            f"[yellow]Metrics table {metrics_table} has no comparable columns.[/yellow]"
        )
        return

    rows = []
    for base, metrics in summary.items():
        total = metrics.get("total_count", 0)
        mismatch = metrics.get("mismatch_sum", 0)
        match_pct = (1.0 - mismatch / total) * 100 if total else None
        rows.append({
            "column": base,
            "match_pct": match_pct,
            "mismatch": mismatch,
            "total": total,
            "left_null": metrics.get("left_null_sum", 0),
            "right_null": metrics.get("right_null_sum", 0),
            "smape": metrics.get("smape_average"),
        })
    # Surface the most-broken columns first.
    rows.sort(key=lambda r: (r["mismatch"], r["left_null"] + r["right_null"]), reverse=True)

    table = Table(title=f"Comparison summary: {metrics_table}", expand=False)
    table.add_column("Column", style="cyan", no_wrap=True)
    table.add_column("Match %", justify="right", no_wrap=True)
    table.add_column("Diffs", justify="right", no_wrap=True)
    table.add_column("Total", justify="right", no_wrap=True)
    table.add_column("L-null", justify="right", no_wrap=True)
    table.add_column("R-null", justify="right", no_wrap=True)
    table.add_column("SMAPE", justify="right", no_wrap=True)

    for r in rows:
        if r["match_pct"] is None:
            pct_cell = "—"
        elif r["match_pct"] >= 100.0:
            pct_cell = f"[green]{r['match_pct']:.2f}[/green]"
        elif r["match_pct"] >= 99.0:
            pct_cell = f"[yellow]{r['match_pct']:.2f}[/yellow]"
        else:
            pct_cell = f"[red]{r['match_pct']:.2f}[/red]"

        smape_cell = f"{r['smape']:.4f}" if r["smape"] is not None else "—"
        table.add_row(
            r["column"],
            pct_cell,
            f"{r['mismatch']:,}",
            f"{r['total']:,}",
            f"{r['left_null']:,}",
            f"{r['right_null']:,}",
            smape_cell,
        )

    console.print(table)


def _print_result_locations(metrics_table, side_by_side_table):
    """Always-shown footer: where the comparison output lives for ad-hoc analysis."""
    console.print()
    console.print("[bold]Output tables[/bold]")
    console.print(f"  Side-by-side : {side_by_side_table}")
    console.print(f"  Metrics      : {metrics_table}")


def _render_results(metrics_table, side_by_side_table, spark_configs, warehouse_id):
    """Render the metrics summary if we can reach the SQL warehouse, else fall back to table names.

    Failures here are non-fatal: the job already succeeded, the tables exist, and
    the user can always query them directly. We surface a one-line dim hint so
    they know why the summary is missing.
    """
    hostname = _extract_databricks_hostname(spark_configs)
    client_id = os.environ.get("DATABRICKS_CLIENT_ID")
    client_secret = os.environ.get("DATABRICKS_CLIENT_SECRET")

    if not (hostname and warehouse_id and client_id and client_secret):
        _print_result_locations(metrics_table, side_by_side_table)
        missing = []
        if not hostname:
            missing.append("Databricks hostname (no catalog URI in spark configs)")
        if not warehouse_id:
            missing.append("DATABRICKS_WAREHOUSE_ID")
        if not (client_id and client_secret):
            missing.append("DATABRICKS_CLIENT_ID/SECRET")
        console.print(f"[dim]Summary skipped (need: {', '.join(missing)}).[/dim]")
        return

    try:
        with console.status("[cyan]Fetching metrics summary…[/cyan]"):
            access_token = _fetch_databricks_oauth_token(hostname, client_id, client_secret)
            cols, rows = _query_metrics_table(hostname, warehouse_id, access_token, metrics_table)
        summary = _aggregate_metrics(cols, rows)
        _render_summary_table(summary, metrics_table)
        _print_result_locations(metrics_table, side_by_side_table)
    except Exception as e:  # noqa: BLE001 — we never want to fail the CLI here
        _print_result_locations(metrics_table, side_by_side_table)
        console.print(f"[dim]Summary skipped: {e}[/dim]")


def _resolve_aws_creds():
    """Best-effort AWS credential resolution → env-var dict for downstream use.

    Customers run this CLI in several environments:
      - Static keys in `~/.aws/credentials`           → both `aws` CLI and boto3 work.
      - SSO via `aws sso login`                       → both work.
      - SSO via wrapper (`aws login`, `login_session` profile entry) → only the
        `aws` CLI knows how to turn this into usable creds; boto3 sees an empty
        profile and falls through to broken refresh paths.

    We try `aws configure export-credentials --format env` FIRST because that's
    the AWS-canonical way to materialize creds and works for all three setups.
    We fall back to boto3 only if the CLI isn't installed or returns nothing —
    important to keep boto3 out of the picture when we can, since once boto3
    caches a partial credential chain it spams `Refreshing temporary credentials
    failed` warnings on every later call.
    """
    try:
        result = subprocess.run(
            ["aws", "configure", "export-credentials", "--format", "env"],
            capture_output=True, text=True, check=False, timeout=30,
        )
        if result.returncode == 0:
            out = {}
            for line in result.stdout.splitlines():
                line = line.strip()
                if not line.startswith("export "):
                    continue
                key, _, value = line[len("export "):].partition("=")
                key = key.strip()
                # Skip AWS_CREDENTIAL_EXPIRATION and similar — they confuse boto3/Java SDK.
                if key in {"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"}:
                    out[key] = value.strip().strip('"').strip("'")
            if out.get("AWS_ACCESS_KEY_ID"):
                return out
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass

    try:
        creds = boto3.Session().get_credentials()
        if creds is None:
            return {}
        frozen = creds.get_frozen_credentials()
        if not frozen.access_key:
            return {}
        out = {
            "AWS_ACCESS_KEY_ID": frozen.access_key,
            "AWS_SECRET_ACCESS_KEY": frozen.secret_key,
        }
        if frozen.token:
            out["AWS_SESSION_TOKEN"] = frozen.token
        return out
    except Exception:  # noqa: BLE001
        return {}


# Match `s3://zipline-artifacts-<customer_name>(/...)?` — `customer_name` is the
# TF deployment identifier and drives the conventional names for the EMR role,
# logs bucket, warehouse bucket, etc.
_ARTIFACT_PREFIX_RE = re.compile(r"^s3://zipline-artifacts-([A-Za-z0-9_-]+)(?:/.*)?$")


def _spark_configs_use_databricks(spark_configs):
    """True iff any spark config value references a `{DATABRICKS_*}` placeholder."""
    return any(
        isinstance(v, str) and _DATABRICKS_PLACEHOLDER_RE.search(v)
        for v in spark_configs.values()
    )


def _parse_left_table(left_table):
    """Split a fully-qualified table name into `(catalog, schema, table)`.

    Returns `(None, None, None)` if the name doesn't look like
    `catalog.schema.table` — callers turn that into a clear error pointing the
    user at `--output-catalog` / `--output-schema`.
    """
    if not left_table:
        return None, None, None
    parts = left_table.split(".")
    if len(parts) != 3 or not all(parts):
        return None, None, None
    return parts[0], parts[1], parts[2]


def _default_output_tables(left_table, output_catalog, output_schema, run_id):
    """Resolve the two output tables given (optional) catalog/schema overrides.

    Each unset piece falls back to the corresponding component of `--left-table`.
    Table names are auto-generated with a per-run timestamp so successive
    invocations never clobber each other.

    Raises `click.ClickException` if we can't resolve a catalog or schema —
    the message tells the user which flag to set.
    """
    left_catalog, left_schema, _ = _parse_left_table(left_table)
    catalog = output_catalog or left_catalog
    schema = output_schema or left_schema
    if not catalog:
        raise click.ClickException(
            f"Could not derive output catalog from --left-table='{left_table}'. "
            "Pass --output-catalog=<catalog>."
        )
    if not schema:
        raise click.ClickException(
            f"Could not derive output schema from --left-table='{left_table}'. "
            "Pass --output-schema=<schema>."
        )
    return (
        f"{catalog}.{schema}.compare_sbs_{run_id}",
        f"{catalog}.{schema}.compare_metrics_{run_id}",
    )


def _parse_customer_name(artifact_prefix):
    """Extract `customer_name` from `s3://zipline-artifacts-<customer_name>`.

    Returns None if `artifact_prefix` is missing or doesn't match. This is the
    same identifier the terraform modules use to name `zipline_*` resources;
    we lean on the convention so a stock TF deployment doesn't need any extra
    teams.py wiring.
    """
    if not artifact_prefix:
        return None
    m = _ARTIFACT_PREFIX_RE.match(artifact_prefix)
    return m.group(1) if m else None


def _derive_emr_role(account_id, customer_name):
    """Build the conventional EMR Serverless execution role ARN."""
    if not account_id or not customer_name:
        return None
    return f"arn:aws:iam::{account_id}:role/zipline_{customer_name}_emr_serverless_role"


def _derive_emr_log_uri(customer_name):
    """Build the conventional EMR Serverless log URI (TF default)."""
    if not customer_name:
        return None
    return f"s3://zipline-logs-{customer_name}/emr/"


def _autodetect_warehouse(spark_configs):
    """Pick a Databricks SQL warehouse for the post-job metrics summary.

    Returns the warehouse id when unambiguous, None when we can't safely auto-pick.
    Specifically:
      - Need Databricks hostname + OAuth creds in env to talk to the workspace.
      - Exactly 1 warehouse → use it.
      - 0 warehouses → return None (the metrics summary will show a hint).
      - >1 warehouses → raise ClickException pointing the user at --warehouse-id
        / DATABRICKS_WAREHOUSE_ID with the IDs printed inline.
    """
    hostname = _extract_databricks_hostname(spark_configs)
    client_id = os.environ.get("DATABRICKS_CLIENT_ID")
    client_secret = os.environ.get("DATABRICKS_CLIENT_SECRET")
    if not (hostname and client_id and client_secret):
        return None
    try:
        token = _fetch_databricks_oauth_token(hostname, client_id, client_secret)
        warehouses = _list_databricks_warehouses(hostname, token)
    except Exception:  # noqa: BLE001 — non-fatal; metrics rendering will skip with a hint
        return None
    if len(warehouses) == 1:
        return warehouses[0][0]
    if len(warehouses) == 0:
        return None
    listing = ", ".join(f"{wid} ({name or 'unnamed'})" for wid, name, _ in warehouses)
    raise click.ClickException(
        f"Found {len(warehouses)} SQL warehouses in the Databricks workspace: {listing}. "
        f"Pass --warehouse-id=<id> or pin DATABRICKS_WAREHOUSE_ID in teams.py."
    )


def _list_databricks_warehouses(hostname, access_token):
    """Return [(id, name, state), ...] for the workspace's SQL warehouses, or []."""
    req = urllib.request.Request(
        f"https://{hostname}/api/2.0/sql/warehouses", method="GET"
    )
    req.add_header("Authorization", f"Bearer {access_token}")
    with urllib.request.urlopen(req, timeout=30) as resp:  # noqa: S310
        body = json.loads(resp.read())
    return [(w["id"], w.get("name", ""), w.get("state", "")) for w in body.get("warehouses", [])]


def _resolve_aws_account_id():
    """Return the 12-digit AWS account ID for the active session, or None."""
    try:
        region = os.environ.get("AWS_REGION") or boto3.Session().region_name or "us-west-2"
        return boto3.client("sts", region_name=region).get_caller_identity()["Account"]
    except Exception:  # noqa: BLE001 — non-fatal; placeholders just won't resolve
        return None


def _write_logback_config():
    """Drop the quiet logback config to disk and return its path."""
    fd, path = tempfile.mkstemp(suffix=".xml", prefix="compare_tables_logback_")
    with os.fdopen(fd, "w") as f:
        f.write(_LOGBACK_QUIET_XML)
    return path


def _run_emr_submit(java_args, subprocess_env, timeout=300):
    """Run the EMR submitter; return stdout. Raises on non-zero exit with full output.

    Captures everything; we only echo back to the user on failure (so happy-path
    is quiet) and the parser still sees the job-id line it needs.

    The 300s timeout caps a hung Java submitter — the EMR Serverless
    StartJobRun call should return in ~1s, so anything past that is almost
    certainly a stuck network / auth / dependency-resolution problem.
    """
    try:
        proc = subprocess.run(
            java_args,
            capture_output=True,
            env=subprocess_env,
            check=False,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as e:
        raise click.ClickException(
            f"EMR submitter timed out after {timeout}s. "
            "Check your AWS session (try `aws sts get-caller-identity`) and network reachability "
            "of emr-serverless.<region>.amazonaws.com."
        ) from e
    output = proc.stdout.decode("utf-8", errors="replace")
    stderr = proc.stderr.decode("utf-8", errors="replace")
    if proc.returncode != 0:
        # On failure, surface everything we captured so the user can diagnose.
        console.print("[red]EMR submitter failed.[/red]")
        if output.strip():
            console.print(output)
        if stderr.strip():
            console.print(stderr)
        raise RuntimeError(f"EMR submitter exited with code {proc.returncode}")
    return output


def _list_arg(flag, comma_separated):
    """Render a Scallop List[String] flag as separate argv elements.

    Why: Scallop's default `opt[List[String]]` only accepts whitespace-separated
    repeated values (`--keys a b c`), not `--keys=a,b,c`. We let users pass comma
    separated values for ergonomics, then expand them into atomic argv elements
    here. Returning a list (not a joined string) keeps the caller from needing
    to `.split()` later — important so that other args containing spaces (e.g.
    `--mapping='{"a": "b"}'`, `--timestamp-millis-left='ts * 1000'`) survive
    intact when we hand the argv to `subprocess.run`.

    Raises UsageError when the comma-separated value resolves to no actual
    items (e.g. `--keys=`, `--keys=,`, `--keys=' , '`) so we fail locally
    with a clear message instead of submitting a malformed argv to the driver.
    """
    parts = [v.strip() for v in comma_separated.split(",") if v.strip()]
    if not parts:
        raise click.UsageError(
            f"{flag} requires at least one non-empty value (got {comma_separated!r})."
        )
    return [flag] + parts


def _build_spark_args(
    left_table,
    right_table,
    keys,
    side_by_side_table,
    metrics_table,
    mapping=None,
    timestamp_millis_left=None,
    timestamp_millis_right=None,
    left_columns=None,
    migration_check=False,
):
    """Build the Spark Driver argv list for the compare-tables subcommand.

    Returns a flat list of strings — never space-joined — so values with embedded
    spaces (mapping JSON, timestamp SQL expressions) stay as single argv elements.
    """
    args = [
        COMPARE_TABLES_SUBCOMMAND,
        f"--left-table={left_table}",
        f"--right-table={right_table}",
        *_list_arg("--keys", keys),
        f"--side-by-side-table={side_by_side_table}",
        f"--metrics-table={metrics_table}",
    ]
    if mapping:
        args.append(f"--mapping={mapping}")
    if timestamp_millis_left:
        args.append(f"--timestamp-millis-left={timestamp_millis_left}")
    if timestamp_millis_right:
        args.append(f"--timestamp-millis-right={timestamp_millis_right}")
    if left_columns:
        args.extend(_list_arg("--left-columns", left_columns))
    if migration_check:
        args.append("--migration-check")
    return args


def submit_compare_tables(
    left_table,
    right_table,
    keys,
    side_by_side_table,
    metrics_table,
    version,
    env_vars,
    spark_configs,
    team_name,
    mapping=None,
    timestamp_millis_left=None,
    timestamp_millis_right=None,
    left_columns=None,
    migration_check=False,
    warehouse_id=None,
):
    """Submit a compare-tables spark job via EMR Serverless and render results on success.

    Resolution order:
      1. AWS creds (env > aws CLI > boto3 chain) — gate every later AWS call.
      2. AWS_ACCOUNT_ID via STS, AWS_REGION via boto3/default.
      3. Derive infra defaults (EMR role, EMR log URI) from `customer_name`
         (parsed from `ARTIFACT_PREFIX`). teams.py values, if any, win.
      4. Resolve Databricks SQL warehouse (auto-list if not supplied).
      5. Interpolate raw env_vars + spark_configs against the now-populated env,
         then export the resolved env vars so the Java subprocess inherits them.
    Each step that can't complete raises a `click.ClickException` with a one-line
    actionable message.
    """
    # 0a. Cloud-provider guard. Only the AWS submit path is implemented today;
    # surface a clean message instead of letting a non-AWS team fail somewhere
    # deep in `AwsRunner.download_zipline_aws_jar`.
    cloud = (env_vars.get("CLOUD_PROVIDER") or os.environ.get("CLOUD_PROVIDER") or "").lower()
    if cloud not in _SUPPORTED_CLOUD_PROVIDERS:
        provider_msg = f"'{cloud}'" if cloud else "unset"
        raise click.ClickException(
            f"compare-tables currently supports AWS only "
            f"(team CLOUD_PROVIDER is {provider_msg}). GCP/Azure support is on the roadmap."
        )

    # 0b. Databricks creds — only required when spark configs reference a
    # `{DATABRICKS_*}` placeholder. AWS+Glue customers without Databricks at
    # all still get the submit path; the metrics-summary fetch just no-ops.
    if _spark_configs_use_databricks(spark_configs):
        if not (os.environ.get("DATABRICKS_CLIENT_ID") and os.environ.get("DATABRICKS_CLIENT_SECRET")):
            raise click.ClickException(
                "teams.py references Databricks credentials, but DATABRICKS_CLIENT_ID "
                "and DATABRICKS_CLIENT_SECRET aren't set. Get a service-principal "
                "credential pair from your workspace admin and export both."
            )
        # Mirror platform/docker/orchestration/start.sh: derive DATABRICKS_CREDENTIAL
        # so the `{DATABRICKS_CREDENTIAL}` placeholder resolves identically in
        # both the orchestration main flow and this CLI. Direct-assign (not
        # setdefault) so a stale DATABRICKS_CREDENTIAL in the shell never wins
        # over the credential pair we just validated for this run.
        os.environ["DATABRICKS_CREDENTIAL"] = (
            f"{os.environ['DATABRICKS_CLIENT_ID']}:{os.environ['DATABRICKS_CLIENT_SECRET']}"
        )

    # 1. AWS_REGION first — `aws configure export-credentials` (and a few boto3
    # calls deeper in the stack) need a region resolved to even ask for creds.
    os.environ.setdefault("AWS_REGION", env_vars.get("AWS_REGION") or boto3.Session().region_name or "us-west-2")

    # 2. AWS creds.
    for k, v in _resolve_aws_creds().items():
        os.environ[k] = v
    if not os.environ.get("AWS_ACCESS_KEY_ID"):
        raise click.ClickException(
            "AWS credentials not found. Run `aws login` (or set AWS_ACCESS_KEY_ID/SECRET) and retry."
        )
    # 3. AWS_ACCOUNT_ID via STS — used to build the EMR role ARN.
    account_id = _resolve_aws_account_id()
    if not account_id:
        raise click.ClickException(
            "Could not resolve AWS account ID (sts:GetCallerIdentity failed). "
            "Verify your AWS session is active (try `aws sts get-caller-identity`)."
        )
    os.environ["AWS_ACCOUNT_ID"] = account_id

    # 4. EMR role + log URI: teams.py override > convention derived from ARTIFACT_PREFIX.
    artifact_prefix = env_vars.get("ARTIFACT_PREFIX") or os.environ.get("ARTIFACT_PREFIX")
    customer_name = _parse_customer_name(artifact_prefix)
    if not env_vars.get("EMR_EXECUTION_ROLE_ARN"):
        derived = _derive_emr_role(account_id, customer_name)
        if not derived:
            raise click.ClickException(
                "Could not derive EMR_EXECUTION_ROLE_ARN. Either set it in teams.py, or "
                "make sure ARTIFACT_PREFIX is `s3://zipline-artifacts-<customer_name>`."
            )
        env_vars["EMR_EXECUTION_ROLE_ARN"] = derived
    if not env_vars.get("EMR_LOG_URI"):
        derived = _derive_emr_log_uri(customer_name)
        if not derived:
            raise click.ClickException(
                "Could not derive EMR_LOG_URI. Either set it in teams.py, or "
                "make sure ARTIFACT_PREFIX is `s3://zipline-artifacts-<customer_name>`."
            )
        env_vars["EMR_LOG_URI"] = derived

    # 5. Databricks warehouse: explicit override > teams.py > auto-list (single match).
    if not warehouse_id:
        warehouse_id = env_vars.get("DATABRICKS_WAREHOUSE_ID")
    if not warehouse_id:
        warehouse_id = _autodetect_warehouse(spark_configs)
    if warehouse_id:
        env_vars["DATABRICKS_WAREHOUSE_ID"] = warehouse_id

    # 6. Interpolation + export.
    env_vars = _interpolate_env_vars(env_vars)
    spark_configs = _interpolate_env_vars(spark_configs)
    # Values we derived this run (EMR role, log URI, warehouse) must override
    # any stale shell env var — otherwise the banner we printed shows the
    # derived value but the Java subprocess inherits the wrong one. Pass-through
    # team values keep `setdefault` so an explicit shell override still wins.
    _RESOLVED_KEYS = {"EMR_EXECUTION_ROLE_ARN", "EMR_LOG_URI", "DATABRICKS_WAREHOUSE_ID"}
    for k, v in env_vars.items():
        if k in _RESOLVED_KEYS:
            os.environ[k] = v
        else:
            os.environ.setdefault(k, v)

    artifacts_bucket_prefix = os.environ.get(
        "ARTIFACT_PREFIX", f"s3://zipline-artifacts-{get_customer_id()}"
    )
    warehouse_bucket_prefix = os.environ.get(
        "WAREHOUSE_PREFIX", f"s3://zipline-warehouse-{get_customer_id()}"
    )

    os.makedirs(ZIPLINE_DIRECTORY, exist_ok=True)

    # Pretty header
    console.print()
    console.print(f"[bold]Compare-tables[/bold] (team=[cyan]{team_name}[/cyan], version=[cyan]{version}[/cyan])")
    console.print(f"  Left  : {left_table}")
    console.print(f"  Right : {right_table}")
    console.print(f"  Keys  : {keys}")
    # Surface the resolved infra config so the user can spot a wrong derivation
    # before a 5-minute EMR submit confirms it for them.
    console.print(f"  Role  : {env_vars['EMR_EXECUTION_ROLE_ARN']}")
    console.print(f"  Logs  : {env_vars['EMR_LOG_URI']}")
    if warehouse_id:
        console.print(f"  SQL WH: {warehouse_id}")
    console.print()

    with console.status("[cyan]Preparing submission...[/cyan]") as status:
        status.update("Downloading EMR jar…")
        # AwsRunner.download_zipline_aws_jar prints hash-comparison diagnostics to stdout
        # via plain print(); capture them so the customer-facing output stays clean.
        with contextlib.redirect_stdout(io.StringIO()):
            jar_path = AwsRunner.download_zipline_aws_jar(
                ZIPLINE_DIRECTORY, artifacts_bucket_prefix, version, ZIPLINE_AWS_JAR_DEFAULT
            )

        spark_args = _build_spark_args(
            left_table=left_table,
            right_table=right_table,
            keys=keys,
            side_by_side_table=side_by_side_table,
            metrics_table=metrics_table,
            mapping=mapping,
            timestamp_millis_left=timestamp_millis_left,
            timestamp_millis_right=timestamp_millis_right,
            left_columns=left_columns,
            migration_check=migration_check,
        )

        jar_uri = f"{artifacts_bucket_prefix}/release/{version}/jars/{ZIPLINE_AWS_JAR_DEFAULT}"

        # Emit spark configs through the standard JobSubmitter conf channel so they reach
        # the executor. The expected input is a compiled-conf MetaData JSON; we mint a
        # minimal one in-memory with our configs under `executionInfo.conf.common`.
        #
        # `--original-mode=metastore` is intentional but somewhat repurposed: it's the
        # only branch in JobSubmitter.getMetadata that parses an arbitrary MetaData
        # object via parseConf without requiring a real conf type or matching the mode
        # against modeConfigs. If a dedicated "ad-hoc spark conf" path lands upstream,
        # we should switch to it.
        conf_args = []
        temp_conf_path = None
        if spark_configs:
            status.update("Uploading spark configs to S3…")
            temp_conf_path = _create_temp_conf(spark_configs)
            conf_filename = extract_filename_from_path(temp_conf_path)
            s3_conf_path = upload_to_blob_store(
                temp_conf_path,
                f"{warehouse_bucket_prefix}/metadata/{conf_filename}",
            )
            # local-conf-path is read by the local java EMR submitter; --files uploads
            # the same file to the cluster for the driver to re-read at runtime.
            conf_args = [
                f"--local-conf-path={temp_conf_path}",
                "--original-mode=metastore",
                f"--files={s3_conf_path}",
            ]

        # Everything stays as an argv LIST end-to-end so values containing spaces
        # (--mapping JSON, --timestamp-millis-* SQL exprs) survive subprocess
        # tokenization. Anything that does `.split()` on this loses spaces.
        emr_args = [
            *spark_args,
            f"--jar-uri={jar_uri}",
            "--job-type=spark",
            f"--main-class={SPARK_DRIVER_CLASS}",
            *conf_args,
        ]

        logback_path = _write_logback_config()
        java_cmd = [
            "java",
            f"-Dlogback.configurationFile={logback_path}",
            "-cp", jar_path,
            EMR_ENTRY,
            *emr_args,
        ]

        status.update("Submitting to EMR Serverless…")
        try:
            output = _run_emr_submit(java_cmd, os.environ.copy())
        finally:
            for p in (temp_conf_path, logback_path):
                if p and os.path.exists(p):
                    os.unlink(p)

    # Parse the job id (the EmrServerlessSubmitter prints this line on success).
    app_id, job_run_id = None, None
    for line in output.splitlines():
        if "Job submitted with ID:" in line:
            parts = line.split("Job submitted with ID:")[-1].strip().split("|")
            if len(parts) == 2:
                app_id, job_run_id = parts[0].strip(), parts[1].strip()
            break

    if not app_id or not job_run_id:
        console.print("[red]Could not parse job ID from submitter output:[/red]")
        console.print(output)
        raise RuntimeError("Failed to parse job ID from EMR Serverless submission output.")

    console.print(f"Submitted: app=[cyan]{app_id}[/cyan] job=[cyan]{job_run_id}[/cyan]")

    region = os.environ.get("AWS_REGION", "us-west-2")
    final_state = _poll_until_terminal(app_id, job_run_id, region)
    if final_state != "SUCCESS":
        return

    _render_results(metrics_table, side_by_side_table, spark_configs, warehouse_id)


def _poll_until_terminal(app_id, job_run_id, region, poll_interval=15, max_attempts=240):
    """Poll EMR Serverless for terminal state. Returns the final state string.

    Uses a single console.status spinner that updates in place — only the
    terminal transition gets a permanent line.
    """
    emr = boto3.client("emr-serverless", region_name=region)
    started = time.monotonic()
    last_state = None
    terminal = {"SUCCESS", "FAILED", "CANCELLED"}

    with console.status("[cyan]Waiting for EMR Serverless job…[/cyan]") as status:
        for _ in range(max_attempts):
            resp = emr.get_job_run(applicationId=app_id, jobRunId=job_run_id)
            state = resp["jobRun"]["state"]
            elapsed = int(time.monotonic() - started)
            status.update(f"[cyan]Job {job_run_id}: {state} ({elapsed}s elapsed)[/cyan]")
            if state in terminal:
                last_state = state
                break
            time.sleep(poll_interval)
        else:
            console.print(f"[red]Timed out after {max_attempts * poll_interval}s[/red]")
            raise RuntimeError(f"Timed out waiting for EMR Serverless job {job_run_id}")

    elapsed = int(time.monotonic() - started)
    if last_state == "SUCCESS":
        console.print(f"[green]✓[/green] Job succeeded ({elapsed}s)")
    else:
        details = resp["jobRun"].get("stateDetails", "no details")
        console.print(f"[red]✗ Job {last_state}[/red]: {details}")
    return last_state


@click.command(
    name="compare-tables",
    context_settings=dict(help_option_names=["-h", "--help"]),
)
@click.option(
    "--left-table",
    required=True,
    help="Fully qualified left table name (e.g. database.schema.table).",
)
@click.option(
    "--right-table",
    required=True,
    help="Fully qualified right table name (e.g. database.schema.table).",
)
@click.option(
    "--keys",
    required=True,
    help="Comma-separated list of key columns to join on (using left table column names).",
)
@click.option(
    "--output-catalog",
    required=False,
    default=None,
    help="Catalog to write the comparison output tables into. Defaults to the "
    "catalog of --left-table.",
)
@click.option(
    "--output-schema",
    required=False,
    default=None,
    help="Schema (database) to write the comparison output tables into. Defaults "
    "to the schema of --left-table.",
)
@click.option(
    "--yes", "-y",
    "yes",
    is_flag=True,
    default=False,
    help="Skip the interactive confirmation of resolved output table names. "
    "Useful for scripted / CI invocations.",
)
@click.option(
    "--team",
    required=True,
    help="Team name from teams.py (e.g. 'aws', 'aws_databricks'). "
    "Loads spark configs and env vars for the target environment.",
)
@click.option(
    "--mapping",
    required=False,
    default=None,
    help='JSON mapping of left_col_name -> SQL expression for renaming/transforming right table columns. '
    'E.g. \'{"left_col": "right_col", "amount": "CAST(value AS DOUBLE)"}\'',
)
@click.option(
    "--timestamp-millis-left",
    required=False,
    default=None,
    help="SQL expression on the left table that produces a timestamp in milliseconds (Long).",
)
@click.option(
    "--timestamp-millis-right",
    required=False,
    default=None,
    help="SQL expression on the right table that produces a timestamp in milliseconds (Long). "
    "Must be paired with --timestamp-millis-left.",
)
@click.option(
    "--left-columns",
    required=False,
    default=None,
    help="Comma-separated subset of left columns to compare. Defaults to all non-key columns.",
)
@click.option(
    "--migration-check",
    is_flag=True,
    default=False,
    help="Allow the left table to have extra columns not present in the right table.",
)
@click.option(
    "--version",
    required=False,
    default=None,
    help="Zipline version to use. Defaults to VERSION from teams.py or env var.",
)
@chronon_root_option(
    help="Path to the Chronon root (containing teams.py). Auto-discovered if omitted.",
)
@click.option(
    "--warehouse-id",
    default=None,
    envvar="DATABRICKS_WAREHOUSE_ID",
    help="Databricks SQL warehouse ID used to fetch the metrics summary after the job "
    "completes. Resolution order: --warehouse-id flag > DATABRICKS_WAREHOUSE_ID env var > "
    "DATABRICKS_WAREHOUSE_ID from teams.py env section. If unresolved, the CLI prints "
    "the output table names and skips the summary.",
)
def compare_tables(
    left_table,
    right_table,
    keys,
    output_catalog,
    output_schema,
    yes,
    team,
    mapping,
    timestamp_millis_left,
    timestamp_millis_right,
    left_columns,
    migration_check,
    version,
    chronon_root,
    warehouse_id,
):
    """Compare two tables by submitting a Spark job via EMR Serverless.

    Joins the two tables on the specified keys, computes comparison metrics,
    and writes both a side-by-side and a per-column metrics table. Output table
    names default to `<output_catalog>.<output_schema>.compare_{sbs,metrics}_<ts>`
    — catalog/schema fall back to those of --left-table when unset.

    Requires a --team flag to load spark configs and env vars from teams.py.
    Run from a directory containing teams.py or pass --chronon-root.

    \b
    Examples:
        zipline compare-tables \\
            --team=aws_databricks \\
            --left-table=workspace.poc.dim_listings \\
            --right-table=workspace.poc.dim_listings_v2 \\
            --keys=listing_id,date

        zipline compare-tables \\
            --team=aws_databricks \\
            --left-table=workspace.poc.dim_listings \\
            --right-table=workspace.poc.dim_listings_v2 \\
            --keys=listing_id \\
            --output-catalog=workspace_iceberg \\
            --output-schema=staging \\
            --mapping='{"old_col": "new_col"}' \\
            --migration-check
    """
    # Mute library loggers as the very first thing so even the loader's
    # "Loaded config for team..." line doesn't leak through.
    _quiet_noisy_loggers()

    if bool(timestamp_millis_left) != bool(timestamp_millis_right):
        raise click.UsageError(
            "--timestamp-millis-left and --timestamp-millis-right must be set together "
            "(or both omitted)."
        )

    if mapping:
        try:
            json.loads(mapping)
        except json.JSONDecodeError as e:
            raise click.UsageError(f"--mapping must be valid JSON: {e}") from e

    root = _resolve_chronon_root(chronon_root)
    env_vars, spark_configs = _load_team_config(team, root)
    version = version or env_vars.get("VERSION") or os.environ.get("VERSION")
    if not version:
        raise click.UsageError(
            "Version is required. Provide --version, set VERSION in teams.py, or set the VERSION env var."
        )
    # Click already pulled --warehouse-id from the shell env; layer in the
    # teams.py value as the next-lowest precedence so customers can pin a
    # default warehouse per-team without each user setting an env var.
    warehouse_id = warehouse_id or env_vars.get("DATABRICKS_WAREHOUSE_ID")

    # Resolve output table names early so we can confirm with the user before
    # the (multi-minute) EMR submit kicks off.
    run_id = time.strftime("%Y%m%d_%H%M%S")
    side_by_side_table, metrics_table = _default_output_tables(
        left_table, output_catalog, output_schema, run_id
    )
    console.print()
    console.print("[bold]Output tables[/bold] (will be created):")
    console.print(f"  Side-by-side : {side_by_side_table}")
    console.print(f"  Metrics      : {metrics_table}")
    if not yes and not click.confirm("Proceed with these output tables?", default=True):
        raise click.ClickException(
            "Aborted. Override defaults with --output-catalog=<catalog> and/or "
            "--output-schema=<schema>, or pass --yes to skip this prompt."
        )

    submit_compare_tables(
        left_table=left_table,
        right_table=right_table,
        keys=keys,
        side_by_side_table=side_by_side_table,
        metrics_table=metrics_table,
        version=version,
        env_vars=env_vars,
        spark_configs=spark_configs,
        team_name=team,
        mapping=mapping,
        timestamp_millis_left=timestamp_millis_left,
        timestamp_millis_right=timestamp_millis_right,
        left_columns=left_columns,
        migration_check=migration_check,
        warehouse_id=warehouse_id,
    )
