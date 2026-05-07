import functools
import json
import logging
import os
import sys
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional

import click
import requests

from ai.chronon.cli.formatter import (
    Format,
    format_print,
    jsonify_exceptions_if_json_format,
)
from ai.chronon.cli.git_utils import get_current_branch
from ai.chronon.cli.theme import (
    print_error,
    print_info,
    print_key_value,
    print_success,
    print_url,
    status_spinner,
)
from ai.chronon.click_helpers import handle_compile, handle_conf_not_found, handle_dry_run_compile
from ai.chronon.repo import hub_uploader, utils
from ai.chronon.repo.auth import get_user_email
from ai.chronon.repo.constants import VALID_CLOUDS, RunMode
from ai.chronon.repo.utils import print_possible_confs, upload_to_blob_store
from ai.chronon.repo.zipline_hub import ZiplineHub
from gen_thrift.api.ttypes import DataKind
from gen_thrift.planner.ttypes import Mode

logger = logging.getLogger(__name__)


def _validate_at_most_daily_schedule(schedule_expression: str) -> Optional[str]:
    """Validates that a schedule expression runs at most once per day.
    Returns None if valid, error message string if invalid."""
    import datetime

    from croniter import croniter

    if not schedule_expression or schedule_expression.strip().lower() in ("", "none", "null", "@daily", "@never"):
        return None

    schedule_expression = schedule_expression.strip()

    try:
        croniter(schedule_expression, datetime.datetime(2024, 1, 1, 0, 0))
    except (ValueError, TypeError) as e:
        return f"Invalid cron expression syntax: {e}"

    try:
        test_start = datetime.datetime(2024, 1, 1, 0, 0)
        for day_offset in range(7):
            day_start = test_start + datetime.timedelta(days=day_offset)
            day_end = day_start + datetime.timedelta(days=1)
            cron_start = day_start - datetime.timedelta(seconds=1)
            cron = croniter(schedule_expression, cron_start)
            executions_in_day = 0
            for _ in range(200):
                next_run = cron.get_next(datetime.datetime)
                if next_run >= day_end:
                    break
                executions_in_day += 1
                if executions_in_day > 1:
                    return (
                        f"Schedule runs {executions_in_day} times on "
                        f"{day_start.strftime('%A')} ({day_start.strftime('%Y-%m-%d')}). "
                        f"Only at-most-daily schedules are allowed."
                    )
    except Exception as e:
        return f"Error validating schedule frequency: {e}"

    return None


ALLOWED_DATE_FORMATS = ["%Y-%m-%d"]


def _resolve_data_type_kinds(obj):
    """Recursively replace numeric `kind` values in dataType objects with their string names."""
    if isinstance(obj, dict):
        if "kind" in obj and isinstance(obj["kind"], int):
            obj = {
                **obj,
                "kind": DataKind._VALUES_TO_NAMES.get(obj["kind"], obj["kind"]),
            }
        return {k: _resolve_data_type_kinds(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_resolve_data_type_kinds(item) for item in obj]
    return obj


DEFAULT_TEAM_METADATA_CONF = "compiled/teams_metadata/default/default_team_metadata"


@dataclass
class HubConfig:
    hub_url: str
    frontend_url: str
    sa_name: Optional[str] = None
    eval_url: Optional[str] = None
    fetcher_url: Optional[str] = None
    cloud_provider: Optional[str] = None
    artifact_prefix: Optional[str] = None
    customer_id: Optional[str] = None
    auth_scope: Optional[str] = None


SCHEDULE_NONE_STR = "None"

@dataclass
class ScheduleModes:
    offline_schedule: str
    online_schedule: str


@click.group(help="Manage Zipline Hub workflows, schedules, and evaluations.")
def hub():
    pass


def repo_option(func):
    return click.option(
        "-r",
        "--repo",
        help="Path to the Chronon repo root.",
        default=".",
        show_default=True,
    )(func)


def use_auth_option(func):
    return click.option(
        "--use-auth/--no-use-auth",
        help="Use authentication when connecting to Zipline Hub",
        default=True,
    )(func)


def hub_url_option(func):
    return click.option(
        "--hub-url",
        help="Zipline Hub address, e.g. http://localhost:3903",
        default=None,
    )(func)


def format_option(func):
    return click.option(
        "-f",
        "--format",
        help="Output format.",
        default=Format.TEXT,
        type=click.Choice(Format, case_sensitive=False),
        show_default=True,
    )(func)


def force_option(func):
    return click.option(
        "--force",
        help="Force compile even if there are version changes to existing confs",
        is_flag=True,
    )(func)


def cloud_provider_option(func):
    return click.option(
        "--cloud",
        help="Cloud provider for the hub and related services",
        type=click.Choice(VALID_CLOUDS, case_sensitive=False),
        required=False,
        default=None,
    )(func)


def customer_id_option(func):
    return click.option(
        "--customer-id",
        help="Customer ID for authentication. Required for Azure.",
        type=str,
        required=False,
        default=None,
    )(func)


def get_conf_type(conf):
    if "compiled/joins" in conf:
        return "joins"
    elif "compiled/staging_queries" in conf:
        return "stagingqueries"
    elif "compiled/group_by" in conf:
        return "groupbys"
    elif "compiled/models" in conf:
        return "models"
    elif "compiled/model_transforms" in conf:
        return "modeltransforms"
    else:
        raise ValueError(f"Unsupported conf type: {conf}")


#### Common click options
def common_options(func):
    func = repo_option(func)
    func = hub_url_option(func)
    func = use_auth_option(func)
    func = format_option(func)
    func = force_option(func)
    return func


def conf_argument(func):
    return click.argument("conf")(func)


def ds_option(func):
    return click.option(
        "--date",
        "--ds",
        "ds",
        help="End date for the backfill (format: YYYY-MM-DD).",
        type=click.DateTime(formats=ALLOWED_DATE_FORMATS),
    )(func)


def start_ds_option(func):
    return click.option(
        "--start-date",
        "--start-ds",
        "start_ds",
        type=click.DateTime(formats=ALLOWED_DATE_FORMATS),
        help="Start date override for a range backfill (format: YYYY-MM-DD). "
        "Supports staging query, group by, and join jobs. "
        "May leave holes in the output table due to the overridden date range.",
    )(func)


def confirm_end_ds_not_future(end_ds, assume_yes: bool = False):
    """Abort with a confirm prompt if end_ds is today or later.

    Data for today (and beyond) typically hasn't landed yet, so a backfill
    covering that range will silently produce partial or empty output.

    When assume_yes is True the prompt is skipped (non-interactive callers /
    CI pipelines that have explicitly opted in to the risk).
    """
    if end_ds is None or assume_yes:
        return
    end_date_value = end_ds.date() if hasattr(end_ds, "date") else end_ds
    today = date.today()
    if end_date_value >= today:
        click.confirm(
            click.style(
                f"End date {end_date_value} is today or in the future. Upstream data may not have "
                "landed yet, so the backfill could be incomplete. Proceed anyway?",
                fg="yellow",
            ),
            abort=True,
        )


def end_ds_option(func):
    """Adds --end-date / --end-ds and a bundled --yes/--assume-yes flag, and
    automatically prompts the user to confirm if end_ds is today or in the
    future. The prompt is skipped when --yes is passed.
    """
    @click.option(
        "--end-date",
        "--end-ds",
        "end_ds",
        help="End date for a range backfill (format: YYYY-MM-DD).",
        type=click.DateTime(formats=ALLOWED_DATE_FORMATS),
        default=str(date.today() - timedelta(days=2)),
        show_default=True,
    )
    @click.option(
        "-y",
        "--yes",
        "--assume-yes",
        "assume_yes",
        is_flag=True,
        default=False,
        help="Skip the end-date confirmation prompt. Use in non-interactive CI where "
        "stdin isn't a TTY and the prompt would otherwise abort the job.",
    )
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        confirm_end_ds_not_future(
            kwargs.get("end_ds"),
            assume_yes=kwargs.get("assume_yes", False),
        )
        return func(*args, **kwargs)

    return wrapper


def _get_zipline_hub(
    hub_url: Optional[str],
    hub_conf: HubConfig,
    use_auth: bool,
    format: Format = Format.TEXT,
):
    scope = ""
    if hub_conf.auth_scope is not None:
        scope = hub_conf.auth_scope
    elif hub_conf.cloud_provider == "azure" and hub_conf.customer_id is not None:
        scope = f"api://{hub_conf.customer_id}-zipline-auth"
    base_url = hub_url if hub_url is not None else hub_conf.hub_url
    return ZiplineHub(
        base_url=base_url,
        sa_name=hub_conf.sa_name,
        use_auth=use_auth,
        cloud_provider=hub_conf.cloud_provider,
        scope=scope,
        format=format,
        auth_url=hub_conf.frontend_url,
    )


def redeploy_streaming(repo, confs, hub_url=None, use_auth=True, format: Format = Format.TEXT):
    hub_confs = [get_hub_conf(conf, root_dir=repo) for conf in confs]
    hub_urls = {hc.hub_url for hc in hub_confs}
    if len(hub_urls) > 1:
        mismatches = "\n".join(
            f"  {conf}: {hc.hub_url}" for conf, hc in zip(confs, hub_confs, strict=True)
        )
        raise ValueError(
            f"All confs must target the same Hub, but found multiple hub_url values:\n{mismatches}"
        )
    hub_conf = hub_confs[0]
    zipline_hub = _get_zipline_hub(hub_url, hub_conf, use_auth, format)

    with status_spinner("Computing local conf hashes...", format=format):
        conf_name_to_hash_dict = hub_uploader.build_local_repo_hashmap(root_dir=repo)
    branch = get_current_branch()
    with status_spinner("Syncing confs with Hub...", format=format):
        hub_uploader.compute_and_upload_diffs(
            branch, zipline_hub=zipline_hub, local_repo_confs=conf_name_to_hash_dict, format=format
        )

    metadata_names = [utils.get_metadata_name_from_conf(repo, conf) for conf in confs]

    with status_spinner(f"Requesting redeploy for {len(metadata_names)} streaming job(s)...", format=format):
        response = zipline_hub.call_streaming_redeploy_api(metadata_names)

    failure_count = response.get("failureCount", 0)

    if format == Format.JSON:
        print(json.dumps(response, indent=4))
        sys.exit(1 if failure_count > 0 else 0)

    for result in response.get("results", []):
        name = result.get("metadataName", "unknown")
        if result.get("success"):
            print_success(f"Redeploy initiated for {name}", format=format)
        else:
            print_error(f"Failed: {name}: {result.get('message', '')}", format=format)

    print_key_value("Total", response.get("totalCount", 0), format=format)
    print_key_value("Succeeded", response.get("successCount", 0), format=format)
    print_key_value("Failed", failure_count, format=format)

    if failure_count > 0:
        sys.exit(1)


def submit_schedule_all(
    repo, cloud, customer_id, hub_url=None, use_auth=True, format: Format = Format.TEXT
):
    """Deploy schedules for all changed confs that have schedules defined."""
    zipline_hub = _get_zipline_hub(
        hub_url,
        get_hub_conf_from_metadata_conf(
            DEFAULT_TEAM_METADATA_CONF,
            root_dir=repo,
            cloud_provider=cloud,
            customer_id=customer_id,
        ),
        use_auth,
        format,
    )

    with status_spinner("Computing local conf hashes...", format=format):
        conf_name_to_obj_dict = hub_uploader.build_local_repo_hashmap(root_dir=repo)

    branch = get_current_branch()

    with status_spinner("Syncing confs with Hub...", format=format):
        diff_confs = hub_uploader.compute_and_upload_diffs(
            branch,
            zipline_hub=zipline_hub,
            local_repo_confs=conf_name_to_obj_dict,
            format=format,
        )

    # Collect confs with schedules
    confs_with_schedules = []
    skipped_confs = []

    for name, conf in diff_confs.items():
        try:
            schedule_modes = get_schedule_modes(conf.localPath)

            # Skip confs without any schedules
            if (
                SCHEDULE_NONE_STR == schedule_modes.offline_schedule
                and SCHEDULE_NONE_STR ==  schedule_modes.online_schedule
            ):
                skipped_confs.append(name)
                continue

            modes = {
                RunMode.BACKFILL.value.upper(): schedule_modes.offline_schedule,
                RunMode.DEPLOY.value.upper(): schedule_modes.online_schedule,
            }

            confs_with_schedules.append(
                {
                    "conf_name": name,
                    "conf_hash": conf.hash,
                    "branch": branch,
                    "modes": modes,
                }
            )
        except Exception as e:
            logger.warning(f"Failed to extract schedule for conf {name}: {e}")
            skipped_confs.append(name)

    if not confs_with_schedules:
        print_info(
            f"No changed confs with schedules found. "
            f"{len(skipped_confs)} conf(s) changed but have no schedules defined.",
            format=format,
        )
        return

    # Deploy schedules via batch API
    with status_spinner(
        f"Deploying schedules for {len(confs_with_schedules)} conf(s)...", format=format
    ):
        response_json = zipline_hub.call_schedule_all_api(confs_with_schedules)

    # Format output
    if format == Format.JSON:
        print(json.dumps(response_json, indent=4))
        sys.exit(0 if response_json.get("failureCount", 0) == 0 else 1)

    # Text format output
    total = response_json.get("totalCount", 0)
    succeeded = response_json.get("successCount", 0)
    failed = response_json.get("failureCount", 0)
    results = response_json.get("results", [])

    if failed > 0:
        print_error(
            f"Schedule deployment completed with errors: "
            f"{succeeded}/{total} succeeded, {failed}/{total} failed",
            format=format,
        )

        # Show failed confs
        for result in results:
            if not result.get("success", True):
                conf_name = result.get("confName", "unknown")
                error = result.get("error", "Unknown error")
                print_error(f"  ✗ {conf_name}: {error}", format=format)

        sys.exit(1)
    else:
        print_success(
            f"Successfully deployed schedules for {succeeded} conf(s)", format=format
        )

        # Show successful deployments
        for result in results:
            if result.get("success", False):
                conf_name = result.get("confName", "unknown")
                schedules = result.get("schedules", {})

                # Format schedule info
                schedule_info = []
                for mode_id, schedule_response in schedules.items():
                    mode_name = "BACKFILL" if mode_id == "0" else "DEPLOY"
                    interval = schedule_response.get("scheduleInterval", "N/A")
                    state = schedule_response.get("state", "UNKNOWN")
                    schedule_info.append(f"{mode_name}: {interval} ({state})")

                print_key_value(
                    f"  ✓ {conf_name}", ", ".join(schedule_info), format=format
                )

    if skipped_confs:
        print_info(
            f"\n{len(skipped_confs)} conf(s) skipped (no schedules defined): "
            f"{', '.join(skipped_confs[:5])}"
            f"{'...' if len(skipped_confs) > 5 else ''}",
            format=format,
        )


def submit_workflow(
    repo,
    conf,
    mode,
    start_ds,
    end_ds,
    hub_url=None,
    use_auth=True,
    format: Format = Format.TEXT,
):
    hub_conf = get_hub_conf(conf, root_dir=repo)
    zipline_hub = _get_zipline_hub(hub_url, hub_conf, use_auth, format)

    with status_spinner("Computing local conf hashes...", format=format):
        conf_name_to_hash_dict = hub_uploader.build_local_repo_hashmap(root_dir=repo)
    branch = get_current_branch()

    with status_spinner("Syncing confs with Hub...", format=format):
        hub_uploader.compute_and_upload_diffs(
            branch,
            zipline_hub=zipline_hub,
            local_repo_confs=conf_name_to_hash_dict,
            format=format,
        )

    # get conf name
    conf_name = utils.get_metadata_name_from_conf(repo, conf)

    with status_spinner(f"Submitting {mode} workflow...", format=format):
        response_json = zipline_hub.call_workflow_start_api(
            conf_name=conf_name,
            mode=mode,
            branch=branch,
            user=get_user_email(),
            start=start_ds,
            end=end_ds,
            conf_hash=conf_name_to_hash_dict[conf_name].hash,
            skip_long_running=False,
        )

    workflow_id = response_json.get("workflowId", "N/A")
    if format == Format.JSON:
        print(json.dumps(response_json, indent=4))
        sys.exit(0)
    print_success("Workflow submitted. 🚀", format=format)
    print_key_value("🆔 Workflow ID", workflow_id, format=format)
    print_key_value("📦 Conf", conf_name, format=format)
    print_key_value("⚙️  Mode", mode, format=format)
    print_wf_url(
        conf=conf,
        conf_name=conf_name,
        mode=mode,
        workflow_id=workflow_id,
        repo=repo,
        format=format,
    )


def submit_schedule(
    repo, conf, hub_url=None, use_auth=True, format: Format = Format.TEXT
):
    hub_conf = get_hub_conf(conf, root_dir=repo)
    zipline_hub = _get_zipline_hub(hub_url, hub_conf, use_auth, format)

    with status_spinner("Computing local conf hashes...", format=format):
        conf_name_to_obj_dict = hub_uploader.build_local_repo_hashmap(root_dir=repo)
    branch = get_current_branch()

    with status_spinner("Syncing confs with Hub...", format=format):
        hub_uploader.compute_and_upload_diffs(
            branch,
            zipline_hub=zipline_hub,
            local_repo_confs=conf_name_to_obj_dict,
            format=format,
        )

    # get conf name
    conf_name = utils.get_metadata_name_from_conf(repo, conf)
    schedule_modes = get_schedule_modes(os.path.join(repo, conf))
    modes = {
        RunMode.BACKFILL.value.upper(): schedule_modes.offline_schedule,
        RunMode.DEPLOY.value.upper(): schedule_modes.online_schedule,
    }

    with status_spinner("Deploying schedule...", format=format):
        response_json = zipline_hub.call_schedule_api(
            modes=modes,
            branch=branch,
            conf_name=conf_name,
            conf_hash=conf_name_to_obj_dict[conf_name].hash,
        )

    if format == Format.JSON:
        print(json.dumps(response_json, indent=4))
        sys.exit(0)

    schedules = response_json.get("schedules", "N/A")
    readable_schedules = {
        Mode._VALUES_TO_NAMES[int(k)]: v for k, v in schedules.items()
    }
    print_success("Schedule deployed. 🗓️", format=format)
    print_key_value("📦 Conf", conf_name, format=format)
    print_key_value("🗓️  Schedules", readable_schedules, format=format)


# zipline hub backfill compiled/joins/join
# adhoc backfills
@hub.command()
@conf_argument
@common_options
@start_ds_option
@end_ds_option
@handle_conf_not_found(log_error=True, callback=print_possible_confs)
@handle_compile
@jsonify_exceptions_if_json_format
def backfill(
    conf,
    repo,
    hub_url,
    use_auth,
    format,
    force,
    start_ds,
    end_ds,
    assume_yes,
    skip_compile,
):
    """Submit a backfill job to Zipline Hub.

    CONF is the path to the compiled conf (e.g. compiled/joins/team/my_join).
    """
    submit_workflow(
        repo,
        conf,
        RunMode.BACKFILL.value,
        start_ds,
        end_ds,
        hub_url=hub_url,
        use_auth=use_auth,
        format=format,
    )


# zipline hub run-adhoc compiled/joins/join
# currently only supports one-off deploy node submission
@hub.command()
@conf_argument
@common_options
@end_ds_option
@handle_conf_not_found(log_error=True, callback=print_possible_confs)
@handle_compile
@jsonify_exceptions_if_json_format
def run_adhoc(
    conf,
    repo,
    hub_url,
    use_auth,
    format,
    force,
    end_ds,
    assume_yes,
    skip_compile,
):
    """Submit a one-off deploy job to test a conf online.

    CONF is the path to the compiled conf (e.g. compiled/joins/team/my_join).
    """
    submit_workflow(
        repo,
        conf,
        RunMode.DEPLOY.value,
        end_ds,
        end_ds,
        hub_url=hub_url,
        use_auth=use_auth,
        format=format,
    )


# zipline hub schedule compiled/joins/join
@hub.command()
@conf_argument
@common_options
@handle_conf_not_found(log_error=True, callback=print_possible_confs)
@handle_compile
@jsonify_exceptions_if_json_format
def schedule(
    conf,
    repo,
    hub_url,
    use_auth,
    format,
    force,
    skip_compile,
):
    """Deploy a recurring schedule for a conf.

    CONF is the path to the compiled conf (e.g. compiled/joins/team/my_join).
    """
    submit_schedule(repo, conf, hub_url=hub_url, use_auth=use_auth, format=format)


# zipline hub schedule-all
@hub.command()
@repo_option
@hub_url_option
@use_auth_option
@format_option
@jsonify_exceptions_if_json_format
@cloud_provider_option
@customer_id_option
@handle_dry_run_compile
def schedule_all(
    repo,
    cloud,
    customer_id,
    hub_url=None,
    use_auth=True,
    format: Format = Format.TEXT,
    compile_pending_changes=None,
):
    """Deploy recurring schedules for all changed confs that have schedules defined."""

    if compile_pending_changes:
        # Check if there are any changes
        added = compile_pending_changes.get("added", [])
        changed = compile_pending_changes.get("changed", [])
        deleted = compile_pending_changes.get("deleted", [])
        has_changes = bool(added or changed or deleted)

        if has_changes:
            print_error(
                "Compilation resulted in changes detected in dry run mode",
                format=format,
            )
            if added:
                print_error(
                    f"  Added: {', '.join([c.name for c in added])}", format=format
                )
            if changed:
                print_error(
                    f"  Changed: {', '.join([c.name for c in changed])}",
                    format=format,
                )
            if deleted:
                print_error(
                    f"  Deleted: {', '.join([c.name for c in deleted])}",
                    format=format,
                )
            sys.exit(1)
        else:
            print_success("No compilation changes detected.", format=format)
    else:
        print_success("No compilation changes detected.", format=format)

    submit_schedule_all(
        repo, cloud, customer_id, hub_url=hub_url, use_auth=use_auth, format=format
    )


@hub.command()
@click.argument("workflow_id")
@repo_option
@hub_url_option
@use_auth_option
@format_option
@jsonify_exceptions_if_json_format
@cloud_provider_option
@customer_id_option
def cancel(workflow_id, repo, hub_url, use_auth, format, cloud, customer_id):
    """Cancel a running workflow.

    WORKFLOW_ID is the ID of the workflow to cancel.
    """
    zipline_hub = _get_zipline_hub(
        hub_url,
        get_hub_conf_from_metadata_conf(
            DEFAULT_TEAM_METADATA_CONF,
            root_dir=repo,
            cloud_provider=cloud,
            customer_id=customer_id,
        ),
        use_auth,
        format,
    )
    response_json = zipline_hub.call_cancel_api(workflow_id)
    if format == Format.JSON:
        print(json.dumps(response_json, indent=4))
        sys.exit(0)
    print_success(f"Workflow cancelled: {workflow_id}", format=format)


@hub.command("clear-downstream")
@conf_argument
@repo_option
@hub_url_option
@use_auth_option
@format_option
@jsonify_exceptions_if_json_format
@start_ds_option
@end_ds_option
@handle_conf_not_found(log_error=True, callback=print_possible_confs)
def clear_downstream(conf, repo, hub_url, use_auth, format, start_ds, end_ds, assume_yes):
    """Clear a conf's terminal node and all downstream nodes for a date range.

    CONF is the path to the compiled conf (e.g. compiled/joins/team/my_join).
    """
    hub_conf = get_hub_conf(conf, root_dir=repo)
    zipline_hub = _get_zipline_hub(hub_url, hub_conf, use_auth, format)

    conf_name = utils.get_metadata_name_from_conf(repo, conf)
    branch = get_current_branch()
    user = get_user_email()

    with status_spinner("Computing downstream node ranges...", format=format):
        preview_json = zipline_hub.preview_clear_downstream(
            conf_name=conf_name,
            branch=branch,
            user=user,
            start=start_ds,
            end=end_ds,
        )

    results = preview_json.get("results", [])
    affected_confs = preview_json.get("affectedConfs", [])

    print_key_value("Conf", conf_name, format=format)
    start_str = start_ds.strftime("%Y-%m-%d") if hasattr(start_ds, "strftime") else str(start_ds)
    end_str = end_ds.strftime("%Y-%m-%d") if hasattr(end_ds, "strftime") else str(end_ds)
    print_key_value("Range", f"{start_str} to {end_str}", format=format)
    print_key_value("Affected confs", len(affected_confs), format=format)
    click.echo()
    for conf_result in affected_confs:
        print_key_value(
            f"  {conf_result.get('confName', 'unknown')}",
            f"{conf_result.get('startPartition', '')} to {conf_result.get('endPartition', '')}",
            format=format,
        )
    click.echo()

    if not assume_yes:
        if not click.confirm(click.style("Proceed with clearing these confs?", fg="yellow")):
            click.echo("Aborted.")
            sys.exit(0)

    with status_spinner("Clearing downstream nodes...", format=format):
        zipline_hub.apply_clear_downstream(
            node_results=results,
            user=user,
            affected_confs=affected_confs,
        )

    print_success(f"Cleared {len(affected_confs)} confs", format=format)
    click.echo()
    click.echo("To recompute, run backfill for each affected conf:")
    click.echo("  zipline hub backfill <compiled_conf_path> --start-ds <start> --end-ds <end>")


def load_json(file_path):
    with open(file_path, "r") as f:
        data = json.load(f)
    return data


def get_metadata_map(file_path):
    data = load_json(file_path)
    metadata_map = data["metaData"]
    return metadata_map


def get_common_env_map(file_path, skip_metadata_extraction=False):
    metadata_map = (
        get_metadata_map(file_path)
        if not skip_metadata_extraction
        else load_json(file_path)
    )
    common_env_map = metadata_map["executionInfo"]["env"]["common"]
    return common_env_map


# zipline hub fetch compiled/joins/join
# call the zipline fetcher from the hub
@hub.command()
@conf_argument
@common_options
@click.option(
    "--fetcher-url",
    help="Fetcher server address (e.g. http://localhost:3904).",
    type=str,
    default=None,
)
@click.option(
    "--schema",
    help="Get only the schema",
    is_flag=True,
)
@click.option("--key-json", help="Json of the keys to fetch", type=str, default=None)
@handle_conf_not_found(log_error=True, callback=print_possible_confs)
@jsonify_exceptions_if_json_format
def fetch(conf, repo, hub_url, use_auth, format, force, fetcher_url, schema, key_json):
    """Fetch data from the Zipline fetcher server.

    CONF is the path to the compiled conf (e.g. compiled/joins/team/my_join).
    """
    hub_conf = get_hub_conf(conf, root_dir=repo)
    fetcher_url = fetcher_url or hub_conf.fetcher_url
    if not fetcher_url:
        raise ValueError(
            "Fetcher URL is not set. Provide --fetcher-url or set fetcher_url in your hub config."
        )
    r = requests.get(f"{fetcher_url}/ping", timeout=100)
    if r.status_code != 200:
        raise RuntimeError(
            f"Fetcher server is not running. Please start the fetcher server and try again. Url: {fetcher_url}/ping Status code: {r.status_code}"
        )
    # Figure out if it's a group by or join
    conf_type = get_conf_type(conf)
    target = utils.get_metadata_name_from_conf(repo, conf)

    # TODO: fix this workaround to just use conf_type directly singular
    if conf_type == "modeltransforms":
        endpoint = "/v1/fetch/{conf_type}".format(conf_type=conf_type)
    else:
        endpoint = "/v1/fetch/{conf_type}".format(conf_type=conf_type[:-1])
    if schema:
        if conf_type != "joins":
            raise ValueError("Schema fetch is only supported for joins")
        endpoint = f"/v1/join/{target}/schema"
    headers = {"Content-Type": "application/json"}
    try:
        if schema:
            url = f"{fetcher_url}{endpoint}"
            response = requests.get(url, headers=headers, timeout=100)
        else:
            url = f"{fetcher_url}{endpoint}/{target}"
            key_json = json.loads(key_json)
            response = requests.post(url, headers=headers, json=key_json, timeout=100)
        if response.status_code != 200:
            raise requests.RequestException(
                f"Request failed: {url} with status code: {response.status_code}\nResponse: {response.text}"
            )
        print(json.dumps(response.json(), indent=4))
    except requests.RequestException as e:
        raise RuntimeError(
            f"""
        Request failed for url: {url}
        The conditions for a successful fetch are:
        - Metadata has been uploaded to the KV Store (run-adhoc command or schedule command)
        - The join needs to be online.
        Please verify the above conditions and try again.
        Error: {e}
        """
        ) from e


# zipline hub eval compiled/joins/join
# localSparkSession evaluation of conf
@hub.command()
@conf_argument
@common_options
@click.option(
    "--eval-url",
    help="Eval server address (e.g. http://localhost:3904).",
    type=str,
    default=None,
)
@click.option(
    "--generate-test-config",
    help="Generate a test config for data testing.",
    is_flag=True,
    default=False,
)
@click.option(
    "--test-data-path",
    help="Path to the test data yaml file to upload and use for evaluation.",
    type=str,
    default=None,
)
@handle_conf_not_found(log_error=True, callback=print_possible_confs)
@handle_compile
@jsonify_exceptions_if_json_format
def eval(
    conf,
    repo,
    hub_url,
    use_auth,
    format,
    force,
    eval_url,
    generate_test_config,
    test_data_path,
    skip_compile,
):
    """Validate a conf against source tables and schemas.

    CONF is the path to the compiled conf (e.g. compiled/joins/team/my_join).
    """
    parameters = {}
    hub_conf = get_hub_conf(conf, root_dir=repo)
    scope = ""
    if hub_conf.auth_scope is not None:
        scope = hub_conf.auth_scope
    elif hub_conf.cloud_provider == "azure" and hub_conf.customer_id is not None:
        scope = f"api://{hub_conf.customer_id}-zipline-auth"
    zipline_hub = ZiplineHub(
        base_url=hub_url or hub_conf.hub_url,
        sa_name=hub_conf.sa_name,
        use_auth=use_auth,
        eval_url=eval_url or hub_conf.eval_url,
        cloud_provider=hub_conf.cloud_provider,
        scope=scope,
        format=format,
        auth_url=hub_conf.frontend_url,
    )
    conf_name_to_hash_dict = hub_uploader.build_local_repo_hashmap(root_dir=repo)
    branch = get_current_branch()
    if test_data_path:
        # Upload the test data skeleton to the bucket.
        if hub_conf.cloud_provider != "gcp":
            raise RuntimeError("Test data path is only supported for GCP.")
        # import here to avoid dependency for other clouds.
        zipline_artifact_prefix = (
            hub_conf.artifact_prefix.rstrip("/") if hub_conf.artifact_prefix else ""
        )
        if not zipline_artifact_prefix:
            raise click.UsageError("Zipline artifact prefix is not set.")
        url = f"eval/test_data/{os.path.basename(test_data_path)}"
        upload_to_blob_store(test_data_path, f"{zipline_artifact_prefix}/{url}")
        parameters["testDataPath"] = f"{zipline_artifact_prefix}/{url}"

    hub_uploader.compute_and_upload_diffs(
        branch, zipline_hub=zipline_hub, local_repo_confs=conf_name_to_hash_dict
    )

    # get conf name
    conf_name = utils.get_metadata_name_from_conf(repo, conf)
    if generate_test_config:
        parameters["generateTestDataSkeleton"] = "true"
    response_json = zipline_hub.call_eval_api(
        conf_name=conf_name,
        conf_hash_map={
            conf.name: conf.hash for conf in conf_name_to_hash_dict.values()
        },
        parameters=parameters,
    )
    if format == Format.JSON:
        print(json.dumps(response_json, indent=4))
        sys.exit(0 if response_json.get("success") else 1)
    if response_json.get("success"):
        print_success("Eval job finished successfully.", format=format)
        format_print(response_json.get("message"), format=format)
    else:
        print_error("Eval job failed.", format=format)
        format_print(response_json.get("message"), format=format)
        sys.exit(1)


# zipline hub eval-table data.loggable_response
# evaluate table schema using eval API
@hub.command()
@click.argument("table")
@repo_option
@click.option(
    "--conf",
    required=False,
    help="Optional conf to use for executionInfo. Takes precedence over --team.",
)
@click.option(
    "--team",
    required=False,
    help="Optional team name to use for executionInfo. If not specified, uses default team metadata.",
)
@hub_url_option
@use_auth_option
@format_option
@click.option(
    "--eval-url",
    help="Eval server address (e.g. http://localhost:3904).",
    type=str,
    default=None,
)
@click.option(
    "--engine-type",
    help="Engine type for table evaluation.",
    type=str,
    default="SPARK",
    show_default=True,
)
@jsonify_exceptions_if_json_format
def eval_table(
    table, repo, conf, team, hub_url, use_auth, format, eval_url, engine_type
):
    """Validate a table's schema.

    TABLE is the table name for schema evaluation (e.g. data.loggable_response).
    """
    # Use conf for executionInfo if provided (highest priority)
    conf_execution_info, team_execution_info, default_execution_info = None, None, None
    team = team or os.environ.get("TEAM")
    if conf:
        file_path = os.path.join(repo, conf)
        conf_execution_info = get_metadata_map(file_path).get("executionInfo")
    # Otherwise use team metadata if specified
    elif team:
        team_metadata_path = f"compiled/teams_metadata/{team}/{team}_team_metadata"
        file_path = os.path.join(repo, team_metadata_path)
        with open(file_path, "r") as f:
            team_execution_info = json.load(f).get("executionInfo")
    # Otherwise use default team metadata
    else:
        file_path = os.path.join(repo, DEFAULT_TEAM_METADATA_CONF)
        with open(file_path, "r") as f:
            default_execution_info = json.load(f).get("executionInfo")
    execution_info = (
        conf_execution_info or team_execution_info or default_execution_info
    )
    common_env = execution_info["env"]["common"]
    common_env.update(os.environ)  # Override conf with env vars if set
    hub_conf = HubConfig(
        **{k: common_env.get(k.upper()) for k in HubConfig.__dataclass_fields__.keys()}
    )
    scope = ""
    if hub_conf.auth_scope is not None:
        scope = hub_conf.auth_scope
    elif hub_conf.cloud_provider == "azure" and hub_conf.customer_id is not None:
        scope = f"api://{hub_conf.customer_id}-zipline-auth"

    zipline_hub = ZiplineHub(
        base_url=hub_url or hub_conf.hub_url,
        sa_name=hub_conf.sa_name,
        use_auth=use_auth,
        eval_url=eval_url or hub_conf.eval_url,
        cloud_provider=hub_conf.cloud_provider,
        scope=scope,
        format=format,
        auth_url=hub_conf.frontend_url,
    )

    execution_info = (
        conf_execution_info or team_execution_info or default_execution_info
    )
    response_json = zipline_hub.call_schema_api(
        table_name=table,
        engine_type=engine_type,
        execution_info=execution_info,
    )

    success = response_json.get("success")
    if format == Format.JSON:
        response_json = _resolve_data_type_kinds(response_json)
        print(json.dumps(response_json, indent=4))
        sys.exit(0 if success else 1)

    if success:
        print_success("Schema evaluation finished successfully.", format=format)
        format_print(response_json.get("message"), format=format)
    else:
        print_error("Schema evaluation failed.", format=format)
        format_print(response_json.get("message"), format=format)
        sys.exit(1)


# zipline hub list-tables demo
# list tables in a schema using eval API
@hub.command()
@click.argument("schema_name")
@repo_option
@click.option(
    "--team",
    required=False,
    help="Optional team name to use for executionInfo. If not specified, uses default team metadata.",
)
@hub_url_option
@use_auth_option
@format_option
@click.option(
    "--eval-url",
    help="Eval server address (e.g. http://localhost:3904).",
    type=str,
    default=None,
)
@click.option(
    "--engine-type",
    help="Engine type for listing tables.",
    type=str,
    default="SPARK",
    show_default=True,
)
@jsonify_exceptions_if_json_format
def list_tables(schema_name, repo, team, hub_url, use_auth, format, eval_url, engine_type):
    """List tables in a schema.

    SCHEMA_NAME is the schema/database to list tables from (e.g. demo).
    """
    team_execution_info, default_execution_info = None, None
    team = team or os.environ.get("TEAM")
    if team:
        team_metadata_path = f"compiled/teams_metadata/{team}/{team}_team_metadata"
        file_path = os.path.join(repo, team_metadata_path)
        with open(file_path, "r") as f:
            team_execution_info = json.load(f).get("executionInfo")
    else:
        file_path = os.path.join(repo, DEFAULT_TEAM_METADATA_CONF)
        with open(file_path, "r") as f:
            default_execution_info = json.load(f).get("executionInfo")
    execution_info = team_execution_info or default_execution_info
    common_env = execution_info["env"]["common"]
    common_env.update(os.environ)
    hub_conf = HubConfig(
        **{k: common_env.get(k.upper()) for k in HubConfig.__dataclass_fields__.keys()}
    )
    scope = ""
    if hub_conf.auth_scope is not None:
        scope = hub_conf.auth_scope
    elif hub_conf.cloud_provider == "azure" and hub_conf.customer_id is not None:
        scope = f"api://{hub_conf.customer_id}-zipline-auth"

    zipline_hub = ZiplineHub(
        base_url=hub_url or hub_conf.hub_url,
        sa_name=hub_conf.sa_name,
        use_auth=use_auth,
        eval_url=eval_url or hub_conf.eval_url,
        cloud_provider=hub_conf.cloud_provider,
        scope=scope,
        format=format,
        auth_url=hub_conf.frontend_url,
    )

    response_json = zipline_hub.call_list_tables_api(
        schema_name=schema_name,
        engine_type=engine_type,
        execution_info=execution_info,
    )

    success = response_json.get("success")
    if format == Format.JSON:
        print(json.dumps(response_json, indent=4))
        sys.exit(0 if success else 1)

    if success:
        print_success("List tables finished successfully.", format=format)
        tables = response_json.get("tables") or []
        for table in tables:
            print_info(table, format=format)
    else:
        print_error("List tables failed.", format=format)
        format_print(response_json.get("message"), format=format)
        sys.exit(1)


def get_hub_conf(conf_path, root_dir="."):
    """
    Get the hub configuration from the config file or environment variables.
    This method is used when the args are not provided.
    Priority is arg -> environment variable -> common env.
    """
    file_path = os.path.join(root_dir, conf_path)
    common_env_map = get_common_env_map(file_path)
    common_env_map.update(os.environ)  # Override config with cli args
    kwargs = {
        k: common_env_map.get(k.upper()) for k in HubConfig.__dataclass_fields__.keys()
    }
    return HubConfig(**kwargs)


def get_hub_conf_from_metadata_conf(
    metadata_path,
    root_dir=".",
    cloud_provider: Optional[str] = None,
    customer_id: Optional[str] = None,
):
    """
    Get the hub configuration from the config file or environment variables.
    This method is used when the args are not provided.
    Priority is arg -> environment variable -> common env.
    """
    file_path = os.path.join(root_dir, metadata_path)
    common_env_map = get_common_env_map(file_path, skip_metadata_extraction=True)
    common_env_map.update(os.environ)  # Override config with cli args
    hub_url = common_env_map.get("HUB_URL")
    frontend_url = common_env_map.get("FRONTEND_URL")
    sa_name = common_env_map.get("SA_NAME")
    eval_url = common_env_map.get("EVAL_URL")

    cloud_provider = cloud_provider or common_env_map.get("CLOUD_PROVIDER")

    if not cloud_provider:
        raise click.UsageError(
            "Cloud provider is not set. Use --cloud or define CLOUD_PROVIDER in team env."
        )

    customer_id = customer_id or common_env_map.get("CUSTOMER_ID")
    if cloud_provider == "azure" and not customer_id:
        raise click.UsageError(
            "Customer ID is not set for Azure. Use --customer-id or define CUSTOMER_ID in team env."
        )

    return HubConfig(
        hub_url=hub_url,
        frontend_url=frontend_url,
        sa_name=sa_name,
        eval_url=eval_url,
        cloud_provider=cloud_provider,
        customer_id=customer_id,
    )


def get_schedule_modes(conf_path: str):
    metadata_map = get_metadata_map(conf_path)
    # Get the online and offline schedules from executionInfo
    online_schedule = metadata_map["executionInfo"].get("onlineSchedule", None)
    offline_schedule = metadata_map["executionInfo"].get("offlineSchedule", None)

    # "@never" explicitly disables online scheduling
    if online_schedule == "@never":
        online_schedule = None

    # Check if "online" is True before proceeding with online_schedule
    is_online = metadata_map.get("online", False)
    if not is_online:
        online_schedule = None

    # Validate schedule expressions using croniter-based validation
    if offline_schedule:
        validation_error = _validate_at_most_daily_schedule(offline_schedule)
        if validation_error:
            raise ValueError(f"Invalid offline_schedule: {validation_error}")

    if online_schedule:
        validation_error = _validate_at_most_daily_schedule(online_schedule)
        if validation_error:
            raise ValueError(f"Invalid online_schedule: {validation_error}")

    # Default to "None" string if schedules are not set
    # This is used by the schedule API to determine if a schedule is active
    online_schedule = online_schedule or SCHEDULE_NONE_STR
    offline_schedule = offline_schedule or SCHEDULE_NONE_STR
    return ScheduleModes(
        offline_schedule=offline_schedule, online_schedule=online_schedule
    )


def print_wf_url(
    conf, conf_name, mode, workflow_id, repo=".", format: Format = Format.TEXT
):
    hub_conf = get_hub_conf(conf, root_dir=repo)
    frontend_url = hub_conf.frontend_url
    hub_conf_type = get_conf_type(conf)

    def _mode_string():
        if mode == "backfill":
            return "offline"
        elif mode == "deploy":
            return "online"
        else:
            raise ValueError(f"Unsupported mode: {mode}")

    workflow_url = f"{frontend_url.rstrip('/')}/{hub_conf_type}/{conf_name}/{_mode_string()}?workflowId={workflow_id}"

    print_url("🔗 Workflow", workflow_url, format=format)


if __name__ == "__main__":
    hub()
