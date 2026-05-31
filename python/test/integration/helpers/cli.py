"""Shared CLI helpers for integration tests."""

import json
import os
import shutil
import subprocess
import sys

from ai.chronon.repo.zipline import zipline


def compile_configs(runner, chronon_root, clean=True):
    """Compile canary configs, optionally from a clean state."""
    if clean:
        compiled_dir = os.path.join(chronon_root, "compiled")
        if os.path.exists(compiled_dir):
            shutil.rmtree(compiled_dir)

    result = runner.invoke(
        zipline,
        ["compile", f"--chronon-root={chronon_root}", "--force"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, f"compile failed:\n{result.output}"


def _extract_workflow_id(output: str) -> str:
    """Extract the workflowId from CLI JSON output."""
    json_start = output.rfind("{")
    assert json_start != -1, f"No JSON in output:\n{output}"
    response = json.loads(output[json_start:])
    workflow_id = response.get("workflowId")
    assert workflow_id, f"No workflowId in response:\n{output}"
    return workflow_id


def submit_backfill(runner, chronon_root, hub_url, conf, start_ds, end_ds):
    """Submit a backfill and return the workflow ID."""
    result = runner.invoke(
        zipline,
        [
            "hub", "backfill",
            conf,
            f"--chronon-root={chronon_root}",
            f"--hub-url={hub_url}",
            f"--start-ds={start_ds}",
            f"--end-ds={end_ds}",
            "--format=json",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, f"backfill failed:\n{result.output}"
    return _extract_workflow_id(result.output)


def submit_run_adhoc(runner, chronon_root, hub_url, conf, end_ds):
    """Submit a run-adhoc deploy and return the workflow ID."""
    result = runner.invoke(
        zipline,
        [
            "hub", "run-adhoc",
            conf,
            f"--chronon-root={chronon_root}",
            f"--hub-url={hub_url}",
            f"--end-ds={end_ds}",
            "--format=json",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, f"run-adhoc failed:\n{result.output}"
    return _extract_workflow_id(result.output)


def cancel_workflow(runner, chronon_root, hub_url, workflow_id, cloud):
    """Cancel a workflow via the CLI."""
    result = runner.invoke(
        zipline,
        [
            "hub", "cancel",
            workflow_id,
            f"--chronon-root={chronon_root}",
            f"--hub-url={hub_url}",
            f"--cloud={cloud}",
            "--format=json",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, f"cancel failed:\n{result.output}"


# ---------------------------------------------------------------------------
# Direct ``zipline run`` helpers (Dataproc path, no orchestrator)
# ---------------------------------------------------------------------------


def submit_run(runner, chronon_root, conf, version, mode="backfill", start_ds=None, end_ds=None, extra_args=None):
    """Invoke ``zipline run`` and assert success."""
    args = ["run", conf, f"--chronon-root={chronon_root}", f"--version={version}", f"--mode={mode}"]
    if start_ds:
        args.append(f"--start-ds={start_ds}")
    if end_ds:
        args.append(f"--end-ds={end_ds}")
    if extra_args:
        args.extend(extra_args)
    result = runner.invoke(zipline, args, catch_exceptions=False)
    assert result.exit_code == 0, f"zipline run failed:\n{result.output}"
    return result


_WRAPPER_SCRIPT = os.path.join(os.path.dirname(__file__), "_run_zipline.py")


def submit_run_subprocess(chronon_root, conf, version, mode="backfill",
                          start_ds=None, end_ds=None, extra_args=None, timeout_s=1800):
    """Thread-safe ``zipline run`` via subprocess (Click's CliRunner is not thread-safe).

    Uses _run_zipline.py wrapper which force-exits with os._exit() to
    avoid hanging on GCP SDK daemon thread cleanup.
    """
    cmd = [sys.executable, _WRAPPER_SCRIPT,
           "run", conf, f"--chronon-root={chronon_root}", f"--version={version}", f"--mode={mode}"]
    if start_ds:
        cmd.append(f"--start-ds={start_ds}")
    if end_ds:
        cmd.append(f"--end-ds={end_ds}")
    if extra_args:
        cmd.extend(extra_args)
    env = os.environ.copy()
    env["PYTHONHASHSEED"] = "0"
    env["PYTHONPATH"] = os.pathsep.join(p for p in sys.path if p)
    try:
        result = subprocess.run(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, env=env, timeout=timeout_s,
        )
    except subprocess.TimeoutExpired as exc:
        raise AssertionError(
            f"zipline run timed out after {timeout_s}s:\n{exc.stdout or ''}"
        ) from exc
    assert result.returncode == 0, \
        f"zipline run failed (exit {result.returncode}):\n{result.stdout}"


def submit_check_partitions(runner, chronon_root, conf, version, partition_names):
    """Invoke ``zipline run`` in metastore check-partitions mode."""
    return submit_run(
        runner, chronon_root, conf, version,
        mode="metastore",
        extra_args=["check-partitions", f"--partition-names={partition_names}"],
    )


def submit_upload(runner, chronon_root, conf, version, ds):
    """Invoke ``zipline run`` in upload mode."""
    return submit_run(runner, chronon_root, conf, version, mode="upload", extra_args=[f"--ds={ds}"])


def submit_upload_to_kv(runner, chronon_root, conf, version, ds):
    """Invoke ``zipline run`` in upload-to-kv mode."""
    return submit_run(runner, chronon_root, conf, version, mode="upload-to-kv", extra_args=[f"--ds={ds}"])


def submit_metadata_upload(runner, chronon_root, conf, version):
    """Invoke ``zipline run`` in metadata-upload mode."""
    return submit_run(runner, chronon_root, conf, version, mode="metadata-upload")


def submit_fetch(runner, chronon_root, conf, version, keys, name):
    """Invoke ``zipline run`` in fetch mode and return the result for assertion."""
    return submit_run(
        runner, chronon_root, conf, version,
        mode="fetch",
        extra_args=["-k", keys, "--name", name],
    )


# ---------------------------------------------------------------------------
# Hub/Orchestrator helpers
# ---------------------------------------------------------------------------


def submit_eval(runner, chronon_root, hub_url, eval_url, conf, test_data_path=None):
    """Submit an eval job and assert success."""
    args = [
        "hub", "eval",
        conf,
        f"--chronon-root={chronon_root}",
        f"--hub-url={hub_url}",
        f"--eval-url={eval_url}",
        "--format=json",
    ]
    if test_data_path:
        args += ["--test-data-path", test_data_path]
    result = runner.invoke(zipline, args, catch_exceptions=False)
    assert result.exit_code == 0, f"eval failed:\n{result.output}"
    return result


def submit_schedule(runner, chronon_root, hub_url, conf):
    """Deploy a recurring schedule for a conf."""
    result = runner.invoke(
        zipline,
        [
            "hub", "schedule",
            conf,
            f"--chronon-root={chronon_root}",
            f"--hub-url={hub_url}",
            "--format=json",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, f"schedule failed:\n{result.output}"
