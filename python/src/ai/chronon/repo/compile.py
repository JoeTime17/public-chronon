import json
import os
import sys

import click

from ai.chronon.cli.compile.compile_context import CompileContext
from ai.chronon.cli.compile.compiler import Compiler
from ai.chronon.cli.compile.parse_teams import PROD_ENV, discover_compile_envs
from ai.chronon.cli.formatter import Format, jsonify_exceptions_if_json_format
from ai.chronon.cli.options import chronon_root_option, format_option
from ai.chronon.cli.theme import STYLE_INFO, console
from gen_thrift.api.ttypes import ConfType


@click.command(name="compile")
@chronon_root_option(help="Path to the root Chronon folder.")
@click.option(
    "--ignore-python-errors",
    is_flag=True,
    default=False,
    help="Allow compilation to proceed even with Python errors (useful for testing)",
)
@format_option()
@click.option(
    "--force",
    is_flag=True,
    help="Force compilation to proceed even with errors",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Perform a dry run of the compilation without writing any files",
)
@jsonify_exceptions_if_json_format
def compile(chronon_root, ignore_python_errors=False, format=Format.TEXT, force=False, dry_run=False):
    """Compile Chronon configs."""
    if chronon_root is None or chronon_root == "":
        chronon_root = os.getcwd()

    # Always move chronon_root to the front of sys.path. teams.<env>.py files
    # can do `from teams import …` to reuse prod team definitions, and that
    # import has to resolve to chronon_root/teams.py — not to some other
    # teams.py sitting on sys.path from an earlier compile or test fixture.
    if chronon_root in sys.path:
        sys.path.remove(chronon_root)
    if format != Format.JSON:
        console.print(
            f"\nAdding [{STYLE_INFO} italic]{chronon_root}[/{STYLE_INFO} italic] to python path, during compile."
        )
    sys.path.insert(0, chronon_root)

    compiled_result, has_errors, _ = __compile(chronon_root=chronon_root, ignore_python_errors=ignore_python_errors, format=format, force=force, dry_run=dry_run)

    return compiled_result


def __compile(
    chronon_root, ignore_python_errors=False, format=Format.TEXT, force=False, dry_run=False, validate_all=False
):
    if chronon_root:
        chronon_root_path = os.path.expanduser(chronon_root)
        os.chdir(chronon_root_path)

    # check that a "teams.py" file exists in the current directory
    if not (os.path.exists("teams.py") or os.path.exists("teams.json")):
        raise click.ClickException(
            (
                "teams.py or teams.json file not found in current directory."
                " Please run from the top level of conf directory."
            )
        )

    text_mode = format != Format.JSON

    # One compile pass per discovered teams.<env>.py file. Prod runs last so its
    # confirm-prompt (in pending-changes) doesn't get sandwiched between other
    # envs' output. The prod pass's results are what's returned and (in JSON
    # mode) what's printed — keeping the external contract stable.
    envs = discover_compile_envs(os.getenv("CHRONON_ROOT", os.getcwd()))
    results = None
    prod_compiler = None
    any_errors = False
    # One record per env, rendered as a summary block after the last env
    # finishes. `output_written` reflects what compiler.py:114-148 actually
    # does: a failed pass deletes its staging dir (no move to output_dir)
    # unless `--ignore-python-errors` overrides that.
    env_summaries = []
    for env_name, teams_file_basename in envs:
        if text_mode:
            console.rule(f"[bold magenta]🚀 BEGIN {env_name.upper()} COMPILATION[/]")
        ctx = CompileContext(
            ignore_python_errors=ignore_python_errors,
            format=format,
            force=force,
            env=env_name,
            teams_file_name=teams_file_basename,
        )
        compiler = Compiler(ctx)
        env_results = compiler.compile(dry_run, validate_all)
        if text_mode:
            console.rule(f"[bold magenta]🚀 END {env_name.upper()} COMPILATION[/]")
        env_has_errors = compiler.has_compilation_errors()
        any_errors |= env_has_errors
        parsed_count, error_count = _count_env_results(compiler)
        # dry_run always deletes the staging dir without moving it to output_dir
        # (compiler.py:115-118), so nothing lands on disk regardless of errors
        # or --ignore-python-errors.
        output_written = (not dry_run) and ((not env_has_errors) or ignore_python_errors)
        env_summaries.append(
            {
                "name": env_name,
                "ok": not env_has_errors,
                "compile_dir": ctx.compile_dir,
                "parsed_count": parsed_count,
                "error_count": error_count,
                "output_written": output_written,
            }
        )
        if env_name == PROD_ENV:
            results = env_results
            prod_compiler = compiler

    if text_mode:
        _print_compile_summary(env_summaries)

    if format == Format.JSON and results is not None:
        print(
            json.dumps(
                {
                    "status": "success",
                    "results": {
                        ConfType._VALUES_TO_NAMES[conf_type]: list(conf_result.obj_dict.keys())
                        for conf_type, conf_result in results.items()
                        if conf_result.obj_dict
                    },
                },
                indent=4,
            )
        )

    # Any env's failure (canary, staging, prod, …) must surface as a non-zero
    # exit so CI catches a broken canary even when prod succeeded.
    if any_errors and not ignore_python_errors:
        sys.exit(1)

    pending_changes = (
        prod_compiler.compile_context.validator.pending_changes if prod_compiler else None
    )
    return results, any_errors, pending_changes


def _count_env_results(compiler):
    """Return (parsed_count, error_count) summed across every class tracker
    for one env's compile pass. `parsed_count` is what would land in the output
    dir if no errors halted the move (compiler.py:142-148 drops the staging
    dir when there are errors and no `--ignore-python-errors`)."""
    parsed = 0
    errors = 0
    for tracker in compiler.compile_context.compile_status.cls_to_tracker.values():
        parsed += len(tracker.new_objs)
        errors += len(tracker.files_to_errors)
    return parsed, errors


def _print_compile_summary(env_summaries):
    """Render a per-env status table. Always shown in text mode so a user can
    see at a glance which envs passed, which failed, where each one wrote its
    output, and crucially — whether anything actually landed on disk for a
    failed env (it doesn't, unless `--ignore-python-errors` is set)."""
    if not env_summaries:
        return
    console.rule("[bold]📋 COMPILATION SUMMARY[/]")
    name_width = max(len(s["name"]) for s in env_summaries)
    dir_width = max(len(s["compile_dir"]) for s in env_summaries)
    for s in env_summaries:
        status = "[bold green]✓ OK    [/]" if s["ok"] else "[bold red]✗ FAILED[/]"
        if s["ok"] and s["output_written"]:
            detail = f"{s['parsed_count']} written"
        elif s["ok"]:
            # dry-run path: parsed cleanly, but nothing was written to disk.
            detail = f"{s['parsed_count']} parsed (dry-run, output not written)"
        elif s["output_written"]:
            # `--ignore-python-errors` forces the move even with errors.
            detail = (
                f"{s['parsed_count']} written, "
                f"[red]{s['error_count']} error(s)[/red] (--ignore-python-errors)"
            )
        else:
            detail = (
                f"[red]{s['error_count']} error(s)[/red]; "
                f"output not written ({s['parsed_count']} parsed and discarded)"
            )
        console.print(
            f"  {s['name'].ljust(name_width)}  {status}  "
            f"→ [dim]{s['compile_dir'].ljust(dir_width)}[/dim]  ({detail})"
        )


if __name__ == "__main__":
    compile()
