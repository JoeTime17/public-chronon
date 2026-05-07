import os
import sys

import click

from ai.chronon.repo import utils
from ai.chronon.repo.admin import admin
from ai.chronon.repo.auth import auth
from ai.chronon.repo.compile import compile
from ai.chronon.repo.config import config
from ai.chronon.repo.hub_runner import hub
from ai.chronon.repo.init import main as init_cmd
from ai.chronon.repo.init_agent import init_agent
from ai.chronon.repo.run import main as run_main

CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])


@click.group(
    help="The Zipline CLI. A tool for compiling and running Zipline pipelines. For more information, see: https://zipline.ai/docs",
    context_settings=CONTEXT_SETTINGS,
)
@click.version_option(version=utils.get_package_version())
@click.pass_context
def zipline(ctx):
    ctx.ensure_object(dict)
    ctx.obj["version"] = utils.get_package_version()


zipline.add_command(auth)
zipline.add_command(compile)
zipline.add_command(config)
zipline.add_command(run_main)
zipline.add_command(hub)
zipline.add_command(admin)
zipline.add_command(init_agent)

admin.add_command(init_cmd)


def main():
    """CLI entry point that ensures deterministic hashing before any config loading.
    PYTHONHASHSEED must be set before interpreter startup, so re-exec if needed."""
    if os.environ.get("PYTHONHASHSEED") != "0":
        os.environ["PYTHONHASHSEED"] = "0"
        os.execv(sys.executable, [sys.executable] + sys.argv)
    zipline()
