import click
import pytest
from click.testing import CliRunner

from ai.chronon.cli.options import (
    CHRONON_REPO_PATH_ENVVAR,
    CHRONON_ROOT_ENVVAR,
    chronon_root_option,
    env_option,
    repo_root_option,
)


def test_chronon_root_option_reads_chronon_root_env():
    @click.command()
    @chronon_root_option()
    def command(chronon_root):
        click.echo(chronon_root)

    result = CliRunner().invoke(
        command,
        [],
        env={CHRONON_ROOT_ENVVAR: "/tmp/chronon-root"},
    )

    assert result.exit_code == 0
    assert result.output.strip() == "/tmp/chronon-root"


def test_repo_root_option_prefers_explicit_chronon_root_over_env():
    @click.command()
    @repo_root_option()
    def command(repo):
        click.echo(repo)

    result = CliRunner().invoke(
        command,
        ["--chronon-root", "/tmp/explicit-root"],
        env={CHRONON_ROOT_ENVVAR: "/tmp/env-root"},
    )

    assert result.exit_code == 0
    assert result.output.strip() == "/tmp/explicit-root"


@pytest.mark.parametrize("option", ["--repo", "-r"])
def test_repo_root_option_supports_deprecated_repo_alias(option):
    @click.command()
    @repo_root_option()
    def command(repo):
        click.echo(repo)

    result = CliRunner().invoke(
        command,
        [option, "/tmp/explicit-root"],
        env={CHRONON_ROOT_ENVVAR: "/tmp/env-root"},
    )

    assert result.exit_code == 0
    assert "DeprecationWarning" in result.output
    assert "Use --chronon-root instead." in result.output
    assert result.output.strip().endswith("/tmp/explicit-root")


def test_repo_root_option_rejects_repo_and_chronon_root_together():
    @click.command()
    @repo_root_option()
    def command(repo):
        click.echo(repo)

    result = CliRunner().invoke(
        command,
        ["--chronon-root", "/tmp/root", "--repo", "/tmp/repo"],
    )

    assert result.exit_code == 2
    assert "Use only one of --chronon-root or --repo." in result.output


def test_repo_root_option_marks_deprecated_alias_in_help():
    @click.command()
    @repo_root_option()
    def command(repo):
        click.echo(repo)

    result = CliRunner().invoke(command, ["--help"])

    assert result.exit_code == 0
    assert "--chronon-root TEXT" in result.output
    assert "-r, --repo TEXT" in result.output
    assert "(DEPRECATED: Use" in result.output
    assert "--chronon-root instead.)" in result.output
    assert result.output.index("--chronon-root") < result.output.index("--repo")


def test_repo_root_option_supports_legacy_chronon_repo_path_fallback():
    @click.command()
    @repo_root_option(envvars=(CHRONON_ROOT_ENVVAR, CHRONON_REPO_PATH_ENVVAR))
    def command(repo):
        click.echo(repo)

    result = CliRunner().invoke(
        command,
        [],
        env={CHRONON_REPO_PATH_ENVVAR: "/tmp/legacy-root"},
    )

    assert result.exit_code == 0
    assert result.output.strip() == "/tmp/legacy-root"


def test_env_option_rejects_unknown_environment():
    @click.command()
    @env_option()
    def command(env):
        click.echo(env)

    result = CliRunner().invoke(command, ["--env", "staging"])

    assert result.exit_code == 2
    assert "Invalid value for '--env'" in result.output
