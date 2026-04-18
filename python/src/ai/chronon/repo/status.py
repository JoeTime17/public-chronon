import logging
import os
import subprocess

import click

from ai.chronon.repo.aws import (
    ZIPLINE_AWS_JAR_DEFAULT,
    ZIPLINE_AWS_ONLINE_CLASS_DEFAULT,
    ZIPLINE_AWS_SERVICE_JAR,
    AwsRunner,
)
from ai.chronon.repo.azure_runner import (
    ZIPLINE_AZURE_JAR_DEFAULT,
    ZIPLINE_AZURE_ONLINE_CLASS_DEFAULT,
    ZIPLINE_AZURE_SERVICE_JAR,
    AzureRunner,
)
from ai.chronon.repo.constants import AWS, AZURE, CLOUD_PROVIDER_KEYWORD, GCP, ZIPLINE_DIRECTORY
from ai.chronon.repo.gcp import (
    ZIPLINE_GCP_JAR_DEFAULT,
    ZIPLINE_GCP_ONLINE_CLASS_DEFAULT,
    ZIPLINE_GCP_SERVICE_JAR,
    GcpRunner,
)
from ai.chronon.repo.utils import get_environ_arg, resolve_conf

LOG = logging.getLogger(__name__)

STATUS_MODES = ["upload-to-kv", "streaming"]
CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])
STATUS_ENTRYPOINT = "ai.chronon.online.status.StatusMain"


def _resolve_cloud_jars(cloud_provider, artifact_prefix, version):
    """Download jars for the cloud provider; return (classpath, online_class)."""
    if cloud_provider.upper() == GCP:
        cloud_jar = GcpRunner.download_zipline_dataproc_jar(
            artifact_prefix, ZIPLINE_DIRECTORY, version, ZIPLINE_GCP_JAR_DEFAULT
        )
        service_jar = GcpRunner.download_zipline_dataproc_jar(
            artifact_prefix, ZIPLINE_DIRECTORY, version, ZIPLINE_GCP_SERVICE_JAR
        )
        return f"{cloud_jar}:{service_jar}", ZIPLINE_GCP_ONLINE_CLASS_DEFAULT
    elif cloud_provider.upper() == AWS:
        cloud_jar = AwsRunner.download_zipline_aws_jar(
            ZIPLINE_DIRECTORY, artifact_prefix, version, ZIPLINE_AWS_JAR_DEFAULT
        )
        service_jar = AwsRunner.download_zipline_aws_jar(
            ZIPLINE_DIRECTORY, artifact_prefix, version, ZIPLINE_AWS_SERVICE_JAR
        )
        return f"{cloud_jar}:{service_jar}", ZIPLINE_AWS_ONLINE_CLASS_DEFAULT
    elif cloud_provider.upper() == AZURE:
        cloud_jar = AzureRunner.download_jar(
            ZIPLINE_DIRECTORY, artifact_prefix, version, ZIPLINE_AZURE_JAR_DEFAULT
        )
        service_jar = AzureRunner.download_jar(
            ZIPLINE_DIRECTORY, artifact_prefix, version, ZIPLINE_AZURE_SERVICE_JAR
        )
        return f"{cloud_jar}:{service_jar}", ZIPLINE_AZURE_ONLINE_CLASS_DEFAULT
    else:
        raise click.UsageError(f"Unsupported cloud provider: {cloud_provider}")


@click.command(name="status", context_settings=CONTEXT_SETTINGS)
@click.argument("conf")
@click.option("-m", "--mode", type=click.Choice(STATUS_MODES),
              required=True,
              help="Job mode to check status for.")
@click.option("-r", "--repo", default=".", show_default=True,
              help="Path to chronon repo.")
@click.option("--online-jar", envvar="CHRONON_ONLINE_JAR",
              help="Path to the online jar. Required when CLOUD_PROVIDER is not set.")
@click.option("--online-class", envvar="CHRONON_ONLINE_CLASS", default=None,
              help="Api implementation class. Required for upload-to-kv when CLOUD_PROVIDER is not set.")
@click.option("--artifact-prefix", envvar="ARTIFACT_PREFIX",
              help="Remote artifact URI for zipline client artifacts.")
@click.option("--version", envvar="VERSION",
              help="Chronon version to use.")
@click.option("--enable-debug", is_flag=True, default=False,
              help="Enables verbose debug logging.")
def status(conf, mode, repo, online_jar, online_class, artifact_prefix, version, enable_debug):
    """Show the status of a Zipline job.

    CONF is the path to the compiled GroupBy conf (e.g. compiled/group_bys/team/groupby_name).
    """
    conf = resolve_conf(repo, conf)

    # TODO: support join confs in addition to group_by confs
    if "group_bys" not in conf:
        raise click.BadParameter(
            "status only supports group_by confs; path must contain 'group_bys'",
            param_hint="CONF",
        )

    cloud_provider = get_environ_arg(CLOUD_PROVIDER_KEYWORD, ignoreError=True)

    if cloud_provider:
        if not artifact_prefix:
            raise click.UsageError("--artifact-prefix (or ARTIFACT_PREFIX) is required when CLOUD_PROVIDER is set")
        if not version:
            raise click.UsageError("--version (or VERSION) is required when CLOUD_PROVIDER is set")
        os.makedirs(ZIPLINE_DIRECTORY, exist_ok=True)
        classpath, online_class = _resolve_cloud_jars(
            cloud_provider, artifact_prefix, version
        )
    else:
        # OSS path: user supplies the jar(s) directly
        if not online_jar:
            raise click.UsageError(
                "--online-jar (or CHRONON_ONLINE_JAR) is required when CLOUD_PROVIDER is not set"
            )
        if mode == "upload-to-kv" and not online_class:
            raise click.UsageError(
                "--online-class (or CHRONON_ONLINE_CLASS) is required for upload-to-kv "
                "when CLOUD_PROVIDER is not set"
            )
        classpath = online_jar

    cmd = [
        "java", "-cp", classpath, STATUS_ENTRYPOINT,
        "--mode", mode,
        "--conf-path", conf,
        "--repo", repo,
    ]

    if mode == "upload-to-kv":
        if online_jar:
            cmd.extend(["--online-jar", online_jar])
        cmd.extend(["--online-class", online_class])

    if enable_debug:
        cmd.append("--enable-debug")

    LOG.info("Running command: %s", " ".join(cmd))
    subprocess.check_call(cmd, bufsize=0)
