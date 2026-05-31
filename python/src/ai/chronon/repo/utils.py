import json
import os
import re
import shutil
import subprocess
import tempfile
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from enum import Enum
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as ver

from click import style

from ai.chronon.cli.compile.parse_teams import EnvOrConfigAttribute
from ai.chronon.logger import get_logger
from ai.chronon.repo.constants import (
    APP_NAME_TEMPLATE,
    SCALA_VERSION_FOR_SPARK,
    SUPPORTED_SPARK,
)

LOG = get_logger()


class JobType(Enum):
    SPARK = "spark"
    FLINK = "flink"


def retry_decorator(retries=3, backoff=20):
    def wrapper(func):
        def wrapped(*args, **kwargs):
            attempt = 0
            while attempt <= retries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    attempt += 1
                    LOG.exception(e)
                    sleep_time = attempt * backoff
                    LOG.info(
                        "[{}] Retry: {} out of {}/ Sleeping for {}".format(
                            func.__name__, attempt, retries, sleep_time
                        )
                    )
                    time.sleep(sleep_time)
            return func(*args, **kwargs)

        return wrapped

    return wrapper


def get_environ_arg(env_name, ignoreError=False) -> str:
    value = os.environ.get(env_name)
    if not value and not ignoreError:
        raise ValueError(f"Please set {env_name} environment variable")
    return value


def get_default_warehouse_bucket() -> str:
    return f"zipline-warehouse-{get_customer_id()}"


def get_customer_id() -> str:
    return get_environ_arg("CUSTOMER_ID")


def extract_filename_from_path(path):
    return os.path.basename(path)


def check_call(cmd):
    LOG.info("Running command: " + cmd)
    return subprocess.check_call(cmd.split(), bufsize=0)


def check_output(cmd, timeout=None, streaming=False):
    LOG.info("Running command: " + cmd)
    proc = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, bufsize=1)
    output_lines = []
    deadline = (time.monotonic() + timeout) if timeout else None
    for line in iter(proc.stdout.readline, b""):
        if streaming:
            print(line.decode("utf-8", errors="replace").rstrip(), flush=True)
        output_lines.append(line)
        if deadline and time.monotonic() > deadline:
            proc.kill()
            proc.wait()
            raise subprocess.TimeoutExpired(cmd, timeout, output=b"".join(output_lines))
    remaining = max(0, deadline - time.monotonic()) if deadline else None
    try:
        proc.wait(timeout=remaining)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
        raise subprocess.TimeoutExpired(cmd, timeout, output=b"".join(output_lines)) from None
    if proc.returncode != 0:
        output = b"".join(output_lines)
        output_str = output.decode("utf-8", errors="replace")
        error_msg = f"Command '{cmd}' returned non-zero exit status {proc.returncode}.\n\nCommand output:\n{output_str}"
        LOG.error(error_msg)
        raise subprocess.CalledProcessError(proc.returncode, cmd, output=output, stderr=error_msg)
    return b"".join(output_lines).strip()


def custom_json(conf):
    """Extract the json stored in customJson for a conf."""
    if conf.get("metaData", {}).get("customJson"):
        return json.loads(conf["metaData"]["customJson"])
    return {}


def download_only_once(url, path, skip_download=False):
    if skip_download:
        LOG.info("Skipping download of " + path)
        return
    should_download = True
    path = path.strip()
    if os.path.exists(path):
        content_output = check_output("curl -sI " + url).decode("utf-8")
        content_length = re.search("(content-length:\\s)(\\d+)", content_output.lower())
        remote_size = int(content_length.group().split()[-1])
        local_size = os.path.getsize(path)
        LOG.info(
            """Files sizes of {url} vs. {path}
    Remote size: {remote_size}
    Local size : {local_size}""".format(**locals())
        )
        if local_size == remote_size:
            LOG.info("Sizes match. Assuming it's already downloaded.")
            should_download = False
        if should_download:
            LOG.info("Different file from remote at local: " + path + ". Re-downloading..")
            check_call("curl {} -o {} --connect-timeout 10".format(url, path))
    else:
        LOG.info("No file at: " + path + ". Downloading..")
        check_call("curl {} -o {} --connect-timeout 10".format(url, path))


# NOTE: this is only for the open source chronon. For the internal zipline version, we have a different jar to download.
@retry_decorator(retries=3, backoff=50)
def download_jar(
    version,
    jar_type="uber",
    release_tag=None,
    spark_version="2.4.0",
    skip_download=False,
):
    assert spark_version in SUPPORTED_SPARK, (
        f"Received unsupported spark version {spark_version}. "
        f"Supported spark versions are {SUPPORTED_SPARK}"
    )
    scala_version = SCALA_VERSION_FOR_SPARK[spark_version]
    maven_url_prefix = os.environ.get("CHRONON_MAVEN_MIRROR_PREFIX", None)
    default_url_prefix = "https://s01.oss.sonatype.org/service/local/repositories/public/content"
    url_prefix = maven_url_prefix if maven_url_prefix else default_url_prefix
    base_url = "{}/ai/chronon/spark_{}_{}".format(url_prefix, jar_type, scala_version)
    LOG.info("Downloading jar from url: " + base_url)
    jar_path = os.environ.get("CHRONON_DRIVER_JAR", None)
    if jar_path is None:
        if version == "latest":
            version = None
        if version is None:
            metadata_content = check_output("curl -s {}/maven-metadata.xml".format(base_url))
            meta_tree = ET.fromstring(metadata_content)
            versions = [
                node.text
                for node in meta_tree.findall("./versioning/versions/")
                if re.search(
                    r"^\d+\.\d+\.\d+{}$".format(
                        r"\_{}\d*".format(release_tag) if release_tag else ""
                    ),
                    node.text,
                )
            ]
            version = versions[-1]
        jar_url = (
            "{base_url}/{version}/spark_{jar_type}_{scala_version}-{version}-assembly.jar".format(
                base_url=base_url,
                version=version,
                scala_version=scala_version,
                jar_type=jar_type,
            )
        )
        jar_path = os.path.join(tempfile.gettempdir(), extract_filename_from_path(jar_url))
        download_only_once(jar_url, jar_path, skip_download)
    return jar_path


def set_runtime_env_v3(params, conf):
    effective_mode = params.get("mode")

    runtime_env = {"APP_NAME": params.get("app_name")}

    if params.get("repo") and conf and effective_mode:
        # get the conf file
        conf_path = os.path.join(params["repo"], conf)
        if os.path.isfile(conf_path):
            with open(conf_path, "r") as infile:
                conf_json = json.load(infile)
                metadata = (
                    conf_json.get("metaData", {}) or conf_json
                )  # user may just pass metadata as the entire json
                env = metadata.get("executionInfo", {}).get("env", {})
                runtime_env.update(
                    env.get(EnvOrConfigAttribute.ENV, {}).get(effective_mode, {})
                    or env.get("common", {})
                )
                # Also set APP_NAME
                try:
                    _, conf_type, team, _ = conf.split("/")[-4:]
                    if not team:
                        team = "default"
                    # context is the environment in which the job is running, which is provided from the args,
                    # default to be dev.
                    if params["env"]:
                        context = params["env"]
                    else:
                        context = "dev"
                    LOG.info(f"Context: {context} -- conf_type: {conf_type} -- team: {team}")

                    runtime_env["APP_NAME"] = APP_NAME_TEMPLATE.format(
                        mode=effective_mode,
                        conf_type=conf_type,
                        context=context,
                        name=conf_json["metaData"]["name"],
                    )
                except Exception:
                    LOG.warn(
                        "Failed to set APP_NAME due to invalid conf path: {}, please ensure to supply the "
                        "relative path to zipline/ folder".format(conf)
                    )
        else:
            if not params.get("app_name") and not os.environ.get("APP_NAME"):
                # Provide basic app_name when no conf is defined.
                # Modes like metadata-upload and metadata-export can rely on conf-type or folder rather than a conf.
                runtime_env["APP_NAME"] = "_".join(
                    [k for k in ["chronon", effective_mode.replace("-", "_")] if k is not None]
                )
    for key, value in runtime_env.items():
        if key not in os.environ and value is not None:
            LOG.info(f"Setting to environment: {key}={value}")
            print(f"Setting to environment: {key}={value}")
            os.environ[key] = value


def split_date_range(start_date, end_date, parallelism):
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")
    if start_date > end_date:
        raise ValueError("Start date should be earlier than end date")
    total_days = (end_date - start_date).days + 1  # +1 to include the end_date in the range

    # Check if parallelism is greater than total_days
    if parallelism > total_days:
        raise ValueError("Parallelism should be less than or equal to total days")

    split_size = total_days // parallelism
    date_ranges = []

    for i in range(parallelism):
        split_start = start_date + timedelta(days=i * split_size)
        if i == parallelism - 1:
            split_end = end_date
        else:
            split_end = split_start + timedelta(days=split_size - 1)
        date_ranges.append((split_start.strftime("%Y-%m-%d"), split_end.strftime("%Y-%m-%d")))
    return date_ranges


def get_metadata_name_from_conf(repo_path, conf_path):
    with open(os.path.join(repo_path, conf_path), "r") as conf_file:
        data = json.load(conf_file)
        return data.get("metaData", {}).get("name", None)


def read_from_blob_store(remote_uri: str) -> str | None:
    """Read a small text file from GCS, S3, Azure Blob Storage, or local filesystem.

    Returns the file contents as a string, or None if the file does not exist.
    """
    try:
        if remote_uri.startswith("gs://"):
            from google.cloud import storage

            parts = remote_uri[len("gs://"):].split("/", 1)
            bucket_name, blob_name = parts[0], parts[1] if len(parts) > 1 else ""
            client = storage.Client()
            blob = client.bucket(bucket_name).blob(blob_name)
            return blob.download_as_text()
        elif remote_uri.startswith("s3://"):
            import boto3
            from botocore.exceptions import ClientError

            parts = remote_uri[len("s3://"):].split("/", 1)
            bucket_name, key = parts[0], parts[1] if len(parts) > 1 else ""
            try:
                LOG.info(f"Checking S3: bucket={bucket_name}, key={key}")
                obj = boto3.client("s3").get_object(Bucket=bucket_name, Key=key)
                return obj["Body"].read().decode("utf-8")
            except ClientError as e:
                if e.response["Error"]["Code"] == "NoSuchKey":
                    LOG.debug(f"S3 object not found: {remote_uri}")
                    return None
                LOG.warning(f"S3 ClientError reading {remote_uri}: {e}")
                raise
        elif remote_uri.startswith("abfss://") or "blob.core.windows.net" in remote_uri:
            from azure.identity import DefaultAzureCredential
            from azure.storage.blob import BlobServiceClient

            account_url, container, blob_name = _parse_azure_uri(remote_uri)
            blob_service = BlobServiceClient(account_url=account_url, credential=DefaultAzureCredential())
            blob_client = blob_service.get_blob_client(container=container, blob=blob_name)
            return blob_client.download_blob().readall().decode("utf-8")
        else:
            if os.path.exists(remote_uri):
                with open(remote_uri) as f:
                    return f.read()
            return None
    except Exception as e:
        LOG.warning(f"Exception reading from blob store {remote_uri}: {e}")
        return None


def download_from_blob_store(remote_uri: str, local_path: str) -> None:
    """Download a binary file from GCS, S3, or Azure Blob Storage to a local path.

    Raises FileNotFoundError if the remote file does not exist.
    """
    if remote_uri.startswith("gs://"):
        from google.cloud import storage

        parts = remote_uri[len("gs://"):].split("/", 1)
        bucket_name, blob_name = parts[0], parts[1] if len(parts) > 1 else ""
        client = storage.Client()
        blob = client.bucket(bucket_name).blob(blob_name)
        blob.download_to_filename(local_path)
    elif remote_uri.startswith("s3://"):
        import boto3

        parts = remote_uri[len("s3://"):].split("/", 1)
        bucket_name, key = parts[0], parts[1] if len(parts) > 1 else ""
        boto3.client("s3").download_file(bucket_name, key, local_path)
    elif remote_uri.startswith("abfss://") or "blob.core.windows.net" in remote_uri:
        _download_from_azure_blob(remote_uri, local_path)
    elif os.path.exists(remote_uri):
        import shutil
        shutil.copy2(remote_uri, local_path)
    else:
        raise FileNotFoundError(f"Remote file not found: {remote_uri}")
    LOG.info(f"Downloaded {remote_uri} -> {local_path}")


def _parse_azure_uri(remote_uri: str):
    """Parse an Azure URI (abfss:// or https://blob.core.windows.net) into (account_url, container, blob_name)."""
    if remote_uri.startswith("abfss://"):
        # abfss://container@account.dfs.core.windows.net/path
        without_scheme = remote_uri[len("abfss://"):]
        container, rest = without_scheme.split("@", 1)
        host, blob_name = rest.split("/", 1)
        # BlobServiceClient works against the blob endpoint even for ADLS Gen2 accounts
        account_url = f"https://{host.replace('.dfs.core.windows.net', '.blob.core.windows.net')}"
    else:
        from urllib.parse import urlparse
        parsed = urlparse(remote_uri)
        account_url = f"{parsed.scheme}://{parsed.netloc}"
        path_parts = parsed.path.lstrip("/").split("/", 1)
        container = path_parts[0]
        blob_name = path_parts[1] if len(path_parts) > 1 else ""
    return account_url, container, blob_name


def _download_from_azure_blob(remote_uri: str, local_path: str) -> None:
    from azure.identity import DefaultAzureCredential
    from azure.storage.blob import BlobServiceClient

    account_url, container, blob_name = _parse_azure_uri(remote_uri)
    blob_service = BlobServiceClient(account_url=account_url, credential=DefaultAzureCredential())
    blob_client = blob_service.get_blob_client(container=container, blob=blob_name)
    with open(local_path, "wb") as f:
        blob_client.download_blob().readinto(f)


def upload_to_blob_store(local_path: str, remote_uri: str) -> str:
    """Upload a local file to GCS, S3, or Azure Blob Storage based on URI scheme.

    Returns the remote URI.
    """
    if remote_uri.startswith("gs://"):
        return _upload_to_gcs(local_path, remote_uri)
    elif remote_uri.startswith("s3://"):
        return _upload_to_s3(local_path, remote_uri)
    elif remote_uri.startswith("abfss://") or "blob.core.windows.net" in remote_uri:
        return _upload_to_azure_blob(local_path, remote_uri)
    else:
        # Local filesystem path
        os.makedirs(os.path.dirname(remote_uri), exist_ok=True)
        shutil.copy2(local_path, remote_uri)
        LOG.info(f"Copied {local_path} -> {remote_uri}")
        return remote_uri


def _upload_to_gcs(local_path: str, gcs_uri: str) -> str:
    from google.cloud import storage

    parts = gcs_uri[len("gs://") :].split("/", 1)
    bucket_name, blob_name = parts[0], parts[1] if len(parts) > 1 else ""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(local_path)
    LOG.info(f"Uploaded {local_path} -> {gcs_uri}")
    return gcs_uri


def _upload_to_s3(local_path: str, s3_uri: str) -> str:
    import boto3

    parts = s3_uri[len("s3://") :].split("/", 1)
    bucket_name, key = parts[0], parts[1] if len(parts) > 1 else ""
    boto3.client("s3").upload_file(local_path, bucket_name, key)
    LOG.info(f"Uploaded {local_path} -> {s3_uri}")
    return s3_uri


def _upload_to_azure_blob(local_path: str, azure_uri: str) -> str:
    from azure.identity import DefaultAzureCredential
    from azure.storage.blob import BlobServiceClient

    account_url, container, blob_name = _parse_azure_uri(azure_uri)
    blob_service = BlobServiceClient(account_url=account_url, credential=DefaultAzureCredential())
    blob_client = blob_service.get_blob_client(container=container, blob=blob_name)
    with open(local_path, "rb") as f:
        blob_client.upload_blob(f, overwrite=True)
    LOG.info(f"Uploaded {local_path} -> {azure_uri}")
    return azure_uri


def resolve_conf(repo, conf):
    """Resolve a conf path, automatically finding versioned conf files.

    If the exact conf path exists, return it as-is. Otherwise, look for files
    matching conf__<version> in the same directory. If exactly one match is found,
    return the resolved conf path. Raises ValueError if multiple matches are found,
    or returns the original conf unchanged if none are found (so downstream code
    can raise the normal FileNotFoundError).
    """
    conf_path = os.path.join(repo, conf)
    if os.path.isfile(conf_path):
        return conf

    conf_dir = os.path.dirname(conf_path)
    basename = os.path.basename(conf_path)
    prefix = basename + "__"

    if not os.path.isdir(conf_dir):
        return conf

    matches = [
        name for name in os.listdir(conf_dir)
        if name.startswith(prefix) and name[len(prefix):].isdigit()
    ]
    if len(matches) == 1:
        resolved = os.path.join(os.path.dirname(conf), matches[0])
        LOG.info(f"Resolved conf '{conf}' -> '{resolved}'")
        return resolved
    elif len(matches) > 1:
        raise ValueError(
            f"Ambiguous conf '{conf}': found multiple versioned files in "
            f"{style(conf_dir, fg='yellow')}:\n - " + "\n - ".join(sorted(matches))
        )

    return conf


def print_possible_confs(conf, repo, *args, **kwargs):
    conf_location = os.path.join(repo, conf)
    conf_dirname = os.path.dirname(conf_location)
    if os.path.exists(conf_dirname):
        print(
            f"Possible confs from {style(conf_dirname, fg='yellow')}: \n -",
            "\n - ".join([name for name in os.listdir(conf_dirname)]),
        )
    else:
        print(f"Directory does not exist: {style(conf_dirname, fg='yellow')}")


def blob_exists(remote_uri: str) -> bool:
    """Check if a blob exists in GCS, S3, Azure Blob Storage, or local filesystem.

    Returns True if the blob exists, False otherwise.
    """
    try:
        if remote_uri.startswith("gs://"):
            from google.cloud import storage

            parts = remote_uri[len("gs://"):].split("/", 1)
            bucket_name, blob_name = parts[0], parts[1] if len(parts) > 1 else ""
            client = storage.Client()
            blob = client.bucket(bucket_name).blob(blob_name)
            try:
                return blob.exists()
            except Exception as e:
                LOG.warning(
                    f"GCS exists check failed for {remote_uri}; falling back to list-based check: {e}"
                )
                matches = client.list_blobs(bucket_name, prefix=blob_name, max_results=1)
                return any(candidate.name == blob_name for candidate in matches)
        elif remote_uri.startswith("s3://"):
            import boto3
            from botocore.exceptions import ClientError

            parts = remote_uri[len("s3://"):].split("/", 1)
            bucket_name, key = parts[0], parts[1] if len(parts) > 1 else ""
            if "s3://" in key:
                key = key.split("s3://", 1)[1]
            try:
                boto3.client("s3").head_object(Bucket=bucket_name, Key=key)
                return True
            except ClientError as e:
                if e.response["Error"]["Code"] == "404":
                    return False
                LOG.warning(f"S3 error checking existence of {remote_uri}: {e}")
                return False
        elif remote_uri.startswith("abfss://") or "blob.core.windows.net" in remote_uri:
            from azure.identity import DefaultAzureCredential
            from azure.storage.blob import BlobServiceClient

            account_url, container, blob_name = _parse_azure_uri(remote_uri)
            blob_service = BlobServiceClient(account_url=account_url, credential=DefaultAzureCredential())
            blob_client = blob_service.get_blob_client(container=container, blob=blob_name)
            return blob_client.exists()
        else:
            return os.path.exists(remote_uri)
    except Exception as e:
        LOG.warning(f"Exception checking blob existence {remote_uri}: {e}")
        return False


def get_package_version():
    try:
        package_version = ver("zipline-ai")
    except PackageNotFoundError:
        package_version = "unknown"
    return package_version

