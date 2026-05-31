"""Zipline admin CLI commands for loading images into customer registries and diagnosing deployments."""

import json
import logging
import os
import shutil
import subprocess
import tarfile
import tempfile
import traceback
from functools import partial

import click
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)
from rich.table import Table

from ai.chronon.cli.theme import STYLE_ERROR, STYLE_SUCCESS, console
from ai.chronon.repo.admin_utils import print_check_table, run_infra_checks
from ai.chronon.repo.constants import (
    FLINK_IMAGE_TAG,
    SPARK_3_5_3_VERSION,
    VALID_CLOUDS,
    get_public_spark_jars_for_admin,
)
from ai.chronon.repo.hub_runner import (
    format_option,
    hub_url_option,
    redeploy_streaming,
    repo_option,
    use_auth_option,
)
from ai.chronon.repo.registry_client import (
    DOCKER_HUB_REGISTRY,
    ImageTarget,
    RegistryClient,
    RegistryError,
)
from ai.chronon.repo.utils import (
    blob_exists,
    get_package_version,
    read_from_blob_store,
    upload_to_blob_store,
)

logger = logging.getLogger(__name__)


def _safe_extractall(tar, dest):
    """Extract tar members after validating no paths escape *dest* (path traversal guard)."""
    dest = os.path.realpath(dest)
    for member in tar.getmembers():
        member_path = os.path.realpath(os.path.join(dest, member.name))
        if not member_path.startswith(dest + os.sep) and member_path != dest:
            raise RuntimeError(f"Tar member {member.name!r} would escape destination directory")
    tar.extractall(dest)


def _app_images(cloud, release):
    """Return the list of (image_type, repo, tag) tuples for application images (excludes engine)."""
    images = [
        ("hub", f"ziplineai/hub-{cloud}", release),
        ("eval", f"ziplineai/eval-{cloud}", release),
        ("frontend", "ziplineai/web-ui", release),
        ("fetcher", "ziplineai/chronon-fetcher", release),
    ]
    # GCP submits jobs to Dataproc, so no custom Flink image is needed
    if cloud != "gcp":
        # Flink uses a fixed tag independent of the Zipline release
        images.append(("flink", "ziplineai/flink", FLINK_IMAGE_TAG))
    return images


def _get_current_kube_context() -> str:
    """Return the active kubectl context, or exit with a helpful message if
    kubectl is missing or unconfigured. Read once at command start so the
    detected context is what gets shown in the confirmation prompt; the
    actual kubectl calls inherit the ambient context as usual."""
    if not shutil.which("kubectl"):
        console.print(
            "[red]kubectl not found.[/red]\n"
            "Install kubectl (https://kubernetes.io/docs/tasks/tools/) and retry."
        )
        raise SystemExit(1)
    result = subprocess.run(
        ["kubectl", "config", "current-context"],
        capture_output=True, text=True,
    )
    if result.returncode != 0 or not result.stdout.strip():
        console.print(
            "[red]No active kubectl context.[/red]\n"
            "Configure one and retry:\n"
            "  aws eks update-kubeconfig --name <cluster-name> --region <region>"
        )
        raise SystemExit(1)
    return result.stdout.strip()


def _confirm_kube_context(context: str, assume_yes: bool = False) -> None:
    """Show the detected kubectl context and prompt for confirmation. Aborts
    on No. Skipped when `assume_yes` is true (for CI / scripted use)."""
    console.print(f"About to run against kubectl context: [bold]{context}[/bold]")
    if assume_yes:
        return
    if not click.confirm("Proceed?", default=False):
        console.print("[yellow]Aborted by user.[/yellow]")
        raise SystemExit(1)


def assume_yes_option(func):
    return click.option(
        "-y", "--yes", "assume_yes",
        is_flag=True,
        default=False,
        help="Skip the kubectl-context confirmation prompt. Use for scripts/CI.",
    )(func)


# EKS service → (deployment_name, container_name, image_repo_template)
_EKS_SERVICES = {
    "hub": ("zipline-orchestration-hub", "orchestration-hub", "ziplineai/hub-{cloud}"),
    "ui": ("zipline-orchestration-ui", "web-ui", "ziplineai/web-ui"),
    "eval": ("zipline-eval", "eval", "ziplineai/eval-{cloud}"),
    "fetcher": ("zipline-fetcher", "fetcher", "ziplineai/chronon-fetcher"),
}
_EKS_NAMESPACE = "zipline-system"
_EKS_ROLLOUT_TIMEOUT = 300


def _get_current_image(deployment, container):
    """Return the current image for a container in a deployment, or None on failure."""
    result = subprocess.run(
        [
            "kubectl", "get", "deployment", deployment,
            "--namespace", _EKS_NAMESPACE,
            "-o", f"jsonpath={{.spec.template.spec.containers[?(@.name==\"{container}\")].image}}",
        ],
        capture_output=True, text=True,
    )
    if result.returncode == 0 and result.stdout.strip():
        return result.stdout.strip()
    return None


def _upgrade_eks_services(cloud, release):
    """Upgrade running EKS deployments to the new release. Skips services that don't exist."""
    if not shutil.which("kubectl"):
        console.print(
            "[red]kubectl not found.[/red]\n"
            "Install kubectl (https://kubernetes.io/docs/tasks/tools/) and retry."
        )
        raise SystemExit(1)

    result = subprocess.run(
        ["kubectl", "cluster-info", "--namespace", _EKS_NAMESPACE],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        console.print(
            "[red]Cannot reach Kubernetes cluster.[/red]\n"
            "Configure your kubeconfig and retry:\n"
            "  aws eks update-kubeconfig --name <cluster-name> --region <region>"
        )
        raise SystemExit(1)

    console.print(f"\n[bold]Upgrading EKS services in namespace {_EKS_NAMESPACE}...[/bold]")
    results = []
    has_failures = False

    for service, (deployment, container, image_template) in _EKS_SERVICES.items():
        image = image_template.format(cloud=cloud) + f":{release}"

        check = subprocess.run(
            ["kubectl", "get", "deployment", deployment, "--namespace", _EKS_NAMESPACE],
            capture_output=True, text=True,
        )
        if check.returncode != 0:
            console.print(
                f"  [dim]{service}: deployment/{deployment} not found — "
                f"skipping (this is expected if the service has not been deployed yet)[/dim]"
            )
            continue

        current_image = _get_current_image(deployment, container)
        current_tag = current_image.rsplit(":", 1)[-1] if current_image else "unknown"
        image_repo = image_template.format(cloud=cloud)

        if current_image == image:
            console.print(f"  [bold]{service}[/bold]: already on {release} — restarting")
            restart_cmd = ["kubectl", "rollout", "restart", f"deployment/{deployment}", "--namespace", _EKS_NAMESPACE]
            restart_result = subprocess.run(restart_cmd, capture_output=True, text=True)
            if restart_result.returncode != 0:
                results.append((service, image_repo, current_tag, release, "FAIL", restart_result.stderr.strip()))
                has_failures = True
                continue
        else:
            console.print(f"  [bold]{service}[/bold]: {current_tag} → {release}")
            set_cmd = ["kubectl", "set", "image", f"deployment/{deployment}", f"{container}={image}", "--namespace", _EKS_NAMESPACE]
            set_result = subprocess.run(set_cmd, capture_output=True, text=True)
            if set_result.returncode != 0:
                results.append((service, image_repo, current_tag, release, "FAIL", set_result.stderr.strip()))
                has_failures = True
                continue

        console.print("    waiting for rollout to complete...")
        rollout_cmd = ["kubectl", "rollout", "status", f"deployment/{deployment}", f"--timeout={_EKS_ROLLOUT_TIMEOUT}s", "--namespace", _EKS_NAMESPACE]
        rollout = subprocess.run(rollout_cmd, capture_output=True, text=True)
        if rollout.returncode == 0:
            detail = "restart complete" if current_image == image else "rollout complete"
            results.append((service, image_repo, current_tag, release, "ok", detail))
        else:
            results.append((service, image_repo, current_tag, release, "FAIL", rollout.stderr.strip()))
            has_failures = True

    if results:
        table = Table(title="EKS Service Upgrade")
        table.add_column("Service", style="cyan")
        table.add_column("Image", style="white")
        table.add_column("Previous", style="dim")
        table.add_column("Current", style="white")
        table.add_column("Status")
        table.add_column("Detail", style="white")
        for service, image_repo, prev_ver, cur_ver, status, detail in results:
            style = "green" if status == "ok" else "red"
            table.add_row(service, image_repo, prev_ver, cur_ver, f"[{style}]{status}[/{style}]", detail)
        console.print(table)

    if has_failures:
        console.print("\n[yellow]Some services failed to upgrade. Check the errors above and retry.[/yellow]")
        raise SystemExit(1)


def _parse_registry(registry):
    """Parse a registry URL into (host, repo_prefix)."""
    from urllib.parse import urlparse

    parsed = urlparse(registry if "://" in registry else f"https://{registry}")
    host = parsed.hostname or ""
    prefix = parsed.path.strip("/")
    return host, prefix


# ── CLI commands ──────────────────────────────────────────────────────


@click.group(
    help="Administrative commands for initializing repos, loading images, and diagnosing deployments."
)
def admin():
    pass


@admin.command("install")
@click.argument("cloud", type=click.Choice(VALID_CLOUDS, case_sensitive=False))
@click.option(
    "--registry",
    default="local",
    show_default=True,
    help="Target registry URL (e.g. us-docker.pkg.dev/project/repo) or 'local' for the local Docker daemon.",
)
@click.option(
    "--api-token",
    envvar="ZIPLINE_API_TOKEN",
    default=None,
    help="Zipline API token for Docker Hub access. Can also be set via ZIPLINE_API_TOKEN env var. "
    "Optional when using --bundle or when already authenticated to Docker Hub.",
)
@click.option(
    "--release", default=None, help="Zipline release to load (e.g. 0.1.42). Defaults to the installed zipline-ai package version."
)
@click.option(
    "--artifact-prefix",
    default=None,
    help="Target prefix for engine JARs: a blob store URI (e.g. gs://bucket/zipline) or a local filesystem path.",
)
@click.option(
    "--bundle",
    default=None,
    type=click.Path(exists=True),
    help="Path to air-gap tarball (alternative to pulling from Docker Hub).",
)
def install(cloud, registry, api_token, release, artifact_prefix, bundle):
    """Install Zipline images into a private registry or the local Docker daemon.

    CLOUD is the cloud provider variant (gcp, aws, or azure).
    """
    for name in ("urllib3", "ai.chronon.logger"):
        logging.getLogger(name).setLevel(logging.WARNING)

    if release is None:
        release = get_package_version()
        if release == "unknown":
            release = "latest"
        console.print(f"Using release [bold]{release}[/bold]")
    else:
        pkg_version = get_package_version()
        # Normalize away any leading "v" before comparing since PyPI versions omit it.
        if pkg_version != "unknown" and release.lstrip("v") != pkg_version.lstrip("v"):
            if not click.confirm(
                f"Specified release {release} does not match installed zipline cli version {pkg_version}. Continue?"
            ):
                raise SystemExit(1)

    if registry == "local":
        _check_docker_available()
        target = ImageTarget()
    else:
        client = RegistryClient()
        host, prefix = _parse_registry(registry)
        target = ImageTarget(client, host, prefix)

    with _make_progress() as progress:
        if bundle:
            results = _load_from_bundle(target, bundle, release, cloud, progress)
        else:
            results = _load_from_docker_hub(target, api_token, release, cloud, progress)

        if artifact_prefix:
            console.print(f"\n[bold]Uploading artifacts to {artifact_prefix}...[/bold]")
            if bundle:
                console.print("[bold]Extracting engine JARs from bundle...[/bold]")
                jar_results = _extract_engine_jars_from_bundle(
                    bundle, cloud, release, artifact_prefix, progress
                )
            else:
                console.print(f"[bold]Downloading and extracting engine image (ziplineai/engine-{cloud}:{release})...[/bold]")
                hub_client = RegistryClient()
                _authenticate_docker_hub(hub_client, api_token)
                jar_results = _upload_engine_jars_to_store(
                    hub_client, DOCKER_HUB_REGISTRY, release, cloud, artifact_prefix, progress
                )
            results.extend(jar_results)

            # Download and upload public Maven jars
            console.print("\n[bold]Processing public Maven dependencies...[/bold]")
            public_jar_results = _download_and_upload_public_jars(artifact_prefix, progress)
            results.extend(public_jar_results)

    _print_summary(results, release, cloud, registry)


# ── Progress helpers ──────────────────────────────────────────────────


def _make_progress():
    """Create a configured rich Progress bar."""
    return Progress(
        SpinnerColumn(),
        TextColumn("[bold]{task.description}"),
        BarColumn(),
        DownloadColumn(),
        TransferSpeedColumn(),
        TimeRemainingColumn(),
        console=console,
    )


def _advance_progress(progress, task_id, n):
    """Callback for on_progress: advance a rich progress bar task by *n* bytes."""
    progress.update(task_id, advance=n)


def _update_status(progress, task_id, base_label, layer_sizes, line):
    """Callback for on_status: parse Docker pull/load output and advance progress.

    Docker non-TTY output emits lines like ``a2abf6c4d29d: Pull complete``.
    We advance the progress bar by one layer's worth of bytes per completed layer.
    """
    parts = line.split(": ", 1)
    if len(parts) == 2:
        _layer_id, status = parts
        if status in ("Pull complete", "Already exists"):
            if layer_sizes:
                progress.update(task_id, advance=layer_sizes.pop(0))
        elif status not in ("Waiting", "Pulling fs layer"):
            progress.update(task_id, description=f"{base_label}: {status[:40]}")
    elif "Digest:" in line or "Status:" in line:
        progress.update(task_id, description=base_label)


def _finish_task(progress, task_id, label, ok):
    """Remove a progress task and print a completion or failure line."""
    progress.remove_task(task_id)
    if ok:
        progress.console.print(f"[{STYLE_SUCCESS}] ✅ SUCCESS [/] {label}")
    else:
        progress.console.print(f"[{STYLE_ERROR}] 🔴 FAILED [/] {label}")


def _make_target_with_progress(target, progress, task_id, action_label, layer_sizes=None):
    """Create an ImageTarget copy wired to a progress bar task."""
    if target.is_local:
        return target.with_callbacks(
            on_status=partial(_update_status, progress, task_id, action_label, layer_sizes or []),
        )
    return target.with_callbacks(
        on_progress=partial(_advance_progress, progress, task_id),
    )


# ── Image loading ─────────────────────────────────────────────────────


def _load_from_docker_hub(target, api_token, release, cloud, progress):
    """Pull images from Docker Hub."""
    hub_client = None
    if target.is_local:
        if api_token:
            console.print("[bold]Logging in to Docker Hub...[/bold]")
            proc = subprocess.run(
                ["docker", "login", "-u", "ziplineai", "--password-stdin"],
                input=api_token,
                capture_output=True,
                text=True,
            )
            if proc.returncode != 0:
                console.print(
                    f"[yellow]Warning: docker login failed: {proc.stderr.strip()}[/yellow]"
                )
        # OCI client for querying image sizes (anonymous access suffices for public images)
        hub_client = RegistryClient()
        try:
            _authenticate_docker_hub(hub_client, api_token)
        except (click.UsageError, RegistryError):
            hub_client.authenticate(DOCKER_HUB_REGISTRY)
    else:
        _authenticate_docker_hub(target.client, api_token)
        _authenticate_target_registry(target.client, target.registry_host)

    results = []
    size_client = hub_client if target.is_local else target.client
    for image_type, repo, tag in _app_images(cloud, release):
        action = "Pulling" if target.is_local else "Copying"
        label = f"{repo}:{tag}"

        total_bytes, layer_sizes = None, []
        if size_client:
            try:
                if target.is_local:
                    total_bytes, layer_sizes = size_client.get_layer_sizes(
                        DOCKER_HUB_REGISTRY, repo, tag
                    )
                else:
                    total_bytes = size_client.get_total_image_size(
                        DOCKER_HUB_REGISTRY, repo, tag
                    )
            except RegistryError:
                pass

        task_id = progress.add_task(f"{action} {label}", total=total_bytes)
        img_target = _make_target_with_progress(
            target, progress, task_id, f"{action} {label}", layer_sizes
        )

        try:
            digest = img_target.copy_from_hub(repo, tag)
            _finish_task(progress, task_id, label, ok=True)
            results.append((image_type, target.ref(repo, tag), digest, "ok"))
        except RegistryError as e:
            _finish_task(progress, task_id, label, ok=False)
            console.print(f"[{STYLE_ERROR}]Error loading {label}:[/]\n{traceback.format_exc()}")
            results.append((image_type, target.ref(repo, tag), "", f"FAILED: {e}"))
    return results


def _get_bundle_image_size(archive_path):
    """Return total bytes of all layer files in an OCI archive."""
    with tarfile.open(archive_path, "r") as tar:
        manifest_member = tar.getmember("manifest.json")
        with tar.extractfile(manifest_member) as f:
            archive_manifests = json.load(f)
        if not archive_manifests:
            return 0
        entry = archive_manifests[0]
        total = 0
        for layer_rel in entry.get("Layers", []):
            try:
                member = tar.getmember(layer_rel)
                total += member.size
            except KeyError:
                pass
        return total


def _load_from_bundle(target, bundle_path, release, cloud, progress):
    """Load images from an air-gap tarball."""
    if not target.is_local:
        _authenticate_target_registry(target.client, target.registry_host)

    results = []
    with tempfile.TemporaryDirectory() as tmpdir:
        console.print(f"[bold]Extracting bundle {bundle_path}...[/bold]")
        with tarfile.open(bundle_path, "r:gz") as tar:
            _safe_extractall(tar, tmpdir)

        for _image_type, repo, tag in _app_images(cloud, release):
            image_name = repo.split("/")[-1]
            archive_path = os.path.join(tmpdir, f"{image_name}.tar")
            if not os.path.exists(archive_path):
                results.append(
                    (image_name, "", "", f"FAILED: {image_name}.tar not found in bundle")
                )
                continue

            action = "Pushing" if not target.is_local else "Loading"
            label = f"{image_name}:{tag}"

            try:
                total_bytes = _get_bundle_image_size(archive_path)
            except Exception:
                total_bytes = None

            task_id = progress.add_task(f"{action} {label}", total=total_bytes)
            img_target = _make_target_with_progress(target, progress, task_id, f"{action} {label}")

            try:
                digest = img_target.load_archive(archive_path, repo, tag)
                _finish_task(progress, task_id, label, ok=True)
                results.append((image_name, target.ref(repo, tag), digest, "ok"))
            except RegistryError as e:
                _finish_task(progress, task_id, label, ok=False)
                console.print(f"[{STYLE_ERROR}]Error loading {label}:[/]\n{traceback.format_exc()}")
                results.append((image_name, target.ref(repo, tag), "", f"FAILED: {e}"))
    return results


# ── Engine JAR extraction ─────────────────────────────────────────────


def _extract_jars_from_layer(layer_path, dest_dir):
    """Extract .jar and .json files from a single tar layer."""
    try:
        with tarfile.open(layer_path, "r") as tar:
            for member in tar.getmembers():
                if member.name.endswith(".jar") or member.name.endswith(".json"):
                    tar.extract(member, dest_dir)
    except tarfile.ReadError:
        pass


def _should_update_latest(release, artifact_store):
    """Check whether release is >= the current latest version in the artifact store."""
    from packaging.version import InvalidVersion, Version

    try:
        current = Version(release.lstrip("v"))
    except InvalidVersion:
        return False

    version_marker = f"{artifact_store.rstrip('/')}/release/latest/VERSION"
    existing = read_from_blob_store(version_marker)
    if existing is None:
        return True
    try:
        return current >= Version(existing.strip().lstrip("v"))
    except InvalidVersion:
        return True


def _extract_scripts_from_layer(layer_path, dest_dir, cloud):
    """Extract files from scripts/<cloud>/ directory in a single tar layer."""
    try:
        with tarfile.open(layer_path, "r") as tar:
            scripts_prefix = f"scripts/{cloud}/"
            for member in tar.getmembers():
                if member.name.startswith(scripts_prefix):
                    tar.extract(member, dest_dir)
    except tarfile.ReadError:
        pass


def _upload_jars_to_store(jars_dir, release, artifact_store):
    """Upload all files in jars_dir to the artifact store (versioned + latest)."""
    results = []
    if not os.path.isdir(jars_dir):
        console.print(f"    No JARs found in {jars_dir}")
        return results

    jar_files = os.listdir(jars_dir)
    console.print(f"    Found {len(jar_files)} JAR(s) to upload")

    update_latest = _should_update_latest(release, artifact_store)
    if update_latest:
        logger.info(f"Will update release/latest/ (release={release})")
    else:
        logger.info(f"Skipping release/latest/ update (release={release} is older than current latest)")

    for idx, jar_file in enumerate(jar_files, 1):
        local_path = os.path.join(jars_dir, jar_file)
        remote_path = f"{artifact_store.rstrip('/')}/release/{release}/jars/{jar_file}"
        try:
            console.print(f"    [{idx}/{len(jar_files)}] Uploading {jar_file}...")
            upload_to_blob_store(local_path, remote_path)
            if update_latest:
                latest_path = f"{artifact_store.rstrip('/')}/release/latest/jars/{jar_file}"
                upload_to_blob_store(local_path, latest_path)
            console.print(f"    [{idx}/{len(jar_files)}] ✓ {jar_file}")
            results.append(("jar", remote_path, "", "ok"))
        except Exception as e:
            console.print(f"[{STYLE_ERROR}]Error uploading {jar_file}:[/]\n{traceback.format_exc()}")
            results.append(("jar", remote_path, "", f"FAILED: {e}"))

    # Write VERSION marker so future installs can compare
    if update_latest and results and all(s == "ok" for _, _, _, s in results):
        try:
            with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
                f.write(release.lstrip("v"))
                tmp_path = f.name
            version_marker = f"{artifact_store.rstrip('/')}/release/latest/VERSION"
            upload_to_blob_store(tmp_path, version_marker)
        except Exception:
            logger.warning("Failed to write VERSION marker to latest/", exc_info=True)
        finally:
            os.unlink(tmp_path)

    return results


def _upload_scripts_to_store(scripts_dir, cloud, release, artifact_store):
    """Upload all files from scripts/<cloud>/ directory to the artifact store."""
    results = []
    scripts_cloud_dir = os.path.join(scripts_dir, "scripts", cloud)
    update_latest = _should_update_latest(release, artifact_store)
    if os.path.isdir(scripts_cloud_dir):
        # Count total scripts first
        total_scripts = sum(len(files) for _, _, files in os.walk(scripts_cloud_dir))
        console.print(f"    Found {total_scripts} script(s) to upload")

        for root, _dirs, files in os.walk(scripts_cloud_dir):
            for script_file in files:
                local_path = os.path.join(root, script_file)
                rel_path = os.path.relpath(local_path, scripts_cloud_dir)
                remote_path = f"{artifact_store.rstrip('/')}/release/{release}/scripts/{cloud}/{rel_path}"
                try:
                    upload_to_blob_store(local_path, remote_path)
                    if update_latest:
                        latest_path = (f"{artifact_store.rstrip('/')}/release/latest/scripts/{cloud}/{rel_path}")
                        upload_to_blob_store(local_path, latest_path)
                    results.append(("script", remote_path, "", "ok"))
                except Exception as e:
                    console.print(f"[{STYLE_ERROR}]Error uploading {script_file}:[/]\n{traceback.format_exc()}")
                    results.append(("script", remote_path, "", f"FAILED: {e}"))
    else:
        console.print(f"    No scripts found in {scripts_cloud_dir}")
    return results


def _download_and_upload_public_jars(artifact_store, progress):
    """Download public Maven jars and upload them to the artifact store.

    Checks which jars already exist in the artifact store and only downloads missing ones.
    """
    import urllib.request

    results = []

    spark_version = SPARK_3_5_3_VERSION
    console.print(f"[bold]Processing public Maven dependencies for Spark {spark_version}...[/bold]")

    # First, check which jars already exist
    console.print(f"[bold]Checking for existing jars in {artifact_store}/spark-{spark_version}/libs/...[/bold]")
    jars_to_download = []

    public_jars = get_public_spark_jars_for_admin(spark_version)
    for jar_url in public_jars:
        jar_filename = jar_url.split("/")[-1]

        remote_path = f"{artifact_store.rstrip('/')}/spark-{SPARK_3_5_3_VERSION}/libs/{jar_filename}"

        console.print(f"Checking for {remote_path}")
        if blob_exists(remote_path):
            console.print(f"  ✓ {jar_filename} already exists in artifact store")
            results.append(("maven-jar", remote_path, "", "already exists"))
        else:
            console.print(f"  ✗ {jar_filename} not found in artifact store, will download and upload")
            jars_to_download.append((jar_url, jar_filename, remote_path))

    if not jars_to_download:
        console.print(f"[green]All {len(public_jars)} Maven jars already exist in artifact store[/green]")
        return results

    console.print(f"[bold]Downloading and uploading {len(jars_to_download)} missing jars...[/bold]")
    label = f"public Maven dependencies ({len(jars_to_download)}/{len(public_jars)} missing)"
    task_id = progress.add_task(f"Processing {label}", total=len(jars_to_download))

    with tempfile.TemporaryDirectory() as tmpdir:
        for idx, (jar_url, jar_filename, remote_path) in enumerate(jars_to_download, 1):
            local_path = os.path.join(tmpdir, jar_filename)

            try:
                console.print(f"  [{idx}/{len(jars_to_download)}] Downloading {jar_filename}...")
                urllib.request.urlretrieve(jar_url, local_path)
                console.print(f"  [{idx}/{len(jars_to_download)}] Uploading {jar_filename} to artifact store...")
                upload_to_blob_store(local_path, remote_path)
                console.print(f"  [{idx}/{len(jars_to_download)}] ✓ {jar_filename}")

                progress.update(task_id, advance=1)
                results.append(("maven-jar", remote_path, "", "ok"))
            except Exception as e:
                progress.update(task_id, advance=1)
                console.print(f"[{STYLE_ERROR}]Error downloading/uploading {jar_filename}:[/]\n{traceback.format_exc()}")
                results.append(("maven-jar", jar_url, "", f"FAILED: {e}"))

    ok = all(status in ("ok", "already exists") for _, _, _, status in results)
    _finish_task(progress, task_id, label, ok=ok)
    return results


def _extract_jars_from_oci_archive(archive_path, cloud, release, artifact_store):
    """Extract JARs and scripts from an OCI image archive (docker save format) and upload to the artifact store."""
    results = []

    with tempfile.TemporaryDirectory() as tmpdir:
        oci_dir = os.path.join(tmpdir, "oci")
        with tarfile.open(archive_path, "r") as tar:
            _safe_extractall(tar, oci_dir)

        manifest_path = os.path.join(oci_dir, "manifest.json")
        with open(manifest_path) as f:
            archive_manifests = json.load(f)

        if not archive_manifests:
            results.append(
                ("engine-jars", artifact_store, "", "FAILED: no manifests in engine archive")
            )
            return results

        entry = archive_manifests[0]

        extract_tmpdir = os.path.join(tmpdir, "extract")
        os.makedirs(extract_tmpdir, exist_ok=True)

        for layer_file_rel in entry["Layers"]:
            layer_path = os.path.join(oci_dir, layer_file_rel)
            _extract_jars_from_layer(layer_path, extract_tmpdir)
            _extract_scripts_from_layer(layer_path, extract_tmpdir, cloud)

        results.extend(
            _upload_jars_to_store(os.path.join(extract_tmpdir, "jars"), release, artifact_store)
        )
        results.extend(
            _upload_scripts_to_store(extract_tmpdir, cloud, release, artifact_store)
        )

    return results


def _extract_engine_jars_from_bundle(bundle_path, cloud, release, artifact_store, progress):
    """Extract engine JARs and scripts from a bundle's engine image archive and copy them to the artifact store."""
    label = f"engine artifacts (engine-{cloud}:{release})"
    task_id = progress.add_task(f"Extracting {label}", total=None)

    with tempfile.TemporaryDirectory() as tmpdir:
        with tarfile.open(bundle_path, "r:gz") as tar:
            _safe_extractall(tar, tmpdir)

        engine_archive = os.path.join(tmpdir, f"engine-{cloud}.tar")
        if not os.path.exists(engine_archive):
            _finish_task(progress, task_id, label, ok=False)
            return [
                (
                    "engine-artifacts",
                    artifact_store,
                    "",
                    f"FAILED: engine-{cloud}.tar not found in bundle",
                )
            ]

        results = _extract_jars_from_oci_archive(engine_archive, cloud, release, artifact_store)

    ok = all(status == "ok" for _, _, _, status in results)
    _finish_task(progress, task_id, label, ok=ok)
    return results


def _upload_engine_jars_to_store(client, registry, release, cloud, artifact_store, progress):
    """Extract JARs and scripts from the engine image and upload to a blob store."""
    results = []
    engine_repo = f"ziplineai/engine-{cloud}"
    label = f"engine artifacts ({engine_repo}:{release})"

    console.print(f"  Resolving manifest for {engine_repo}:{release}...")
    try:
        manifest = client.resolve_single_platform(registry, engine_repo, release)
    except RegistryError as e:
        console.print(f"[{STYLE_ERROR}]Error resolving {engine_repo}:{release}:[/]\n{traceback.format_exc()}")
        results.append(("engine-artifacts", artifact_store, "", f"FAILED: {e}"))
        return results

    num_layers = len(manifest.get("layers", []))
    total_bytes = sum(layer.get("size", 0) for layer in manifest.get("layers", []))
    console.print(f"  Found {num_layers} layers ({total_bytes / (1024*1024):.1f} MB total)")
    task_id = progress.add_task(f"Extracting {label}", total=total_bytes)

    with tempfile.TemporaryDirectory() as tmpdir:
        for layer in manifest.get("layers", []):
            layer_digest = layer["digest"]
            layer_path = os.path.join(tmpdir, "layer.tar")
            client.extract_blob_to_file(
                registry,
                engine_repo,
                layer_digest,
                layer_path,
                on_progress=partial(_advance_progress, progress, task_id),
            )
            _extract_jars_from_layer(layer_path, tmpdir)
            _extract_scripts_from_layer(layer_path, tmpdir, cloud)

        console.print(f"  Uploading engine JARs to {artifact_store}...")
        results.extend(_upload_jars_to_store(os.path.join(tmpdir, "jars"), release, artifact_store))
        console.print(f"  Uploading scripts to {artifact_store}...")
        results.extend(_upload_scripts_to_store(tmpdir, cloud, release, artifact_store))

    ok = all(status == "ok" for _, _, _, status in results)
    _finish_task(progress, task_id, label, ok=ok)
    return results


# ── Auth helpers ──────────────────────────────────────────────────────


def _check_docker_available():
    """Verify that the Docker CLI is on PATH and the daemon is running."""
    if not shutil.which("docker"):
        raise click.UsageError(
            "Docker CLI not found on PATH. Install Docker to use --registry local."
        )
    try:
        subprocess.run(["docker", "info"], capture_output=True, check=True)
    except subprocess.CalledProcessError as exc:
        raise click.UsageError(
            "Docker daemon is not running. Start Docker to use --registry local."
        ) from exc


def _authenticate_docker_hub(client, api_token):
    """Authenticate the OCI client against Docker Hub using an API token or local Docker credentials."""
    if api_token:
        client.authenticate(DOCKER_HUB_REGISTRY, username="ziplineai", password=api_token)
        return

    username, password = _get_docker_credentials(DOCKER_HUB_REGISTRY)
    if username and password:
        client.authenticate(DOCKER_HUB_REGISTRY, username=username, password=password)
        return

    raise click.UsageError(
        "Docker Hub credentials not found. Provide --api-token or log in with 'docker login'."
    )


def _base_domain(server):
    """Extract base domain from a Docker config server key (e.g. 'https://index.docker.io/v1/' -> 'docker.io')."""
    from urllib.parse import urlparse

    parsed = urlparse(server if "://" in server else f"https://{server}")
    hostname = parsed.hostname or ""
    parts = hostname.rsplit(".", 2)
    return ".".join(parts[-2:]) if len(parts) >= 2 else hostname


def _get_docker_credentials(registry):
    """Read credentials for a registry from the local Docker credential store (~/.docker/config.json)."""
    config_path = os.path.join(os.path.expanduser("~"), ".docker", "config.json")
    if not os.path.exists(config_path):
        return None, None

    with open(config_path) as f:
        config = json.load(f)

    registry_domain = _base_domain(registry)

    for server, helper in config.get("credHelpers", {}).items():
        if _base_domain(server) == registry_domain:
            username, password = _creds_from_helper(helper, server)
            if username:
                return username, password

    creds_store = config.get("credsStore")
    if creds_store:
        for server in config.get("auths", {}):
            if _base_domain(server) == registry_domain:
                username, password = _creds_from_helper(creds_store, server)
                if username:
                    return username, password

    import base64

    for server, entry in config.get("auths", {}).items():
        if _base_domain(server) == registry_domain:
            auth_b64 = entry.get("auth")
            if auth_b64:
                try:
                    decoded = base64.b64decode(auth_b64).decode()
                except Exception:
                    continue
                if ":" not in decoded:
                    continue
                username, password = decoded.split(":", 1)
                return username, password

    return None, None


def _creds_from_helper(helper_name, server):
    """Get credentials from a Docker credential helper binary."""
    try:
        proc = subprocess.run(
            [f"docker-credential-{helper_name}", "get"],
            input=server,
            capture_output=True,
            text=True,
        )
        if proc.returncode == 0:
            creds = json.loads(proc.stdout)
            return creds.get("Username"), creds.get("Secret")
    except (FileNotFoundError, json.JSONDecodeError):
        pass
    return None, None


def _authenticate_target_registry(client, registry):
    """Set up auth for the target registry using ambient cloud credentials."""
    if "pkg.dev" in registry:
        try:
            result = subprocess.run(
                ["gcloud", "auth", "print-access-token"], capture_output=True, text=True, check=True
            )
            client.authenticate(
                registry, username="oauth2accesstoken", password=result.stdout.strip()
            )
        except (subprocess.CalledProcessError, FileNotFoundError):
            console.print(
                "[yellow]Warning: Could not get gcloud access token. Target registry auth may fail.[/yellow]"
            )
    elif ".dkr.ecr." in registry:
        try:
            import base64

            import boto3

            ecr = boto3.client("ecr")
            token_resp = ecr.get_authorization_token()
            auth_data = token_resp["authorizationData"][0]
            decoded = base64.b64decode(auth_data["authorizationToken"]).decode()
            username, password = decoded.split(":", 1)
            client.authenticate(registry, username=username, password=password)
        except Exception:
            console.print(
                "[yellow]Warning: Could not get ECR auth token. Target registry auth may fail.[/yellow]"
            )
    elif ".azurecr.io" in registry:
        try:
            result = subprocess.run(
                ["az", "acr", "login", "--name", registry.split(".")[0], "--expose-token"],
                capture_output=True,
                text=True,
                check=True,
            )
            token_data = json.loads(result.stdout)
            client.authenticate(
                registry,
                username="00000000-0000-0000-0000-000000000000",
                password=token_data["accessToken"],
            )
        except (subprocess.CalledProcessError, FileNotFoundError):
            console.print(
                "[yellow]Warning: Could not get ACR token. Target registry auth may fail.[/yellow]"
            )


# ── Output ────────────────────────────────────────────────────────────


def _print_summary(results, release, cloud, registry):
    """Print a summary table of the load operation."""
    is_local = registry == "local"
    title = (
        f"Zipline {release} ({cloud}) -> local Docker"
        if is_local
        else f"Zipline {release} ({cloud}) -> {registry}"
    )
    table = Table(title=title)
    table.add_column("Type", style="cyan")
    table.add_column("Reference", style="white")
    table.add_column("Status", style="green")

    all_ok = True
    for entry_type, ref, _digest, status in results:
        if status == "ok":
            style = "green"
        elif status == "already exists":
            style = "cyan"
        else:
            style = "red"
            all_ok = False
        table.add_row(entry_type, ref, f"[{style}]{status}[/{style}]")

    console.print(table)

    if all_ok:
        console.print("\n[bold green]All artifacts loaded successfully.[/bold green]")
        if is_local:
            console.print("\nImages available in local Docker daemon:")
            for _image_type, repo, tag in _app_images(cloud, release):
                console.print(f"  {repo}:{tag}")
        else:
            console.print("\nFor terraform.tfvars:")
            console.print(f'  hub_image      = "{registry}/ziplineai/hub-{cloud}:{release}"')
            console.print(f'  eval_image     = "{registry}/ziplineai/eval-{cloud}:{release}"')
            console.print(f'  frontend_image = "{registry}/ziplineai/web-ui:{release}"')
            console.print(f'  fetcher_image  = "{registry}/ziplineai/chronon-fetcher:{release}"')
            if cloud != "gcp":
                console.print(f'  flink_image    = "{registry}/ziplineai/flink:{FLINK_IMAGE_TAG}"')
            console.print(f'  engine_image   = "{registry}/ziplineai/engine-{cloud}:{release}"')
    else:
        console.print("\n[bold red]Some artifacts failed to load. See errors above.[/bold red]")
        raise SystemExit(1)


@admin.group("upgrade")
def upgrade():
    """Upgrade Zipline control-plane EKS services or trigger a data-plane streaming job redeploy."""
    pass


@upgrade.command("control-plane")
@click.argument("cloud", type=click.Choice(VALID_CLOUDS, case_sensitive=False))
@click.option(
    "--release",
    default=None,
    help="Zipline release to upgrade to (e.g. 1.4.2). Defaults to the installed zipline-ai package version.",
)
@assume_yes_option
def control_plane(cloud, release, assume_yes):
    """Upgrade running EKS service deployments to a given release.

    CLOUD is the cloud provider variant (gcp, aws, or azure).
    Currently only AWS EKS deployments are supported.
    """
    if cloud != "aws":
        console.print(f"[yellow]Upgrade is currently only supported for AWS (got {cloud}).[/yellow]")
        raise SystemExit(1)

    if release is None:
        release = get_package_version()
        if release == "unknown":
            console.print("[red]Could not detect installed zipline version. Please specify --release.[/red]")
            raise SystemExit(1)
        console.print(f"Using release [bold]{release}[/bold]")

    # Detect the active kubectl context after input validation passes — we
    # don't want a kubectl-not-found error to mask an obvious bad arg.
    context = _get_current_kube_context()
    _confirm_kube_context(context, assume_yes=assume_yes)

    _upgrade_eks_services(cloud, release)


@upgrade.command("data-plane")
@click.argument("confs", nargs=-1, required=True)
@repo_option
@hub_url_option
@use_auth_option
@format_option
def data_plane(confs, repo, hub_url, use_auth, format):
    """Redeploy running streaming GroupBy jobs.

    CONFs are the path to the compiled conf (e.g. compiled/joins/team/my_join).
    Syncs confs to Hub then triggers a redeploy - the job will use the version as configured in the VERSION of the
    conf / teams.py (in that order)
    """
    redeploy_streaming(
        repo=repo,
        confs=list(confs),
        hub_url=hub_url,
        use_auth=use_auth,
        format=format,
    )



@admin.group("doctor")
def doctor():
    """Diagnose a Zipline deployment."""
    pass


@doctor.command("hub-health")
@click.argument("hub_url")
@click.option("--expected-version", default=None, help="Expected Zipline version (optional).")
def hub_health(hub_url, expected_version):
    """Check that the Zipline hub is reachable and healthy.

    HUB_URL is the URL of the running Zipline hub (e.g. https://hub.example.com).
    """
    import urllib3

    http = urllib3.PoolManager()
    hub_url = hub_url.rstrip("/")

    results = []

    console.print(f"[bold]Checking hub health at {hub_url}/debug...[/bold]")
    try:
        resp = http.request("GET", f"{hub_url}/debug", timeout=10.0)
        if resp.status == 200:
            body = json.loads(resp.data)
            actual_version = body.get("version", "unknown")
            results.append(("Hub Health", f"{hub_url}/debug", "ok", f"version={actual_version}"))

            if expected_version and actual_version != expected_version:
                results.append(
                    (
                        "Version Check",
                        "",
                        "WARN",
                        f"Expected {expected_version}, got {actual_version}",
                    )
                )
            elif expected_version:
                results.append(("Version Check", "", "ok", f"matches {expected_version}"))
        else:
            results.append(("Hub Health", f"{hub_url}/debug", "FAIL", f"HTTP {resp.status}"))
    except Exception as e:
        results.append(("Hub Health", f"{hub_url}/debug", "FAIL", str(e)))

    console.print(f"[bold]Checking upload API at {hub_url}/upload/v2/diff...[/bold]")
    try:
        resp = http.request("POST", f"{hub_url}/upload/v2/diff", timeout=10.0, body=b"{}")
        if resp.status < 500:
            results.append(
                ("Upload API", f"{hub_url}/upload/v2/diff", "ok", f"HTTP {resp.status} (reachable)")
            )
        else:
            results.append(
                ("Upload API", f"{hub_url}/upload/v2/diff", "FAIL", f"HTTP {resp.status}")
            )
    except Exception as e:
        results.append(("Upload API", f"{hub_url}/upload/v2/diff", "FAIL", str(e)))

    print_check_table(f"Zipline Hub Diagnostics: {hub_url}", results)


@doctor.command("streaming-health")
@click.option(
    "--cloud",
    type=click.Choice(VALID_CLOUDS, case_sensitive=False),
    default="aws",
    show_default=True,
    help="Cloud provider variant.",
)
@assume_yes_option
def streaming_health(cloud, assume_yes):
    """Check that the Flink streaming infrastructure is correctly configured.

    Requires kubectl to be installed and configured with access to the cluster.
    """
    context = _get_current_kube_context()
    _confirm_kube_context(context, assume_yes=assume_yes)
    console.print("[bold]Running Kubernetes infrastructure checks...[/bold]")
    results = run_infra_checks(cloud=cloud)
    print_check_table("Zipline Streaming Infrastructure Diagnostics", results)

if __name__ == "__main__":
    admin()
