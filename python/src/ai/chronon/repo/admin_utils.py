"""Utilities for Zipline admin CLI health checks."""

import json
import logging
import shutil
import subprocess
from functools import partial
from urllib.parse import urlparse

from rich.table import Table

from ai.chronon.cli.theme import console

logger = logging.getLogger(__name__)

_KUBECTL_TIMEOUT = 10  # seconds


def _run_kubectl(args, timeout=_KUBECTL_TIMEOUT, context=None):
    """Run a kubectl subcommand with a timeout.

    Returns a CompletedProcess-like object; on TimeoutExpired, logs a warning
    and returns a namespace with returncode=1 and empty stdout/stderr so
    callers that check returncode behave correctly without special-casing.

    When `context` is set, `--context <name>` is prepended so the call targets
    that kubeconfig context instead of the ambient active one. Callers that
    want deterministic env routing should pass it; legacy callers can leave it
    None to preserve the old behavior.
    """
    cmd = ["kubectl"]
    if context:
        cmd.extend(["--context", context])
    cmd.extend(args)
    try:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        logger.warning("kubectl %s timed out after %ss", " ".join(args), timeout)

        class _TimedOut:
            returncode = 1
            stdout = ""
            stderr = f"timed out after {timeout}s"

        return _TimedOut()


def run_infra_checks(cloud="aws", kube_context=None):
    """Run Kubernetes infrastructure checks for the Flink streaming setup.

    Returns a list of (check_name, what, status, detail) tuples.

    When `kube_context` is set, every kubectl call targets that context — used
    by the env-aware streaming-health command so we deterministically check
    the prod or canary cluster rather than the ambient active one.
    """
    results = []
    kctl = partial(_run_kubectl, context=kube_context)

    if not shutil.which("kubectl"):
        results.append(
            (
                "kubectl",
                "kubectl binary present",
                "FAIL",
                "not found — install it and configure kubeconfig",
            )
        )
        return results
    results.append(("kubectl", "kubectl binary present", "ok", ""))

    r = kctl(["cluster-info"])
    if r.returncode != 0:
        results.append(
            (
                "cluster",
                "Can reach the Kubernetes API",
                "FAIL",
                r.stderr.strip() or "kubectl cluster-info failed",
            )
        )
        return results
    results.append(("cluster", "Can reach the Kubernetes API", "ok", ""))

    for ns in ("zipline-flink", "zipline-system"):
        r = kctl(["get", "namespace", ns])
        if r.returncode != 0:
            results.append(
                (
                    ns,
                    f"{ns} namespace exists",
                    "FAIL",
                    "not found — Terraform may not have been applied",
                )
            )
        else:
            results.append((ns, f"{ns} namespace exists", "ok", ""))

    r = kctl(["get", "role", "orchestration-flink-role", "-n", "zipline-flink"])
    if r.returncode != 0:
        results.append(
            (
                "orchestration-flink-role",
                "Flink RBAC role exists",
                "FAIL",
                "role missing — Terraform may not have been applied",
            )
        )
    else:
        results.append(("orchestration-flink-role", "Flink RBAC role exists", "ok", ""))

    r = kctl(
        [
            "auth",
            "can-i",
            "create",
            "flinkdeployments.flink.apache.org",
            "--namespace",
            "zipline-flink",
            "--as",
            "system:serviceaccount:zipline-system:orchestration-sa",
        ]
    )
    if r.returncode != 0 or r.stdout.strip() != "yes":
        results.append(
            (
                "FlinkDeployment RBAC",
                "orchestration-sa can submit Flink jobs",
                "FAIL",
                "orchestration-sa lacks create permission on flinkdeployments",
            )
        )
    else:
        results.append(("FlinkDeployment RBAC", "orchestration-sa can submit Flink jobs", "ok", ""))

    r = kctl(
        [
            "auth",
            "can-i",
            "create",
            "ingresses.networking.k8s.io",
            "--namespace",
            "zipline-flink",
            "--as",
            "system:serviceaccount:zipline-system:orchestration-sa",
        ]
    )
    if r.returncode != 0 or r.stdout.strip() != "yes":
        results.append(
            (
                "ingress RBAC",
                "orchestration-sa can manage Flink ingresses",
                "FAIL",
                "orchestration-sa lacks ingress permissions — check orchestration-flink-role",
            )
        )
    else:
        results.append(("ingress RBAC", "orchestration-sa can manage Flink ingresses", "ok", ""))

    r = kctl(["get", "serviceaccount", "zipline-flink-sa", "-n", "zipline-flink"])
    if r.returncode != 0:
        results.append(
            (
                "zipline-flink-sa",
                "Flink job service account exists",
                "FAIL",
                "SA missing — Terraform may not have been applied",
            )
        )
    else:
        results.append(("zipline-flink-sa", "Flink job service account exists", "ok", ""))

    r = kctl(["get", "crd", "flinkdeployments.flink.apache.org"])
    if r.returncode != 0:
        results.append(
            (
                "FlinkDeployment CRD",
                "Flink operator CRD installed",
                "FAIL",
                "CRD missing — flink-operator Helm chart or CRD manifest not applied",
            )
        )
    else:
        results.append(("FlinkDeployment CRD", "Flink operator CRD installed", "ok", ""))

    hub_base_url = None
    r = kctl(
        [
            "get",
            "deployment",
            "zipline-orchestration-hub",
            "-n",
            "zipline-system",
            "-o",
            "jsonpath={.spec.template.spec.containers[0].env}",
        ]
    )
    if r.returncode != 0:
        results.append(
            (
                "HUB_BASE_URL",
                "Hub base URL is configured",
                "FAIL",
                "could not read hub deployment env vars",
            )
        )
    else:
        try:
            env_vars = json.loads(r.stdout)
            hub_base_url = next(
                (e.get("value") for e in env_vars if e.get("name") == "HUB_BASE_URL" and e.get("value") is not None),
                None,
            )
            if hub_base_url:
                results.append(("HUB_BASE_URL", "Hub base URL is configured", "ok", hub_base_url))
            else:
                results.append(
                    (
                        "HUB_BASE_URL",
                        "Hub base URL is configured",
                        "FAIL",
                        "not set — check hub_domain/hub_external_url vars or set-hub-base-url Job",
                    )
                )
        except (json.JSONDecodeError, StopIteration):
            results.append(
                (
                    "HUB_BASE_URL",
                    "Hub base URL is configured",
                    "FAIL",
                    "could not parse deployment env vars",
                )
            )

    if hub_base_url:
        hub_hostname = urlparse(hub_base_url).hostname or ""
        r = kctl(
            [
                "get",
                "ingress",
                "orchestration-hub-ingress",
                "-n",
                "zipline-system",
                "-o",
                "jsonpath={.spec.rules[0].host}",
            ]
        )
        if r.returncode != 0:
            results.append(
                (
                    "hub ingress",
                    "Hub ingress host matches HUB_BASE_URL",
                    "WARN",
                    "could not read ingress — may not exist yet",
                )
            )
        else:
            ingress_host = r.stdout.strip()
            if ingress_host == hub_hostname:
                results.append(
                    (
                        "hub ingress",
                        "Hub ingress host matches HUB_BASE_URL",
                        "ok",
                        ingress_host,
                    )
                )
            else:
                results.append(
                    (
                        "hub ingress",
                        "Hub ingress host matches HUB_BASE_URL",
                        "WARN",
                        f"ingress host {ingress_host!r} != HUB_BASE_URL hostname {hub_hostname!r}",
                    )
                )

        is_elb = ".elb." in hub_base_url and ".amazonaws.com" in hub_base_url
        if is_elb:
            r = kctl(
                [
                    "get",
                    "events",
                    "-n",
                    "zipline-system",
                    "--field-selector",
                    "reason=Completed",
                ]
            )
            if r.returncode == 0 and "set-hub-base-url" in r.stdout:
                results.append(
                    (
                        "set-hub-base-url",
                        "ELB hostname discovery job completed",
                        "ok",
                        "completed successfully",
                    )
                )
            else:
                results.append(
                    (
                        "set-hub-base-url",
                        "ELB hostname discovery job completed",
                        "WARN",
                        "no completion event — check: kubectl logs -n zipline-system -l job-name=set-hub-base-url",
                    )
                )

    if cloud == "azure":
        results.extend(_run_azure_infra_checks(kube_context=kube_context))

    return results


def _run_azure_infra_checks(kube_context=None):
    """Run Azure-specific checks for Flink-on-AKS with Workload Identity.

    Returns a list of (check_name, what, status, detail) tuples.
    """
    results = []
    kctl = partial(_run_kubectl, context=kube_context)

    # Namespace label: tells the WI mutating webhook to activate for pods in this namespace.
    # Without it, the webhook skips the namespace entirely — SA annotation and client-id are irrelevant.
    r = kctl(
        [
            "get",
            "namespace",
            "zipline-flink",
            "-o",
            "jsonpath={.metadata.labels.azure\\.workload\\.identity/use}",
        ]
    )
    if r.returncode != 0 or r.stdout.strip() != "true":
        results.append(
            (
                "WI namespace label",
                "zipline-flink has azure.workload.identity/use=true",
                "FAIL",
                "label missing — token injection webhook won't activate for Flink pods",
            )
        )
    else:
        results.append(
            ("WI namespace label", "zipline-flink has azure.workload.identity/use=true", "ok", "")
        )

    # SA annotation: binds the SA to an Azure managed identity (client-id).
    # Without it, the webhook won't inject the federated token volume — Flink pods can't authenticate to ABFS or Key Vault.
    r = kctl(
        [
            "get",
            "serviceaccount",
            "zipline-flink-sa",
            "-n",
            "zipline-flink",
            "-o",
            "jsonpath={.metadata.annotations.azure\\.workload\\.identity/client-id}",
        ]
    )
    client_id = r.stdout.strip() if r.returncode == 0 else ""
    if not client_id:
        results.append(
            (
                "WI SA annotation",
                "zipline-flink-sa has azure.workload.identity/client-id",
                "FAIL",
                "annotation missing — Flink pods won't get Azure tokens for ABFS access",
            )
        )
    else:
        results.append(
            (
                "WI SA annotation",
                "zipline-flink-sa has azure.workload.identity/client-id",
                "ok",
                client_id,
            )
        )

    # Flink operator: reconciles FlinkDeployment CRs into JM/TM pods.
    # Without a running operator, submitted jobs will be accepted by the API but never materialize.
    r = kctl(
        [
            "get",
            "deployment",
            "flink-kubernetes-operator",
            "-n",
            "flink-operator",
            "-o",
            "jsonpath={.status.availableReplicas}",
        ]
    )
    available = r.stdout.strip() if r.returncode == 0 else ""
    if not available or available == "0":
        results.append(
            (
                "Flink operator",
                "flink-kubernetes-operator deployment is available",
                "FAIL",
                "no available replicas — check: kubectl get pods -n flink-operator",
            )
        )
    else:
        results.append(
            (
                "Flink operator",
                "flink-kubernetes-operator deployment is available",
                "ok",
                f"availableReplicas={available}",
            )
        )

    # Hub env vars: AksFlinkSubmitter reads these to configure the WI identity and target namespace for submitted jobs.
    # Missing values mean jobs are submitted with the wrong (or no) identity, causing silent ABFS/Event Hubs auth failures.
    r = kctl(
        [
            "get",
            "deployment",
            "zipline-orchestration-hub",
            "-n",
            "zipline-system",
            "-o",
            "jsonpath={.spec.template.spec.containers[0].env}",
        ]
    )
    if r.returncode != 0:
        for check, what in [
            ("Flink Azure env vars", "FLINK_AZURE_CLIENT_ID and FLINK_AZURE_TENANT_ID set on hub"),
            ("Flink AKS env vars", "FLINK_AKS_SERVICE_ACCOUNT and FLINK_AKS_NAMESPACE set on hub"),
        ]:
            results.append((check, what, "FAIL", "could not read hub deployment env vars"))
    else:
        try:
            env_vars = json.loads(r.stdout)
            env_map = {e.get("name"): e.get("value") for e in env_vars if e.get("name")}

            azure_client_id = env_map.get("FLINK_AZURE_CLIENT_ID", "")
            azure_tenant_id = env_map.get("FLINK_AZURE_TENANT_ID", "")
            if azure_client_id and azure_tenant_id:
                results.append(
                    (
                        "Flink Azure env vars",
                        "FLINK_AZURE_CLIENT_ID and FLINK_AZURE_TENANT_ID set on hub",
                        "ok",
                        f"client_id={azure_client_id}",
                    )
                )
            else:
                missing = ", ".join(
                    v for v in ["FLINK_AZURE_CLIENT_ID", "FLINK_AZURE_TENANT_ID"] if not env_map.get(v)
                )
                results.append(
                    (
                        "Flink Azure env vars",
                        "FLINK_AZURE_CLIENT_ID and FLINK_AZURE_TENANT_ID set on hub",
                        "FAIL",
                        f"missing: {missing} — check helm values flink.azureClientId / flink.azureTenantId",
                    )
                )

            sa = env_map.get("FLINK_AKS_SERVICE_ACCOUNT", "")
            ns = env_map.get("FLINK_AKS_NAMESPACE", "")
            if sa and ns:
                results.append(
                    (
                        "Flink AKS env vars",
                        "FLINK_AKS_SERVICE_ACCOUNT and FLINK_AKS_NAMESPACE set on hub",
                        "ok",
                        f"sa={sa}, ns={ns}",
                    )
                )
            else:
                missing = ", ".join(
                    v for v in ["FLINK_AKS_SERVICE_ACCOUNT", "FLINK_AKS_NAMESPACE"] if not env_map.get(v)
                )
                results.append(
                    (
                        "Flink AKS env vars",
                        "FLINK_AKS_SERVICE_ACCOUNT and FLINK_AKS_NAMESPACE set on hub",
                        "FAIL",
                        f"missing: {missing} — check helm values flink.aksServiceAccount / flink.aksNamespace",
                    )
                )
        except (json.JSONDecodeError, AttributeError):
            for check, what in [
                ("Flink Azure env vars", "FLINK_AZURE_CLIENT_ID and FLINK_AZURE_TENANT_ID set on hub"),
                ("Flink AKS env vars", "FLINK_AKS_SERVICE_ACCOUNT and FLINK_AKS_NAMESPACE set on hub"),
            ]:
                results.append((check, what, "FAIL", "could not parse hub deployment env vars"))

    # WI webhook: the mutating webhook that injects the federated token volume into pods at admission time.
    # Without it, namespace label and SA annotation are both no-ops — pods never receive an Azure token.
    # AKS names this differently depending on installation method:
    #   - Helm/standalone: azure-workload-identity-webhook
    #   - AKS managed add-on: azure-wi-webhook-mutating-webhook-configuration
    _WI_WEBHOOK_NAMES = [
        "azure-wi-webhook-mutating-webhook-configuration",
        "azure-workload-identity-webhook",
    ]
    wi_webhook_found = None
    for webhook_name in _WI_WEBHOOK_NAMES:
        r = kctl(["get", "mutatingwebhookconfiguration", webhook_name])
        if r.returncode == 0:
            wi_webhook_found = webhook_name
            break
    if wi_webhook_found:
        results.append(
            ("WI webhook", "Azure Workload Identity webhook is installed", "ok", wi_webhook_found)
        )
    else:
        results.append(
            (
                "WI webhook",
                "Azure Workload Identity webhook is installed",
                "FAIL",
                "webhook missing — token injection won't work; check AKS workload identity add-on",
            )
        )

    return results


def print_check_table(title, results):
    """Render a diagnostics results table and exit 1 if any check FAILed."""
    table = Table(title=title, expand=False)
    table.add_column("Check", style="cyan", no_wrap=True)
    table.add_column("What", style="white")
    table.add_column("", width=1)
    table.add_column("Detail", style="white")

    all_ok = True
    for check, what, status, detail in results:
        if status == "ok":
            status_cell = "[green]✓[/green]"
        elif status == "WARN":
            status_cell = "[yellow]⚠[/yellow]"
        else:
            status_cell = "[red]✗[/red]"
            all_ok = False
        table.add_row(check, what, status_cell, detail)

    console.print(table)

    if all_ok:
        console.print("\n[bold green]All checks passed.[/bold green]")
    else:
        console.print("\n[bold red]Some checks failed.[/bold red]")
        raise SystemExit(1)
