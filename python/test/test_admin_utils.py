"""Unit tests for admin_utils: _run_kubectl, run_infra_checks, print_check_table."""

import json
import subprocess
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from ai.chronon.repo.admin_utils import _run_kubectl, print_check_table, run_infra_checks


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ok(stdout="", stderr=""):
    return SimpleNamespace(returncode=0, stdout=stdout, stderr=stderr)


def _fail(stderr="error", stdout=""):
    return SimpleNamespace(returncode=1, stdout=stdout, stderr=stderr)


# ---------------------------------------------------------------------------
# _run_kubectl
# ---------------------------------------------------------------------------


class TestRunKubectl:
    def test_success_returns_completed_process(self):
        with patch("subprocess.run", return_value=_ok(stdout="ok")) as mock_run:
            result = _run_kubectl(["cluster-info"])
        mock_run.assert_called_once_with(
            ["kubectl", "cluster-info"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        assert result.returncode == 0
        assert result.stdout == "ok"

    def test_timeout_returns_returncode_1(self):
        with patch("subprocess.run", side_effect=subprocess.TimeoutExpired("kubectl", 10)):
            result = _run_kubectl(["cluster-info"])
        assert result.returncode == 1
        assert result.stdout == ""
        assert "timed out" in result.stderr

    def test_custom_timeout_forwarded(self):
        with patch("subprocess.run", return_value=_ok()) as mock_run:
            _run_kubectl(["get", "pods"], timeout=30)
        _, kwargs = mock_run.call_args
        assert kwargs["timeout"] == 30


# ---------------------------------------------------------------------------
# run_infra_checks — early-exit paths
# ---------------------------------------------------------------------------


class TestRunInfraChecksEarlyExit:
    def test_no_kubectl_binary(self):
        with patch("shutil.which", return_value=None):
            results = run_infra_checks()
        assert len(results) == 1
        check, _, status, _ = results[0]
        assert check == "kubectl"
        assert status == "FAIL"

    def test_cluster_unreachable_stops_early(self):
        with patch("shutil.which", return_value="/usr/bin/kubectl"), \
             patch("ai.chronon.repo.admin_utils._run_kubectl", return_value=_fail(stderr="connection refused")) as mock_kc:
            results = run_infra_checks()
        # Only kubectl-present + cluster checks emitted, then bail out
        statuses = {r[0]: r[2] for r in results}
        assert statuses["kubectl"] == "ok"
        assert statuses["cluster"] == "FAIL"
        # detail should propagate the stderr
        cluster_detail = next(r[3] for r in results if r[0] == "cluster")
        assert "connection refused" in cluster_detail
        # No further checks after cluster failure
        assert len(results) == 2


# ---------------------------------------------------------------------------
# run_infra_checks — full happy path
# ---------------------------------------------------------------------------


class TestRunInfraChecksHappyPath:
    def _make_kubectl(self, hub_url="https://hub.example.com"):
        """Return a mock _run_kubectl that answers ok to every call."""
        env_payload = json.dumps([{"name": "HUB_BASE_URL", "value": hub_url}])

        def side_effect(args, **kwargs):
            # Hub deployment env query returns the JSON payload
            if "jsonpath={.spec.template.spec.containers[0].env}" in " ".join(args):
                return _ok(stdout=env_payload)
            # Ingress host query returns the hostname
            if "jsonpath={.spec.rules[0].host}" in " ".join(args):
                return _ok(stdout="hub.example.com")
            return _ok(stdout="yes")

        return side_effect

    def test_all_ok(self):
        with patch("shutil.which", return_value="/usr/bin/kubectl"), \
             patch("ai.chronon.repo.admin_utils._run_kubectl", side_effect=self._make_kubectl()):
            results = run_infra_checks()

        statuses = {r[0]: r[2] for r in results}
        assert all(s == "ok" for s in statuses.values()), statuses

    def test_hub_base_url_present_in_results(self):
        hub_url = "https://hub.example.com"
        with patch("shutil.which", return_value="/usr/bin/kubectl"), \
             patch("ai.chronon.repo.admin_utils._run_kubectl", side_effect=self._make_kubectl(hub_url)):
            results = run_infra_checks()

        hub_result = next(r for r in results if r[0] == "HUB_BASE_URL")
        assert hub_result[2] == "ok"
        assert hub_result[3] == hub_url


# ---------------------------------------------------------------------------
# run_infra_checks — HUB_BASE_URL edge cases
# ---------------------------------------------------------------------------


class TestRunInfraChecksHubBaseUrl:
    def _base_kubectl(self, hub_env_response):
        """Mock where everything passes except the hub env query returns hub_env_response."""
        def side_effect(args, **kwargs):
            if "jsonpath={.spec.template.spec.containers[0].env}" in " ".join(args):
                return hub_env_response
            return _ok(stdout="yes")
        return side_effect

    def test_hub_base_url_missing_from_env(self):
        payload = json.dumps([{"name": "OTHER_VAR", "value": "x"}])
        with patch("shutil.which", return_value="/usr/bin/kubectl"), \
             patch("ai.chronon.repo.admin_utils._run_kubectl",
                   side_effect=self._base_kubectl(_ok(stdout=payload))):
            results = run_infra_checks()

        hub = next(r for r in results if r[0] == "HUB_BASE_URL")
        assert hub[2] == "FAIL"

    def test_hub_base_url_entry_without_value_key(self):
        # Entry matches by name but has no "value" key — should not KeyError
        payload = json.dumps([{"name": "HUB_BASE_URL"}])
        with patch("shutil.which", return_value="/usr/bin/kubectl"), \
             patch("ai.chronon.repo.admin_utils._run_kubectl",
                   side_effect=self._base_kubectl(_ok(stdout=payload))):
            results = run_infra_checks()

        hub = next(r for r in results if r[0] == "HUB_BASE_URL")
        assert hub[2] == "FAIL"

    def test_hub_base_url_invalid_json(self):
        with patch("shutil.which", return_value="/usr/bin/kubectl"), \
             patch("ai.chronon.repo.admin_utils._run_kubectl",
                   side_effect=self._base_kubectl(_ok(stdout="not-json"))):
            results = run_infra_checks()

        hub = next(r for r in results if r[0] == "HUB_BASE_URL")
        assert hub[2] == "FAIL"
        assert "could not parse" in hub[3]

    def test_hub_base_url_kubectl_failure(self):
        with patch("shutil.which", return_value="/usr/bin/kubectl"), \
             patch("ai.chronon.repo.admin_utils._run_kubectl",
                   side_effect=self._base_kubectl(_fail())):
            results = run_infra_checks()

        hub = next(r for r in results if r[0] == "HUB_BASE_URL")
        assert hub[2] == "FAIL"
        assert "could not read" in hub[3]


# ---------------------------------------------------------------------------
# run_infra_checks — ingress host mismatch
# ---------------------------------------------------------------------------


class TestRunInfraChecksIngress:
    def test_ingress_host_mismatch_is_warn(self):
        env_payload = json.dumps([{"name": "HUB_BASE_URL", "value": "https://hub.example.com"}])

        def kubectl(args, **kwargs):
            if "jsonpath={.spec.template.spec.containers[0].env}" in " ".join(args):
                return _ok(stdout=env_payload)
            if "jsonpath={.spec.rules[0].host}" in " ".join(args):
                return _ok(stdout="wrong.host.com")
            return _ok(stdout="yes")

        with patch("shutil.which", return_value="/usr/bin/kubectl"), \
             patch("ai.chronon.repo.admin_utils._run_kubectl", side_effect=kubectl):
            results = run_infra_checks()

        ingress = next(r for r in results if r[0] == "hub ingress")
        assert ingress[2] == "WARN"
        assert "wrong.host.com" in ingress[3]

    def test_ingress_kubectl_failure_is_warn(self):
        env_payload = json.dumps([{"name": "HUB_BASE_URL", "value": "https://hub.example.com"}])

        def kubectl(args, **kwargs):
            if "jsonpath={.spec.template.spec.containers[0].env}" in " ".join(args):
                return _ok(stdout=env_payload)
            if "jsonpath={.spec.rules[0].host}" in " ".join(args):
                return _fail()
            return _ok(stdout="yes")

        with patch("shutil.which", return_value="/usr/bin/kubectl"), \
             patch("ai.chronon.repo.admin_utils._run_kubectl", side_effect=kubectl):
            results = run_infra_checks()

        ingress = next(r for r in results if r[0] == "hub ingress")
        assert ingress[2] == "WARN"

    def test_elb_url_triggers_set_hub_base_url_check(self):
        elb_url = "https://abc.elb.us-east-1.amazonaws.com"
        env_payload = json.dumps([{"name": "HUB_BASE_URL", "value": elb_url}])

        def kubectl(args, **kwargs):
            if "jsonpath={.spec.template.spec.containers[0].env}" in " ".join(args):
                return _ok(stdout=env_payload)
            if "jsonpath={.spec.rules[0].host}" in " ".join(args):
                return _ok(stdout="abc.elb.us-east-1.amazonaws.com")
            if "reason=Completed" in " ".join(args):
                return _ok(stdout="set-hub-base-url completed")
            return _ok(stdout="yes")

        with patch("shutil.which", return_value="/usr/bin/kubectl"), \
             patch("ai.chronon.repo.admin_utils._run_kubectl", side_effect=kubectl):
            results = run_infra_checks()

        elb_check = next((r for r in results if r[0] == "set-hub-base-url"), None)
        assert elb_check is not None
        assert elb_check[2] == "ok"

    def test_non_elb_url_skips_set_hub_base_url_check(self):
        env_payload = json.dumps([{"name": "HUB_BASE_URL", "value": "https://hub.example.com"}])

        def kubectl(args, **kwargs):
            if "jsonpath={.spec.template.spec.containers[0].env}" in " ".join(args):
                return _ok(stdout=env_payload)
            if "jsonpath={.spec.rules[0].host}" in " ".join(args):
                return _ok(stdout="hub.example.com")
            return _ok(stdout="yes")

        with patch("shutil.which", return_value="/usr/bin/kubectl"), \
             patch("ai.chronon.repo.admin_utils._run_kubectl", side_effect=kubectl):
            results = run_infra_checks()

        assert not any(r[0] == "set-hub-base-url" for r in results)


# ---------------------------------------------------------------------------
# _run_azure_infra_checks (via run_infra_checks(cloud="azure"))
# ---------------------------------------------------------------------------


class TestAzureInfraChecks:
    """Tests for the Azure-specific WI and operator checks."""

    def _base_kubectl(self, overrides=None):
        """Return a kubectl side_effect where every call succeeds by default.

        overrides: dict mapping a substring of the kubectl args string to a
        SimpleNamespace response, applied before the default fallback.
        """
        overrides = overrides or {}

        hub_env = json.dumps([
            {"name": "HUB_BASE_URL", "value": "https://hub.example.com"},
            {"name": "FLINK_AZURE_CLIENT_ID", "value": "client-abc"},
            {"name": "FLINK_AZURE_TENANT_ID", "value": "tenant-xyz"},
            {"name": "FLINK_AKS_SERVICE_ACCOUNT", "value": "zipline-flink-sa"},
            {"name": "FLINK_AKS_NAMESPACE", "value": "zipline-flink"},
        ])

        def side_effect(args, **kwargs):
            joined = " ".join(args)
            for key, response in overrides.items():
                if key in joined:
                    return response
            if "jsonpath={.spec.template.spec.containers[0].env}" in joined:
                return _ok(stdout=hub_env)
            if "jsonpath={.spec.rules[0].host}" in joined:
                return _ok(stdout="hub.example.com")
            if "jsonpath={.metadata.labels.azure" in joined:
                return _ok(stdout="true")
            if "jsonpath={.metadata.annotations.azure" in joined:
                return _ok(stdout="client-abc")
            if "jsonpath={.status.availableReplicas}" in joined:
                return _ok(stdout="1")
            if "azure-wi-webhook-mutating-webhook-configuration" in joined:
                return _ok()
            return _ok(stdout="yes")

        return side_effect

    def _run(self, overrides=None):
        with patch("shutil.which", return_value="/usr/bin/kubectl"), \
             patch("ai.chronon.repo.admin_utils._run_kubectl", side_effect=self._base_kubectl(overrides)):
            return run_infra_checks(cloud="azure")

    def _azure_statuses(self, overrides=None):
        return {r[0]: (r[2], r[3]) for r in self._run(overrides)}

    # Happy path

    def test_all_azure_checks_pass(self):
        statuses = self._azure_statuses()
        for check in [
            "WI namespace label",
            "WI SA annotation",
            "Flink operator",
            "Flink Azure env vars",
            "Flink AKS env vars",
            "WI webhook",
        ]:
            assert statuses[check][0] == "ok", f"{check} expected ok, got {statuses[check]}"

    # WI namespace label

    def test_wi_namespace_label_missing_fails(self):
        statuses = self._azure_statuses({"jsonpath={.metadata.labels.azure": _ok(stdout="")})
        assert statuses["WI namespace label"][0] == "FAIL"

    def test_wi_namespace_label_kubectl_failure_fails(self):
        statuses = self._azure_statuses({"jsonpath={.metadata.labels.azure": _fail()})
        assert statuses["WI namespace label"][0] == "FAIL"

    # WI SA annotation

    def test_wi_sa_annotation_missing_fails(self):
        statuses = self._azure_statuses({"jsonpath={.metadata.annotations.azure": _ok(stdout="")})
        assert statuses["WI SA annotation"][0] == "FAIL"
        assert "ABFS" in statuses["WI SA annotation"][1]

    def test_wi_sa_annotation_kubectl_failure_fails(self):
        statuses = self._azure_statuses({"jsonpath={.metadata.annotations.azure": _fail()})
        assert statuses["WI SA annotation"][0] == "FAIL"

    def test_wi_sa_annotation_shows_client_id_on_ok(self):
        statuses = self._azure_statuses()
        assert statuses["WI SA annotation"][1] == "client-abc"

    # Flink operator

    def test_flink_operator_no_replicas_fails(self):
        statuses = self._azure_statuses({"jsonpath={.status.availableReplicas}": _ok(stdout="0")})
        assert statuses["Flink operator"][0] == "FAIL"

    def test_flink_operator_empty_replicas_fails(self):
        statuses = self._azure_statuses({"jsonpath={.status.availableReplicas}": _ok(stdout="")})
        assert statuses["Flink operator"][0] == "FAIL"

    def test_flink_operator_kubectl_failure_fails(self):
        statuses = self._azure_statuses({"jsonpath={.status.availableReplicas}": _fail()})
        assert statuses["Flink operator"][0] == "FAIL"

    def test_flink_operator_shows_replica_count_on_ok(self):
        statuses = self._azure_statuses()
        assert "availableReplicas=1" in statuses["Flink operator"][1]

    # Flink Azure env vars (FLINK_AZURE_CLIENT_ID + FLINK_AZURE_TENANT_ID)

    def test_flink_azure_client_id_missing_fails(self):
        hub_env = json.dumps([
            {"name": "HUB_BASE_URL", "value": "https://hub.example.com"},
            {"name": "FLINK_AZURE_TENANT_ID", "value": "tenant-xyz"},
            {"name": "FLINK_AKS_SERVICE_ACCOUNT", "value": "zipline-flink-sa"},
            {"name": "FLINK_AKS_NAMESPACE", "value": "zipline-flink"},
        ])
        statuses = self._azure_statuses(
            {"jsonpath={.spec.template.spec.containers[0].env}": _ok(stdout=hub_env)}
        )
        assert statuses["Flink Azure env vars"][0] == "FAIL"
        assert "FLINK_AZURE_CLIENT_ID" in statuses["Flink Azure env vars"][1]

    def test_flink_azure_tenant_id_missing_fails(self):
        hub_env = json.dumps([
            {"name": "HUB_BASE_URL", "value": "https://hub.example.com"},
            {"name": "FLINK_AZURE_CLIENT_ID", "value": "client-abc"},
            {"name": "FLINK_AKS_SERVICE_ACCOUNT", "value": "zipline-flink-sa"},
            {"name": "FLINK_AKS_NAMESPACE", "value": "zipline-flink"},
        ])
        statuses = self._azure_statuses(
            {"jsonpath={.spec.template.spec.containers[0].env}": _ok(stdout=hub_env)}
        )
        assert statuses["Flink Azure env vars"][0] == "FAIL"
        assert "FLINK_AZURE_TENANT_ID" in statuses["Flink Azure env vars"][1]

    def test_flink_azure_env_vars_kubectl_failure_fails(self):
        statuses = self._azure_statuses(
            {"jsonpath={.spec.template.spec.containers[0].env}": _fail()}
        )
        assert statuses["Flink Azure env vars"][0] == "FAIL"

    # Flink AKS env vars (FLINK_AKS_SERVICE_ACCOUNT + FLINK_AKS_NAMESPACE)

    def test_flink_aks_namespace_missing_fails(self):
        hub_env = json.dumps([
            {"name": "HUB_BASE_URL", "value": "https://hub.example.com"},
            {"name": "FLINK_AZURE_CLIENT_ID", "value": "client-abc"},
            {"name": "FLINK_AZURE_TENANT_ID", "value": "tenant-xyz"},
            {"name": "FLINK_AKS_SERVICE_ACCOUNT", "value": "zipline-flink-sa"},
        ])
        statuses = self._azure_statuses(
            {"jsonpath={.spec.template.spec.containers[0].env}": _ok(stdout=hub_env)}
        )
        assert statuses["Flink AKS env vars"][0] == "FAIL"
        assert "FLINK_AKS_NAMESPACE" in statuses["Flink AKS env vars"][1]

    def test_flink_aks_service_account_missing_fails(self):
        hub_env = json.dumps([
            {"name": "HUB_BASE_URL", "value": "https://hub.example.com"},
            {"name": "FLINK_AZURE_CLIENT_ID", "value": "client-abc"},
            {"name": "FLINK_AZURE_TENANT_ID", "value": "tenant-xyz"},
            {"name": "FLINK_AKS_NAMESPACE", "value": "zipline-flink"},
        ])
        statuses = self._azure_statuses(
            {"jsonpath={.spec.template.spec.containers[0].env}": _ok(stdout=hub_env)}
        )
        assert statuses["Flink AKS env vars"][0] == "FAIL"
        assert "FLINK_AKS_SERVICE_ACCOUNT" in statuses["Flink AKS env vars"][1]

    def test_flink_aks_env_vars_shows_sa_and_ns_on_ok(self):
        statuses = self._azure_statuses()
        assert "zipline-flink-sa" in statuses["Flink AKS env vars"][1]
        assert "zipline-flink" in statuses["Flink AKS env vars"][1]

    # WI webhook — both known name variants

    def test_wi_webhook_aks_addon_name_passes(self):
        # Default mock already returns ok for the AKS add-on name
        statuses = self._azure_statuses()
        assert statuses["WI webhook"][0] == "ok"
        assert "azure-wi-webhook-mutating-webhook-configuration" in statuses["WI webhook"][1]

    def test_wi_webhook_standalone_helm_name_passes(self):
        def kubectl(args, **kwargs):
            joined = " ".join(args)
            if "azure-wi-webhook-mutating-webhook-configuration" in joined:
                return _fail()
            if "azure-workload-identity-webhook" in joined:
                return _ok()
            return self._base_kubectl()(args, **kwargs)

        with patch("shutil.which", return_value="/usr/bin/kubectl"), \
             patch("ai.chronon.repo.admin_utils._run_kubectl", side_effect=kubectl):
            results = run_infra_checks(cloud="azure")

        statuses = {r[0]: (r[2], r[3]) for r in results}
        assert statuses["WI webhook"][0] == "ok"
        assert "azure-workload-identity-webhook" in statuses["WI webhook"][1]

    def test_wi_webhook_neither_name_found_fails(self):
        statuses = self._azure_statuses({
            "azure-wi-webhook-mutating-webhook-configuration": _fail(),
            "azure-workload-identity-webhook": _fail(),
        })
        assert statuses["WI webhook"][0] == "FAIL"

    # Azure checks not run for non-azure clouds

    def test_azure_checks_not_run_for_aws(self):
        with patch("shutil.which", return_value="/usr/bin/kubectl"), \
             patch("ai.chronon.repo.admin_utils._run_kubectl", side_effect=self._base_kubectl()):
            results = run_infra_checks(cloud="aws")

        check_names = {r[0] for r in results}
        assert "WI namespace label" not in check_names
        assert "WI SA annotation" not in check_names
        assert "WI webhook" not in check_names


# ---------------------------------------------------------------------------
# print_check_table
# ---------------------------------------------------------------------------


class TestPrintCheckTable:
    def test_all_ok_does_not_raise(self):
        results = [
            ("check-a", "thing a", "ok", ""),
            ("check-b", "thing b", "ok", "detail"),
        ]
        # Should complete without raising SystemExit
        with patch("ai.chronon.repo.admin_utils.console"):
            print_check_table("Test", results)

    def test_any_fail_raises_system_exit(self):
        results = [
            ("check-a", "thing a", "ok", ""),
            ("check-b", "thing b", "FAIL", "broken"),
        ]
        with patch("ai.chronon.repo.admin_utils.console"), pytest.raises(SystemExit):
            print_check_table("Test", results)

    def test_warn_alone_does_not_raise(self):
        results = [
            ("check-a", "thing a", "WARN", "might be wrong"),
        ]
        with patch("ai.chronon.repo.admin_utils.console"):
            print_check_table("Test", results)  # no SystemExit
