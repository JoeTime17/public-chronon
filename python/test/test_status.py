from unittest.mock import patch

import pytest
from click.testing import CliRunner

from ai.chronon.repo.status import status

MOCK_CHECK_CALL = "ai.chronon.repo.status.subprocess.check_call"


@pytest.fixture
def runner():
    return CliRunner()


# --- CLI argument validation ---


class TestCliValidation:
    def test_mode_is_required(self, runner):
        result = runner.invoke(status, ["compiled/group_bys/team/my_gb"])
        assert result.exit_code != 0
        assert "--mode" in result.output or "Missing" in result.output

    def test_invalid_mode_rejected(self, runner):
        result = runner.invoke(status, [
            "compiled/group_bys/team/my_gb",
            "--mode", "invalid-mode",
        ])
        assert result.exit_code != 0
        assert "invalid-mode" in result.output or "Invalid" in result.output

    def test_conf_argument_is_required(self, runner):
        result = runner.invoke(status, ["--mode", "upload-to-kv"])
        assert result.exit_code != 0


# --- GroupBy conf validation ---


class TestConfValidation:
    @patch("ai.chronon.repo.status.resolve_conf", return_value="compiled/joins/team/my_join")
    def test_non_group_by_conf_rejected(self, mock_resolve, runner):
        result = runner.invoke(status, [
            "compiled/joins/team/my_join",
            "--mode", "upload-to-kv",
        ])
        assert result.exit_code != 0
        assert "group_by" in result.output


# --- OSS path (no CLOUD_PROVIDER) ---


class TestOssPath:
    @patch("ai.chronon.repo.status.get_environ_arg", return_value=None)
    @patch("ai.chronon.repo.status.resolve_conf", return_value="compiled/group_bys/team/my_gb")
    def test_missing_online_jar_fails(self, mock_resolve, mock_env, runner):
        result = runner.invoke(status, [
            "compiled/group_bys/team/my_gb",
            "--mode", "upload-to-kv",
        ])
        assert result.exit_code != 0
        assert "--online-jar" in result.output

    @patch("ai.chronon.repo.status.get_environ_arg", return_value=None)
    @patch("ai.chronon.repo.status.resolve_conf", return_value="compiled/group_bys/team/my_gb")
    def test_upload_to_kv_without_online_class_fails(self, mock_resolve, mock_env, runner):
        result = runner.invoke(status, [
            "compiled/group_bys/team/my_gb",
            "--mode", "upload-to-kv",
            "--online-jar", "/tmp/my.jar",
        ])
        assert result.exit_code != 0
        assert "--online-class" in result.output

    @patch(MOCK_CHECK_CALL)
    @patch("ai.chronon.repo.status.get_environ_arg", return_value=None)
    @patch("ai.chronon.repo.status.resolve_conf", return_value="compiled/group_bys/team/my_gb")
    def test_streaming_does_not_require_online_class(self, mock_resolve, mock_env, mock_call, runner):
        result = runner.invoke(status, [
            "compiled/group_bys/team/my_gb",
            "--mode", "streaming",
            "--online-jar", "/tmp/my.jar",
            "--flink-url", "http://flink-jm:8081",
        ])
        assert result.exit_code == 0

    @patch("ai.chronon.repo.status.get_environ_arg", return_value=None)
    @patch("ai.chronon.repo.status.resolve_conf", return_value="compiled/group_bys/team/my_gb")
    def test_streaming_without_flink_url_or_online_class_fails(self, mock_resolve, mock_env, runner):
        result = runner.invoke(status, [
            "compiled/group_bys/team/my_gb",
            "--mode", "streaming",
            "--online-jar", "/tmp/my.jar",
        ])
        assert result.exit_code != 0
        assert "--flink-url" in result.output

    @patch(MOCK_CHECK_CALL)
    @patch("ai.chronon.repo.status.get_environ_arg", return_value=None)
    @patch("ai.chronon.repo.status.resolve_conf", return_value="compiled/group_bys/team/my_gb")
    def test_streaming_with_online_class_auto_discovers(self, mock_resolve, mock_env, mock_call, runner):
        """When --online-class is provided without --flink-url, it should pass --online-class for auto-discovery."""
        result = runner.invoke(status, [
            "compiled/group_bys/team/my_gb",
            "--mode", "streaming",
            "--online-jar", "/tmp/my.jar",
            "--online-class", "com.example.MyApi",
        ])
        assert result.exit_code == 0
        cmd = mock_call.call_args[0][0]
        assert "--online-class" in cmd
        assert "com.example.MyApi" in cmd
        assert "--flink-url" not in cmd


# --- Cloud provider path ---


class TestCloudProviderPath:
    @patch(MOCK_CHECK_CALL)
    @patch("ai.chronon.repo.status._resolve_cloud_jars",
           return_value=("/tmp/cloud.jar:/tmp/service.jar", "ai.chronon.integrations.cloud_gcp.GcpApiImpl"))
    @patch("ai.chronon.repo.status.get_environ_arg", return_value="GCP")
    @patch("ai.chronon.repo.status.resolve_conf", return_value="compiled/group_bys/team/my_gb")
    def test_gcp_constructs_correct_classpath(self, mock_resolve, mock_env, mock_cloud, mock_call, runner):
        result = runner.invoke(status, [
            "compiled/group_bys/team/my_gb",
            "--mode", "upload-to-kv",
            "--artifact-prefix", "gs://bucket/artifacts",
            "--version", "1.0.0",
        ])
        assert result.exit_code == 0
        cmd = mock_call.call_args[0][0]
        assert "/tmp/cloud.jar:/tmp/service.jar" in cmd
        assert "ai.chronon.integrations.cloud_gcp.GcpApiImpl" in cmd

    @patch(MOCK_CHECK_CALL)
    @patch("ai.chronon.repo.status._resolve_cloud_jars",
           return_value=("/tmp/cloud.jar:/tmp/service.jar", "ai.chronon.integrations.cloud_gcp.GcpApiImpl"))
    @patch("ai.chronon.repo.status.get_environ_arg", return_value="GCP")
    @patch("ai.chronon.repo.status.resolve_conf", return_value="compiled/group_bys/team/my_gb")
    def test_gcp_streaming_passes_online_class_for_auto_discovery(self, mock_resolve, mock_env, mock_cloud, mock_call, runner):
        result = runner.invoke(status, [
            "compiled/group_bys/team/my_gb",
            "--mode", "streaming",
            "--artifact-prefix", "gs://bucket/artifacts",
            "--version", "1.0.0",
        ])
        assert result.exit_code == 0
        cmd = mock_call.call_args[0][0]
        assert "--online-class" in cmd
        assert "ai.chronon.integrations.cloud_gcp.GcpApiImpl" in cmd

    @patch("ai.chronon.repo.status.get_environ_arg", return_value="UNSUPPORTED_CLOUD")
    @patch("ai.chronon.repo.status.resolve_conf", return_value="compiled/group_bys/team/my_gb")
    def test_unsupported_cloud_provider_fails(self, mock_resolve, mock_env, runner):
        result = runner.invoke(status, [
            "compiled/group_bys/team/my_gb",
            "--mode", "upload-to-kv",
            "--artifact-prefix", "gs://bucket/artifacts",
            "--version", "1.0.0",
        ])
        assert result.exit_code != 0
        assert "Unsupported cloud provider" in result.output


# --- Command construction ---


class TestCommandConstruction:
    @patch(MOCK_CHECK_CALL)
    @patch("ai.chronon.repo.status.get_environ_arg", return_value=None)
    @patch("ai.chronon.repo.status.resolve_conf", return_value="compiled/group_bys/team/my_gb")
    def test_upload_to_kv_includes_online_class(self, mock_resolve, mock_env, mock_call, runner):
        result = runner.invoke(status, [
            "compiled/group_bys/team/my_gb",
            "--mode", "upload-to-kv",
            "--online-jar", "/tmp/my.jar",
            "--online-class", "com.example.MyApi",
        ])
        assert result.exit_code == 0
        cmd = mock_call.call_args[0][0]
        assert "--online-class" in cmd
        assert "com.example.MyApi" in cmd
        assert "-cp" in cmd
        assert "/tmp/my.jar" in cmd

    @patch(MOCK_CHECK_CALL)
    @patch("ai.chronon.repo.status.get_environ_arg", return_value=None)
    @patch("ai.chronon.repo.status.resolve_conf", return_value="compiled/group_bys/team/my_gb")
    def test_streaming_does_not_include_online_class(self, mock_resolve, mock_env, mock_call, runner):
        result = runner.invoke(status, [
            "compiled/group_bys/team/my_gb",
            "--mode", "streaming",
            "--online-jar", "/tmp/my.jar",
            "--flink-url", "http://flink-jm:8081",
        ])
        assert result.exit_code == 0
        cmd = mock_call.call_args[0][0]
        assert "--online-class" not in cmd

    @patch(MOCK_CHECK_CALL)
    @patch("ai.chronon.repo.status.get_environ_arg", return_value=None)
    @patch("ai.chronon.repo.status.resolve_conf", return_value="compiled/group_bys/team/my_gb")
    def test_streaming_includes_flink_url(self, mock_resolve, mock_env, mock_call, runner):
        result = runner.invoke(status, [
            "compiled/group_bys/team/my_gb",
            "--mode", "streaming",
            "--online-jar", "/tmp/my.jar",
            "--flink-url", "http://flink-jm:8081",
        ])
        assert result.exit_code == 0
        cmd = mock_call.call_args[0][0]
        assert "--flink-url" in cmd
        assert "http://flink-jm:8081" in cmd

    @patch(MOCK_CHECK_CALL)
    @patch("ai.chronon.repo.status.get_environ_arg", return_value=None)
    @patch("ai.chronon.repo.status.resolve_conf", return_value="compiled/group_bys/team/my_gb")
    def test_enable_debug_flag_passed(self, mock_resolve, mock_env, mock_call, runner):
        result = runner.invoke(status, [
            "compiled/group_bys/team/my_gb",
            "--mode", "streaming",
            "--online-jar", "/tmp/my.jar",
            "--flink-url", "http://flink-jm:8081",
            "--enable-debug",
        ])
        assert result.exit_code == 0
        cmd = mock_call.call_args[0][0]
        assert "--enable-debug" in cmd

    @patch(MOCK_CHECK_CALL)
    @patch("ai.chronon.repo.status.get_environ_arg", return_value=None)
    @patch("ai.chronon.repo.status.resolve_conf", return_value="compiled/group_bys/team/my_gb")
    def test_custom_repo_path_passed(self, mock_resolve, mock_env, mock_call, runner):
        result = runner.invoke(status, [
            "compiled/group_bys/team/my_gb",
            "--mode", "streaming",
            "--online-jar", "/tmp/my.jar",
            "--flink-url", "http://flink-jm:8081",
            "--repo", "/custom/repo",
        ])
        assert result.exit_code == 0
        cmd = mock_call.call_args[0][0]
        assert "--repo" in cmd
        assert "/custom/repo" in cmd

    @patch(MOCK_CHECK_CALL)
    @patch("ai.chronon.repo.status.get_environ_arg", return_value=None)
    @patch("ai.chronon.repo.status.resolve_conf", return_value="compiled/group_bys/team/my_gb")
    def test_entrypoint_class_in_command(self, mock_resolve, mock_env, mock_call, runner):
        result = runner.invoke(status, [
            "compiled/group_bys/team/my_gb",
            "--mode", "streaming",
            "--online-jar", "/tmp/my.jar",
            "--flink-url", "http://flink-jm:8081",
        ])
        assert result.exit_code == 0
        cmd = mock_call.call_args[0][0]
        assert "ai.chronon.online.status.StatusMain" in cmd

    @patch(MOCK_CHECK_CALL)
    @patch("ai.chronon.repo.status.get_environ_arg", return_value=None)
    @patch("ai.chronon.repo.status.resolve_conf", return_value="compiled/group_bys/team/my_gb")
    def test_command_is_a_list(self, mock_resolve, mock_env, mock_call, runner):
        result = runner.invoke(status, [
            "compiled/group_bys/team/my_gb",
            "--mode", "streaming",
            "--online-jar", "/tmp/my.jar",
            "--flink-url", "http://flink-jm:8081",
        ])
        assert result.exit_code == 0
        cmd = mock_call.call_args[0][0]
        assert isinstance(cmd, list)


# --- Env var fallback ---


class TestEnvVarFallback:
    @patch(MOCK_CHECK_CALL)
    @patch("ai.chronon.repo.status.get_environ_arg", return_value=None)
    @patch("ai.chronon.repo.status.resolve_conf", return_value="compiled/group_bys/team/my_gb")
    def test_chronon_online_jar_env_var_used(self, mock_resolve, mock_env, mock_call, runner):
        result = runner.invoke(status, [
            "compiled/group_bys/team/my_gb",
            "--mode", "streaming",
            "--flink-url", "http://flink-jm:8081",
        ], env={"CHRONON_ONLINE_JAR": "/tmp/env_jar.jar"})
        assert result.exit_code == 0
        cmd = mock_call.call_args[0][0]
        assert "/tmp/env_jar.jar" in cmd

    @patch(MOCK_CHECK_CALL)
    @patch("ai.chronon.repo.status.get_environ_arg", return_value=None)
    @patch("ai.chronon.repo.status.resolve_conf", return_value="compiled/group_bys/team/my_gb")
    def test_flink_rest_url_env_var_used(self, mock_resolve, mock_env, mock_call, runner):
        result = runner.invoke(status, [
            "compiled/group_bys/team/my_gb",
            "--mode", "streaming",
            "--online-jar", "/tmp/my.jar",
        ], env={"FLINK_REST_URL": "http://flink-from-env:8081"})
        assert result.exit_code == 0
        cmd = mock_call.call_args[0][0]
        assert "--flink-url" in cmd
        assert "http://flink-from-env:8081" in cmd
