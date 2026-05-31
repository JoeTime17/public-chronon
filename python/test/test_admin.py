import base64
import json
import os
import re
import tarfile
import tempfile
from unittest.mock import MagicMock, call, patch

import pytest

from click.testing import CliRunner

from ai.chronon.repo.admin import (
    _EKS_NAMESPACE,
    _EKS_ROLLOUT_TIMEOUT,
    _EKS_SERVICES,
    _app_images,
    _authenticate_docker_hub,
    _base_domain,
    _creds_from_helper,
    _extract_jars_from_oci_archive,
    _extract_scripts_from_layer,
    _get_current_image,
    _get_docker_credentials,
    _parse_registry,
    _should_update_latest,
    _upgrade_eks_services,
    _upload_jars_to_store,
    _upload_scripts_to_store,
    admin,
)
from ai.chronon.repo.constants import FLINK_IMAGE_TAG
from ai.chronon.repo.registry_client import DOCKER_HUB_REGISTRY


def strip_ansi(text):
    ansi_escape = re.compile(r'\x1b\[[0-9;]*[mGJKHF]')
    return ansi_escape.sub('', text)

# --- _parse_registry ---


class TestParseRegistry:
    def test_gcloud_style_with_path(self):
        host, prefix = _parse_registry("us-central1-docker.pkg.dev/canary-443022/canary-images")
        assert host == "us-central1-docker.pkg.dev"
        assert prefix == "canary-443022/canary-images"

    def test_oci_style_hostname_only(self):
        host, prefix = _parse_registry("us-central1-docker.pkg.dev")
        assert host == "us-central1-docker.pkg.dev"
        assert prefix == ""

    def test_with_https_scheme(self):
        host, prefix = _parse_registry("https://us-central1-docker.pkg.dev/project/repo")
        assert host == "us-central1-docker.pkg.dev"
        assert prefix == "project/repo"

    def test_ecr(self):
        host, prefix = _parse_registry("123456789.dkr.ecr.us-east-1.amazonaws.com")
        assert host == "123456789.dkr.ecr.us-east-1.amazonaws.com"
        assert prefix == ""

    def test_trailing_slash_stripped(self):
        host, prefix = _parse_registry("us-central1-docker.pkg.dev/project/repo/")
        assert host == "us-central1-docker.pkg.dev"
        assert prefix == "project/repo"

    def test_scheme_only_hostname(self):
        host, prefix = _parse_registry("https://ghcr.io")
        assert host == "ghcr.io"
        assert prefix == ""


# --- _base_domain ---


class TestBaseDomain:
    def test_full_url_with_path(self):
        assert _base_domain("https://index.docker.io/v1/") == "docker.io"

    def test_bare_hostname(self):
        assert _base_domain("registry-1.docker.io") == "docker.io"

    def test_subdomain_hostname(self):
        assert _base_domain("index.docker.io") == "docker.io"

    def test_gcr_artifact_registry(self):
        assert _base_domain("us-docker.pkg.dev") == "pkg.dev"

    def test_ecr(self):
        assert _base_domain("123456789.dkr.ecr.us-east-1.amazonaws.com") == "amazonaws.com"

    def test_ghcr(self):
        assert _base_domain("ghcr.io") == "ghcr.io"

    def test_single_label(self):
        assert _base_domain("localhost") == "localhost"

    def test_docker_hub_entries_match_each_other(self):
        """The canonical docker login URL and the registry API hostname should resolve to the same base domain."""
        assert _base_domain("https://index.docker.io/v1/") == _base_domain("registry-1.docker.io")
        assert _base_domain("https://index.docker.io/v1/") == _base_domain(DOCKER_HUB_REGISTRY)


# --- _get_docker_credentials ---


class TestGetDockerCredentials:
    def test_no_config_file(self, tmp_path):
        with patch.dict(os.environ, {"HOME": str(tmp_path)}):
            with patch("ai.chronon.repo.admin.os.path.expanduser", return_value=str(tmp_path)):
                assert _get_docker_credentials("registry-1.docker.io") == (None, None)

    def test_inline_auths(self, tmp_path):
        auth_value = base64.b64encode(b"myuser:mypass").decode()
        config = {
            "auths": {
                "https://index.docker.io/v1/": {"auth": auth_value}
            }
        }
        docker_dir = tmp_path / ".docker"
        docker_dir.mkdir()
        (docker_dir / "config.json").write_text(json.dumps(config))

        with patch("ai.chronon.repo.admin.os.path.expanduser", return_value=str(tmp_path)):
            username, password = _get_docker_credentials("registry-1.docker.io")
            assert username == "myuser"
            assert password == "mypass"

    def test_inline_auths_no_match(self, tmp_path):
        auth_value = base64.b64encode(b"myuser:mypass").decode()
        config = {
            "auths": {
                "https://index.docker.io/v1/": {"auth": auth_value}
            }
        }
        docker_dir = tmp_path / ".docker"
        docker_dir.mkdir()
        (docker_dir / "config.json").write_text(json.dumps(config))

        with patch("ai.chronon.repo.admin.os.path.expanduser", return_value=str(tmp_path)):
            assert _get_docker_credentials("ghcr.io") == (None, None)

    def test_cred_helpers(self, tmp_path):
        config = {
            "credHelpers": {
                "https://index.docker.io/v1/": "osxkeychain"
            }
        }
        docker_dir = tmp_path / ".docker"
        docker_dir.mkdir()
        (docker_dir / "config.json").write_text(json.dumps(config))

        with patch("ai.chronon.repo.admin.os.path.expanduser", return_value=str(tmp_path)):
            with patch("ai.chronon.repo.admin._creds_from_helper", return_value=("helperuser", "helperpass")) as mock:
                username, password = _get_docker_credentials("registry-1.docker.io")
                assert username == "helperuser"
                assert password == "helperpass"
                mock.assert_called_once_with("osxkeychain", "https://index.docker.io/v1/")

    def test_creds_store(self, tmp_path):
        config = {
            "credsStore": "desktop",
            "auths": {
                "https://index.docker.io/v1/": {}
            }
        }
        docker_dir = tmp_path / ".docker"
        docker_dir.mkdir()
        (docker_dir / "config.json").write_text(json.dumps(config))

        with patch("ai.chronon.repo.admin.os.path.expanduser", return_value=str(tmp_path)):
            with patch("ai.chronon.repo.admin._creds_from_helper", return_value=("storeuser", "storepass")) as mock:
                username, password = _get_docker_credentials("registry-1.docker.io")
                assert username == "storeuser"
                assert password == "storepass"
                mock.assert_called_once_with("desktop", "https://index.docker.io/v1/")

    def test_cred_helpers_takes_priority_over_creds_store(self, tmp_path):
        config = {
            "credsStore": "desktop",
            "credHelpers": {
                "https://index.docker.io/v1/": "osxkeychain"
            },
            "auths": {
                "https://index.docker.io/v1/": {}
            }
        }
        docker_dir = tmp_path / ".docker"
        docker_dir.mkdir()
        (docker_dir / "config.json").write_text(json.dumps(config))

        with patch("ai.chronon.repo.admin.os.path.expanduser", return_value=str(tmp_path)):
            with patch("ai.chronon.repo.admin._creds_from_helper", return_value=("helperuser", "helperpass")) as mock:
                _get_docker_credentials("registry-1.docker.io")
                mock.assert_called_once_with("osxkeychain", "https://index.docker.io/v1/")

    def test_malformed_base64_skipped(self, tmp_path):
        config = {
            "auths": {
                "https://index.docker.io/v1/": {"auth": "not-valid-base64!!!"}
            }
        }
        docker_dir = tmp_path / ".docker"
        docker_dir.mkdir()
        (docker_dir / "config.json").write_text(json.dumps(config))

        with patch("ai.chronon.repo.admin.os.path.expanduser", return_value=str(tmp_path)):
            assert _get_docker_credentials("registry-1.docker.io") == (None, None)

    def test_missing_colon_skipped(self, tmp_path):
        auth_value = base64.b64encode(b"nocolonhere").decode()
        config = {
            "auths": {
                "https://index.docker.io/v1/": {"auth": auth_value}
            }
        }
        docker_dir = tmp_path / ".docker"
        docker_dir.mkdir()
        (docker_dir / "config.json").write_text(json.dumps(config))

        with patch("ai.chronon.repo.admin.os.path.expanduser", return_value=str(tmp_path)):
            assert _get_docker_credentials("registry-1.docker.io") == (None, None)

    def test_non_docker_hub_registry(self, tmp_path):
        auth_value = base64.b64encode(b"gcruser:gcrpass").decode()
        config = {
            "auths": {
                "us-docker.pkg.dev": {"auth": auth_value}
            }
        }
        docker_dir = tmp_path / ".docker"
        docker_dir.mkdir()
        (docker_dir / "config.json").write_text(json.dumps(config))

        with patch("ai.chronon.repo.admin.os.path.expanduser", return_value=str(tmp_path)):
            username, password = _get_docker_credentials("us-docker.pkg.dev")
            assert username == "gcruser"
            assert password == "gcrpass"


# --- _creds_from_helper ---


class TestCredsFromHelper:
    def test_successful_helper_call(self):
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = json.dumps({"Username": "user1", "Secret": "secret1"})

        with patch("ai.chronon.repo.admin.subprocess.run", return_value=mock_result) as mock_run:
            username, password = _creds_from_helper("osxkeychain", "https://index.docker.io/v1/")
            assert username == "user1"
            assert password == "secret1"
            mock_run.assert_called_once_with(
                ["docker-credential-osxkeychain", "get"],
                input="https://index.docker.io/v1/",
                capture_output=True,
                text=True,
            )

    def test_helper_not_found(self):
        with patch("ai.chronon.repo.admin.subprocess.run", side_effect=FileNotFoundError):
            assert _creds_from_helper("missing", "https://index.docker.io/v1/") == (None, None)

    def test_helper_nonzero_exit(self):
        mock_result = MagicMock()
        mock_result.returncode = 1

        with patch("ai.chronon.repo.admin.subprocess.run", return_value=mock_result):
            assert _creds_from_helper("osxkeychain", "https://index.docker.io/v1/") == (None, None)


# --- _authenticate_docker_hub ---


class TestAuthenticateDockerHub:
    def test_with_api_token(self):
        client = MagicMock()
        _authenticate_docker_hub(client, "my-token")
        client.authenticate.assert_called_once_with(DOCKER_HUB_REGISTRY, username="ziplineai", password="my-token")

    def test_with_local_credentials(self):
        client = MagicMock()
        with patch("ai.chronon.repo.admin._get_docker_credentials", return_value=("localuser", "localpass")):
            _authenticate_docker_hub(client, None)
            client.authenticate.assert_called_once_with(DOCKER_HUB_REGISTRY, username="localuser", password="localpass")

    def test_no_credentials_raises(self):
        client = MagicMock()
        with patch("ai.chronon.repo.admin._get_docker_credentials", return_value=(None, None)):
            with pytest.raises(Exception, match="Docker Hub credentials not found"):
                _authenticate_docker_hub(client, None)

    def test_api_token_takes_priority(self):
        client = MagicMock()
        with patch("ai.chronon.repo.admin._get_docker_credentials", return_value=("localuser", "localpass")) as mock:
            _authenticate_docker_hub(client, "my-token")
            mock.assert_not_called()
            client.authenticate.assert_called_once_with(DOCKER_HUB_REGISTRY, username="ziplineai", password="my-token")


# --- install release resolution ---


class TestInstallReleaseResolution:
    """Tests for the default --release resolution logic in the install command."""

    @patch("ai.chronon.repo.admin._check_docker_available")
    @patch("ai.chronon.repo.admin._load_from_docker_hub")
    @patch("ai.chronon.repo.admin.get_package_version", return_value="1.0.15")
    def test_defaults_to_package_version(self, mock_version, mock_load, mock_docker):
        mock_load.return_value = []
        runner = CliRunner()
        result = runner.invoke(admin, ["install", "gcp", "--registry", "local"])
        assert result.exit_code == 0
        assert "Using release" in result.output and " 1.0.15 " in result.output
        mock_load.assert_called_once()
        assert mock_load.call_args[0][2] == "1.0.15"

    @patch("ai.chronon.repo.admin._check_docker_available")
    @patch("ai.chronon.repo.admin._load_from_docker_hub")
    @patch("ai.chronon.repo.admin.get_package_version", return_value="unknown")
    def test_falls_back_to_latest_when_package_not_installed(self, mock_version, mock_load, mock_docker):
        mock_load.return_value = []
        runner = CliRunner()
        result = runner.invoke(admin, ["install", "gcp", "--registry", "local"])
        assert result.exit_code == 0
        assert "Using release " in result.output and " latest " in result.output
        mock_load.assert_called_once()
        assert mock_load.call_args[0][2] == "latest"

    @patch("ai.chronon.repo.admin._check_docker_available")
    @patch("ai.chronon.repo.admin._load_from_docker_hub")
    @patch("ai.chronon.repo.admin.get_package_version", return_value="1.0.15")
    def test_explicit_release_matching_version_no_prompt(self, mock_version, mock_load, mock_docker):
        mock_load.return_value = []
        runner = CliRunner()
        result = runner.invoke(admin, ["install", "gcp", "--registry", "local", "--release", "1.0.15"])
        assert result.exit_code == 0
        assert "does not match" not in result.output
        mock_load.assert_called_once()
        assert mock_load.call_args[0][2] == "1.0.15"

    @patch("ai.chronon.repo.admin._check_docker_available")
    @patch("ai.chronon.repo.admin._load_from_docker_hub")
    @patch("ai.chronon.repo.admin.get_package_version", return_value="1.0.15")
    def test_mismatched_release_confirmed_proceeds(self, mock_version, mock_load, mock_docker):
        mock_load.return_value = []
        runner = CliRunner()
        result = runner.invoke(admin, ["install", "gcp", "--registry", "local", "--release", "1.0.14"], input="y\n")
        assert result.exit_code == 0
        assert "does not match installed zipline cli version 1.0.15" in result.output
        mock_load.assert_called_once()
        assert mock_load.call_args[0][2] == "1.0.14"

    @patch("ai.chronon.repo.admin._check_docker_available")
    @patch("ai.chronon.repo.admin._load_from_docker_hub")
    @patch("ai.chronon.repo.admin.get_package_version", return_value="1.0.15")
    def test_mismatched_release_declined_aborts(self, mock_version, mock_load, mock_docker):
        mock_load.return_value = []
        runner = CliRunner()
        result = runner.invoke(admin, ["install", "gcp", "--registry", "local", "--release", "1.0.14"], input="n\n")
        assert result.exit_code == 1
        assert "does not match installed zipline cli version 1.0.15" in result.output
        mock_load.assert_not_called()

    @patch("ai.chronon.repo.admin._check_docker_available")
    @patch("ai.chronon.repo.admin._load_from_docker_hub")
    @patch("ai.chronon.repo.admin.get_package_version", return_value="unknown")
    def test_mismatched_release_skips_prompt_when_version_unknown(self, mock_version, mock_load, mock_docker):
        mock_load.return_value = []
        runner = CliRunner()
        result = runner.invoke(admin, ["install", "gcp", "--registry", "local", "--release", "1.0.14"])
        assert result.exit_code == 0
        assert "does not match" not in result.output
        mock_load.assert_called_once()
        assert mock_load.call_args[0][2] == "1.0.14"


# --- _should_update_latest ---


class TestShouldUpdateLatest:
    @patch("ai.chronon.repo.admin.read_from_blob_store", return_value=None)
    def test_no_existing_marker_returns_true(self, mock_read):
        assert _should_update_latest("1.0.0", "gs://bucket/store") is True
        mock_read.assert_called_once_with("gs://bucket/store/release/latest/VERSION")

    @patch("ai.chronon.repo.admin.read_from_blob_store", return_value="1.0.0")
    def test_newer_version_returns_true(self, mock_read):
        assert _should_update_latest("2.0.0", "gs://bucket/store") is True

    @patch("ai.chronon.repo.admin.read_from_blob_store", return_value="1.0.0")
    def test_same_version_returns_true(self, mock_read):
        assert _should_update_latest("1.0.0", "gs://bucket/store") is True

    @patch("ai.chronon.repo.admin.read_from_blob_store", return_value="2.0.0")
    def test_older_version_returns_false(self, mock_read):
        assert _should_update_latest("1.0.0", "gs://bucket/store") is False

    @patch("ai.chronon.repo.admin.read_from_blob_store", return_value="1.0.0")
    def test_v_prefix_stripped_from_release(self, mock_read):
        assert _should_update_latest("v2.0.0", "gs://bucket/store") is True

    @patch("ai.chronon.repo.admin.read_from_blob_store", return_value="v1.0.0\n")
    def test_v_prefix_and_whitespace_stripped_from_marker(self, mock_read):
        assert _should_update_latest("2.0.0", "gs://bucket/store") is True

    @patch("ai.chronon.repo.admin.read_from_blob_store", return_value="not-a-version")
    def test_invalid_existing_marker_returns_true(self, mock_read):
        assert _should_update_latest("1.0.0", "gs://bucket/store") is True

    def test_invalid_release_version_returns_false(self):
        assert _should_update_latest("not-a-version", "gs://bucket/store") is False

    @patch("ai.chronon.repo.admin.read_from_blob_store", return_value=None)
    def test_trailing_slash_stripped_from_store(self, mock_read):
        _should_update_latest("1.0.0", "s3://bucket/store/")
        mock_read.assert_called_once_with("s3://bucket/store/release/latest/VERSION")


# --- _upload_jars_to_store ---


class TestUploadJarsToStore:
    def test_nonexistent_dir_returns_empty(self, tmp_path):
        results = _upload_jars_to_store(str(tmp_path / "nope"), "1.0.0", "gs://b/store")
        assert results == []

    @patch("ai.chronon.repo.admin._should_update_latest", return_value=True)
    @patch("ai.chronon.repo.admin.upload_to_blob_store")
    def test_uploads_versioned_and_latest(self, mock_upload, mock_latest, tmp_path):
        jar = tmp_path / "foo.jar"
        jar.write_text("data")
        results = _upload_jars_to_store(str(tmp_path), "1.0.0", "gs://b/store")
        assert len(results) == 1
        assert results[0][3] == "ok"
        # versioned + latest jar + VERSION marker = 3 uploads
        assert mock_upload.call_count == 3
        paths = [call.args[1] for call in mock_upload.call_args_list]
        assert "gs://b/store/release/1.0.0/jars/foo.jar" in paths
        assert "gs://b/store/release/latest/jars/foo.jar" in paths
        assert "gs://b/store/release/latest/VERSION" in paths

    @patch("ai.chronon.repo.admin._should_update_latest", return_value=False)
    @patch("ai.chronon.repo.admin.upload_to_blob_store")
    def test_skips_latest_when_older(self, mock_upload, mock_latest, tmp_path):
        jar = tmp_path / "foo.jar"
        jar.write_text("data")
        results = _upload_jars_to_store(str(tmp_path), "0.9.0", "gs://b/store")
        assert len(results) == 1
        assert results[0][3] == "ok"
        # Only versioned upload, no latest
        assert mock_upload.call_count == 1
        assert mock_upload.call_args.args[1] == "gs://b/store/release/0.9.0/jars/foo.jar"

    @patch("ai.chronon.repo.admin._should_update_latest", return_value=True)
    @patch("ai.chronon.repo.admin.upload_to_blob_store")
    def test_multiple_jars(self, mock_upload, mock_latest, tmp_path):
        (tmp_path / "a.jar").write_text("a")
        (tmp_path / "b.jar").write_text("b")
        results = _upload_jars_to_store(str(tmp_path), "1.0.0", "gs://b/store")
        assert len(results) == 2
        assert all(r[3] == "ok" for r in results)
        # 2 versioned + 2 latest + 1 VERSION marker = 5 uploads
        assert mock_upload.call_count == 5

    @patch("ai.chronon.repo.admin._should_update_latest", return_value=True)
    @patch("ai.chronon.repo.admin.upload_to_blob_store", side_effect=Exception("upload failed"))
    def test_upload_failure_recorded(self, mock_upload, mock_latest, tmp_path):
        (tmp_path / "foo.jar").write_text("data")
        results = _upload_jars_to_store(str(tmp_path), "1.0.0", "gs://b/store")
        assert len(results) == 1
        assert "FAILED" in results[0][3]

    @patch("ai.chronon.repo.admin._should_update_latest", return_value=True)
    @patch("ai.chronon.repo.admin.upload_to_blob_store")
    def test_version_marker_not_written_on_failure(self, mock_upload, mock_latest, tmp_path):
        (tmp_path / "foo.jar").write_text("data")
        # First call (versioned) succeeds, second call (latest jar) raises
        mock_upload.side_effect = [None, Exception("fail")]
        results = _upload_jars_to_store(str(tmp_path), "1.0.0", "gs://b/store")
        assert "FAILED" in results[0][3]
        # No VERSION marker write attempted since results contain a failure
        paths = [call.args[1] for call in mock_upload.call_args_list]
        assert "gs://b/store/release/latest/VERSION" not in paths


# --- _extract_scripts_from_layer ---


class TestExtractScriptsFromLayer:
    def test_extracts_scripts_for_matching_cloud(self, tmp_path):
        layer_path = tmp_path / "layer.tar"
        dest_dir = tmp_path / "dest"
        dest_dir.mkdir()

        with tarfile.open(layer_path, "w") as tar:
            script_file = tmp_path / "test_script.sh"
            script_file.write_text("#!/bin/bash\necho test")
            tar.add(script_file, arcname="scripts/gcp/test_script.sh")

            other_script = tmp_path / "other_script.sh"
            other_script.write_text("#!/bin/bash\necho other")
            tar.add(other_script, arcname="scripts/aws/other_script.sh")

        _extract_scripts_from_layer(str(layer_path), str(dest_dir), "gcp")

        extracted_script = dest_dir / "scripts" / "gcp" / "test_script.sh"
        assert extracted_script.exists()
        assert extracted_script.read_text() == "#!/bin/bash\necho test"

        not_extracted_script = dest_dir / "scripts" / "aws" / "other_script.sh"
        assert not not_extracted_script.exists()

    def test_extracts_nested_directories(self, tmp_path):
        layer_path = tmp_path / "layer.tar"
        dest_dir = tmp_path / "dest"
        dest_dir.mkdir()

        with tarfile.open(layer_path, "w") as tar:
            nested_script = tmp_path / "nested.py"
            nested_script.write_text("print('hello')")
            tar.add(nested_script, arcname="scripts/azure/subdir/nested.py")

        _extract_scripts_from_layer(str(layer_path), str(dest_dir), "azure")

        extracted_script = dest_dir / "scripts" / "azure" / "subdir" / "nested.py"
        assert extracted_script.exists()
        assert extracted_script.read_text() == "print('hello')"

    def test_ignores_non_matching_cloud(self, tmp_path):
        layer_path = tmp_path / "layer.tar"
        dest_dir = tmp_path / "dest"
        dest_dir.mkdir()

        with tarfile.open(layer_path, "w") as tar:
            script_file = tmp_path / "script.sh"
            script_file.write_text("#!/bin/bash\necho test")
            tar.add(script_file, arcname="scripts/gcp/script.sh")

        _extract_scripts_from_layer(str(layer_path), str(dest_dir), "aws")

        extracted_script = dest_dir / "scripts" / "gcp" / "script.sh"
        assert not extracted_script.exists()

    def test_handles_empty_tar(self, tmp_path):
        layer_path = tmp_path / "layer.tar"
        dest_dir = tmp_path / "dest"
        dest_dir.mkdir()

        with tarfile.open(layer_path, "w") as tar:
            pass

        _extract_scripts_from_layer(str(layer_path), str(dest_dir), "gcp")

        assert not (dest_dir / "scripts").exists()

    def test_handles_corrupt_tar(self, tmp_path):
        layer_path = tmp_path / "corrupt.tar"
        layer_path.write_text("not a valid tar file")
        dest_dir = tmp_path / "dest"
        dest_dir.mkdir()

        _extract_scripts_from_layer(str(layer_path), str(dest_dir), "gcp")


# --- _upload_scripts_to_store ---


class TestUploadScriptsToStore:
    def test_uploads_scripts_for_cloud(self, tmp_path):
        scripts_dir = tmp_path / "extract"
        scripts_cloud_dir = scripts_dir / "scripts" / "gcp"
        scripts_cloud_dir.mkdir(parents=True)

        (scripts_cloud_dir / "script1.sh").write_text("#!/bin/bash\necho 1")
        (scripts_cloud_dir / "script2.py").write_text("print('hello')")

        with patch("ai.chronon.repo.admin.upload_to_blob_store") as mock_upload:
            results = _upload_scripts_to_store(str(scripts_dir), "gcp", "1.0.0", "gs://bucket/zipline")

        assert len(results) == 2
        assert all(status == "ok" for _, _, _, status in results)

        uploaded_paths = [call_args[0][1] for call_args in mock_upload.call_args_list]
        assert "gs://bucket/zipline/release/1.0.0/scripts/gcp/script1.sh" in uploaded_paths
        assert "gs://bucket/zipline/release/1.0.0/scripts/gcp/script2.py" in uploaded_paths

    def test_uploads_nested_directory_structure(self, tmp_path):
        scripts_dir = tmp_path / "extract"
        scripts_cloud_dir = scripts_dir / "scripts" / "aws"
        subdir = scripts_cloud_dir / "subdir"
        subdir.mkdir(parents=True)

        (scripts_cloud_dir / "top_level.sh").write_text("#!/bin/bash")
        (subdir / "nested.sh").write_text("#!/bin/bash")

        with patch("ai.chronon.repo.admin.upload_to_blob_store") as mock_upload:
            results = _upload_scripts_to_store(str(scripts_dir), "aws", "1.0.0", "s3://bucket/zipline")

        assert len(results) == 2
        uploaded_paths = [call_args[0][1] for call_args in mock_upload.call_args_list]
        assert "s3://bucket/zipline/release/1.0.0/scripts/aws/top_level.sh" in uploaded_paths
        assert "s3://bucket/zipline/release/1.0.0/scripts/aws/subdir/nested.sh" in uploaded_paths

    def test_handles_missing_scripts_directory(self, tmp_path):
        scripts_dir = tmp_path / "extract"
        scripts_dir.mkdir()

        with patch("ai.chronon.repo.admin.upload_to_blob_store") as mock_upload:
            results = _upload_scripts_to_store(str(scripts_dir), "gcp", "1.0.0", "gs://bucket/zipline")

        assert len(results) == 0
        mock_upload.assert_not_called()

    def test_handles_upload_failure(self, tmp_path):
        scripts_dir = tmp_path / "extract"
        scripts_cloud_dir = scripts_dir / "scripts" / "azure"
        scripts_cloud_dir.mkdir(parents=True)

        (scripts_cloud_dir / "script.sh").write_text("#!/bin/bash")

        with patch("ai.chronon.repo.admin.upload_to_blob_store", side_effect=Exception("Upload failed")):
            results = _upload_scripts_to_store(str(scripts_dir), "azure", "1.0.0", "gs://bucket/zipline")

        assert len(results) == 1
        assert results[0][3].startswith("FAILED:")

    @patch("ai.chronon.repo.admin._should_update_latest", return_value=False)
    def test_strips_trailing_slash_from_artifact_store(self, mock_latest, tmp_path):
        scripts_dir = tmp_path / "extract"
        scripts_cloud_dir = scripts_dir / "scripts" / "gcp"
        scripts_cloud_dir.mkdir(parents=True)

        (scripts_cloud_dir / "script.sh").write_text("#!/bin/bash")

        with patch("ai.chronon.repo.admin.upload_to_blob_store") as mock_upload:
            _upload_scripts_to_store(str(scripts_dir), "gcp", "1.0.0", "gs://bucket/zipline/")

        uploaded_path = mock_upload.call_args[0][1]
        assert uploaded_path == "gs://bucket/zipline/release/1.0.0/scripts/gcp/script.sh"


# --- _extract_jars_from_oci_archive (integration with scripts) ---


class TestExtractJarsFromOciArchive:
    def test_extracts_both_jars_and_scripts(self, tmp_path):
        archive_path = tmp_path / "engine-gcp.tar"
        oci_dir = tmp_path / "oci_build"
        oci_dir.mkdir()

        jars_dir = tmp_path / "jars_build"
        jars_dir.mkdir()
        (jars_dir / "engine.jar").write_text("fake jar content")

        scripts_build = tmp_path / "scripts_build"
        scripts_gcp = scripts_build / "gcp"
        scripts_gcp.mkdir(parents=True)
        (scripts_gcp / "opsagent_setup.sh").write_text("#!/bin/bash\nstart")

        layer_tar = oci_dir / "layer.tar"
        with tarfile.open(layer_tar, "w") as tar:
            tar.add(jars_dir / "engine.jar", arcname="jars/engine.jar")
            tar.add(scripts_gcp / "opsagent_setup.sh", arcname="scripts/gcp/opsagent_setup.sh")

        manifest = [{"Layers": ["layer.tar"]}]
        (oci_dir / "manifest.json").write_text(json.dumps(manifest))

        with tarfile.open(archive_path, "w") as tar:
            tar.add(oci_dir / "manifest.json", arcname="manifest.json")
            tar.add(layer_tar, arcname="layer.tar")

        with patch("ai.chronon.repo.admin.upload_to_blob_store") as mock_upload:
            results = _extract_jars_from_oci_archive(
                str(archive_path), "gcp", "1.0.0", "gs://bucket/zipline"
            )

        uploaded_paths = [call_args[0][1] for call_args in mock_upload.call_args_list]
        assert any("jars/engine.jar" in path for path in uploaded_paths)
        assert any("scripts/gcp/opsagent_setup.sh" in path for path in uploaded_paths)
        assert all(status == "ok" for _, _, _, status in results)

    def test_handles_no_manifests(self, tmp_path):
        archive_path = tmp_path / "engine-aws.tar"
        oci_dir = tmp_path / "oci_build"
        oci_dir.mkdir()

        (oci_dir / "manifest.json").write_text(json.dumps([]))

        with tarfile.open(archive_path, "w") as tar:
            tar.add(oci_dir / "manifest.json", arcname="manifest.json")

        results = _extract_jars_from_oci_archive(
            str(archive_path), "aws", "1.0.0", "s3://bucket/zipline"
        )

        assert len(results) == 1
        assert results[0][3] == "FAILED: no manifests in engine archive"


# --- _download_and_upload_public_jars ---


class TestDownloadAndUploadPublicJars:
    @patch("ai.chronon.repo.admin.blob_exists", return_value=True)
    @patch("ai.chronon.repo.admin.get_public_spark_jars_for_admin", return_value=["https://repo1.maven.org/maven2/test.jar"])
    def test_all_jars_exist_skips_download(self, mock_get_jars, mock_exists):
        """Test that when all jars exist, no downloads are performed."""
        from rich.progress import Progress

        from ai.chronon.repo.admin import _download_and_upload_public_jars

        with Progress() as progress:
            results = _download_and_upload_public_jars("s3://bucket/store", progress)

        assert len(results) == 1
        assert results[0][3] == "already exists"
        mock_exists.assert_called_once_with("s3://bucket/store/spark-3.5.3/libs/test.jar")

    @patch("urllib.request.urlretrieve")
    @patch("ai.chronon.repo.admin.upload_to_blob_store")
    @patch("ai.chronon.repo.admin.blob_exists", return_value=False)
    @patch("ai.chronon.repo.admin.get_public_spark_jars_for_admin", return_value=["https://repo1.maven.org/maven2/test.jar"])
    def test_missing_jars_are_downloaded_and_uploaded(self, mock_get_jars, mock_exists, mock_upload, mock_download):
        """Test that missing jars are downloaded from Maven and uploaded to blob store."""
        from rich.progress import Progress

        from ai.chronon.repo.admin import _download_and_upload_public_jars

        with Progress() as progress:
            results = _download_and_upload_public_jars("s3://bucket/store", progress)

        assert len(results) == 1
        assert results[0][3] == "ok"
        mock_download.assert_called_once()
        mock_upload.assert_called_once()

        upload_call_args = mock_upload.call_args
        assert upload_call_args[0][1] == "s3://bucket/store/spark-3.5.3/libs/test.jar"

    @patch("ai.chronon.repo.admin.blob_exists", side_effect=[True, False, True])
    @patch("urllib.request.urlretrieve")
    @patch("ai.chronon.repo.admin.upload_to_blob_store")
    @patch(
        "ai.chronon.repo.admin.get_public_spark_jars_for_admin",
        return_value=[
            "https://repo1.maven.org/maven2/jar1.jar",
            "https://repo1.maven.org/maven2/jar2.jar",
            "https://repo1.maven.org/maven2/jar3.jar",
        ],
    )
    def test_only_missing_jars_downloaded(self, mock_get_jars, mock_upload, mock_download, mock_exists):
        """Test that only missing jars are downloaded, existing ones are skipped."""
        from rich.progress import Progress

        from ai.chronon.repo.admin import _download_and_upload_public_jars

        with Progress() as progress:
            results = _download_and_upload_public_jars("gs://bucket/store", progress)

        assert len(results) == 3
        # "already exists" results are added first during check, then downloaded jars
        # jar1 exists (added first), jar3 exists (added second), jar2 downloaded (added last)
        existing_count = sum(1 for r in results if r[3] == "already exists")
        downloaded_count = sum(1 for r in results if r[3] == "ok")
        assert existing_count == 2
        assert downloaded_count == 1

        # Only jar2 should be downloaded and uploaded
        assert mock_download.call_count == 1
        assert mock_upload.call_count == 1

    @patch("urllib.request.urlretrieve", side_effect=Exception("Download failed"))
    @patch("ai.chronon.repo.admin.blob_exists", return_value=False)
    @patch("ai.chronon.repo.admin.get_public_spark_jars_for_admin", return_value=["https://repo1.maven.org/maven2/test.jar"])
    def test_download_failure_recorded(self, mock_get_jars, mock_exists, mock_download):
        """Test that download failures are properly recorded in results."""
        from rich.progress import Progress

        from ai.chronon.repo.admin import _download_and_upload_public_jars

        with Progress() as progress:
            results = _download_and_upload_public_jars("s3://bucket/store", progress)

        assert len(results) == 1
        assert "FAILED" in results[0][3]

    @patch("urllib.request.urlretrieve")
    @patch("ai.chronon.repo.admin.upload_to_blob_store", side_effect=Exception("Upload failed"))
    @patch("ai.chronon.repo.admin.blob_exists", return_value=False)
    @patch("ai.chronon.repo.admin.get_public_spark_jars_for_admin", return_value=["https://repo1.maven.org/maven2/test.jar"])
    def test_upload_failure_recorded(self, mock_get_jars, mock_exists, mock_upload, mock_download):
        """Test that upload failures are properly recorded in results."""
        from rich.progress import Progress

        from ai.chronon.repo.admin import _download_and_upload_public_jars

        with Progress() as progress:
            results = _download_and_upload_public_jars("s3://bucket/store", progress)

        assert len(results) == 1
        assert "FAILED" in results[0][3]

    @patch("urllib.request.urlretrieve")
    @patch("ai.chronon.repo.admin.upload_to_blob_store")
    @patch("ai.chronon.repo.admin.blob_exists", return_value=False)
    @patch("ai.chronon.repo.admin.get_public_spark_jars_for_admin", return_value=["https://repo1.maven.org/maven2/test.jar"])
    def test_strips_trailing_slash_from_artifact_store(self, mock_get_jars, mock_exists, mock_upload, mock_download):
        """Test that trailing slashes are properly stripped from artifact store paths."""
        from rich.progress import Progress

        from ai.chronon.repo.admin import _download_and_upload_public_jars

        with Progress() as progress:
            _download_and_upload_public_jars("s3://bucket/store/", progress)

        upload_call_args = mock_upload.call_args
        # Should not have double slashes
        assert upload_call_args[0][1] == "s3://bucket/store/spark-3.5.3/libs/test.jar"

    @patch("ai.chronon.repo.admin.blob_exists", return_value=False)
    @patch("urllib.request.urlretrieve")
    @patch("ai.chronon.repo.admin.upload_to_blob_store")
    @patch(
        "ai.chronon.repo.admin.get_public_spark_jars_for_admin",
        return_value=["https://repo1.maven.org/maven2/commons-collections4-4.4.jar"],
    )
    def test_extracts_jar_filename_correctly(self, mock_get_jars, mock_upload, mock_download, mock_exists):
        """Test that jar filename is correctly extracted from Maven URL."""
        from rich.progress import Progress

        from ai.chronon.repo.admin import _download_and_upload_public_jars

        with Progress() as progress:
            _download_and_upload_public_jars("s3://bucket/store", progress)

        upload_call_args = mock_upload.call_args
        assert upload_call_args[0][1].endswith("spark-3.5.3/libs/commons-collections4-4.4.jar")


# --- _upgrade_eks_services ---


def _make_subprocess_result(returncode=0, stdout="", stderr=""):
    result = MagicMock()
    result.returncode = returncode
    result.stdout = stdout
    result.stderr = stderr
    return result


class TestUpgradeEksServices:
    @patch("ai.chronon.repo.admin.shutil.which", return_value=None)
    def test_exits_when_kubectl_not_found(self, mock_which, capsys):
        with pytest.raises(SystemExit):
            _upgrade_eks_services("aws", "1.0.0")
        mock_which.assert_called_once_with("kubectl")
        output = strip_ansi(capsys.readouterr().out)
        assert "kubectl not found" in output
        assert "retry" in output.lower()

    @patch("ai.chronon.repo.admin.shutil.which", return_value=None)
    def test_kubectl_not_found_shows_install_link(self, mock_which, capsys):
        with pytest.raises(SystemExit):
            _upgrade_eks_services("aws", "1.0.0")
        output = strip_ansi(capsys.readouterr().out)
        assert "kubernetes.io" in output

    @patch("ai.chronon.repo.admin.subprocess.run")
    @patch("ai.chronon.repo.admin.shutil.which", return_value="/usr/local/bin/kubectl")
    def test_exits_when_cluster_unreachable(self, mock_which, mock_run, capsys):
        mock_run.return_value = _make_subprocess_result(returncode=1, stderr="connection refused")
        with pytest.raises(SystemExit):
            _upgrade_eks_services("aws", "1.0.0")
        output = strip_ansi(capsys.readouterr().out)
        assert "Cannot reach Kubernetes cluster" in output

    @patch("ai.chronon.repo.admin.subprocess.run")
    @patch("ai.chronon.repo.admin.shutil.which", return_value="/usr/local/bin/kubectl")
    def test_cluster_unreachable_shows_kubeconfig_help(self, mock_which, mock_run, capsys):
        mock_run.return_value = _make_subprocess_result(returncode=1, stderr="connection refused")
        with pytest.raises(SystemExit):
            _upgrade_eks_services("aws", "1.0.0")
        output = strip_ansi(capsys.readouterr().out)
        assert "aws eks update-kubeconfig" in output

    @patch("ai.chronon.repo.admin.subprocess.run")
    @patch("ai.chronon.repo.admin.shutil.which", return_value="/usr/local/bin/kubectl")
    def test_skips_deployment_not_found(self, mock_which, mock_run, capsys):
        """All deployments missing — each one should print 'not found' with explanation."""
        def side_effect(cmd, **kwargs):
            if "cluster-info" in cmd:
                return _make_subprocess_result(returncode=0)
            if "get" in cmd and "deployment" in cmd:
                return _make_subprocess_result(returncode=1, stderr="NotFound")
            return _make_subprocess_result(returncode=0)

        mock_run.side_effect = side_effect
        _upgrade_eks_services("aws", "1.0.0")
        output = strip_ansi(capsys.readouterr().out)
        for service, (deployment, _, _) in _EKS_SERVICES.items():
            assert f"deployment/{deployment} not found" in output
        assert "not been deployed yet" in output

    @patch("ai.chronon.repo.admin.subprocess.run")
    @patch("ai.chronon.repo.admin.shutil.which", return_value="/usr/local/bin/kubectl")
    def test_successful_upgrade_all_services(self, mock_which, mock_run, capsys):
        mock_run.return_value = _make_subprocess_result(returncode=0)
        _upgrade_eks_services("aws", "1.0.0")
        output = strip_ansi(capsys.readouterr().out)
        assert "EKS Service Upgrade" in output
        assert "Previous" in output
        assert "Current" in output
        assert "rollout complete" in output
        assert "ok" in output
        assert "failed" not in output.lower()

    @patch("ai.chronon.repo.admin.subprocess.run")
    @patch("ai.chronon.repo.admin.shutil.which", return_value="/usr/local/bin/kubectl")
    def test_table_shows_previous_version(self, mock_which, mock_run, capsys):
        """Table should show the previous version tag for upgraded services."""
        def side_effect(cmd, **kwargs):
            if "cluster-info" in cmd:
                return _make_subprocess_result(returncode=0)
            if "get" in cmd and "jsonpath" in str(cmd):
                return _make_subprocess_result(returncode=0, stdout="ziplineai/hub-aws:0.9.0")
            if "get" in cmd:
                return _make_subprocess_result(returncode=0)
            if "set" in cmd or "rollout" in cmd:
                return _make_subprocess_result(returncode=0)
            return _make_subprocess_result(returncode=0)

        mock_run.side_effect = side_effect
        _upgrade_eks_services("aws", "1.0.0")
        output = strip_ansi(capsys.readouterr().out)
        assert "0.9.0" in output
        assert "1.0.0" in output

    @patch("ai.chronon.repo.admin.subprocess.run")
    @patch("ai.chronon.repo.admin.shutil.which", return_value="/usr/local/bin/kubectl")
    def test_set_image_failure_exits_with_retry_message(self, mock_which, mock_run, capsys):
        def side_effect(cmd, **kwargs):
            if "cluster-info" in cmd:
                return _make_subprocess_result(returncode=0)
            if "get" in cmd:
                return _make_subprocess_result(returncode=0)
            if "set" in cmd:
                return _make_subprocess_result(returncode=1, stderr="unauthorized")
            return _make_subprocess_result(returncode=0)

        mock_run.side_effect = side_effect
        with pytest.raises(SystemExit):
            _upgrade_eks_services("aws", "1.0.0")
        output = strip_ansi(capsys.readouterr().out)
        assert "FAIL" in output
        assert "unauthorized" in output
        assert "retry" in output.lower()

    @patch("ai.chronon.repo.admin.subprocess.run")
    @patch("ai.chronon.repo.admin.shutil.which", return_value="/usr/local/bin/kubectl")
    def test_rollout_timeout_exits_with_retry_message(self, mock_which, mock_run, capsys):
        def side_effect(cmd, **kwargs):
            if "cluster-info" in cmd:
                return _make_subprocess_result(returncode=0)
            if "get" in cmd:
                return _make_subprocess_result(returncode=0)
            if "set" in cmd:
                return _make_subprocess_result(returncode=0)
            if "rollout" in cmd:
                return _make_subprocess_result(returncode=1, stderr="timed out waiting")
            return _make_subprocess_result(returncode=0)

        mock_run.side_effect = side_effect
        with pytest.raises(SystemExit):
            _upgrade_eks_services("aws", "1.0.0")
        output = strip_ansi(capsys.readouterr().out)
        assert "FAIL" in output
        assert "timed out" in output
        assert "retry" in output.lower()

    @patch("ai.chronon.repo.admin.subprocess.run")
    @patch("ai.chronon.repo.admin.shutil.which", return_value="/usr/local/bin/kubectl")
    def test_mixed_success_and_failure(self, mock_which, mock_run, capsys):
        """Some services succeed, some fail — both should appear in the summary table."""
        call_count = {"set": 0}

        def side_effect(cmd, **kwargs):
            if "cluster-info" in cmd:
                return _make_subprocess_result(returncode=0)
            if "get" in cmd:
                return _make_subprocess_result(returncode=0)
            if "set" in cmd:
                call_count["set"] += 1
                if call_count["set"] == 1:
                    return _make_subprocess_result(returncode=1, stderr="image pull error")
                return _make_subprocess_result(returncode=0)
            if "rollout" in cmd:
                return _make_subprocess_result(returncode=0)
            return _make_subprocess_result(returncode=0)

        mock_run.side_effect = side_effect
        with pytest.raises(SystemExit):
            _upgrade_eks_services("aws", "1.0.0")
        output = strip_ansi(capsys.readouterr().out)
        assert "FAIL" in output
        assert "rollout complete" in output

    @patch("ai.chronon.repo.admin.subprocess.run")
    @patch("ai.chronon.repo.admin.shutil.which", return_value="/usr/local/bin/kubectl")
    def test_restarts_services_already_on_target_version(self, mock_which, mock_run, capsys):
        """Services already on target image should be restarted via rollout restart."""
        def side_effect(cmd, **kwargs):
            if "cluster-info" in cmd:
                return _make_subprocess_result(returncode=0)
            if "get" in cmd and "deployment" in cmd and "jsonpath" not in str(cmd):
                return _make_subprocess_result(returncode=0)
            if "jsonpath" in str(cmd):
                for _svc, (dep, _cont, tmpl) in _EKS_SERVICES.items():
                    if dep in str(cmd):
                        return _make_subprocess_result(
                            returncode=0, stdout=tmpl.format(cloud="aws") + ":1.0.0"
                        )
                return _make_subprocess_result(returncode=0, stdout="")
            return _make_subprocess_result(returncode=0)

        mock_run.side_effect = side_effect
        _upgrade_eks_services("aws", "1.0.0")
        output = strip_ansi(capsys.readouterr().out)
        assert "already on 1.0.0" in output
        assert "restarting" in output.lower()
        assert "restart complete" in output
        # Should use rollout restart, not set image
        restart_calls = [c for c in mock_run.call_args_list if "rollout" in c[0][0] and "restart" in c[0][0]]
        assert len(restart_calls) == len(_EKS_SERVICES)
        set_calls = [c for c in mock_run.call_args_list if "set" in c[0][0]]
        assert len(set_calls) == 0

    @patch("ai.chronon.repo.admin.subprocess.run")
    @patch("ai.chronon.repo.admin.shutil.which", return_value="/usr/local/bin/kubectl")
    def test_image_tags_use_correct_cloud_and_release(self, mock_which, mock_run, capsys):
        """Verify kubectl set image is called with the correct image:tag for each service."""
        set_image_calls = []

        def side_effect(cmd, **kwargs):
            if "cluster-info" in cmd:
                return _make_subprocess_result(returncode=0)
            if "get" in cmd:
                return _make_subprocess_result(returncode=0)
            if "set" in cmd:
                set_image_calls.append(cmd)
                return _make_subprocess_result(returncode=0)
            if "rollout" in cmd:
                return _make_subprocess_result(returncode=0)
            return _make_subprocess_result(returncode=0)

        mock_run.side_effect = side_effect
        _upgrade_eks_services("aws", "2.5.0")

        assert len(set_image_calls) == len(_EKS_SERVICES)
        # `kubectl --context X set image deployment/Y container=image:tag ...` —
        # find the `container=image:tag` arg by pattern rather than index, since
        # the prepended `--context X` flag shifts positional indices.
        for cmd in set_image_calls:
            container_image_arg = next(arg for arg in cmd if "=ziplineai/" in str(arg))
            assert ":2.5.0" in container_image_arg
        # Check cloud-specific images contain "aws"
        cloud_specific = [
            next(a for a in c if "=ziplineai/" in str(a))
            for c in set_image_calls
            if any("hub" in str(a) or "eval" in str(a) for a in c)
        ]
        for arg in cloud_specific:
            assert "aws" in arg

    @patch("ai.chronon.repo.admin.subprocess.run")
    @patch("ai.chronon.repo.admin.shutil.which", return_value="/usr/local/bin/kubectl")
    def test_uses_correct_namespace(self, mock_which, mock_run):
        """All kubectl calls should use the _EKS_NAMESPACE constant."""
        mock_run.return_value = _make_subprocess_result(returncode=0)
        _upgrade_eks_services("aws", "1.0.0")
        for c in mock_run.call_args_list:
            cmd = c[0][0]
            assert _EKS_NAMESPACE in cmd or _EKS_NAMESPACE in str(cmd)

    @patch("ai.chronon.repo.admin.subprocess.run")
    @patch("ai.chronon.repo.admin.shutil.which", return_value="/usr/local/bin/kubectl")
    def test_uses_correct_rollout_timeout(self, mock_which, mock_run):
        """Rollout status should use _EKS_ROLLOUT_TIMEOUT."""
        mock_run.return_value = _make_subprocess_result(returncode=0)
        _upgrade_eks_services("aws", "1.0.0")
        rollout_calls = [c[0][0] for c in mock_run.call_args_list if "rollout" in c[0][0]]
        for cmd in rollout_calls:
            assert f"--timeout={_EKS_ROLLOUT_TIMEOUT}s" in cmd

    @patch("ai.chronon.repo.admin.subprocess.run")
    @patch("ai.chronon.repo.admin.shutil.which", return_value="/usr/local/bin/kubectl")
    def test_no_table_when_all_deployments_missing(self, mock_which, mock_run, capsys):
        """If every deployment is missing, no summary table should be printed."""
        def side_effect(cmd, **kwargs):
            if "cluster-info" in cmd:
                return _make_subprocess_result(returncode=0)
            if "get" in cmd:
                return _make_subprocess_result(returncode=1)
            return _make_subprocess_result(returncode=0)

        mock_run.side_effect = side_effect
        _upgrade_eks_services("aws", "1.0.0")
        output = strip_ansi(capsys.readouterr().out)
        assert "EKS Service Upgrade" not in output

    @patch("ai.chronon.repo.admin.subprocess.run")
    @patch("ai.chronon.repo.admin.shutil.which", return_value="/usr/local/bin/kubectl")
    def test_partial_deployment_existence(self, mock_which, mock_run, capsys):
        """Only existing deployments are upgraded; missing ones are skipped."""
        existing_deployment = "zipline-orchestration-hub"

        def side_effect(cmd, **kwargs):
            if "cluster-info" in cmd:
                return _make_subprocess_result(returncode=0)
            if "get" in cmd and "deployment" in cmd:
                if existing_deployment in cmd:
                    return _make_subprocess_result(returncode=0)
                return _make_subprocess_result(returncode=1)
            if "set" in cmd or "rollout" in cmd:
                return _make_subprocess_result(returncode=0)
            return _make_subprocess_result(returncode=0)

        mock_run.side_effect = side_effect
        _upgrade_eks_services("aws", "1.0.0")
        output = strip_ansi(capsys.readouterr().out)
        assert "rollout complete" in output
        assert "not found" in output


class TestUpgradeCommand:
    # --- control-plane subcommand ---

    @patch("ai.chronon.repo.admin._get_current_kube_context", return_value="test-ctx")
    @patch("ai.chronon.repo.admin._upgrade_eks_services")
    @patch("ai.chronon.repo.admin.get_package_version", return_value="1.0.0")
    def test_upgrade_control_plane_aws_with_release(self, mock_ver, mock_upgrade, mock_ctx):
        runner = CliRunner()
        result = runner.invoke(
            admin,
            ["upgrade", "control-plane", "aws", "--release", "1.4.2", "--yes"],
        )
        assert result.exit_code == 0
        mock_upgrade.assert_called_once_with("aws", "1.4.2")

    @patch("ai.chronon.repo.admin._get_current_kube_context", return_value="test-ctx")
    @patch("ai.chronon.repo.admin._upgrade_eks_services")
    @patch("ai.chronon.repo.admin.get_package_version", return_value="1.0.0")
    def test_upgrade_control_plane_defaults_to_package_version(self, mock_ver, mock_upgrade, mock_ctx):
        runner = CliRunner()
        result = runner.invoke(
            admin,
            ["upgrade", "control-plane", "aws", "--yes"],
        )
        assert result.exit_code == 0
        mock_upgrade.assert_called_once_with("aws", "1.0.0")

    @patch("ai.chronon.repo.admin._upgrade_eks_services")
    def test_upgrade_control_plane_gcp_rejected(self, mock_upgrade):
        # cloud check fires before context detection, so no kubectl mock needed
        runner = CliRunner()
        result = runner.invoke(admin, ["upgrade", "control-plane", "gcp"])
        assert result.exit_code != 0
        mock_upgrade.assert_not_called()
        assert "only supported for AWS" in result.output

    @patch("ai.chronon.repo.admin._upgrade_eks_services")
    @patch("ai.chronon.repo.admin.get_package_version", return_value="unknown")
    def test_upgrade_control_plane_unknown_version_no_release_fails(self, mock_ver, mock_upgrade):
        # release check fires before context detection, so no kubectl mock needed
        runner = CliRunner()
        result = runner.invoke(admin, ["upgrade", "control-plane", "aws"])
        assert result.exit_code != 0
        mock_upgrade.assert_not_called()
        assert "--release" in result.output

    @patch("ai.chronon.repo.admin._get_current_kube_context", return_value="test-ctx")
    @patch("ai.chronon.repo.admin._upgrade_eks_services")
    @patch("ai.chronon.repo.admin.get_package_version", return_value="1.0.0")
    def test_upgrade_control_plane_aborts_on_no(self, mock_ver, mock_upgrade, mock_ctx):
        # Without --yes, the prompt fires. Sending "n" to stdin aborts before
        # the upgrade runs.
        runner = CliRunner()
        result = runner.invoke(
            admin,
            ["upgrade", "control-plane", "aws", "--release", "1.4.2"],
            input="n\n",
        )
        assert result.exit_code != 0
        mock_upgrade.assert_not_called()
        assert "Aborted by user" in result.output

    # --- data-plane subcommand ---

    @patch("ai.chronon.repo.admin.redeploy_streaming")
    def test_upgrade_data_plane_calls_redeploy(self, mock_redeploy, canary):
        runner = CliRunner()
        result = runner.invoke(admin, [
            "upgrade", "data-plane",
            "compiled/group_bys/aws/dim_listings.v1__0",
            "--chronon-root", canary,
            "--no-use-auth",
        ])
        assert result.exit_code == 0
        mock_redeploy.assert_called_once()
        call_kwargs = mock_redeploy.call_args[1]
        assert call_kwargs["repo"] == canary
        assert call_kwargs["confs"] == ["compiled/group_bys/aws/dim_listings.v1__0"]
        assert call_kwargs["use_auth"] is False

    @patch("ai.chronon.repo.admin.redeploy_streaming")
    def test_upgrade_data_plane_multiple_confs(self, mock_redeploy, canary):
        runner = CliRunner()
        result = runner.invoke(admin, [
            "upgrade", "data-plane",
            "compiled/group_bys/aws/dim_listings.v1__0",
            "compiled/group_bys/aws/dim_merchants.v1__0",
            "--chronon-root", canary,
            "--no-use-auth",
        ])
        assert result.exit_code == 0
        call_kwargs = mock_redeploy.call_args[1]
        assert set(call_kwargs["confs"]) == {
            "compiled/group_bys/aws/dim_listings.v1__0",
            "compiled/group_bys/aws/dim_merchants.v1__0",
        }

    def test_upgrade_data_plane_no_confs_fails(self, canary):
        runner = CliRunner()
        result = runner.invoke(admin, [
            "upgrade", "data-plane",
            "--chronon-root", canary,
            "--no-use-auth",
        ])
        assert result.exit_code != 0


# --- _app_images ---


class TestAppImages:
    def test_cloud_specific_images_use_release(self):
        images = dict((itype, (repo, tag)) for itype, repo, tag in _app_images("aws", "1.2.3"))
        assert images["hub"] == ("ziplineai/hub-aws", "1.2.3")
        assert images["eval"] == ("ziplineai/eval-aws", "1.2.3")

    def test_cloud_agnostic_images_use_release(self):
        images = dict((itype, (repo, tag)) for itype, repo, tag in _app_images("gcp", "1.2.3"))
        assert images["frontend"] == ("ziplineai/web-ui", "1.2.3")
        assert images["fetcher"] == ("ziplineai/chronon-fetcher", "1.2.3")

    def test_flink_uses_fixed_tag(self):
        for cloud in ("aws", "azure"):
            images = dict((itype, (repo, tag)) for itype, repo, tag in _app_images(cloud, "1.2.3"))
            repo, tag = images["flink"]
            assert repo == "ziplineai/flink"
            assert tag == FLINK_IMAGE_TAG
            assert tag != "1.2.3"

    def test_flink_excluded_for_gcp(self):
        images = dict((itype, (repo, tag)) for itype, repo, tag in _app_images("gcp", "1.2.3"))
        assert "flink" not in images

    def test_cloud_suffix_varies_by_cloud(self):
        for cloud in ("aws", "gcp", "azure"):
            images = dict((itype, (repo, tag)) for itype, repo, tag in _app_images(cloud, "1.0.0"))
            assert images["hub"][0] == f"ziplineai/hub-{cloud}"
            assert images["eval"][0] == f"ziplineai/eval-{cloud}"
