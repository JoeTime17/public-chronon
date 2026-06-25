#     Copyright (C) 2023 The Chronon Authors.
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.
import datetime
from unittest.mock import Mock, patch

import click
import pytest
from click.testing import CliRunner
from rich.text import Text

from ai.chronon.cli.formatter import Format
from ai.chronon.repo.hub_runner import get_conf_type, hub, redeploy_streaming, repo_option
from ai.chronon.repo.zipline_hub import _format_hub_partition
from gen_thrift.api.ttypes import Environment


def _plain(text: str) -> str:
    """Strip ANSI escape sequences using Rich's own parser."""
    return Text.from_ansi(text).plain

@pytest.mark.parametrize("conf,expected", [
    # prod paths (under compiled/)
    ("compiled/joins/team/x__0", "joins"),
    ("compiled/staging_queries/team/x__0", "stagingqueries"),
    ("compiled/group_bys/team/x__0", "groupbys"),
    ("compiled/models/team/x__1.0", "models"),
    ("compiled/model_transforms/team/x__1", "modeltransforms"),
    # canary paths (under compiled_canary/) — these used to raise before the
    # path-component fix, since the substring "compiled/joins" doesn't match
    # "compiled_canary/joins/...".
    ("compiled_canary/joins/team/x__0", "joins"),
    ("compiled_canary/staging_queries/team/x__0", "stagingqueries"),
    ("compiled_canary/group_bys/team/x__0", "groupbys"),
    # arbitrary env name (any compiled_<env> prefix should work)
    ("compiled_staging/joins/team/x__0", "joins"),
    # absolute paths
    ("/abs/repo/compiled_canary/joins/team/x__0", "joins"),
])
def test_get_conf_type_handles_per_env_output_dirs(conf, expected):
    """get_conf_type must work for any compiled_<env>/ folder, not just
    compiled/, otherwise canary-targeted hub commands fail with ValueError."""
    assert get_conf_type(conf) == expected


def test_get_conf_type_rejects_unknown_folder():
    with pytest.raises(ValueError, match="Unsupported conf type"):
        get_conf_type("compiled_canary/widgets/team/x")


class TestHubRunner:
    """Test cases for hub_runner backfill command."""

    def _run_and_print(self, runner, command, args):
        """Helper method to run command and print output."""
        result = runner.invoke(command, args)

        # Print stdout
        if result.output:
            print(f"\n=== STDOUT ===\n{result.output}")

        # Print stderr if separate
        if hasattr(result, 'stderr') and result.stderr:
            print(f"\n=== STDERR ===\n{result.stderr}")

        # Print exception if any
        if result.exception:
            print(f"\n=== EXCEPTION ===\n{result.exception}")
            import traceback
            traceback.print_exception(type(result.exception), result.exception, result.exception.__traceback__)

        return result

    def test_hub_runner(self):
        """Test that hub command group can be invoked."""
        runner = CliRunner()
        result = self._run_and_print(runner, hub, ["--help"])
        assert result.exit_code == 0
        assert "Usage:" in result.output

    def test_repo_option_reads_chronon_root_env(self):
        """The hub root option should match the rest of the CLI and accept CHRONON_ROOT."""

        @click.command()
        @repo_option
        def command(repo):
            click.echo(repo)

        runner = CliRunner()
        result = runner.invoke(command, [], env={"CHRONON_ROOT": "/tmp/chronon-root"})

        assert result.exit_code == 0
        assert result.output.strip() == "/tmp/chronon-root"

    def test_repo_option_prefers_explicit_chronon_root_over_chronon_root_env(self):
        @click.command()
        @repo_option
        def command(repo):
            click.echo(repo)

        runner = CliRunner()
        result = runner.invoke(
            command,
            ["--chronon-root", "/tmp/explicit-root"],
            env={"CHRONON_ROOT": "/tmp/env-root"},
        )

        assert result.exit_code == 0
        assert result.output.strip() == "/tmp/explicit-root"

    def test_repo_option_supports_deprecated_repo_alias(self):
        @click.command()
        @repo_option
        def command(repo):
            click.echo(repo)

        runner = CliRunner()
        result = runner.invoke(command, ["--repo", "/tmp/explicit-root"])

        assert result.exit_code == 0
        assert "DeprecationWarning" in result.output
        assert "Use --chronon-root instead." in result.output
        assert result.output.strip().endswith("/tmp/explicit-root")

    @patch('requests.post')
    @patch('ai.chronon.repo.hub_runner.get_current_branch')
    def test_backfill_end_to_end_post_request(
        self,
        mock_get_current_branch,
        mock_post,
        canary,
        online_join_conf,
    ):
        """Test end-to-end that the actual POST request contains the right date parameters."""
        # Mock get_current_branch to return a test branch name
        mock_get_current_branch.return_value = "test-branch"

        # Run backfill command
        runner = CliRunner()
        result = self._run_and_print(runner, hub, [
            'backfill',
            online_join_conf,
            '--chronon-root', canary,
            '--no-use-auth',
            '--start-ds', '2024-01-15',
            '--end-ds', '2024-02-15',
            '--concurrency', '250',
        ])

        assert result.exit_code == 0

        # Verify the actual POST request was made with correct parameters
        mock_post.assert_called()
        call_args = mock_post.call_args

        # Check URL
        assert call_args[0][0].endswith("/workflow/v2/start")

        # Check JSON payload
        json_payload = call_args[1]['json']
        assert json_payload['confName'] == ".".join(online_join_conf.split("/")[-2:])
        assert json_payload['mode'] == "backfill"
        assert json_payload['start'] == "2024-01-15"
        assert json_payload['end'] == "2024-02-15"
        assert json_payload['branch'] == "test-branch"
        assert json_payload['workflowConcurrency'] == 250

        # Check headers
        headers = call_args[1]['headers']
        assert headers['Content-Type'] == "application/json"

    @patch('requests.post')
    @patch('ai.chronon.repo.hub_runner.get_current_branch')
    def test_backfill_rejects_end_ds_before_start_ds(
        self,
        mock_get_current_branch,
        mock_post,
        canary,
        online_join_conf,
    ):
        mock_get_current_branch.return_value = "test-branch"

        runner = CliRunner()
        result = self._run_and_print(runner, hub, [
            'backfill',
            online_join_conf,
            '--chronon-root', canary,
            '--no-use-auth',
            '--start-ds', '2024-02-15',
            '--end-ds', '2024-01-15',
        ])

        assert result.exit_code != 0
        assert "End date 2024-01-15 is before start date 2024-02-15" in result.output
        mock_post.assert_not_called()

    @patch('requests.post')
    @patch('ai.chronon.repo.hub_runner.get_current_branch')
    def test_backfill_allows_equal_start_and_end_ds(
        self,
        mock_get_current_branch,
        mock_post,
        canary,
        online_join_conf,
    ):
        mock_get_current_branch.return_value = "test-branch"

        runner = CliRunner()
        result = self._run_and_print(runner, hub, [
            'backfill',
            online_join_conf,
            '--chronon-root', canary,
            '--no-use-auth',
            '--start-ds', '2024-01-15',
            '--end-ds', '2024-01-15',
        ])

        assert result.exit_code == 0

    @patch('requests.post')
    @patch('ai.chronon.repo.hub_runner.get_current_branch')
    def test_backfill_accepts_subdaily_date_formats(
        self,
        mock_get_current_branch,
        mock_post,
        canary,
        online_join_conf,
    ):
        """Subdaily Hub backfills should validate common date spellings and send Chronon partitions."""
        mock_get_current_branch.return_value = "test-branch"

        runner = CliRunner()
        result = self._run_and_print(runner, hub, [
            'backfill',
            online_join_conf,
            '--chronon-root', canary,
            '--no-use-auth',
            '--start-ds', '2024-01-15 03:30',
            '--end-ds', '2024-01-15T06:30',
            '--skip-compile',
        ])

        assert result.exit_code == 0
        json_payload = mock_post.call_args[1]['json']
        assert json_payload['start'] == "2024-01-15-03-30"
        assert json_payload['end'] == "2024-01-15-06-30"

    @patch('requests.post')
    @patch('ai.chronon.repo.hub_runner.get_current_branch')
    def test_backfill_accepts_slash_and_zero_second_date_formats(
        self,
        mock_get_current_branch,
        mock_post,
        canary,
        online_join_conf,
    ):
        mock_get_current_branch.return_value = "test-branch"

        runner = CliRunner()
        result = self._run_and_print(runner, hub, [
            'backfill',
            online_join_conf,
            '--chronon-root', canary,
            '--no-use-auth',
            '--start-ds', '2024/01/15 03:30:00',
            '--end-ds', '2024-01-15-06:30',
            '--skip-compile',
        ])

        assert result.exit_code == 0
        json_payload = mock_post.call_args[1]['json']
        assert json_payload['start'] == "2024-01-15-03-30"
        assert json_payload['end'] == "2024-01-15-06-30"

    def test_backfill_rejects_invalid_date_formats(self, canary, online_join_conf):
        runner = CliRunner()
        result = runner.invoke(hub, [
            'backfill',
            online_join_conf,
            '--chronon-root', canary,
            '--no-use-auth',
            '--start-ds', '2024-99-15',
            '--end-ds', '2024-01-15',
            '--skip-compile',
        ])

        assert result.exit_code != 0
        assert "does not match any supported date format" in result.output

    def test_backfill_rejects_second_precision_date_formats(self, canary, online_join_conf):
        runner = CliRunner()
        result = runner.invoke(hub, [
            'backfill',
            online_join_conf,
            '--chronon-root', canary,
            '--no-use-auth',
            '--start-ds', '2024-01-15 03:30:01',
            '--end-ds', '2024-01-15',
            '--skip-compile',
        ])

        assert result.exit_code != 0
        assert "must be aligned to minute precision" in result.output

    def test_zipline_hub_partition_format_preserves_subdaily_datetimes(self):
        assert _format_hub_partition(datetime.date(2024, 1, 15), None) == "2024-01-15"
        assert _format_hub_partition(datetime.datetime(2024, 1, 15, 0, 0), None) == "2024-01-15"
        assert _format_hub_partition(datetime.datetime(2024, 1, 15, 3, 30), None) == "2024-01-15-03-30"
        with pytest.raises(ValueError, match="minute precision"):
            _format_hub_partition(datetime.datetime(2024, 1, 15, 3, 30, 1), None)

    @patch('requests.post')
    @patch('ai.chronon.repo.hub_runner.get_current_branch')
    def test_adhoc_end_to_end_post_request(
        self,
        mock_get_current_branch,
        mock_post,
        canary,
        online_join_conf,
    ):
        """Test end-to-end that the actual POST request contains the right date parameters."""
        # Mock get_current_branch to return a test branch name
        mock_get_current_branch.return_value = "test-branch"

        # Run backfill command
        runner = CliRunner()
        result = self._run_and_print(runner, hub, [
            'run-adhoc',
            online_join_conf,
            '--chronon-root', canary,
            '--no-use-auth',
            '--start-ds', '2024-01-15',
            '--end-ds', '2024-02-15',
        ])

        # start-ds is not supported
        assert result.exit_code != 0
        result = self._run_and_print(runner, hub, [
            'run-adhoc',
            online_join_conf,
            '--chronon-root', canary,
            '--no-use-auth',
            '--end-ds', '2024-02-15',
        ])
        assert result.exit_code == 0

        # Verify the actual POST request was made with correct parameters
        mock_post.assert_called()
        call_args = mock_post.call_args

        # Check URL
        assert call_args[0][0].endswith("/workflow/v2/start")

        # Check JSON payload
        json_payload = call_args[1]['json']
        assert json_payload['confName'] == ".".join(online_join_conf.split("/")[-2:])
        assert json_payload['mode'] == "deploy"
        assert json_payload['end'] == "2024-02-15"
        assert json_payload['branch'] == "test-branch"

        # Check headers
        headers = call_args[1]['headers']
        assert headers['Content-Type'] == "application/json"

    @patch('requests.post')
    @patch('ai.chronon.repo.hub_runner.get_current_branch')
    def test_schedule_end_to_end_post_request(
        self,
        mock_get_current_branch,
        mock_post,
        canary,
        online_join_conf,
    ):
        """Test end-to-end that the actual POST request contains the right date parameters."""
        # Mock get_current_branch to return a test branch name
        mock_get_current_branch.return_value = "test-branch"

        # Run backfill command
        runner = CliRunner(catch_exceptions=False)
        result = self._run_and_print(runner, hub, [
            'schedule',
            online_join_conf,
            '--chronon-root', canary,
            '--no-use-auth',
        ])

        assert result.exit_code == 0

        # Verify the actual POST request was made with correct parameters
        mock_post.assert_called()
        call_args = mock_post.call_args

        # Check URL
        assert call_args[0][0].endswith("/schedule/v2/schedules")

        # Check JSON payload
        json_payload = call_args[1]['json']
        assert json_payload['confName'] == ".".join(online_join_conf.split("/")[-2:])
        assert json_payload['branch'] == "test-branch"
        assert json_payload['modeSchedules'] == {"BACKFILL": "@daily", "DEPLOY": "@daily"}

        # Check headers
        headers = call_args[1]['headers']
        assert headers['Content-Type'] == "application/json"

    @patch('requests.post')
    def test_cancel_end_to_end_post_request(
        self,
        mock_post,
        canary,
    ):
        """Test end-to-end that the cancel command makes the right API call."""
        # Mock the response from the cancel API
        mock_post.return_value.json.return_value = {
            "success": True,
            "message": "Workflow cancelled successfully"
        }
        mock_post.return_value.raise_for_status.return_value = None

        # Run cancel command
        runner = CliRunner()
        workflow_id = "test-workflow-123"
        result = self._run_and_print(runner, hub, [
            'cancel',
            workflow_id,
            '--chronon-root', canary,
            '--no-use-auth',
            '--cloud', 'gcp',
        ])

        assert result.exit_code == 0
        plain_output = _plain(result.output)
        assert "Workflow cancelled" in plain_output
        assert workflow_id in plain_output

        # Verify the actual POST request was made with correct parameters
        mock_post.assert_called_once()
        call_args = mock_post.call_args

        # Check URL contains the workflow ID and cancel endpoint
        url = call_args[0][0]
        assert f"/workflow/v2/{workflow_id}/cancel" in url

    @patch('requests.post')
    def test_cancel_with_azure_and_customer_id(
        self,
        mock_post,
        canary,
    ):
        """Test cancel command with Azure cloud provider and customer ID."""
        # Mock the response from the cancel API
        mock_post.return_value.json.return_value = {
            "success": True,
            "message": "Workflow cancelled successfully"
        }
        mock_post.return_value.raise_for_status.return_value = None

        # Run cancel command with Azure and customer ID
        runner = CliRunner()
        workflow_id = "test-workflow-456"
        customer_id = "test-customer-123"
        result = self._run_and_print(runner, hub, [
            'cancel',
            workflow_id,
            '--chronon-root', canary,
            '--no-use-auth',
            '--cloud', 'azure',
            '--customer-id', customer_id,
        ])

        assert result.exit_code == 0
        plain_output = _plain(result.output)
        assert "Workflow cancelled" in plain_output
        assert workflow_id in plain_output

        # Verify the actual POST request was made
        mock_post.assert_called_once()
        call_args = mock_post.call_args

        # Check URL contains the workflow ID and cancel endpoint
        url = call_args[0][0]
        assert f"/workflow/v2/{workflow_id}/cancel" in url

    @patch('ai.chronon.repo.hub_runner.get_common_env_map')
    def test_cancel_with_azure_missing_customer_id(
        self,
        mock_get_common_env_map,
        canary,
    ):
        """Test cancel command fails when Azure is specified without customer ID."""
        # Mock get_common_env_map to return config without CUSTOMER_ID
        mock_get_common_env_map.return_value = {
            "HUB_URL": "http://localhost:3903",
            "FRONTEND_URL": "http://localhost:3000",
            # Intentionally not including CUSTOMER_ID
        }

        # Run cancel command with Azure but no customer ID
        runner = CliRunner()
        workflow_id = "test-workflow-789"
        result = self._run_and_print(runner, hub, [
            'cancel',
            workflow_id,
            '--chronon-root', canary,
            '--no-use-auth',
            '--cloud', 'azure',
            # Intentionally not providing --customer-id
        ])

        # click.UsageError exits with code 2
        assert result.exit_code == 2
        assert "Customer ID is not set for Azure" in result.output

    @patch('ai.chronon.repo.hub_runner.submit_schedule_all')
    @patch('ai.chronon.click_helpers.__compile')
    def test_schedule_all_no_changes(
        self,
        mock_compile,
        mock_submit_schedule_all,
        canary,
    ):
        """Test schedule_all when there are no compilation changes."""
        # Mock __compile to return no pending changes
        mock_compile.return_value = ({}, False, {"added": [], "changed": [], "deleted": []})

        runner = CliRunner()
        result = self._run_and_print(runner, hub, [
            'schedule-all',
            '--chronon-root', canary,
            '--cloud', 'gcp',
            '--no-use-auth',
        ])

        # Should succeed with exit code 0
        assert result.exit_code == 0
        plain_output = _plain(result.output)
        assert "No compilation changes detected" in plain_output

        # submit_schedule_all SHOULD be called since there are no changes
        mock_submit_schedule_all.assert_called_once()

    @patch('ai.chronon.repo.hub_runner.submit_schedule_all')
    @patch('ai.chronon.click_helpers.__compile')
    def test_schedule_all_with_added_changes(
        self,
        mock_compile,
        mock_submit_schedule_all,
        canary,
    ):
        """Test schedule_all when there are added configs."""
        # Create mock ConfigChange objects
        from ai.chronon.cli.compile.conf_validator import ConfigChange

        added_change = ConfigChange(
            name="test_team.new_join",
            obj_type="Join",
            online=True,
        )

        # Mock __compile to return pending changes
        mock_compile.return_value = (
            {},
            False,
            {"added": [added_change], "changed": [], "deleted": []}
        )

        runner = CliRunner()
        result = self._run_and_print(runner, hub, [
            'schedule-all',
            '--chronon-root', canary,
            '--cloud', 'gcp',
            '--no-use-auth',
        ])

        # Should fail with exit code 1
        assert result.exit_code == 1
        plain_output = _plain(result.output)
        assert "Compilation resulted in changes" in plain_output
        assert "Added: test_team.new_join" in plain_output

        # submit_schedule_all should NOT be called
        mock_submit_schedule_all.assert_not_called()

    @patch('ai.chronon.repo.hub_runner.submit_schedule_all')
    @patch('ai.chronon.click_helpers.__compile')
    def test_schedule_all_with_changed_configs(
        self,
        mock_compile,
        mock_submit_schedule_all,
        canary,
    ):
        """Test schedule_all when there are changed configs."""
        from ai.chronon.cli.compile.conf_validator import ConfigChange

        changed_change = ConfigChange(
            name="test_team.existing_join",
            obj_type="Join",
            online=True,
        )

        # Mock __compile to return pending changes
        mock_compile.return_value = (
            {},
            False,
            {"added": [], "changed": [changed_change], "deleted": []}
        )

        runner = CliRunner()
        result = self._run_and_print(runner, hub, [
            'schedule-all',
            '--chronon-root', canary,
            '--cloud', 'gcp',
            '--no-use-auth',
        ])

        # Should fail with exit code 1
        assert result.exit_code == 1
        plain_output = _plain(result.output)
        assert "Compilation resulted in changes" in plain_output
        assert "Changed: test_team.existing_join" in plain_output

        # submit_schedule_all should NOT be called
        mock_submit_schedule_all.assert_not_called()

    @patch('ai.chronon.repo.hub_runner.submit_schedule_all')
    @patch('ai.chronon.click_helpers.__compile')
    def test_schedule_all_with_deleted_configs(
        self,
        mock_compile,
        mock_submit_schedule_all,
        canary,
    ):
        """Test schedule_all when there are deleted configs."""
        from ai.chronon.cli.compile.conf_validator import ConfigChange

        deleted_change = ConfigChange(
            name="test_team.old_join",
            obj_type="Join",
            online=False,
        )

        # Mock __compile to return pending changes
        mock_compile.return_value = (
            {},
            False,
            {"added": [], "changed": [], "deleted": [deleted_change]}
        )

        runner = CliRunner()
        result = self._run_and_print(runner, hub, [
            'schedule-all',
            '--chronon-root', canary,
            '--cloud', 'gcp',
            '--no-use-auth',
        ])

        # Should fail with exit code 1
        assert result.exit_code == 1
        plain_output = _plain(result.output)
        assert "Compilation resulted in changes" in plain_output
        assert "Deleted: test_team.old_join" in plain_output

        # submit_schedule_all should NOT be called
        mock_submit_schedule_all.assert_not_called()

    @patch('ai.chronon.repo.hub_runner.submit_schedule_all')
    @patch('ai.chronon.click_helpers.__compile')
    def test_schedule_all_with_multiple_changes(
        self,
        mock_compile,
        mock_submit_schedule_all,
        canary,
    ):
        """Test schedule_all when there are multiple types of changes."""
        from ai.chronon.cli.compile.conf_validator import ConfigChange

        added_change = ConfigChange(name="test_team.new_join", obj_type="Join", online=True)
        changed_change = ConfigChange(name="test_team.existing_join", obj_type="Join", online=True)
        deleted_change = ConfigChange(name="test_team.old_join", obj_type="Join", online=False)

        # Mock __compile to return pending changes
        mock_compile.return_value = (
            {},
            False,
            {
                "added": [added_change],
                "changed": [changed_change],
                "deleted": [deleted_change]
            }
        )

        runner = CliRunner()
        result = self._run_and_print(runner, hub, [
            'schedule-all',
            '--chronon-root', canary,
            '--cloud', 'gcp',
            '--no-use-auth',
        ])

        # Should fail with exit code 1
        assert result.exit_code == 1
        plain_output = _plain(result.output)
        assert "Compilation resulted in changes" in plain_output
        assert "Added: test_team.new_join" in plain_output
        assert "Changed: test_team.existing_join" in plain_output
        assert "Deleted: test_team.old_join" in plain_output

        # submit_schedule_all should NOT be called
        mock_submit_schedule_all.assert_not_called()


    @patch('ai.chronon.repo.hub_runner.get_schedule_modes')
    @patch('ai.chronon.repo.hub_runner.hub_uploader.compute_and_upload_diffs')
    @patch('ai.chronon.repo.hub_runner.hub_uploader.build_local_repo_hashmap')
    @patch('ai.chronon.repo.hub_runner.get_current_branch')
    @patch('ai.chronon.repo.hub_runner.ZiplineHub')
    def test_schedule_all_skips_confs_with_none_str_schedules(
        self,
        mock_zipline_hub,
        mock_get_current_branch,
        mock_build_hashmap,
        mock_compute_diffs,
        mock_get_schedule_modes,
        canary,
    ):
        """Test that submit_schedule_all skips confs where both schedules are SCHEDULE_NONE_STR."""
        from ai.chronon.repo.hub_runner import (
            SCHEDULE_NONE_STR,
            ScheduleModes,
            submit_schedule_all,
        )
        from gen_thrift.api.ttypes import Conf

        mock_get_current_branch.return_value = "test-branch"

        # Mock build_local_repo_hashmap to return a conf without schedules
        conf_without_schedules = Conf(
            name="test_team.join_without_schedules",
            localPath="/path/to/conf",
            hash="hash1",
        )
        mock_build_hashmap.return_value = {
            "test_team.join_without_schedules": conf_without_schedules,
        }

        # Mock compute_and_upload_diffs (still called to upload any changes)
        mock_compute_diffs.return_value = {}

        # Mock get_schedule_modes to return SCHEDULE_NONE_STR for both schedules
        mock_get_schedule_modes.return_value = ScheduleModes(
            offline_schedule=SCHEDULE_NONE_STR,
            online_schedule=SCHEDULE_NONE_STR
        )

        # Mock ZiplineHub instance
        mock_hub_instance = mock_zipline_hub.return_value

        # Call submit_schedule_all
        submit_schedule_all(
            repo=canary,
            cloud='gcp',
            customer_id=None,
            hub_url=None,
            use_auth=False
        )

        # Verify call_schedule_all_api was NOT called since all confs have no schedules
        mock_hub_instance.call_schedule_all_api.assert_not_called()

    @patch('ai.chronon.repo.hub_runner.get_metadata_map')
    @patch('ai.chronon.repo.hub_runner.get_schedule_modes')
    @patch('ai.chronon.repo.hub_runner.hub_uploader.compute_and_upload_diffs')
    @patch('ai.chronon.repo.hub_runner.hub_uploader.build_local_repo_hashmap')
    @patch('ai.chronon.repo.hub_runner.get_current_branch')
    @patch('ai.chronon.repo.hub_runner.ZiplineHub')
    def test_schedule_all_schedules_unchanged_confs(
        self,
        mock_zipline_hub,
        mock_get_current_branch,
        mock_build_hashmap,
        mock_compute_diffs,
        mock_get_schedule_modes,
        mock_get_metadata_map,
        canary,
    ):
        """Confs with schedules should be deployed even if they aren't in the diff."""
        from ai.chronon.repo.hub_runner import (
            ScheduleModes,
            submit_schedule_all,
        )
        from gen_thrift.api.ttypes import Conf

        mock_get_current_branch.return_value = "test-branch"

        changed_conf = Conf(
            name="test_team.changed_join",
            localPath="/path/to/changed",
            hash="hash_changed",
        )
        unchanged_conf = Conf(
            name="test_team.unchanged_join",
            localPath="/path/to/unchanged",
            hash="hash_unchanged",
        )
        # build_local_repo_hashmap returns ALL confs in the repo — this is what
        # the schedule-all loop iterates (per PR #1831). compute_and_upload_diffs
        # is still called for the sync side-effect but its return is irrelevant.
        mock_build_hashmap.return_value = {
            "test_team.changed_join": changed_conf,
            "test_team.unchanged_join": unchanged_conf,
        }
        mock_compute_diffs.return_value = {
            "test_team.changed_join": changed_conf,
        }

        # Both confs have prod environments and real schedules — both must be
        # submitted. Mocked because the localPaths above are fake.
        mock_get_metadata_map.return_value = {
            "environments": [Environment.PROD],
            "executionInfo": {"offlineSchedule": "@daily"},
        }
        mock_get_schedule_modes.return_value = ScheduleModes(
            offline_schedule="@daily",
            online_schedule="@hourly",
        )

        mock_hub_instance = mock_zipline_hub.return_value
        mock_hub_instance.call_schedule_all_api.return_value = {
            "totalCount": 2,
            "successCount": 2,
            "failureCount": 0,
            "results": [
                {"confName": "test_team.changed_join", "success": True, "schedules": {}},
                {"confName": "test_team.unchanged_join", "success": True, "schedules": {}},
            ],
        }

        submit_schedule_all(
            repo=canary,
            cloud='gcp',
            customer_id=None,
            hub_url=None,
            use_auth=False,
        )

        # Both confs (changed and unchanged) should be submitted for scheduling.
        mock_hub_instance.call_schedule_all_api.assert_called_once()
        submitted = mock_hub_instance.call_schedule_all_api.call_args[0][0]
        submitted_names = {entry["conf_name"] for entry in submitted}
        assert submitted_names == {
            "test_team.changed_join",
            "test_team.unchanged_join",
        }

    @patch('requests.post')
    @patch('ai.chronon.repo.hub_runner.get_current_branch')
    @patch('ai.chronon.repo.hub_uploader.compute_and_upload_diffs')
    @patch('ai.chronon.repo.hub_uploader.build_local_repo_hashmap')
    def test_redeploy_streaming_success(
        self,
        mock_build_hashmap,
        mock_upload_diffs,
        mock_get_current_branch,
        mock_post,
        canary,
    ):
        """Test redeploy_streaming syncs confs and calls the redeploy API."""
        mock_get_current_branch.return_value = "main"
        mock_build_hashmap.return_value = {}
        mock_upload_diffs.return_value = []
        mock_post.return_value.json.return_value = {
            "results": [
                {"metadataName": "aws.dim_listings.v1__0", "success": True, "message": "Redeploy initiated"},
            ],
            "totalCount": 1,
            "successCount": 1,
            "failureCount": 0,
        }
        mock_post.return_value.raise_for_status.return_value = None

        redeploy_streaming(
            repo=canary,
            confs=["compiled/group_bys/aws/dim_listings.v1__0"],
            use_auth=False,
        )

        mock_build_hashmap.assert_called_once_with(root_dir=canary, env="prod")
        mock_upload_diffs.assert_called_once()
        mock_post.assert_called_once()
        url = mock_post.call_args[0][0]
        assert "/streaming/v1/redeploy" in url
        body = mock_post.call_args[1]["json"]
        assert body["metadataNames"] == ["aws.dim_listings.v1__0"]

    @patch('requests.post')
    @patch('ai.chronon.repo.hub_runner.get_current_branch')
    @patch('ai.chronon.repo.hub_uploader.compute_and_upload_diffs')
    @patch('ai.chronon.repo.hub_uploader.build_local_repo_hashmap')
    def test_redeploy_streaming_multiple_confs(
        self,
        mock_build_hashmap,
        mock_upload_diffs,
        mock_get_current_branch,
        mock_post,
        canary,
    ):
        """Test redeploy_streaming with multiple confs passes all metadata names."""
        mock_get_current_branch.return_value = "main"
        mock_build_hashmap.return_value = {}
        mock_upload_diffs.return_value = []
        mock_post.return_value.json.return_value = {
            "results": [
                {"metadataName": "aws.dim_listings.v1__0", "success": True, "message": "Redeploy initiated"},
                {"metadataName": "aws.dim_merchants.v1__0", "success": True, "message": "Redeploy initiated"},
            ],
            "totalCount": 2,
            "successCount": 2,
            "failureCount": 0,
        }
        mock_post.return_value.raise_for_status.return_value = None

        redeploy_streaming(
            repo=canary,
            confs=[
                "compiled/group_bys/aws/dim_listings.v1__0",
                "compiled/group_bys/aws/dim_merchants.v1__0",
            ],
            use_auth=False,
        )

        body = mock_post.call_args[1]["json"]
        assert set(body["metadataNames"]) == {"aws.dim_listings.v1__0", "aws.dim_merchants.v1__0"}

    @patch('requests.post')
    @patch('ai.chronon.repo.hub_runner.get_current_branch')
    @patch('ai.chronon.repo.hub_uploader.compute_and_upload_diffs')
    @patch('ai.chronon.repo.hub_uploader.build_local_repo_hashmap')
    def test_redeploy_streaming_sync_called_before_api(
        self,
        mock_build_hashmap,
        mock_upload_diffs,
        mock_get_current_branch,
        mock_post,
        canary,
    ):
        """Test that sync happens before the redeploy API call."""
        call_order = []
        mock_get_current_branch.return_value = "main"
        mock_build_hashmap.side_effect = lambda **_: call_order.append("build_hashmap") or {}
        mock_upload_diffs.side_effect = lambda *a, **kw: call_order.append("upload_diffs")
        mock_post.return_value.json.return_value = {
            "results": [], "totalCount": 0, "successCount": 0, "failureCount": 0,
        }
        mock_post.return_value.raise_for_status.return_value = None
        mock_post.side_effect = lambda *a, **kw: call_order.append("api_call") or mock_post.return_value

        redeploy_streaming(
            repo=canary,
            confs=["compiled/group_bys/aws/dim_listings.v1__0"],
            use_auth=False,
        )

        assert call_order.index("build_hashmap") < call_order.index("upload_diffs")
        assert call_order.index("upload_diffs") < call_order.index("api_call")

    @patch('requests.post')
    @patch('ai.chronon.repo.hub_runner.get_current_branch')
    @patch('ai.chronon.repo.hub_uploader.compute_and_upload_diffs')
    @patch('ai.chronon.repo.hub_uploader.build_local_repo_hashmap')
    def test_redeploy_streaming_exits_nonzero_on_failure(
        self,
        mock_build_hashmap,
        mock_upload_diffs,
        mock_get_current_branch,
        mock_post,
        canary,
    ):
        """Test that redeploy_streaming exits with code 1 when failureCount > 0."""
        mock_get_current_branch.return_value = "main"
        mock_build_hashmap.return_value = {}
        mock_upload_diffs.return_value = []
        mock_post.return_value.json.return_value = {
            "results": [
                {"metadataName": "aws.dim_listings.v1__0", "success": True, "message": "Redeploy initiated"},
                {"metadataName": "aws.dim_merchants.v1__0", "success": False, "message": "Job not found"},
            ],
            "totalCount": 2,
            "successCount": 1,
            "failureCount": 1,
        }
        mock_post.return_value.raise_for_status.return_value = None

        with pytest.raises(SystemExit) as exc_info:
            redeploy_streaming(
                repo=canary,
                confs=[
                    "compiled/group_bys/aws/dim_listings.v1__0",
                    "compiled/group_bys/aws/dim_merchants.v1__0",
                ],
                use_auth=False,
            )
        assert exc_info.value.code == 1

    @patch('requests.post')
    @patch('ai.chronon.repo.hub_runner.get_current_branch')
    @patch('ai.chronon.repo.hub_uploader.compute_and_upload_diffs')
    @patch('ai.chronon.repo.hub_uploader.build_local_repo_hashmap')
    def test_redeploy_streaming_json_exits_nonzero_on_failure(
        self,
        mock_build_hashmap,
        mock_upload_diffs,
        mock_get_current_branch,
        mock_post,
        canary,
    ):
        """Test that redeploy_streaming exits with code 1 in JSON format when failureCount > 0."""
        mock_get_current_branch.return_value = "main"
        mock_build_hashmap.return_value = {}
        mock_upload_diffs.return_value = []
        mock_post.return_value.json.return_value = {
            "results": [
                {"metadataName": "aws.dim_listings.v1__0", "success": False, "message": "Timeout"},
            ],
            "totalCount": 1,
            "successCount": 0,
            "failureCount": 1,
        }
        mock_post.return_value.raise_for_status.return_value = None

        with pytest.raises(SystemExit) as exc_info:
            redeploy_streaming(
                repo=canary,
                confs=["compiled/group_bys/aws/dim_listings.v1__0"],
                use_auth=False,
                format=Format.JSON,
            )
        assert exc_info.value.code == 1

    @patch('requests.post')
    @patch('ai.chronon.repo.hub_runner.get_user_email')
    @patch('ai.chronon.repo.hub_runner.get_current_branch')
    def test_clear_downstream_preview_and_apply(
        self,
        mock_get_current_branch,
        mock_get_user_email,
        mock_post,
        canary,
        online_join_conf,
    ):
        """Test clear-downstream calls preview then apply after confirmation."""
        mock_get_current_branch.return_value = "test-branch"
        mock_get_user_email.return_value = "test@example.com"

        preview_response = Mock()
        preview_response.json.return_value = {
            "results": [
                {"nodeName": "aws.my_node.v1", "nodeHash": "h1", "semanticHash": "s1",
                 "startPartition": "2024-01-01", "endPartition": "2024-01-05"},
            ],
            "affectedConfs": [
                {"confName": "aws.my_conf.v1", "startPartition": "2024-01-01", "endPartition": "2024-01-05", "mode": "backfill"},
            ],
            "totalNodesCleared": 1,
            "message": "Preview: 1 confs would be cleared",
        }
        preview_response.raise_for_status.return_value = None

        apply_response = Mock()
        apply_response.json.return_value = {
            "results": [
                {"nodeName": "aws.my_node.v1", "startPartition": "2024-01-01", "endPartition": "2024-01-05"},
            ],
            "totalNodesCleared": 1,
            "message": "Cleared 1 nodes",
        }
        apply_response.raise_for_status.return_value = None

        mock_post.side_effect = [preview_response, apply_response]

        runner = CliRunner()
        result = self._run_and_print(runner, hub, [
            'clear-downstream',
            online_join_conf,
            '--chronon-root', canary,
            '--no-use-auth',
            '--start-ds', '2024-01-01',
            '--end-ds', '2024-01-05',
            '--yes',
        ])

        assert result.exit_code == 0
        plain_output = _plain(result.output)
        assert "aws.my_conf.v1 (batch)" in plain_output
        assert "Cleared 1 confs" in plain_output
        assert "zipline hub backfill aws.my_conf.v1 --start-ds 2024-01-01 --end-ds 2024-01-05" in plain_output

        assert mock_post.call_count == 2

        preview_call = mock_post.call_args_list[0]
        assert "/workflow/v2/clear-downstream/preview" in preview_call[0][0]
        preview_payload = preview_call[1]['json']
        assert preview_payload['confName'] == ".".join(online_join_conf.split("/")[-2:])
        assert preview_payload['branch'] == "test-branch"
        assert preview_payload['start'] == "2024-01-01"
        assert preview_payload['end'] == "2024-01-05"

        apply_call = mock_post.call_args_list[1]
        assert "/workflow/v2/clear-downstream/apply" in apply_call[0][0]
        apply_payload = apply_call[1]['json']
        assert len(apply_payload['nodeResults']) == 1
        assert apply_payload['user'] == "test@example.com"
        assert len(apply_payload['affectedConfs']) == 1
        assert apply_payload['affectedConfs'][0]['confName'] == "aws.my_conf.v1"

    @patch('ai.chronon.repo.hub_runner.get_metadata_map')
    @patch('ai.chronon.repo.hub_runner.get_schedule_modes')
    @patch('ai.chronon.repo.hub_runner.hub_uploader.compute_and_upload_diffs')
    @patch('ai.chronon.repo.hub_runner.hub_uploader.build_local_repo_hashmap')
    @patch('ai.chronon.repo.hub_runner.get_current_branch')
    @patch('ai.chronon.repo.hub_runner.ZiplineHub')
    def test_schedule_all_filters_by_environment(
        self,
        mock_zipline_hub,
        mock_get_current_branch,
        mock_build_hashmap,
        mock_compute_diffs,
        mock_get_schedule_modes,
        mock_get_metadata_map,
        canary,
    ):
        """Test that submit_schedule_all filters confs based on environment."""
        from ai.chronon.repo.hub_runner import (
            ScheduleModes,
            submit_schedule_all,
        )
        from gen_thrift.api.ttypes import Conf

        mock_get_current_branch.return_value = "test-branch"

        # Create test confs: one with prod, one with canary, one with both
        prod_conf = Conf(name="test_team.prod_join", localPath="/path/to/prod", hash="hash1")
        canary_conf = Conf(name="test_team.canary_join", localPath="/path/to/canary", hash="hash2")
        both_conf = Conf(name="test_team.both_join", localPath="/path/to/both", hash="hash3")

        # The schedule-all loop iterates build_local_repo_hashmap's return —
        # not compute_and_upload_diffs (see PR #1831).
        mock_build_hashmap.return_value = {
            "test_team.prod_join": prod_conf,
            "test_team.canary_join": canary_conf,
            "test_team.both_join": both_conf,
        }
        mock_compute_diffs.return_value = {}

        # Set up environments for each conf
        def get_metadata_side_effect(path):
            if "prod" in path:
                return {"environments": [Environment.PROD], "executionInfo": {"offlineSchedule": "@daily"}}
            elif "canary" in path:
                return {"environments": [Environment.CANARY], "executionInfo": {"offlineSchedule": "@daily"}}
            elif "both" in path:
                return {"environments": [Environment.PROD, Environment.CANARY], "executionInfo": {"offlineSchedule": "@daily"}}
            return {}

        mock_get_metadata_map.side_effect = get_metadata_side_effect

        # Mock schedules
        mock_get_schedule_modes.return_value = ScheduleModes(
            offline_schedule="@daily",
            online_schedule="None"
        )

        # Mock ZiplineHub
        mock_hub_instance = mock_zipline_hub.return_value
        mock_hub_instance.call_schedule_all_api.return_value = {
            "totalCount": 2,
            "successCount": 2,
            "failureCount": 0,
            "results": []
        }

        # Test with env='prod'
        submit_schedule_all(
            repo=canary,
            cloud='gcp',
            customer_id=None,
            env='prod',
            hub_url=None,
            use_auth=False
        )

        # Should schedule prod_join and both_join, but not canary_join
        call_args = mock_hub_instance.call_schedule_all_api.call_args[0][0]
        conf_names = [conf["conf_name"] for conf in call_args]
        assert "test_team.prod_join" in conf_names
        assert "test_team.both_join" in conf_names
        assert "test_team.canary_join" not in conf_names

    @patch('ai.chronon.repo.hub_runner.get_metadata_map')
    @patch('ai.chronon.repo.hub_runner.get_schedule_modes')
    @patch('ai.chronon.repo.hub_runner.hub_uploader.compute_and_upload_diffs')
    @patch('ai.chronon.repo.hub_runner.hub_uploader.build_local_repo_hashmap')
    @patch('ai.chronon.repo.hub_runner.get_current_branch')
    @patch('ai.chronon.repo.hub_runner.ZiplineHub')
    def test_schedule_all_with_canary_environment(
        self,
        mock_zipline_hub,
        mock_get_current_branch,
        mock_build_hashmap,
        mock_compute_diffs,
        mock_get_schedule_modes,
        mock_get_metadata_map,
        canary,
    ):
        """Test that submit_schedule_all correctly filters for canary environment."""
        from ai.chronon.repo.hub_runner import (
            ScheduleModes,
            submit_schedule_all,
        )
        from gen_thrift.api.ttypes import Conf

        mock_get_current_branch.return_value = "test-branch"

        prod_conf = Conf(name="test_team.prod_join", localPath="/path/to/prod", hash="hash1")
        canary_conf = Conf(name="test_team.canary_join", localPath="/path/to/canary", hash="hash2")
        both_conf = Conf(name="test_team.both_join", localPath="/path/to/both", hash="hash3")

        # See note in test_schedule_all_filters_by_environment about #1831.
        mock_build_hashmap.return_value = {
            "test_team.prod_join": prod_conf,
            "test_team.canary_join": canary_conf,
            "test_team.both_join": both_conf,
        }
        mock_compute_diffs.return_value = {}

        def get_metadata_side_effect(path):
            if "prod" in path:
                return {"environments": [Environment.PROD], "executionInfo": {"offlineSchedule": "@daily"}}
            elif "canary" in path:
                return {"environments": [Environment.CANARY], "executionInfo": {"offlineSchedule": "@daily"}}
            elif "both" in path:
                return {"environments": [Environment.PROD, Environment.CANARY], "executionInfo": {"offlineSchedule": "@daily"}}
            return {}

        mock_get_metadata_map.side_effect = get_metadata_side_effect

        mock_get_schedule_modes.return_value = ScheduleModes(
            offline_schedule="@daily",
            online_schedule="None"
        )

        mock_hub_instance = mock_zipline_hub.return_value
        mock_hub_instance.call_schedule_all_api.return_value = {
            "totalCount": 2,
            "successCount": 2,
            "failureCount": 0,
            "results": []
        }

        # Test with env='canary'
        submit_schedule_all(
            repo=canary,
            cloud='gcp',
            customer_id=None,
            env='canary',
            hub_url=None,
            use_auth=False
        )

        # Should schedule canary_join and both_join, but not prod_join
        call_args = mock_hub_instance.call_schedule_all_api.call_args[0][0]
        conf_names = [conf["conf_name"] for conf in call_args]
        assert "test_team.canary_join" in conf_names
        assert "test_team.both_join" in conf_names
        assert "test_team.prod_join" not in conf_names

    @patch('ai.chronon.repo.hub_runner.get_metadata_map')
    @patch('ai.chronon.repo.hub_runner.get_schedule_modes')
    @patch('ai.chronon.repo.hub_runner.hub_uploader.compute_and_upload_diffs')
    @patch('ai.chronon.repo.hub_runner.hub_uploader.build_local_repo_hashmap')
    @patch('ai.chronon.repo.hub_runner.get_current_branch')
    @patch('ai.chronon.repo.hub_runner.ZiplineHub')
    def test_schedule_all_defaults_to_prod_environment(
        self,
        mock_zipline_hub,
        mock_get_current_branch,
        mock_build_hashmap,
        mock_compute_diffs,
        mock_get_schedule_modes,
        mock_get_metadata_map,
        canary,
    ):
        """Test that confs without environments field default to ['prod']."""
        from ai.chronon.repo.hub_runner import (
            ScheduleModes,
            submit_schedule_all,
        )
        from gen_thrift.api.ttypes import Conf

        mock_get_current_branch.return_value = "test-branch"

        # Conf without environments field
        legacy_conf = Conf(name="test_team.legacy_join", localPath="/path/to/legacy", hash="hash1")

        # Loop iterates build_local_repo_hashmap (see #1831), not the diff set.
        mock_build_hashmap.return_value = {
            "test_team.legacy_join": legacy_conf,
        }
        mock_compute_diffs.return_value = {}

        # Return metadata without environments field (should default to ['prod'])
        mock_get_metadata_map.return_value = {
            "executionInfo": {"offlineSchedule": "@daily"}
        }

        mock_get_schedule_modes.return_value = ScheduleModes(
            offline_schedule="@daily",
            online_schedule="None"
        )

        mock_hub_instance = mock_zipline_hub.return_value
        mock_hub_instance.call_schedule_all_api.return_value = {
            "totalCount": 1,
            "successCount": 1,
            "failureCount": 0,
            "results": []
        }

        # Test with env='prod' (default)
        submit_schedule_all(
            repo=canary,
            cloud='gcp',
            customer_id=None,
            env='prod',
            hub_url=None,
            use_auth=False
        )

        # Should schedule legacy_join since it defaults to prod
        call_args = mock_hub_instance.call_schedule_all_api.call_args[0][0]
        conf_names = [conf["conf_name"] for conf in call_args]
        assert "test_team.legacy_join" in conf_names

    @patch('ai.chronon.repo.hub_runner.get_metadata_map')
    @patch('ai.chronon.repo.hub_runner.get_schedule_modes')
    @patch('ai.chronon.repo.hub_runner.hub_uploader.compute_and_upload_diffs')
    @patch('ai.chronon.repo.hub_runner.hub_uploader.build_local_repo_hashmap')
    @patch('ai.chronon.repo.hub_runner.get_current_branch')
    @patch('ai.chronon.repo.hub_runner.ZiplineHub')
    @pytest.mark.parametrize("environments_value", [None, []])
    def test_schedule_all_treats_null_or_empty_environments_as_prod(
        self,
        mock_zipline_hub,
        mock_get_current_branch,
        mock_build_hashmap,
        mock_compute_diffs,
        mock_get_schedule_modes,
        mock_get_metadata_map,
        canary,
        environments_value,
    ):
        """A conf with an explicit `environments: null` or `environments: []` on
        disk must behave the same as a conf where the key is omitted entirely:
        both default to prod-only deploy. Guards against a regression where
        authoring stops writing the field but the consumer keeps a stricter
        ``in`` check."""
        from ai.chronon.repo.hub_runner import (
            ScheduleModes,
            submit_schedule_all,
        )
        from gen_thrift.api.ttypes import Conf

        mock_get_current_branch.return_value = "test-branch"
        conf = Conf(name="test_team.unset", localPath="/path/to/x", hash="hash1")
        mock_build_hashmap.return_value = {"test_team.unset": conf}
        mock_compute_diffs.return_value = {}
        mock_get_metadata_map.return_value = {
            "environments": environments_value,
            "executionInfo": {"offlineSchedule": "@daily"},
        }
        mock_get_schedule_modes.return_value = ScheduleModes(
            offline_schedule="@daily", online_schedule="None",
        )
        mock_hub_instance = mock_zipline_hub.return_value
        mock_hub_instance.call_schedule_all_api.return_value = {
            "totalCount": 1, "successCount": 1, "failureCount": 0, "results": [],
        }

        submit_schedule_all(
            repo=canary, cloud='gcp', customer_id=None,
            env='prod', hub_url=None, use_auth=False,
        )

        call_args = mock_hub_instance.call_schedule_all_api.call_args[0][0]
        conf_names = [c["conf_name"] for c in call_args]
        assert "test_team.unset" in conf_names

    @patch('ai.chronon.repo.hub_runner.get_metadata_map')
    @patch('ai.chronon.repo.hub_runner.get_schedule_modes')
    @patch('ai.chronon.repo.hub_runner.hub_uploader.compute_and_upload_diffs')
    @patch('ai.chronon.repo.hub_runner.hub_uploader.build_local_repo_hashmap')
    @patch('ai.chronon.repo.hub_runner.get_current_branch')
    @patch('ai.chronon.repo.hub_runner.ZiplineHub')
    def test_schedule_all_no_matching_environment(
        self,
        mock_zipline_hub,
        mock_get_current_branch,
        mock_build_hashmap,
        mock_compute_diffs,
        mock_get_schedule_modes,
        mock_get_metadata_map,
        canary,
    ):
        """Test that submit_schedule_all reports when no confs match the environment."""
        from ai.chronon.repo.hub_runner import (
            ScheduleModes,
            submit_schedule_all,
        )
        from gen_thrift.api.ttypes import Conf

        mock_get_current_branch.return_value = "test-branch"

        # Only prod confs — the canary env filter must reject all of them so
        # call_schedule_all_api is never invoked.
        prod_conf = Conf(name="test_team.prod_join", localPath="/path/to/prod", hash="hash1")

        mock_build_hashmap.return_value = {
            "test_team.prod_join": prod_conf,
        }
        mock_compute_diffs.return_value = {}

        mock_get_metadata_map.return_value = {
            "environments": [Environment.PROD],
            "executionInfo": {"offlineSchedule": "@daily"}
        }

        mock_get_schedule_modes.return_value = ScheduleModes(
            offline_schedule="@daily",
            online_schedule="None"
        )

        mock_hub_instance = mock_zipline_hub.return_value

        # Test with env='canary' (should not match any confs)
        submit_schedule_all(
            repo=canary,
            cloud='gcp',
            customer_id=None,
            env='canary',
            hub_url=None,
            use_auth=False
        )

        # Should NOT call schedule API since no confs match
        mock_hub_instance.call_schedule_all_api.assert_not_called()

    @patch('ai.chronon.repo.hub_runner.submit_schedule_all')
    @patch('ai.chronon.click_helpers.__compile')
    def test_schedule_all_command_accepts_env_parameter(
        self,
        mock_compile,
        mock_submit_schedule_all,
        canary,
    ):
        """Test that schedule-all command accepts --env parameter."""
        mock_compile.return_value = ({}, False, {"added": [], "changed": [], "deleted": []})

        runner = CliRunner()

        # Test with --env prod
        result = self._run_and_print(runner, hub, [
            'schedule-all',
            '--chronon-root', canary,
            '--cloud', 'gcp',
            '--no-use-auth',
            '--env', 'prod',
        ])
        assert result.exit_code == 0

        # Verify submit_schedule_all was called with env='prod'
        call_kwargs = mock_submit_schedule_all.call_args[1]
        assert call_kwargs['env'] == 'prod'

        # Test with --env canary
        result = self._run_and_print(runner, hub, [
            'schedule-all',
            '--chronon-root', canary,
            '--cloud', 'gcp',
            '--no-use-auth',
            '--env', 'canary',
        ])
        assert result.exit_code == 0

        # Verify submit_schedule_all was called with env='canary'
        call_kwargs = mock_submit_schedule_all.call_args[1]
        assert call_kwargs['env'] == 'canary'

    @patch('ai.chronon.repo.hub_runner.submit_schedule_all')
    @patch('ai.chronon.click_helpers.__compile')
    def test_schedule_all_command_rejects_invalid_env(
        self,
        mock_compile,
        mock_submit_schedule_all,
        canary,
    ):
        """Test that schedule-all command rejects invalid --env values."""
        mock_compile.return_value = ({}, False, {"added": [], "changed": [], "deleted": []})

        runner = CliRunner()

        # Test with invalid env value
        result = self._run_and_print(runner, hub, [
            'schedule-all',
            '--chronon-root', canary,
            '--cloud', 'gcp',
            '--no-use-auth',
            '--env', 'invalid',
        ])

        # Should fail with exit code 2 (invalid option)
        assert result.exit_code == 2
        assert "Invalid value for '--env'" in result.output or "'invalid' is not one of" in result.output

        # submit_schedule_all should NOT be called
        mock_submit_schedule_all.assert_not_called()


class TestEnvironmentValidation:
    """Test environment string validation in API functions."""

    def test_join_accepts_valid_environments(self):
        """Test that Join accepts 'prod' and 'canary' strings."""
        from ai.chronon.join import Join
        from ai.chronon.query import Query
        from gen_thrift.api.ttypes import EventSource, Source

        left = Source(events=EventSource(table="test.table", query=Query(selects={"a": "a"})))

        # Should not raise for valid environments
        join = Join(
            left=left,
            right_parts=[],
            row_ids="id",
            environments=['prod']
        )
        assert join.metaData.environments == [0]  # Environment.PROD

        join = Join(
            left=left,
            right_parts=[],
            row_ids="id",
            environments=['canary']
        )
        assert join.metaData.environments == [1]  # Environment.CANARY

        join = Join(
            left=left,
            right_parts=[],
            row_ids="id",
            environments=['prod', 'canary']
        )
        assert join.metaData.environments == [0, 1]

        # Test case insensitivity
        join = Join(
            left=left,
            right_parts=[],
            row_ids="id",
            environments=['PROD', 'Canary']
        )
        assert join.metaData.environments == [0, 1]

    def test_join_leaves_environments_unset_when_not_provided(self):
        """Authoring without `environments=` must leave metaData.environments
        as None — downstream consumers (hub schedule-all) default missing /
        empty to [Environment.PROD] at read time. Keeps compiled output free
        of a hard-coded default."""
        from ai.chronon.join import Join
        from ai.chronon.query import Query
        from gen_thrift.api.ttypes import EventSource, Source

        left = Source(events=EventSource(table="test.table", query=Query(selects={"a": "a"})))

        join = Join(left=left, right_parts=[], row_ids="id")
        assert join.metaData.environments is None

        # Empty list also leaves the field unset.
        join = Join(left=left, right_parts=[], row_ids="id", environments=[])
        assert join.metaData.environments in (None, [])

    def test_join_rejects_invalid_environment(self):
        """Test that Join raises ValueError for invalid environment strings."""
        from ai.chronon.join import Join
        from ai.chronon.query import Query
        from gen_thrift.api.ttypes import EventSource, Source
        import pytest

        left = Source(events=EventSource(table="test.table", query=Query(selects={"a": "a"})))

        with pytest.raises(ValueError) as exc_info:
            Join(
                left=left,
                right_parts=[],
                row_ids="id",
                environments=['invalid']
            )
        assert "Invalid environment 'invalid'" in str(exc_info.value)
        assert "Must be one of: ['prod', 'canary']" in str(exc_info.value)

    def test_group_by_rejects_invalid_environment(self):
        """Test that GroupBy raises ValueError for invalid environment strings."""
        from ai.chronon.group_by import GroupBy
        from ai.chronon.query import Query
        from gen_thrift.api.ttypes import EventSource, Source
        import pytest

        source = Source(events=EventSource(table="test.table", query=Query(selects={"a": "a"})))

        with pytest.raises(ValueError) as exc_info:
            GroupBy(
                sources=[source],
                keys=['a'],
                aggregations=None,
                environments=['staging']
            )
        assert "Invalid environment 'staging'" in str(exc_info.value)

    def test_staging_query_rejects_invalid_environment(self):
        """Test that StagingQuery raises ValueError for invalid environment strings."""
        from ai.chronon.staging_query import StagingQuery
        import pytest

        with pytest.raises(ValueError) as exc_info:
            StagingQuery(
                query="SELECT * FROM table",
                environments=['dev']
            )
        assert "Invalid environment 'dev'" in str(exc_info.value)

    def test_model_rejects_invalid_environment(self):
        """Test that Model raises ValueError for invalid environment strings."""
        from ai.chronon.model import Model
        import pytest

        with pytest.raises(ValueError) as exc_info:
            Model(
                version="v1",
                environments=['test']
            )
        assert "Invalid environment 'test'" in str(exc_info.value)

    def test_utils_convert_environments_to_enum(self):
        """Test the shared utils.convert_environments_to_enum function."""
        from ai.chronon.utils import convert_environments_to_enum
        from gen_thrift.api.ttypes import Environment
        import pytest

        # Test valid inputs
        assert convert_environments_to_enum(['prod']) == [Environment.PROD]
        assert convert_environments_to_enum(['canary']) == [Environment.CANARY]
        assert convert_environments_to_enum(['prod', 'canary']) == [Environment.PROD, Environment.CANARY]

        # Test case insensitivity
        assert convert_environments_to_enum(['PROD']) == [Environment.PROD]
        assert convert_environments_to_enum(['Canary']) == [Environment.CANARY]
        assert convert_environments_to_enum(['PrOd', 'CaNaRy']) == [Environment.PROD, Environment.CANARY]

        # Test invalid input
        with pytest.raises(ValueError) as exc_info:
            convert_environments_to_enum(['staging'])
        assert "Invalid environment 'staging'" in str(exc_info.value)
        assert "Must be one of: ['prod', 'canary']" in str(exc_info.value)
