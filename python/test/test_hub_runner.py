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
import pytest
from unittest.mock import Mock, patch

from click.testing import CliRunner
from rich.text import Text

from ai.chronon.cli.formatter import Format
from ai.chronon.repo.hub_runner import hub, redeploy_streaming


def _plain(text: str) -> str:
    """Strip ANSI escape sequences using Rich's own parser."""
    return Text.from_ansi(text).plain

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
            '--repo', canary,
            '--no-use-auth',
            '--start-ds', '2024-01-15',
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
        assert json_payload['mode'] == "backfill"
        assert json_payload['start'] == "2024-01-15"
        assert json_payload['end'] == "2024-02-15"
        assert json_payload['branch'] == "test-branch"

        # Check headers
        headers = call_args[1]['headers']
        assert headers['Content-Type'] == "application/json"

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
            '--repo', canary,
            '--no-use-auth',
            '--start-ds', '2024-01-15',
            '--end-ds', '2024-02-15',
        ])

        # start-ds is not supported
        assert result.exit_code != 0
        result = self._run_and_print(runner, hub, [
            'run-adhoc',
            online_join_conf,
            '--repo', canary,
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
            '--repo', canary,
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
            '--repo', canary,
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
            '--repo', canary,
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
            '--repo', canary,
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
            '--repo', canary,
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
            '--repo', canary,
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
            '--repo', canary,
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
            '--repo', canary,
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
            '--repo', canary,
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
        mock_build_hashmap.return_value = {}

        # Mock compute_and_upload_diffs to return a conf without schedules
        conf_without_schedules = Conf(
            name="test_team.join_without_schedules",
            localPath="/path/to/conf",
            hash="hash1",
        )
        mock_compute_diffs.return_value = {
            "test_team.join_without_schedules": conf_without_schedules,
        }

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

        mock_build_hashmap.assert_called_once_with(root_dir=canary)
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
                {"confName": "aws.my_conf.v1", "startPartition": "2024-01-01", "endPartition": "2024-01-05"},
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
            '--repo', canary,
            '--no-use-auth',
            '--start-ds', '2024-01-01',
            '--end-ds', '2024-01-05',
            '--yes',
        ])

        assert result.exit_code == 0
        plain_output = _plain(result.output)
        assert "aws.my_conf.v1" in plain_output
        assert "Cleared 1 confs" in plain_output
        assert "zipline hub backfill" in plain_output

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
