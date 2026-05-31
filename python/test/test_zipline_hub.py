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

import os
from datetime import date, timedelta
from unittest.mock import Mock, patch, MagicMock

import pytest
import requests

from ai.chronon.repo.zipline_hub import ZiplineHub


class TestZiplineHub:
    """Test cases for ZiplineHub class."""

    def test_base_url_strips_trailing_slash(self):
        """Test that base_url strips trailing slashes."""
        hub_with_slash = ZiplineHub("http://example.com/")
        assert hub_with_slash.base_url == "http://example.com"

        hub_with_multiple_slashes = ZiplineHub("http://example.com///")
        assert hub_with_multiple_slashes.base_url == "http://example.com"

        hub_without_slash = ZiplineHub("http://example.com")
        assert hub_without_slash.base_url == "http://example.com"

    @patch("requests.post")
    def test_call_workflow_start_api_with_default_dates(self, mock_post):
        """Test workflow start API call with default dates."""
        mock_response = Mock()
        mock_response.json.return_value = {"workflowId": "456"}
        mock_post.return_value = mock_response

        hub = ZiplineHub("http://example.com")
        result = hub.call_workflow_start_api(
            "test_conf", "daily", "main", "user1", "hash123"
        )

        expected_start = str(date.today() - timedelta(days=14))
        expected_end = str(date.today())

        assert result == {"workflowId": "456"}
        mock_post.assert_called_once()
        call_kwargs = mock_post.call_args
        assert call_kwargs[0][0] == "http://example.com/workflow/v2/start"
        assert call_kwargs[1]["json"] == {
            "confName": "test_conf",
            "confHash": "hash123",
            "mode": "daily",
            "branch": "main",
            "user": "user1",
            "start": expected_start,
            "end": expected_end,
            "skipLongRunningNodes": False,
        }
        assert call_kwargs[1]["headers"]["Content-Type"] == "application/json"
        assert "X-Zipline-Version" in call_kwargs[1]["headers"]

    @patch("requests.post")
    def test_call_workflow_start_api_with_concurrency(self, mock_post):
        """Test workflow start API includes requested workflow concurrency."""
        mock_response = Mock()
        mock_response.json.return_value = {"workflowId": "456"}
        mock_post.return_value = mock_response

        hub = ZiplineHub("http://example.com")
        result = hub.call_workflow_start_api(
            "test_conf", "daily", "main", "user1", "hash123", concurrency=250
        )

        assert result == {"workflowId": "456"}
        call_kwargs = mock_post.call_args
        assert call_kwargs[1]["json"]["workflowConcurrency"] == 250

    @patch("requests.post")
    def test_call_streaming_redeploy_api_success(self, mock_post):
        """Test streaming redeploy API call posts correct URL and body."""
        mock_response = Mock()
        mock_response.json.return_value = {
            "results": [
                {"metadataName": "aws.my_gb.v1__1", "success": True, "message": "Redeploy initiated"},
            ],
            "totalCount": 1,
            "successCount": 1,
            "failureCount": 0,
        }
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response

        hub = ZiplineHub("http://example.com")
        metadata_names = ["aws.my_gb.v1__1"]
        result = hub.call_streaming_redeploy_api(metadata_names)

        assert result["successCount"] == 1
        assert result["failureCount"] == 0
        mock_post.assert_called_once()
        call_kwargs = mock_post.call_args
        assert call_kwargs[0][0] == "http://example.com/streaming/v1/redeploy"
        assert call_kwargs[1]["json"] == {"metadataNames": metadata_names}

    @patch("requests.post")
    def test_call_streaming_redeploy_api_raises_on_request_error(self, mock_post):
        """Test streaming redeploy API raises on HTTP error."""
        mock_post.side_effect = requests.RequestException("connection refused")

        hub = ZiplineHub("http://example.com")
        with pytest.raises(requests.RequestException):
            hub.call_streaming_redeploy_api(["aws.my_gb.v1__1"])

    @patch("requests.post")
    def test_call_streaming_redeploy_api_raises_on_bad_json(self, mock_post):
        """Test streaming redeploy API raises on invalid JSON response."""
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.side_effect = requests.exceptions.JSONDecodeError("bad json", "", 0)
        mock_response.status_code = 200
        mock_response.text = "not json"
        mock_post.return_value = mock_response

        hub = ZiplineHub("http://example.com")
        with pytest.raises(requests.exceptions.JSONDecodeError):
            hub.call_streaming_redeploy_api(["aws.my_gb.v1__1"])

    @patch("requests.post")
    def test_preview_clear_downstream_success(self, mock_post):
        """Test preview clear-downstream API call posts correct URL and body."""
        mock_response = Mock()
        mock_response.json.return_value = {
            "results": [
                {"nodeName": "source_node", "nodeHash": "h1", "semanticHash": "s1",
                 "startPartition": "2024-01-01", "endPartition": "2024-01-05"},
                {"nodeName": "child_node", "nodeHash": "h2", "semanticHash": "s2",
                 "startPartition": "2024-01-01", "endPartition": "2024-01-12"},
            ],
            "totalNodesCleared": 2,
            "message": "Preview: 2 nodes would be cleared",
        }
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response

        hub = ZiplineHub("http://example.com")
        result = hub.preview_clear_downstream(
            conf_name="aws.demo.v1",
            branch="main",
            user="test@example.com",
            start=date(2024, 1, 1),
            end=date(2024, 1, 5),
        )

        assert result["totalNodesCleared"] == 2
        assert len(result["results"]) == 2
        mock_post.assert_called_once()
        call_kwargs = mock_post.call_args
        assert call_kwargs[0][0] == "http://example.com/workflow/v2/clear-downstream/preview"

    @patch("requests.post")
    def test_preview_clear_downstream_raises_on_request_error(self, mock_post):
        mock_post.side_effect = requests.RequestException("connection refused")

        hub = ZiplineHub("http://example.com")
        with pytest.raises(requests.RequestException):
            hub.preview_clear_downstream(
                conf_name="aws.demo.v1",
                branch="main",
                user="test@example.com",
                start=date(2024, 1, 1),
                end=date(2024, 1, 5),
            )

    @patch("requests.post")
    def test_apply_clear_downstream_success(self, mock_post):
        """Test apply clear-downstream API call posts correct URL and body."""
        node_results = [
            {"nodeName": "source_node", "nodeHash": "h1", "semanticHash": "s1",
             "startPartition": "2024-01-01", "endPartition": "2024-01-05"},
        ]
        mock_response = Mock()
        mock_response.json.return_value = {
            "results": node_results,
            "totalNodesCleared": 1,
            "message": "Cleared 1 nodes",
        }
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response

        hub = ZiplineHub("http://example.com")
        result = hub.apply_clear_downstream(
            node_results=node_results,
            user="test@example.com",
        )

        assert result["totalNodesCleared"] == 1
        mock_post.assert_called_once()
        call_kwargs = mock_post.call_args
        assert call_kwargs[0][0] == "http://example.com/workflow/v2/clear-downstream/apply"
        assert call_kwargs[1]["json"]["nodeResults"] == node_results
        assert call_kwargs[1]["json"]["user"] == "test@example.com"

    @patch("requests.post")
    def test_apply_clear_downstream_raises_on_bad_json(self, mock_post):
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.side_effect = requests.exceptions.JSONDecodeError("bad json", "", 0)
        mock_response.status_code = 200
        mock_response.text = "not json"
        mock_post.return_value = mock_response

        hub = ZiplineHub("http://example.com")
        with pytest.raises(requests.exceptions.JSONDecodeError):
            hub.apply_clear_downstream(
                node_results=[{"nodeName": "n"}],
                user="test@example.com",
            )
