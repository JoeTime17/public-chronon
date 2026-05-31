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
import gen_thrift.api.ttypes as api
import pytest

from ai.chronon import utils
from ai.chronon.repo.serializer import file2thrift


@pytest.fixture
def event_group_by(repo):
    """
    Sample source taken from one of the group bys.
    This is an event source, not streaming
    """
    from group_bys.sample_team.sample_group_by_from_module import v1

    return v1


@pytest.fixture
def event_source(event_group_by):
    """
    Sample source taken from one of the group bys.
    This is an event source, not streaming
    """
    return event_group_by.sources[0]



def test_edit_distance():
    assert utils.edit_distance("test", "test") == 0
    assert utils.edit_distance("test", "testy") > 0
    assert utils.edit_distance("test", "testing") <= (
        utils.edit_distance("test", "tester") + utils.edit_distance("tester", "testing")
    )


def test_source_utils(event_source):
    assert utils.get_table(event_source) == "sample_namespace.sample_table_group_by"
    assert not utils.is_streaming(event_source)


def test_group_by_utils(event_group_by):
    assert not utils.get_streaming_sources(event_group_by)


def test_normalize_sources_rejects_invalid_element():
    with pytest.raises(TypeError, match=r"sources\[1\] must be a supported source type, got int"):
        utils.normalize_sources([api.EventSource(), 123])


@pytest.mark.parametrize(
    "materialized_group_by,table_name",
    [
        (
            "entity_sample_group_by_from_module.v1",
            "chronon_db.sample_team_entity_sample_group_by_from_module_v1",
        ),
        (
            "event_sample_group_by.v1",
            "sample_namespace.sample_team_event_sample_group_by_v1",
        ),
        ("group_by_with_kwargs.v1", "chronon_db.sample_team_group_by_with_kwargs_v1"),
        (
            "sample_chaining_group_by.chaining_group_by_v1",
            "test_namespace.sample_team_sample_chaining_group_by_chaining_group_by_v1",
        ),
    ],
)
def test_group_by_table_names(repo, materialized_group_by, table_name):
    gb = file2thrift(
        os.path.join(repo, "production/group_bys/sample_team", materialized_group_by),
        api.GroupBy,
    )
    from ai.chronon.group_by import _get_output_table_name
    assert _get_output_table_name(gb, True) == table_name


def test_compose():
    computed = utils.compose(
        "user_id_approx_distinct_count_by_query",
        "map_entries",
        "array_sort (x, y) -> IF(y.value > x.value, -1, IF(y.value < x.value, 1, 0))",
        "transform entry -> entry.key",
    )

    expected = """
transform(
    array_sort(
        map_entries(
            user_id_approx_distinct_count_by_query
        ),
        (x, y) -> IF(y.value > x.value, -1, IF(y.value < x.value, 1, 0))
    ),
    entry -> entry.key
)
""".strip()
    assert computed == expected, f"Expected: \n{expected}\nbut got: \n{computed}"


def test_clean_expression():
    expr = """
transform(
    funct2(  arg)
)
"""

    assert utils.clean_expression(expr) == "transform( funct2( arg) )"


# --- blob_exists tests ---


def test_blob_exists_s3_file_exists():
    """Test blob_exists returns True for existing S3 object."""
    from unittest.mock import MagicMock, patch

    from ai.chronon.repo.utils import blob_exists

    with patch("boto3.client") as mock_client_constructor:
        mock_client = MagicMock()
        mock_client_constructor.return_value = mock_client
        mock_client.head_object.return_value = {}

        assert blob_exists("s3://bucket/path/to/file.jar") is True
        mock_client.head_object.assert_called_once_with(Bucket="bucket", Key="path/to/file.jar")


def test_blob_exists_s3_file_not_found():
    """Test blob_exists returns False for non-existent S3 object."""
    from unittest.mock import MagicMock, patch

    from botocore.exceptions import ClientError

    from ai.chronon.repo.utils import blob_exists

    with patch("boto3.client") as mock_client_constructor:
        mock_client = MagicMock()
        mock_client_constructor.return_value = mock_client
        error_response = {"Error": {"Code": "404"}}
        mock_client.head_object.side_effect = ClientError(error_response, "HeadObject")

        assert blob_exists("s3://bucket/path/to/missing.jar") is False


def test_blob_exists_s3_permission_error():
    """Test blob_exists returns False for S3 permission errors."""
    from unittest.mock import MagicMock, patch

    from botocore.exceptions import ClientError

    from ai.chronon.repo.utils import blob_exists

    with patch("boto3.client") as mock_client_constructor:
        mock_client = MagicMock()
        mock_client_constructor.return_value = mock_client
        error_response = {"Error": {"Code": "AccessDenied"}}
        mock_client.head_object.side_effect = ClientError(error_response, "HeadObject")

        assert blob_exists("s3://bucket/path/to/file.jar") is False


def test_blob_exists_gcs_file_exists():
    """Test blob_exists returns True for existing GCS object."""
    from unittest.mock import MagicMock, patch

    from ai.chronon.repo.utils import blob_exists

    with patch("google.cloud.storage.Client") as mock_client_constructor:
        mock_client = MagicMock()
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_client_constructor.return_value = mock_client
        mock_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        mock_blob.exists.return_value = True

        assert blob_exists("gs://bucket/path/to/file.jar") is True
        mock_client.bucket.assert_called_once_with("bucket")
        mock_bucket.blob.assert_called_once_with("path/to/file.jar")
        mock_blob.exists.assert_called_once()
        mock_client.list_blobs.assert_not_called()


def test_blob_exists_gcs_file_not_found():
    """Test blob_exists returns False for non-existent GCS object."""
    from unittest.mock import MagicMock, patch

    from ai.chronon.repo.utils import blob_exists

    with patch("google.cloud.storage.Client") as mock_client_constructor:
        mock_client = MagicMock()
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_client_constructor.return_value = mock_client
        mock_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        mock_blob.exists.return_value = False

        assert blob_exists("gs://bucket/path/to/missing.jar") is False
        mock_client.bucket.assert_called_once_with("bucket")
        mock_bucket.blob.assert_called_once_with("path/to/missing.jar")
        mock_blob.exists.assert_called_once()
        mock_client.list_blobs.assert_not_called()


def test_blob_exists_gcs_requires_exact_name_match():
    """Test fallback list check does not treat prefix-only matches as existence."""
    from unittest.mock import MagicMock, patch

    from ai.chronon.repo.utils import blob_exists

    class FakeBlob:
        def __init__(self, name):
            self.name = name

    with patch("google.cloud.storage.Client") as mock_client_constructor:
        mock_client = MagicMock()
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_client_constructor.return_value = mock_client
        mock_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        mock_blob.exists.side_effect = Exception("403 forbidden")
        mock_client.list_blobs.return_value = [FakeBlob("path/to/file.jar.backup")]

        assert blob_exists("gs://bucket/path/to/file.jar") is False
        mock_client.bucket.assert_called_once_with("bucket")
        mock_bucket.blob.assert_called_once_with("path/to/file.jar")
        mock_blob.exists.assert_called_once()
        mock_client.list_blobs.assert_called_once_with(
            "bucket",
            prefix="path/to/file.jar",
            max_results=1,
        )


def test_blob_exists_gcs_fallback_to_list_on_exists_error():
    """Test blob_exists falls back to list-based check when exists() fails."""
    from unittest.mock import MagicMock, patch

    from ai.chronon.repo.utils import blob_exists

    class FakeBlob:
        def __init__(self, name):
            self.name = name

    with patch("google.cloud.storage.Client") as mock_client_constructor:
        mock_client = MagicMock()
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_client_constructor.return_value = mock_client
        mock_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        mock_blob.exists.side_effect = Exception("403 forbidden")
        mock_client.list_blobs.return_value = [FakeBlob("path/to/file.jar")]

        assert blob_exists("gs://bucket/path/to/file.jar") is True
        mock_client.bucket.assert_called_once_with("bucket")
        mock_bucket.blob.assert_called_once_with("path/to/file.jar")
        mock_blob.exists.assert_called_once()
        mock_client.list_blobs.assert_called_once_with(
            "bucket",
            prefix="path/to/file.jar",
            max_results=1,
        )


def test_blob_exists_gcs_returns_false_when_exists_and_list_both_fail():
    """Test blob_exists returns False when both GCS checks fail."""
    from unittest.mock import MagicMock, patch

    from ai.chronon.repo.utils import blob_exists

    with patch("google.cloud.storage.Client") as mock_client_constructor:
        mock_client = MagicMock()
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_client_constructor.return_value = mock_client
        mock_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        mock_blob.exists.side_effect = Exception("403 forbidden")
        mock_client.list_blobs.side_effect = Exception("list denied")

        assert blob_exists("gs://bucket/path/to/file.jar") is False
        mock_blob.exists.assert_called_once()
        mock_client.list_blobs.assert_called_once_with(
            "bucket",
            prefix="path/to/file.jar",
            max_results=1,
        )


def test_blob_exists_azure_file_exists():
    """Test blob_exists returns True for existing Azure blob."""
    pytest.importorskip("azure.storage.blob")
    from unittest.mock import MagicMock, patch

    from ai.chronon.repo.utils import blob_exists

    with patch("azure.storage.blob.BlobServiceClient") as mock_service, patch(
        "azure.identity.DefaultAzureCredential"
    ):
        mock_client = MagicMock()
        mock_blob_client = MagicMock()
        mock_service.return_value = mock_client
        mock_client.get_blob_client.return_value = mock_blob_client
        mock_blob_client.exists.return_value = True

        assert blob_exists("https://account.blob.core.windows.net/container/path/file.jar") is True
        mock_blob_client.exists.assert_called_once()


def test_blob_exists_azure_file_not_found():
    """Test blob_exists returns False for non-existent Azure blob."""
    pytest.importorskip("azure.storage.blob")
    from unittest.mock import MagicMock, patch

    from ai.chronon.repo.utils import blob_exists

    with patch("azure.storage.blob.BlobServiceClient") as mock_service, patch(
        "azure.identity.DefaultAzureCredential"
    ):
        mock_client = MagicMock()
        mock_blob_client = MagicMock()
        mock_service.return_value = mock_client
        mock_client.get_blob_client.return_value = mock_blob_client
        mock_blob_client.exists.return_value = False

        assert blob_exists("https://account.blob.core.windows.net/container/path/missing.jar") is False


def test_blob_exists_local_file_exists(tmp_path):
    """Test blob_exists returns True for existing local file."""
    from ai.chronon.repo.utils import blob_exists

    test_file = tmp_path / "test.jar"
    test_file.write_text("content")

    assert blob_exists(str(test_file)) is True


def test_blob_exists_local_file_not_found(tmp_path):
    """Test blob_exists returns False for non-existent local file."""
    from ai.chronon.repo.utils import blob_exists

    assert blob_exists(str(tmp_path / "missing.jar")) is False


def test_blob_exists_handles_exception():
    """Test blob_exists returns False on unexpected exceptions."""
    from unittest.mock import MagicMock, patch

    from ai.chronon.repo.utils import blob_exists

    with patch("boto3.client") as mock_client_constructor:
        mock_client = MagicMock()
        mock_client_constructor.return_value = mock_client
        mock_client.head_object.side_effect = Exception("Network error")

        assert blob_exists("s3://bucket/path/to/file.jar") is False


def test_blob_exists_s3_double_prefix():
    """Test blob_exists handles double s3:// prefix correctly."""
    from unittest.mock import MagicMock, patch

    from ai.chronon.repo.utils import blob_exists

    with patch("boto3.client") as mock_client_constructor:
        mock_client = MagicMock()
        mock_client_constructor.return_value = mock_client
        mock_client.head_object.return_value = {}

        # Simulate the double prefix issue that could occur
        assert blob_exists("s3://bucket/s3://path/file.jar") is True
        # Should strip the nested s3:// from the key
        mock_client.head_object.assert_called_once_with(Bucket="bucket", Key="path/file.jar")


# --- _parse_azure_uri ---


class TestParseAzureUri:
    def test_abfss_uri_parses_correctly(self):
        from ai.chronon.repo.utils import _parse_azure_uri

        account_url, container, blob_name = _parse_azure_uri(
            "abfss://mycontainer@myaccount.dfs.core.windows.net/path/to/file.jar"
        )
        assert account_url == "https://myaccount.blob.core.windows.net"
        assert container == "mycontainer"
        assert blob_name == "path/to/file.jar"

    def test_abfss_uri_converts_dfs_to_blob_endpoint(self):
        from ai.chronon.repo.utils import _parse_azure_uri

        account_url, _, _ = _parse_azure_uri(
            "abfss://container@account.dfs.core.windows.net/file.jar"
        )
        assert ".blob.core.windows.net" in account_url
        assert ".dfs.core.windows.net" not in account_url

    def test_blob_https_uri_parses_correctly(self):
        from ai.chronon.repo.utils import _parse_azure_uri

        account_url, container, blob_name = _parse_azure_uri(
            "https://myaccount.blob.core.windows.net/mycontainer/path/to/file.jar"
        )
        assert account_url == "https://myaccount.blob.core.windows.net"
        assert container == "mycontainer"
        assert blob_name == "path/to/file.jar"

    def test_abfss_nested_path(self):
        from ai.chronon.repo.utils import _parse_azure_uri

        _, container, blob_name = _parse_azure_uri(
            "abfss://dev-zipline-artifacts@ziplineai2.dfs.core.windows.net/release/1.0.0/jars/engine.jar"
        )
        assert container == "dev-zipline-artifacts"
        assert blob_name == "release/1.0.0/jars/engine.jar"


# --- blob_exists Azure abfss ---


def test_blob_exists_abfss_file_exists():
    """Test blob_exists returns True for existing ADLS Gen2 (abfss://) blob."""
    pytest.importorskip("azure.storage.blob")
    from unittest.mock import MagicMock, patch

    from ai.chronon.repo.utils import blob_exists

    with patch("azure.storage.blob.BlobServiceClient") as mock_service, patch(
        "azure.identity.DefaultAzureCredential"
    ):
        mock_client = MagicMock()
        mock_service.return_value = mock_client
        mock_blob_client = MagicMock()
        mock_client.get_blob_client.return_value = mock_blob_client
        mock_blob_client.exists.return_value = True

        assert blob_exists("abfss://mycontainer@myaccount.dfs.core.windows.net/path/file.jar") is True
        mock_blob_client.exists.assert_called_once()
        # Verify it used the blob endpoint, not dfs
        call_kwargs = mock_service.call_args[1]
        assert ".blob.core.windows.net" in call_kwargs["account_url"]


def test_blob_exists_abfss_file_not_found():
    """Test blob_exists returns False for non-existent ADLS Gen2 (abfss://) blob."""
    pytest.importorskip("azure.storage.blob")
    from unittest.mock import MagicMock, patch

    from ai.chronon.repo.utils import blob_exists

    with patch("azure.storage.blob.BlobServiceClient") as mock_service, patch(
        "azure.identity.DefaultAzureCredential"
    ):
        mock_client = MagicMock()
        mock_service.return_value = mock_client
        mock_blob_client = MagicMock()
        mock_client.get_blob_client.return_value = mock_blob_client
        mock_blob_client.exists.return_value = False

        assert blob_exists("abfss://mycontainer@myaccount.dfs.core.windows.net/path/missing.jar") is False


# --- upload_to_blob_store Azure ---


def test_upload_to_blob_store_abfss():
    """Test upload_to_blob_store correctly uploads to ADLS Gen2 (abfss://) URI."""
    pytest.importorskip("azure.storage.blob")
    import tempfile
    from unittest.mock import MagicMock, patch

    from ai.chronon.repo.utils import upload_to_blob_store

    with tempfile.NamedTemporaryFile(suffix=".jar") as f:
        f.write(b"fake jar")
        f.flush()

        with patch("azure.storage.blob.BlobServiceClient") as mock_service, patch(
            "azure.identity.DefaultAzureCredential"
        ):
            mock_client = MagicMock()
            mock_service.return_value = mock_client
            mock_blob_client = MagicMock()
            mock_client.get_blob_client.return_value = mock_blob_client

            uri = "abfss://mycontainer@myaccount.dfs.core.windows.net/path/file.jar"
            result = upload_to_blob_store(f.name, uri)

            assert result == uri
            mock_blob_client.upload_blob.assert_called_once()
            # Verify container and blob name
            mock_client.get_blob_client.assert_called_once_with(
                container="mycontainer", blob="path/file.jar"
            )
            # Verify it used the blob endpoint
            call_kwargs = mock_service.call_args[1]
            assert ".blob.core.windows.net" in call_kwargs["account_url"]


def test_upload_to_blob_store_azure_blob_https():
    """Test upload_to_blob_store still works for https://...blob.core.windows.net URIs."""
    pytest.importorskip("azure.storage.blob")
    import tempfile
    from unittest.mock import MagicMock, patch

    from ai.chronon.repo.utils import upload_to_blob_store

    with tempfile.NamedTemporaryFile(suffix=".jar") as f:
        f.write(b"fake jar")
        f.flush()

        with patch("azure.storage.blob.BlobServiceClient") as mock_service, patch(
            "azure.identity.DefaultAzureCredential"
        ):
            mock_client = MagicMock()
            mock_service.return_value = mock_client
            mock_blob_client = MagicMock()
            mock_client.get_blob_client.return_value = mock_blob_client

            uri = "https://myaccount.blob.core.windows.net/mycontainer/path/file.jar"
            result = upload_to_blob_store(f.name, uri)

            assert result == uri
            mock_blob_client.upload_blob.assert_called_once()


# --- read_from_blob_store Azure ---


def test_read_from_blob_store_abfss():
    """Test read_from_blob_store correctly reads from ADLS Gen2 (abfss://) URI."""
    pytest.importorskip("azure.storage.blob")
    from unittest.mock import MagicMock, patch

    from ai.chronon.repo.utils import read_from_blob_store

    with patch("azure.storage.blob.BlobServiceClient") as mock_service, patch(
        "azure.identity.DefaultAzureCredential"
    ):
        mock_client = MagicMock()
        mock_service.return_value = mock_client
        mock_blob_client = MagicMock()
        mock_client.get_blob_client.return_value = mock_blob_client
        mock_blob_client.download_blob.return_value.readall.return_value = b"1.0.0"

        result = read_from_blob_store(
            "abfss://mycontainer@myaccount.dfs.core.windows.net/release/latest/VERSION"
        )

        assert result == "1.0.0"
        mock_client.get_blob_client.assert_called_once_with(
            container="mycontainer", blob="release/latest/VERSION"
        )
        call_kwargs = mock_service.call_args[1]
        assert ".blob.core.windows.net" in call_kwargs["account_url"]


def test_read_from_blob_store_abfss_not_found():
    """Test read_from_blob_store returns None when ADLS Gen2 blob does not exist."""
    pytest.importorskip("azure.storage.blob")
    from unittest.mock import MagicMock, patch

    from ai.chronon.repo.utils import read_from_blob_store

    with patch("azure.storage.blob.BlobServiceClient") as mock_service, patch(
        "azure.identity.DefaultAzureCredential"
    ):
        mock_client = MagicMock()
        mock_service.return_value = mock_client
        mock_blob_client = MagicMock()
        mock_client.get_blob_client.return_value = mock_blob_client
        mock_blob_client.download_blob.side_effect = Exception("BlobNotFound")

        result = read_from_blob_store(
            "abfss://mycontainer@myaccount.dfs.core.windows.net/release/latest/VERSION"
        )

        assert result is None
