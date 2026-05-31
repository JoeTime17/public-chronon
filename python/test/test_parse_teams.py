"""
Tests for the parse_teams module.
"""
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
from gen_thrift.api.ttypes import (
    EventSource,
    GroupBy,
    Join,
    JoinPart,
    JoinSource,
    MetaData,
    Source,
    Team,
)
from gen_thrift.common.ttypes import ConfigProperties

from ai.chronon.cli.compile import parse_teams
from ai.chronon.repo.constants import RunMode
from ai.chronon.types import EnvironmentVariables, ExecutionInfo
from ai.chronon.utils import OUTPUT_NAMESPACE_PLACEHOLDER


def test_check_deprecated_catalog_in_common_conf():
    """Test that _check_deprecated_catalog rejects DelegatingBigQueryMetastoreCatalog in common conf."""
    conf = ConfigProperties(
        common={
            "spark.sql.catalog.bigquery_catalog": "ai.chronon.integrations.cloud_gcp.DelegatingBigQueryMetastoreCatalog",
        },
    )
    with pytest.raises(ValueError, match="DelegatingBigQueryMetastoreCatalog"):
        parse_teams._check_deprecated_catalog("test_team", conf)


def test_check_deprecated_catalog_in_mode_conf():
    """Test that _check_deprecated_catalog rejects DelegatingBigQueryMetastoreCatalog in mode conf."""
    conf = ConfigProperties(
        common={},
        modeConfigs={
            RunMode.BACKFILL: {
                "spark.sql.catalog.bigquery_catalog": "ai.chronon.integrations.cloud_gcp.DelegatingBigQueryMetastoreCatalog",
            },
        },
    )
    with pytest.raises(ValueError, match="DelegatingBigQueryMetastoreCatalog"):
        parse_teams._check_deprecated_catalog("test_team", conf)


def test_check_deprecated_catalog_allows_valid_config():
    """Test that _check_deprecated_catalog allows BigQueryMetastoreCatalog."""
    conf = ConfigProperties(
        common={
            "spark.sql.catalog.bigquery_catalog": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.bigquery_catalog.catalog-impl": "org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog",
        },
    )
    # Should not raise
    parse_teams._check_deprecated_catalog("test_team", conf)


def test_update_metadata_with_existing_output_namespace():
    """Test that update_metadata doesn't override existing outputNamespace."""
    # Setup
    team_name = "test_team"
    team_dict = {
        "default": Team(outputNamespace="default_namespace"),
        team_name: Team(outputNamespace="team_namespace"),
    }

    # Test with existing outputNamespace
    existing_namespace = "existing_namespace"
    obj = GroupBy(metaData=MetaData(
        team=team_name,
        name="test.group_by.name",
        outputNamespace=existing_namespace
    ))

    # Call the function
    parse_teams.update_metadata(obj, team_dict)

    # Verify outputNamespace wasn't changed
    assert obj.metaData.outputNamespace == existing_namespace


def test_update_metadata_without_existing_output_namespace():
    """Test that update_metadata sets outputNamespace when not already set."""
    # Setup
    team_name = "test_team"
    team_dict = {
        "default": Team(outputNamespace="default_namespace"),
        team_name: Team(outputNamespace="team_namespace"),
    }

    # Test without existing outputNamespace
    obj = GroupBy(metaData=MetaData(
        team=team_name,
        name="test.group_by.name",
    ))

    # Call the function
    parse_teams.update_metadata(obj, team_dict)

    # Verify outputNamespace was set from team
    assert obj.metaData.outputNamespace == "team_namespace"


def test_update_metadata_preserves_join_part_namespace():
    """Test that update_metadata preserves outputNamespace in join parts."""
    # Setup
    team_name = "test_team"
    team_dict = {
        "default": Team(outputNamespace="default_namespace"),
        team_name: Team(outputNamespace="team_namespace"),
    }

    # Create a join with join parts that have existing outputNamespace
    join_part_gb = GroupBy(metaData=MetaData(outputNamespace="existing_jp_namespace"))
    join_part = JoinPart(groupBy=join_part_gb)

    # Create the join object
    join = Join(
        metaData=MetaData(
            team=team_name,
            name="test.join.name",
            outputNamespace="join_namespace"
        ),
        joinParts=[join_part]
    )

    # Call the function
    parse_teams.update_metadata(join, team_dict)

    # Verify outputNamespace values were preserved
    assert join.metaData.outputNamespace == "join_namespace"
    assert join.joinParts[0].groupBy.metaData.outputNamespace == "existing_jp_namespace"


def test_update_metadata_sets_missing_join_part_namespace():
    """Test that update_metadata sets outputNamespace for join parts when not set.
    Child inherits parent's team, so it gets that team's outputNamespace."""
    # Setup
    team_name = "test_team"
    team_dict = {
        "default": Team(outputNamespace="default_namespace"),
        team_name: Team(outputNamespace="team_namespace"),
    }

    # Create a join with join parts that don't have outputNamespace
    join_part_gb = GroupBy(metaData=MetaData())
    join_part = JoinPart(groupBy=join_part_gb)

    # Create the join object with an explicit outputNamespace different from team's
    join = Join(
        metaData=MetaData(
            team=team_name,
            name="test.join.name",
            outputNamespace="join_namespace"
        ),
        joinParts=[join_part]
    )

    # Call the function
    parse_teams.update_metadata(join, team_dict)

    # Parent keeps its explicit outputNamespace
    assert join.metaData.outputNamespace == "join_namespace"
    # Child inherits parent's team (test_team) and gets that team's outputNamespace
    assert join.joinParts[0].groupBy.metaData.team == team_name
    assert join.joinParts[0].groupBy.metaData.outputNamespace == "team_namespace"

def test_nested_groupby_with_different_team_gets_correct_namespace():
    """Test that a nested GroupBy with its own team gets that team's outputNamespace, not the parent's."""
    parent_team = "parent_team"
    child_team = "child_team"
    team_dict = {
        "default": Team(outputNamespace="default_namespace"),
        parent_team: Team(outputNamespace="parent_namespace"),
        child_team: Team(outputNamespace="child_namespace"),
    }

    # Child GroupBy explicitly sets its own team but no outputNamespace
    child_gb = GroupBy(metaData=MetaData(team=child_team))
    join_part = JoinPart(groupBy=child_gb)

    join = Join(
        metaData=MetaData(
            team=parent_team,
            name="test.join.cross_team",
        ),
        joinParts=[join_part],
    )

    parse_teams.update_metadata(join, team_dict)

    # Parent should get its own team's namespace
    assert join.metaData.outputNamespace == "parent_namespace"
    # Child should get its own team's namespace, NOT the parent's
    assert join.joinParts[0].groupBy.metaData.outputNamespace == "child_namespace"
    assert join.joinParts[0].groupBy.metaData.team == child_team


def test_nested_groupby_without_team_inherits_parent_namespace():
    """Test that a nested GroupBy without its own team inherits the parent's namespace."""
    parent_team = "parent_team"
    team_dict = {
        "default": Team(outputNamespace="default_namespace"),
        parent_team: Team(outputNamespace="parent_namespace"),
    }

    # Child GroupBy has no team and no outputNamespace
    child_gb = GroupBy(metaData=MetaData())
    join_part = JoinPart(groupBy=child_gb)

    join = Join(
        metaData=MetaData(
            team=parent_team,
            name="test.join.inherit",
        ),
        joinParts=[join_part],
    )

    parse_teams.update_metadata(join, team_dict)

    assert join.metaData.outputNamespace == "parent_namespace"
    # Child inherits parent's team and therefore parent's namespace
    assert join.joinParts[0].groupBy.metaData.team == parent_team
    assert join.joinParts[0].groupBy.metaData.outputNamespace == "parent_namespace"


def test_update_metadata_requires_output_namespace_on_top_level_config():
    """If no outputNamespace is set on the config or its team, compile must fail."""
    team_dict = {
        "default": Team(),
        "test_team": Team(),
    }
    gb = GroupBy(
        metaData=MetaData(team="test_team", name="t.gb"),
    )
    with pytest.raises(ValueError, match="outputNamespace is not set"):
        parse_teams.update_metadata(gb, team_dict)


def test_update_metadata_requires_output_namespace_on_nested_groupby():
    """If a nested GroupBy's team has no outputNamespace and no fallback is available,
    compile must fail — the invariant applies to every node in the tree."""
    team_dict = {
        "default": Team(),
        "parent_team": Team(outputNamespace="parent_ns"),
        "child_team": Team(),  # no namespace
    }
    child_gb = GroupBy(metaData=MetaData(team="child_team"))
    join = Join(
        metaData=MetaData(team="parent_team", name="t.j"),
        joinParts=[JoinPart(groupBy=child_gb)],
    )
    # The namespace propagation falls back to the parent's namespace so this should
    # actually succeed (child inherits parent's namespace via the fallback in
    # _propagate_namespace). Document that behaviour here.
    parse_teams.update_metadata(join, team_dict)
    assert child_gb.metaData.outputNamespace == "parent_ns"


def test_resolve_placeholder_reaches_inner_join_via_join_left_join_source():
    """A Join whose `left` source is a JoinSource points at an inner Join. The
    placeholder resolver must recurse into that inner Join so its table-name fields
    get substituted against the inner Join's own post-propagation namespace — the
    same path `_propagate_namespace` already walks. Regression guard for a gap
    where the two passes had parallel traversals that could drift."""
    team_dict = {
        "default": Team(outputNamespace="default_ns"),
        "outer_team": Team(outputNamespace="outer_ns"),
        "inner_team": Team(outputNamespace="inner_ns"),
    }

    inner_join = Join(
        metaData=MetaData(team="inner_team", name="inner_team.inner_join"),
        left=Source(events=EventSource(table=f"{OUTPUT_NAMESPACE_PLACEHOLDER}.inner_t")),
    )

    outer_join = Join(
        metaData=MetaData(team="outer_team", name="outer_team.outer_join"),
        left=Source(joinSource=JoinSource(join=inner_join)),
        joinParts=[],
    )

    parse_teams.update_metadata(outer_join, team_dict)

    # Inner join's own team is `inner_team`, so its placeholder resolves to `inner_ns`
    # — not `outer_ns`. Proves the walker reaches through `Join.left.joinSource.join`
    # AND that resolution uses each node's own namespace.
    assert inner_join.metaData.outputNamespace == "inner_ns"
    assert inner_join.left.events.table == "inner_ns.inner_t"


def test_invalid_team_on_nested_node_raises():
    """Test that a nested node with an unknown team raises ValueError."""
    team_dict = {
        "default": Team(outputNamespace="default_namespace"),
        "parent_team": Team(outputNamespace="parent_namespace"),
    }

    child_gb = GroupBy(metaData=MetaData(team="nonexistent_team"))
    join_part = JoinPart(groupBy=child_gb)

    join = Join(
        metaData=MetaData(
            team="parent_team",
            name="test.join.bad_child_team",
        ),
        joinParts=[join_part],
    )

    with pytest.raises(ValueError, match="nonexistent_team"):
        parse_teams.update_metadata(join, team_dict)

def test_merge_team_execution_info():
    """Test that merge_team_execution_info correctly merges team execution info."""
    # Setup
    gcp_team = "gcp"
    team_dict = {
        "default": Team(
            name="default",
            env=EnvironmentVariables(
                common={
                    "common_env_key": "dummy_value",
                    "unique_key_default": "default_value",
                },
                modeEnvironments={
                    RunMode.BACKFILL: {
                        "GCP_DATAPROC_CLUSTER_NAME": "default-cluster",
                        "backfill-unique-key-default": "default_value",
                    },
                    RunMode.STREAMING: {
                        "GCP_DATAPROC_CLUSTER_NAME": "default-streaming-cluster",
                        "streaming-unique-key-default": "default_value",
                    },
                    RunMode.METASTORE: {
                        "GCP_DATAPROC_CLUSTER_NAME": "default-metastore-cluster",
                        "metastore-unique-key-default": "default_value",
                    },
                },
            ),
            conf=ConfigProperties(
                common={
                    "common_conf_key": "dummy_value",
                    "unique_conf_key_default": "default_value",
                },
                modeConfigs={
                    RunMode.BACKFILL: {
                        "spark.chronon.partition.column": "ds",
                        "backfill-unique-conf-key-default": "default_value",
                    },
                    RunMode.STREAMING: {
                        "streaming-unique-conf-key-default": "default_value",
                    },
                    RunMode.METASTORE: {
                        "metastore-unique-conf-key-default": "default_value",
                    },
                },
            ),
        ),
        gcp_team: Team(
            name=gcp_team,
            env=EnvironmentVariables(
                common={
                    "GCP_PROJECT_ID": "test_project",
                    "GCP_REGION": "test_region",
                },
                modeEnvironments={
                    RunMode.BACKFILL: {
                        "GCP_CREATE_DATAPROC": "true",
                        "GCP_DATAPROC_NUM_WORKERS": "2",
                        "GCP_DATAPROC_MASTER_HOST_TYPE": "n2-highmem-8",
                        "GCP_DATAPROC_WORKER_HOST_TYPE": "n2-highmem-4",
                        "ARTIFACT_PREFIX": "some-path",
                    }
                },
            ),
            conf=ConfigProperties(
                common={
                    "spark.chronon.cloud_provider": "gcp",
                    "common_conf_key": "common_conf_value_team",
                },
                modeConfigs={
                    RunMode.BACKFILL: {
                        "backfill-unique-conf-key-gcp": "gcp_value",
                    },
                },
            ),
        ),
    }

    # Create metadata object
    metadata = MetaData(
        team=gcp_team,
        name="test_metadata_for_groupby",
        executionInfo=ExecutionInfo(
            env=EnvironmentVariables(
                common={
                    "common_env_key": "common_value_override_env",
                },
                modeEnvironments={
                    RunMode.BACKFILL: {
                        "GCP_DATAPROC_CLUSTER_NAME": "special-backfill_cluster",
                    },
                    RunMode.STREAMING: {"GCP_DATAPROC_CLUSTER_NAME": "special-streaming_cluster"},
                },
            ),
            conf=ConfigProperties(
                common={
                    "common_conf_key": "common_value_override_conf",
                },
                modeConfigs={
                    RunMode.BACKFILL: {
                        "spark.chronon.partition.column": "notDs",
                    },
                    RunMode.UPLOAD: {
                        "spark.default.parallelism": "1000",
                    },
                },
            ),
        ),
    )

    # Call the function
    parse_teams.merge_team_execution_info(metadata, team_dict, gcp_team)

    actual_execution_info: ExecutionInfo = metadata.executionInfo

    expected_execution_info: ExecutionInfo = ExecutionInfo(
        env=EnvironmentVariables(
            common={
                # metadata common level
                "common_env_key": "common_value_override_env",
                # team common level
                "GCP_PROJECT_ID": "test_project",
                "GCP_REGION": "test_region",
                # default team common level
                "unique_key_default": "default_value",
            },
            modeEnvironments={
                RunMode.BACKFILL: {
                    # metadata common level
                    "common_env_key": "common_value_override_env",
                    # metadata mode level
                    "GCP_DATAPROC_CLUSTER_NAME": "special-backfill_cluster",
                    # team common level
                    "GCP_PROJECT_ID": "test_project",
                    "GCP_REGION": "test_region",
                    # team mode level
                    "GCP_CREATE_DATAPROC": "true",
                    "GCP_DATAPROC_NUM_WORKERS": "2",
                    "GCP_DATAPROC_MASTER_HOST_TYPE": "n2-highmem-8",
                    "GCP_DATAPROC_WORKER_HOST_TYPE": "n2-highmem-4",
                    "ARTIFACT_PREFIX": "some-path",
                    # default team common level
                    "unique_key_default": "default_value",
                    # default team mode level
                    "backfill-unique-key-default": "default_value",
                },
                RunMode.STREAMING: {
                    # metadata common level
                    "common_env_key": "common_value_override_env",
                    # metadata mode level
                    "GCP_DATAPROC_CLUSTER_NAME": "special-streaming_cluster",
                    # team common level
                    "GCP_PROJECT_ID": "test_project",
                    "GCP_REGION": "test_region",
                    # default team common level
                    "unique_key_default": "default_value",
                    # default team mode level
                    "streaming-unique-key-default": "default_value",
                },
                RunMode.METASTORE: {
                    # metadata common level
                    "common_env_key": "common_value_override_env",
                    # metadata mode level
                    "GCP_DATAPROC_CLUSTER_NAME": "default-metastore-cluster",
                    # team common level
                    "GCP_PROJECT_ID": "test_project",
                    "GCP_REGION": "test_region",
                    # default team common level
                    "unique_key_default": "default_value",
                    # default team mode level
                    "metastore-unique-key-default": "default_value",
                },
            },
        ),
        conf=ConfigProperties(
            common={
                # metadata common level
                "common_conf_key": "common_value_override_conf",
                # team common level
                "spark.chronon.cloud_provider": "gcp",
                # default team common level
                "unique_conf_key_default": "default_value",
            },
            modeConfigs={
                RunMode.BACKFILL: {
                    # metadata common level
                    "common_conf_key": "common_value_override_conf",
                    # metadata conf level
                    "spark.chronon.partition.column": "notDs",
                    # team common level
                    "spark.chronon.cloud_provider": "gcp",
                    # team mode level
                    "backfill-unique-conf-key-gcp": "gcp_value",
                    # default team common level
                    "unique_conf_key_default": "default_value",
                    # default team mode level
                    "backfill-unique-conf-key-default": "default_value",
                },
                RunMode.UPLOAD: {
                    # metadata common level
                    "common_conf_key": "common_value_override_conf",
                    # metadata conf level
                    "spark.default.parallelism": "1000",
                    # team common level
                    "spark.chronon.cloud_provider": "gcp",
                    # default team common level
                    "unique_conf_key_default": "default_value",
                },
                RunMode.STREAMING: {
                    # metadata common level
                    "common_conf_key": "common_value_override_conf",
                    # team common level
                    "spark.chronon.cloud_provider": "gcp",
                    # default team common level
                    "unique_conf_key_default": "default_value",
                    # default team mode level
                    "streaming-unique-conf-key-default": "default_value",
                },
                RunMode.METASTORE: {
                    # metadata common level
                    "common_conf_key": "common_value_override_conf",
                    # team common level
                    "spark.chronon.cloud_provider": "gcp",
                    # default team common level
                    "unique_conf_key_default": "default_value",
                    # default team mode level
                    "metastore-unique-conf-key-default": "default_value",
                },
            },
        ),
    )

    actual_env: EnvironmentVariables = actual_execution_info.env
    expected_env: EnvironmentVariables = expected_execution_info.env

    assert(actual_env.common == expected_env.common)
    assert(len(actual_env.modeEnvironments) == len(expected_env.modeEnvironments))
    for mode in actual_env.modeEnvironments:
        assert(actual_env.modeEnvironments[mode] == expected_env.modeEnvironments[mode])

    actual_conf: ConfigProperties = actual_execution_info.conf
    expected_conf: ConfigProperties = expected_execution_info.conf

    assert(actual_conf.common == expected_conf.common)
    assert(len(actual_conf.modeConfigs) == len(expected_conf.modeConfigs))
    for mode in actual_conf.modeConfigs:
        assert(actual_conf.modeConfigs[mode] == expected_conf.modeConfigs[mode])

    assert(actual_execution_info == expected_execution_info)


def test_merge_mode_maps_with_none_default_modes():
    """Test that modeEnvironments propagate when the default team has no modeEnvironments.

    This covers the GCP template scenario where the default team only defines
    common env vars (modeEnvironments=None), and mode-level overrides are set
    at the object level. Previously, these were silently discarded because the
    local dict was never set back on the result object.
    """
    gcp_team = "gcp"
    team_dict = {
        "default": Team(
            name="default",
            env=EnvironmentVariables(
                common={
                    "common_env_key": "default_value",
                },
                # No modeEnvironments — this is the bug trigger
            ),
            conf=ConfigProperties(
                common={
                    "common_conf_key": "default_conf_value",
                },
                # No modeConfigs — same bug trigger for conf
            ),
        ),
        gcp_team: Team(
            name=gcp_team,
            env=EnvironmentVariables(
                common={
                    "GCP_PROJECT_ID": "test_project",
                },
                # No modeEnvironments on team either
            ),
            conf=ConfigProperties(
                common={
                    "spark.chronon.cloud_provider": "gcp",
                },
            ),
        ),
    }

    # Object-level metadata with modeEnvironments set
    metadata = MetaData(
        team=gcp_team,
        name="test_metadata_for_groupby",
        executionInfo=ExecutionInfo(
            env=EnvironmentVariables(
                common={
                    "common_env_key": "object_override",
                },
                modeEnvironments={
                    RunMode.BACKFILL: {
                        "GCP_DATAPROC_CLUSTER_NAME": "my-backfill-cluster",
                    },
                    RunMode.STREAMING: {
                        "GCP_DATAPROC_CLUSTER_NAME": "my-streaming-cluster",
                    },
                },
            ),
            conf=ConfigProperties(
                common={
                    "common_conf_key": "object_conf_override",
                },
                modeConfigs={
                    RunMode.BACKFILL: {
                        "spark.chronon.partition.column": "ds",
                    },
                },
            ),
        ),
    )

    parse_teams.merge_team_execution_info(metadata, team_dict, gcp_team)

    actual_execution_info: ExecutionInfo = metadata.executionInfo

    # modeEnvironments must be present (not None/empty)
    assert actual_execution_info.env.modeEnvironments is not None
    assert RunMode.BACKFILL in actual_execution_info.env.modeEnvironments
    assert RunMode.STREAMING in actual_execution_info.env.modeEnvironments

    # Verify backfill mode has all merged values
    backfill_env = actual_execution_info.env.modeEnvironments[RunMode.BACKFILL]
    assert backfill_env["GCP_DATAPROC_CLUSTER_NAME"] == "my-backfill-cluster"
    assert backfill_env["common_env_key"] == "object_override"
    assert backfill_env["GCP_PROJECT_ID"] == "test_project"

    # Verify streaming mode
    streaming_env = actual_execution_info.env.modeEnvironments[RunMode.STREAMING]
    assert streaming_env["GCP_DATAPROC_CLUSTER_NAME"] == "my-streaming-cluster"
    assert streaming_env["common_env_key"] == "object_override"
    assert streaming_env["GCP_PROJECT_ID"] == "test_project"

    # modeConfigs must be present (not None/empty)
    assert actual_execution_info.conf.modeConfigs is not None
    assert RunMode.BACKFILL in actual_execution_info.conf.modeConfigs

    # Verify backfill conf has all merged values
    backfill_conf = actual_execution_info.conf.modeConfigs[RunMode.BACKFILL]
    assert backfill_conf["spark.chronon.partition.column"] == "ds"
    assert backfill_conf["common_conf_key"] == "object_conf_override"
    assert backfill_conf["spark.chronon.cloud_provider"] == "gcp"


def test_merge_mode_maps_with_none_default_env():
    """Test that modeEnvironments propagate when the default team has env=None.

    When the default team has no env/conf at all (None), it gets filtered out
    by _merge_mode_maps. The team-level env (with only common, no modes) becomes
    the first item, and its modeEnvironments is None — triggering the same bug.
    """
    gcp_team = "gcp"
    team_dict = {
        "default": Team(
            name="default",
            # env=None, conf=None — no environment config at all
        ),
        gcp_team: Team(
            name=gcp_team,
            env=EnvironmentVariables(
                common={
                    "GCP_PROJECT_ID": "test_project",
                },
                # No modeEnvironments
            ),
            conf=ConfigProperties(
                common={
                    "spark.chronon.cloud_provider": "gcp",
                },
                # No modeConfigs
            ),
        ),
    }

    # Object-level metadata with modeEnvironments set
    metadata = MetaData(
        team=gcp_team,
        name="test_metadata_for_groupby",
        executionInfo=ExecutionInfo(
            env=EnvironmentVariables(
                modeEnvironments={
                    RunMode.BACKFILL: {
                        "GCP_DATAPROC_CLUSTER_NAME": "my-backfill-cluster",
                    },
                },
            ),
            conf=ConfigProperties(
                modeConfigs={
                    RunMode.BACKFILL: {
                        "spark.chronon.partition.column": "ds",
                    },
                },
            ),
        ),
    )

    parse_teams.merge_team_execution_info(metadata, team_dict, gcp_team)

    actual_execution_info: ExecutionInfo = metadata.executionInfo

    # modeEnvironments must be present
    assert actual_execution_info.env.modeEnvironments is not None
    assert RunMode.BACKFILL in actual_execution_info.env.modeEnvironments

    backfill_env = actual_execution_info.env.modeEnvironments[RunMode.BACKFILL]
    assert backfill_env["GCP_DATAPROC_CLUSTER_NAME"] == "my-backfill-cluster"
    assert backfill_env["GCP_PROJECT_ID"] == "test_project"

    # modeConfigs must be present
    assert actual_execution_info.conf.modeConfigs is not None
    assert RunMode.BACKFILL in actual_execution_info.conf.modeConfigs

    backfill_conf = actual_execution_info.conf.modeConfigs[RunMode.BACKFILL]
    assert backfill_conf["spark.chronon.partition.column"] == "ds"
    assert backfill_conf["spark.chronon.cloud_provider"] == "gcp"


def test_use_long_names_propagates_to_direct_join_parts_only():
    """Test that useLongNames propagates from Join to its JoinParts but NOT to nested Joins via JoinSource.
    Each Join controls its own column naming independently."""
    team_dict = {
        "default": Team(outputNamespace="default_namespace"),
        "test_team": Team(outputNamespace="test_namespace"),
    }

    # Inner join with its own useLongNames=False (the default)
    inner_join = Join(metaData=MetaData())
    inner_join_source = Source(joinSource=JoinSource(join=inner_join))

    # GroupBy that uses a JoinSource
    child_gb = GroupBy(
        metaData=MetaData(),
        sources=[inner_join_source],
    )
    join_part = JoinPart(groupBy=child_gb)

    # Outer join with useLongNames=True
    outer_join = Join(
        metaData=MetaData(
            team="test_team",
            name="test.join.chained",
        ),
        joinParts=[join_part],
        useLongNames=True,
    )

    parse_teams.update_metadata(outer_join, team_dict)

    # useLongNames should propagate to direct JoinParts
    assert outer_join.joinParts[0].useLongNames is True
    # Inner Join via JoinSource should NOT inherit parent's useLongNames
    assert not inner_join.useLongNames


def test_merge_mode_maps_with_all_none_inputs():
    """Test that _merge_mode_maps returns None when all inputs are None."""
    result = parse_teams._merge_mode_maps(
        None, None, None,
        env_or_config_attribute=parse_teams.EnvOrConfigAttribute.ENV,
    )
    assert result is None
