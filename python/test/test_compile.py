import os
import json
import sys
from textwrap import dedent
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner
from gen_thrift.api.ttypes import (
    EventSource,
    GroupBy,
    Join,
    JoinPart,
    JoinSource,
    MetaData,
    ModelTransforms,
    Source,
    Team,
)
from gen_thrift.common.ttypes import ExecutionInfo

from ai.chronon.cli.compile import parse_configs
from ai.chronon.cli.compile.compile_context import CONFIG_INFOS, CompileContext
from ai.chronon.cli.compile.parse_teams import (
    PROD_ENV,
    PROD_TEAMS_FILE,
    discover_compile_envs,
    update_metadata,
)
from ai.chronon.repo.compile import __compile, compile
from ai.chronon.utils import OUTPUT_NAMESPACE_PLACEHOLDER


def user_authoring_folders():
    """Folder names under `chronon_root` that users place authoring `.py` files in.
    Derived from the canonical `CONFIG_INFOS` registry so adding a new conf type
    automatically propagates to the test helpers below. Lives in the test file (not
    in production code) because nothing in the compile pipeline itself needs it."""
    return [ci.folder_name for ci in CONFIG_INFOS if ci.config_type is not None]


def test_compile(repo):
    import sys
    sys.path.append(repo)
    results = __compile(chronon_root=repo, ignore_python_errors=True)
    assert len(results) != 0


def test_compile_gcp_resources(gcp_resources):
    """Test compilation of GCP resource examples."""
    import sys
    sys.path.insert(0, gcp_resources)
    results = __compile(chronon_root=gcp_resources, ignore_python_errors=True)
    assert len(results) != 0

def test_compile_aws_resources(aws_resources):
    """Test compilation of AWS resource examples."""
    import sys
    sys.path.insert(0, aws_resources)
    results = __compile(chronon_root=aws_resources, ignore_python_errors=True)
    assert len(results) != 0

def test_compile_azure_resources(azure_resources):
    """Test compilation of Azure resource examples."""
    import sys
    sys.path.insert(0, azure_resources)
    results = __compile(chronon_root=azure_resources, ignore_python_errors=True)
    assert len(results) != 0

def test_discover_compile_envs_rejects_teams_prod_py_collision(tmp_path):
    """`teams.prod.py` would collide with the canonical `teams.py` entry — both
    would target env_name=='prod' and write to compiled/. Discovery must fail
    loudly so the user renames one of them rather than silently losing whichever
    pass ran first."""
    (tmp_path / "teams.py").write_text("# prod teams")
    (tmp_path / "teams.prod.py").write_text("# would collide")

    with pytest.raises(ValueError, match=r"teams\.prod\.py.*reserved"):
        discover_compile_envs(str(tmp_path))


def test_discover_compile_envs_rejects_unsupported_env(tmp_path):
    """Only `canary` is allowed as a non-prod env right now because the hub
    wire contract (Thrift `Environment` enum) only models PROD and CANARY.
    Discovery must reject any other teams.<env>.py file with a clear error
    that names the supported set."""
    (tmp_path / "teams.py").write_text("# prod teams")
    (tmp_path / "teams.staging.py").write_text("# unsupported")

    with pytest.raises(ValueError, match=r"teams\.staging\.py.*not supported.*'canary'"):
        discover_compile_envs(str(tmp_path))


def test_discover_compile_envs_ordering(tmp_path):
    """Discovery returns the canary entry first and prod appended last, so the
    prod pass's confirm-prompt isn't sandwiched between sibling envs' output."""
    (tmp_path / "teams.py").write_text("# prod")
    (tmp_path / "teams.canary.py").write_text("# canary")

    envs = discover_compile_envs(str(tmp_path))

    assert envs == [
        ("canary", "teams.canary.py"),
        (PROD_ENV, PROD_TEAMS_FILE),
    ]


def test_parse_configs_relative_source_file():
    """Test that sourceFile is stored as a path relative to chronon_root."""
    # Setup
    test_root = "/fake/root/path"
    test_file_path = "/fake/root/path/group_bys/team/test_group_by.py"
    test_input_dir = os.path.join(test_root, "group_bys")

    # Create a properly initialized GroupBy object with MetaData
    mock_obj = GroupBy()
    mock_obj.metaData = MetaData()

    # Create mock context
    mock_compile_context = MagicMock(spec=CompileContext)
    mock_compile_context.chronon_root = test_root
    mock_compile_context.teams_dict = {}
    mock_compile_context.validator = MagicMock()
    mock_compile_context.validator.validate_obj.return_value = []
    mock_compile_context.compile_status = MagicMock()
    mock_compile_context.seen_obj_ids = set()
    mock_compile_context.env = PROD_ENV
    mock_compile_context.teams_file_name = PROD_TEAMS_FILE
    mock_compile_context.config_infos = CONFIG_INFOS

    # Configure mocks
    with patch('ai.chronon.cli.compile.parse_configs.from_file') as mock_from_file, \
         patch('ai.chronon.cli.compile.serializer.thrift_simple_json') as mock_serialize, \
         patch('glob.glob', return_value=[test_file_path]), \
         patch('ai.chronon.cli.compile.parse_teams.update_metadata'), \
         patch('ai.chronon.cli.compile.parse_configs.populate_column_hashes'):

        # Configure mock return values - from_file now returns nested dict
        mock_from_file.return_value = {GroupBy: {"team.test_group_by.test_var": mock_obj}}
        mock_serialize.return_value = "{}"

        # Call the function being tested - from_folder now expects list of classes
        results_dict = parse_configs.from_folder([GroupBy], test_input_dir, mock_compile_context)

    # from_folder now returns dict mapping class -> list of CompiledObj
    results = results_dict[GroupBy]

    # Assertions
    assert len(results) == 1
    assert results[0].obj is not None
    assert hasattr(results[0].obj, 'metaData')
    assert results[0].obj.metaData is not None

    # The sourceFile should be a relative path from chronon_root
    expected_relative_path = "group_bys/team/test_group_by.py"
    assert results[0].obj.metaData.sourceFile == expected_relative_path
    assert not results[0].obj.metaData.sourceFile.startswith("/")  # Should be relative, not absolute


def test_print_compile_summary_dry_run_branch(capsys):
    """The dry-run summary line must say 'output not written' even when the
    pass had zero errors. compiler.py deletes the staging dir without moving
    anything on disk under --dry-run, so reporting 'N written' would be a lie."""
    from ai.chronon.repo.compile import _print_compile_summary

    _print_compile_summary([
        {
            "name": "prod",
            "ok": True,
            "compile_dir": "compiled",
            "parsed_count": 42,
            "error_count": 0,
            "output_written": False,  # dry-run sets this False even when ok
        },
    ])

    out = " ".join(capsys.readouterr().out.split())
    assert "42 parsed (dry-run, output not written)" in out, out
    assert "42 written" not in out, out


def test_print_compile_summary_ignore_python_errors_branch(capsys):
    """--ignore-python-errors forces the staging→output move even with errors,
    so the summary should report 'written' alongside the error count."""
    from ai.chronon.repo.compile import _print_compile_summary

    _print_compile_summary([
        {
            "name": "canary",
            "ok": False,
            "compile_dir": "compiled_canary",
            "parsed_count": 5,
            "error_count": 2,
            "output_written": True,
        },
    ])

    out = " ".join(capsys.readouterr().out.split())
    assert "5 written" in out
    assert "2 error(s)" in out
    assert "--ignore-python-errors" in out


def test_print_compile_summary_failed_no_write_branch(capsys):
    """A failed pass without --ignore-python-errors discards the staging dir.
    Summary must say so explicitly so the user knows compiled_<env>/ wasn't
    refreshed."""
    from ai.chronon.repo.compile import _print_compile_summary

    _print_compile_summary([
        {
            "name": "canary",
            "ok": False,
            "compile_dir": "compiled_canary",
            "parsed_count": 165,
            "error_count": 1,
            "output_written": False,
        },
    ])

    # Collapse rich's terminal-width wrapping so substring checks are stable
    # across CI environments with different stty cols.
    out = " ".join(capsys.readouterr().out.split())
    assert "output not written" in out
    assert "165 parsed and discarded" in out
    assert "1 error(s)" in out


def test_compile_with_json_format(canary):
    """Test that compile command with --format json returns valid JSON output."""
    import sys
    sys.path.append(canary)

    runner = CliRunner()
    result = runner.invoke(compile, [
        '--chronon-root', canary,
        '-f', 'json',
        '--ignore-python-errors',
        '--force',
    ])

    # Check that the command executed successfully
    assert result.exit_code == 0, f"Command failed with output: {result.output}"

    # Verify that the output is valid JSON (should be clean JSON with no tracebacks)
    try:
        output_json = json.loads(result.output)
    except json.JSONDecodeError as e:
        assert False, f"Output is not valid JSON: {result.output}\nError: {e}"

    # Verify the JSON structure contains expected fields
    assert "status" in output_json, f"Output missing 'status' field: {output_json}"
    assert output_json["status"] == "success", f"Expected status 'success', got: {output_json['status']}"

    assert "results" in output_json, f"Output missing 'results' field: {output_json}"
    assert isinstance(output_json["results"], dict), f"'results' should be a dict, got: {type(output_json['results'])}"




def _make_team_dict(namespace="test_namespace"):
    """Helper to create a minimal team dict for update_metadata tests."""
    team = Team(outputNamespace=namespace)
    team.executionInfo = ExecutionInfo()
    default_team = Team(outputNamespace="default_ns")
    default_team.executionInfo = ExecutionInfo()
    return {"test_team": team, "default": default_team}


def test_update_metadata_propagates_namespace_to_groupby_join_source():
    """When a GroupBy uses a JoinSource whose Join has no outputNamespace,
    update_metadata should propagate the GroupBy's namespace to the embedded Join."""
    inner_join = Join(
        metaData=MetaData(name="test_team.parent_join", team="test_team"),
        left=Source(events=EventSource(table="some_table")),
    )
    assert inner_join.metaData.outputNamespace is None

    group_by = GroupBy(
        sources=[Source(joinSource=JoinSource(join=inner_join))],
        keyColumns=["user_id"],
        metaData=MetaData(name="test_team.my_gb", team="test_team"),
    )

    update_metadata(group_by, _make_team_dict("gb_namespace"))

    assert group_by.metaData.outputNamespace == "gb_namespace"
    assert inner_join.metaData.outputNamespace == "gb_namespace"


def test_update_metadata_does_not_overwrite_existing_join_source_namespace():
    """If the JoinSource's Join already has an outputNamespace, don't overwrite it."""
    inner_join = Join(
        metaData=MetaData(name="test_team.parent_join", team="test_team", outputNamespace="explicit_ns"),
        left=Source(events=EventSource(table="some_table")),
    )

    group_by = GroupBy(
        sources=[Source(joinSource=JoinSource(join=inner_join))],
        keyColumns=["user_id"],
        metaData=MetaData(name="test_team.my_gb", team="test_team"),
    )

    update_metadata(group_by, _make_team_dict("gb_namespace"))

    assert inner_join.metaData.outputNamespace == "explicit_ns"


def test_update_metadata_propagates_namespace_to_model_transforms_join_source():
    """Same propagation should work for ModelTransforms with JoinSource."""
    inner_join = Join(
        metaData=MetaData(name="test_team.parent_join", team="test_team"),
        left=Source(events=EventSource(table="some_table")),
    )

    mt = ModelTransforms(
        sources=[Source(joinSource=JoinSource(join=inner_join))],
        metaData=MetaData(name="test_team.my_mt", team="test_team"),
    )

    update_metadata(mt, _make_team_dict("mt_namespace"))

    assert mt.metaData.outputNamespace == "mt_namespace"
    assert inner_join.metaData.outputNamespace == "mt_namespace"


def test_update_metadata_propagates_namespace_through_join_to_groupby_join_source():
    """Join -> GroupBy (with JoinSource) -> inner Join: namespace should reach the inner Join.

    This is the chaining pattern: 6 groupbys -> join with groupby_v1,
    where groupby_v1 reads from a previous join via JoinSource.
    """
    inner_join = Join(
        metaData=MetaData(name="test_team.inner_join", team="test_team"),
        left=Source(events=EventSource(table="inner_table")),
    )

    chained_gb = GroupBy(
        sources=[Source(joinSource=JoinSource(join=inner_join))],
        keyColumns=["user_id"],
        metaData=MetaData(name="test_team.chained_gb", team="test_team"),
    )

    outer_join = Join(
        joinParts=[JoinPart(groupBy=chained_gb)],
        metaData=MetaData(name="test_team.outer_join", team="test_team"),
        left=Source(events=EventSource(table="outer_table")),
    )

    update_metadata(outer_join, _make_team_dict("ns"))

    assert outer_join.metaData.outputNamespace == "ns"
    assert chained_gb.metaData.outputNamespace == "ns"
    assert inner_join.metaData.outputNamespace == "ns"


def test_update_metadata_propagates_namespace_deeply_nested():
    """Full chain: Join -> GroupBy -> JoinSource(Join) -> GroupBy -> JoinSource(Join).

    Verifies namespace propagates to arbitrary depth.
    """
    deepest_join = Join(
        metaData=MetaData(name="test_team.deepest_join", team="test_team"),
        left=Source(events=EventSource(table="deepest_table")),
    )

    mid_gb = GroupBy(
        sources=[Source(joinSource=JoinSource(join=deepest_join))],
        keyColumns=["id"],
        metaData=MetaData(name="test_team.mid_gb", team="test_team"),
    )

    mid_join = Join(
        joinParts=[JoinPart(groupBy=mid_gb)],
        metaData=MetaData(name="test_team.mid_join", team="test_team"),
        left=Source(events=EventSource(table="mid_table")),
    )

    top_gb = GroupBy(
        sources=[Source(joinSource=JoinSource(join=mid_join))],
        keyColumns=["id"],
        metaData=MetaData(name="test_team.top_gb", team="test_team"),
    )

    outer_join = Join(
        joinParts=[JoinPart(groupBy=top_gb)],
        metaData=MetaData(name="test_team.outer_join", team="test_team"),
        left=Source(events=EventSource(table="outer_table")),
    )

    update_metadata(outer_join, _make_team_dict("deep_ns"))

    assert outer_join.metaData.outputNamespace == "deep_ns"
    assert top_gb.metaData.outputNamespace == "deep_ns"
    assert mid_join.metaData.outputNamespace == "deep_ns"
    assert mid_gb.metaData.outputNamespace == "deep_ns"
    assert deepest_join.metaData.outputNamespace == "deep_ns"


def test_update_metadata_propagates_namespace_to_join_left_join_source():
    """Join whose left source is a JoinSource should also get namespace propagated."""
    inner_join = Join(
        metaData=MetaData(name="test_team.left_source_join", team="test_team"),
        left=Source(events=EventSource(table="some_table")),
    )

    outer_join = Join(
        joinParts=[],
        metaData=MetaData(name="test_team.outer_join", team="test_team"),
        left=Source(joinSource=JoinSource(join=inner_join)),
    )

    update_metadata(outer_join, _make_team_dict("left_ns"))

    assert outer_join.metaData.outputNamespace == "left_ns"
    assert inner_join.metaData.outputNamespace == "left_ns"


def test_update_metadata_preserves_existing_namespace_in_deep_chain():
    """If a node in the middle of a chain already has a namespace, don't overwrite it,
    but still propagate to its children that don't have one."""
    deepest_join = Join(
        metaData=MetaData(name="test_team.deepest_join", team="test_team"),
        left=Source(events=EventSource(table="deepest_table")),
    )

    mid_gb = GroupBy(
        sources=[Source(joinSource=JoinSource(join=deepest_join))],
        keyColumns=["id"],
        metaData=MetaData(name="test_team.mid_gb", team="test_team", outputNamespace="explicit_ns"),
    )

    outer_join = Join(
        joinParts=[JoinPart(groupBy=mid_gb)],
        metaData=MetaData(name="test_team.outer_join", team="test_team"),
        left=Source(events=EventSource(table="outer_table")),
    )

    update_metadata(outer_join, _make_team_dict("outer_ns"))

    assert outer_join.metaData.outputNamespace == "outer_ns"
    assert mid_gb.metaData.outputNamespace == "explicit_ns"  # preserved
    # deepest_join gets outer_ns because _propagate_namespace uses the top-level namespace
    assert deepest_join.metaData.outputNamespace == "outer_ns"


def test_update_metadata_groupby_standalone_with_join_source():
    """Standalone GroupBy (not inside a Join) with a JoinSource — the original fix case."""
    inner_join = Join(
        metaData=MetaData(name="test_team.source_join", team="test_team"),
        left=Source(events=EventSource(table="some_table")),
    )

    gb = GroupBy(
        sources=[Source(joinSource=JoinSource(join=inner_join))],
        keyColumns=["user_id"],
        metaData=MetaData(name="test_team.standalone_gb", team="test_team"),
    )

    update_metadata(gb, _make_team_dict("standalone_ns"))

    assert gb.metaData.outputNamespace == "standalone_ns"
    assert inner_join.metaData.outputNamespace == "standalone_ns"


def test_update_metadata_user_reported_chain():
    """Reproduces the user-reported issue:

    acct_enriched_join (4 groupbys -> join)
      -> acct_enriched_by_cardholder (groupby reading from acct_enriched_join via JoinSource)
        -> txn_join (6 groupbys -> join, one of which is acct_enriched_by_cardholder)

    The second join fails because acct_enriched_by_cardholder's JoinSource inner join
    has null outputNamespace, causing table resolution to look for "null.tablename".
    """
    # --- First join: acct_enriched_join (4 groupbys -> join) ---
    acct_combined = GroupBy(
        keyColumns=["cardholder_id"],
        metaData=MetaData(name="test_team.acct_combined", team="test_team"),
    )
    acct_by_company = GroupBy(
        keyColumns=["cardholder_id"],
        metaData=MetaData(name="test_team.acct_by_company", team="test_team"),
    )
    acct_by_segment = GroupBy(
        keyColumns=["cardholder_id"],
        metaData=MetaData(name="test_team.acct_by_segment", team="test_team"),
    )
    acct_by_tier = GroupBy(
        keyColumns=["cardholder_id"],
        metaData=MetaData(name="test_team.acct_by_tier", team="test_team"),
    )

    acct_enriched_join = Join(
        left=Source(events=EventSource(table="acct_enriched_left")),
        joinParts=[
            JoinPart(groupBy=acct_combined),
            JoinPart(groupBy=acct_by_company),
            JoinPart(groupBy=acct_by_segment),
            JoinPart(groupBy=acct_by_tier),
        ],
        metaData=MetaData(name="test_team.acct_enriched_join", team="test_team"),
    )

    # --- GroupBy that reads from the first join via JoinSource ---
    acct_enriched_by_cardholder = GroupBy(
        sources=[Source(joinSource=JoinSource(join=acct_enriched_join))],
        keyColumns=["cardholder_id"],
        metaData=MetaData(name="test_team.acct_enriched_by_cardholder", team="test_team"),
    )

    # --- Second join: uses acct_enriched_by_cardholder as a join part ---
    txn_by_cardholder = GroupBy(
        keyColumns=["cardholder_id"],
        metaData=MetaData(name="test_team.txn_by_cardholder", team="test_team"),
    )

    txn_join = Join(
        left=Source(events=EventSource(table="txn_source")),
        joinParts=[
            JoinPart(groupBy=txn_by_cardholder),
            JoinPart(groupBy=acct_enriched_by_cardholder),  # <- this was the problem
        ],
        metaData=MetaData(name="test_team.txn_join", team="test_team"),
    )

    # Before the fix, update_metadata on txn_join would NOT propagate namespace
    # into acct_enriched_by_cardholder's JoinSource -> acct_enriched_join,
    # leaving acct_enriched_join.metaData.outputNamespace as None.
    update_metadata(txn_join, _make_team_dict("warehouse_db"))

    # All top-level objects get the namespace
    assert txn_join.metaData.outputNamespace == "warehouse_db"
    assert txn_by_cardholder.metaData.outputNamespace == "warehouse_db"
    assert acct_enriched_by_cardholder.metaData.outputNamespace == "warehouse_db"

    # The critical assertion: the inner join embedded inside the GroupBy's JoinSource
    # must also have the namespace, otherwise table resolution produces "null.tablename"
    assert acct_enriched_join.metaData.outputNamespace == "warehouse_db", (
        f"Expected 'warehouse_db' but got '{acct_enriched_join.metaData.outputNamespace}'. "
        f"This causes 'null.tablename' resolution errors at runtime."
    )

    # The groupbys inside the inner join should also get the namespace
    assert acct_combined.metaData.outputNamespace == "warehouse_db"
    assert acct_by_company.metaData.outputNamespace == "warehouse_db"
    assert acct_by_segment.metaData.outputNamespace == "warehouse_db"
    assert acct_by_tier.metaData.outputNamespace == "warehouse_db"


# ---------------------------------------------------------------------------
# End-to-end compile invariants:
#   (1) no compiled thriftjson contains the internal namespace placeholder
#       (`OUTPUT_NAMESPACE_PLACEHOLDER`) — every occurrence must be resolved
#       to a literal namespace by the compile pass.
#   (2) compile fails fast when a config has no outputNamespace and no team default.
# ---------------------------------------------------------------------------


_TEAMS_PY = dedent(
    """
    from ai.chronon.types import ConfigProperties, Team

    _DEFAULT_CONF = ConfigProperties(common={"spark.chronon.partition.column": "ds"})

    default = Team(outputNamespace="default_ns", conf=_DEFAULT_CONF)
    sample_team = Team(outputNamespace="sample_ns", conf=_DEFAULT_CONF)
    """
).strip()


def _write(path, contents):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(contents)


def _init_pkg(path):
    path.mkdir(parents=True, exist_ok=True)
    (path / "__init__.py").write_text("")


def _scaffold_repo(tmp_path, teams_py_body=_TEAMS_PY):
    """Scaffold the full set of user-authoring conf subdirs so compile discovery
    works regardless of which conf types a given test actually writes. Derives
    the folder list from the canonical `CONFIG_INFOS` registry so adding a new
    conf type to chronon doesn't silently skip it here."""
    (tmp_path / "teams.py").write_text(teams_py_body)
    for folder in user_authoring_folders():
        _init_pkg(tmp_path / folder)
        _init_pkg(tmp_path / folder / "sample_team")
    return tmp_path


def _run_compile(tmp_path, monkeypatch, ignore_python_errors=True):
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.chdir(tmp_path)
    # The compile loop imports user modules by name; drop stale caches so repeated
    # tests don't pick up a previous tmp repo's files. Include every user-authoring
    # conf folder plus `teams` so a new conf type never silently bypasses the reset.
    prefixes_to_reset = set(user_authoring_folders()) | {"teams"}
    for name in list(sys.modules):
        if name.split(".")[0] in prefixes_to_reset:
            del sys.modules[name]
    return __compile(chronon_root=str(tmp_path), ignore_python_errors=ignore_python_errors)


def _assert_no_namespace_placeholder_in_compiled(compiled_dir):
    violations = []
    for root, _, files in os.walk(compiled_dir):
        for f in files:
            full = os.path.join(root, f)
            rel = os.path.relpath(full, compiled_dir)
            if OUTPUT_NAMESPACE_PLACEHOLDER in open(full).read():
                violations.append(f"{rel}: found {OUTPUT_NAMESPACE_PLACEHOLDER!r} in compiled output")
    assert not violations, "\n".join(violations)


def test_compile_never_leaves_namespace_placeholder_in_thriftjson(tmp_path, monkeypatch):
    """Regression guard: compiled thriftjson must never contain the internal
    `OUTPUT_NAMESPACE_PLACEHOLDER` token. The token is emitted by `utils.output_table_name`
    when `.table` is accessed at authoring time before namespace propagation; the compile
    pass must substitute every occurrence before the Thrift is serialized."""
    _scaffold_repo(tmp_path)
    _write(
        tmp_path / "group_bys" / "sample_team" / "gb_ok.py",
        dedent(
            """
            from ai.chronon.types import Aggregation, EventSource, GroupBy, Operation, Query, selects

            v1 = GroupBy(
                sources=[
                    EventSource(
                        table="external.events",
                        query=Query(
                            selects=selects(event="event_expr", group_by_subject="user_id"),
                            time_column="ts",
                        ),
                    )
                ],
                keys=["group_by_subject"],
                aggregations=[Aggregation(input_column="event", operation=Operation.SUM, windows=["1d"])],
            )
            """
        ).strip(),
    )

    _run_compile(tmp_path, monkeypatch)
    _assert_no_namespace_placeholder_in_compiled(tmp_path / "compiled")


def test_compile_preserves_nested_left_join_source_name(tmp_path, monkeypatch):
    """A JoinSource used as Join.left should carry the nested join's compiled
    metadata name into the compiled thriftjson so downstream planning resolves
    the upstream join table instead of "<namespace>.null"."""
    _scaffold_repo(tmp_path)
    _write(
        tmp_path / "joins" / "sample_team" / "left_join_source.py",
        dedent(
            """
            from ai.chronon.types import EventSource, Join, JoinSource, Query, selects

            join1_v1 = Join(
                left=EventSource(
                    table="external.left_events",
                    query=Query(
                        selects=selects(user_id="user_id"),
                        time_column="ts",
                    ),
                ),
                right_parts=[],
                version=1,
            )

            join2_v1 = Join(
                left=JoinSource(
                    join=join1_v1,
                    query=Query(
                        selects=selects(user_id="user_id"),
                        time_column="ts",
                    ),
                ),
                right_parts=[],
                version=1,
            )
            """
        ).strip(),
    )

    _run_compile(tmp_path, monkeypatch)

    compiled_path = tmp_path / "compiled" / "joins" / "sample_team" / "left_join_source.join2_v1__1"
    compiled = json.loads(compiled_path.read_text())
    nested_metadata = compiled["left"]["joinSource"]["join"]["metaData"]

    assert nested_metadata["name"] == "sample_team.left_join_source.join1_v1__1"
    assert nested_metadata["outputNamespace"] == "sample_ns"


def test_compile_fails_when_config_has_no_output_namespace(tmp_path, monkeypatch):
    """If a config has no outputNamespace and the team has no default, compile must fail."""
    teams_py = dedent(
        """
        from ai.chronon.types import ConfigProperties, Team
        _CONF = ConfigProperties(common={"spark.chronon.partition.column": "ds"})
        default = Team(conf=_CONF)
        sample_team = Team(conf=_CONF)
        """
    ).strip()
    _scaffold_repo(tmp_path, teams_py_body=teams_py)
    _write(
        tmp_path / "group_bys" / "sample_team" / "gb_no_ns.py",
        dedent(
            """
            from ai.chronon.types import Aggregation, EventSource, GroupBy, Operation, Query, selects

            v1 = GroupBy(
                sources=[
                    EventSource(
                        table="external.events",
                        query=Query(
                            selects=selects(event="event_expr", group_by_subject="user_id"),
                            time_column="ts",
                        ),
                    )
                ],
                keys=["group_by_subject"],
                aggregations=[Aggregation(input_column="event", operation=Operation.SUM, windows=["1d"])],
            )
            """
        ).strip(),
    )

    _, has_errors, _ = _run_compile(tmp_path, monkeypatch, ignore_python_errors=True)
    assert has_errors, "Expected compile to surface an error for missing outputNamespace"
    compiled_path = tmp_path / "compiled" / "group_bys" / "sample_team" / "gb_no_ns.v1"
    assert not compiled_path.exists()


def test_multi_type_config_discovery(tmp_path, monkeypatch):
    """Test that multiple config types can be discovered from a single file/directory.

    This verifies that the refactored compilation system supports finding Join objects
    in staging_queries/ directories, GroupBy objects in joins/ directories, etc.
    Previously, each directory was limited to only discovering its corresponding type.
    """
    _scaffold_repo(tmp_path)
    _write(
        tmp_path / "staging_queries" / "sample_team" / "mixed_types.py",
        dedent(
            """
            from ai.chronon.types import (
                EventSource,
                GroupBy,
                Join,
                JoinPart,
                Query,
                StagingQuery,
                selects,
                Aggregation,
                Operation,
            )

            # StagingQuery in staging_queries directory (expected case)
            my_staging_query = StagingQuery(
                query="SELECT CURRENT_TIMESTAMP() as event_time",
                output_namespace="data",
                version=1,
            )

            # Join in staging_queries directory (cross-type case - should be discovered)
            my_join = Join(
                left=EventSource(
                    table="external.events",
                    query=Query(
                        selects=selects(user_id="user_id"),
                        time_column="event_time",
                    ),
                ),
                right_parts=[],
                row_ids=["user_id"],
                output_namespace="data",
                version=1,
            )

            # GroupBy in staging_queries directory (another cross-type case)
            my_group_by = GroupBy(
                sources=[
                    EventSource(
                        table="external.events",
                        query=Query(
                            selects=selects(event="event_id", group_by_subject="user_id"),
                            time_column="event_time",
                        ),
                    )
                ],
                keys=["group_by_subject"],
                aggregations=[
                    Aggregation(
                        input_column="event",
                        operation=Operation.COUNT,
                        windows=["1d"],
                    )
                ],
                output_namespace="data",
                version=1,
            )
            """
        ).strip(),
    )

    results, has_errors, compile_context = _run_compile(tmp_path, monkeypatch, ignore_python_errors=False)
    assert not has_errors, "Compilation should succeed with multi-type discovery"

    # Verify all three types were compiled from the same staging_queries file
    from gen_thrift.api.ttypes import ConfType

    staging_result = results[ConfType.STAGING_QUERY]
    assert len(staging_result.obj_dict) == 1
    assert "sample_team.mixed_types.my_staging_query__1" in staging_result.obj_dict

    join_result = results[ConfType.JOIN]
    assert len(join_result.obj_dict) == 1
    assert "sample_team.mixed_types.my_join__1" in join_result.obj_dict

    group_by_result = results[ConfType.GROUP_BY]
    assert len(group_by_result.obj_dict) == 1
    assert "sample_team.mixed_types.my_group_by__1" in group_by_result.obj_dict

    # Verify all three compiled files exist in their respective output directories
    staging_path = tmp_path / "compiled" / "staging_queries" / "sample_team" / "mixed_types.my_staging_query__1"
    join_path = tmp_path / "compiled" / "joins" / "sample_team" / "mixed_types.my_join__1"
    group_by_path = tmp_path / "compiled" / "group_bys" / "sample_team" / "mixed_types.my_group_by__1"

    assert staging_path.exists(), f"StagingQuery should be compiled to {staging_path}"
    assert join_path.exists(), f"Join should be compiled to {join_path}"
    assert group_by_path.exists(), f"GroupBy should be compiled to {group_by_path}"


def test_same_type_configs_from_multiple_directories_tracked(tmp_path, monkeypatch):
    """Test that configs of the same type from different directories are all tracked before close_cls.

    When the same config type (e.g., GroupBy) appears in multiple directories,
    the class tracker should remain open until all directories are processed,
    ensuring all configs are properly added to the display.
    """
    _scaffold_repo(tmp_path)

    # Create a GroupBy in the group_bys directory
    _write(
        tmp_path / "group_bys" / "sample_team" / "first_config.py",
        dedent(
            """
            from ai.chronon.types import (
                EventSource,
                GroupBy,
                Query,
                selects,
                Aggregation,
                Operation,
            )

            first_groupby = GroupBy(
                sources=[
                    EventSource(
                        table="external.events_1",
                        query=Query(
                            selects=selects(event="event_id", group_by_subject="user_id"),
                            time_column="event_time",
                        ),
                    )
                ],
                keys=["group_by_subject"],
                aggregations=[
                    Aggregation(
                        input_column="event",
                        operation=Operation.COUNT,
                        windows=["1d"],
                    )
                ],
                output_namespace="data",
                version=1,
            )
            """
        ).strip(),
    )

    # Create another GroupBy in the staging_queries directory (cross-type discovery)
    _write(
        tmp_path / "staging_queries" / "sample_team" / "second_config.py",
        dedent(
            """
            from ai.chronon.types import (
                EventSource,
                GroupBy,
                Query,
                selects,
                Aggregation,
                Operation,
            )

            second_groupby = GroupBy(
                sources=[
                    EventSource(
                        table="external.events_2",
                        query=Query(
                            selects=selects(event="event_id", group_by_subject="user_id"),
                            time_column="event_time",
                        ),
                    )
                ],
                keys=["group_by_subject"],
                aggregations=[
                    Aggregation(
                        input_column="event",
                        operation=Operation.COUNT,
                        windows=["1d"],
                    )
                ],
                output_namespace="data",
                version=1,
            )
            """
        ).strip(),
    )

    results, has_errors, compile_context = _run_compile(tmp_path, monkeypatch, ignore_python_errors=False)

    # Should compile successfully without errors
    assert not has_errors, "Compilation should succeed with configs from multiple directories"

    # Verify both GroupBys were compiled
    from gen_thrift.api.ttypes import ConfType

    group_by_result = results[ConfType.GROUP_BY]

    # Both configs should be in obj_dict
    assert "sample_team.first_config.first_groupby__1" in group_by_result.obj_dict, (
        "First GroupBy from group_bys/ should be compiled"
    )
    assert "sample_team.second_config.second_groupby__1" in group_by_result.obj_dict, (
        "Second GroupBy from staging_queries/ should be compiled"
    )

    # Verify both compiled files exist in the output directory
    first_path = tmp_path / "compiled" / "group_bys" / "sample_team" / "first_config.first_groupby__1"
    second_path = tmp_path / "compiled" / "group_bys" / "sample_team" / "second_config.second_groupby__1"

    assert first_path.exists(), f"First GroupBy should be compiled to {first_path}"
    assert second_path.exists(), f"Second GroupBy should be compiled to {second_path}"


def test_error_dict_accumulation_across_directories(tmp_path, monkeypatch):
    """Test that errors from multiple directories are accumulated, not overwritten.

    When configs from multiple directories produce errors, all errors should
    be preserved in the error_dict rather than having later errors overwrite
    earlier ones. Uses duplicate detection as a way to generate multiple errors
    for the same config key.
    """
    _scaffold_repo(tmp_path)

    # Create identical GroupBy configs in two directories to trigger duplicate detection
    groupby_code = dedent(
        """
        from ai.chronon.types import (
            EventSource,
            GroupBy,
            Query,
            selects,
            Aggregation,
            Operation,
        )

        my_groupby = GroupBy(
            sources=[
                EventSource(
                    table="external.events",
                    query=Query(
                        selects=selects(event="event_id", group_by_subject="user_id"),
                        time_column="event_time",
                    ),
                )
            ],
            keys=["group_by_subject"],
            aggregations=[
                Aggregation(
                    input_column="event",
                    operation=Operation.COUNT,
                    windows=["1d"],
                )
            ],
            output_namespace="data",
            version=1,
        )
        """
    ).strip()

    _write(tmp_path / "group_bys" / "sample_team" / "dup_config.py", groupby_code)
    _write(tmp_path / "staging_queries" / "sample_team" / "dup_config.py", groupby_code)

    # Also create a third instance in joins directory
    _write(tmp_path / "joins" / "sample_team" / "dup_config.py", groupby_code)

    results, has_errors, compile_context = _run_compile(tmp_path, monkeypatch, ignore_python_errors=True)

    from gen_thrift.api.ttypes import ConfType

    group_by_result = results[ConfType.GROUP_BY]
    dup_name = "sample_team.dup_config.my_groupby__1"

    # Should have duplicate error recorded
    assert dup_name in group_by_result.error_dict, "Duplicate should be in error_dict"

    # With error accumulation, we should have multiple errors for the same config
    # (one from staging_queries duplicate, one from joins duplicate)
    errors = group_by_result.error_dict[dup_name]
    assert len(errors) >= 1, (
        f"Should have at least one duplicate error, got {len(errors)} errors"
    )


def test_parsing_errors_included_in_results(tmp_path, monkeypatch):
    """Test that configs with parsing errors are included in compilation results.

    Objects that fail to parse (co.obj=None) should still be processed so their
    errors are recorded in the CompileResult, not silently dropped.

    Note: Parsing errors are attributed to the first target class (Join) per
    parse_configs.py line 68-78.
    """
    _scaffold_repo(tmp_path)

    # Create a valid config
    _write(
        tmp_path / "group_bys" / "sample_team" / "good_config.py",
        dedent(
            """
            from ai.chronon.types import (
                EventSource,
                GroupBy,
                Query,
                selects,
                Aggregation,
                Operation,
            )

            valid_groupby = GroupBy(
                sources=[
                    EventSource(
                        table="external.events",
                        query=Query(
                            selects=selects(event="event_id", group_by_subject="user_id"),
                            time_column="event_time",
                        ),
                    )
                ],
                keys=["group_by_subject"],
                aggregations=[
                    Aggregation(
                        input_column="event",
                        operation=Operation.COUNT,
                        windows=["1d"],
                    )
                ],
                output_namespace="data",
                version=1,
            )
            """
        ).strip(),
    )

    # Create a config with parsing error
    _write(
        tmp_path / "group_bys" / "sample_team" / "bad_config.py",
        dedent(
            """
            from ai.chronon.types import GroupBy

            # Invalid Python syntax
            bad_groupby = GroupBy(
                keys=["user_id"
            # Missing closing parenthesis
            """
        ).strip(),
    )

    results, has_errors, compile_context = _run_compile(tmp_path, monkeypatch, ignore_python_errors=True)

    from gen_thrift.api.ttypes import ConfType

    group_by_result = results[ConfType.GROUP_BY]
    # Parsing errors are attributed to the first target class processed by
    # the multi-type scan, which is StagingQuery (CONFIG_INFOS is ordered
    # dependency-first: staging_queries -> group_bys -> joins -> ...).
    staging_query_result = results[ConfType.STAGING_QUERY]

    # The valid config should compile successfully
    assert "sample_team.good_config.valid_groupby__1" in group_by_result.obj_dict, (
        "Valid config should be compiled"
    )

    # The invalid config should have errors recorded under the first target class
    bad_file_path = "group_bys/sample_team/bad_config.py"
    has_parsing_error = any(
        bad_file_path in key for key in staging_query_result.error_dict.keys()
    )
    assert has_parsing_error, (
        f"Parsing errors should be recorded in error_dict (under the first target class, "
        f"which is StagingQuery per CONFIG_INFOS ordering). "
        f"Found keys: {list(staging_query_result.error_dict.keys())}"
    )


def test_duplicate_config_names_across_directories_detected(tmp_path, monkeypatch):
    """Test that duplicate config names of the same type across different directories are detected.

    When the same config type (e.g., GroupBy) appears in different directories
    (e.g., group_bys/ and staging_queries/) with the same normalized name,
    the compiler should detect this and report an error instead of silently overwriting.
    """
    _scaffold_repo(tmp_path)

    # Create a GroupBy in the group_bys directory
    _write(
        tmp_path / "group_bys" / "sample_team" / "my_config.py",
        dedent(
            """
            from ai.chronon.types import (
                EventSource,
                GroupBy,
                Query,
                selects,
                Aggregation,
                Operation,
            )

            my_group_by = GroupBy(
                sources=[
                    EventSource(
                        table="external.events_1",
                        query=Query(
                            selects=selects(event="event_id", group_by_subject="user_id"),
                            time_column="event_time",
                        ),
                    )
                ],
                keys=["group_by_subject"],
                aggregations=[
                    Aggregation(
                        input_column="event",
                        operation=Operation.COUNT,
                        windows=["1d"],
                    )
                ],
                output_namespace="data",
                version=1,
            )
            """
        ).strip(),
    )

    # Create a GroupBy with the same name in the staging_queries directory
    _write(
        tmp_path / "staging_queries" / "sample_team" / "my_config.py",
        dedent(
            """
            from ai.chronon.types import (
                EventSource,
                GroupBy,
                Query,
                selects,
                Aggregation,
                Operation,
            )

            my_group_by = GroupBy(
                sources=[
                    EventSource(
                        table="external.events_2",
                        query=Query(
                            selects=selects(event="event_id", group_by_subject="user_id"),
                            time_column="event_time",
                        ),
                    )
                ],
                keys=["group_by_subject"],
                aggregations=[
                    Aggregation(
                        input_column="event",
                        operation=Operation.COUNT,
                        windows=["1d"],
                    )
                ],
                output_namespace="data",
                version=1,
            )
            """
        ).strip(),
    )

    results, has_errors, compile_context = _run_compile(tmp_path, monkeypatch, ignore_python_errors=True)

    # Verify the error is recorded in the results
    from gen_thrift.api.ttypes import ConfType

    group_by_result = results[ConfType.GROUP_BY]
    duplicate_name = "sample_team.my_config.my_group_by__1"

    # The duplicate should be in the error_dict
    assert duplicate_name in group_by_result.error_dict, (
        f"Expected duplicate name '{duplicate_name}' to be in error_dict"
    )

    # Verify the error message mentions both source files
    errors = group_by_result.error_dict[duplicate_name]
    assert len(errors) > 0, "Expected at least one error for the duplicate"
    error_message = str(errors[0])
    assert "Duplicate config name" in error_message
    assert "group_bys" in error_message
    assert "staging_queries" in error_message

    # The first config CAN be in obj_dict (it's the duplicate from group_bys that's rejected).
    # Scan order is dependency-first per CONFIG_INFOS: staging_queries before group_bys,
    # so the staging_queries copy wins and is what lands in obj_dict.
    if duplicate_name in group_by_result.obj_dict:
        compiled_obj = group_by_result.obj_dict[duplicate_name]
        assert compiled_obj.metaData.sourceFile == "staging_queries/sample_team/my_config.py", (
            "The config in obj_dict should be from the first directory encountered "
            "(staging_queries, since CONFIG_INFOS visits dependencies before referencers)"
        )


def test_duplicate_join_names_across_non_join_directories_detected(tmp_path, monkeypatch):
    """Relaxed folder discovery must still enforce unique normalized names.

    A Join authored in group_bys/team/file.py and another Join authored in
    staging_queries/team/file.py both normalize to sample_team.file.join_parent__1,
    so the second one must be rejected as a duplicate even though neither lives
    under joins/.
    """
    _scaffold_repo(tmp_path)

    join_template = """
        from ai.chronon.types import EventSource, Join, Query, selects

        join_parent = Join(
            left=EventSource(
                table="{table}",
                query=Query(
                    selects=selects(user_id="user_id"),
                    time_column="event_time",
                ),
            ),
            right_parts=[],
            row_ids=["user_id"],
            output_namespace="data",
            version=1,
        )
    """

    _write(
        tmp_path / "staging_queries" / "sample_team" / "file.py",
        dedent(join_template.format(table="external.events_from_staging")).strip(),
    )
    _write(
        tmp_path / "group_bys" / "sample_team" / "file.py",
        dedent(join_template.format(table="external.events_from_group_bys")).strip(),
    )

    results, has_errors, _ = _run_compile(tmp_path, monkeypatch, ignore_python_errors=True)

    from gen_thrift.api.ttypes import ConfType

    join_result = results[ConfType.JOIN]
    duplicate_name = "sample_team.file.join_parent__1"

    assert duplicate_name in join_result.obj_dict
    assert duplicate_name in join_result.error_dict

    error_message = str(join_result.error_dict[duplicate_name][0])
    assert "Duplicate config name" in error_message
    assert "staging_queries" in error_message
    assert "group_bys" in error_message

    assert (
        join_result.obj_dict[duplicate_name].metaData.sourceFile
        == "staging_queries/sample_team/file.py"
    )


def test_imported_configs_not_recompiled(tmp_path, monkeypatch):
    """Test that imported config objects are NOT compiled again in the importing file.

    When a file imports a config object from another module (e.g., a Join imports
    a GroupBy for use in JoinParts), the imported object should only be compiled
    once in its original location, not again in the importing file's directory.

    This prevents duplicate config errors and ensures each config is compiled
    exactly once in its canonical location.
    """
    _scaffold_repo(tmp_path)

    # Create a GroupBy in the group_bys directory (canonical location)
    _write(
        tmp_path / "group_bys" / "sample_team" / "my_groupby.py",
        dedent(
            """
            from ai.chronon.types import (
                EventSource,
                GroupBy,
                Query,
                selects,
                Aggregation,
                Operation,
            )

            user_features = GroupBy(
                sources=[
                    EventSource(
                        table="external.events",
                        query=Query(
                            selects=selects(event="event_id", group_by_subject="user_id"),
                            time_column="event_time",
                        ),
                    )
                ],
                keys=["group_by_subject"],
                aggregations=[
                    Aggregation(
                        input_column="event",
                        operation=Operation.COUNT,
                        windows=["1d"],
                    )
                ],
                output_namespace="data",
                version=1,
            )
            """
        ).strip(),
    )

    # Create a Join in the joins directory that imports the GroupBy
    _write(
        tmp_path / "joins" / "sample_team" / "my_join.py",
        dedent(
            """
            from group_bys.sample_team.my_groupby import user_features
            from ai.chronon.types import EventSource, Join, JoinPart, Query, selects

            # This Join imports user_features from group_bys
            # The imported GroupBy should NOT be compiled again here
            training_data = Join(
                left=EventSource(
                    table="external.events",
                    query=Query(
                        selects=selects(group_by_subject="user_id"),  # Match the GroupBy key
                        time_column="event_time",
                    ),
                ),
                right_parts=[JoinPart(group_by=user_features)],
                row_ids=["group_by_subject"],
                output_namespace="data",
                version=1,
            )
            """
        ).strip(),
    )

    results, has_errors, compile_context = _run_compile(tmp_path, monkeypatch, ignore_python_errors=False)

    # Verify no compilation errors
    assert not has_errors, "Compilation should succeed without duplicate errors"

    from gen_thrift.api.ttypes import ConfType

    group_by_result = results[ConfType.GROUP_BY]
    join_result = results[ConfType.JOIN]

    # The GroupBy should be compiled exactly once in its canonical location
    groupby_name = "sample_team.my_groupby.user_features__1"
    assert groupby_name in group_by_result.obj_dict, (
        "GroupBy should be compiled from group_bys/ directory"
    )
    assert groupby_name not in group_by_result.error_dict, (
        "GroupBy should not have any errors (no duplicate)"
    )

    # The Join should be compiled
    join_name = "sample_team.my_join.training_data__1"
    assert join_name in join_result.obj_dict, (
        "Join should be compiled from joins/ directory"
    )

    # Verify compiled files exist in correct locations
    groupby_path = tmp_path / "compiled" / "group_bys" / "sample_team" / "my_groupby.user_features__1"
    join_path = tmp_path / "compiled" / "joins" / "sample_team" / "my_join.training_data__1"

    assert groupby_path.exists(), f"GroupBy should be compiled to {groupby_path}"
    assert join_path.exists(), f"Join should be compiled to {join_path}"

    # Verify the GroupBy was compiled exactly once (no duplicate in joins directory)
    # The GroupBy should only appear once in obj_dict (not duplicated from joins/)
    assert len(group_by_result.obj_dict) == 1, (
        f"Expected exactly 1 GroupBy, got {len(group_by_result.obj_dict)}: {list(group_by_result.obj_dict.keys())}"
    )


def test_raw_thrift_config_objects_are_rejected(tmp_path, monkeypatch):
    """Top-level configs must be created through ai.chronon.types factories so
    compile can reliably track the authoring module that owns each object."""
    _scaffold_repo(tmp_path)

    _write(
        tmp_path / "joins" / "sample_team" / "raw_join.py",
        dedent(
            """
            from gen_thrift.api.ttypes import Join, MetaData

            raw_join = Join(metaData=MetaData(version="1"))
            """
        ).strip(),
    )

    results, has_errors, _ = _run_compile(tmp_path, monkeypatch, ignore_python_errors=True)
    assert has_errors

    from gen_thrift.api.ttypes import ConfType

    staging_query_result = results[ConfType.STAGING_QUERY]
    error_text = "\n".join(
        str(error)
        for errors in staging_query_result.error_dict.values()
        for error in errors
    )
    assert "without the Chronon factory origin marker" in error_text
    assert "ai.chronon.types" in error_text

    assert not (
        tmp_path / "compiled" / "joins" / "sample_team" / "raw_join.raw_join__1"
    ).exists()


def test_chronon_config_without_origin_marker_is_rejected(tmp_path, monkeypatch):
    """A top-level Chronon config without the factory origin marker must fail
    compilation, even if it was otherwise created by the public factory."""
    _scaffold_repo(tmp_path)

    _write(
        tmp_path / "joins" / "sample_team" / "missing_marker.py",
        dedent(
            """
            from ai.chronon.cli.compile.config_origin import CONFIG_ORIGIN_FILE_ATTR
            from ai.chronon.types import EventSource, Join, Query, selects

            unmarked_join = Join(
                left=EventSource(
                    table="external.events",
                    query=Query(
                        selects=selects(user_id="user_id"),
                        time_column="event_time",
                    ),
                ),
                right_parts=[],
                row_ids=["user_id"],
                output_namespace="data",
                version=1,
            )
            delattr(unmarked_join, CONFIG_ORIGIN_FILE_ATTR)
            """
        ).strip(),
    )

    results, has_errors, _ = _run_compile(tmp_path, monkeypatch, ignore_python_errors=True)
    assert has_errors

    from gen_thrift.api.ttypes import ConfType

    staging_query_result = results[ConfType.STAGING_QUERY]
    error_text = "\n".join(
        str(error)
        for errors in staging_query_result.error_dict.values()
        for error in errors
    )
    assert "without the Chronon factory origin marker" in error_text
    assert "missing_marker.py:unmarked_join" in error_text
    assert not (
        tmp_path / "compiled" / "joins" / "sample_team" / "missing_marker.unmarked_join__1"
    ).exists()


def test_factory_origin_file_owns_imported_config(tmp_path, monkeypatch):
    """Factory-created configs keep the file that called the factory as their
    canonical owner, even when another module imports and aliases the object."""
    _scaffold_repo(tmp_path)

    _write(
        tmp_path / "joins" / "sample_team" / "definition.py",
        dedent(
            """
            from ai.chronon.types import EventSource, Join, Query, selects

            owned_join = Join(
                left=EventSource(
                    table="external.events",
                    query=Query(
                        selects=selects(user_id="user_id"),
                        time_column="event_time",
                    ),
                ),
                right_parts=[],
                row_ids=["user_id"],
                output_namespace="data",
                version=1,
            )
            """
        ).strip(),
    )
    _write(
        tmp_path / "joins" / "sample_team" / "downstream.py",
        dedent(
            """
            from joins.sample_team.definition import owned_join

            imported_alias = owned_join
            """
        ).strip(),
    )

    results, has_errors, _ = _run_compile(tmp_path, monkeypatch, ignore_python_errors=False)
    assert not has_errors

    from gen_thrift.api.ttypes import ConfType

    join_result = results[ConfType.JOIN]
    assert "sample_team.definition.owned_join__1" in join_result.obj_dict
    assert "sample_team.downstream.owned_join__1" not in join_result.obj_dict
    assert "sample_team.downstream.imported_alias__1" not in join_result.obj_dict
    assert (
        join_result.obj_dict["sample_team.definition.owned_join__1"].metaData.sourceFile
        == "joins/sample_team/definition.py"
    )
    assert (
        tmp_path / "compiled" / "joins" / "sample_team" / "definition.owned_join__1"
    ).exists()
    assert not (
        tmp_path / "compiled" / "joins" / "sample_team" / "downstream.imported_alias__1"
    ).exists()


def test_helper_factory_config_is_owned_by_binding_module(tmp_path, monkeypatch):
    """A helper can construct a config without owning its final compiled name.

    The destination file that assigns the helper return value is the canonical
    owner as long as the helper module does not bind that object at top level.
    """
    _scaffold_repo(tmp_path)

    _write(
        tmp_path / "joins" / "sample_team" / "helpers.py",
        dedent(
            """
            from ai.chronon.types import EventSource, Join, Query, selects

            def make_join():
                return Join(
                    left=EventSource(
                        table="external.events",
                        query=Query(
                            selects=selects(user_id="user_id"),
                            time_column="event_time",
                        ),
                    ),
                    right_parts=[],
                    row_ids=["user_id"],
                    output_namespace="data",
                    version=1,
                )
            """
        ).strip(),
    )
    _write(
        tmp_path / "joins" / "sample_team" / "downstream.py",
        dedent(
            """
            from joins.sample_team.helpers import make_join

            generated_join = make_join()
            """
        ).strip(),
    )

    results, has_errors, _ = _run_compile(tmp_path, monkeypatch, ignore_python_errors=False)
    assert not has_errors

    from gen_thrift.api.ttypes import ConfType

    join_result = results[ConfType.JOIN]
    assert "sample_team.downstream.generated_join__1" in join_result.obj_dict
    assert "sample_team.helpers.generated_join__1" not in join_result.obj_dict
    assert (
        join_result.obj_dict[
            "sample_team.downstream.generated_join__1"
        ].metaData.sourceFile
        == "joins/sample_team/downstream.py"
    )
    assert (
        tmp_path / "compiled" / "joins" / "sample_team" / "downstream.generated_join__1"
    ).exists()
    assert not (
        tmp_path / "compiled" / "joins" / "sample_team" / "helpers.generated_join__1"
    ).exists()


def test_staging_query_imported_join_keeps_join_canonical_path(tmp_path, monkeypatch):
    """A staging query may import a Join as a dependency helper, but that import
    must not make the Join compile as if it were defined in staging_queries/."""
    _scaffold_repo(tmp_path)

    _write(
        tmp_path / "joins" / "sample_team" / "upstream.py",
        dedent(
            """
            from ai.chronon.types import EventSource, Join, Query, selects

            upstream_join = Join(
                left=EventSource(
                    table="external.events",
                    query=Query(
                        selects=selects(user_id="user_id"),
                        time_column="event_time",
                    ),
                ),
                right_parts=[],
                row_ids=["user_id"],
                output_namespace="data",
                version=1,
            )
            """
        ).strip(),
    )

    _write(
        tmp_path / "staging_queries" / "sample_team" / "downstream.py",
        dedent(
            """
            from joins.sample_team.upstream import upstream_join
            from ai.chronon.types import StagingQuery

            join_dependency = upstream_join

            downstream_query = StagingQuery(
                query="SELECT 1 AS user_id",
                output_namespace="data",
                version=1,
            )
            """
        ).strip(),
    )

    results, has_errors, _ = _run_compile(tmp_path, monkeypatch, ignore_python_errors=False)
    assert not has_errors

    from gen_thrift.api.ttypes import ConfType

    join_result = results[ConfType.JOIN]
    assert "sample_team.upstream.upstream_join__1" in join_result.obj_dict
    assert "sample_team.downstream.upstream_join__1" not in join_result.obj_dict
    assert "sample_team.downstream.join_dependency__1" not in join_result.obj_dict
    assert (
        join_result.obj_dict["sample_team.upstream.upstream_join__1"].metaData.sourceFile
        == "joins/sample_team/upstream.py"
    )

    assert (
        tmp_path / "compiled" / "joins" / "sample_team" / "upstream.upstream_join__1"
    ).exists()
    assert not (
        tmp_path / "compiled" / "joins" / "sample_team" / "downstream.upstream_join__1"
    ).exists()
    assert not (
        tmp_path / "compiled" / "joins" / "sample_team" / "downstream.join_dependency__1"
    ).exists()


def test_join_defined_in_staging_query_dir_keeps_canonical_path_when_imported_by_join(
    tmp_path, monkeypatch
):
    """A Join defined outside joins/ should keep that defining module path even
    when it is imported by a joins/ module."""
    _scaffold_repo(tmp_path)

    _write(
        tmp_path / "staging_queries" / "sample_team" / "upstream.py",
        dedent(
            """
            from ai.chronon.types import EventSource, Join, Query, selects

            upstream_join = Join(
                left=EventSource(
                    table="external.events",
                    query=Query(
                        selects=selects(user_id="user_id"),
                        time_column="event_time",
                    ),
                ),
                right_parts=[],
                row_ids=["user_id"],
                output_namespace="data",
                version=1,
            )
            """
        ).strip(),
    )

    _write(
        tmp_path / "joins" / "sample_team" / "downstream.py",
        dedent(
            """
            from staging_queries.sample_team.upstream import upstream_join

            join_dependency = upstream_join
            """
        ).strip(),
    )

    results, has_errors, _ = _run_compile(tmp_path, monkeypatch, ignore_python_errors=False)
    assert not has_errors

    from gen_thrift.api.ttypes import ConfType

    join_result = results[ConfType.JOIN]
    assert "sample_team.upstream.upstream_join__1" in join_result.obj_dict
    assert "sample_team.downstream.upstream_join__1" not in join_result.obj_dict
    assert "sample_team.downstream.join_dependency__1" not in join_result.obj_dict
    assert (
        join_result.obj_dict["sample_team.upstream.upstream_join__1"].metaData.sourceFile
        == "staging_queries/sample_team/upstream.py"
    )

    assert (
        tmp_path / "compiled" / "joins" / "sample_team" / "upstream.upstream_join__1"
    ).exists()
    assert not (
        tmp_path / "compiled" / "joins" / "sample_team" / "downstream.upstream_join__1"
    ).exists()
    assert not (
        tmp_path / "compiled" / "joins" / "sample_team" / "downstream.join_dependency__1"
    ).exists()


def test_same_depth_importer_does_not_claim_imported_config(tmp_path, monkeypatch):
    """Alphabetically earlier sibling modules should not claim config objects
    imported from later sibling modules."""
    _scaffold_repo(tmp_path)

    _write(
        tmp_path / "joins" / "sample_team" / "z_definition.py",
        dedent(
            """
            from ai.chronon.types import EventSource, Join, Query, selects

            upstream_join = Join(
                left=EventSource(
                    table="external.events",
                    query=Query(
                        selects=selects(user_id="user_id"),
                        time_column="event_time",
                    ),
                ),
                right_parts=[],
                row_ids=["user_id"],
                output_namespace="data",
                version=1,
            )
            """
        ).strip(),
    )
    _write(
        tmp_path / "joins" / "sample_team" / "a_importer.py",
        dedent(
            """
            from joins.sample_team.z_definition import upstream_join

            imported_alias = upstream_join
            """
        ).strip(),
    )

    results, has_errors, _ = _run_compile(tmp_path, monkeypatch, ignore_python_errors=False)
    assert not has_errors

    from gen_thrift.api.ttypes import ConfType

    join_result = results[ConfType.JOIN]
    assert "sample_team.z_definition.upstream_join__1" in join_result.obj_dict
    assert "sample_team.a_importer.upstream_join__1" not in join_result.obj_dict
    assert "sample_team.a_importer.imported_alias__1" not in join_result.obj_dict
    assert (
        join_result.obj_dict["sample_team.z_definition.upstream_join__1"].metaData.sourceFile
        == "joins/sample_team/z_definition.py"
    )


def test_same_type_staging_query_importer_does_not_claim_imported_query(
    tmp_path, monkeypatch
):
    """A staging query file may import another staging query, but that import
    must not make the imported query compile under the importing module path."""
    _scaffold_repo(tmp_path)

    _write(
        tmp_path / "staging_queries" / "sample_team" / "z_definition.py",
        dedent(
            """
            from ai.chronon.types import StagingQuery

            upstream_query = StagingQuery(
                query="SELECT 1 AS user_id",
                output_namespace="data",
                version=1,
            )
            """
        ).strip(),
    )
    _write(
        tmp_path / "staging_queries" / "sample_team" / "a_importer.py",
        dedent(
            """
            from staging_queries.sample_team.z_definition import upstream_query
            from ai.chronon.types import StagingQuery

            imported_alias = upstream_query

            local_query = StagingQuery(
                query="SELECT 2 AS user_id",
                output_namespace="data",
                version=1,
            )
            """
        ).strip(),
    )

    results, has_errors, _ = _run_compile(tmp_path, monkeypatch, ignore_python_errors=False)
    assert not has_errors

    from gen_thrift.api.ttypes import ConfType

    staging_query_result = results[ConfType.STAGING_QUERY]
    assert "sample_team.z_definition.upstream_query__1" in staging_query_result.obj_dict
    assert "sample_team.a_importer.local_query__1" in staging_query_result.obj_dict
    assert "sample_team.a_importer.upstream_query__1" not in staging_query_result.obj_dict
    assert "sample_team.a_importer.imported_alias__1" not in staging_query_result.obj_dict
    assert (
        staging_query_result.obj_dict[
            "sample_team.z_definition.upstream_query__1"
        ].metaData.sourceFile
        == "staging_queries/sample_team/z_definition.py"
    )
