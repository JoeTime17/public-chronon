"""End-to-end `zipline compile` checks against the canary confs.

These don't require a cluster — they just exercise the compile CLI locally and
assert post-conditions on the compiled thriftjson under `canary/compiled/` and
`canary/compiled_canary/`. The two trees are produced by two separate compile
passes: prod reads `teams.py`, canary reads `teams.canary.py`. Strict
isolation is enforced at the file boundary, so a sentinel set in one teams
file must never appear in the output of the other.

Kept in the unit suite (not `test/integration/`) so PR CI runs them via
`./mill python.test`; the integration suite only runs post-merge via
`push_to_canary.yaml` and filters to `-k test_run_quickstart`.
"""

import json
import os

import pytest
from click.testing import CliRunner

from ai.chronon.repo.compile import compile
from ai.chronon.utils import OUTPUT_NAMESPACE_PLACEHOLDER


def _compile_canary(canary_root):
    """Run `zipline compile --chronon-root <canary_root> --force`. Raises on non-zero
    exit with the full CLI output for actionable failure messages."""
    runner = CliRunner()
    result = runner.invoke(
        compile,
        ["--chronon-root", canary_root, "--force"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, f"Canary compile failed:\n{result.output}"


def test_canary_compile_resolves_namespace_placeholder(canary):
    """Run `zipline compile` against the canary confs and assert no compiled
    thriftjson contains the internal `OUTPUT_NAMESPACE_PLACEHOLDER` token. The
    placeholder is emitted by `utils.output_table_name` when `.table` is accessed
    at Python authoring time before namespace propagation; the compile pass must
    substitute every occurrence before the Thrift is serialized. Every
    fully-qualified table name must land as a literal `<namespace>.<name>`.
    """
    _compile_canary(canary)

    compiled_dir = os.path.join(canary, "compiled")
    assert os.path.isdir(compiled_dir), f"Expected compiled/ at {compiled_dir}"

    violations = []
    for root, _, files in os.walk(compiled_dir):
        for f in files:
            path = os.path.join(root, f)
            with open(path) as fh:
                contents = fh.read()
            if OUTPUT_NAMESPACE_PLACEHOLDER in contents:
                violations.append(os.path.relpath(path, canary))

    assert not violations, (
        f"Canary compile leaked {OUTPUT_NAMESPACE_PLACEHOLDER!r} into compiled output:\n"
        + "\n".join(violations)
    )


# (description, compiled_relative_path, expected_namespace) — each exercises team-default
# namespace inheritance for a conf type whose authoring file sets no explicit `output_namespace=`.
_TEAM_DEFAULT_FIXTURES = [
    (
        "StagingQuery (no output_namespace= -> gcp team default 'data')",
        "compiled/staging_queries/gcp/team_default_ns_example.v1__0",
        "data",
    ),
    (
        "Join (no output_namespace= -> gcp team default 'data')",
        "compiled/joins/gcp/team_default_ns_join.v1__0",
        "data",
    ),
    (
        "GroupBy (no output_namespace= -> gcp team default 'data')",
        "compiled/group_bys/gcp/team_default_ns_gb.v1__0",
        "data",
    ),
    (
        "ModelTransforms (no output_namespace= -> gcp team default 'data')",
        "compiled/model_transforms/gcp/team_default_ns_mt.v1__1",
        "data",
    ),
    (
        "Model (no output_namespace= -> gcp team default 'data')",
        "compiled/models/gcp/click_through_rate.ctr_model__1.0",
        "data",
    ),
]


@pytest.mark.parametrize("description,relpath,expected_namespace", _TEAM_DEFAULT_FIXTURES)
def test_team_default_namespace_inheritance_per_conf_type(
    canary, description, relpath, expected_namespace
):
    """For each conf type, a canary fixture that omits `output_namespace=` must compile
    with the team's default namespace stamped onto `metaData.outputNamespace`. Guards
    against regressions in `_propagate_namespace` — every supported conf type must
    participate in inheritance.
    """
    _compile_canary(canary)

    path = os.path.join(canary, relpath)
    assert os.path.exists(path), f"Expected compiled output at {relpath} for {description}"

    payload = json.loads(open(path).read())
    ns = payload.get("metaData", {}).get("outputNamespace")
    assert ns == expected_namespace, (
        f"{description}: expected outputNamespace={expected_namespace!r}, got {ns!r}"
    )


def test_team_default_placeholder_resolves_through_cross_config_table_refs(canary):
    """When a consumer config captures a producer's `.table` at Python authoring time and
    the producer had no `output_namespace=` set, the compile pipeline's placeholder pass
    must resolve the token to the producer's team-default namespace in the consumer's
    compiled output.

    Exercised by: `joins/gcp/team_default_ns_join.py` reading
    `team_default_ns_example.v1.table` (a StagingQuery with no output_namespace).
    """
    _compile_canary(canary)

    path = os.path.join(canary, "compiled/joins/gcp/team_default_ns_join.v1__0")
    payload = json.loads(open(path).read())

    left_table = payload["left"]["events"]["table"]
    assert left_table == "data.gcp_team_default_ns_example_v1__0", (
        f"Join.left.events.table should resolve the internal placeholder against the "
        f"producer's team-default namespace, got {left_table!r}"
    )


# Sentinel values embedded in the `gcp` Team in python/test/canary/teams.py
# (prod) and python/test/canary/teams.canary.py (canary). The leak-isolation
# tests below assert that each compile pass reads from exactly one teams file.
_PROD_ONLY_SENTINELS = (
    "prod-only-sentinel-value-9b8a7c",       # env.common in teams.py
    "prod-only-conf-sentinel-4f5a6b",        # conf.common in teams.py
    "prod-only-cluster-sentinel-2e3f4a",     # clusterConf in teams.py
)
_CANARY_ONLY_SENTINELS = (
    "canary-only-sentinel-value-1d2e3f",     # env.common in teams.canary.py
    "canary-only-conf-sentinel-7c8d9e",      # conf.common in teams.canary.py
    "canary-only-cluster-sentinel-5b6c7d",   # clusterConf in teams.canary.py
)


def _find_sentinels_in_tree(root_dir, needles):
    """Return list of (relpath, needle) pairs for every file under `root_dir`
    whose contents contain any needle. Walks subdirectories."""
    hits = []
    for dirpath, _, files in os.walk(root_dir):
        for f in files:
            full = os.path.join(dirpath, f)
            with open(full) as fh:
                contents = fh.read()
            for needle in needles:
                if needle in contents:
                    hits.append((os.path.relpath(full, root_dir), needle))
    return hits


def test_canary_compile_emits_canary_folder(canary):
    """A `zipline compile` invocation must produce `compiled_canary/` alongside
    `compiled/`. The folder layout mirrors `compiled/` (per-conf-type subdirs
    plus `teams_metadata/`)."""
    _compile_canary(canary)

    compiled_canary = os.path.join(canary, "compiled_canary")
    assert os.path.isdir(compiled_canary), f"Expected compiled_canary/ at {compiled_canary}"

    # At minimum the team-metadata folder should exist (every team writes one).
    assert os.path.isdir(os.path.join(compiled_canary, "teams_metadata")), (
        "compiled_canary/teams_metadata/ missing — team-metadata pass didn't run for canary"
    )


def test_gcp_join_compiles_to_both_folders_when_team_has_both_configs(canary):
    """The headline scenario: a Join authored under `joins/gcp/` whose Team is
    defined in both `teams.py` and `teams.canary.py` must land in both output
    folders with distinct `executionInfo` reflecting the right teams file."""
    _compile_canary(canary)

    relpath = "joins/gcp/training_set.v1_dev__0"
    prod_path = os.path.join(canary, "compiled", relpath)
    canary_path = os.path.join(canary, "compiled_canary", relpath)

    assert os.path.exists(prod_path), f"Expected prod output at {prod_path}"
    assert os.path.exists(canary_path), f"Expected canary output at {canary_path}"

    prod_payload = json.loads(open(prod_path).read())
    canary_payload = json.loads(open(canary_path).read())

    prod_env = prod_payload["metaData"]["executionInfo"]["env"]["common"]
    canary_env = canary_payload["metaData"]["executionInfo"]["env"]["common"]

    assert prod_env.get("PROD_ONLY_SENTINEL_GCP") == "prod-only-sentinel-value-9b8a7c", (
        f"prod executionInfo.env should reflect teams.py's gcp.env, got {prod_env}"
    )
    assert "CANARY_ONLY_SENTINEL_GCP" not in prod_env, (
        f"prod executionInfo.env leaked canary value: {prod_env}"
    )
    assert canary_env.get("CANARY_ONLY_SENTINEL_GCP") == "canary-only-sentinel-value-1d2e3f", (
        f"canary executionInfo.env should reflect teams.canary.py's gcp.env, got {canary_env}"
    )
    assert "PROD_ONLY_SENTINEL_GCP" not in canary_env, (
        f"canary executionInfo.env leaked prod value: {canary_env}"
    )


def test_canary_team_metadata_uses_canary_fields(canary):
    """The gcp `_team_metadata` file in `compiled_canary/` should reflect the
    gcp Team in `teams.canary.py` (not the one in `teams.py`)."""
    _compile_canary(canary)

    path = os.path.join(canary, "compiled_canary/teams_metadata/gcp/gcp_team_metadata")
    assert os.path.exists(path), f"Expected canary gcp team metadata at {path}"

    payload = json.loads(open(path).read())
    env_common = payload["executionInfo"]["env"]["common"]
    conf_common = payload["executionInfo"]["conf"]["common"]

    assert env_common.get("CANARY_ONLY_SENTINEL_GCP") == "canary-only-sentinel-value-1d2e3f"
    assert "PROD_ONLY_SENTINEL_GCP" not in env_common
    assert conf_common.get("spark.chronon.test.canary_only_sentinel") == "canary-only-conf-sentinel-7c8d9e"
    assert "spark.chronon.test.prod_only_sentinel" not in conf_common


def test_canary_team_without_canary_fields_inherits_default(canary):
    """A team declared in `teams.canary.py` with no env/conf of its own still
    gets a team-metadata file in `compiled_canary/`. Its `executionInfo`
    reflects what the `default` team in `teams.canary.py` provides."""
    _compile_canary(canary)

    # `test` team in teams.canary.py has no env/conf of its own. `default` in
    # teams.canary.py has env with VERSION/CUSTOMER_ID/FRONTEND_URL/HUB_URL and
    # conf with spark.chronon.partition.column.
    path = os.path.join(canary, "compiled_canary/teams_metadata/test/test_team_metadata")
    assert os.path.exists(path), f"Expected canary test team metadata at {path}"

    payload = json.loads(open(path).read())
    exec_info = payload.get("executionInfo", {})
    env_common = exec_info.get("env", {}).get("common", {}) if exec_info.get("env") else {}

    assert "PROD_ONLY_SENTINEL_GCP" not in env_common
    assert "CANARY_ONLY_SENTINEL_GCP" not in env_common
    assert env_common.get("VERSION") == "latest"


def test_no_leak_prod_fields_into_compiled_canary(canary):
    """Strict isolation: nothing under compiled_canary/ should contain any
    prod-only sentinel value from `gcp` Team in `teams.py`. Proves the canary
    compile pass loads only `teams.canary.py`."""
    _compile_canary(canary)

    compiled_canary = os.path.join(canary, "compiled_canary")
    hits = _find_sentinels_in_tree(compiled_canary, _PROD_ONLY_SENTINELS)
    assert not hits, (
        "Prod-only Team values leaked into compiled_canary/:\n"
        + "\n".join(f"  {rel} contains {needle!r}" for rel, needle in hits)
    )


def test_no_leak_canary_fields_into_compiled(canary):
    """Strict isolation: nothing under compiled/ should contain any canary-only
    sentinel value from `gcp` Team in `teams.canary.py`. Proves the prod
    compile pass loads only `teams.py`."""
    _compile_canary(canary)

    compiled = os.path.join(canary, "compiled")
    hits = _find_sentinels_in_tree(compiled, _CANARY_ONLY_SENTINELS)
    assert not hits, (
        "Canary-only Team values leaked into compiled/:\n"
        + "\n".join(f"  {rel} contains {needle!r}" for rel, needle in hits)
    )


# The tests below exercise the `from teams import …` pattern in teams.canary.py.
# A canary teams file only needs to redefine the teams that actually differ
# from prod; for teams that match prod, importing them satisfies strict
# missing-team without duplicating the whole definition. Mutating an imported
# Team's env is a clean way to layer canary-only overrides (e.g. extra env
# vars) on top of the prod baseline.
#
# Layout in python/test/canary/teams.canary.py:
#   from teams import aws_databricks, azure, quickstart
#   aws_databricks.env.common['DATABRICKS_EXTRA'] = 'DATABRICKS_EXTRA_1'


# (team, relpath under compiled_canary/) tuples covering one config per
# imported team. Confirms strict missing-team validation is satisfied for
# teams declared via `from teams import …` rather than redefined.
_IMPORTED_TEAM_CONFIGS = [
    ("aws_databricks", "compiled_canary/staging_queries/aws_databricks/exports.dim_listings__0"),
    ("azure", "compiled_canary/group_bys/azure/dim_listings.v3"),
    ("quickstart", "compiled_canary/group_bys/quickstart/dim_listings.v1__0"),
]


@pytest.mark.parametrize("team,relpath", _IMPORTED_TEAM_CONFIGS)
def test_imported_team_configs_compile_under_canary(canary, team, relpath):
    """A team that's brought into `teams.canary.py` purely via
    `from teams import <team>` (no redefinition) must satisfy the strict
    missing-team check, and its configs must land in compiled_canary/. This
    is the headline ergonomic — `teams.canary.py` only redeclares the teams
    that diverge from prod."""
    _compile_canary(canary)

    path = os.path.join(canary, relpath)
    assert os.path.exists(path), (
        f"Expected canary output at {relpath} — team {team!r} was imported "
        f"from teams.py but its configs didn't compile under canary."
    )


def test_canary_mutation_to_imported_team_lands_in_canary_only(canary):
    """When teams.canary.py mutates an imported Team (e.g.
    `aws_databricks.env.common['DATABRICKS_EXTRA'] = …`), the mutation must
    appear in compiled_canary/ but NOT in compiled/. The prod pass re-loads
    teams.py from scratch via import_module_from_file's exec_module call, so
    each pass owns an isolated copy of the prod Team objects."""
    _compile_canary(canary)

    canary_meta = json.loads(
        open(os.path.join(
            canary, "compiled_canary/teams_metadata/aws_databricks/aws_databricks_team_metadata"
        )).read()
    )
    prod_meta = json.loads(
        open(os.path.join(
            canary, "compiled/teams_metadata/aws_databricks/aws_databricks_team_metadata"
        )).read()
    )

    canary_env_common = canary_meta["executionInfo"]["env"]["common"]
    prod_env_common = prod_meta["executionInfo"]["env"]["common"]

    assert canary_env_common.get("DATABRICKS_EXTRA") == "DATABRICKS_EXTRA_1", (
        f"Expected canary aws_databricks env.common['DATABRICKS_EXTRA'] == "
        f"'DATABRICKS_EXTRA_1', got {canary_env_common.get('DATABRICKS_EXTRA')!r}. "
        "The teams.canary.py mutation didn't take effect on the canary compile."
    )
    assert "DATABRICKS_EXTRA" not in prod_env_common, (
        f"DATABRICKS_EXTRA leaked into prod's aws_databricks team_metadata: "
        f"{prod_env_common.get('DATABRICKS_EXTRA')!r}. The canary mutation was "
        "supposed to be isolated by import_module_from_file re-execing teams.py."
    )


def test_canary_mutation_propagates_to_owning_team_configs(canary):
    """The canary mutation on `aws_databricks.env.common` must propagate into
    every config owned by aws_databricks, not just the team-metadata file.
    Catches a regression where Team-level env merge silently skips
    user-mutated entries."""
    _compile_canary(canary)

    canary_sq = json.loads(open(os.path.join(
        canary, "compiled_canary/staging_queries/aws_databricks/exports.dim_listings__0"
    )).read())
    prod_sq = json.loads(open(os.path.join(
        canary, "compiled/staging_queries/aws_databricks/exports.dim_listings__0"
    )).read())

    canary_env_common = canary_sq["metaData"]["executionInfo"]["env"]["common"]
    prod_env_common = prod_sq["metaData"]["executionInfo"]["env"]["common"]

    assert canary_env_common.get("DATABRICKS_EXTRA") == "DATABRICKS_EXTRA_1"
    assert "DATABRICKS_EXTRA" not in prod_env_common
