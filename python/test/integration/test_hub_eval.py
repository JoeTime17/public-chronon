"""Cloud-agnostic hub eval integration tests.

Exercises: compile -> upload diffs -> eval (schema validation and test-data evaluation).
"""

import os

import pytest
from click.testing import CliRunner

from .helpers.cli import compile_configs, submit_eval

DEMO_JOIN = {
    "gcp": "compiled/joins/gcp/demo.v1__1",
    "aws": "compiled/joins/aws/demo.v1__1",
    "azure": "compiled/group_bys/azure/dim_listings.v3",
}


@pytest.mark.integration
def test_eval(confs, chronon_root, hub_url, eval_url, cloud):
    """Eval validates a conf against source tables and schemas."""
    runner = CliRunner()
    compile_configs(runner, chronon_root)

    submit_eval(runner, chronon_root, hub_url, eval_url, confs(DEMO_JOIN[cloud]))


@pytest.mark.integration
def test_eval_with_test_data(confs, chronon_root, hub_url, eval_url, cloud):
    """Eval with a test data YAML uses fixture rows instead of real source tables."""
    if cloud != "gcp":
        pytest.skip(f"test data eval is only supported for gcp, skipping for cloud: {cloud}")
    runner = CliRunner()
    compile_configs(runner, chronon_root)

    test_data_path = os.path.join(chronon_root, "test_data", "exports.yaml")
    submit_eval(
        runner, chronon_root, hub_url, eval_url,
        confs(DEMO_JOIN[cloud]),
        test_data_path=test_data_path,
    )
