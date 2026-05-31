## Commands

***All commands assume you are in the root directory of this project***.
For me, that looks like `~/workspace/chronon`.

## Prerequisites
1. Install Thrift (e.g. via `brew install thrift` on macOS. Version 0.22 is recommended)
2. Install Java. Ideally version 11, e.g. via `brew install openjdk@11` on macOS - newer versions of Java can run into issues due to stricter Java platform module system checks particularly with engines like Spark / Flink)
3. Install Scala 2.12
4. Install Python 3.11 or higher

## Using a Plugin Manager
You can use a plugin manager like [asdf](https://asdf-vm.com/guide/getting-started.html#_2-download-asdf)
* Install [asdf](https://asdf-vm.com/guide/getting-started.html#_2-download-asdf)
* ```asdf plugin add asdf-plugin-manager```
* ```asdf install asdf-plugin-manager latest```
* ```asdf exec asdf-plugin-manager add-all``` (see `.plugin-versions` for required plugins)
* ```asdf exec asdf-plugin-manager update-all```
* ```asdf install``` (see `.tool-versions` for required runtimes and versions)

## Build system
We use [mill](https://mill-build.org/mill/index.html) for building Scala and Python code.

### Why mill?
mill has a couple of nice side effects:
1. Builds are very fast - they are cached and executed in parallel
2. Good Python support - comes with
   a. ruff linting
   b. pytest support
   c. wheel
   d. pex bundling
   e. pypi uploads for free  (literally 20 lines of code to get all our py workflow into mill)
3. Easy to reason about dependencies and modules
   a. mill is a significant reduction in code size and indirection
   b. the ide click-into and auto complete works even with build files
4. Much smaller build output sizes (e.g output size for spark dropped from 480MB to 62MB)
   a. simply because we can mark spark deps as “provided” that only are available for compilation but not for assembly or runtime.

### Useful scala commands
```
# reformat scala code
./mill __.reformat

# compile scala code
./mill __.compile

# build assembly jar
./mill __.assembly

# run specific tests that match a pattern etc
./mill spark.test.testOnly "ai.chronon.spark.analyzer.*"

# run a particular test case inside a test class
./mill spark.test.testOnly "ai.chronon.spark.kv_store.KVUploadNodeRunnerTest" -- -z "should handle GROUP_BY_UPLOAD_TO_KV successfully"

# run all tests of a sub-module
./mill api.test

# to find where a dependency comes from
./mill spark.showMvnDepsTree --whatDependsOn com.google.protobuf:protobuf-java

# show actions available for a given module
./mill resolve spark.__
```

### useful python commands
```
# reformat python code
./mill python.ruffCheck --fix

# run python tests
./mill python.test

# build the wheel - the user needs to have python installed
./mill python.wheel

# build an editable package (for ide and development)
./mill python.installEditable

# build and install current wheel
./mill python.installWheel

# build and run the entry point (zipline.py)
./mill python.run hub backfill ...

# run coverage
./mill python.test.coverageReport --omit='**/test/**,**/out/**'

# publish to pypi
# ask nikhil to generate a token for you
export MILL_TWINE_REPOSITORY_URL=https://pypi.org/
export MILL_TWINE_USERNAME=__token__
export MILL_TWINE_PASSWORD=<apitoken> 
./mill python.publish

# build self contained python binary - include python and all deps (transitive included) into a single file
# uses pex under the hood
./mill python.bundle
```

### project setup

Mill build spans across a central build.mill and a per module package.mill file.

Here is a simple but real example

```scala
object `package` extends build.BaseModule {
  def moduleDeps = Seq(build.api)

  // equivalent to "provided" , meaning the package is only available for compile
  // but at runtime it needs to come through from class path.
  def compileMvnDeps = Seq(
    mvn"org.apache.spark::spark-sql:${build.Constants.sparkVersion}"
  )
  
  // the actual dependencies
  def mvnDeps = build.Constants.commonDeps ++ build.Constants.loggingDeps ++ build.Constants.utilityDeps ++ Seq(
    mvn"org.apache.datasketches:datasketches-memory:3.0.2",
    mvn"org.apache.datasketches:datasketches-java:6.1.1",
  )
  
  // test setup
  object test extends build.BaseTestModule {
    def moduleDeps = Seq(build.aggregator, build.api.test)
    def mvnDeps = super.mvnDeps() ++ build.Constants.testDeps
  }
}
```

## Configuring IntelliJ IDEA
* Install the Build Server Protocol (BSP) plugin
* From the menu, select File -> New -> Project from Existing Sources...
* Select the root directory of the project
* Select "Import project from external model" and choose "BSP"

## Integration testing

End-to-end tests that drive a real Hub / Eval / Orchestrator deployment from a laptop.
They live under `python/test/integration/` and are **excluded from the default `pytest` run** (see `norecursedirs` in `python/pyproject.toml`); use the dedicated Mill target `python.test_integration`.

### Layout

```
python/test/integration/
├── conftest.py                 # session/per-test fixtures, CLI options
├── helpers/
│   ├── cli.py                  # CliRunner wrappers around `zipline` commands
│   ├── hub_api.py              # direct HTTP calls to Hub endpoints
│   ├── workflow.py             # poll_workflow / poll_workflow_until
│   ├── cleanup.py              # GCP / AWS / Azure / Dataproc cleanup classes
│   ├── templates.py            # test_id-scoped conf copy + import rewriting
│   └── _run_zipline.py         # subprocess wrapper around `zipline run`
├── test_hub_backfill.py        # compile → backfill → poll
├── test_hub_eval.py            # compile → eval (schema + test-data)
├── test_hub_run_adhoc.py       # streaming deploy + cancel
├── test_hub_schedule.py        # schedule deploy / list / delete
└── test_run_quickstart.py      # full `zipline run` pipeline (no Hub)
```

Canary configs referenced by the tests live at `python/test/canary/{staging_queries,group_bys,joins,models}/<cloud>/*.py`.

### Prerequisites

1. **Cloud SDKs** — `gcloud`, `aws`, or `az` configured for the canary project (default GCP project `canary-443022`, dataset `data`).
2. **A reachable Hub + Eval** — canary deployment, or a local stack on `localhost:3903` / `localhost:3904`.
3. **Auth** — pick one:
   - `ZIPLINE_TOKEN` + `ZIPLINE_AUTH_URL` — service-principal session token (exchanged for a JWT via `ai.chronon.repo.token_exchange.resolve_token`)
   - `ZIPLINE_TOKEN=$(zipline auth get-access-token)` — short-lived JWT, no `ZIPLINE_AUTH_URL` needed
   - `GCP_ID_TOKEN=$(gcloud auth print-identity-token)` — IAM identity token (or `AWS_ID_TOKEN` for AWS)

### Running locally

```bash
# Full integration suite against canary (GCP, service principal):
ZIPLINE_TOKEN=<session_token> \
  ZIPLINE_AUTH_URL=https://canary.zipline.ai \
  HUB_URL=https://canary-orch.zipline.ai \
  EVAL_URL=https://canary-eval.zipline.ai \
  ./mill python.test_integration

# Filter by name:
PYTEST_ADDOPTS="-k test_backfill_no_data" \
  ZIPLINE_TOKEN=$(zipline auth get-access-token) \
  HUB_URL=https://canary-orch.zipline.ai \
  ./mill python.test_integration

# Quickstart test — exercises `zipline run` against Dataproc; needs VERSION:
VERSION=<image_tag> \
  ZIPLINE_TOKEN=<token> \
  HUB_URL=https://canary-orch.zipline.ai \
  PYTEST_ADDOPTS="-k test_run_quickstart" \
  ./mill python.test_integration

# AWS:
CLOUD=aws \
  AWS_ID_TOKEN=<token> \
  HUB_URL=https://canary-aws-orch.zipline.ai \
  EVAL_URL=https://canary-aws-eval.zipline.ai \
  ./mill python.test_integration
```

#### Environment variables

| Var | Default | Notes |
|---|---|---|
| `HUB_URL` | `http://localhost:3903` | Orchestrator base URL |
| `EVAL_URL` | `http://localhost:3904` | Eval server base URL |
| `CLOUD` | `gcp` | One of `gcp`, `aws`, `azure` |
| `VERSION` | _(unset)_ | Required by `test_run_quickstart` (image tag for `zipline run`) |
| `ZIPLINE_TOKEN` | _(unset)_ | Service-principal token or short JWT |
| `ZIPLINE_AUTH_URL` | _(unset)_ | Required when `ZIPLINE_TOKEN` is a session token |
| `GCP_ID_TOKEN` / `AWS_ID_TOKEN` | _(unset)_ | Cloud IAM token; alternative to `ZIPLINE_TOKEN` |
| `GCP_BQ_PROJECT` | `canary-443022` | BigQuery cleanup target |
| `GCP_BQ_DATASET` | `data` | BigQuery cleanup target |
| `GCP_PROJECT_ID` | `canary-443022` | Dataproc Flink cleanup |
| `GCP_REGION` | `us-central1` | Dataproc Flink cleanup |
| `AWS_REGION` | `us-west-2` | Glue client region |
| `AWS_GLUE_DATABASE` | `default` | Glue cleanup target |
| `AZURE_CATALOG` | `default` | Cleanup stub (not yet implemented) |

The Mill target sets `PYTHONPATH=<generated_thrift>:<src>:<test>` and runs pytest with `-s -v --log-cli-level=INFO` after clearing `addopts`. Use `PYTEST_ADDOPTS` to pass filters; arguments after `--` on the Mill command line are not currently forwarded to pytest.

### Adding a new test

```python
# python/test/integration/test_my_feature.py
import pytest
from click.testing import CliRunner

from .helpers.cli import compile_configs, submit_backfill
from .helpers.workflow import poll_workflow

DEMO_JOIN = {
    "gcp":   "compiled/joins/gcp/demo.v1__1",
    "aws":   "compiled/joins/aws/demo.v1__1",
    "azure": "compiled/joins/azure/demo.v2",
}


@pytest.mark.integration
def test_my_feature(confs, chronon_root, hub_url, cloud):
    runner = CliRunner()
    compile_configs(runner, chronon_root)

    workflow_id = submit_backfill(
        runner, chronon_root, hub_url,
        confs(DEMO_JOIN[cloud]),
        "2026-03-01", "2026-03-03",
    )
    poll_workflow(hub_url, workflow_id, timeout=1800, interval=45)
```

#### Fixtures (from `conftest.py`)

| Fixture | Scope | Provides |
|---|---|---|
| `hub_url`, `eval_url`, `cloud` | session | resolved from env / `--hub-url` etc. |
| `version` | session | `VERSION` env var; only inject if your test uses `zipline run` |
| `chronon_root` | session | absolute path to `python/test/canary` |
| `chronon_env` | autouse | sets `PYTHONPATH`, `CUSTOMER_ID=canary`, prepends sys.path |
| `test_id` | function | random 8-char suffix; copies + rewrites canary configs and tears them down |
| `confs` | function | callable: `confs("compiled/joins/gcp/demo.v1__1")` → test-scoped path |

`@pytest.mark.integration` is required so the suite stays filterable and CI gating on the marker keeps working as it gets wired up.

#### Adding a new conf

1. Add the source `.py` config under `python/test/canary/<category>/<cloud>/<name>.py` (category ∈ `staging_queries`, `group_bys`, `joins`, `models`).
2. Reference it via `confs(...)` using the **compiled** path (`compiled/<category>/<cloud>/<name>.<member>__<index>`). Compilation runs when you call `compile_configs(...)`.
3. The `test_id` fixture's templating layer copies every `.py` under the cloud's config directories into `{name}_{test_id}.py` and rewrites intra-config imports — sibling imports work as normal, no special handling needed.

#### Helper cheat sheet

| Helper | Use case |
|---|---|
| `cli.compile_configs(runner, chronon_root)` | clean + recompile canary configs |
| `cli.submit_backfill(...)` → workflow_id | `zipline hub backfill` |
| `cli.submit_run_adhoc(...)` → workflow_id | streaming deploy |
| `cli.cancel_workflow(...)` | `zipline hub cancel` |
| `cli.submit_eval(...)` | schema or test-data eval |
| `cli.submit_schedule(...)` | recurring schedule |
| `cli.submit_run(...)` | direct `zipline run` (Click `CliRunner`) |
| `cli.submit_run_subprocess(...)` | thread-safe `zipline run`; required when fanning jobs out via `ThreadPoolExecutor` |
| `cli.submit_check_partitions / submit_upload / submit_upload_to_kv / submit_metadata_upload / submit_fetch` | metastore / KV / fetch flows (Dataproc-only) |
| `workflow.poll_workflow(hub_url, id)` | wait for `SUCCEEDED` (raises on `FAILED`) |
| `workflow.poll_workflow_until(hub_url, id, target_statuses={"FAILED"})` | wait for any explicit terminal state |
| `hub_api.list_schedules / delete_schedule / find_schedules_by_test_id` | direct schedule REST calls |
| `hub_api.get_flink_job_ids(hub_url, workflow_id)` | resolve Dataproc job IDs to cancel |
| `cleanup.GCPCleanup / AWSCleanup / DataprocFlinkCleanup` | only if your test creates resources outside the standard `test_id` path |

#### Cloud-specific tests

Use `pytest.skip(...)` for clouds you don't support (see `test_eval_with_test_data` and `test_run_adhoc`). Keep conf path mappings as `dict[cloud, str]` constants at the top of the file.

#### Cleanup

- BigQuery / Glue tables whose names contain the `test_id` are deleted automatically by the `test_id` fixture's teardown.
- Dataproc Flink jobs (streaming) are **not** auto-tracked; if your test starts one, request the `flink_cleanup` fixture (see `test_hub_run_adhoc.py`) and stash the workflow id.
- Hub schedules are not auto-deleted by `test_id` cleanup. If your test creates a schedule, delete it explicitly via `hub_api.delete_schedule`.

### Debugging

- **Live logs**: pytest runs with `-s -v --log-cli-level=INFO`; `print(...)` and `logger.info(...)` land on stdout immediately. `poll_workflow` already prints status transitions.
- **Inspect a failed workflow**: copy the `workflow_id` from output and hit `GET {HUB_URL}/workflow/v2/{id}` with the same auth header.
- **Auth issues**: `_get_auth_headers` in `helpers/workflow.py` / `helpers/hub_api.py` resolves `ZIPLINE_TOKEN` → `GCP_ID_TOKEN` → `AWS_ID_TOKEN`; first one set wins.
- **Templates not rewriting**: `templates._discover_sources` only looks at `staging_queries`, `group_bys`, `joins`, `models`. INFO-level logs show every copy + rewrite.
- **`zipline run` hangs**: prefer `submit_run_subprocess` over `submit_run` when running multiple jobs concurrently — Click's `CliRunner` is not thread-safe and the GCP SDK has daemon threads that block normal interpreter shutdown. The subprocess wrapper force-exits with `os._exit()`.
- **Leaked tables / schedules from a crashed run**: the `test_id` is logged at the start of every test (`test_id=<id> for <test_name>`). Pass that id to `GCPCleanup.cleanup_tables` / `delete_schedule` directly to mop up.

### CI

Integration tests are **not** wired into PR checks today; they run manually against canary. If you add a test that depends on canary state (new tables, new images), coordinate with whoever maintains the canary deployment before merging.

## Pushing code

Our CI pipeline runs scalafmt checks on scala code before allowing merges.
You can either add a git pre-commit hook to run scalafmt before every commit, or use the following alias and function to format, commit, and push in one command.

```sh
alias mill_scalafmt='./mill __.reformat'

function zpush() {
    if [ $# -eq 0 ]; then
        echo "Error: Please provide a commit message."
        return 1
    fi

    local commit_message="$1"

    mill_scalafmt && \
    git add -u && \
    git commit -m "$commit_message" && \
    git push

    if [ $? -eq 0 ]; then
        echo "Successfully compiled, formatted, committed, and pushed changes."
    else
        echo "An error occurred during the process."
    fi
}
```

You can invoke this command as below

```
zpush "Your commit message"
```

> Note: The quotes are necessary for multi-word commit message.