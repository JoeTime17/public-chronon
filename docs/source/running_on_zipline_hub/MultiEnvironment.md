---
title: "Multi-Environment Compile & Deploy"
order: 6
---

# Multi-Environment Compile & Deploy

Chronon supports compiling and deploying the same set of configs against more than one deployment environment from a single repo. Today this is limited to two envs: a default **prod** env and an optional **canary** env. The canary env is typically a parallel deployment with smaller cluster sizes, a non-production GCP project / S3 bucket, or different upstream data sources — useful for shadow-running a config before promoting it to prod.

This page is the single source of truth for the multi-env feature. The [Project Setup](Setup.md) and [Deploying Chronon Entities](Deploy.md) pages reference back here for details.

> **Only `canary` is supported as a non-prod env today.** The hub's wire contract — the Thrift `Environment` enum on `metaData.environments` — currently models `PROD` and `CANARY` only. Dropping in e.g. `teams.staging.py` will fail discovery with a clear error. Supporting a new env end-to-end requires extending `thrift/api.thrift` and the hub server in lockstep.

## How it works

`zipline compile` discovers env-specific teams files in the repo root:

```
<repo-root>/
├── teams.py           # → compiles to compiled/         (prod)
├── teams.canary.py    # → compiles to compiled_canary/  (canary, optional)
├── group_bys/
├── joins/
└── staging_queries/
```

Each pass writes to its own output folder and uses *only* its own teams file — there is no implicit fallback from the canary pass to `teams.py`. Every team referenced by a config in the canary pass must be declared (or imported, see below) in `teams.canary.py`, or compile fails loudly.

After every run, `zipline compile` prints a per-env summary so you can see what landed on disk:

```
─────────── 📋 COMPILATION SUMMARY ───────────
  canary  ✓ OK      → compiled_canary  (165 written)
  prod    ✓ OK      → compiled         (212 written)
```

If a pass fails (e.g. a Python syntax error in a config), its staging dir is discarded and `compiled_<env>/` is **not** updated — the summary will report `(N error(s); output not written (M parsed and discarded))`. Any env's failure causes `zipline compile` to exit with code `1`, so a broken canary pass surfaces in CI even when the prod pass succeeded.

## Authoring `teams.canary.py`

`teams.canary.py` is a regular Python module. The recommended pattern is to import teams from `teams.py` for the ones that are identical to prod, layer canary-only mutations on top, and only redeclare teams that diverge structurally:

```python
# teams.canary.py
from ai.chronon.repo.spark_catalog_confs import *
from ai.chronon.types import ConfigProperties, EnvironmentVariables, Team

# Reuse identical-to-prod teams via plain Python import.
from teams import aws_databricks, azure, quickstart

# Layer a canary-only override on top of an imported team. This mutation only
# affects the canary compile pass — the prod pass re-loads teams.py from
# scratch and never sees this change.
aws_databricks.env.common['ARTIFACT_PREFIX'] = "s3://my-org-artifacts-canary"

# Teams that differ structurally from prod — redeclare in full.
default = Team(
    outputNamespace="default",
    env=EnvironmentVariables(common={"VERSION": "latest", "CUSTOMER_ID": "canary"}),
)

gcp = Team(
    outputNamespace="data",
    env=EnvironmentVariables(
        common={
            "CLOUD_PROVIDER": "gcp",
            "CUSTOMER_ID": "canary",
            "GCP_PROJECT_ID": "my-org-canary",
            "ARTIFACT_PREFIX": "gs://my-org-artifacts-canary",
            "WAREHOUSE_PREFIX": "gs://my-org-warehouse-canary",
        },
    ),
    conf=ConfigProperties(
        common={
            **BigQueryConfiguration({
                "spark.sql.catalog.spark_catalog.warehouse": "gs://my-org-warehouse-canary/data/tables/",
                "spark.sql.catalog.spark_catalog.gcp.bigquery.location": "us-central1",
                "spark.sql.catalog.spark_catalog.gcp.bigquery.project-id": "my-org-canary",
            }),
            "spark.chronon.partition.format": "yyyy-MM-dd",
            "spark.chronon.partition.column": "ds",
            "spark.chronon.table_write.format": "iceberg",
        },
    ),
)
```

The import-and-mutate pattern works because the prod pass re-loads `teams.py` from a fresh Python module each compile invocation — mutations applied during the canary pass don't survive into the prod pass. Same field structure as the prod-side `Team` API; see [Project Setup](Setup.md) for the full field reference.

## Per-entity opt-in via `environments=[...]`

Authoring a `teams.canary.py` makes the canary *compile* possible, but each `GroupBy`/`Join`/`StagingQuery` still has to opt in to canary *deploys* explicitly:

```python
GroupBy(
    ...
    environments=['prod', 'canary'],  # scheduled by both prod and canary deploys
)
```

Entities without an explicit `environments=` default to `['prod']` only — so a config running on production data never accidentally rolls out to canary until you opt in.

## Deploying with `--env`

The hub schedule and deploy commands take an `--env` flag that selects which compile output to read and which configs to deploy:

```bash
zipline hub schedule-all --cloud aws --env prod     # default — reads compiled/
zipline hub schedule-all --cloud aws --env canary   # reads compiled_canary/
```

`schedule-all` reads each conf's `metaData.environments` field and skips entities whose list doesn't contain the target env. Behavior matrix:

| `environments=` value     | `--env prod` (default) | `--env canary` |
|---------------------------|------------------------|----------------|
| `['prod']`                | ✓ scheduled            | ✗ skipped      |
| `['canary']`              | ✗ skipped              | ✓ scheduled    |
| `['prod', 'canary']`      | ✓ scheduled            | ✓ scheduled    |
| omitted (defaults to prod)| ✓ scheduled            | ✗ skipped      |

The default (prod-only) is intentional: a config running on production data never rolls out to canary clusters until you explicitly add `'canary'` to its `environments` list.

## CI workflow

A typical CI pipeline runs both compile passes and both deploys:

```bash
# In CI, on merge to main:
zipline compile                                     # produces compiled/ + compiled_canary/
zipline hub schedule-all --cloud aws --env prod     # deploys prod-tagged confs
zipline hub schedule-all --cloud aws --env canary   # deploys canary-tagged confs
```

`zipline compile` exits non-zero if either env's pass had errors, so a broken canary won't slip through CI even when the prod pass is clean.
