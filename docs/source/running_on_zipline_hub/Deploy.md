---
title: "Deploying Chronon Entities"
order: 3
---

# Deploying

Once you have authored and tested your `GroupBy`, `Join` or `StagingQuery`, you have several options for deploying them to production.

## Deployment Modes

### [run-adhoc](/docs/reference/cli#zipline-hub-run-adhoc)

Deploys the online components of a single entity in a one-off manner. Useful for testing feature fetching without committing to a recurring schedule. **Note:** streaming jobs launched via `run-adhoc` will not be restarted automatically on failure.

```bash
zipline hub run-adhoc --end-ds 2026-04-05 compiled/group_bys/aws/user_activities.v1__1
```

After a workflow run is created, you can monitor it in the Zipline Hub UI or poll the [Workflow Status API](/docs/running_on_zipline_hub/Workflow_Status_API).

### [schedule](/docs/reference/cli#zipline-hub-schedule)

Schedules a single `GroupBy` or `Join` for recurring execution. This ensures the relevant online and offline jobs run daily, and keeps any streaming job alive by restarting it on failure.

```bash
zipline hub schedule compiled/group_bys/aws/user_activities.v1__1
```

### [schedule-all](/docs/reference/cli#zipline-hub-schedule-all)

Schedules all configs based on their versions in the `main`/`master` branch. This is intended to be triggered as part of your CI pipeline so that merging changes automatically keeps scheduled jobs in sync.

```bash
zipline hub schedule-all --cloud aws
```

#### Targeting an environment with `--env`

`schedule-all` takes an `--env` flag that selects which compile output to read and which configs to deploy:

```bash
zipline hub schedule-all --cloud aws --env prod    # default — reads compiled/
zipline hub schedule-all --cloud aws --env canary  # reads compiled_canary/
```

`--env` accepts `prod` or `canary` today. The `canary` case requires a `teams.canary.py` in the repo root and per-entity opt-in via `environments=['prod', 'canary']` on the `GroupBy`/`Join`/`StagingQuery`. Entities default to prod-only — they're never scheduled under `--env canary` unless explicitly opted in.

See **[Multi-Environment Compile & Deploy](MultiEnvironment.md)** for the full guide: authoring `teams.canary.py`, the `environments=` behavior matrix, and the CI workflow.

## Scheduled Jobs by Entity Type

### GroupBy

`GroupBy`s will get the following jobs:

1. (if `online=True`) Batch upload — updates the online KV store with feature values for serving
2. (if `online=True` and a streaming topic is configured) Streaming updates to the online KV store for serving
3. (if `offline_schedule` is set) Batch snapshots that write partitions into the output table

### Join

`Join`s will get the following jobs:

1. (if `online=True`) A metadata upload job that informs the fetcher of which features are part of this join (beyond that, the actual feature computation is scheduled by the `GroupBy`s used within the `Join`; these can also be tracked on the `Join`s page in the Zipline Hub UI)
2. (if `offline_schedule` is set) Batch jobs that frontfill data into the most recent partitions of the output table

### StagingQuery

`StagingQuery`s will get a batch job that writes data into the most recent partition of the output table.

## Disabling Schedules

To prevent a particular schedule from being created, set the relevant schedule field to `"@never"`. This works for both `online_schedule` and `offline_schedule`:

```python
GroupBy(
    ...
    online=True,
    online_schedule="@never",   # disables the online batch upload schedule
    offline_schedule="@never",  # disables the offline snapshot schedule
)
```

This is useful when you want fine-grained control — for example, disabling the `GroupBy`'s own batch schedule while still allowing a downstream `Join` to drive execution.
