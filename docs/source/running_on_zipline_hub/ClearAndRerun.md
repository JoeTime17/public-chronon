---
title: "Clear & Rerun"
order: 7
---

# Clear & Rerun

`zipline hub clear-downstream` walks from a conf to every downstream conf that consumes its output and marks the affected partitions as needing recompute, then prints the exact backfill / adhoc commands to rerun them. Use it after an upstream fix that invalidates already-computed data, when you want every affected downstream conf rebuilt without manually tracing the graph.

> **Clearing is a signal, not a job.** The command marks the affected partitions cleared — the actual recompute happens when you run the printed follow-up commands (or wait for the next scheduled run, for batch confs on a schedule).

## How it works

Two-step preview → apply:

1. **Preview** — walks from the source conf through every downstream conf that consumes its output and lists the partition ranges that would be cleared. In a typical `StagingQuery → GroupBy → Join` chain, clearing the StagingQuery surfaces all three confs in the preview, each with its own affected range.
2. **Apply** — writes one cleared step for every affected step across the confs in the preview. You only specify the source range; downstream partition ranges are computed automatically per conf, respecting each conf's configured offsets (e.g. a GroupBy with a 7-day window pulls in 6 extra days forward).

The CLI runs the preview, prompts before calling apply, and prints the exact `backfill` / `run-adhoc` follow-ups so you can recompute without manually tracing the graph. A UI surface for clear & rerun is on the roadmap; until it lands, the CLI is the only entry point.

## CLI

```bash
zipline hub clear-downstream compiled/staging_queries/team/user_purchases_sq \
  --start-ds 2026-05-01 --end-ds 2026-05-15
```

Each downstream conf gets the range its offsets imply — not just a copy of the source range:

```
Conf:           team.user_purchases_sq
Range:          2026-05-01 to 2026-05-15
Affected confs: 4

  team.user_purchases_sq (batch)        2026-05-01 to 2026-05-15
  team.user_purchase_features (batch)   2026-05-01 to 2026-05-21
  team.user_features_join (batch)       2026-05-01 to 2026-05-21
  team.user_purchase_features (online)

Proceed with clearing these confs? [y/N]: y
✓ Cleared 4 confs

To recompute batch data, run backfill for each affected conf:
  zipline hub backfill team.user_purchases_sq --start-ds 2026-05-01 --end-ds 2026-05-15
  zipline hub backfill team.user_purchase_features --start-ds 2026-05-01 --end-ds 2026-05-21
  zipline hub backfill team.user_features_join --start-ds 2026-05-01 --end-ds 2026-05-21

To fix online data, run adhoc upload for each deploy conf:
  zipline hub run-adhoc team.user_purchase_features
```

Pass `--yes` to skip the confirmation in scripts; `--format json` swaps the output for machine-parseable JSON. The git branch you invoke from selects which `branch_conf` the hub resolves the conf against — clearing on a feature branch only marks steps in that branch's view of the DAG.

## Deprecated: `forceRecompute`

`NodeExecutionRequest.forceRecompute` is **deprecated**. It is no longer consumed anywhere in the orchestrator — `clear-downstream` replaces it with an explicit preview/apply flow that surfaces the downstream impact before any rows change. The field is retained on the Thrift type only for wire compatibility with older clients; new code should ignore it.
