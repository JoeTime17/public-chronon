# Sub-Daily Partitions

Chronon outputs can be partitioned finer than a day: hourly, 3-hourly, 15-minutely — any
interval that divides 24 hours evenly, with an optional boundary offset. Daily stays the
default, and existing daily confs are unaffected (byte-identical compiled output, identical
job behavior).

## The model

| Concept | Meaning |
|---|---|
| Partition ds | the formatted UTC instant of the interval **start**, e.g. `2026-06-03-04-00` |
| Grid | interval + offset: a `3h` interval at `1h` offset has boundaries 01:00, 04:00, ..., 22:00 UTC |
| Data held | each ds holds the half-open interval `[start, start + interval)` |
| Schedule | a cron that says **when to fire** — it never defines the grid |
| Delay | the cron fire phase relative to the grid: a fire at 04:15 over a 3h@01:00 grid processes the partition that closed at 04:00 |

The grid is a property of the **data**; the schedule is a property of **execution**. Keeping
them separate is what lets a job fire late without changing which partition it produces.

Two rules govern every grid:

- the interval must divide 24h evenly (`1m` … `90m` … `12h`) or be exactly `1d`. Week- or
  month-sized *partitions* are not supported — weekly and monthly cadences are *schedules*
  over daily partitions (see below).
- offsets only exist below a day (`0 <= offset < interval`); daily partitions always have
  their boundaries at midnight.

## Declaring a sub-daily output

```python
my_group_by = GroupBy(
    sources=[EventSource(
        table="data.checkouts",
        query=Query(time_column="ts", partition_interval="3h", partition_offset="1h"),
    )],
    keys=["user_id"],
    aggregations=[Aggregation(input_column="amount", operation=Operation.SUM, windows=["7d"])],
    online=True,
    partition_interval="3h",     # output grid interval
    partition_offset="1h",       # output grid offset: boundaries at 01:00, 04:00, ..., 22:00 UTC
    offline_schedule="15 1-22/3 * * *",  # fires at 01:15, 04:15, ... = 15m delay over the grid
)
```

Each 04:15 fire computes the partition `...-01-00` (holding `[01:00, 04:00)`),
uploads it, and online serving merges streaming events on top of the new batch end.

You usually don't need `partition_interval` at all — it is **inferred from the schedule**:

- a cron firing more than once a day implies that interval (`0 */2 * * *` → 2h grid),
- a cron firing once a day or less implies daily — the 24h ceiling. A weekly report
  (`0 6 * * MON`) or monthly job (`0 6 1 * *`) runs over **daily partitions**.

**Each fire materializes exactly one partition** — the latest one closed on the grid at fire
time. A cron coarser than the grid therefore produces a **sparse output by design**: a weekly
report writes one daily partition per week; a 3h cron over a 90m grid writes every
other 90m partition. Downstream readers handle gappy inputs; online freshness follows the
cron, not the grid.

Declare `partition_interval` explicitly when the grid can't be inferred: backfill-only confs
(no schedule), or grids no single cron can express (the 90m case above). When both are
present, the cron interval must be a multiple of the declared one.

**The offset is never inferred from the cron.** Fire phase is treated purely as processing
delay; if your data's boundaries are at 01:00, say so with `partition_offset="1h"`.

Sub-daily crons must be **regular**: evenly spaced across the whole UTC day, with `*` in the
day-of-month, month, and weekday fields. `0 */2 * * MON` (every 2h, Mondays only) is rejected
— a 2h grid inferred from it would have six days of partitions that never get computed.

## Mixed-interval joins

Join parts move independently — a single join can serve realtime, 3-hourly, and daily
features side by side:

```python
my_join = Join(
    left=EventSource(table="data.checkouts",
                     query=Query(time_column="ts", partition_interval="3h", partition_offset="1h")),
    right_parts=[
        JoinPart(group_by=realtime_gb),     # TEMPORAL: fresh per event
        JoinPart(group_by=three_hour_gb),   # snapshot on its own 3h grid
        JoinPart(group_by=daily_dims_gb),   # snapshot refreshed daily
    ],
    partition_interval="3h",
    partition_offset="1h",
)
```

The two kinds of upstreams follow different rules:

- **Right parts: any cadence is fine.** Each row picks the latest snapshot of each part at
  or before the row's timestamp, on the *part's own grid*. A daily part under a 3h join
  means those features refresh daily — bounded staleness, never missing data. Nothing to
  declare, nothing validated.
- **The left and groupBy/model sources: every output boundary must also be a source
  boundary.** The output partition `[13:00, 16:00)` needs input data through 16:00 — a
  daily source can't provide that until the day closes, which would silently make the whole
  pipeline a day stale. So a sub-daily conf over a coarser (or undeclared, hence implicitly
  daily) source is a **compile error**.

## Sources that don't write sub-daily partitions

- **Same-or-finer declared interval**: declare `partition_interval`/`partition_offset` on the
  source `Query` — readiness and scans resolve on that grid.
- **`time_partitioned=True`**: the partition column is a real timestamp/date column and data
  lands continuously. It only changes how readiness is sensed (max timestamp instead of
  partition existence). External time-partitioned sources can omit `partition_interval`/
  `partition_offset`; Chronon slices them on the consumer grid.

  ```python
  source = EventSource(
      table="vendor.events",
      query=Query(
          time_column="ts",
          partition_column="ts",
          time_partitioned=True,
      ),
  )
  ```
- **Chronon-produced tables**: use `producer.table` (or `join.derived_table` for modular
  join derivation output). For non-daily producers, that reference carries the producer grid
  into the consumer. Avoid string composition such as `f"{join.table}__derived"`; it drops
  grid metadata and compile will ask you to use `join.derived_table` or set the grid
  explicitly.
- **`triggerExpr`**: unchanged escape hatch for custom SQL readiness.

For catalog-partitioned upstreams with none of the above, partition-exists is trusted: a ds
present means its data is there (today's daily semantics).

## Changing a grid is a breaking change

The output grid (interval + offset) participates in the semantic hash: changing it renames
every ds in the output table, so it forces a version bump and a new table — old and new ds
shapes never mix. Schedule changes (and therefore delay changes) never affect the hash; reschedule
freely. Daily confs are untouched: an absent grid, `1d`, and `24h` all hash identically to
before.

## Formats

ds values default to `yyyy-MM-dd` (daily) and `yyyy-MM-dd-HH-mm` (sub-daily) —
dash-separated, because ds values become object-store directory names and spaces/colons
URL-escape. Custom output formats are discouraged (compile warns): compact formats like
`yyyyMMddHH` can silently mismatch downstream readers. Input tables keep declaring whatever
format they actually have. If a table's listing yields zero ds values that parse under its
declared format, jobs log a loud format-mismatch error instead of treating the table as
empty.

Hub CLI partition inputs are forgiving but the stored partition string is not. `--start-ds`
and `--end-ds` accept daily dates, dash-separated sub-daily values, ISO datetimes, and
space-separated datetimes such as `2024-01-15`, `2024-01-15-03`,
`2024-01-15-03-30`, `2024-01-15T03:30`, or `2024/01/15 03:30:00`. The CLI
validates the value and sends daily partitions as `yyyy-MM-dd` and sub-daily partitions as
`yyyy-MM-dd-HH-mm`. Inputs with non-zero seconds are rejected because Chronon partitions are
minute-aligned.

## Rules at a glance

1. Interval divides 24h or equals `1d`; offsets only below a day; whole minutes; all UTC
   (no DST-aware local grids).
2. Sub-daily crons: regular spacing, `*` day fields. Daily-or-coarser crons: any day
   restrictions welcome (weekly/monthly reports).
3. Snapshot-accuracy windows must be multiples of the conf's grid (a 6h window on a 3h grid
   is fine; 4h is not). Sub-daily ENTITIES snapshots compile with a cost warning — each
   snapshot is a full copy of dimensional state; prefer mutations + TEMPORAL accuracy when
   intraday entity state matters.
4. The left and sources need a boundary-compatible (same-or-finer) declared interval or
   `time_partitioned`; right parts are free.
5. Each fire materializes exactly one partition (the latest closed on the grid). Extra fires
   are idempotent no-ops; fires missed during downtime are recovered one-per-fire by
   scheduler catch-up. A cron coarser than the grid yields a sparse output by design.
6. Changing `partition_interval`/`partition_offset` = version bump = new output table.
7. `BatchNodeRunner` is the supported runner for sub-daily nodes; `Driver.scala` ad-hoc
   subcommands remain daily.
