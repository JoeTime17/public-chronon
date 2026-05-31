# Enhanced Stats Drift Context

This note captures the current drift implementation for future development.

## Purpose

Enhanced stats stores mergeable intermediate results in `ENHANCED_STATS`. Drift endpoints compare two
distribution summaries without reading raw feature rows.

Supported distribution sources:

- `APPROX_PERCENTILE` KLL sketches for high-cardinality numeric features.
- `HISTOGRAM` maps for low-cardinality categorical or numeric features.
- Boolean features reconstructed as `{true, false, null}` from `*_true_sum`, `*__null_sum`, and `total_count`.

All supported metric types return the same distance object:

```json
{"linf-distance": 0.0, "l2-distance": 0.0, "l1-distance": 0.0}
```

## Files

- `online/src/main/scala/ai/chronon/online/stats/DriftMetrics.scala`
  - KLL sketch distance logic.
  - Histogram Lp distance logic.
- `online/src/main/scala/ai/chronon/online/JavaStatsService.scala`
  - Fetches, decodes, merges enhanced stats IRs.
  - Resolves metric names.
  - Computes point and trailing drift.
- `service/src/main/java/ai/chronon/service/handlers/StatsDriftHandler.java`
  - `GET /v1/stats/:tableName/drift`
- `service/src/main/java/ai/chronon/service/handlers/StatsTrailingDriftHandler.java`
  - `GET /v1/stats/:tableName/trailing-drift`
- `service/src/main/java/ai/chronon/service/FetcherVerticle.java`
  - Route registration. Specific drift routes must stay before `/v1/stats/:tableName`.

## Point Drift Endpoint

Endpoint:

```text
GET /v1/stats/:tableName/drift
```

Required query parameters:

- `referenceStartTime`
- `referenceEndTime`
- `comparisonStartTime`
- `comparisonEndTime`

Optional query parameters:

- `metric`
- `dataset`, defaults to `ENHANCED_STATS`
- `semanticHash`

The service fetches both windows from KV, denormalizes tile IRs, merges them separately, resolves the metric,
then computes distances from the merged distributions.

Example:

```bash
curl -s "http://localhost:9000/v1/stats/gcp.demo.vStatsTest__2/drift?comparisonStartTime=1777334400000&comparisonEndTime=1777593599999&referenceStartTime=1777593600000&referenceEndTime=1777852799999&metric=listing_id_price_cents" | jq
```

## Trailing Drift Endpoint

Endpoint:

```text
GET /v1/stats/:tableName/trailing-drift
```

Required query parameters:

- `startDate`, `yyyy-MM-dd`
- `endDate`, `yyyy-MM-dd`
- `windowDays`

Optional query parameters:

- `metric`
- `distance`, one of `linf`, `l2`, `l1`; defaults to `linf`
- `dataset`, defaults to `ENHANCED_STATS`
- `semanticHash`

For each anchor date `ds`, trailing drift compares:

```text
reference:  [ds - windowDays, ds)
comparison: [ds - 2 * windowDays, ds - windowDays)
```

Internally those half-open date windows are converted to inclusive millisecond bounds because KV reads and
cached window merges are inclusive:

```scala
referenceStart = anchorMillis - windowMillis
referenceEnd = anchorMillis - 1
comparisonEnd = referenceStart - 1
comparisonStart = comparisonEnd - windowMillis + 1
```

The endpoint returns a JSON array of `x`/`y` points:

```json
[
  {"x": "2026-05-04", "y": 0.0189},
  {"x": "2026-05-05", "y": 0.0191}
]
```

If a single anchor cannot be computed, that point is returned with `y: null` and an `error` field. The whole
series does not fail unless request validation or setup fails.

Trailing drift uses the optimized path: it fetches the full backing range once, decodes each tile IR once, and
reuses cached decoded IRs for each anchor date. Because `RowAggregator.merge` mutates the left IR and KLL
sketches are merge-only, cached IRs must be cloned before merging into per-window accumulators.

Example:

```bash
curl -s "http://localhost:9000/v1/stats/gcp.demo.vStatsTest__2/trailing-drift?startDate=2026-05-04&endDate=2026-05-11&windowDays=3&metric=listing_id_price_cents&distance=linf" | jq
```

## Metric Resolution

The `metric` parameter can be an exact output metric or a base feature name.

Exact names:

- `feature_approx_percentile`
- `feature_histogram`
- `feature__str_histogram`
- `feature_true_sum` for booleans

Base feature names are resolved in this order:

1. `feature_approx_percentile`
2. `feature_histogram`
3. `feature__str_histogram`
4. `feature_true_sum`

This lets callers use `metric=listing_id_price_cents` even when enhanced stats emitted
`listing_id_price_cents__str_histogram`.

If no metric is supplied and exactly one drift-capable metric exists, that metric is used. Otherwise the service
returns an error listing available metrics.

## Distance Semantics

For histograms and boolean distributions:

- Normalize each count map to probability mass.
- Missing buckets are probability `0`.
- `linf` is max absolute bucket difference.
- `l2` is sqrt of squared bucket differences.
- `l1` is sum of absolute bucket differences.

For KLL percentile sketches:

- Build quantile values from both sketches.
- Use midpoints of adjacent distinct quantile values as PMF split points.
- Do not drop min/max quantiles. Dropping extrema caused separated point-mass sketches to produce no split
  points and false zero drift.
- Drop only bins whose mass is below `noiseFloor` in both distributions, then renormalize. This avoids tiny
  sketch boundary artifacts polluting distance values.
- `linf` is the max of PMF L-inf and CDF/rank L-inf over the split points.

## Validation Notes

Useful local validation commands:

```bash
./mill online.compile
./mill service.compile
./mill online.test.testOnly ai.chronon.online.test.stats.DriftMetricsTest
```

Useful smoke-test curls:

```bash
curl -s "http://localhost:9000/v1/stats/<table>/drift?comparisonStartTime=<ms>&comparisonEndTime=<ms>&referenceStartTime=<ms>&referenceEndTime=<ms>&metric=<feature>" | jq
```

```bash
curl -s "http://localhost:9000/v1/stats/<table>/trailing-drift?startDate=2026-05-04&endDate=2026-05-11&windowDays=3&metric=<feature>&distance=linf" | jq
```

Known good manual checks from development:

- `metric=listing_id_is_active` resolves boolean true/false/null drift.
- `metric=listing_id_price_cents` resolves low-cardinality numeric histogram drift.
