# POC: Postgres analytics-aggregation support

**Status:** POC (TT-17841) — extends the Postgres driver's `Aggregate` pipeline
translator so the analytics-style aggregation pipelines used by Tyk consumers
(e.g. tyk-analytics) can run on Postgres through the same
`Aggregate(ctx, row, pipeline []DBM) ([]DBM, error)` interface that already
works on MongoDB.

## Background

`Aggregate` takes a MongoDB-style pipeline. The **Mongo** driver forwards it
straight to the server's native aggregation engine (a passthrough). The
**Postgres** driver must translate the pipeline into SQL by hand
(`translateAggregationPipeline` in `persistent/internal/driver/postgres/query.go`).
That translator previously supported only `$match`, `$group`
(`$sum`/`$avg`/`$min`/`$max`/`$count` over plain field references), `$project`
(`$concat`), `$sort`, `$limit`, `$skip` — not enough for real analytics reports.

## What this POC adds (B1 — raw-log & uptime reports)

### Conditional accumulators — `$sum: {$cond: …}` → `SUM(CASE WHEN … END)`

This is the headline capability. Analytics compute success/error and
response-code rollups with conditional counts, e.g.:

```go
"Success": DBM{"$sum": DBM{"$cond": DBM{
    "if":   DBM{"$or": []any{ DBM{"$eq": []any{"$Code", 200}}, /* … 207 */ }},
    "then": 1, "else": 0,
}}}
```

is now compiled to:

```sql
SUM(CASE WHEN (Code = 200 OR Code = 201 OR …) THEN 1 ELSE 0 END) AS Success
```

Supported inside `$cond`:
- **Boolean/logical:** `$and`, `$or` (n-ary), and comparisons `$eq`, `$ne`,
  `$gt`, `$gte`, `$lt`, `$lte` (2-element array form, MongoDB style).
- **Value expressions:** field references (`"$col"`), numeric/boolean/string
  literals, and nested `$cond`. Both `$cond` forms are accepted — object
  (`{if,then,else}`) and array (`[if,then,else]`).

### Hardening
- **Field-identifier validation** (`sanitizeAggField`): any `$field` referenced
  from an aggregation expression must be a plain identifier (`[A-Za-z_][A-Za-z0-9_]*`,
  dots → underscores); anything else is rejected, closing the SQL-injection vector
  the old `fmt.Sprintf`-based translator had.
- **Literal inlining:** numeric/boolean literals inside expressions are inlined
  (validated), not bound as `?` params, so SELECT-list expressions do not disturb
  positional parameter ordering relative to the WHERE clause.

### Related query-operator fixes (CRUD path, `translateQuery`)
Surfaced by the tyk-sink migration; also needed for analytics `$match`:
- **`$ne` now matches NULL rows** — `(col IS NULL OR col <> ?)`, matching MongoDB
  `$ne` semantics (a bare `NOT (col = ?)` wrongly excludes NULLs). This is what
  lets `{"is_oas": {"$ne": true}}` behave the same on Mongo and Postgres.
- **`$regex` / `$options`** → Postgres `~` (or `~*` when options contain `i`).
- **`$exists`** → `col IS NOT NULL` / `col IS NULL`.

### Tests
`TestTranslateAggregationConditional` (string-level, no DB) covers the
success/error conditional counts, the `$cond` array form, identifier rejection,
unsupported-operator errors, and the `$unwind` limitation. The `$ne/$regex/
$exists` changes are covered by the existing DB-backed `TestTranslateQuery`
(run under a build tag against a live Postgres).

## B2 — pre-aggregated / graph report (attempted; needs schema alignment)

The pre-aggregated report (`tyk_aggregated` / graph / MCP) does **not** reduce to
a single-table translation, because the Mongo and SQL **schemas diverge**:

- Mongo stores counters in arrays/sub-documents (`$lists.keyendpoints`,
  `$apikeys.<key>.hits`) and the pipeline does `$unwind $lists.*` + `$group` over
  a **dynamically-computed field-path prefix**.
- The equivalent SQL schema stores the same data as **one row per element**,
  keyed by `dimension` / `dimension_value` columns.

So `$unwind` over the array schema has no single-table SQL rewrite unless the two
schemas are aligned (or the translator is taught the dimension/row convention).
Rather than emit an incorrect query, the translator now **rejects `$unwind`
explicitly** with an actionable error pointing here. Closing B2 is a larger,
separate effort and is the main open decision for "remove *all* analytics SQL":

1. Align the SQL and Mongo schemas (store arrays, or teach the translator the
   `dimension`/`dimension_value` convention), **or**
2. add a scoped raw-SQL escape hatch for the handful of pre-aggregated queries.

## Still open (documented, not in this POC)
- Date-bucket `$group._id` via a first-class expression operator (`$dateTrunc`).
  Raw-log/uptime reports already bucket by grouping on pre-projected
  `Year/Month/Day/Hour` fields, which the existing `_id` handling supports; the
  pre-aggregated report's `date_trunc`/`EXTRACT(EPOCH …)` bucketing is part of B2.
- `$first`/`$last` accumulators (no direct GROUP BY equivalent without window
  functions).
- `$match`-after-`$group` → `HAVING`.
- Sharded aggregation (reuse the existing `_date_sharding` UNION-ALL mechanism in
  the aggregate path).
