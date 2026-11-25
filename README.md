<p align="center">
  <img src="https://query.farm/media-kit/products/vgi/vgi-logo.png" alt="Vector Gateway Interface (VGI)" width="320">
</p>

# Query PostHog Events, Persons, Sessions and HogQL in DuckDB

> **vgi-posthog** · a [Query.Farm](https://query.farm) VGI worker

[![CI](https://github.com/Query-farm/vgi-posthog/actions/workflows/ci.yml/badge.svg)](https://github.com/Query-farm/vgi-posthog/actions/workflows/ci.yml)
[![vgi worker: query.farm](https://query.farm/media-kit/shields/vgi-worker.svg)](https://query.farm)

[PostHog](https://posthog.com) product analytics as SQL. Run any HogQL query through PostHog's
[Query API](https://posthog.com/docs/api/queries) and get typed DuckDB columns back; read events,
persons, sessions and groups with your `WHERE` clause translated into HogQL; and list feature flags,
experiments, surveys, insights, dashboards, cohorts and definitions. Works with **US Cloud**,
**EU Cloud** and **self-hosted** PostHog.

```sql
-- Which pages did pro-plan users view last week, and how often?
SELECT current_url, count(*) AS views
FROM posthog.main.events_where(event := '$pageview', properties := '{"plan": "pro"}',
                               after := TIMESTAMPTZ '2026-11-08', before := TIMESTAMPTZ '2026-11-15')
GROUP BY current_url ORDER BY views DESC LIMIT 10;
```

The event name, the property filter and the date range all run inside PostHog as HogQL.

## Quick start

**Prerequisites:** [uv](https://docs.astral.sh/uv/) and DuckDB 2.0 or [Haybarn](https://query.farm/products/haybarn) 2.0.

The worker is installed straight from this repository (it is not published to PyPI). `LOCATION`
is the command DuckDB runs to start it:

vgi-posthog 0.2.0 speaks VGI protocol 3.4, so it needs Haybarn 2.0.1 or newer (Haybarn 2.0.0 loads a 3.3 `vgi` extension that cannot attach it).

```sql
-- DuckDB 2.0
SET allow_extension_repositories = 'allowed';
CREATE EXTENSION REPOSITORY IF NOT EXISTS query_farm WITH PREFIX 'https://duckdb-extensions.query.farm';
INSTALL vgi FROM query_farm;
LOAD vgi FROM query_farm;
-- Haybarn 2.0: INSTALL vgi FROM community; LOAD vgi;

ATTACH 'posthog' (TYPE vgi,
  LOCATION 'uvx --from git+https://github.com/Query-farm/vgi-posthog@v0.2.0 vgi-posthog-worker');
```

From a checkout, `make build` and use `LOCATION '/abs/path/to/vgi-posthog/.venv/bin/vgi-posthog-worker'`.

## Credentials

Create a **personal API key** in PostHog (Settings → Personal API keys, starts with `phx_`) with read
access to *Query* and the resources you want to list, then a DuckDB secret **after** `ATTACH`:

```sql
-- US Cloud (default HOST https://us.posthog.com)
CREATE SECRET ph (TYPE posthog, PERSONAL_API_KEY 'phx_...', PROJECT_ID '12345');

-- EU Cloud
CREATE SECRET ph (TYPE posthog, PERSONAL_API_KEY 'phx_...', PROJECT_ID '12345', HOST 'https://eu.posthog.com');

-- Self-hosted
CREATE SECRET ph (TYPE posthog, PERSONAL_API_KEY 'phx_...', PROJECT_ID '1', HOST 'https://posthog.example.com');
```

| Field | Required | Meaning |
|---|---|---|
| `PERSONAL_API_KEY` | yes | Personal API key (`phx_...`), sent as a bearer token. Not the `phc_` project key. Redacted in `duckdb_secrets()`. |
| `PROJECT_ID` | for everything but `metadata.projects` | Numeric project (environment) id. `SELECT id, name FROM posthog.metadata.projects` lists them. |
| `HOST` | no | PostHog app URL. Default `https://us.posthog.com`. |
| `PAGE_SIZE` | no | Rows per HogQL page for the typed tables, 1–50,000 (default 10,000). |
| `TIMEOUT` | no | Per-request timeout in seconds (default 60). |
| `MAX_WAIT` | no | How long to poll an asynchronous query, in seconds (default 300). |

The key is read from the secret on every call. It never enters table-function state, logs or
query text, and it is only ever sent to `HOST`: REST `next` links that point at another host are
rebased onto `HOST` before they are followed.

## What's inside

### `posthog.main`: analytics data through HogQL

| Object | What it returns |
|---|---|
| `hogql(query, values := NULL, max_rows := 50000, refresh := 'blocking')` | Any HogQL `SELECT`, with columns typed from the response's ClickHouse types |
| `events` | `uuid`, `event`, `timestamp`, `distinct_id`, `person_id`, `session_id`, `current_url`, `properties` (JSON), `elements_chain`, `created_at` |
| `events_where(event, properties, distinct_id, person_id, after, before, max_rows, newest_first)` | Events filtered in HogQL, including JSON property equality, with an optional row cap |
| `persons` | `id`, `created_at`, `is_identified`, `properties` (JSON) |
| `person_distinct_ids` | `distinct_id`, `person_id` |
| `sessions` | `session_id`, start/end timestamps, `duration_seconds`, entry/exit URLs, UTM fields, `channel_type`, `pageview_count`, `autocapture_count`, `is_bounce` |
| `groups` | `group_type_index`, `group_key`, `created_at`, `properties` (JSON) |
| `clickhouse_type_to_duckdb(type)` | The DuckDB type `hogql()` gives a ClickHouse type (runs offline) |

### `posthog.metadata`: project resources through REST

`projects`, `group_types`, `event_definitions`, `property_definitions`, `actions` (match steps as
`LIST(STRUCT)`), `cohorts`, `feature_flags` (variants and release conditions with rollout
percentages), `experiments`, `surveys`, `insights` (definitions and query kind, not cached results),
`dashboards` and `annotations`.

Nothing secret is exposed: `projects` has no project API token column, and `feature_flags` reports
only `has_payloads`, never the payloads (teams often keep configuration values in them).

### HogQL

```sql
-- Constants go in `values` and are referenced as {name}: never quote them into the query.
SELECT * FROM posthog.main.hogql(
  'SELECT properties.$browser AS browser, count() AS users
   FROM events WHERE event = {ev} AND timestamp >= {since} GROUP BY browser',
  values := '{"ev": "$pageview", "since": "2026-11-01"}')
ORDER BY users DESC;
```

At bind time the worker sends `SELECT * FROM (<query>) LIMIT 0` to learn the columns and their
ClickHouse types (`String` → `VARCHAR`, `DateTime64` → `TIMESTAMPTZ`, `UInt64` → `UBIGINT`,
`Array(T)` → `T[]`, `Map(K, V)` → `MAP`, `Tuple(...)` → `STRUCT`, `Decimal(p, s)` → `DECIMAL`, anything
else → `VARCHAR`). It then sends

```
SELECT <columns DuckDB needs> FROM (<query>) WHERE <pushed predicates> [ORDER BY <col> LIMIT n] LIMIT <max_rows>
```

so projections, simple predicates on the result and a Top-N run in PostHog. Columns whose names are
not plain identifiers (an un-aliased `count()`) are read with `*`; alias computed columns.
`max_rows := 0` sends the query verbatim, with no wrapper. `refresh := 'async'` or `'force_async'`
lets PostHog run the query asynchronously; the worker polls
`GET /api/projects/:id/query/:query_id/` until it completes (up to `MAX_WAIT`).

## Pushdown

DuckDB hands the worker each table's simple predicates; the worker renders them as HogQL with every
constant in the Query API's `values` map (`{vgi_p0}`, `{vgi_p1}` ...), so no SQL value is ever spliced
into HogQL text. Identifiers are emitted only if they are plain or quote-safe; anything else simply
isn't pushed. Every pushed predicate is re-checked on the returned rows, so a predicate that can't be
translated still holds.

| Table | Pushed into the HogQL `WHERE` | Notes |
|---|---|---|
| `events` | `=`, `<>`, `<`, `<=`, `>`, `>=`, `IN`, `IS [NOT] NULL` and AND/OR of them on `event`, `timestamp`, `created_at`, `distinct_id`, `person_id`, `uuid`, `session_id` (`properties.$session_id`), `current_url` (`properties.$current_url`), `elements_chain` | Always bound `timestamp`. |
| `persons` | `id`, `created_at`, `is_identified` | |
| `person_distinct_ids` | `distinct_id`, `person_id` | |
| `sessions` | every column (`$start_timestamp`, `$channel_type`, `$is_bounce` ...) | |
| `groups` | `group_type_index` (`index`), `group_key` (`key`), `created_at` | |
| `events_where()` | its arguments, including `properties.<key> = {value}` for each JSON property | |
| `hogql()` | predicates on its result columns, wrapped around the query | |

- **Projection**: only the columns the query needs (plus the paging key) are selected.
- **Top-N**: `ORDER BY <column> [DESC] LIMIT n [OFFSET m]` on a table *with no `WHERE`* becomes one
  HogQL `ORDER BY ... LIMIT n+m` query. DuckDB only passes the limit to the scan when there is no
  filter; for a filtered "latest N", use `events_where(..., max_rows := N, newest_first := true)`.
- **Not pushed from SQL**: predicates on the JSON `properties` columns (`properties->>'plan' = 'pro'`)
  — DuckDB does not hand those to table scans. They still work, applied after reading; use
  `events_where(properties := '{"plan": "pro"}')` to push them.

## Limits and pagination

- PostHog returns at most **50,000 rows per HogQL query** (100 when the query has no `LIMIT`) and
  rejects `OFFSET` for personal API keys. The typed tables therefore page with a **keyset cursor**:
  `events` orders by `(timestamp, uuid)` and asks for `timestamp > t OR (timestamp = t AND uuid > u)`,
  `persons` pages by `id`, `sessions` by `session_id`, `groups` by `(index, key)`. The cursor travels in
  the scan state, so a long scan resumes on any worker process over the HTTP transport.
- `hogql()` is one request, capped at `max_rows` (≤ 50,000). Aggregate in HogQL, or read large row sets
  through `events`.
- PostHog rate-limits each project (240 queries a minute, 2,400 an hour, 3 concurrent, plus an hourly
  read budget). The worker retries HTTP 429 and 502/503/504 with backoff, honouring `Retry-After`.
  The query API is not an export path: for bulk history use PostHog's batch exports.
- REST listings page with DRF `limit`/`offset` and `next` links (100 per request).

## Examples

```sql
-- Daily active users this month
SELECT date_trunc('day', timestamp) AS day, count(DISTINCT person_id) AS dau
FROM posthog.main.events
WHERE timestamp >= TIMESTAMPTZ '2026-11-01' AND event = '$pageview'
GROUP BY day ORDER BY day;

-- Bounce rate by channel
SELECT channel_type, avg(is_bounce::INT) AS bounce_rate, median(duration_seconds) AS median_s
FROM posthog.main.sessions WHERE start_timestamp >= TIMESTAMPTZ '2026-11-01'
GROUP BY channel_type ORDER BY bounce_rate DESC;

-- Purchases per person email
SELECT p.properties->>'email' AS email, count(*) AS purchases
FROM posthog.main.events e JOIN posthog.main.persons p ON p.id = e.person_id
WHERE e.event = 'purchase' AND e.timestamp >= TIMESTAMPTZ '2026-11-01'
GROUP BY email ORDER BY purchases DESC;

-- Feature flags rolled out to less than 100%, one row per release condition
SELECT f.key, c.rollout_percentage, c.properties
FROM posthog.metadata.feature_flags f, unnest(f.conditions) AS u(c)
WHERE f.active AND coalesce(c.rollout_percentage, 100) < 100;

-- Insights on each dashboard
SELECT d.name, count(*) AS insights
FROM posthog.metadata.insights i, unnest(i.dashboard_ids) AS u(dashboard_id)
JOIN posthog.metadata.dashboards d ON d.id = u.dashboard_id GROUP BY d.name;
```

## Testing

```bash
make test-unit                 # unit tests: type mapping, HogQL building, pushdown, client
make test-sql                  # test/sql/*.test through Haybarn (stdio)
make test-sql TRANSPORT=http   # …over HTTP (also: launch)
make check                     # everything CI runs
```

PostHog is far too heavy to self-host in CI, so the tests run against `test/mock_api/server.py`, a
stdlib-only stand-in for the Query API and REST endpoints built from PostHog's documented shapes and
the fixture project in `test/fixtures/posthog.json` (regenerate with `python3 test/fixtures/generate.py`).
It **evaluates** the HogQL the worker sends: the worker's HogQL subset is rewritten to SQLite and run
against the fixture tables, so a wrong pushdown returns wrong rows. It also behaves like PostHog where
it matters: default 100-row limit, 50,000-row cap, HTTP 400 on `OFFSET`, `query_status` polling for
async queries, a 429 with `Retry-After` on the first request to every path, `next` links on a foreign
host, and a request log (`GET /__requests`) that `test/sql/pushdown.test` reads to assert the exact
HogQL and placeholder values. No network or credentials are needed.

## Metadata and the quality score

[vgi-lint](https://github.com/Query-farm/vgi-lint-check) checks the worker's metadata; settings live
in `vgi-lint.toml` (gate: `fail_on = "info"`, 100/100 with zero findings). Every table and function
needs a `posthog` secret, so CI runs the check with `execute: false`; the SQL suite covers behaviour
against the mock. `vgi-agent-tests.yaml` holds the graders for `make lint-ai`, written against the
fixture project.

## Logging and debugging

stdout carries the Arrow protocol, so the worker logs to stderr. `--debug` logs every HogQL query
(with placeholder names, never values or the key).

## Deploying

The same worker serves stdio (DuckDB starts it) or HTTP (`vgi-posthog-worker --http --port 8000`, or
the Dockerfile). Credentials still come from the DuckDB secret of each session, never from the
server's environment.

## Support

Open an issue at <https://github.com/Query-farm/vgi-posthog/issues>.

## Authorship & License

Copyright 2026 Query Farm LLC - https://query.farm

Released under the [BSD Zero Clause License](LICENSE) (0BSD). PostHog is a trademark of PostHog Inc.;
this worker is an independent project that uses PostHog's public API.
