# Changelog

## 0.1.0 (unreleased)

Initial PostHog worker, built from vgi-starter-python v1.3.0.

- `hogql(query, values, max_rows, refresh)`: any HogQL query through the Query API, typed from the
  response's ClickHouse types; projection, predicates and Top-N wrapped into HogQL; async queries polled.
- Tables `events`, `persons`, `person_distinct_ids`, `sessions`, `groups` with WHERE pushdown into HogQL
  (placeholders only) and keyset pagination; `events_where(...)` for property equality and row caps.
- `metadata` schema: projects, group types, event/property definitions, actions, cohorts, feature flags
  (no payloads), experiments, surveys, insights, dashboards, annotations.
- `clickhouse_type_to_duckdb(type)`: the type mapping hogql() applies.
- `posthog` secret (PERSONAL_API_KEY, PROJECT_ID, HOST for EU Cloud / self-hosted); 429 retry with Retry-After.

Targets: vgi-python 0.51 · VGI protocol 3.3 · Haybarn 2.0.0rc11 · vgi-lint-check 2.x (unpinned)
