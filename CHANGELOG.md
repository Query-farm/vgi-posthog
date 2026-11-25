# Changelog

## 0.2.0 (2026-11-25)

- Speaks VGI protocol 3.4: require vgi-python `>=0.55.1,<0.56`; uv.lock re-locked. This release
  needs Haybarn 2.0.1 (or stock DuckDB 2.0 with `INSTALL vgi FROM query_farm`): Haybarn 2.0.0
  loads a 3.3 `vgi` extension that cannot attach it.
- SQL tests run on haybarn-unittest 2.0.1rc3 (CI and Makefile).
- New `serve` extra (`vgi-python[http,oauth]`); the Docker image installs it, so the image runs in
  Orchard hosted mode (`VGI_ORCHARD_HOSTED=1` with VGI_JWT_ISSUER, VGI_JWT_AUDIENCE,
  VGI_JWT_JWKS_URI and VGI_SIGNING_KEY). The Docker HEALTHCHECK uses `HEAD /health`, which
  stays anonymous in hosted mode.

## 0.1.1 (2026-11-16)

- Bump vgi-python to 0.52.2.
- Requires Python 3.14 (CI, Docker image and tooling moved to 3.14).

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
