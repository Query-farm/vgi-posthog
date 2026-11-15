"""Assemble the ``posthog`` catalog and run the worker.

Run it:

    vgi-posthog-worker                      # stdio: what DuckDB launches by default
    vgi-posthog-worker --http --port 8000   # HTTP: for Docker / hosted deploys

Attach it from DuckDB, then give it credentials:

    ATTACH 'posthog' (TYPE vgi, LOCATION '/abs/path/to/.venv/bin/vgi-posthog-worker');
    CREATE SECRET (TYPE posthog, PERSONAL_API_KEY 'phx_...', PROJECT_ID '12345');
"""

from __future__ import annotations

import json
from typing import Any

import pyarrow as pa
from vgi import Worker
from vgi.catalog import Catalog, Schema, Table
from vgi.catalog.constraints import constraint_record
from vgi.metadata import FunctionExample

from .client import POSTHOG_SECRET
from .meta import (
    CATALOG_DESCRIPTION,
    CATALOG_NAME,
    CATALOG_TAGS,
    MAIN_CATEGORIES,
    MAIN_DOC_LLM,
    MAIN_DOC_MD,
    METADATA_CATEGORIES,
    METADATA_DOC_LLM,
    METADATA_DOC_MD,
    SOURCE_URL,
    TABLE_DOCS,
    keywords_array,
    object_tags,
)
from .query_fn import HogQL
from .rest import (
    Actions,
    Annotations,
    Cohorts,
    Dashboards,
    EventDefinitions,
    Experiments,
    FeatureFlags,
    GroupTypes,
    Insights,
    Projects,
    PropertyDefinitions,
    Surveys,
)
from .tables import Events, EventsWhere, Groups, PersonDistinctIds, Persons, Sessions
from .typemap_fn import ClickhouseTypeToDuckdb

Q = f"{CATALOG_NAME}.main"
M = f"{CATALOG_NAME}.metadata"

EXAMPLES: dict[str, list[tuple[str, str]]] = {
    "events": [
        (
            "Event counts for one day (the timestamp range runs in HogQL)",
            f"SELECT event, count(*) AS n FROM {Q}.events WHERE timestamp >= TIMESTAMPTZ '2026-11-01 00:00:00+00' "
            "AND timestamp < TIMESTAMPTZ '2026-11-02 00:00:00+00' GROUP BY event ORDER BY n DESC",
        ),
        (
            "Pageviews per URL for one user",
            f"SELECT current_url, count(*) AS views FROM {Q}.events WHERE event = '$pageview' "
            "AND distinct_id = 'user-1' GROUP BY current_url ORDER BY views DESC",
        ),
    ],
    "persons": [
        (
            "Identified persons and their email property",
            f"SELECT id, properties->>'email' AS email, created_at FROM {Q}.persons WHERE is_identified "
            "ORDER BY created_at",
        )
    ],
    "person_distinct_ids": [
        (
            "Persons with more than one distinct id",
            f"SELECT person_id, list(distinct_id ORDER BY distinct_id) AS ids FROM {Q}.person_distinct_ids "
            "GROUP BY person_id HAVING count(*) > 1",
        )
    ],
    "sessions": [
        (
            "Bounce rate and median duration per channel for November",
            f"SELECT channel_type, avg(is_bounce::INT) AS bounce_rate, median(duration_seconds) AS median_s "
            f"FROM {Q}.sessions WHERE start_timestamp >= TIMESTAMPTZ '2026-11-01 00:00:00+00' "
            "GROUP BY channel_type ORDER BY bounce_rate DESC",
        )
    ],
    "groups": [
        (
            "Companies by seat count",
            f"SELECT g.group_key, g.properties->>'name' AS name, (g.properties->>'seats')::INT AS seats "
            f"FROM {Q}.groups g JOIN {M}.group_types t USING (group_type_index) WHERE t.group_type = 'company' "
            "ORDER BY seats DESC",
        )
    ],
    "projects": [("Projects the key can read", f"SELECT id, name, timezone FROM {M}.projects ORDER BY id")],
    "group_types": [("Group type names", f"SELECT group_type_index, group_type FROM {M}.group_types ORDER BY 1")],
    "event_definitions": [
        (
            "Verified events by 30-day volume",
            f"SELECT name, volume_30_day, last_seen_at FROM {M}.event_definitions WHERE verified "
            "ORDER BY volume_30_day DESC NULLS LAST",
        )
    ],
    "property_definitions": [
        (
            "Numeric event properties",
            f"SELECT name, property_type FROM {M}.property_definitions WHERE definition_type = 'event' "
            "AND is_numerical ORDER BY name",
        )
    ],
    "actions": [
        (
            "Actions and the events their steps match",
            f"SELECT a.name, s.event, s.selector FROM {M}.actions a, unnest(a.steps) AS u(s) ORDER BY a.name",
        )
    ],
    "cohorts": [
        (
            "Dynamic cohorts by size",
            f"SELECT name, count, last_calculation FROM {M}.cohorts WHERE NOT is_static AND NOT deleted "
            "ORDER BY count DESC NULLS LAST",
        )
    ],
    "feature_flags": [
        (
            "Active flags and their rollout",
            f"SELECT key, rollout_percentage, is_multivariate FROM {M}.feature_flags WHERE active AND NOT deleted "
            "ORDER BY key",
        ),
        (
            "Flags used by running experiments",
            f"SELECT f.key, e.name AS experiment, e.start_date FROM {M}.feature_flags f "
            f"JOIN {M}.experiments e ON e.feature_flag_key = f.key WHERE e.end_date IS NULL "
            "AND e.start_date IS NOT NULL ORDER BY f.key",
        ),
    ],
    "experiments": [
        (
            "Running experiments",
            f"SELECT name, feature_flag_key, start_date FROM {M}.experiments WHERE start_date IS NOT NULL "
            "AND end_date IS NULL AND NOT archived ORDER BY start_date",
        )
    ],
    "surveys": [
        (
            "Launched surveys and their size",
            f"SELECT name, type, question_count FROM {M}.surveys WHERE start_date IS NOT NULL ORDER BY name",
        )
    ],
    "insights": [
        (
            "Saved insights per query kind",
            f"SELECT query_kind, count(*) AS insights FROM {M}.insights WHERE saved AND NOT deleted "
            "GROUP BY query_kind ORDER BY insights DESC",
        ),
        (
            "Insights on each dashboard",
            f"SELECT d.name AS dashboard, count(*) AS insights FROM {M}.insights i, unnest(i.dashboard_ids) "
            f"AS u(dashboard_id) JOIN {M}.dashboards d ON d.id = u.dashboard_id GROUP BY d.name ORDER BY d.name",
        ),
    ],
    "dashboards": [("Pinned dashboards", f"SELECT id, name FROM {M}.dashboards WHERE pinned ORDER BY name")],
    "annotations": [
        (
            "Deploy annotations",
            f"SELECT date_marker, content FROM {M}.annotations WHERE creation_type = 'GIT' ORDER BY date_marker",
        )
    ],
}


def _result_columns(schema: pa.Schema) -> str:
    def type_name(t: pa.DataType) -> str:
        if pa.types.is_timestamp(t):
            return "TIMESTAMPTZ"
        if pa.types.is_boolean(t):
            return "BOOLEAN"
        if pa.types.is_int64(t):
            return "BIGINT"
        if pa.types.is_int32(t):
            return "INTEGER"
        if pa.types.is_floating(t):
            return "DOUBLE"
        if getattr(t, "extension_name", None) == "arrow.json":
            return "JSON"
        if pa.types.is_list(t):
            return "LIST"
        return "VARCHAR"

    return json.dumps(
        [
            {"name": f.name, "type": type_name(f.type), "description": (f.metadata or {})[b"comment"].decode()}
            for f in schema
        ]
    )


def _examples_json(name: str) -> str:
    return json.dumps([{"description": d, "sql": s} for d, s in EXAMPLES[name]])


def _table(name: str, scan: type[Any], schema: list[str], key: list[str]) -> Table:
    """Build a catalog table over ``scan`` with NOT NULL and primary key records.

    No foreign keys are declared: DuckDB accepts only ENFORCED / ALL_ROWS constraint records, and
    PostHog keeps events whose person or session was deleted or merged away, so none can be attested.
    """
    doc = TABLE_DOCS[name]
    constraints = [
        constraint_record(
            schema,
            name,
            f"{name}_{f.name}_not_null",
            "NOT_NULL",
            [[f.name]],
            enforcement="ENFORCED",
            validation="ALL_ROWS",
        )
        for f in scan.FIXED_SCHEMA
        if not f.nullable
    ]
    constraints.append(
        constraint_record(
            schema,
            name,
            f"{name}_pk",
            "PRIMARY_KEY",
            [[c] for c in key],
            enforcement="ENFORCED",
            validation="ALL_ROWS",
        )
    )
    return Table(
        name=name,
        function=scan,
        comment=doc["comment"],
        constraints=tuple(constraints),
        column_comments={f.name: (f.metadata or {})[b"comment"].decode() for f in scan.FIXED_SCHEMA},
        tags={
            **object_tags(
                title=doc["title"],
                category=doc["category"],
                keywords=doc["keywords"],
                doc_llm=doc["doc_llm"],
                doc_md=doc["doc_md"],
            ),
            "domain": "product-analytics",
            "vgi.example_queries": _examples_json(name),
        },
    )


MAIN_TABLES = [
    _table("events", Events, ["main"], ["uuid"]),
    _table("persons", Persons, ["main"], ["id"]),
    _table("person_distinct_ids", PersonDistinctIds, ["main"], ["distinct_id"]),
    _table("sessions", Sessions, ["main"], ["session_id"]),
    _table("groups", Groups, ["main"], ["group_type_index", "group_key"]),
]
METADATA_TABLES = [
    _table("projects", Projects, ["metadata"], ["id"]),
    _table("group_types", GroupTypes, ["metadata"], ["group_type_index"]),
    _table("event_definitions", EventDefinitions, ["metadata"], ["id"]),
    _table("property_definitions", PropertyDefinitions, ["metadata"], ["id"]),
    _table("actions", Actions, ["metadata"], ["id"]),
    _table("cohorts", Cohorts, ["metadata"], ["id"]),
    _table("feature_flags", FeatureFlags, ["metadata"], ["id"]),
    _table("experiments", Experiments, ["metadata"], ["id"]),
    _table("surveys", Surveys, ["metadata"], ["id"]),
    _table("insights", Insights, ["metadata"], ["id"]),
    _table("dashboards", Dashboards, ["metadata"], ["id"]),
    _table("annotations", Annotations, ["metadata"], ["id"]),
]

HogQL.Meta.tags = {
    **object_tags(
        title="Run HogQL",
        category="query",
        keywords="hogql, sql, query api, clickhouse, ad hoc query, trends, aggregation",
        doc_llm=(
            "Run a HogQL SELECT (PostHog's SQL dialect over ClickHouse; tables events, persons, sessions, groups; "
            "property access as properties.$browser or person.properties.email) through PostHog's Query API and "
            "return its columns with DuckDB types mapped from the ClickHouse types in the response: String becomes "
            "`VARCHAR`, DateTime64 `TIMESTAMPTZ`, Array a `LIST`, Map a `MAP`, Tuple a `STRUCT`, Decimal a "
            "`DECIMAL`, UInt64 `UBIGINT` (clickhouse_type_to_duckdb shows the mapping). Pass constants in values as "
            "a JSON object and reference them as {name} placeholders instead of quoting them, and alias every "
            "computed column. The query is wrapped so DuckDB's projection and simple predicates on the result run "
            "in PostHog, capped at max_rows (default and PostHog maximum 50,000; 0 sends the query verbatim, where "
            "PostHog's default of 100 rows applies unless it has a LIMIT). There is no OFFSET paging: aggregate in "
            "HogQL, or read large row sets from the events table. refresh 'async' or 'force_async' runs the query "
            "asynchronously and the worker polls until it completes."
        ),
        doc_md=(
            "Runs any HogQL `SELECT` and returns typed columns. Bind constants with `values` and `{name}` "
            "placeholders instead of quoting them. At most 50,000 rows per query (PostHog's cap)."
        ),
    ),
    "domain": "product-analytics",
    "vgi.result_dynamic_columns_md": (
        "The columns are the HogQL query's own output columns, in order, named as PostHog names them (alias "
        "computed columns), all nullable. Each column's type follows the ClickHouse type PostHog reports "
        "(`clickhouse_type_to_duckdb` shows the mapping). For example, the query "
        "`SELECT event, count() AS n, max(timestamp) AS last_seen FROM events GROUP BY event` returns:\n\n"
        "| Name | Type | Description |\n"
        "|---|---|---|\n"
        "| event | VARCHAR | ClickHouse String: the event name. |\n"
        "| n | UBIGINT | ClickHouse UInt64: count() of events. |\n"
        "| last_seen | TIMESTAMPTZ | ClickHouse DateTime64(6, 'UTC'): latest event time. |"
    ),
}
HogQL.Meta.examples = [
    FunctionExample(
        sql=(
            f"SELECT event, n FROM {Q}.hogql('SELECT event, count() AS n FROM events WHERE timestamp >= {{since}} "
            'GROUP BY event\', values := \'{"since": "2026-11-01"}\') WHERE n > 1 ORDER BY n DESC'
        ),
        description="Event counts computed in PostHog, with the date bound as a placeholder",
    ),
    FunctionExample(
        sql=(
            f"SELECT plan, sum(revenue) AS revenue FROM {Q}.hogql('SELECT properties.plan AS plan, "
            'toFloat(properties.amount) AS revenue FROM events WHERE event = {ev}\', values := \'{"ev": "purchase"}\') '
            "GROUP BY plan ORDER BY revenue DESC"
        ),
        description="Revenue per plan from purchase events",
    ),
]

EventsWhere.Meta.tags = {
    **object_tags(
        title="Events Where",
        category="events",
        keywords="events, property filter, latest events, event properties, filtered events, limit",
        doc_llm=(
            "Return events (same columns as the events table) filtered inside PostHog by every argument given: "
            "event name, property equality given as a JSON object (values compared as text, the way HogQL reads "
            "properties; JSON null matches a missing property), distinct_id, person_id, after (inclusive "
            "`TIMESTAMPTZ`) and before (exclusive), with max_rows sent as the HogQL LIMIT. newest_first := true "
            "pages from the newest event, so with max_rows it reads the latest N. Reach for it instead of the "
            "events table when filtering on JSON properties, which DuckDB cannot push from SQL, or when a "
            "filtered scan should stop early; SQL predicates on typed columns are pushed as well."
        ),
        doc_md=("Events with property equality and a row cap pushed into HogQL, all arguments named and optional."),
    ),
    "domain": "product-analytics",
    "vgi.result_columns_schema": _result_columns(Events.FIXED_SCHEMA),
}
EventsWhere.Meta.examples = [
    FunctionExample(
        sql=(
            f"SELECT count(*) AS purchases, sum((properties->>'amount')::DOUBLE) AS revenue FROM {Q}.events_where("
            "event := 'purchase', properties := '{\"plan\": \"pro\"}')"
        ),
        description="Revenue from purchases by pro-plan users (property filter runs in HogQL)",
    ),
    FunctionExample(
        sql=(
            f"SELECT timestamp, event, current_url FROM {Q}.events_where(distinct_id := 'user-1', max_rows := 20, "
            "newest_first := true) ORDER BY timestamp DESC"
        ),
        description="The 20 latest events of one user",
    ),
]

FUNCTIONS: list[type[Any]] = [HogQL, EventsWhere, ClickhouseTypeToDuckdb]
for fn in FUNCTIONS:
    fn.Meta.tags = {
        **fn.Meta.tags,
        "vgi.example_queries": json.dumps([{"description": e.description, "sql": e.sql} for e in fn.Meta.examples]),
    }


def _schema_examples(names: list[str], extra: list[type[Any]]) -> str:
    out = [{"description": d, "sql": s} for n in names for d, s in EXAMPLES[n][:1]]
    out += [{"description": e.description, "sql": e.sql} for fn in extra for e in fn.Meta.examples[:1]]
    return json.dumps(out)


class PosthogWorker(Worker):
    """The worker process: the ``posthog`` catalog with ``main`` and ``metadata`` schemas."""

    catalog = Catalog(
        name=CATALOG_NAME,
        comment=CATALOG_DESCRIPTION,
        source_url=SOURCE_URL,
        tags=CATALOG_TAGS,
        schemas=[
            Schema(
                path=["main"],
                comment="PostHog analytics data through the Query API: HogQL, events, persons, sessions, groups.",
                tables=MAIN_TABLES,
                functions=FUNCTIONS,
                tags={
                    "vgi.title": "PostHog Analytics Data",
                    "domain": "product-analytics",
                    "vgi.keywords": keywords_array("hogql, events, persons, sessions, groups, query api"),
                    "vgi.doc_llm": MAIN_DOC_LLM,
                    "vgi.doc_md": MAIN_DOC_MD,
                    "vgi.categories": json.dumps(MAIN_CATEGORIES),
                    "vgi.example_queries": _schema_examples([t.name for t in MAIN_TABLES], FUNCTIONS),
                },
            ),
            Schema(
                path=["metadata"],
                comment="PostHog project resources: definitions, flags, experiments, surveys, insights, dashboards.",
                tables=METADATA_TABLES,
                tags={
                    "vgi.title": "PostHog Project Resources",
                    "domain": "product-analytics",
                    "vgi.keywords": keywords_array(
                        "feature flags, experiments, insights, dashboards, cohorts, definitions"
                    ),
                    "vgi.doc_llm": METADATA_DOC_LLM,
                    "vgi.doc_md": METADATA_DOC_MD,
                    "vgi.categories": json.dumps(METADATA_CATEGORIES),
                    "vgi.example_queries": _schema_examples([t.name for t in METADATA_TABLES], []),
                },
            ),
        ],
    )

    secret_types = [POSTHOG_SECRET]


def main() -> None:
    """Console-script entry point: parse ``--http`` / ``--unix`` / logging flags and serve."""
    PosthogWorker.main()


if __name__ == "__main__":
    main()
