"""Discovery metadata: catalog, schemas, tables and functions.

DuckDB, coding agents and the ``vgi-lint`` metadata checker all read these
tags. Every table and function carries ``object_tags(...)``: a title, a category
from its schema's category list, keywords, and LLM- and human-facing docs.
"""

from __future__ import annotations

import json
from typing import Any

import pyarrow as pa

CATALOG_NAME = "posthog"

SOURCE_URL = "https://github.com/Query-farm/vgi-posthog"
QUERY_API_DOCS = "https://posthog.com/docs/api/queries"

MAIN_CATEGORIES = [
    {"name": "events", "title": "Events", "description": "Captured events, filtered and paged in HogQL."},
    {"name": "people", "title": "People and groups", "description": "Persons, their distinct ids and groups."},
    {"name": "sessions", "title": "Sessions", "description": "Web sessions with entry/exit, duration and channel."},
    {"name": "query", "title": "HogQL", "description": "Arbitrary HogQL queries through the Query API."},
]
METADATA_CATEGORIES = [
    {"name": "projects", "title": "Projects", "description": "Projects and group types."},
    {"name": "definitions", "title": "Data management", "description": "Event/property definitions and actions."},
    {"name": "release", "title": "Flags, experiments, surveys", "description": "Feature flags, experiments, surveys."},
    {"name": "analytics", "title": "Analytics", "description": "Cohorts, insights, dashboards and annotations."},
]
_ALL_CATEGORIES = {c["name"] for c in MAIN_CATEGORIES + METADATA_CATEGORIES}


def keywords_array(keywords: str) -> str:
    """Turn ``"a, b, c"`` into the JSON array string ``vgi.keywords`` requires."""
    return json.dumps([k.strip() for k in keywords.split(",") if k.strip()])


def object_tags(*, title: str, category: str, keywords: str, doc_llm: str, doc_md: str) -> dict[str, str]:
    """Build the standard per-object discovery tags."""
    if category not in _ALL_CATEGORIES:
        raise ValueError(f"unknown category {category!r}")
    return {
        "vgi.title": title,
        "vgi.category": category,
        "vgi.keywords": keywords_array(keywords),
        "vgi.doc_llm": doc_llm,
        "vgi.doc_md": doc_md,
    }


def column(name: str, type_: pa.DataType, comment: str, *, nullable: bool = True) -> pa.Field[Any]:
    """Build an Arrow field whose ``comment`` shows up in DuckDB's ``DESCRIBE`` and agent tools."""
    return pa.field(name, type_, nullable=nullable, metadata={b"comment": comment.encode()})


CREDENTIALS_MD = (
    "Credentials come from a DuckDB secret of type `posthog`, created **after** `ATTACH`:\n\n"
    "```sql\n"
    "-- US Cloud (default host https://us.posthog.com)\n"
    "CREATE SECRET ph (TYPE posthog, PERSONAL_API_KEY 'phx_...', PROJECT_ID '12345');\n"
    "-- EU Cloud or self-hosted\n"
    "CREATE SECRET ph (TYPE posthog, PERSONAL_API_KEY 'phx_...', PROJECT_ID '12345',\n"
    "                  HOST 'https://eu.posthog.com');\n"
    "```\n\n"
    "Use a *personal* API key (Settings → Personal API keys) with read scopes for Query and the resources "
    "you list, not the `phc_` project key."
)

CATALOG_DESCRIPTION = (
    "PostHog product analytics as SQL: run HogQL through the Query API, read events, persons, sessions "
    "and groups with filters pushed into HogQL, and list feature flags, experiments, insights and other "
    "project resources. Works with US Cloud, EU Cloud and self-hosted PostHog."
)

CATALOG_DOC_LLM = (
    "Query one PostHog project (US Cloud, EU Cloud or self-hosted) from DuckDB. A DuckDB secret of type "
    "`posthog` created after ATTACH supplies the personal API key (phx_...), the numeric project id and, for "
    "EU Cloud or self-hosted PostHog, the HOST. Analytics data (events, persons, sessions, groups) is read "
    "through PostHog's Query API in HogQL, PostHog's SQL dialect over ClickHouse: WHERE predicates on typed "
    "columns are translated into the HogQL WHERE clause with placeholder values, and scans page with a keyset "
    "cursor because the API refuses OFFSET. Arbitrary HogQL can be run directly and comes back with "
    "DuckDB types mapped from the ClickHouse types in the response. Project resources (feature flags and "
    "their rollout, experiments, surveys, insights, dashboards, cohorts, annotations, actions, event and "
    "property definitions) come from the REST API. Timestamps are `TIMESTAMPTZ` in UTC; open-ended shapes "
    "such as properties are `JSON`. PostHog caps one query at 50,000 rows and rate-limits each project "
    "(240 queries a minute, 2,400 an hour); the worker retries HTTP 429 after Retry-After. Feature flag "
    "payloads and project API tokens are never exposed."
)

CATALOG_DOC_MD = (
    "# PostHog\n\n"
    "PostHog product analytics in DuckDB SQL: run HogQL through PostHog's "
    f"[Query API]({QUERY_API_DOCS}) and get typed columns back, read events, persons, sessions and groups "
    "with your `WHERE` clause translated into HogQL, and list feature flags, experiments, insights, "
    "dashboards, cohorts and definitions.\n\n"
    f"{CREDENTIALS_MD}"
)

MAIN_DOC_LLM = (
    "Analytics data of the secret's PostHog project, read through the Query API in HogQL. Typed tables "
    "cover captured events, persons and the distinct ids merged into them, web sessions and group-analytics "
    "entities; equality, range, IN and NULL predicates on their typed columns run inside PostHog, so bound "
    "the timestamp to keep scans small. JSON property predicates cannot be pushed from SQL and are applied "
    "by DuckDB after reading; the events_where table function pushes property equality and a row cap "
    "instead. The hogql table function runs any HogQL query, with constants passed as named placeholders "
    "bound from a JSON values argument, and returns at most 50,000 rows per call. Events link to persons "
    "through person_id and to sessions through session_id."
)

MAIN_DOC_MD = (
    "## main\n\n"
    "Analytics data through PostHog's Query API, in HogQL.\n\n"
    "`WHERE` predicates on typed columns become HogQL and scans page with a keyset cursor; bound the "
    "timestamp to keep them small.\n\n"
    "JSON property filters are applied by DuckDB; use the property-filter table function to push them, or "
    "write the HogQL yourself."
)

METADATA_DOC_LLM = (
    "PostHog project resources from the REST API, for the project in the secret (the project listing needs no "
    "project id). Covers data management (event and property definitions, actions), release tooling (feature "
    "flags with variants and release conditions, experiments, surveys) and analytics assets (cohorts, saved "
    "insight definitions without cached results, dashboards, chart annotations). Use it to discover event "
    "names and property keys before querying events, to audit flag rollout, or to map insights to "
    "dashboards. Feature flag payloads and project API tokens are never returned."
)

METADATA_DOC_MD = (
    "## metadata\n\n"
    "Project resources from PostHog's REST API: definitions, flags, experiments, surveys and analytics "
    "assets.\n\n"
    "Feature flag payloads and project tokens are never exposed; flags keep their variants and release "
    "conditions."
)

AGENT_TEST_TASKS = [
    {
        "name": "top_events",
        "prompt": "Using the posthog worker, how many events of each name were captured on 2026-11-01 (UTC)?",
    },
    {
        "name": "pro_purchases",
        "prompt": "How many purchase events have the event property plan equal to 'pro', and what is their "
        "total amount?",
    },
    {
        "name": "active_flags",
        "prompt": "List the keys of the active feature flags, with the rollout percentage of their first "
        "release condition.",
    },
    {"name": "bounce_sessions", "prompt": "Which sessions bounced? Give their session ids and entry URLs."},
    {
        "name": "purchases_by_email",
        "prompt": "How many purchase events did each person make? Show the person's email property "
        "(NULL when unknown) and the count.",
    },
    {
        "name": "merged_identities",
        "prompt": "Which persons have more than one distinct id, and how many ids does each have?",
    },
    {
        "name": "company_seats",
        "prompt": "For groups of the 'company' group type, list each group key with its seats property.",
    },
    {"name": "hogql_event_count", "prompt": "Run a HogQL query in PostHog that counts all events."},
    {"name": "project_timezones", "prompt": "Which PostHog projects can the key see, and in which time zones?"},
    {
        "name": "numeric_properties",
        "prompt": "Which event properties are numeric according to the property definitions?",
    },
    {
        "name": "verified_volume",
        "prompt": "What is the total 30-day volume of verified event definitions?",
    },
    {"name": "action_steps", "prompt": "How many match steps does each action have?"},
    {"name": "largest_cohort", "prompt": "Which cohort has the most persons?"},
    {
        "name": "running_experiments",
        "prompt": "Which experiments are running (started, not ended, not archived), and which flag drives them?",
    },
    {"name": "survey_questions", "prompt": "How many questions does each survey have?"},
    {
        "name": "dashboard_insights",
        "prompt": "How many saved insights are on each dashboard? Show the dashboard name.",
    },
    {"name": "deploy_annotations", "prompt": "Which annotations came from a deploy (git) integration?"},
    {
        "name": "array_type",
        "prompt": "What DuckDB type does a ClickHouse Array(String) column returned by a HogQL query become?",
    },
]

CATALOG_TAGS = {
    "vgi.title": "PostHog Product Analytics",
    "vgi.subtitle": "PostHog events, persons, sessions and HogQL as SQL, plus flags, experiments and insights.",
    "vgi.doc_llm": CATALOG_DOC_LLM,
    "vgi.doc_md": CATALOG_DOC_MD,
    "vgi.source_url": SOURCE_URL,
    "vgi.icon_url": "https://query.farm/media-kit/products/vgi/vgi-logo.png",
    "vgi.license": "0BSD",
    "vgi.author": "Query.Farm",
    "vgi.copyright": "Copyright 2026 Query Farm LLC - https://query.farm",
    "vgi.keywords": keywords_array(
        "posthog, hogql, product analytics, events, feature flags, experiments, sessions, query api"
    ),
    "vgi.support_contact": f"{SOURCE_URL}/issues",
    "vgi.support_policy_url": f"{SOURCE_URL}#support",
    "vgi.agent_test_tasks": json.dumps(AGENT_TEST_TASKS),
}


def _t(title: str, category: str, keywords: str, doc_llm: str, doc_md: str, comment: str) -> dict[str, str]:
    return {
        "title": title,
        "category": category,
        "keywords": keywords,
        "doc_llm": doc_llm,
        "doc_md": doc_md,
        "comment": comment,
    }


TABLE_DOCS: dict[str, dict[str, str]] = {
    "events": _t(
        "PostHog Events",
        "events",
        "events, pageviews, event stream, product analytics, clickstream, tracking",
        "Every captured event of the project, read from HogQL's events table ordered by (timestamp, uuid) with "
        "keyset pagination. Columns: uuid, event, timestamp (`TIMESTAMPTZ` UTC), distinct_id, person_id, "
        "session_id ($session_id), current_url ($current_url), properties (JSON), elements_chain, created_at. "
        "Pushed into HogQL: =, <>, <, <=, >, >=, IN and IS [NOT] NULL on event, timestamp, created_at, "
        "distinct_id, person_id, uuid, session_id, current_url and elements_chain, with AND/OR; only the "
        "needed columns are selected; ORDER BY <column> LIMIT n with no WHERE becomes one HogQL ORDER BY ... "
        "LIMIT n. Always bound timestamp. JSON property filters are applied by DuckDB; use events_where() to "
        "push property equality.",
        "Captured events. `WHERE` on `event`, `timestamp`, `distinct_id`, `person_id`, "
        "`session_id` ... runs in HogQL; bound `timestamp` to keep scans fast.",
        "PostHog events (HogQL events table), with WHERE pushed into HogQL.",
    ),
    "persons": _t(
        "PostHog Persons",
        "people",
        "persons, users, people, person properties, identified users",
        "Persons of the project from HogQL's persons table, paged by id: id (`UUID` text), created_at, "
        "is_identified, properties (JSON, e.g. properties->>'email'). Pushed into HogQL: id, created_at "
        "ranges and is_identified. Join events.person_id = persons.id, or person_distinct_ids for the "
        "distinct ids of a person.",
        "Persons with their properties. Join `events.person_id = persons.id`.",
        "PostHog persons (HogQL persons table).",
    ),
    "person_distinct_ids": _t(
        "Identity Map",
        "people",
        "distinct ids, identity, aliases, person mapping, anonymous ids",
        "Mapping of every distinct_id to the person it currently resolves to (HogQL person_distinct_ids): "
        "distinct_id, person_id. Pushed into HogQL: distinct_id and person_id equality / IN. Use it to find "
        "all ids merged into one person.",
        "Which person each distinct id belongs to.",
        "Which person each distinct id resolves to (HogQL person_distinct_ids table).",
    ),
    "sessions": _t(
        "Web Sessions",
        "sessions",
        "sessions, web analytics, bounce rate, session duration, entry page, utm, channel",
        "Sessions from HogQL's sessions table: session_id, distinct_id, start_timestamp, end_timestamp, "
        "duration_seconds, entry_current_url, exit_current_url, entry_referring_domain, entry_utm_source, "
        "entry_utm_medium, entry_utm_campaign, channel_type, pageview_count, autocapture_count, is_bounce. "
        "Pushed into HogQL: comparisons on every column (start_timestamp ranges, channel_type, is_bounce ...). "
        "Bound start_timestamp. Join events.session_id = sessions.session_id.",
        "Web sessions with entry/exit pages, UTM, channel type, duration and bounce flag.",
        "PostHog sessions (HogQL sessions table).",
    ),
    "groups": _t(
        "Group Analytics Entities",
        "people",
        "groups, companies, accounts, organizations, group analytics, b2b",
        "Groups (group analytics) from HogQL's groups table: group_type_index (0-4, names in "
        "metadata.group_types), group_key, created_at, properties (JSON). Pushed into HogQL: group_type_index "
        "and group_key comparisons, created_at ranges.",
        "Group analytics entities (companies, teams ...). Names of the types are in `metadata.group_types`.",
        "PostHog groups (HogQL groups table).",
    ),
    "projects": _t(
        "PostHog Projects",
        "projects",
        "projects, environments, project id, organization, timezone",
        "Projects (environments) visible to the personal API key: id (the PROJECT_ID secret value), uuid, "
        "name, organization_id, timezone, is_demo, ingested_event, created_at. The project API token is "
        "deliberately not exposed. Needs no PROJECT_ID in the secret.",
        "Projects the key can read; use `id` as the secret's `PROJECT_ID`.",
        "Projects (environments) the personal API key can read, with ids, names and time zones.",
    ),
    "group_types": _t(
        "Group Type Names",
        "projects",
        "group types, group analytics, company type, group index",
        "Group types of the project: group_type_index (matches main.groups.group_type_index), group_type "
        "(key such as 'company'), name_singular, name_plural.",
        "Names of the group type slots used by `main.groups`.",
        "Group type slots of the project (company, team ...) with display names.",
    ),
    "event_definitions": _t(
        "Event Catalog",
        "definitions",
        "event definitions, data management, event catalog, event volume, taxonomy",
        "Event definitions from data management: id, name, description, tags, verified, volume_30_day, "
        "query_usage_30_day, created_at, last_seen_at, last_updated_at. Use it to discover event names before "
        "querying events.",
        "The event catalog with 30-day volume and last-seen time.",
        "Event catalog from data management: names, 30-day volume, last seen, verification.",
    ),
    "property_definitions": _t(
        "Property Catalog",
        "definitions",
        "property definitions, properties, taxonomy, schema, person properties, event properties",
        "Property definitions: id, name, definition_type (event, person, group or session), group_type_index, "
        "property_type (String, Numeric, Boolean, DateTime, Duration), is_numerical, description, tags, "
        "verified. Use it to find property keys for properties->>'key' or events_where(properties := ...).",
        "Known property keys and their value types.",
        "Property catalog: keys of event, person, group and session properties with value types.",
    ),
    "actions": _t(
        "Saved Actions",
        "definitions",
        "actions, action steps, event matching, selectors, autocapture",
        "Actions: id, name, description, steps (`LIST` of `STRUCT` event, url, url_matching, selector, text, href, "
        "tag_name; an event matches the action when any step matches), tags, deleted, created_at, created_by.",
        "Saved actions and the steps that match events to them.",
        "Saved actions and the match steps (event, URL, selector) that define them.",
    ),
    "cohorts": _t(
        "Person Cohorts",
        "analytics",
        "cohorts, segments, audiences, static cohort, dynamic cohort",
        "Cohorts: id, name, description, is_static, count (persons at last calculation), is_calculating, "
        "last_calculation, errors_calculating, deleted, created_at, created_by, filters (JSON), query (JSON).",
        "Cohorts with their size and definition.",
        "Person cohorts with their size, calculation state and filter definition.",
    ),
    "feature_flags": _t(
        "Feature Flag Rollout",
        "release",
        "feature flags, rollout, release conditions, variants, multivariate, toggles",
        "Feature flags: id, key, name, active, deleted, is_multivariate, variants (`LIST` of `STRUCT` key, name, "
        "rollout_percentage), conditions (`LIST` of `STRUCT` rollout_percentage, variant, properties JSON text), "
        "rollout_percentage (first condition), ensure_experience_continuity, has_payloads, experiment_ids, "
        "tags, created_at, created_by. Payloads are never returned (they often hold configuration secrets). "
        "unnest(conditions) gives one row per release condition.",
        "Flag definitions and rollout. Payloads are not exposed.",
        "Feature flags with variants, release conditions and rollout percentages (no payloads).",
    ),
    "experiments": _t(
        "A/B Experiments",
        "release",
        "experiments, a/b tests, split tests, variants, hypotheses",
        "Experiments: id, name, description, type (product/web), feature_flag_key, start_date (NULL = draft), "
        "end_date (NULL = running), archived, created_at, created_by, parameters (JSON), metrics (JSON). Join "
        "feature_flags on key = feature_flag_key.",
        "A/B tests with their flag and dates.",
        "A/B experiments with their feature flag, launch and end dates, and metrics.",
    ),
    "surveys": _t(
        "Survey Definitions",
        "release",
        "surveys, nps, questionnaires, feedback, questions",
        "Surveys: id, name, description, type (popover, widget, api ...), linked_flag_key, question_count, "
        "questions (JSON), start_date, end_date, archived, created_at, created_by. Responses are events "
        "named 'survey sent' in main.events.",
        "Survey definitions. Responses are `survey sent` events.",
        "Survey definitions with their questions, linked flag and launch dates.",
    ),
    "insights": _t(
        "Saved Insights",
        "analytics",
        "insights, saved queries, trends, funnels, charts, reports",
        "Saved insights (definitions, not cached results): id, short_id, name, derived_name, description, "
        "query_kind (TrendsQuery, FunnelsQuery, HogQLQuery ...), query (JSON), dashboard_ids, favorited, saved, "
        "deleted, tags, created_at, created_by, last_modified_at.",
        "Saved insight definitions and the dashboards they are on.",
        "Saved insight definitions (query kind and query JSON) and the dashboards that show them.",
    ),
    "dashboards": _t(
        "Analytics Dashboards",
        "analytics",
        "dashboards, reports, pinned dashboards, shared dashboards",
        "Dashboards: id, name, description, pinned, is_shared, deleted, tags, created_at, created_by. "
        "insights.dashboard_ids references dashboards.id.",
        "Dashboards of the project.",
        "Analytics dashboards of the project, with pinned and sharing state.",
    ),
    "annotations": _t(
        "Chart Annotations",
        "analytics",
        "annotations, chart notes, deploy markers, releases, timeline",
        "Annotations: id, content, date_marker (the annotated moment), scope (dashboard_item, dashboard, "
        "project, organization), creation_type (USR or GIT), insight_id, deleted, created_at, updated_at, "
        "created_by.",
        "Notes marked on charts, such as releases and incidents.",
        "Chart annotations marking releases and incidents, from users or deploy integrations.",
    ),
}
