"""The ``metadata`` schema: PostHog REST resources (definitions, flags, insights ...) as tables.

Each table pages through one DRF list endpoint under ``/api/projects/:id/``
(``limit``/``offset`` with ``next`` links). The cursor is the ``next`` link's
path and query string; it is rebased onto the configured host before it is
followed, so the API key is never sent elsewhere.

Nothing secret is exposed: ``projects`` omits the project API token, and
``feature_flags`` omits payloads (which teams use for config values) and keeps
only the rollout definition.
"""

from __future__ import annotations

import urllib.parse
from dataclasses import dataclass
from typing import Annotated, Any, ClassVar

import pyarrow as pa
from vgi.arguments import Secret
from vgi.invocation import BindResponse
from vgi.table_function import (
    BindParams,
    OutputCollector,
    ProcessParams,
    TableFunctionGenerator,
    init_single_worker,
)
from vgi_rpc import ArrowSerializableDataclass

from .client import POSTHOG_SECRET_TYPE, PostHogClient
from .hogql import TSTZ, convert
from .meta import column
from .scan import NoArgs, client_from

PAGE_LIMIT = 100
STR_LIST = pa.list_(pa.string())
INT_LIST = pa.list_(pa.int64())
DEFINITION_TYPES = {1: "event", 2: "person", 3: "group", 4: "session"}


@dataclass(kw_only=True)
class RestState(ArrowSerializableDataclass):
    """Cursor: the next page's path + query ('' before the first page), and whether we are done."""

    next: str = ""
    done: bool = False


def _user(value: Any) -> str | None:
    """Email (or name) of a ``created_by`` user object."""
    if isinstance(value, dict):
        return value.get("email") or value.get("first_name") or None
    return None


def _tags(value: Any) -> list[str]:
    return [str(t) for t in value or [] if t is not None]


@init_single_worker
class RestScan(TableFunctionGenerator[NoArgs, RestState]):
    """Base class for tables read from one PostHog REST list endpoint."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema([])
    PATH: ClassVar[str] = ""
    PROJECT_SCOPED: ClassVar[bool] = True
    QUERY: ClassVar[dict[str, Any]] = {}

    class Meta:
        """Overridden by every concrete table."""

        name = "rest_scan"
        description = "Base class for PostHog REST scans"

    @classmethod
    def on_bind(
        cls,
        params: BindParams[NoArgs],
        *,
        posthog: Annotated[dict[str, pa.Scalar[Any]] | None, Secret(POSTHOG_SECRET_TYPE)] = None,
    ) -> BindResponse:
        """Declare the fixed schema and ask DuckDB for the ``posthog`` secret."""
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[NoArgs]) -> RestState:
        """Start at the first page."""
        return RestState()

    @classmethod
    def row(cls, item: dict[str, Any]) -> dict[str, Any]:
        """Map one API object to column values (raw; converted to the column type afterwards)."""
        raise NotImplementedError

    @classmethod
    def first_url(cls, client: PostHogClient) -> str:
        """URL of the first page."""
        path = cls.PATH.format(project=client.project_id()) if cls.PROJECT_SCOPED else cls.PATH
        return client.url(path, {"limit": PAGE_LIMIT, **cls.QUERY})

    @classmethod
    def process(cls, params: ProcessParams[NoArgs], state: RestState, out: OutputCollector) -> None:
        """Read one page, emit it, and remember the ``next`` link."""
        if state.done:
            out.finish()
            return
        client = client_from(params)
        url = client.url(state.next) if state.next else cls.first_url(client)
        body = client.get(url)
        if isinstance(body, list):
            items, nxt = body, None
        else:
            items, nxt = list(body.get("results") or []), body.get("next")
        if nxt:
            parts = urllib.parse.urlsplit(str(nxt))
            state.next = parts.path + (f"?{parts.query}" if parts.query else "")
        else:
            state.done = True
        if not items:
            out.finish()
            return
        schema = params.output_schema
        rows = [cls.row(item) for item in items if isinstance(item, dict)]
        columns = {f.name: [convert(r.get(f.name), f.type) for r in rows] for f in schema}
        out.emit(pa.RecordBatch.from_pydict(columns, schema=schema))


class Projects(RestScan):
    """Rows for ``posthog.metadata.projects``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            column("id", pa.int64(), "Project (environment) id; the PROJECT_ID secret field.", nullable=False),
            column("uuid", pa.string(), "Project UUID."),
            column("name", pa.string(), "Project name."),
            column("organization_id", pa.string(), "UUID of the organization that owns the project."),
            column("timezone", pa.string(), "Project time zone, e.g. 'UTC' or 'Europe/Berlin'."),
            column("is_demo", pa.bool_(), "True for a demo project."),
            column("ingested_event", pa.bool_(), "True once the project has received an event."),
            column("created_at", TSTZ, "When the project was created."),
        ]
    )
    PATH: ClassVar[str] = "/api/projects/"
    PROJECT_SCOPED: ClassVar[bool] = False

    class Meta:
        """Table metadata."""

        name = "projects_scan"
        description = "Scan behind the projects table"

    @classmethod
    def row(cls, item: dict[str, Any]) -> dict[str, Any]:
        """Map project fields, without the project API token."""
        org = item.get("organization")
        return {
            "id": item.get("id"),
            "uuid": item.get("uuid"),
            "name": item.get("name"),
            "organization_id": org.get("id") if isinstance(org, dict) else org,
            "timezone": item.get("timezone"),
            "is_demo": item.get("is_demo"),
            "ingested_event": item.get("ingested_event"),
            "created_at": item.get("created_at"),
        }


class EventDefinitions(RestScan):
    """Rows for ``posthog.metadata.event_definitions``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            column("id", pa.string(), "Definition UUID.", nullable=False),
            column("name", pa.string(), "Event name.", nullable=False),
            column("description", pa.string(), "Description from the data management page."),
            column("tags", STR_LIST, "Tags on the definition."),
            column("verified", pa.bool_(), "True when a team member verified the event."),
            column("volume_30_day", pa.int64(), "Events in the last 30 days (count; NULL when not computed)."),
            column("query_usage_30_day", pa.int64(), "Queries using the event in the last 30 days (count)."),
            column("created_at", TSTZ, "When the event was first defined."),
            column("last_seen_at", TSTZ, "When the event was last ingested."),
            column("last_updated_at", TSTZ, "When the definition was last edited."),
        ]
    )
    PATH: ClassVar[str] = "/api/projects/{project}/event_definitions/"

    class Meta:
        """Table metadata."""

        name = "event_definitions_scan"
        description = "Scan behind the event_definitions table"

    @classmethod
    def row(cls, item: dict[str, Any]) -> dict[str, Any]:
        """Map event definition fields."""
        return {**item, "tags": _tags(item.get("tags"))}


class PropertyDefinitions(RestScan):
    """Rows for ``posthog.metadata.property_definitions``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            column("id", pa.string(), "Definition UUID.", nullable=False),
            column("name", pa.string(), "Property name, e.g. '$browser' or 'plan'.", nullable=False),
            column("definition_type", pa.string(), "What the property belongs to: event, person, group or session."),
            column(
                "group_type_index", pa.int32(), "Group type slot index (0 to 4) for group properties; NULL otherwise."
            ),
            column("property_type", pa.string(), "Value type: String, Numeric, Boolean, DateTime or Duration."),
            column("is_numerical", pa.bool_(), "True when values are numbers."),
            column("description", pa.string(), "Description from the data management page."),
            column("tags", STR_LIST, "Tags on the definition."),
            column("verified", pa.bool_(), "True when a team member verified the property."),
        ]
    )
    PATH: ClassVar[str] = "/api/projects/{project}/property_definitions/"

    class Meta:
        """Table metadata."""

        name = "property_definitions_scan"
        description = "Scan behind the property_definitions table"

    @classmethod
    def row(cls, item: dict[str, Any]) -> dict[str, Any]:
        """Map property definition fields; the numeric ``type`` becomes its name."""
        kind = item.get("type")
        return {
            **item,
            "definition_type": DEFINITION_TYPES.get(kind, kind) if isinstance(kind, int) else kind,
            "tags": _tags(item.get("tags")),
        }


class Cohorts(RestScan):
    """Rows for ``posthog.metadata.cohorts``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            column("id", pa.int64(), "Numeric identifier of the cohort (ids, not quantities).", nullable=False),
            column("name", pa.string(), "Cohort name."),
            column("description", pa.string(), "Cohort description."),
            column("is_static", pa.bool_(), "True for a static (uploaded) cohort, false for a dynamic one."),
            column("count", pa.int64(), "Persons in the cohort at the last calculation (count)."),
            column("is_calculating", pa.bool_(), "True while PostHog recalculates the cohort."),
            column("last_calculation", TSTZ, "When the cohort was last calculated."),
            column("errors_calculating", pa.int64(), "Consecutive calculation errors (count)."),
            column("deleted", pa.bool_(), "True for a soft-deleted cohort."),
            column("created_at", TSTZ, "When the cohort was created."),
            column("created_by", pa.string(), "Email of the user who created the cohort."),
            column("filters", pa.json_(), "Cohort definition (property groups), JSON."),
            column("query", pa.json_(), "HogQL/insight query defining the cohort, JSON (newer cohorts)."),
        ]
    )
    PATH: ClassVar[str] = "/api/projects/{project}/cohorts/"

    class Meta:
        """Table metadata."""

        name = "cohorts_scan"
        description = "Scan behind the cohorts table"

    @classmethod
    def row(cls, item: dict[str, Any]) -> dict[str, Any]:
        """Map cohort fields."""
        return {**item, "created_by": _user(item.get("created_by"))}


VARIANT = pa.struct(
    [pa.field("key", pa.string()), pa.field("name", pa.string()), pa.field("rollout_percentage", pa.float64())]
)
CONDITION = pa.struct(
    [
        pa.field("rollout_percentage", pa.float64()),
        pa.field("variant", pa.string()),
        pa.field("properties", pa.string()),
    ]
)


class FeatureFlags(RestScan):
    """Rows for ``posthog.metadata.feature_flags``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            column("id", pa.int64(), "Numeric identifier of the flag (ids, not quantities).", nullable=False),
            column("key", pa.string(), "Flag key used in code, e.g. 'new-checkout'.", nullable=False),
            column("name", pa.string(), "Flag description."),
            column("active", pa.bool_(), "True when the flag is enabled."),
            column("deleted", pa.bool_(), "True for a soft-deleted flag."),
            column("is_multivariate", pa.bool_(), "True when the flag has variants."),
            column("variants", pa.list_(VARIANT), "Variants: key, name and rollout_percentage (0-100)."),
            column(
                "conditions",
                pa.list_(CONDITION),
                "Release conditions: rollout_percentage (0-100, NULL = 100), variant override, and the "
                "property filters as JSON text.",
            ),
            column("rollout_percentage", pa.float64(), "Rollout of the first condition (0-100), when it has one."),
            column("ensure_experience_continuity", pa.bool_(), "True when flag values persist across identify."),
            column("has_payloads", pa.bool_(), "True when the flag carries payloads (the payloads are not exposed)."),
            column(
                "experiment_ids",
                INT_LIST,
                "Numeric identifiers of the experiments that use the flag (ids, not quantities).",
            ),
            column("tags", STR_LIST, "Tags on the flag."),
            column("created_at", TSTZ, "When the flag was created."),
            column("created_by", pa.string(), "Email of the user who created the flag."),
        ]
    )
    PATH: ClassVar[str] = "/api/projects/{project}/feature_flags/"

    class Meta:
        """Table metadata."""

        name = "feature_flags_scan"
        description = "Scan behind the feature_flags table"

    @classmethod
    def row(cls, item: dict[str, Any]) -> dict[str, Any]:
        """Map flag definition and rollout; payloads are dropped."""
        import json

        filters = item.get("filters") or {}
        multivariate = filters.get("multivariate") or {}
        variants = [
            {"key": v.get("key"), "name": v.get("name"), "rollout_percentage": v.get("rollout_percentage")}
            for v in multivariate.get("variants") or []
            if isinstance(v, dict)
        ]
        conditions = [
            {
                "rollout_percentage": g.get("rollout_percentage"),
                "variant": g.get("variant"),
                "properties": json.dumps(g.get("properties") or [], sort_keys=True),
            }
            for g in filters.get("groups") or []
            if isinstance(g, dict)
        ]
        payloads = filters.get("payloads") or {}
        rollout = item.get("rollout_percentage")
        if rollout is None and conditions:
            rollout = conditions[0]["rollout_percentage"]
        return {
            "id": item.get("id"),
            "key": item.get("key"),
            "name": item.get("name"),
            "active": item.get("active"),
            "deleted": item.get("deleted"),
            "is_multivariate": bool(variants),
            "variants": variants,
            "conditions": conditions,
            "rollout_percentage": rollout,
            "ensure_experience_continuity": item.get("ensure_experience_continuity"),
            "has_payloads": any(v not in (None, "", {}) for v in payloads.values()),
            "experiment_ids": [int(e) for e in item.get("experiment_set") or [] if isinstance(e, int)],
            "tags": _tags(item.get("tags")),
            "created_at": item.get("created_at"),
            "created_by": _user(item.get("created_by")),
        }


class Experiments(RestScan):
    """Rows for ``posthog.metadata.experiments``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            column("id", pa.int64(), "Numeric identifier of the experiment (ids, not quantities).", nullable=False),
            column("name", pa.string(), "Experiment name."),
            column("description", pa.string(), "Experiment description / hypothesis."),
            column("type", pa.string(), "'product' or 'web'."),
            column("feature_flag_key", pa.string(), "Key of the flag that splits traffic."),
            column("start_date", TSTZ, "When the experiment was launched; NULL for a draft."),
            column("end_date", TSTZ, "When the experiment was stopped; NULL while running."),
            column("archived", pa.bool_(), "True for an archived experiment."),
            column("created_at", TSTZ, "When the experiment was created."),
            column("created_by", pa.string(), "Email of the user who created the experiment."),
            column("parameters", pa.json_(), "Experiment parameters (variants, minimum detectable effect), JSON."),
            column("metrics", pa.json_(), "Primary metrics, JSON."),
        ]
    )
    PATH: ClassVar[str] = "/api/projects/{project}/experiments/"

    class Meta:
        """Table metadata."""

        name = "experiments_scan"
        description = "Scan behind the experiments table"

    @classmethod
    def row(cls, item: dict[str, Any]) -> dict[str, Any]:
        """Map experiment fields."""
        flag = item.get("feature_flag")
        key = item.get("feature_flag_key") or (flag.get("key") if isinstance(flag, dict) else None)
        return {**item, "feature_flag_key": key, "created_by": _user(item.get("created_by"))}


def _query_kind(query: Any) -> str | None:
    if not isinstance(query, dict):
        return None
    source = query.get("source")
    if isinstance(source, dict) and source.get("kind"):
        return str(source["kind"])
    return str(query["kind"]) if query.get("kind") else None


class Insights(RestScan):
    """Rows for ``posthog.metadata.insights``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            column("id", pa.int64(), "Numeric identifier of the insight (ids, not quantities).", nullable=False),
            column("short_id", pa.string(), "Short id used in insight URLs (/insights/<short_id>)."),
            column("name", pa.string(), "Insight name; NULL for unnamed insights."),
            column("derived_name", pa.string(), "Name PostHog generated from the query."),
            column("description", pa.string(), "Insight description."),
            column("query_kind", pa.string(), "Kind of query, e.g. TrendsQuery, FunnelsQuery or HogQLQuery."),
            column("query", pa.json_(), "Insight query definition, JSON."),
            column(
                "dashboard_ids",
                INT_LIST,
                "Numeric identifiers of the dashboards the insight is on (ids, not quantities).",
            ),
            column("favorited", pa.bool_(), "True when starred."),
            column("saved", pa.bool_(), "True for a saved insight."),
            column("deleted", pa.bool_(), "True for a soft-deleted insight."),
            column("tags", STR_LIST, "Tags on the insight."),
            column("created_at", TSTZ, "When the insight was created."),
            column("created_by", pa.string(), "Email of the user who created the insight."),
            column("last_modified_at", TSTZ, "When the insight was last edited."),
        ]
    )
    PATH: ClassVar[str] = "/api/projects/{project}/insights/"

    class Meta:
        """Table metadata."""

        name = "insights_scan"
        description = "Scan behind the insights table"

    @classmethod
    def row(cls, item: dict[str, Any]) -> dict[str, Any]:
        """Map insight definition fields (cached results are not read)."""
        return {
            **item,
            "query_kind": _query_kind(item.get("query")),
            "dashboard_ids": [int(d) for d in item.get("dashboards") or [] if isinstance(d, int)],
            "tags": _tags(item.get("tags")),
            "created_by": _user(item.get("created_by")),
        }


class Dashboards(RestScan):
    """Rows for ``posthog.metadata.dashboards``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            column("id", pa.int64(), "Numeric identifier of the dashboard (ids, not quantities).", nullable=False),
            column("name", pa.string(), "Dashboard name."),
            column("description", pa.string(), "Dashboard description."),
            column("pinned", pa.bool_(), "True when pinned to the sidebar."),
            column("is_shared", pa.bool_(), "True when shared publicly."),
            column("deleted", pa.bool_(), "True for a soft-deleted dashboard."),
            column("tags", STR_LIST, "Tags on the dashboard."),
            column("created_at", TSTZ, "When the dashboard was created."),
            column("created_by", pa.string(), "Email of the user who created the dashboard."),
        ]
    )
    PATH: ClassVar[str] = "/api/projects/{project}/dashboards/"

    class Meta:
        """Table metadata."""

        name = "dashboards_scan"
        description = "Scan behind the dashboards table"

    @classmethod
    def row(cls, item: dict[str, Any]) -> dict[str, Any]:
        """Map dashboard fields."""
        return {**item, "tags": _tags(item.get("tags")), "created_by": _user(item.get("created_by"))}


class Annotations(RestScan):
    """Rows for ``posthog.metadata.annotations``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            column("id", pa.int64(), "Numeric identifier of the annotation (ids, not quantities).", nullable=False),
            column("content", pa.string(), "Annotation text."),
            column("date_marker", TSTZ, "The moment the annotation marks on charts."),
            column("scope", pa.string(), "Where it shows: dashboard_item, dashboard, project or organization."),
            column("creation_type", pa.string(), "'USR' (user) or 'GIT' (from a deploy integration)."),
            column(
                "insight_id",
                pa.int64(),
                "Insight the annotation is attached to, for scope dashboard_item (an id, not a quantity).",
            ),
            column("deleted", pa.bool_(), "True for a soft-deleted annotation."),
            column("created_at", TSTZ, "When the annotation was created."),
            column("updated_at", TSTZ, "When the annotation was last edited."),
            column("created_by", pa.string(), "Email of the user who created the annotation."),
        ]
    )
    PATH: ClassVar[str] = "/api/projects/{project}/annotations/"

    class Meta:
        """Table metadata."""

        name = "annotations_scan"
        description = "Scan behind the annotations table"

    @classmethod
    def row(cls, item: dict[str, Any]) -> dict[str, Any]:
        """Map annotation fields."""
        return {**item, "insight_id": item.get("dashboard_item"), "created_by": _user(item.get("created_by"))}


STEP = pa.struct(
    [
        pa.field("event", pa.string()),
        pa.field("url", pa.string()),
        pa.field("url_matching", pa.string()),
        pa.field("selector", pa.string()),
        pa.field("text", pa.string()),
        pa.field("href", pa.string()),
        pa.field("tag_name", pa.string()),
    ]
)


class Actions(RestScan):
    """Rows for ``posthog.metadata.actions``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            column("id", pa.int64(), "Numeric identifier of the action (ids, not quantities).", nullable=False),
            column("name", pa.string(), "Action name."),
            column("description", pa.string(), "Action description."),
            column(
                "steps",
                pa.list_(STEP),
                "Match steps (any step matches): event, url, url_matching, selector, text, href, tag_name.",
            ),
            column("tags", STR_LIST, "Tags on the action."),
            column("deleted", pa.bool_(), "True for a soft-deleted action."),
            column("created_at", TSTZ, "When the action was created."),
            column("created_by", pa.string(), "Email of the user who created the action."),
        ]
    )
    PATH: ClassVar[str] = "/api/projects/{project}/actions/"

    class Meta:
        """Table metadata."""

        name = "actions_scan"
        description = "Scan behind the actions table"

    @classmethod
    def row(cls, item: dict[str, Any]) -> dict[str, Any]:
        """Map action fields."""
        return {**item, "tags": _tags(item.get("tags")), "created_by": _user(item.get("created_by"))}


class Surveys(RestScan):
    """Rows for ``posthog.metadata.surveys``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            column("id", pa.string(), "Survey UUID.", nullable=False),
            column("name", pa.string(), "Survey name."),
            column("description", pa.string(), "Survey description."),
            column("type", pa.string(), "popover, widget, api or external_survey."),
            column("linked_flag_key", pa.string(), "Flag that limits who sees the survey."),
            column("question_count", pa.int32(), "Number of questions (count)."),
            column("questions", pa.json_(), "Questions (type, text, choices), JSON."),
            column("start_date", TSTZ, "When the survey was launched; NULL for a draft."),
            column("end_date", TSTZ, "When the survey was stopped."),
            column("archived", pa.bool_(), "True for an archived survey."),
            column("created_at", TSTZ, "When the survey was created."),
            column("created_by", pa.string(), "Email of the user who created the survey."),
        ]
    )
    PATH: ClassVar[str] = "/api/projects/{project}/surveys/"

    class Meta:
        """Table metadata."""

        name = "surveys_scan"
        description = "Scan behind the surveys table"

    @classmethod
    def row(cls, item: dict[str, Any]) -> dict[str, Any]:
        """Map survey fields."""
        flag = item.get("linked_flag")
        questions = item.get("questions")
        return {
            **item,
            "linked_flag_key": flag.get("key") if isinstance(flag, dict) else None,
            "question_count": len(questions) if isinstance(questions, list) else None,
            "created_by": _user(item.get("created_by")),
        }


class GroupTypes(RestScan):
    """Rows for ``posthog.metadata.group_types``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            column(
                "group_type_index",
                pa.int32(),
                "Group type slot index, 0 to 4; matches main.groups.group_type_index.",
                nullable=False,
            ),
            column("group_type", pa.string(), "Group type key, e.g. 'company'.", nullable=False),
            column("name_singular", pa.string(), "Display name, singular."),
            column("name_plural", pa.string(), "Display name, plural."),
        ]
    )
    PATH: ClassVar[str] = "/api/projects/{project}/groups_types/"

    class Meta:
        """Table metadata."""

        name = "group_types_scan"
        description = "Scan behind the group_types table"

    @classmethod
    def row(cls, item: dict[str, Any]) -> dict[str, Any]:
        """Map group type fields."""
        return dict(item)
