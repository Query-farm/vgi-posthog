"""The ``main`` schema's HogQL-backed tables and the ``events_where`` function.

Each class names a HogQL table (``SOURCE``), maps every output column to a
HogQL expression (``COLS``) and the key that orders and pages the scan (``KEY``).
``scan.HogQLScan`` turns pushed-down predicates into the HogQL ``WHERE`` clause
(see ``pushdown.py``); the column kinds decide how constants are rendered.
"""

from __future__ import annotations

import datetime as dt
import json
from dataclasses import dataclass
from typing import Annotated, Any, ClassVar

import pyarrow as pa
from vgi.arguments import Arg
from vgi.table_function import ProcessParams

from .client import PostHogError
from .hogql import TSTZ, HogQLBuilder, ident
from .meta import column
from .pushdown import Col
from .scan import HogQLScan, NoArgs

EVENT_FIELDS = [
    column("uuid", pa.string(), "Event UUID, unique per event.", nullable=False),
    column("event", pa.string(), "Event name, e.g. '$pageview' or 'signed_up'.", nullable=False),
    column("timestamp", TSTZ, "When the event happened (UTC).", nullable=False),
    column("distinct_id", pa.string(), "Distinct id the event was captured with.", nullable=False),
    column("person_id", pa.string(), "UUID of the person the distinct id resolves to."),
    column("session_id", pa.string(), "Session id ($session_id property); NULL for events outside a session."),
    column("current_url", pa.string(), "Page URL ($current_url property) for web events."),
    column("properties", pa.json_(), "All event properties as a JSON object."),
    column("elements_chain", pa.string(), "Autocapture element chain; empty for non-autocapture events."),
    column("created_at", TSTZ, "When PostHog ingested the event (UTC)."),
]

EVENT_COLS = {
    "uuid": Col("uuid", "uuid"),
    "event": Col("event"),
    "timestamp": Col("timestamp", "ts"),
    "distinct_id": Col("distinct_id"),
    "person_id": Col("person_id", "uuid"),
    "session_id": Col("properties.$session_id"),
    "current_url": Col("properties.$current_url"),
    "properties": Col("properties", "json"),
    "elements_chain": Col("elements_chain"),
    "created_at": Col("created_at", "ts"),
}


class Events(HogQLScan):
    """Rows for ``posthog.main.events``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(EVENT_FIELDS)
    SOURCE: ClassVar[str] = "events"
    COLS: ClassVar[dict[str, Col]] = EVENT_COLS
    KEY: ClassVar[list[str]] = ["timestamp", "uuid"]

    class Meta:
        """Table metadata."""

        name = "events_scan"
        description = "Scan behind the events table"
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True


@dataclass(slots=True, frozen=True, kw_only=True)
class EventsWhereArgs:
    """Arguments for ``events_where(...)``; every one is optional and pushed into HogQL."""

    event: Annotated[str | None, Arg("event", default=None, doc="Event name to match exactly, e.g. '$pageview'.")]
    properties: Annotated[
        str | None,
        Arg(
            "properties",
            default=None,
            doc=(
                "Property equalities as one JSON object mapping property key to value (for instance plan to pro); "
                "values are compared as text and a JSON null matches a missing property."
            ),
        ),
    ]
    distinct_id: Annotated[str | None, Arg("distinct_id", default=None, doc="Distinct id to match exactly.")]
    person_id: Annotated[
        str | None, Arg("person_id", default=None, doc="Person id to match exactly (the persons.id value).")
    ]
    after: Annotated[
        dt.datetime | None,
        Arg("after", default=None, doc="Only events at or after this moment (inclusive).", arrow_type=TSTZ),
    ]
    before: Annotated[
        dt.datetime | None,
        Arg("before", default=None, doc="Only events strictly before this moment (exclusive).", arrow_type=TSTZ),
    ]
    max_rows: Annotated[
        int | None,
        Arg(
            "max_rows",
            default=None,
            doc="Stop after this many events (sent to PostHog as LIMIT); NULL reads all.",
            ge=1,
        ),
    ]
    newest_first: Annotated[
        bool,
        Arg("newest_first", default=False, doc="Page from the newest event backwards (with max_rows: the latest N)."),
    ]


def property_clauses(raw: str | None, builder: HogQLBuilder) -> list[str]:
    """Translate ``'{"key": value, ...}'`` into ``properties.key = {p}`` clauses.

    Args:
        raw: JSON object text, or None.
        builder: Collects placeholder values.

    Returns:
        One HogQL clause per key.

    Raises:
        PostHogError: Not a JSON object, or a key that cannot be expressed safely.

    """
    if raw is None or raw.strip() == "":
        return []
    try:
        parsed = json.loads(raw)
    except ValueError as err:
        raise PostHogError(f"events_where: properties must be a JSON object, got {raw[:80]!r}") from err
    if not isinstance(parsed, dict):
        raise PostHogError('events_where: properties must be a JSON object such as \'{"plan": "pro"}\'')
    out = []
    for key, value in parsed.items():
        name = ident(str(key))
        if name is None:
            raise PostHogError(
                f"events_where: property name {key!r} contains characters that cannot be used in a HogQL "
                "identifier (backtick, backslash, quotes, braces); filter it in SQL instead"
            )
        if value is None:
            out.append(f"properties.{name} IS NULL")
            continue
        if isinstance(value, bool):
            text = "true" if value else "false"
        elif isinstance(value, int | float | str):
            text = str(value)
        else:
            text = json.dumps(value, separators=(",", ":"))
        out.append(f"properties.{name} = {builder.param(text)}")
    return out


class EventsWhere(Events):
    """``events_where(...)``: events with property equality and a row cap pushed into HogQL."""

    FunctionArguments = EventsWhereArgs

    class Meta:
        """Function metadata (tags and examples are set in ``worker.py``)."""

        name = "events_where"
        description = "Events filtered in HogQL by name, properties, ids and time, with an optional row cap"
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags: ClassVar[dict[str, str]] = {}
        examples: ClassVar[list[Any]] = []

    @classmethod
    def extra_clauses(cls, params: ProcessParams[Any], builder: HogQLBuilder) -> list[str]:
        """Clauses from the function's arguments."""
        args: EventsWhereArgs = params.args
        out: list[str] = []
        if args.event is not None:
            out.append(f"event = {builder.param(args.event)}")
        if args.distinct_id is not None:
            out.append(f"distinct_id = {builder.param(args.distinct_id)}")
        if args.person_id is not None:
            out.append(f"toString(person_id) = {builder.param(args.person_id)}")
        if args.after is not None:
            out.append(f"timestamp >= toDateTime({builder.param(args.after)})")
        if args.before is not None:
            out.append(f"timestamp < toDateTime({builder.param(args.before)})")
        out.extend(property_clauses(args.properties, builder))
        return out

    @classmethod
    def max_rows(cls, params: ProcessParams[Any]) -> int | None:
        """Return the ``max_rows`` argument."""
        value: int | None = params.args.max_rows
        return value

    @classmethod
    def descending(cls, params: ProcessParams[Any]) -> bool:
        """Return the ``newest_first`` argument."""
        return bool(params.args.newest_first)


class Persons(HogQLScan):
    """Rows for ``posthog.main.persons``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            column("id", pa.string(), "Person UUID.", nullable=False),
            column("created_at", TSTZ, "When the person was first seen (UTC)."),
            column("is_identified", pa.bool_(), "True once the person was identified (posthog.identify)."),
            column("properties", pa.json_(), "Person properties as a JSON object."),
        ]
    )
    SOURCE: ClassVar[str] = "persons"
    COLS: ClassVar[dict[str, Col]] = {
        "id": Col("id", "uuid"),
        "created_at": Col("created_at", "ts"),
        "is_identified": Col("is_identified", "bool"),
        "properties": Col("properties", "json"),
    }
    KEY: ClassVar[list[str]] = ["id"]

    class Meta:
        """Table metadata."""

        name = "persons_scan"
        description = "Scan behind the persons table"
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True


class PersonDistinctIds(HogQLScan):
    """Rows for ``posthog.main.person_distinct_ids``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            column("distinct_id", pa.string(), "A distinct id seen in events.", nullable=False),
            column("person_id", pa.string(), "UUID of the person it currently resolves to.", nullable=False),
        ]
    )
    SOURCE: ClassVar[str] = "person_distinct_ids"
    COLS: ClassVar[dict[str, Col]] = {"distinct_id": Col("distinct_id"), "person_id": Col("person_id", "uuid")}
    KEY: ClassVar[list[str]] = ["distinct_id"]

    class Meta:
        """Table metadata."""

        name = "person_distinct_ids_scan"
        description = "Scan behind the person_distinct_ids table"
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True


class Sessions(HogQLScan):
    """Rows for ``posthog.main.sessions``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            column("session_id", pa.string(), "Session id (UUIDv7 text).", nullable=False),
            column("distinct_id", pa.string(), "Distinct id of the session's first event."),
            column("start_timestamp", TSTZ, "First event of the session (UTC)."),
            column("end_timestamp", TSTZ, "Last event of the session (UTC)."),
            column("duration_seconds", pa.int64(), "end_timestamp - start_timestamp in seconds."),
            column("entry_current_url", pa.string(), "URL of the first pageview."),
            column("exit_current_url", pa.string(), "URL of the last pageview."),
            column("entry_referring_domain", pa.string(), "Referring domain of the first pageview."),
            column("entry_utm_source", pa.string(), "utm_source of the first pageview."),
            column("entry_utm_medium", pa.string(), "utm_medium of the first pageview."),
            column("entry_utm_campaign", pa.string(), "utm_campaign of the first pageview."),
            column("channel_type", pa.string(), "PostHog channel type, e.g. 'Direct', 'Organic Search'."),
            column("pageview_count", pa.int64(), "Number of $pageview events (count)."),
            column("autocapture_count", pa.int64(), "Number of $autocapture events (count)."),
            column("is_bounce", pa.bool_(), "True for a single-pageview session shorter than 10 seconds."),
        ]
    )
    SOURCE: ClassVar[str] = "sessions"
    COLS: ClassVar[dict[str, Col]] = {
        "session_id": Col("session_id"),
        "distinct_id": Col("distinct_id"),
        "start_timestamp": Col("$start_timestamp", "ts"),
        "end_timestamp": Col("$end_timestamp", "ts"),
        "duration_seconds": Col("$session_duration", "int"),
        "entry_current_url": Col("$entry_current_url"),
        "exit_current_url": Col("$exit_current_url"),
        "entry_referring_domain": Col("$entry_referring_domain"),
        "entry_utm_source": Col("$entry_utm_source"),
        "entry_utm_medium": Col("$entry_utm_medium"),
        "entry_utm_campaign": Col("$entry_utm_campaign"),
        "channel_type": Col("$channel_type"),
        "pageview_count": Col("$pageview_count", "int"),
        "autocapture_count": Col("$autocapture_count", "int"),
        "is_bounce": Col("$is_bounce", "bool"),
    }
    KEY: ClassVar[list[str]] = ["session_id"]

    class Meta:
        """Table metadata."""

        name = "sessions_scan"
        description = "Scan behind the sessions table"
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True


class Groups(HogQLScan):
    """Rows for ``posthog.main.groups``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            column(
                "group_type_index",
                pa.int32(),
                "Group type slot index, 0 to 4; names in metadata.group_types.",
                nullable=False,
            ),
            column("group_key", pa.string(), "The group's key, e.g. a company id.", nullable=False),
            column("created_at", TSTZ, "When the group was first seen (UTC)."),
            column("properties", pa.json_(), "Group properties as a JSON object."),
        ]
    )
    SOURCE: ClassVar[str] = "groups"
    COLS: ClassVar[dict[str, Col]] = {
        "group_type_index": Col("`index`", "int"),
        "group_key": Col("`key`"),
        "created_at": Col("created_at", "ts"),
        "properties": Col("properties", "json"),
    }
    KEY: ClassVar[list[str]] = ["group_type_index", "group_key"]

    class Meta:
        """Table metadata."""

        name = "groups_scan"
        description = "Scan behind the groups table"
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True


__all__ = ["Events", "EventsWhere", "EventsWhereArgs", "Groups", "NoArgs", "PersonDistinctIds", "Persons", "Sessions"]
