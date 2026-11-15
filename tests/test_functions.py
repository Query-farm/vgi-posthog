"""Unit tests for HogQL building, type mapping, pushdown and the client. SQL tests live in test/sql."""

from __future__ import annotations

import datetime as dt
import decimal
import json

import pyarrow as pa
import pytest
from vgi.table_filter_pushdown import (
    AndFilter,
    ComparisonOp,
    ConstantFilter,
    InFilter,
    IsNullFilter,
    OrFilter,
    PushdownFilters,
)

from posthog_worker.client import PostHogClient, PostHogConfig, PostHogError, retry_delay
from posthog_worker.hogql import TSTZ, HogQLBuilder, arrow_type, convert, ident, iso_utc
from posthog_worker.pushdown import Col, translate
from posthog_worker.scan import keyset_clause
from posthog_worker.tables import EVENT_COLS, property_clauses

KEY = "phx_unit_test_key"


def _const(col: str, op: ComparisonOp, value: object, type_: pa.DataType | None = None) -> ConstantFilter:
    return ConstantFilter(column_name=col, column_index=0, op=op, value=pa.scalar(value, type=type_))


# ---------------------------------------------------------------------- identifiers


def test_ident_plain_quoted_and_refused() -> None:
    assert ident("plan") == "plan"
    assert ident("$browser") == "$browser"
    assert ident("utm source") == "`utm source`"
    assert ident("key") == "`key`"  # keyword
    for bad in ("a`b", "a\\b", "x'y", "{p}", "a\nb", ""):
        assert ident(bad) is None


def test_builder_placeholders_never_inline_values() -> None:
    b = HogQLBuilder()
    p = b.param("pro' OR 1=1 --")
    assert p == "{vgi_p0}"
    assert b.values == {"vgi_p0": "pro' OR 1=1 --"}
    assert b.param(dt.datetime(2026, 11, 1, 5, tzinfo=dt.timezone(dt.timedelta(hours=-5)))) == "{vgi_p1}"
    assert b.values["vgi_p1"] == "2026-11-01 10:00:00.000000"
    assert iso_utc(dt.date(2026, 11, 2)) == "2026-11-02 00:00:00.000000"


# ---------------------------------------------------------------------------- types


@pytest.mark.parametrize(
    ("ch", "arrow", "nullable"),
    [
        ("String", pa.string(), False),
        ("Nullable(String)", pa.string(), True),
        ("LowCardinality(Nullable(String))", pa.string(), True),
        ("UInt64", pa.uint64(), False),
        ("Int8", pa.int8(), False),
        ("Float32", pa.float64(), False),
        ("Bool", pa.bool_(), False),
        ("Date", pa.date32(), False),
        ("DateTime64(6, 'UTC')", TSTZ, False),
        ("DateTime('America/New_York')", TSTZ, False),
        ("Decimal(18, 4)", pa.decimal128(18, 4), False),
        ("Decimal64(2)", pa.decimal128(18, 2), False),
        ("Array(Nullable(String))", pa.list_(pa.string()), False),
        ("Map(String, Array(UInt8))", pa.map_(pa.string(), pa.list_(pa.uint8())), False),
        ("UUID", pa.string(), False),
        ("Enum8('a' = 1, 'b' = 2)", pa.string(), False),
        ("Nothing", pa.string(), True),
    ],
)
def test_arrow_type(ch: str, arrow: pa.DataType, nullable: bool) -> None:
    assert arrow_type(ch) == (arrow, nullable)


def test_arrow_type_tuples() -> None:
    named, _ = arrow_type("Tuple(label String, rank UInt8)")
    assert named == pa.struct([pa.field("label", pa.string()), pa.field("rank", pa.uint8())])
    unnamed, _ = arrow_type("Tuple(String, DateTime64(3, 'UTC'))")
    assert [unnamed.field(i).name for i in range(2)] == ["_1", "_2"]


def test_convert_values() -> None:
    assert convert("2026-11-02T09:30:00.250000-05:00", TSTZ) == dt.datetime(2026, 11, 2, 14, 30, 0, 250000, dt.UTC)
    assert convert("2026-11-02T09:30:00Z", TSTZ) == dt.datetime(2026, 11, 2, 9, 30, tzinfo=dt.UTC)
    assert convert("12.5000", pa.decimal128(18, 4)) == decimal.Decimal("12.5000")
    assert convert(["first", 1], arrow_type("Tuple(label String, rank UInt8)")[0]) == {"label": "first", "rank": 1}
    assert convert({"k": "v"}, pa.map_(pa.string(), pa.string())) == [("k", "v")]
    assert convert({"a": 1}, pa.json_()) == '{"a": 1}'
    assert convert('{"a": 1}', pa.json_()) == '{"a": 1}'
    assert convert({"a": 1}, pa.string()) == '{"a": 1}'
    assert convert("true", pa.bool_()) is True
    assert convert(None, pa.int64()) is None
    assert convert("not a date", TSTZ) is None


# -------------------------------------------------------------------------- pushdown


def test_translate_events_filters() -> None:
    b = HogQLBuilder()
    filters = PushdownFilters(
        filters=(
            _const("event", ComparisonOp.EQ, "purchase"),
            _const("timestamp", ComparisonOp.GE, dt.datetime(2026, 11, 1, tzinfo=dt.UTC), TSTZ),
            InFilter(column_name="distinct_id", column_index=0, values=pa.array(["user-1", "user-2"])),
            _const("person_id", ComparisonOp.EQ, "01890000-0000-7000-8000-000000000001"),
            IsNullFilter(column_name="session_id", column_index=0),
        )
    )
    tr = translate(filters, EVENT_COLS, b)
    assert tr.exact
    assert tr.clauses == [
        "event = {vgi_p0}",
        "timestamp >= toDateTime({vgi_p1})",
        "distinct_id IN ({vgi_p2}, {vgi_p3})",
        "toString(person_id) = {vgi_p4}",
        "properties.$session_id IS NULL",
    ]
    assert b.values["vgi_p1"] == "2026-11-01 00:00:00.000000"


def test_translate_or_and_untranslatable() -> None:
    b = HogQLBuilder()
    either = OrFilter(
        column_name="event",
        column_index=0,
        children=(_const("event", ComparisonOp.EQ, "a"), _const("event", ComparisonOp.EQ, "b")),
    )
    on_json = _const("properties", ComparisonOp.EQ, "{}")
    nested = AndFilter(column_name="event", column_index=0, children=(on_json, _const("event", ComparisonOp.NE, "c")))
    tr = translate(PushdownFilters(filters=(either, nested)), EVENT_COLS, b)
    assert tr.clauses == ["(event = {vgi_p0} OR event = {vgi_p1})", "event != {vgi_p2}"]
    assert not tr.exact  # the JSON predicate stays with DuckDB


def test_translate_rejects_mismatched_constant() -> None:
    tr = translate(
        PushdownFilters(filters=(_const("n", ComparisonOp.EQ, "x"),)), {"n": Col("n", "int")}, HogQLBuilder()
    )
    assert tr.clauses == [] and not tr.exact


def test_keyset_clause() -> None:
    b = HogQLBuilder()
    key = [("timestamp", Col("timestamp", "ts")), ("uuid", Col("uuid", "uuid"))]
    clause = keyset_clause(key, ["2026-11-02T10:00:00Z", "0193a"], b)
    assert clause == (
        "(timestamp > toDateTime({vgi_p0}) OR (timestamp = toDateTime({vgi_p1}) AND uuid > toUUID({vgi_p2})))"
    )
    assert keyset_clause([("id", Col("id"))], ["x"], HogQLBuilder(), "<") == "id < {vgi_p0}"


def test_property_clauses() -> None:
    b = HogQLBuilder()
    clauses = property_clauses('{"plan": "pro", "amount": 49, "is_trial": true, "utm source": null}', b)
    assert clauses == [
        "properties.plan = {vgi_p0}",
        "properties.amount = {vgi_p1}",
        "properties.is_trial = {vgi_p2}",
        "properties.`utm source` IS NULL",
    ]
    assert b.values == {"vgi_p0": "pro", "vgi_p1": "49", "vgi_p2": "true"}
    with pytest.raises(PostHogError):
        property_clauses('{"a`b": 1}', HogQLBuilder())
    with pytest.raises(PostHogError):
        property_clauses("[1]", HogQLBuilder())


# ---------------------------------------------------------------------------- client


def test_config_validation() -> None:
    cfg = PostHogConfig.from_secret({"personal_api_key": pa.scalar(KEY), "project_id": pa.scalar("42")})
    assert cfg.host == "https://us.posthog.com" and cfg.project_id == "42"
    assert KEY not in repr(cfg)
    with pytest.raises(PostHogError, match="CREATE SECRET"):
        PostHogConfig.from_secret(None)
    with pytest.raises(PostHogError, match="PROJECT_ID"):
        PostHogConfig.from_secret({"personal_api_key": KEY, "project_id": "my-project"})
    with pytest.raises(PostHogError, match="HOST"):
        PostHogConfig.from_secret({"personal_api_key": KEY, "host": "eu.posthog.com"})


def test_same_origin_rebases_next_links() -> None:
    client = PostHogClient(PostHogConfig(host="https://ph.example.com/posthog", api_key=KEY))
    assert (
        client.same_origin("http://internal:8000/posthog/api/projects/1/cohorts/?offset=2")
        == "https://ph.example.com/posthog/api/projects/1/cohorts/?offset=2"
    )
    assert client.same_origin("/api/x/?a=1") == "https://ph.example.com/posthog/api/x/?a=1"


def test_retry_delay() -> None:
    assert retry_delay("3", 1) == 3.0
    assert retry_delay("999", 1) == 60.0
    assert retry_delay(None, 2) == 2.0


def test_unwrap_shapes() -> None:
    done = {"columns": ["n"], "types": [["n", "UInt64"]], "results": [[1]], "hasMore": False}
    assert PostHogClient._unwrap(done) == done
    assert PostHogClient._unwrap({"query_status": {"id": "q", "complete": False}}) is None
    assert PostHogClient._unwrap({"query_status": {"id": "q", "complete": True, "results": done}}) == done
    with pytest.raises(PostHogError, match="boom"):
        PostHogClient._unwrap({"query_status": {"complete": True, "error": True, "error_message": "boom"}})
    result = PostHogClient._result({"columns": ["a", "b"], "types": ["String", "Int64"], "results": []})
    assert result.types == ["String", "Int64"]


def test_query_against_mock(mock_api: str) -> None:
    """Async refresh is polled to completion; values travel as placeholders; OFFSET is refused."""
    client = PostHogClient(PostHogConfig(host=mock_api, api_key="phx_fixture_key", project_id="4242"))
    result = client.query(
        "SELECT event, count() AS n FROM events WHERE event = {ev} GROUP BY event",
        {"ev": "purchase"},
        refresh="force_async",
    )
    assert result.columns == ["event", "n"]
    assert result.results == [["purchase", 3]]
    with pytest.raises(PostHogError, match="HTTP 400"):
        client.query("SELECT event FROM events LIMIT 1 OFFSET 1")
    bad = PostHogClient(PostHogConfig(host=mock_api, api_key="phx_wrong", project_id="4242"))
    with pytest.raises(PostHogError, match="HTTP 401"):
        bad.query("SELECT 1")
    log = json.loads(__import__("urllib.request").request.urlopen(f"{mock_api}/__requests").read())
    polls = [e for e in log if e["method"] == "GET" and "/query/" in e["path"]]
    assert polls, "the async query was never polled"
