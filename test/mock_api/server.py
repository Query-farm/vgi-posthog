#!/usr/bin/env python3
"""An offline stand-in for PostHog's Query API and REST API, used by the tests.

PostHog self-hosting is far too heavy for CI, so this server replays the shapes
PostHog documents, built from ``test/fixtures/posthog.json``:

* ``POST /api/projects/<id>/query/`` takes ``{"query": {"kind": "HogQLQuery",
  "query": ..., "values": {...}}, "refresh": ...}`` and *evaluates* the HogQL.
  The worker's HogQL is a small, regular subset (column lists, ``properties.x``,
  ``toDateTime({p})``, ``IN``, ``AND``/``OR``, ``ORDER BY``, ``LIMIT``), which is
  rewritten to SQLite and run against the fixture tables, so a wrong pushdown
  returns wrong rows, not just a wrong log line. The response carries
  ``columns``, ``types`` (``[name, ClickHouse type]`` pairs), ``results`` and
  ``hasMore``, like ``HogQLQueryResponse``.
  Like PostHog, a query without LIMIT returns at most 100 rows, LIMIT is
  clamped to 50,000, and ``OFFSET`` is refused with HTTP 400.
* ``refresh`` = ``async`` / ``force_async`` / ``lazy_async`` answers with
  ``{"query_status": {"id": ..., "complete": false}}``; ``GET
  /api/projects/<id>/query/<query_id>/`` reports it incomplete once, then
  returns ``query_status.results``.
* Canned responses (``canned`` in the fixture) replay a recorded column/type
  set (arrays, maps, tuples, decimals, nullable timestamps) when their query
  text appears in a request; the canned rows are loaded into a table so the
  worker's wrapper (projection, WHERE, LIMIT) still evaluates.
* ``GET /api/projects/`` and ``GET /api/projects/<id>/<resource>/`` serve the
  REST listings with DRF ``limit``/``offset`` pagination and absolute ``next``
  links. Cohorts deliberately return ``next`` links on a foreign host, which a
  client must rebase rather than follow.

Every request must send ``Authorization: Bearer <MOCK_API_KEY>`` (default
``phx_fixture_key``); the project id must be 4242. ``MOCK_FLAKY=1`` makes the
first request to every (method, path) answer 429 with ``Retry-After: 0`` and
PostHog's ``api_queries_budget_exceeded`` body.

Test hooks: ``GET /__requests`` returns the request log (seq, method, path,
query string, HogQL, values, refresh, status); ``GET /__reset`` clears it.

Usage::

    python3 test/mock_api/server.py --fixture test/fixtures/posthog.json   # prints PORT:<n>
"""

from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import json
import os
import re
import sqlite3
import sys
import threading
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse

PROJECT = "4242"
API_KEY = os.environ.get("MOCK_API_KEY", "phx_fixture_key")
FLAKY = os.environ.get("MOCK_FLAKY") == "1"
DEFAULT_LIMIT = 100
MAX_LIMIT = 50_000
TS_TYPE = "DateTime64(6, 'UTC')"

KNOWN_TYPES = {
    "uuid": "UUID",
    "event": "String",
    "timestamp": TS_TYPE,
    "distinct_id": "String",
    "person_id": "UUID",
    "properties": "String",
    "elements_chain": "String",
    "created_at": TS_TYPE,
    "id": "UUID",
    "is_identified": "Bool",
    "session_id": "String",
    "$start_timestamp": TS_TYPE,
    "$end_timestamp": TS_TYPE,
    "$session_duration": "Int64",
    "$pageview_count": "UInt64",
    "$autocapture_count": "UInt64",
    "$is_bounce": "Bool",
    "index": "Int8",
    "key": "String",
}
FUNCS = {
    "todatetime": "ph_dt",
    "touuid": "ph_same",
    "tostring": "ph_same",
    "todate": "ph_date",
}

LOCK = threading.Lock()
LOG: list[dict[str, Any]] = []
SEEN: set[tuple[str, str]] = set()
JOBS: dict[str, dict[str, Any]] = {}
FIXTURE: dict[str, Any] = {}
DB: sqlite3.Connection | None = None


# ----------------------------------------------------------------------------- SQLite


def _norm_ts(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().replace("T", " ").replace("Z", "+00:00")
    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo:
        parsed = parsed.astimezone(dt.UTC).replace(tzinfo=None)
    return parsed.strftime("%Y-%m-%d %H:%M:%S.%f")


def _prop(props: str | None, key: str) -> str | None:
    if props is None:
        return None
    try:
        value = json.loads(props).get(key)
    except ValueError, AttributeError:
        return None
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        return value
    return json.dumps(value)


def _setup_db() -> sqlite3.Connection:
    con = sqlite3.connect(":memory:", check_same_thread=False)
    con.create_function("ph_dt", 1, _norm_ts, deterministic=True)
    con.create_function("ph_date", 1, lambda v: None if v is None else str(v)[:10], deterministic=True)
    con.create_function("ph_same", 1, lambda v: v, deterministic=True)
    con.create_function("ph_prop", 2, _prop, deterministic=True)
    for name, table in FIXTURE["hogql_tables"].items():
        cols = table["columns"]
        con.execute(f'CREATE TABLE "{name}" ({", ".join(f"{_q(c)}" for c in cols)})')
        rows = []
        for row in table["rows"]:
            values = []
            for c, v in zip(cols, row, strict=True):
                if KNOWN_TYPES.get(c) == TS_TYPE:
                    v = _norm_ts(v)
                elif isinstance(v, dict | list):
                    v = json.dumps(v)
                values.append(v)
            rows.append(values)
        con.executemany(f'INSERT INTO "{name}" VALUES ({", ".join("?" * len(cols))})', rows)
    for i, canned in enumerate(FIXTURE.get("canned", [])):
        cols = [c for c, _ in canned["types"]]
        con.execute(f"CREATE TABLE canned_{i} ({', '.join(_q(c) for c in cols)})")
        con.executemany(
            f"INSERT INTO canned_{i} VALUES ({', '.join('?' * len(cols))})",
            [[_cell(v) for v in row] for row in canned["results"]],
        )
    return con


def _cell(value: Any) -> Any:
    """Store nested values as JSON text and integers beyond SQLite's range as text."""
    if isinstance(value, dict | list):
        return json.dumps(value)
    if isinstance(value, int) and not isinstance(value, bool) and abs(value) > 2**63 - 1:
        return str(value)
    return value


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


class HogQLError(ValueError):
    """The mock cannot run (or PostHog would refuse) the query."""


def to_sqlite(hogql: str) -> str:
    """Rewrite the HogQL subset the worker emits into SQLite SQL."""
    for i, canned in enumerate(FIXTURE.get("canned", [])):
        hogql = hogql.replace(f"({canned['query']})", f"canned_{i}")
        if hogql.strip() == canned["query"]:
            hogql = f"SELECT * FROM canned_{i}"
    out: list[str] = []
    i = 0
    n = len(hogql)
    saw_limit = False
    while i < n:
        ch = hogql[i]
        if ch == "'":
            j = i + 1
            while j < n and hogql[j] != "'":
                j += 2 if hogql[j] == "\\" else 1
            out.append(hogql[i : j + 1])
            i = j + 1
        elif ch == "{":
            j = hogql.index("}", i)
            out.append(":" + hogql[i + 1 : j])
            i = j + 1
        elif ch == "`":
            j = hogql.index("`", i + 1)
            out.append(_q(hogql[i + 1 : j]))
            i = j + 1
        elif ch.isalpha() or ch in "_$":
            j = i
            while j < n and (hogql[j].isalnum() or hogql[j] in "_$"):
                j += 1
            word = hogql[i:j]
            if word == "properties" and j < n and hogql[j] == ".":
                k = j + 1
                if k < n and hogql[k] == "`":
                    end = hogql.index("`", k + 1)
                    key, j = hogql[k + 1 : end], end + 1
                else:
                    end = k
                    while end < n and (hogql[end].isalnum() or hogql[end] in "_$"):
                        end += 1
                    key, j = hogql[k:end], end
                out.append(f"ph_prop(properties, '{key}')")
            elif word.upper() == "OFFSET":
                raise HogQLError("OFFSET is not supported for API queries; use keyset pagination")
            elif word.lower() in FUNCS and j < n and hogql[j] == "(":
                out.append(FUNCS[word.lower()])
            elif word.lower() == "count" and hogql[j : j + 2] == "()":
                out.append("count(*)")
                j += 2
            elif word.startswith("$"):
                out.append(_q(word))
            else:
                if word.upper() == "LIMIT":
                    saw_limit = True
                out.append(word)
            i = j
        else:
            out.append(ch)
            i += 1
    sql = "".join(out)
    if not saw_limit:
        sql += f" LIMIT {DEFAULT_LIMIT}"

    def clamp(m: re.Match[str]) -> str:
        return f"LIMIT {min(int(m.group(1)), MAX_LIMIT)}"

    return re.sub(r"LIMIT (\d+)", clamp, sql)


def _ch_type(name: str, values: list[Any]) -> str:
    if name in KNOWN_TYPES:
        base = KNOWN_TYPES[name]
    else:
        sample = next((v for v in values if v is not None), None)
        if isinstance(sample, bool):
            base = "Bool"
        elif isinstance(sample, int):
            base = "UInt64" if name.startswith("count") else "Int64"
        elif isinstance(sample, float):
            base = "Float64"
        elif sample is None:
            return "Nullable(Nothing)"
        else:
            base = "String"
    return f"Nullable({base})" if any(v is None for v in values) else base


def _out_value(value: Any, ch_type: str) -> Any:
    inner = ch_type.removeprefix("Nullable(").removesuffix(")") if ch_type.startswith("Nullable(") else ch_type
    if value is None:
        return None
    if inner.startswith("DateTime") and isinstance(value, str) and " " in value:
        return value.replace(" ", "T") + "Z"
    if inner == "Bool":
        return bool(value)
    if inner.startswith(("UInt", "Int")) and isinstance(value, str):
        return int(value)
    if inner.startswith(("Array", "Map", "Tuple")) and isinstance(value, str):
        return json.loads(value)
    return value


def run_hogql(hogql: str, values: dict[str, Any]) -> dict[str, Any]:
    """Evaluate one HogQL query; return a HogQLQueryResponse-shaped dict."""
    assert DB is not None
    sql = to_sqlite(hogql)
    params = {k: (int(v) if isinstance(v, bool) else v) for k, v in values.items()}
    with LOCK:
        try:
            cur = DB.execute(sql, params)
            rows = [list(r) for r in cur.fetchall()]
            names = [d[0] for d in cur.description]
            sample = rows
            if not rows and re.search(r"LIMIT 0\s*$", sql):
                sample = [list(r) for r in DB.execute(re.sub(r"LIMIT 0\s*$", "LIMIT 1000", sql), params).fetchall()]
        except sqlite3.Error as err:
            raise HogQLError(f"{err} (sqlite: {sql})") from err
    names = [re.sub(r"^ph_prop\(properties, '(.*)'\)$", r"\1", c).replace("count(*)", "count()") for c in names]
    canned_types: dict[str, str] = {}
    for canned in FIXTURE.get("canned", []):
        if canned["query"] in hogql:
            canned_types = dict(canned["types"])
    types = [canned_types.get(c) or _ch_type(c, [r[i] for r in sample]) for i, c in enumerate(names)]
    results = [[_out_value(v, t) for v, t in zip(r, types, strict=True)] for r in rows]
    limit = re.findall(r"LIMIT (\d+)", sql)
    return {
        "columns": names,
        "types": [[c, t] for c, t in zip(names, types, strict=True)],
        "results": results,
        "hasMore": bool(limit) and len(rows) == int(limit[-1]) and int(limit[-1]) > 0,
        "hogql": hogql,
        "is_cached": False,
    }


# ----------------------------------------------------------------------------- HTTP


class Handler(BaseHTTPRequestHandler):
    """Serve the Query API, the REST listings and the test hooks."""

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002 - stdlib signature
        """Keep the test output quiet."""

    def _json(self, status: int, body: Any, headers: dict[str, str] | None = None) -> None:
        raw = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(raw)

    def _record(self, status: int, **extra: Any) -> None:
        url = urlparse(self.path)
        with LOCK:
            LOG.append(
                {
                    "seq": len(LOG) + 1,
                    "method": self.command,
                    "path": url.path,
                    "query_string": url.query,
                    "hogql": extra.get("hogql"),
                    "values": json.dumps(extra.get("values")) if extra.get("values") is not None else None,
                    "refresh": extra.get("refresh"),
                    "status": status,
                }
            )

    def _gate(self, **extra: Any) -> bool:
        """Auth, project and flaky checks; True when the request may proceed."""
        url = urlparse(self.path)
        if self.headers.get("Authorization") != f"Bearer {API_KEY}":
            self._record(401, **extra)
            self._json(401, {"type": "authentication_error", "detail": "Invalid personal API key."})
            return False
        m = re.match(r"^/api/projects/([^/]+)/", url.path)
        if m and m.group(1) != PROJECT:
            self._record(404, **extra)
            self._json(404, {"type": "invalid_request", "detail": "Not found."})
            return False
        if FLAKY:
            key = (self.command, url.path)
            with LOCK:
                first = key not in SEEN
                SEEN.add(key)
            if first:
                self._record(429, **extra)
                self._json(
                    429,
                    {"type": "throttled_error", "code": "api_queries_budget_exceeded", "detail": "Slow down."},
                    {"Retry-After": "0"},
                )
                return False
        return True

    def do_GET(self) -> None:
        """REST listings, async query polls and test hooks."""
        url = urlparse(self.path)
        if url.path == "/__requests":
            with LOCK:
                self._json(200, list(LOG) or [{"seq": 0, "method": None, "path": None}])
            return
        if url.path == "/__reset":
            with LOCK:
                LOG.clear()
            self._json(200, [{"reset": True}])
            return
        if not self._gate():
            return
        m = re.match(rf"^/api/projects/{PROJECT}/query/([^/]+)/$", url.path)
        if m:
            job = JOBS.get(m.group(1))
            if job is None:
                self._record(404)
                self._json(404, {"detail": "Query not found."})
                return
            job["polls"] += 1
            self._record(200)
            status: dict[str, Any] = {"id": m.group(1), "team_id": int(PROJECT), "complete": job["polls"] >= 2}
            if status["complete"]:
                status.update(job["status"])
            self._json(200, {"query_status": status})
            return
        if url.path == "/api/projects/":
            self._list(FIXTURE["rest"]["projects"], url)
            return
        m = re.match(rf"^/api/projects/{PROJECT}/([a-z_]+)/$", url.path)
        if m and m.group(1) in FIXTURE["rest"]:
            items = FIXTURE["rest"][m.group(1)]
            if m.group(1) == "groups_types":
                self._record(200)
                self._json(200, items)
                return
            self._list(items, url, foreign_next=m.group(1) == "cohorts")
            return
        self._record(404)
        self._json(404, {"detail": "Not found."})

    def _list(self, items: list[Any], url: Any, *, foreign_next: bool = False) -> None:
        qs = parse_qs(url.query)
        # Like a DRF max_limit: the mock serves at most 2 items per page to exercise paging.
        limit = min(int(qs.get("limit", ["100"])[0]), 2)
        offset = int(qs.get("offset", ["0"])[0])
        page = items[offset : offset + limit]
        nxt = None
        if offset + limit < len(items):
            host = "http://posthog-web.internal:8000" if foreign_next else f"http://{self.headers.get('Host')}"
            nxt = f"{host}{url.path}?{urlencode({'limit': limit, 'offset': offset + limit})}"
        self._record(200)
        self._json(200, {"count": len(items), "next": nxt, "previous": None, "results": page})

    def do_POST(self) -> None:
        """The Query API."""
        url = urlparse(self.path)
        length = int(self.headers.get("Content-Length") or 0)
        try:
            body = json.loads(self.rfile.read(length) or b"{}")
        except ValueError:
            self._json(400, {"detail": "invalid JSON"})
            return
        query = body.get("query") or {}
        extra = {"hogql": query.get("query"), "values": query.get("values"), "refresh": body.get("refresh")}
        if not self._gate(**extra):
            return
        if url.path != f"/api/projects/{PROJECT}/query/":
            self._record(404, **extra)
            self._json(404, {"detail": "Not found."})
            return
        if query.get("kind") != "HogQLQuery":
            self._record(400, **extra)
            self._json(400, {"type": "validation_error", "detail": "mock supports only HogQLQuery"})
            return
        try:
            response = run_hogql(str(query.get("query") or ""), dict(query.get("values") or {}))
        except HogQLError as err:
            self._record(400, **extra)
            self._json(400, {"type": "validation_error", "code": "query_error", "detail": str(err)})
            return
        if body.get("refresh") in ("async", "force_async", "lazy_async"):
            job_id = uuid.uuid4().hex
            JOBS[job_id] = {"polls": 0, "status": {"results": response, "error": False}}
            self._record(202, **extra)
            self._json(202, {"query_status": {"id": job_id, "team_id": int(PROJECT), "complete": False}})
            return
        self._record(200, **extra)
        self._json(200, response)


def main() -> None:
    """Parse arguments, load the fixture and serve until killed."""
    global DB
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--fixture", default=str(Path(__file__).resolve().parent.parent / "fixtures" / "posthog.json"))
    args = parser.parse_args()
    FIXTURE.update(json.loads(Path(args.fixture).read_text()))
    DB = _setup_db()
    server = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    print(f"PORT:{server.server_address[1]}", flush=True)
    with contextlib.suppress(KeyboardInterrupt):
        server.serve_forever()
    sys.exit(0)


if __name__ == "__main__":
    main()
