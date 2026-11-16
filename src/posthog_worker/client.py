"""HTTP client for PostHog's private API: the Query API (HogQL) and the REST resources.

Everything that touches the network lives here.

* **Configuration** comes from a DuckDB secret of type ``posthog`` (see
  ``POSTHOG_SECRET``): a personal API key, a project id and the host
  (``https://us.posthog.com`` by default; ``https://eu.posthog.com`` for EU Cloud,
  or a self-hosted URL). Nothing is read from the environment.
* **Queries** go to ``POST /api/projects/:id/query/`` as a ``HogQLQuery`` with
  ``values`` for every constant, so no user value is ever spliced into HogQL text.
  When PostHog answers asynchronously (``query_status.complete == false``) the
  client polls ``GET /api/projects/:id/query/:query_id/`` until it completes.
* **REST listings** follow DRF's ``next`` links, but only on the configured
  host: a ``next`` URL pointing elsewhere is rebuilt on the configured origin, so
  the API key is never sent to another host.
* **Politeness**: every request has a timeout; 429 and 502/503/504 are retried
  with backoff that honours ``Retry-After`` (PostHog sends it with
  ``api_queries_budget_exceeded``).

The key is never written to table-function state or logs: each ``process``
call rebuilds the client from the secret the SDK hands it.
"""

from __future__ import annotations

import datetime as dt
import email.utils
import json
import logging
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

import pyarrow as pa
from vgi.catalog import SecretTypeSpec

log = logging.getLogger(__name__)

POSTHOG_SECRET_TYPE = "posthog"
DEFAULT_HOST = "https://us.posthog.com"
USER_AGENT = "vgi-posthog-worker/0.1"
MAX_ATTEMPTS = 5
MAX_BACKOFF_SECONDS = 60.0
RETRY_STATUSES = frozenset({429, 502, 503, 504})
DOC_URL = "https://github.com/Query-farm/vgi-posthog#credentials"
# PostHog returns at most 50,000 rows per HogQL query (100 without an explicit LIMIT).
MAX_QUERY_ROWS = 50_000
REFRESH_MODES = ("blocking", "async", "force_blocking", "force_async", "force_cache", "lazy_async")


def _field(
    name: str, doc: str, type_: pa.DataType | None = None, *, redact: bool = False, required: bool = False
) -> Any:
    meta = {"vgi_doc": doc}
    if redact:
        meta["redact"] = "true"
    if required:
        meta["vgi_required"] = "true"
    return pa.field(name, type_ or pa.string(), metadata=meta)


POSTHOG_SECRET = SecretTypeSpec(
    name=POSTHOG_SECRET_TYPE,
    description=(
        "Connection to one PostHog project: a personal API key (phx_...), the numeric project id, and the "
        "host (US Cloud by default; EU Cloud or a self-hosted URL via HOST)."
    ),
    schema=pa.schema(
        [
            _field(
                "personal_api_key",
                "Personal API key (starts with phx_) with read access to Query and the project's resources. "
                "Not the project API key (phc_) used for capture.",
                redact=True,
                required=True,
            ),
            _field(
                "project_id",
                "Numeric project (environment) id from Project settings, e.g. '12345'. Needed by every table "
                "except metadata.projects.",
            ),
            _field(
                "host",
                "PostHog app URL: 'https://us.posthog.com' (default), 'https://eu.posthog.com', or a "
                "self-hosted instance such as 'https://posthog.example.com'.",
            ),
            _field("timeout", "Per-request timeout in seconds (default 60).", pa.float64()),
            _field("page_size", "Rows per HogQL page for the typed tables, 1 to 50000 (default 10000).", pa.int64()),
            _field(
                "max_wait",
                "Seconds to keep polling an asynchronous query before giving up (default 300).",
                pa.float64(),
            ),
        ],
        metadata={"vgi_doc_url": DOC_URL},
    ),
)


class PostHogError(RuntimeError):
    """The PostHog API returned an error, or the secret is unusable."""


@dataclass(frozen=True, slots=True)
class PostHogConfig:
    """Connection settings parsed from a ``posthog`` secret."""

    host: str
    api_key: str = field(repr=False)
    project_id: str | None = None
    timeout: float = 60.0
    page_size: int = 10_000
    max_wait: float = 300.0

    @classmethod
    def from_secret(cls, secret: Mapping[str, Any] | None) -> PostHogConfig:
        """Build and validate a config from a resolved secret's key/value pairs.

        Args:
            secret: Secret fields; values may be ``pa.Scalar`` or plain Python.

        Returns:
            The validated configuration.

        Raises:
            PostHogError: No secret, or an invalid field value.

        """
        if not secret:
            raise PostHogError(
                "no PostHog credentials: run CREATE SECRET (TYPE posthog, PERSONAL_API_KEY 'phx_...', "
                f"PROJECT_ID '12345') after ATTACH (add HOST 'https://eu.posthog.com' for EU Cloud). See {DOC_URL}"
            )

        def get(name: str) -> Any:
            value = secret.get(name)
            if isinstance(value, pa.Scalar):
                value = value.as_py()
            return None if value == "" else value

        key = get("personal_api_key")
        if not key:
            raise PostHogError("posthog secret: PERSONAL_API_KEY must be set (a phx_... personal API key)")
        host = str(get("host") or DEFAULT_HOST).rstrip("/")
        if not host.startswith(("http://", "https://")):
            raise PostHogError("posthog secret: HOST must be an http:// or https:// URL")
        project = get("project_id")
        if project is not None:
            project = str(project).strip()
            if not project.isdigit():
                raise PostHogError(f"posthog secret: PROJECT_ID must be the numeric project id, not {project!r}")
        page_size = int(get("page_size") or 10_000)
        if not 1 <= page_size <= MAX_QUERY_ROWS:
            raise PostHogError(f"posthog secret: PAGE_SIZE must be between 1 and {MAX_QUERY_ROWS}")
        return cls(
            host=host,
            api_key=str(key),
            project_id=project,
            timeout=float(get("timeout") or 60.0),
            page_size=page_size,
            max_wait=float(get("max_wait") or 300.0),
        )


def retry_delay(header: str | None, attempt: int) -> float:
    """Seconds to wait before retry ``attempt`` given a ``Retry-After`` header value.

    Args:
        header: The ``Retry-After`` value (seconds or an HTTP date), if any.
        attempt: 1-based attempt number, for exponential backoff without a header.

    Returns:
        Delay in seconds, capped at ``MAX_BACKOFF_SECONDS``.

    """
    if header:
        header = header.strip()
        if header.replace(".", "", 1).isdigit():
            return min(float(header), MAX_BACKOFF_SECONDS)
        try:
            when = email.utils.parsedate_to_datetime(header)
            return max(0.0, min((when - dt.datetime.now(dt.UTC)).total_seconds(), MAX_BACKOFF_SECONDS))
        except TypeError, ValueError:
            pass
    return min(0.5 * float(2**attempt), MAX_BACKOFF_SECONDS)


def _error_detail(body: str) -> str:
    try:
        detail = json.loads(body)
    except ValueError:
        return body
    if isinstance(detail, dict):
        return str(detail.get("detail") or detail.get("error") or detail.get("message") or body)
    return body


@dataclass(slots=True)
class QueryResult:
    """One HogQL response: column names, ClickHouse type strings and row lists."""

    columns: list[str]
    types: list[str]
    results: list[list[Any]]
    has_more: bool = False


class PostHogClient:
    """Authenticated JSON requests against one PostHog host."""

    def __init__(self, config: PostHogConfig) -> None:
        """Bind to a configuration."""
        self.config = config

    # ------------------------------------------------------------------ plumbing

    def _send(self, method: str, url: str, body: Any = None) -> Any:
        """Send one request with retries and return the decoded JSON body."""
        data = None if body is None else json.dumps(body).encode()
        headers = {
            "Accept": "application/json",
            "User-Agent": USER_AGENT,
            "Authorization": f"Bearer {self.config.api_key}",
        }
        if data is not None:
            headers["Content-Type"] = "application/json"
        for attempt in range(1, MAX_ATTEMPTS + 1):
            request = urllib.request.Request(url, data=data, headers=headers, method=method)
            try:
                with urllib.request.urlopen(request, timeout=self.config.timeout) as resp:
                    raw = resp.read()
                    return json.loads(raw) if raw else None
            except urllib.error.HTTPError as err:
                if err.code in RETRY_STATUSES and attempt < MAX_ATTEMPTS:
                    delay = retry_delay(err.headers.get("Retry-After"), attempt)
                    log.info("posthog: HTTP %s from %s %s; retrying in %.1fs", err.code, method, url, delay)
                    time.sleep(delay)
                    continue
                detail = _error_detail(err.read(2000).decode(errors="replace"))
                hint = ""
                if err.code in (401, 403):
                    hint = " (check the posthog secret's PERSONAL_API_KEY, its scopes and PROJECT_ID)"
                raise PostHogError(f"{method} {url} failed with HTTP {err.code}{hint}: {detail}") from err
            except (urllib.error.URLError, TimeoutError, ConnectionError) as err:
                if attempt == MAX_ATTEMPTS:
                    reason = getattr(err, "reason", err)
                    raise PostHogError(f"{method} {url} failed: {reason}") from err
                time.sleep(min(0.5 * 2**attempt, MAX_BACKOFF_SECONDS))
        raise PostHogError(f"{method} {url}: gave up after {MAX_ATTEMPTS} attempts")

    def project_id(self) -> str:
        """Return the configured project id.

        Returns:
            The numeric project id as a string.

        Raises:
            PostHogError: The secret has no PROJECT_ID.

        """
        if not self.config.project_id:
            raise PostHogError(
                "posthog secret: PROJECT_ID is required for this table (find it in Project settings, or "
                "SELECT id, name FROM posthog.metadata.projects)"
            )
        return self.config.project_id

    def url(self, path: str, query: Mapping[str, Any] | None = None) -> str:
        """Absolute URL on the configured host.

        Args:
            path: Path starting with ``/``.
            query: Optional query parameters (``None`` values dropped).

        Returns:
            The URL.

        """
        clean = {k: v for k, v in (query or {}).items() if v is not None}
        qs = urllib.parse.urlencode(clean, doseq=True)
        return f"{self.config.host}{path}" + (f"?{qs}" if qs else "")

    def same_origin(self, url: str) -> str:
        """Rebase a ``next`` link onto the configured host so the key never leaves it.

        Args:
            url: Absolute or relative URL from a ``next`` field.

        Returns:
            The URL with the configured scheme, host and port.

        """
        parts = urllib.parse.urlsplit(url)
        base = urllib.parse.urlsplit(self.config.host)
        prefix = base.path.rstrip("/")
        path = parts.path if parts.path.startswith(prefix + "/") or not prefix else prefix + parts.path
        return urllib.parse.urlunsplit((base.scheme, base.netloc, path, parts.query, ""))

    # ------------------------------------------------------------------ REST

    def get(self, url: str) -> Any:
        """GET an absolute URL on the configured host and decode its JSON body.

        Args:
            url: URL produced by :meth:`url` or :meth:`same_origin`.

        Returns:
            The decoded JSON response.

        """
        return self._send("GET", self.same_origin(url))

    # ------------------------------------------------------------------ Query API

    def query(self, hogql: str, values: Mapping[str, Any] | None = None, *, refresh: str = "blocking") -> QueryResult:
        """Run one HogQL query and return its rows, polling if PostHog runs it asynchronously.

        Args:
            hogql: HogQL text. Constants appear as ``{name}`` placeholders.
            values: Placeholder values, sent as the query's ``values``.
            refresh: The Query API ``refresh`` mode.

        Returns:
            The query's columns, types and rows.

        Raises:
            PostHogError: HTTP failure, a query error, or a poll timeout.

        """
        project = self.project_id()
        payload: dict[str, Any] = {
            "query": {"kind": "HogQLQuery", "query": hogql, "values": dict(values or {})},
            "refresh": refresh,
            "name": "vgi-posthog",
        }
        log.debug("posthog: HogQL %s values=%s", hogql, list((values or {}).keys()))
        body = self._send("POST", self.url(f"/api/projects/{project}/query/"), payload)
        deadline = time.monotonic() + self.config.max_wait
        delay = 0.25
        while True:
            response = self._unwrap(body)
            if response is not None:
                return self._result(response)
            status = body.get("query_status") or {}
            query_id = status.get("id")
            if not query_id:
                raise PostHogError(f"PostHog query API returned neither results nor a query id: {str(body)[:300]}")
            if time.monotonic() > deadline:
                raise PostHogError(
                    f"PostHog query {query_id} did not finish within {self.config.max_wait:.0f}s "
                    "(raise MAX_WAIT in the posthog secret, or narrow the query)"
                )
            time.sleep(delay)
            delay = min(delay * 2, 5.0)
            quoted = urllib.parse.quote(str(query_id), safe="")
            body = self._send("GET", self.url(f"/api/projects/{project}/query/{quoted}/"))

    @staticmethod
    def _unwrap(body: Any) -> dict[str, Any] | None:
        """Return the HogQL response in ``body``, or None while the query is still running."""
        if not isinstance(body, dict):
            raise PostHogError(f"unexpected PostHog query response: {str(body)[:300]}")
        if "results" in body and "query_status" not in body:
            return body
        status = body.get("query_status")
        if not isinstance(status, dict):
            return body if "results" in body else None
        if status.get("error"):
            raise PostHogError(f"PostHog query failed: {status.get('error_message') or 'unknown error'}")
        if status.get("complete"):
            inner = status.get("results")
            if isinstance(inner, dict):
                return inner
            if "results" in body:
                return body
            raise PostHogError("PostHog reported the query complete but returned no results")
        return None

    @staticmethod
    def _result(response: dict[str, Any]) -> QueryResult:
        if response.get("error"):
            raise PostHogError(f"PostHog query failed: {response['error']}")
        rows = list(response.get("results") or [])
        columns = [str(c) for c in response.get("columns") or []]
        types: list[str] = []
        for entry in response.get("types") or []:
            # HogQLQueryResponse.types is a list of [name, clickhouse_type] pairs.
            if isinstance(entry, list | tuple) and len(entry) == 2:
                types.append(str(entry[1]))
                if len(columns) < len(types):
                    columns.append(str(entry[0]))
            else:
                types.append(str(entry))
        while len(types) < len(columns):
            types.append("String")
        return QueryResult(columns=columns, types=types, results=rows, has_more=bool(response.get("hasMore")))
