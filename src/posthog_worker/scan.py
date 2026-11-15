"""Shared machinery for the HogQL-backed tables: query building, keyset paging, Top-N.

Every typed table (``events``, ``persons``, ``sessions`` ...) is a subclass of
``HogQLScan`` that declares:

* ``SOURCE``: the HogQL table to read, e.g. ``events``;
* ``COLS``: each output column's HogQL expression and kind (``pushdown.Col``);
* ``KEY``: the columns that order the scan and drive keyset pagination.

One ``process`` call sends one HogQL query and emits one batch:

``SELECT <projected columns + key> FROM <source> WHERE <pushed filters> AND
<keyset cursor> ORDER BY <key> LIMIT <page_size>``

PostHog rejects ``OFFSET`` for personal API keys and recommends keyset
pagination (on ``timestamp`` for events, ``id`` for persons), so the cursor is
the last row's key values. It travels in ``ScanState``, holds no credentials, and
lets a scan resume on any worker process over the HTTP transport.

**Top-N.** For ``ORDER BY <col> [DESC] LIMIT n`` DuckDB passes an order hint with
the limit (offset included). When every pushed predicate translated exactly and
``n`` is at most 50,000, the scan sends a single ``ORDER BY <col> LIMIT n`` query
instead of paging. (DuckDB only sends the limit when the query has no WHERE
clause on the table; ``events_where(max_rows := n)`` covers filtered limits.)
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Annotated, Any, ClassVar

import pyarrow as pa
from vgi.arguments import Secret
from vgi.invocation import BindResponse
from vgi.table_function import (
    BindParams,
    OrderByDirection,
    OrderByNullOrder,
    OutputCollector,
    ProcessParams,
    TableFunctionGenerator,
    init_single_worker,
)
from vgi_rpc import ArrowSerializableDataclass

from .client import MAX_QUERY_ROWS, POSTHOG_SECRET_TYPE, PostHogClient, PostHogConfig
from .hogql import HogQLBuilder, convert
from .pushdown import Col, translate

log = logging.getLogger(__name__)

TOPN_KINDS = frozenset({"ts", "int", "float", "date", "str"})


@dataclass(slots=True, frozen=True, kw_only=True)
class NoArgs:
    """Catalog tables take no arguments."""


@dataclass(kw_only=True)
class ScanState(ArrowSerializableDataclass):
    """Resumable cursor: the last emitted key (JSON list, empty before the first page)."""

    cursor: str = ""
    emitted: int = 0
    done: bool = False


def secret_from(params: ProcessParams[Any]) -> dict[str, Any] | None:
    """Return the first resolved ``posthog`` secret, if any.

    Args:
        params: The process parameters carrying resolved secrets.

    Returns:
        The secret's fields, or None.

    """
    for secret in params.secrets.of_type(POSTHOG_SECRET_TYPE):
        return dict(secret)
    return None


def client_from(params: ProcessParams[Any]) -> PostHogClient:
    """Build a client from the secret DuckDB resolved for this call.

    Args:
        params: The process parameters carrying resolved secrets.

    Returns:
        A configured client.

    """
    return PostHogClient(PostHogConfig.from_secret(secret_from(params)))


def keyset_clause(key: list[tuple[str, Col]], cursor: list[Any], builder: HogQLBuilder, op: str = ">") -> str:
    """Render ``(k1, k2, ...) > (c1, c2, ...)`` (or ``<``) as nested OR/AND comparisons.

    Args:
        key: ``(name, Col)`` pairs in sort order.
        cursor: The last row's raw key values, same order.
        builder: Collects placeholder values.
        op: ``>`` for an ascending scan, ``<`` for a descending one.

    Returns:
        The HogQL condition.

    """

    def rhs(col: Col, value: Any) -> str:
        p = builder.param(value)
        if col.kind == "ts":
            return f"toDateTime({p})"
        if col.kind == "uuid":
            return f"toUUID({p})"
        return p

    ors = []
    for i, (_, col) in enumerate(key):
        parts = [f"{key[j][1].expr} = {rhs(key[j][1], cursor[j])}" for j in range(i)]
        parts.append(f"{col.expr} {op} {rhs(col, cursor[i])}")
        ors.append(" AND ".join(parts) if len(parts) == 1 else "(" + " AND ".join(parts) + ")")
    return ors[0] if len(ors) == 1 else "(" + " OR ".join(ors) + ")"


@init_single_worker
class HogQLScan(TableFunctionGenerator[NoArgs, ScanState]):
    """Base class for tables read from one HogQL table with keyset pagination."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema([])
    SOURCE: ClassVar[str] = ""
    COLS: ClassVar[dict[str, Col]] = {}
    KEY: ClassVar[list[str]] = []

    class Meta:
        """Overridden by every concrete table."""

        name = "hogql_scan"
        description = "Base class for HogQL scans"

    @classmethod
    def on_bind(
        cls,
        params: BindParams[Any],
        *,
        posthog: Annotated[dict[str, pa.Scalar[Any]] | None, Secret(POSTHOG_SECRET_TYPE)] = None,
    ) -> BindResponse:
        """Declare the fixed schema and ask DuckDB for the ``posthog`` secret."""
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[Any]) -> ScanState:
        """Start before the first row."""
        return ScanState()

    @classmethod
    def extra_clauses(cls, params: ProcessParams[Any], builder: HogQLBuilder) -> list[str]:
        """WHERE clauses from function arguments (none for plain tables)."""
        return []

    @classmethod
    def max_rows(cls, params: ProcessParams[Any]) -> int | None:
        """Row cap from function arguments (none for plain tables)."""
        return None

    @classmethod
    def descending(cls, params: ProcessParams[Any]) -> bool:
        """Whether the scan pages newest-first (function argument; tables page ascending)."""
        return False

    @classmethod
    def _topn(cls, params: ProcessParams[Any], exact: bool) -> tuple[str, int] | None:
        """``(ORDER BY text, limit)`` when DuckDB's Top-N hint can be sent to PostHog."""
        init = params.init_call
        assert init is not None
        name, limit = init.order_by_column_name, init.order_by_limit
        col = cls.COLS.get(name or "")
        if not exact or col is None or col.kind not in TOPN_KINDS or not limit or limit > MAX_QUERY_ROWS:
            return None
        direction = "DESC" if init.order_by_direction == OrderByDirection.DESC else "ASC"
        nulls = "NULLS FIRST" if init.order_by_null_order == OrderByNullOrder.NULLS_FIRST else "NULLS LAST"
        return f"{col.expr} {direction} {nulls}", int(limit)

    @classmethod
    def process(cls, params: ProcessParams[Any], state: ScanState, out: OutputCollector) -> None:
        """Fetch one page with the pushed filters and cursor, emit it, and advance."""
        if state.done:
            out.finish()
            return
        client = client_from(params)
        builder = HogQLBuilder()
        tr = translate(params.current_pushdown_filters, cls.COLS, builder)
        clauses = tr.clauses + cls.extra_clauses(params, builder)
        wanted = list(params.output_schema.names)
        cap = cls.max_rows(params)
        desc = cls.descending(params)
        topn = None if cap is not None else cls._topn(params, tr.exact)
        # The keyset key is selected only when paging; a Top-N query is a single request.
        select = wanted if topn is not None and wanted else list(dict.fromkeys([*wanted, *cls.KEY]))
        if topn is not None:
            order, limit = topn
        else:
            if state.cursor:
                cursor = json.loads(state.cursor)
                key = [(k, cls.COLS[k]) for k in cls.KEY]
                clauses.append(keyset_clause(key, cursor, builder, "<" if desc else ">"))
            order = ", ".join(f"{cls.COLS[k].expr}{' DESC' if desc else ''}" for k in cls.KEY)
            limit = client.config.page_size
            if cap is not None:
                limit = min(limit, cap - state.emitted)
        hogql = f"SELECT {', '.join(cls.COLS[c].expr for c in select)} FROM {cls.SOURCE}"
        if clauses:
            hogql += " WHERE " + " AND ".join(clauses)
        hogql += f" ORDER BY {order} LIMIT {int(limit)}"
        result = client.query(hogql, builder.values)
        rows = result.results
        if rows:
            schema = params.output_schema
            columns = {f.name: [convert(r[select.index(f.name)], f.type) for r in rows] for f in schema}
            out.emit(pa.RecordBatch.from_pydict(columns, schema=schema))
        state.emitted += len(rows)
        if topn is not None or len(rows) < limit or (cap is not None and state.emitted >= cap):
            state.done = True
        else:
            state.cursor = json.dumps([rows[-1][select.index(k)] for k in cls.KEY])
        if not rows:
            out.finish()
