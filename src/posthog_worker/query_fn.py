"""``hogql(query, ...)``: run any HogQL query through PostHog's Query API.

**Schema.** The output columns come from the query itself: at bind time the
worker sends ``SELECT * FROM (<query>) LIMIT 0`` and maps the response's
ClickHouse ``types`` to DuckDB types (see ``hogql.arrow_type``).

**Pushdown.** The query is wrapped, so DuckDB's projection, predicates and
Top-N hint run inside PostHog:
``SELECT <needed columns> FROM (<query>) WHERE <pushed predicates>
[ORDER BY <col> LIMIT n] LIMIT <max_rows>``. Columns whose names are not plain
identifiers (e.g. an un-aliased ``count()``) are read with ``*`` and filtered
locally. ``max_rows := 0`` sends the query verbatim (no wrapper, no pushdown);
PostHog then applies its own default of 100 rows unless the query has a LIMIT.

**Limits.** PostHog returns at most 50,000 rows per query and rejects
``OFFSET`` for personal API keys, so ``hogql`` makes one request. For larger
reads use the typed tables (``events``, ``persons`` ...), which page with a
keyset cursor.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Annotated, Any, ClassVar

import pyarrow as pa
from vgi.arguments import Arg, Secret
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

from .client import MAX_QUERY_ROWS, POSTHOG_SECRET_TYPE, REFRESH_MODES, PostHogClient, PostHogConfig, PostHogError
from .hogql import HogQLBuilder, arrow_type, convert, ident
from .meta import column
from .pushdown import Col, translate
from .scan import TOPN_KINDS, client_from

log = logging.getLogger(__name__)


@dataclass(slots=True, frozen=True, kw_only=True)
class HogQLArgs:
    """Arguments for ``hogql(query, values := NULL, max_rows := 50000, refresh := 'blocking')``."""

    query: Annotated[
        str,
        Arg(0, doc="A HogQL SELECT, e.g. 'SELECT event, count() AS n FROM events GROUP BY event'."),
    ]
    values: Annotated[
        str | None,
        Arg(
            "values",
            default=None,
            doc=(
                "JSON object of placeholder values referenced as {name} in the query, e.g. "
                '\'{"ev": "$pageview"}\' for WHERE event = {ev}. Use these instead of quoting constants.'
            ),
        ),
    ]
    max_rows: Annotated[
        int,
        Arg(
            "max_rows",
            default=MAX_QUERY_ROWS,
            doc="Row cap sent as the outer LIMIT, 1 to 50000 (default 50000); 0 sends the query verbatim.",
            ge=0,
            le=MAX_QUERY_ROWS,
        ),
    ]
    refresh: Annotated[
        str,
        Arg(
            "refresh",
            default="blocking",
            doc="Query API refresh mode: 'blocking' (default), 'async' or 'force_async' (polled), "
            "'force_blocking', 'force_cache' or 'lazy_async'.",
            choices=REFRESH_MODES,
        ),
    ]


@dataclass(kw_only=True)
class HogQLState(ArrowSerializableDataclass):
    """One request per scan; ``done`` once it has been emitted."""

    done: bool = False


def _user_values(raw: str | None) -> dict[str, Any]:
    if raw is None or raw.strip() == "":
        return {}
    try:
        parsed = json.loads(raw)
    except ValueError as err:
        raise PostHogError(f"hogql: values must be a JSON object, got {raw[:80]!r}") from err
    if not isinstance(parsed, dict):
        raise PostHogError('hogql: values must be a JSON object such as \'{"ev": "$pageview"}\'')
    for key in parsed:
        if str(key).startswith("vgi_p"):
            raise PostHogError(f"hogql: placeholder name {key!r} is reserved (vgi_p* names are used by the worker)")
    return {str(k): v for k, v in parsed.items()}


def _kind(type_: pa.DataType) -> str:
    if pa.types.is_timestamp(type_):
        return "ts"
    if pa.types.is_date(type_):
        return "date"
    if pa.types.is_boolean(type_):
        return "bool"
    if pa.types.is_integer(type_):
        return "int"
    if pa.types.is_floating(type_):
        return "float"
    if pa.types.is_string(type_):
        return "str"
    return "json"


def _cols(schema: pa.Schema) -> dict[str, Col]:
    out = {}
    for f in schema:
        name = ident(f.name)
        if name is not None:
            out[f.name] = Col(name, _kind(f.type))
    return out


def run(client: PostHogClient, args: HogQLArgs, hogql: str, builder: HogQLBuilder) -> Any:
    """Send one query with the user's placeholder values merged in.

    Args:
        client: Configured client.
        args: The function arguments.
        hogql: Final HogQL text.
        builder: The worker's placeholder values.

    Returns:
        The ``QueryResult``.

    """
    return client.query(hogql, {**_user_values(args.values), **builder.values}, refresh=args.refresh)


@init_single_worker
class HogQL(TableFunctionGenerator[HogQLArgs, HogQLState]):
    """Run a HogQL query; columns and types come from PostHog's response."""

    class Meta:
        """Function metadata (tags and examples are set in ``worker.py``)."""

        name = "hogql"
        description = "Run a HogQL query through PostHog's Query API and return its typed result"
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags: ClassVar[dict[str, str]] = {}
        examples: ClassVar[list[Any]] = []

    @classmethod
    def on_bind(
        cls,
        params: BindParams[HogQLArgs],
        *,
        posthog: Annotated[dict[str, pa.Scalar[Any]] | None, Secret(POSTHOG_SECRET_TYPE)] = None,
    ) -> BindResponse:
        """Probe the query's columns and types with a ``LIMIT 0`` wrapper."""
        args = params.args
        client = PostHogClient(PostHogConfig.from_secret(posthog))
        probe = args.query if args.max_rows == 0 else f"SELECT * FROM ({args.query}) LIMIT 0"
        result = run(client, args, probe, HogQLBuilder())
        if not result.columns:
            raise PostHogError("hogql: the query returned no columns")
        fields = []
        seen: set[str] = set()
        for name, ch_type in zip(result.columns, result.types, strict=False):
            if name in seen:
                raise PostHogError(f"hogql: duplicate output column {name!r}; give each column a distinct alias")
            seen.add(name)
            type_, _ = arrow_type(ch_type)
            fields.append(column(name, type_, f"HogQL result column (ClickHouse type {ch_type})."))
        return BindResponse(output_schema=pa.schema(fields))

    @classmethod
    def initial_state(cls, params: ProcessParams[HogQLArgs]) -> HogQLState:
        """Nothing sent yet."""
        return HogQLState()

    @classmethod
    def process(cls, params: ProcessParams[HogQLArgs], state: HogQLState, out: OutputCollector) -> None:
        """Send the (wrapped) query once and emit its rows."""
        if state.done:
            out.finish()
            return
        state.done = True
        args = params.args
        client = client_from(params)
        schema = params.output_schema
        builder = HogQLBuilder()
        if args.max_rows == 0:
            hogql = args.query
        else:
            cols = _cols(schema)
            tr = translate(params.current_pushdown_filters, cols, builder)
            names = schema.names
            select = ", ".join(cols[n].expr for n in names) if names and all(n in cols for n in names) else "*"
            hogql = f"SELECT {select} FROM ({args.query})"
            if tr.clauses:
                hogql += " WHERE " + " AND ".join(tr.clauses)
            limit = args.max_rows
            init = params.init_call
            assert init is not None
            hint = cols.get(init.order_by_column_name or "")
            if tr.exact and hint is not None and hint.kind in TOPN_KINDS and init.order_by_limit:
                direction = "DESC" if init.order_by_direction == OrderByDirection.DESC else "ASC"
                nulls = "NULLS FIRST" if init.order_by_null_order == OrderByNullOrder.NULLS_FIRST else "NULLS LAST"
                hogql += f" ORDER BY {hint.expr} {direction} {nulls}"
                limit = min(limit, int(init.order_by_limit))
            hogql += f" LIMIT {int(limit)}"
        result = run(client, args, hogql, builder)
        if args.max_rows and len(result.results) >= args.max_rows:
            log.warning("hogql: result reached max_rows=%d; later rows were not read", args.max_rows)
        index = {name: i for i, name in enumerate(result.columns)}
        columns: dict[str, list[Any]] = {}
        for f in schema:
            if f.name not in index:
                raise PostHogError(f"hogql: column {f.name!r} missing from the PostHog response")
            i = index[f.name]
            columns[f.name] = [convert(row[i], f.type) for row in result.results]
        if result.results:
            out.emit(pa.RecordBatch.from_pydict(columns, schema=schema))
        else:
            out.finish()
