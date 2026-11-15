"""Translate the predicates DuckDB pushes into a scan into a HogQL ``WHERE`` clause.

DuckDB hands a scan simple per-column predicates: comparisons with a constant,
``IN`` lists, ``IS [NOT] NULL`` and AND/OR combinations of those. Each one whose
column has a HogQL expression is rendered with placeholders (never inlined
constants). The scans set ``auto_apply_filters``, so the SDK re-applies every
pushed predicate to each batch: a predicate that is not translated (or only
widened) still holds, translation only narrows what PostHog returns.

``Translation.exact`` is True when every pushed predicate was translated
exactly; only then may a row limit be sent to PostHog as well.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from vgi.table_filter_pushdown import (
    AndFilter,
    ComparisonOp,
    ConstantFilter,
    Filter,
    InFilter,
    IsNotNullFilter,
    IsNullFilter,
    OrFilter,
    PushdownFilters,
)

from .hogql import HogQLBuilder

MAX_IN_VALUES = 1000
_OPS = {
    ComparisonOp.EQ: "=",
    ComparisonOp.NE: "!=",
    ComparisonOp.GT: ">",
    ComparisonOp.GE: ">=",
    ComparisonOp.LT: "<",
    ComparisonOp.LE: "<=",
}


@dataclass(frozen=True, slots=True)
class Col:
    """How one SQL column maps to HogQL.

    Attributes:
        expr: HogQL expression selecting the column, e.g. ``properties.$session_id``.
        kind: ``str``, ``uuid``, ``ts``, ``date``, ``int``, ``float``, ``bool`` or ``json``
            (``json`` columns are never filtered in HogQL).

    """

    expr: str
    kind: str = "str"


@dataclass
class Translation:
    """The WHERE clauses produced for a scan, and whether they are exact."""

    clauses: list[str] = field(default_factory=list)
    exact: bool = True
    pushed_columns: set[str] = field(default_factory=set)


def _constant(builder: HogQLBuilder, col: Col, value: Any) -> str | None:
    if col.kind == "ts":
        return f"toDateTime({builder.param(value)})"
    if col.kind == "date":
        return f"toDate({builder.param(value)})"
    if col.kind in ("int", "float") and isinstance(value, int | float) and not isinstance(value, bool):
        return builder.param(value)
    if col.kind == "bool" and isinstance(value, bool):
        return builder.param(value)
    if col.kind in ("str", "uuid") and isinstance(value, str):
        return builder.param(value)
    return None


def _lhs(col: Col) -> str:
    # UUID columns compare as text so any user string is a valid operand.
    return f"toString({col.expr})" if col.kind == "uuid" else col.expr


def _one(f: Filter, cols: dict[str, Col], builder: HogQLBuilder) -> str | None:
    """Translate one filter exactly, or return None."""
    if isinstance(f, AndFilter | OrFilter):
        parts = [_one(child, cols, builder) for child in f.children]
        if not parts or any(p is None for p in parts):
            return None
        joiner = " AND " if isinstance(f, AndFilter) else " OR "
        return "(" + joiner.join(p for p in parts if p) + ")"
    col = cols.get(f.column_name)
    if col is None or col.kind == "json":
        return None
    if isinstance(f, ConstantFilter):
        op = _OPS.get(f.op)
        value = _constant(builder, col, f.value.as_py()) if op else None
        if op is None or value is None:
            return None
        if col.kind == "bool" and op not in ("=", "!="):
            return None
        return f"{_lhs(col)} {op} {value}"
    if isinstance(f, InFilter):
        items = [v for v in f.values.to_pylist() if v is not None]
        if not items or len(items) > MAX_IN_VALUES:
            return None
        rendered = [_constant(builder, col, v) for v in items]
        if any(r is None for r in rendered):
            return None
        return f"{_lhs(col)} IN ({', '.join(r for r in rendered if r)})"
    if isinstance(f, IsNullFilter):
        return f"{col.expr} IS NULL"
    if isinstance(f, IsNotNullFilter):
        return f"{col.expr} IS NOT NULL"
    return None


def translate(filters: PushdownFilters | None, cols: dict[str, Col], builder: HogQLBuilder) -> Translation:
    """Translate the pushed-down predicates of one scan.

    Args:
        filters: The pushed-down filters (a conjunction), or None.
        cols: SQL column name to HogQL mapping.
        builder: Collects the placeholder values.

    Returns:
        WHERE clauses (to AND together) and whether the translation is exact.

    """
    out = Translation()
    for f in filters.filters if filters else ():
        if isinstance(f, AndFilter):
            # A top-level AND may be translated child by child.
            sub = translate(PushdownFilters(filters=tuple(f.children)), cols, builder)
            out.clauses.extend(sub.clauses)
            out.exact = out.exact and sub.exact
            out.pushed_columns |= sub.pushed_columns
            continue
        clause = _one(f, cols, builder)
        if clause is None:
            out.exact = False
        else:
            out.clauses.append(clause)
            out.pushed_columns.add(f.column_name)
    return out
