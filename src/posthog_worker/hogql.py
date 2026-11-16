"""HogQL text building, ClickHouse type mapping and result-value conversion.

**Injection safety.** Every constant the worker puts into a query goes through
``HogQLBuilder.param``, which stores it in the Query API's ``values`` map and
returns a ``{p<n>}`` placeholder. Identifiers (property keys, projected column
names) are only ever emitted by ``ident``, which accepts plain identifiers as is,
backtick-quotes anything else made of safe characters, and refuses names
containing backticks, backslashes, quotes, braces or control characters: such a
column or property is simply not pushed down, never spliced in.

**Types.** HogQL responses carry ClickHouse type strings (``types`` is a list of
``[name, type]`` pairs). ``arrow_type`` parses them (``Nullable``,
``LowCardinality``, ``Array``, ``Tuple``, ``Map``, ``DateTime64(6, 'UTC')``,
``Decimal(p, s)`` ...) into Arrow types, and ``convert`` turns the JSON values in
``results`` into Python values of that type.
"""

from __future__ import annotations

import datetime as dt
import decimal
import json
import re
from dataclasses import dataclass, field
from typing import Any

import pyarrow as pa

TSTZ = pa.timestamp("us", tz="UTC")
_SIMPLE_IDENT = re.compile(r"^[A-Za-z_$][A-Za-z0-9_$]*$")
_QUOTABLE_IDENT = re.compile(r"^[A-Za-z0-9_$ .:/@#+\-]+$")
# Words that cannot appear unquoted as a column name in HogQL.
_KEYWORDS = frozenset(
    [
        "all",
        "and",
        "anti",
        "any",
        "array",
        "as",
        "asc",
        "asof",
        "between",
        "both",
        "by",
        "case",
        "cast",
        "cohort",
        "collate",
        "cross",
        "cube",
        "current",
        "date",
        "day",
        "desc",
        "distinct",
        "else",
        "end",
        "exists",
        "extract",
        "false",
        "final",
        "first",
        "following",
        "for",
        "from",
        "full",
        "group",
        "having",
        "hour",
        "id",
        "if",
        "ilike",
        "in",
        "index",
        "inf",
        "inner",
        "interval",
        "is",
        "join",
        "key",
        "last",
        "leading",
        "left",
        "like",
        "limit",
        "minute",
        "month",
        "nan",
        "natural",
        "not",
        "null",
        "nulls",
        "offset",
        "on",
        "or",
        "order",
        "outer",
        "over",
        "partition",
        "preceding",
        "prewhere",
        "quarter",
        "range",
        "recursive",
        "right",
        "rollup",
        "row",
        "rows",
        "sample",
        "second",
        "select",
        "semi",
        "settings",
        "then",
        "ties",
        "to",
        "top",
        "trailing",
        "trim",
        "true",
        "unbounded",
        "union",
        "using",
        "week",
        "when",
        "where",
        "window",
        "with",
        "year",
    ]
)


def ident(name: str) -> str | None:
    """Render ``name`` as a HogQL identifier, or None when it is unsafe to emit.

    Args:
        name: Column or property name.

    Returns:
        The identifier text (backtick-quoted when needed), or None.

    """
    if _SIMPLE_IDENT.match(name) and name.lower() not in _KEYWORDS:
        return name
    if _QUOTABLE_IDENT.match(name):
        return f"`{name}`"
    return None


def iso_utc(value: Any) -> str:
    """Format a timestamp/date constant as a UTC string HogQL's ``toDateTime`` parses.

    Args:
        value: ``datetime``, ``date`` or anything else (stringified).

    Returns:
        ``YYYY-MM-DD HH:MM:SS.ffffff`` in UTC for datetimes and dates.

    """
    if isinstance(value, dt.datetime):
        aware = value if value.tzinfo else value.replace(tzinfo=dt.UTC)
        return aware.astimezone(dt.UTC).strftime("%Y-%m-%d %H:%M:%S.%f")
    if isinstance(value, dt.date):
        return f"{value.isoformat()} 00:00:00.000000"
    return str(value)


@dataclass
class HogQLBuilder:
    """Collects placeholder values while a query is assembled."""

    values: dict[str, Any] = field(default_factory=dict)

    def param(self, value: Any) -> str:
        """Store ``value`` and return its ``{name}`` placeholder.

        Args:
            value: A JSON-serializable constant (datetimes become UTC strings).

        Returns:
            The placeholder text to embed in HogQL.

        """
        if isinstance(value, dt.datetime | dt.date):
            value = iso_utc(value)
        elif isinstance(value, decimal.Decimal):
            value = float(value)
        name = f"vgi_p{len(self.values)}"
        self.values[name] = value
        return "{" + name + "}"


# --------------------------------------------------------------------------- types


def _split_args(text: str) -> list[str]:
    """Split ``a, b(c, d), 'e'`` at top-level commas."""
    parts: list[str] = []
    depth = 0
    quote = False
    current: list[str] = []
    i = 0
    while i < len(text):
        ch = text[i]
        if quote:
            current.append(ch)
            if ch == "\\" and i + 1 < len(text):
                current.append(text[i + 1])
                i += 1
            elif ch == "'":
                quote = False
        elif ch == "'":
            quote = True
            current.append(ch)
        elif ch == "(":
            depth += 1
            current.append(ch)
        elif ch == ")":
            depth -= 1
            current.append(ch)
        elif ch == "," and depth == 0:
            parts.append("".join(current).strip())
            current = []
        else:
            current.append(ch)
        i += 1
    if current or parts:
        parts.append("".join(current).strip())
    return parts


def _parse(text: str) -> tuple[str, list[str]]:
    text = text.strip()
    paren = text.find("(")
    if paren == -1 or not text.endswith(")"):
        return text, []
    return text[:paren].strip(), _split_args(text[paren + 1 : -1])


_INTS = {
    "Int8": pa.int8(),
    "Int16": pa.int16(),
    "Int32": pa.int32(),
    "Int64": pa.int64(),
    "UInt8": pa.uint8(),
    "UInt16": pa.uint16(),
    "UInt32": pa.uint32(),
    "UInt64": pa.uint64(),
}


def arrow_type(ch_type: str) -> tuple[pa.DataType, bool]:
    """Map a ClickHouse type string to ``(arrow_type, nullable)``.

    Args:
        ch_type: Type string from a HogQL response, e.g. ``Nullable(DateTime64(6, 'UTC'))``.

    Returns:
        The Arrow type and whether ClickHouse declared it ``Nullable``.

    """
    name, args = _parse(ch_type)
    if name == "Nullable" and args:
        return arrow_type(args[0])[0], True
    if name in ("LowCardinality", "SimpleAggregateFunction") and args:
        return arrow_type(args[-1])
    if name in _INTS:
        return _INTS[name], False
    if name in ("Float32", "Float64"):
        return pa.float64(), False
    if name == "Bool":
        return pa.bool_(), False
    if name in ("Date", "Date32"):
        return pa.date32(), False
    if name in ("DateTime", "DateTime64"):
        return TSTZ, False
    if name == "Decimal" and len(args) == 2 and args[0].isdigit() and args[1].isdigit():
        precision, scale = int(args[0]), int(args[1])
        if precision <= 38:
            return pa.decimal128(precision, scale), False
        return pa.string(), False
    if name in ("Decimal32", "Decimal64", "Decimal128") and len(args) == 1 and args[0].isdigit():
        precision = {"Decimal32": 9, "Decimal64": 18, "Decimal128": 38}[name]
        return pa.decimal128(precision, int(args[0])), False
    if name == "Array" and args:
        return pa.list_(arrow_type(args[0])[0]), False
    if name == "Map" and len(args) == 2:
        key = arrow_type(args[0])[0]
        if not (pa.types.is_string(key) or pa.types.is_integer(key)):
            key = pa.string()
        return pa.map_(key, arrow_type(args[1])[0]), False
    if name == "Tuple" and args:
        fields = []
        for i, arg in enumerate(args):
            match = re.match(r"^([A-Za-z_][A-Za-z0-9_]*)\s+(.+)$", arg)
            if match and not match.group(2).startswith("("):
                fname, ftype = match.group(1), match.group(2)
            else:
                fname, ftype = f"_{i + 1}", arg
            fields.append(pa.field(fname, arrow_type(ftype)[0]))
        return pa.struct(fields), False
    if name in ("JSON", "Object"):
        return pa.json_(), False
    if name == "Nothing":
        return pa.string(), True
    # String, FixedString, UUID, IPv4/6, Enum8/16 and anything unknown.
    return pa.string(), False


# --------------------------------------------------------------------------- values


def parse_ts(value: Any) -> dt.datetime | None:
    """Parse a PostHog timestamp (ISO string with ``Z``/offset, or epoch seconds).

    Args:
        value: The JSON value.

    Returns:
        An aware UTC datetime, or None.

    """
    if value is None or value == "":
        return None
    if isinstance(value, int | float):
        return dt.datetime.fromtimestamp(float(value), dt.UTC)
    text = str(value).strip().replace("Z", "+00:00")
    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed.astimezone(dt.UTC) if parsed.tzinfo else parsed.replace(tzinfo=dt.UTC)


def _json_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        try:
            json.loads(value)
            return value
        except ValueError:
            return json.dumps(value)
    return json.dumps(value, sort_keys=True)


def convert(value: Any, type_: pa.DataType) -> Any:
    """Convert one JSON result value to a Python value of Arrow type ``type_``.

    Args:
        value: Value from a HogQL ``results`` row or a REST payload.
        type_: Target Arrow type.

    Returns:
        A value ``pa.array`` accepts for that type, or None when it cannot be read.

    """
    if value is None:
        return None
    if isinstance(type_, pa.ExtensionType) and type_.extension_name == "arrow.json":
        return _json_text(value)
    if pa.types.is_timestamp(type_):
        return parse_ts(value)
    if pa.types.is_date(type_):
        try:
            return dt.date.fromisoformat(str(value)[:10])
        except ValueError:
            return None
    if pa.types.is_boolean(type_):
        if isinstance(value, str):
            return value.lower() in ("true", "1", "t", "yes")
        return bool(value)
    if pa.types.is_integer(type_):
        try:
            return int(value)
        except TypeError, ValueError:
            return None
    if pa.types.is_floating(type_):
        try:
            return float(value)
        except TypeError, ValueError:
            return None
    if pa.types.is_decimal(type_):
        try:
            return decimal.Decimal(str(value))
        except decimal.InvalidOperation:
            return None
    if pa.types.is_list(type_):
        if not isinstance(value, list | tuple):
            return None
        return [convert(v, type_.value_type) for v in value]
    if pa.types.is_map(type_):
        if not isinstance(value, dict):
            return None
        return [(convert(k, type_.key_type), convert(v, type_.item_type)) for k, v in value.items()]
    if pa.types.is_struct(type_):
        names = [type_.field(i).name for i in range(type_.num_fields)]
        if isinstance(value, dict):
            return {n: convert(value.get(n), type_.field(n).type) for n in names}
        if isinstance(value, list | tuple):
            return {n: convert(v, type_.field(n).type) for n, v in zip(names, value, strict=False)}
        return None
    if isinstance(value, str):
        return value
    return json.dumps(value, sort_keys=True)
