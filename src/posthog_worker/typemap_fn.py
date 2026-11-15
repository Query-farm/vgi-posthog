"""``clickhouse_type_to_duckdb(ch_type)``: the type mapping ``hogql()`` applies, as a scalar.

HogQL responses carry ClickHouse type strings; ``hogql()`` turns each into a
DuckDB column type. This scalar exposes the same mapping so users (and agents)
can predict a column's type, or check how an unusual ClickHouse type will land,
without a PostHog connection.
"""

from __future__ import annotations

import json
from typing import Annotated

import pyarrow as pa
from vgi.arguments import Param, Returns
from vgi.metadata import FunctionExample
from vgi.scalar_function import ScalarFunction

from .hogql import arrow_type
from .meta import object_tags

_SIMPLE = {
    pa.string(): "VARCHAR",
    pa.int8(): "TINYINT",
    pa.int16(): "SMALLINT",
    pa.int32(): "INTEGER",
    pa.int64(): "BIGINT",
    pa.uint8(): "UTINYINT",
    pa.uint16(): "USMALLINT",
    pa.uint32(): "UINTEGER",
    pa.uint64(): "UBIGINT",
    pa.float64(): "DOUBLE",
    pa.bool_(): "BOOLEAN",
    pa.date32(): "DATE",
}


def duckdb_type_name(type_: pa.DataType) -> str:
    """Render an Arrow type the way DuckDB names the column type it becomes.

    Args:
        type_: Arrow type produced by ``hogql.arrow_type``.

    Returns:
        The DuckDB type name, e.g. ``TIMESTAMP WITH TIME ZONE`` or ``VARCHAR[]``.

    """
    if getattr(type_, "extension_name", None) == "arrow.json":
        return "JSON"
    if type_ in _SIMPLE:
        return _SIMPLE[type_]
    if pa.types.is_timestamp(type_):
        return "TIMESTAMP WITH TIME ZONE"
    if pa.types.is_decimal(type_):
        return f"DECIMAL({type_.precision},{type_.scale})"
    if pa.types.is_list(type_):
        return f"{duckdb_type_name(type_.value_type)}[]"
    if pa.types.is_map(type_):
        return f"MAP({duckdb_type_name(type_.key_type)}, {duckdb_type_name(type_.item_type)})"
    if pa.types.is_struct(type_):
        fields = ", ".join(
            f'"{type_.field(i).name}" {duckdb_type_name(type_.field(i).type)}' for i in range(type_.num_fields)
        )
        return f"STRUCT({fields})"
    return "VARCHAR"


class ClickhouseTypeToDuckdb(ScalarFunction):
    """``clickhouse_type_to_duckdb(ch_type)``: the DuckDB type ``hogql()`` gives a ClickHouse type."""

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "clickhouse_type_to_duckdb"
        description = "DuckDB column type that hogql() produces for a ClickHouse type string from a HogQL response"
        tags = {
            **object_tags(
                title="ClickHouse to DuckDB Type",
                category="query",
                keywords="clickhouse type, type mapping, hogql types, duckdb type, schema, nullable",
                doc_llm=(
                    "Return the DuckDB type name that hogql() gives a column of the given ClickHouse type, as "
                    "PostHog reports it in a HogQL response's types: 'String' gives `VARCHAR`, "
                    "\"DateTime64(6, 'UTC')\" gives `TIMESTAMP WITH TIME ZONE`, 'Array(String)' gives "
                    "`VARCHAR[]`, 'Map(String, String)' gives `MAP(VARCHAR, VARCHAR)`, named tuples become "
                    "`STRUCT`, 'UInt64' gives `UBIGINT`. Nullable and LowCardinality wrappers are unwrapped "
                    "(every hogql() column is nullable); unknown types fall back to `VARCHAR`. Runs locally, "
                    "no PostHog connection or secret needed. NULL in, NULL out."
                ),
                doc_md=(
                    "Predict the DuckDB type of a `hogql()` result column from its ClickHouse type, without "
                    "calling PostHog. Works offline; NULL in, NULL out."
                ),
            ),
            "domain": "product-analytics",
            "vgi.executable_examples": json.dumps(
                [
                    {
                        "description": "How common HogQL result types land in DuckDB",
                        "sql": (
                            "SELECT t, posthog.main.clickhouse_type_to_duckdb(t) AS duckdb_type FROM (VALUES "
                            "('Nullable(String)'), ('DateTime64(6, ''UTC'')'), ('Array(UInt8)'), "
                            "('Decimal(18, 4)')) AS v(t) ORDER BY t"
                        ),
                        "expected_result": [
                            {"t": "Array(UInt8)", "duckdb_type": "UTINYINT[]"},
                            {"t": "DateTime64(6, 'UTC')", "duckdb_type": "TIMESTAMP WITH TIME ZONE"},
                            {"t": "Decimal(18, 4)", "duckdb_type": "DECIMAL(18,4)"},
                            {"t": "Nullable(String)", "duckdb_type": "VARCHAR"},
                        ],
                    }
                ]
            ),
        }
        examples = [
            FunctionExample(
                sql=("SELECT posthog.main.clickhouse_type_to_duckdb('Tuple(label String, rank UInt8)') AS duckdb_type"),
                description="A named ClickHouse tuple becomes a DuckDB STRUCT",
            ),
        ]

    @classmethod
    def compute(
        cls,
        ch_type: Annotated[
            pa.StringArray,
            Param(doc="ClickHouse type string from a HogQL response, e.g. 'Nullable(DateTime64(6, ''UTC''))'."),
        ],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Map every ClickHouse type string in the column; NULL stays NULL."""
        out = [None if v is None else duckdb_type_name(arrow_type(v)[0]) for v in ch_type.to_pylist()]
        return pa.array(out, type=pa.string())
