"""Unit tests for built-in data dictionary dialect registration."""

from typing import Any

import pytest
from sqlglot import exp

from sqlspec.data_dictionary import get_dialect_config


def test_mssql_dialect_registered() -> None:
    """The MSSQL data-dictionary dialect should be available by canonical name."""
    cfg = get_dialect_config("mssql")

    assert cfg.name == "mssql"


def test_tsql_alias_resolves_to_mssql() -> None:
    """The tsql alias should resolve to the MSSQL dialect config."""
    assert get_dialect_config("tsql") is get_dialect_config("mssql")


def test_sqlserver_alias_resolves_to_mssql() -> None:
    """The sqlserver alias should resolve to the MSSQL dialect config."""
    assert get_dialect_config("sqlserver") is get_dialect_config("mssql")


@pytest.mark.parametrize(
    ("dialect", "parser_dialect", "expected_types", "unbounded"),
    [
        ("postgres", "postgres", ("INTEGER", "BIGINT", "DOUBLE PRECISION", "VARCHAR(255)"), "TEXT"),
        ("mssql", "tsql", ("INT", "BIGINT", "FLOAT", "NVARCHAR(255)"), "NVARCHAR(MAX)"),
        ("oracle", "oracle", ("NUMBER(10)", "NUMBER(19)", "BINARY_DOUBLE", "VARCHAR2(255)"), "CLOB"),
        ("mysql", "mysql", ("INT", "BIGINT", "DOUBLE", "VARCHAR(255)"), "TEXT"),
        ("mariadb", "mysql", ("INT", "BIGINT", "DOUBLE", "VARCHAR(255)"), "TEXT"),
        ("sqlite", "sqlite", ("INTEGER", "INTEGER", "REAL", "TEXT"), "TEXT"),
        ("duckdb", "duckdb", ("INTEGER", "BIGINT", "DOUBLE", "TEXT"), "TEXT"),
        ("spanner", "spanner", ("INT64", "INT64", "FLOAT64", "STRING(255)"), "STRING(MAX)"),
        ("bigquery", "bigquery", ("INT64", "INT64", "FLOAT64", "STRING"), "STRING"),
        ("cockroachdb", "postgres", ("INT8", "INT8", "FLOAT8", "VARCHAR(255)"), "STRING"),
    ],
)
def test_logical_types_use_dialect_syntax(
    dialect: str, parser_dialect: str, expected_types: tuple[str, ...], unbounded: str
) -> None:
    config = get_dialect_config(dialect)
    for logical_type, expected in zip(("integer", "bigint", "float", "varchar"), expected_types):
        actual = config.get_optimal_type(logical_type, length=255)
        assert actual == expected
        assert isinstance(exp.DataType.build(actual, dialect=parser_dialect), exp.DataType)
    assert config.get_optimal_type("varchar") == unbounded
    assert config.get_optimal_type("text") == unbounded


@pytest.mark.parametrize(
    ("category", "expected"),
    [
        ("json", "VARIANT"),
        ("datetime", "TIMESTAMP_TZ"),
        ("timestamp", "TIMESTAMP_TZ"),
        ("timestamptz", "TIMESTAMP_TZ"),
        ("binary", "BINARY"),
        ("boolean", "BOOLEAN"),
        ("uuid", "VARCHAR(36)"),
        ("integer", "NUMBER(38,0)"),
        ("varchar", "VARCHAR"),
    ],
)
def test_snowflake_dialect_types(category: str, expected: str) -> None:
    """Snowflake catalog configuration exposes lossless native logical types."""
    config = get_dialect_config("snowflake")
    assert config.get_optimal_type(category) == expected
    assert config.default_schema is None
    assert config.parameter_style == "qmark"
    assert config.get_feature_flag("supports_time_travel") is True
    assert config.get_feature_flag("supports_returning") is False


def test_snowflake_composite_primary_and_unique_keys() -> None:
    """SHOW keys preserve catalog identity and composite column order."""
    from sqlspec.data_dictionary.dialects.snowflake import parse_snowflake_primary_or_unique_keys

    rows: list[dict[str, Any]] = [
        {
            "schema_name": "PUBLIC",
            "table_name": "ITEMS",
            "constraint_name": "PK_ITEMS",
            "column_name": "LINE",
            "key_sequence": 2,
        },
        {
            "SCHEMA_NAME": "PUBLIC",
            "TABLE_NAME": "ITEMS",
            "CONSTRAINT_NAME": "PK_ITEMS",
            "COLUMN_NAME": "ORDER_ID",
            "KEY_SEQUENCE": "1",
        },
    ]
    assert parse_snowflake_primary_or_unique_keys(rows) == [
        {
            "schema_name": "PUBLIC",
            "database_name": None,
            "table_name": "ITEMS",
            "constraint_name": "PK_ITEMS",
            "constraint_type": "PRIMARY KEY",
            "column_names": ["ORDER_ID", "LINE"],
            "columns": ["ORDER_ID", "LINE"],
        }
    ]
    unique = parse_snowflake_primary_or_unique_keys(rows, unique=True)
    assert unique[0]["constraint_type"] == "UNIQUE"
    assert parse_snowflake_primary_or_unique_keys(rows, table_name="OTHER") == []


def test_snowflake_imported_keys_order_and_referential_actions() -> None:
    """Foreign keys use fk_name and normalize actions without guessing unnamed keys."""
    from sqlspec.data_dictionary import ForeignKeyMetadata
    from sqlspec.data_dictionary.dialects.snowflake import parse_snowflake_imported_keys

    base: dict[str, Any] = {
        "fk_table_name": "LEDGER",
        "fk_schema_name": "FIN",
        "fk_name": "FK_LEDGER",
        "pk_name": "PK_ACCOUNTS",
        "pk_table_name": "ACCOUNTS",
        "pk_schema_name": "CORE",
        "delete_rule": "set_null",
        "update_rule": "NO ACTION",
    }
    rows: list[dict[str, Any]] = [
        {**base, "fk_column_name": "LINE_ID", "pk_column_name": "ID_2", "key_sequence": 2},
        {**base, "fk_column_name": "ACCT_ID", "pk_column_name": "ID_1", "key_sequence": "1"},
    ]
    assert parse_snowflake_imported_keys(rows) == [
        ForeignKeyMetadata(
            "LEDGER", "ACCT_ID", "ACCOUNTS", "ID_1", "FK_LEDGER", "FIN", "CORE", "SET NULL", "NO ACTION"
        ),
        ForeignKeyMetadata(
            "LEDGER", "LINE_ID", "ACCOUNTS", "ID_2", "FK_LEDGER", "FIN", "CORE", "SET NULL", "NO ACTION"
        ),
    ]
    unnamed = parse_snowflake_imported_keys([{**rows[0], "fk_name": ""}])
    assert unnamed[0].constraint_name is None
    assert parse_snowflake_imported_keys(rows, table_name="OTHER") == []


@pytest.mark.parametrize(
    ("identifier", "dialect", "expected"),
    [
        ("orders", "snowflake", "ORDERS"),
        ("MyTable", "sf", "MYTABLE"),
        ('"MyTable"', "snowflake", "MyTable"),
        ('  "My""Table"  ', "snowflake", 'My"Table'),
    ],
)
def test_snowflake_identifier_folding(identifier: str, dialect: str, expected: str) -> None:
    """Quoted identifiers retain exact case and escaped double quotes."""
    from sqlspec.utils.text import normalize_identifier

    assert normalize_identifier(identifier, dialect) == expected


def test_snowflake_key_parsers_handle_incomplete_catalog_rows() -> None:
    """Incomplete rows do not fabricate keys, and malformed ordinals remain stable."""
    from sqlspec.data_dictionary.dialects.snowflake import (
        parse_snowflake_imported_keys,
        parse_snowflake_primary_or_unique_keys,
    )

    assert parse_snowflake_imported_keys([{}, {"fk_table_name": "ORDERS"}]) == []
    rows: list[dict[str, Any]] = [
        {},
        {"table_name": "ORDERS", "column_name": "ID", "key_sequence": "unknown"},
        {"table_name": "ORDERS", "column_name": "ID", "key_sequence": None},
    ]
    assert parse_snowflake_primary_or_unique_keys(rows)[0]["column_names"] == ["ID"]


_SNOWFLAKE_ROWS = [
    {"schema_name": "PUBLIC", "table_name": "ORDERS", "column_name": "ID"},
    {"schema_name": "public", "table_name": "orders", "column_name": "id"},
    {"schema_name": "app", "table_name": "items", "column_name": "id"},
]


@pytest.mark.parametrize(
    ("schema_name", "schema_exact", "table_name", "table_exact", "expected"),
    [
        ("PUBLIC", False, "ORDERS", False, ["ID"]),
        ("APP", False, "ITEMS", False, ["id"]),
        ("public", True, "orders", True, ["id"]),
        ("APP", True, None, False, []),
        ("app", True, "ITEMS", True, []),
    ],
    ids=["exact-wins", "unique-case-insensitive", "quoted-exact", "quoted-schema-no-fold", "quoted-table-no-fold"],
)
def test_snowflake_catalog_rows_match_exact_then_unique_insensitive(
    schema_name: str, schema_exact: bool, table_name: str | None, table_exact: bool, expected: list[str]
) -> None:
    from sqlspec.data_dictionary.dialects.snowflake import match_snowflake_catalog_rows

    rows = match_snowflake_catalog_rows(
        _SNOWFLAKE_ROWS,
        schema_name=schema_name,
        schema_exact=schema_exact,
        table_name=table_name,
        table_exact=table_exact,
    )

    assert [row["column_name"] for row in rows] == expected


def test_snowflake_catalog_rows_reject_ambiguous_unquoted_names() -> None:
    from sqlspec.data_dictionary.dialects.snowflake import match_snowflake_catalog_rows
    from sqlspec.exceptions import AmbiguousIdentifierError

    with pytest.raises(AmbiguousIdentifierError):
        match_snowflake_catalog_rows(_SNOWFLAKE_ROWS, schema_name="Public", schema_exact=False)


@pytest.mark.parametrize(
    ("schema", "table", "expected"),
    [(None, None, (True, False)), ("app", "orders", (False, False)), ('"app"', '"Orders"', (True, True))],
)
def test_snowflake_scope_exactness_follows_quoting(
    schema: str | None, table: str | None, expected: tuple[bool, bool]
) -> None:
    from sqlspec.data_dictionary.dialects.snowflake import snowflake_scope_exactness

    assert snowflake_scope_exactness(schema, table) == expected
    if table is not None:
        assert snowflake_scope_exactness(None, f"{schema}.{table}") == expected


@pytest.mark.parametrize(
    ("schema", "table", "database", "expected"),
    [
        (None, None, None, "IN SCHEMA"),
        ("SALES", None, None, 'IN SCHEMA "SALES"'),
        ("SALES", None, "DB", 'IN SCHEMA "DB"."SALES"'),
        (None, None, "DB", 'IN DATABASE "DB"'),
        ("sales", "Orders", "DB", 'IN TABLE "DB"."sales"."Orders"'),
        (None, "Orders", None, 'IN TABLE "Orders"'),
    ],
)
def test_snowflake_show_scope_quotes_exact_names(
    schema: str | None, table: str | None, database: str | None, expected: str
) -> None:
    from sqlspec.data_dictionary.dialects.snowflake import format_snowflake_show_scope

    assert format_snowflake_show_scope(schema, table, database_name=database) == expected


def test_snowflake_show_scope_rejects_nul_names() -> None:
    from sqlspec.data_dictionary.dialects.snowflake import format_snowflake_show_scope
    from sqlspec.exceptions import SQLSpecError

    with pytest.raises(SQLSpecError, match="NUL"):
        format_snowflake_show_scope("sales", "orders\x00")
