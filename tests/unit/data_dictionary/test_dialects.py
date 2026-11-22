"""Unit tests for built-in data dictionary dialect registration."""

import ast
import importlib
from pathlib import Path
from typing import Any

import pytest
from sqlglot import exp
from tools.scripts.mypyc_inventory import build_inventory, validate_inventory

from sqlspec.data_dictionary import get_dialect_config
from sqlspec.driver import AsyncDataDictionaryBase, SyncDataDictionaryBase

PROJECT_ROOT = Path(__file__).resolve().parents[3]

SHARED_DIALECT_PACKAGES: tuple[str, ...] = (
    "bigquery",
    "cockroachdb",
    "db2",
    "duckdb",
    "mariadb",
    "mssql",
    "mysql",
    "oracle",
    "postgres",
    "snowflake",
    "spanner",
    "sqlanywhere",
    "sqlite",
    "sybase",
    "teradata",
)

SHARED_DIALECT_BASE_CLASSES: tuple[tuple[str, str, str], ...] = (
    ("mysql", "MySQLSyncDataDictionary", "mysql"),
    ("mysql", "MySQLAsyncDataDictionary", "mysql"),
    ("mariadb", "MariaDBSyncDataDictionary", "mariadb"),
    ("mariadb", "MariaDBAsyncDataDictionary", "mariadb"),
    ("postgres", "PostgresSyncDataDictionary", "postgres"),
    ("postgres", "PostgresAsyncDataDictionary", "postgres"),
    ("cockroachdb", "CockroachDBSyncDataDictionary", "cockroachdb"),
    ("cockroachdb", "CockroachDBAsyncDataDictionary", "cockroachdb"),
    ("sqlite", "SqliteSyncDataDictionary", "sqlite"),
    ("sqlite", "SqliteAsyncDataDictionary", "sqlite"),
    ("mssql", "MssqlSyncDataDictionary", "mssql"),
    ("oracle", "OracleSyncDataDictionary", "oracle"),
    ("oracle", "OracleAsyncDataDictionary", "oracle"),
    ("duckdb", "DuckDBSyncDataDictionary", "duckdb"),
    ("bigquery", "BigQuerySyncDataDictionary", "bigquery"),
    ("spanner", "SpannerSyncDataDictionary", "spanner"),
    ("spanner", "SpannerAsyncDataDictionary", "spanner"),
    ("db2", "Db2SyncDataDictionary", "db2"),
    ("db2", "Db2AsyncDataDictionary", "db2"),
    ("snowflake", "SnowflakeSyncDataDictionary", "snowflake"),
    ("sybase", "SybaseSyncDataDictionary", "sybase"),
    ("sqlanywhere", "SqlanywhereSyncDataDictionary", "sqlanywhere"),
    ("teradata", "TeradataSyncDataDictionary", "teradata"),
)

ADAPTER_DICTIONARY_SUBCLASSES: tuple[tuple[str, str, str, str, str, str | None], ...] = (
    ("pymysql", "PyMysqlDataDictionary", "mysql", "MySQLSyncDataDictionary", "mysql", "pymysql"),
    ("aiomysql", "AiomysqlDataDictionary", "mysql", "MySQLAsyncDataDictionary", "mysql", "aiomysql"),
    ("asyncmy", "AsyncmyDataDictionary", "mysql", "MySQLAsyncDataDictionary", "mysql", "asyncmy"),
    (
        "mysqlconnector",
        "MysqlConnectorSyncDataDictionary",
        "mysql",
        "MySQLSyncDataDictionary",
        "mysql",
        "mysql.connector",
    ),
    (
        "mysqlconnector",
        "MysqlConnectorAsyncDataDictionary",
        "mysql",
        "MySQLAsyncDataDictionary",
        "mysql",
        "mysql.connector",
    ),
    ("asyncpg", "AsyncpgDataDictionary", "postgres", "PostgresAsyncDataDictionary", "postgres", "asyncpg"),
    ("psqlpy", "PsqlpyDataDictionary", "postgres", "PostgresAsyncDataDictionary", "postgres", "psqlpy"),
    ("psycopg", "PsycopgSyncDataDictionary", "postgres", "PostgresSyncDataDictionary", "postgres", "psycopg"),
    ("psycopg", "PsycopgAsyncDataDictionary", "postgres", "PostgresAsyncDataDictionary", "postgres", "psycopg"),
    (
        "cockroach_asyncpg",
        "CockroachAsyncpgDataDictionary",
        "cockroachdb",
        "CockroachDBAsyncDataDictionary",
        "cockroachdb",
        "asyncpg",
    ),
    (
        "cockroach_psycopg",
        "CockroachPsycopgSyncDataDictionary",
        "cockroachdb",
        "CockroachDBSyncDataDictionary",
        "cockroachdb",
        "psycopg",
    ),
    (
        "cockroach_psycopg",
        "CockroachPsycopgAsyncDataDictionary",
        "cockroachdb",
        "CockroachDBAsyncDataDictionary",
        "cockroachdb",
        "psycopg",
    ),
    ("sqlite", "SqliteDataDictionary", "sqlite", "SqliteSyncDataDictionary", "sqlite", None),
    ("aiosqlite", "AiosqliteDataDictionary", "sqlite", "SqliteAsyncDataDictionary", "sqlite", "aiosqlite"),
    ("mssql_python", "MssqlPythonSyncDataDictionary", "mssql", "MssqlSyncDataDictionary", "mssql", "mssql_python"),
    ("pymssql", "PymssqlSyncDataDictionary", "mssql", "MssqlSyncDataDictionary", "mssql", "pymssql"),
    ("oracledb", "OracledbSyncDataDictionary", "oracle", "OracleSyncDataDictionary", "oracle", "oracledb"),
    ("oracledb", "OracledbAsyncDataDictionary", "oracle", "OracleAsyncDataDictionary", "oracle", "oracledb"),
    ("duckdb", "DuckDBDataDictionary", "duckdb", "DuckDBSyncDataDictionary", "duckdb", "duckdb"),
    (
        "bigquery",
        "BigQueryDataDictionary",
        "bigquery",
        "BigQuerySyncDataDictionary",
        "bigquery",
        "google.cloud.bigquery",
    ),
    (
        "spanner",
        "SpannerSyncDataDictionary",
        "spanner",
        "SpannerSyncDataDictionary",
        "spanner",
        "google.cloud.spanner_v1",
    ),
    (
        "spanner",
        "SpannerAsyncDataDictionary",
        "spanner",
        "SpannerAsyncDataDictionary",
        "spanner",
        "google.cloud.spanner_v1",
    ),
    ("db2", "Db2SyncDataDictionary", "db2", "Db2SyncDataDictionary", "db2", "ibm_db"),
    ("db2", "Db2AsyncDataDictionary", "db2", "Db2AsyncDataDictionary", "db2", "ibm_db"),
    ("snowflake", "SnowflakeSyncDataDictionary", "snowflake", "SnowflakeSyncDataDictionary", "snowflake", None),
    ("sybase", "SybaseSyncDataDictionary", "sybase", "SybaseSyncDataDictionary", "sybase", "pymssql"),
    (
        "sqlanywhere",
        "SqlanywhereSyncDataDictionary",
        "sqlanywhere",
        "SqlanywhereSyncDataDictionary",
        "sqlanywhere",
        "sqlanydb",
    ),
    ("teradata", "TeradataSyncDataDictionary", "teradata", "TeradataSyncDataDictionary", "teradata", None),
)

MULTI_DIALECT_ADAPTER_DICTIONARIES: tuple[tuple[str, str, str], ...] = (
    ("adbc", "AdbcDataDictionary", "adbc_driver_manager"),
    ("arrow_odbc", "ArrowOdbcDataDictionary", "arrow_odbc"),
)

ADAPTER_DICTIONARY_EXPORTS: tuple[tuple[str, tuple[str, ...], str | None], ...] = (
    ("pymysql", ("PyMysqlDataDictionary",), "pymysql"),
    ("aiomysql", ("AiomysqlDataDictionary",), "aiomysql"),
    ("asyncmy", ("AsyncmyDataDictionary",), "asyncmy"),
    ("mysqlconnector", ("MysqlConnectorAsyncDataDictionary", "MysqlConnectorSyncDataDictionary"), "mysql.connector"),
    ("asyncpg", ("AsyncpgDataDictionary",), "asyncpg"),
    ("psqlpy", ("PsqlpyDataDictionary",), "psqlpy"),
    ("psycopg", ("PsycopgAsyncDataDictionary", "PsycopgSyncDataDictionary"), "psycopg"),
    ("cockroach_asyncpg", ("CockroachAsyncpgDataDictionary",), "asyncpg"),
    ("cockroach_psycopg", ("CockroachPsycopgAsyncDataDictionary", "CockroachPsycopgSyncDataDictionary"), "psycopg"),
    ("sqlite", ("SqliteDataDictionary",), None),
    ("aiosqlite", ("AiosqliteDataDictionary",), "aiosqlite"),
    ("mssql_python", ("MssqlPythonSyncDataDictionary", "MssqlVersionInfo"), "mssql_python"),
    ("pymssql", ("MssqlVersionInfo", "PymssqlSyncDataDictionary"), "pymssql"),
    (
        "oracledb",
        (
            "JSONStorageType",
            "OracleVersionCache",
            "OracleVersionInfo",
            "OracledbAsyncDataDictionary",
            "OracledbSyncDataDictionary",
            "resolve_oracle_connection_compatible_major",
            "resolve_oracle_connection_major",
            "storage_type_from_version",
        ),
        "oracledb",
    ),
    ("duckdb", ("DuckDBDataDictionary",), "duckdb"),
    ("bigquery", ("BigQueryDataDictionary", "BigQueryMetadataScope"), "google.cloud.bigquery"),
    ("spanner", ("SpannerAsyncDataDictionary", "SpannerSyncDataDictionary"), "google.cloud.spanner_v1"),
    (
        "db2",
        (
            "DB2_CONFIG",
            "Db2AsyncDataDictionary",
            "Db2SyncDataDictionary",
            "Db2VersionInfo",
            "build_column_metadata",
            "build_foreign_key_metadata",
            "build_index_metadata",
            "parse_db2_version",
        ),
        "ibm_db",
    ),
    ("snowflake", ("SnowflakeSyncDataDictionary",), None),
    (
        "sybase",
        (
            "SYBASE_CONFIG",
            "SybaseDataDictionary",
            "SybaseSyncDataDictionary",
            "SybaseVersionInfo",
            "build_column_metadata",
            "build_foreign_key_metadata",
            "build_index_metadata",
            "parse_sybase_version",
        ),
        "pymssql",
    ),
    (
        "sqlanywhere",
        (
            "SQLANYWHERE_CONFIG",
            "SqlanywhereDataDictionary",
            "SqlanywhereSyncDataDictionary",
            "SqlanywhereVersionInfo",
            "build_column_metadata",
            "build_foreign_key_metadata",
            "build_index_metadata",
            "parse_sqlanywhere_version",
        ),
        "sqlanydb",
    ),
    (
        "teradata",
        (
            "TERADATA_CONFIG",
            "TeradataDataDictionary",
            "TeradataSyncDataDictionary",
            "TeradataVersionInfo",
            "build_column_metadata",
            "build_foreign_key_metadata",
            "build_index_metadata",
            "parse_teradata_version",
        ),
        None,
    ),
)


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


def test_mypyc_inventory_validates_shared_dialect_surface() -> None:
    """MyPyC inventory preserves compiled counts with the dialects dictionary glob exclusion."""
    errors = validate_inventory(PROJECT_ROOT)
    assert errors == []

    inventory = build_inventory(PROJECT_ROOT)
    summary = inventory["summary"]
    assert summary["compiled_count"] == 273
    assert summary["interpreted_count"] == 371
    assert summary["total_modules"] == 644
    assert "sqlspec/data_dictionary/dialects/**/dictionary.py" in inventory["preserved_exclusions"]


@pytest.mark.parametrize("dialect", SHARED_DIALECT_PACKAGES)
def test_shared_dialect_dictionary_classified_hard_block(dialect: str) -> None:
    """Each shared dialect dictionary module stays interpreted and classified as hard_block."""
    inventory = build_inventory(PROJECT_ROOT)
    module_path = f"sqlspec/data_dictionary/dialects/{dialect}/dictionary.py"

    assert module_path in inventory["interpreted_modules"]
    assert module_path not in inventory["compiled_modules"]
    hot_entry = inventory["hot_surfaces"][module_path]
    assert hot_entry["classification"] == "hard_block"
    assert hot_entry["status"] == "interpreted"
    assert hot_entry["surface"] == "hard_block"


@pytest.mark.parametrize(("dialect", "class_name", "expected_dialect"), SHARED_DIALECT_BASE_CLASSES)
def test_shared_dialect_base_class_instantiation(dialect: str, class_name: str, expected_dialect: str) -> None:
    """Shared dialect base dictionary classes instantiate directly with the expected dialect attribute."""
    package = importlib.import_module(f"sqlspec.data_dictionary.dialects.{dialect}")
    cls = getattr(package, class_name)
    instance = cls()

    assert cls.dialect == expected_dialect
    assert instance.dialect == expected_dialect
    assert isinstance(instance, (SyncDataDictionaryBase, AsyncDataDictionaryBase))
    if dialect == "spanner":
        assert getattr(instance, "mode", None) == "googlesql"


@pytest.mark.parametrize(
    ("adapter_name", "adapter_class", "base_dialect", "base_class", "expected_dialect", "driver_dep"),
    ADAPTER_DICTIONARY_SUBCLASSES,
)
def test_adapter_dictionary_subclass_linkage(
    adapter_name: str,
    adapter_class: str,
    base_dialect: str,
    base_class: str,
    expected_dialect: str,
    driver_dep: str | None,
) -> None:
    """Adapter data dictionaries subclass their shared dialect base without aliasing it."""
    if driver_dep is not None:
        pytest.importorskip(driver_dep)

    adapter_module = importlib.import_module(f"sqlspec.adapters.{adapter_name}.data_dictionary")
    base_module = importlib.import_module(f"sqlspec.data_dictionary.dialects.{base_dialect}.dictionary")
    adapter_cls = getattr(adapter_module, adapter_class)
    shared_base = getattr(base_module, base_class)
    instance = adapter_cls()

    assert issubclass(adapter_cls, shared_base)
    assert adapter_cls is not shared_base
    assert isinstance(instance, (SyncDataDictionaryBase, AsyncDataDictionaryBase))
    assert adapter_cls.dialect == expected_dialect
    assert instance.dialect == expected_dialect


@pytest.mark.parametrize(("adapter_name", "adapter_class", "driver_dep"), MULTI_DIALECT_ADAPTER_DICTIONARIES)
def test_multi_dialect_adapter_dictionaries_stay_independent(
    adapter_name: str, adapter_class: str, driver_dep: str
) -> None:
    """Multi-dialect dispatchers inherit directly from SyncDataDictionaryBase without single-dialect bases."""
    pytest.importorskip(driver_dep)

    adapter_module = importlib.import_module(f"sqlspec.adapters.{adapter_name}.data_dictionary")
    adapter_cls = getattr(adapter_module, adapter_class)
    shared_bases = tuple(
        getattr(importlib.import_module(f"sqlspec.data_dictionary.dialects.{dialect}.dictionary"), class_name)
        for dialect, class_name, _ in SHARED_DIALECT_BASE_CLASSES
    )

    assert issubclass(adapter_cls, SyncDataDictionaryBase)
    assert adapter_cls.__bases__ == (SyncDataDictionaryBase,)
    assert not issubclass(adapter_cls, shared_bases)


@pytest.mark.parametrize(("adapter_name", "expected_all", "driver_dep"), ADAPTER_DICTIONARY_EXPORTS)
def test_adapter_dictionary_exports(adapter_name: str, expected_all: tuple[str, ...], driver_dep: str | None) -> None:
    """Adapter data_dictionary modules preserve their exact public __all__ exports."""
    if driver_dep is not None:
        pytest.importorskip(driver_dep)

    adapter_module = importlib.import_module(f"sqlspec.adapters.{adapter_name}.data_dictionary")

    assert adapter_module.__all__ == expected_all
    assert all(hasattr(adapter_module, export_name) for export_name in expected_all)


def test_shared_dialect_packages_cover_every_dialect_directory() -> None:
    """Every dialect package directory under sqlspec/data_dictionary/dialects is registered and root AST is clean."""
    dialects_root = PROJECT_ROOT / "sqlspec" / "data_dictionary" / "dialects"
    discovered = tuple(
        sorted(path.name for path in dialects_root.iterdir() if path.is_dir() and not path.name.startswith(("_", ".")))
    )
    assert discovered == SHARED_DIALECT_PACKAGES

    init_file = dialects_root / "__init__.py"
    init_tree = ast.parse(init_file.read_text(encoding="utf-8"), filename=str(init_file))
    pkg_parts = list(init_file.parent.relative_to(PROJECT_ROOT).parts)
    assert _iter_adapter_module_imports(init_tree, pkg_parts) == []
    assert _iter_runtime_dictionary_imports(init_tree, pkg_parts) == []


@pytest.mark.parametrize("dialect", SHARED_DIALECT_PACKAGES)
def test_shared_dialect_package_ast_isolation(dialect: str) -> None:
    """Shared dialect packages avoid sqlspec.adapters imports and keep dictionary imports lazy."""
    dialect_dir = PROJECT_ROOT / "sqlspec" / "data_dictionary" / "dialects" / dialect
    assert dialect_dir.is_dir()

    adapter_violations: list[str] = []
    for py_file in sorted(dialect_dir.rglob("*.py")):
        tree = ast.parse(py_file.read_text(encoding="utf-8"), filename=str(py_file))
        pkg_parts = list(py_file.parent.relative_to(PROJECT_ROOT).parts)
        for lineno, target in _iter_adapter_module_imports(tree, pkg_parts):
            adapter_violations.append(f"{py_file.relative_to(PROJECT_ROOT)}:{lineno}: imports {target}")

    assert not adapter_violations, "Unexpected sqlspec.adapters imports:\n" + "\n".join(adapter_violations)

    init_file = dialect_dir / "__init__.py"
    init_tree = ast.parse(init_file.read_text(encoding="utf-8"), filename=str(init_file))
    init_pkg_parts = list(init_file.parent.relative_to(PROJECT_ROOT).parts)
    runtime_dictionary_imports = _iter_runtime_dictionary_imports(init_tree, init_pkg_parts)
    assert not runtime_dictionary_imports, (
        f"Runtime dictionary imports found in {init_file.relative_to(PROJECT_ROOT)}: {runtime_dictionary_imports}"
    )

    compiled_refs = [
        getattr(node, "lineno", 0)
        for node in ast.walk(init_tree)
        if (isinstance(node, ast.Name) and node.id == "_COMPILED")
        or (isinstance(node, ast.Attribute) and node.attr == "_COMPILED")
    ]
    assert not compiled_refs, f"Unexpected _COMPILED warmup references in {init_file.relative_to(PROJECT_ROOT)}"


def _is_type_checking_guard(test: ast.expr) -> bool:
    return (isinstance(test, ast.Name) and test.id == "TYPE_CHECKING") or (
        isinstance(test, ast.Attribute) and test.attr == "TYPE_CHECKING"
    )


def _resolve_import_from(node: ast.ImportFrom, pkg_parts: list[str]) -> str | None:
    if node.level > 0:
        base_parts = pkg_parts[: -(node.level - 1)] if node.level > 1 else pkg_parts
        target_parts = [*base_parts, *node.module.split(".")] if node.module else base_parts
        return ".".join(target_parts)
    return node.module


def _iter_adapter_module_imports(tree: ast.AST, pkg_parts: list[str]) -> list[tuple[int, str]]:
    """Collect any runtime or TYPE_CHECKING imports targeting sqlspec.adapters."""
    hits: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            lineno = getattr(node, "lineno", 0)
            hits.extend(
                (lineno, alias.name)
                for alias in node.names
                if alias.name == "sqlspec.adapters" or alias.name.startswith("sqlspec.adapters.")
            )
        elif isinstance(node, ast.ImportFrom):
            lineno = getattr(node, "lineno", 0)
            module = _resolve_import_from(node, pkg_parts)
            if module == "sqlspec.adapters" or (module is not None and module.startswith("sqlspec.adapters.")):
                hits.append((lineno, module))
            elif module == "sqlspec":
                hits.extend(
                    (lineno, f"sqlspec.{alias.name}")
                    for alias in node.names
                    if alias.name == "adapters" or alias.name.startswith("adapters.")
                )
    return hits


def _iter_runtime_dictionary_imports(node: ast.AST, pkg_parts: list[str]) -> list[tuple[int, str]]:
    """Collect non-TYPE_CHECKING imports referencing any dialect dictionary module."""
    hits: list[tuple[int, str]] = []
    if isinstance(node, ast.If) and _is_type_checking_guard(node.test):
        for child in node.orelse:
            hits.extend(_iter_runtime_dictionary_imports(child, pkg_parts))
        return hits

    if isinstance(node, ast.Import):
        lineno = getattr(node, "lineno", 0)
        hits.extend(
            (lineno, alias.name)
            for alias in node.names
            if alias.name.endswith(".dictionary") or ".dictionary." in alias.name
        )
        return hits

    if isinstance(node, ast.ImportFrom):
        lineno = getattr(node, "lineno", 0)
        module = _resolve_import_from(node, pkg_parts)
        if module is not None:
            if module.endswith(".dictionary") or ".dictionary." in module:
                hits.append((lineno, module))
            elif module.startswith("sqlspec.data_dictionary.dialects."):
                hits.extend((lineno, f"{module}.dictionary") for alias in node.names if alias.name == "dictionary")
        return hits

    for child in ast.iter_child_nodes(node):
        hits.extend(_iter_runtime_dictionary_imports(child, pkg_parts))
    return hits
