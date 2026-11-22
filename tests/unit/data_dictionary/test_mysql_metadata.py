"""Unit tests for MySQL-family data-dictionary metadata."""

import subprocess
import sys
from importlib import import_module
from inspect import isawaitable
from typing import Any, cast
from unittest.mock import AsyncMock, Mock

import pytest
import sqlglot
from sqlglot import exp

from sqlspec.adapters.aiomysql.data_dictionary import AiomysqlDataDictionary
from sqlspec.adapters.asyncmy.data_dictionary import AsyncmyDataDictionary
from sqlspec.adapters.mysqlconnector.data_dictionary import MysqlConnectorSyncDataDictionary
from sqlspec.adapters.pymysql.data_dictionary import PyMysqlDataDictionary
from sqlspec.core import SQL
from sqlspec.data_dictionary import (
    DataDictionaryLoader,
    DDLResult,
    MetadataFidelity,
    MetadataRisk,
    MetadataSource,
    MetadataSupport,
    SystemMetadataRequest,
    SystemMetadataResult,
    VersionInfo,
    get_dialect_config,
    normalize_dialect_name,
)
from sqlspec.data_dictionary.dialects.mariadb import MariaDBAsyncDataDictionary, MariaDBSyncDataDictionary
from sqlspec.data_dictionary.dialects.mysql import (
    MARIADB_CONFIG,
    MySQLAsyncDataDictionary,
    MySQLSyncDataDictionary,
    make_mysql_ddl_result,
    resolve_mysql_json_type,
)
from sqlspec.exceptions import SQLSpecError

MYSQL_DIALECT_PACKAGE_EXPORTS: tuple[str, ...] = (
    "MARIADB_CONFIG",
    "MYSQL_CONFIG",
    "MYSQL_VERSION_PATTERN",
    "MySQLAsyncDataDictionary",
    "MySQLEngineVersion",
    "MySQLSyncDataDictionary",
    "build_mysql_metadata_capability_profile",
    "build_mysql_show_create_statement",
    "build_mysql_system_metadata_capability",
    "format_mysql_identifier",
    "make_mysql_ddl_result",
    "mysql_system_metadata_query_name",
    "parse_mysql_engine_version",
    "resolve_mysql_json_type",
)
MARIADB_DIALECT_PACKAGE_EXPORTS: tuple[str, ...] = (
    "MARIADB_CONFIG",
    "MariaDBAsyncDataDictionary",
    "MariaDBSyncDataDictionary",
)


class _SyncDDLDriver:
    def __init__(self) -> None:
        self.statements: list[str] = []
        self.sql_mode = "STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION"
        self.sql_quote_show_create = "1"

    def select_one(self, statement: str, *args: Any, **kwargs: Any) -> dict[str, str]:
        self.statements.append(str(statement))
        query = str(statement)
        if "SELECT VERSION()" in query:
            return {
                "raw_version": "8.4.0",
                "sql_mode": self.sql_mode,
                "sql_quote_show_create": self.sql_quote_show_create,
            }
        return {"Table": "orders", "Create Table": "CREATE TABLE `orders` (`id` int)"}

    def select_value_or_none(self, statement: object, *args: Any, **kwargs: Any) -> str | None:
        self.statements.append(str(statement))
        query = str(statement)
        if "@@sql_mode" in query:
            return "STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION"
        if "@@sql_quote_show_create" in query:
            return "1"
        if "VERSION()" in query:
            return "8.4.0"
        return None

    def select(self, statement: str, *args: Any, **kwargs: Any) -> list[dict[str, object]]:
        self.statements.append(str(statement))
        return [{"table_schema": "shop", "table_name": "orders", "rows_fetched": 5}]


@pytest.mark.parametrize(
    ("version_text", "family", "version_tuple", "variant"),
    (
        ("8.4.0-commercial", "mysql", (8, 4, 0), "commercial"),
        ("10.11.7-MariaDB-1:10.11.7+maria~ubu2204", "mariadb", (10, 11, 7), "MariaDB"),
        ("8.0.34-google", "mysql", (8, 0, 34), "google"),
    ),
)
def test_mysql_detects_engine_family(
    version_text: str, family: str, version_tuple: tuple[int, int, int], variant: str
) -> None:
    """MySQL-family version parsing distinguishes MariaDB from MySQL variants."""
    mysql_dialect = import_module("sqlspec.data_dictionary.dialects.mysql")

    detected = mysql_dialect.parse_mysql_engine_version(version_text)

    assert detected.engine_family == family
    assert detected.version.version_tuple == version_tuple
    assert detected.variant_markers[0] == variant


def test_mariadb_is_registered_as_distinct_dialect() -> None:
    """MariaDB should not normalize to the MySQL query pack or capability profile."""
    assert normalize_dialect_name("mariadb") == "mariadb"
    assert get_dialect_config("mariadb").name == "mariadb"
    assert get_dialect_config("mariadb") is not get_dialect_config("mysql")


@pytest.mark.parametrize(
    ("dialect", "domain", "query_name", "expected_fragment"),
    (
        ("mysql", "schemas", "list", "information_schema.schemata"),
        ("mysql", "objects", "by_schema", "information_schema.events"),
        ("mysql", "constraints", "by_schema", "information_schema.check_constraints"),
        ("mysql", "ddl", "table_by_name", "SHOW CREATE TABLE"),
        ("mysql", "system", "performance_schema_tables", "performance_schema"),
        ("mariadb", "schemas", "list", "information_schema.schemata"),
        ("mariadb", "objects", "by_schema", "SEQUENCE"),
        ("mariadb", "plugins", "list", "information_schema.plugins"),
        ("mariadb", "ddl", "sequence_by_name", "SHOW CREATE SEQUENCE"),
    ),
)
def test_mysql_and_mariadb_domain_query_packs(
    dialect: str, domain: str, query_name: str, expected_fragment: str
) -> None:
    """MySQL and MariaDB direct domain packs expose rich metadata queries."""
    query = DataDictionaryLoader().get_domain_query(dialect, domain, query_name)

    assert query.is_supported is True
    assert query.query_text is not None
    assert expected_fragment.lower() in query.query_text.lower()


def test_mysql_constraints_include_check_unique_and_fk_sources() -> None:
    """Constraint metadata query should cover check, unique, primary, and FK metadata."""
    query_text = DataDictionaryLoader().get_domain_query_text("mysql", "constraints", "by_schema")

    assert query_text is not None
    assert "information_schema.table_constraints" in query_text.lower()
    assert "information_schema.key_column_usage" in query_text.lower()
    assert "information_schema.referential_constraints" in query_text.lower()
    assert "information_schema.check_constraints" in query_text.lower()


def test_mariadb_sequences_not_reported_as_mysql_tables() -> None:
    """MariaDB gets its own object profile instead of lying through MySQL tables."""
    mysql_query = DataDictionaryLoader().get_domain_query_text("mysql", "objects", "by_schema")
    mariadb_query = DataDictionaryLoader().get_domain_query_text("mariadb", "objects", "by_schema")

    assert mysql_query is not None
    assert mariadb_query is not None
    assert mysql_query != mariadb_query
    assert "'SEQUENCE'" not in mysql_query
    assert "'SEQUENCE'" in mariadb_query


@pytest.mark.parametrize("dictionary_type", (MysqlConnectorSyncDataDictionary, PyMysqlDataDictionary))
def test_sync_mysql_dictionary_reports_supported_replacement_domains(dictionary_type: type[Any]) -> None:
    """Sync MySQL-family dictionaries expose replacement-domain capability profiles."""
    profile = dictionary_type().get_metadata_capabilities(_SyncDDLDriver())

    assert profile.dialect == "mysql"
    assert profile.get("schemas").support == MetadataSupport.SUPPORTED
    assert profile.get("constraints").source == MetadataSource.INFORMATION_SCHEMA
    assert profile.get("ddl").fidelity == MetadataFidelity.NATIVE
    assert profile.get("system").support == MetadataSupport.SUPPORTED
    assert profile.get("system").warnings == ("Sensitive system metadata requires explicit opt-in.",)


@pytest.mark.parametrize("dictionary_type", (AiomysqlDataDictionary, AsyncmyDataDictionary))
@pytest.mark.anyio
async def test_async_mysql_dictionary_reports_supported_replacement_domains(dictionary_type: type[Any]) -> None:
    """Async MySQL-family dictionaries expose the same replacement-domain capabilities."""
    profile = await dictionary_type().get_metadata_capabilities(object())

    assert profile.dialect == "mysql"
    assert profile.get("schemas").support == MetadataSupport.SUPPORTED
    assert profile.get("constraints").source == MetadataSource.INFORMATION_SCHEMA
    assert profile.get("ddl").fidelity == MetadataFidelity.NATIVE
    assert profile.get("system").support == MetadataSupport.SUPPORTED


def test_mysql_native_ddl_records_sql_mode() -> None:
    """Native SHOW CREATE helpers quote identifiers and preserve replay-sensitive context."""
    driver = _SyncDDLDriver()
    result = PyMysqlDataDictionary().get_ddl(cast(Any, driver), "orders", schema="shop")

    assert isinstance(result, DDLResult)
    assert result.status == MetadataSupport.SUPPORTED
    assert result.fidelity == MetadataFidelity.NATIVE
    assert result.source == MetadataSource.NATIVE_API
    assert result.context == {
        "engine_family": "mysql",
        "server_version": "8.4.0",
        "sql_mode": "STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION",
        "sql_quote_show_create": "1",
    }
    assert driver.statements[-1] == "SHOW CREATE TABLE `shop`.`orders`"


def test_mysql_system_metadata_is_gated_by_default() -> None:
    """System metadata should require explicit opt-in even when the domain is supported."""
    driver = _SyncDDLDriver()
    result = PyMysqlDataDictionary().get_system_metadata(cast(Any, driver), "table_statistics")

    assert isinstance(result, SystemMetadataResult)
    assert result.capability.support == MetadataSupport.GATED
    assert MetadataRisk.REDACTED in result.capability.risks
    assert not result.rows


def test_mysql_system_metadata_returns_rows_when_opted_in() -> None:
    """Opted-in system metadata should use the shared result envelope and redaction path."""
    driver = _SyncDDLDriver()
    request = SystemMetadataRequest("table_statistics", include_system=True, schema="shop")
    result = PyMysqlDataDictionary().get_system_metadata(cast(Any, driver), request)

    assert isinstance(result, SystemMetadataResult)
    assert result.capability.support == MetadataSupport.SUPPORTED
    assert result.rows == ({"table_schema": "shop", "table_name": "orders", "rows_fetched": 5},)
    assert "sys.schema_table_statistics" in driver.statements[-1]


def test_mariadb_columns_derive_primary_key_flag_from_column_key() -> None:
    """MariaDB column metadata projects ``is_primary`` from the ``column_key`` value."""
    columns = DataDictionaryLoader().get_domain_query_text("mariadb", "columns", "by_schema")

    assert columns is not None
    select = sqlglot.parse_one(columns, read="mysql")
    assert isinstance(select, exp.Select)
    projections = {
        projection.alias_or_name: projection.this.sql(dialect="mysql")
        for projection in select.selects
        if isinstance(projection, exp.Alias)
    }
    assert projections["is_primary"] == "column_key = 'PRI'"


def test_mariadb_complete_query_pack_and_mysql_single_version_probe() -> None:
    """MariaDB exposes full query pack and MySQL get_version uses a single cached VERSION() probe."""
    loader = DataDictionaryLoader()
    for domain, query_name in (
        ("foreign_keys", "by_schema"),
        ("foreign_keys", "by_table"),
        ("version", "version"),
        ("columns", "by_table"),
        ("indexes", "by_table"),
        ("tables", "by_schema"),
    ):
        query_text = loader.get_domain_query_text("mariadb", domain, query_name)
        assert query_text is not None, f"mariadb/{domain}/{query_name} must exist"

    mariadb_tables = loader.get_domain_query_text("mariadb", "tables", "by_schema")
    assert mariadb_tables is not None and "dependency_levels" in mariadb_tables

    mysql_cols_by_table = loader.get_domain_query_text("mysql", "columns", "by_table")
    mariadb_cols_by_table = loader.get_domain_query_text("mariadb", "columns", "by_table")
    assert mysql_cols_by_table is not None and "generation_expression" in mysql_cols_by_table
    assert mariadb_cols_by_table is not None and "generation_expression" in mariadb_cols_by_table

    driver = _SyncDDLDriver()
    dictionary = PyMysqlDataDictionary()
    version = dictionary.get_version(cast(Any, driver))
    version_again = dictionary.get_version(cast(Any, driver))

    assert version is not None
    assert version.version_tuple == (8, 4, 0)
    assert version_again == version
    assert len(driver.statements) == 1


def test_mysql_object_ddl_routes_through_native_hook() -> None:
    """get_object_ddl and get_ddl use SHOW CREATE through the native hook; prefer_native=False skips it."""
    native_driver = _SyncDDLDriver()
    dictionary = PyMysqlDataDictionary()

    object_ddl = dictionary.get_object_ddl(cast(Any, native_driver), "orders", schema="`shop`")
    assert object_ddl.fidelity == MetadataFidelity.NATIVE
    assert object_ddl.ddl == "CREATE TABLE `orders` (`id` int)"
    assert native_driver.statements[-1] == "SHOW CREATE TABLE `shop`.`orders`"

    class _EmptyCatalogDriver(_SyncDDLDriver):
        def select(self, statement: str, *args: Any, **kwargs: Any) -> list[dict[str, object]]:
            self.statements.append(str(statement))
            return []

    synth_driver = _EmptyCatalogDriver()
    synthesized = dictionary.get_object_ddl(cast(Any, synth_driver), "orders", schema="shop", prefer_native=False)
    assert synthesized.status == MetadataSupport.UNSUPPORTED
    assert not any(statement.startswith("SHOW CREATE") for statement in synth_driver.statements)


@pytest.mark.parametrize("dictionary_type", [PyMysqlDataDictionary, AiomysqlDataDictionary])
async def test_mysql_native_ddl_refreshes_session_context(dictionary_type: type[Any]) -> None:
    """DDL captures mutable SQL mode and quote settings for every statement."""
    session = _SyncDDLDriver()
    driver = (
        Mock(select_one=AsyncMock(side_effect=session.select_one))
        if dictionary_type is AiomysqlDataDictionary
        else session
    )
    dictionary = dictionary_type()
    for sql_mode, quotes in [(session.sql_mode, "1"), ("NO_BACKSLASH_ESCAPES", "0")]:
        session.sql_mode = sql_mode
        session.sql_quote_show_create = quotes
        result = dictionary.get_ddl(driver, "orders", schema="shop")
        if isawaitable(result):
            result = await result
        assert result.context["sql_mode"] == sql_mode
        assert result.context["sql_quote_show_create"] == quotes


@pytest.mark.parametrize("dictionary_type", [PyMysqlDataDictionary, AiomysqlDataDictionary])
@pytest.mark.parametrize("object_name", [None, "orders"])
async def test_mysql_get_privileges_binds_optional_object_name(
    dictionary_type: type[Any], object_name: str | None
) -> None:
    from sqlspec.adapters.pymysql.core import default_statement_config

    def select_rows(statement: str, **parameters: Any) -> list[dict[str, str]]:
        compiled_sql, bound = SQL(str(statement), statement_config=default_statement_config, **parameters).compile()
        assert "information_schema.table_privileges" in compiled_sql
        assert bound == {"schema_name": "shop", "object_name": object_name}
        return [{"table_name": "orders"}]

    is_async = dictionary_type is AiomysqlDataDictionary
    driver = Mock(
        select=AsyncMock(side_effect=select_rows) if is_async else Mock(side_effect=select_rows),
        select_value_or_none=AsyncMock(return_value="8.4.0") if is_async else Mock(return_value="8.4.0"),
    )
    result = dictionary_type().get_privileges(driver, object_name=object_name, schema="shop")
    if isawaitable(result):
        result = await result
    assert result.items == ({"table_name": "orders"},)


@pytest.mark.parametrize("dictionary_type", [PyMysqlDataDictionary, AiomysqlDataDictionary])
@pytest.mark.parametrize("table_name", [None, "orders"])
async def test_mysql_get_indexes_binds_optional_table_name(dictionary_type: type[Any], table_name: str | None) -> None:
    """get_indexes binds table_name across schema-wide and table-scoped MySQL queries."""
    from sqlspec.adapters.pymysql.core import default_statement_config

    def select_rows(statement: str, **parameters: Any) -> list[dict[str, Any]]:
        compiled_sql, bound = SQL(str(statement), statement_config=default_statement_config, **parameters).compile()
        assert "information_schema.statistics" in compiled_sql
        assert bound["schema_name"] == "shop"
        assert bound["table_name"] == table_name
        return []

    is_async = dictionary_type is AiomysqlDataDictionary
    driver = Mock(
        dialect="mysql",
        select=AsyncMock(side_effect=select_rows) if is_async else Mock(side_effect=select_rows),
        select_value_or_none=AsyncMock(return_value="8.4.0") if is_async else Mock(return_value="8.4.0"),
    )
    result = dictionary_type().get_indexes(driver, table=table_name, schema="shop")
    if isawaitable(result):
        result = await result
    assert result == []


@pytest.mark.parametrize("dictionary_type", [PyMysqlDataDictionary, AiomysqlDataDictionary])
@pytest.mark.parametrize("table_name", [None, "orders"])
async def test_mysql_get_constraints_binds_optional_table_name(
    dictionary_type: type[Any], table_name: str | None
) -> None:
    """get_constraints binds table_name cleanly when inspecting whole schemas or single tables."""
    from sqlspec.adapters.pymysql.core import default_statement_config

    def select_rows(statement: str, **parameters: Any) -> list[dict[str, str]]:
        compiled_sql, bound = SQL(str(statement), statement_config=default_statement_config, **parameters).compile()
        assert "information_schema.table_constraints" in compiled_sql
        assert bound == {"schema_name": "shop", "table_name": table_name}
        return [{"table_name": "orders", "constraint_name": "pk_orders"}]

    is_async = dictionary_type is AiomysqlDataDictionary
    driver = Mock(
        select=AsyncMock(side_effect=select_rows) if is_async else Mock(side_effect=select_rows),
        select_value_or_none=AsyncMock(return_value="8.4.0") if is_async else Mock(return_value="8.4.0"),
    )
    result = dictionary_type().get_constraints(driver, table=table_name, schema="shop")
    if isawaitable(result):
        result = await result
    assert result.items == ({"table_name": "orders", "constraint_name": "pk_orders"},)


@pytest.mark.parametrize(
    ("package_name", "dictionary_module_name", "class_name", "expected_all"),
    (
        (
            "sqlspec.data_dictionary.dialects.mysql",
            "sqlspec.data_dictionary.dialects.mysql.dictionary",
            "MySQLAsyncDataDictionary",
            MYSQL_DIALECT_PACKAGE_EXPORTS,
        ),
        (
            "sqlspec.data_dictionary.dialects.mysql",
            "sqlspec.data_dictionary.dialects.mysql.dictionary",
            "MySQLSyncDataDictionary",
            MYSQL_DIALECT_PACKAGE_EXPORTS,
        ),
        (
            "sqlspec.data_dictionary.dialects.mariadb",
            "sqlspec.data_dictionary.dialects.mariadb.dictionary",
            "MariaDBAsyncDataDictionary",
            MARIADB_DIALECT_PACKAGE_EXPORTS,
        ),
        (
            "sqlspec.data_dictionary.dialects.mariadb",
            "sqlspec.data_dictionary.dialects.mariadb.dictionary",
            "MariaDBSyncDataDictionary",
            MARIADB_DIALECT_PACKAGE_EXPORTS,
        ),
    ),
)
def test_mysql_and_mariadb_package_lazy_exports(
    package_name: str, dictionary_module_name: str, class_name: str, expected_all: tuple[str, ...]
) -> None:
    """MySQL and MariaDB dialect packages expose lazy PEP 562 dictionary re-exports."""
    package = import_module(package_name)
    dictionary_module = import_module(dictionary_module_name)

    assert package.__all__ == expected_all
    assert class_name in dir(package)
    exported = getattr(package, class_name)
    assert exported is getattr(dictionary_module, class_name)
    assert package.__dict__[class_name] is exported

    with pytest.raises(AttributeError, match="has no attribute 'NonExistentExport'"):
        getattr(package, "NonExistentExport")


def test_mariadb_dictionary_classes_inherit_mysql_bases() -> None:
    """MariaDB sync and async data dictionary bases inherit the shared MySQL bases."""
    assert issubclass(MariaDBSyncDataDictionary, MySQLSyncDataDictionary)
    assert issubclass(MariaDBAsyncDataDictionary, MySQLAsyncDataDictionary)
    assert MariaDBSyncDataDictionary.dialect == "mariadb"
    assert MariaDBAsyncDataDictionary.dialect == "mariadb"


def test_mariadb_and_mysql_packages_remain_lazy_on_bare_import() -> None:
    """Bare dialect package imports do not eagerly load dictionary modules or sqlspec.driver."""
    probe = (
        "import sys; "
        "import sqlspec.data_dictionary.dialects as dialects; "
        "import sqlspec.data_dictionary.dialects.mysql as mysql_pkg; "
        "import sqlspec.data_dictionary.dialects.mariadb as mariadb_pkg; "
        "assert 'sqlspec.data_dictionary.dialects.mysql.dictionary' not in sys.modules; "
        "assert 'sqlspec.data_dictionary.dialects.mariadb.dictionary' not in sys.modules; "
        "assert 'sqlspec.driver' not in sys.modules; "
        "assert 'MySQLSyncDataDictionary' in dir(mysql_pkg); "
        "assert 'MySQLAsyncDataDictionary' in dir(mysql_pkg); "
        "assert 'MariaDBSyncDataDictionary' in dir(mariadb_pkg); "
        "assert 'MariaDBAsyncDataDictionary' in dir(mariadb_pkg); "
        "assert 'sqlspec.data_dictionary.dialects.mysql.dictionary' not in sys.modules; "
        "assert 'sqlspec.data_dictionary.dialects.mariadb.dictionary' not in sys.modules; "
        "assert dialects.MARIADB_CONFIG is mariadb_pkg.MARIADB_CONFIG"
    )
    completed = subprocess.run([sys.executable, "-c", probe], capture_output=True, text=True, check=False)
    assert completed.returncode == 0, completed.stderr or completed.stdout


@pytest.mark.parametrize(
    ("version_info", "config", "expected"),
    (
        (VersionInfo(10, 1, 48), MARIADB_CONFIG, "TEXT"),
        (VersionInfo(10, 2, 7), MARIADB_CONFIG, "JSON"),
        (VersionInfo(5, 7, 8), MARIADB_CONFIG, "TEXT"),
        (None, MARIADB_CONFIG, "TEXT"),
        (VersionInfo(5, 7, 7), None, "TEXT"),
        (VersionInfo(5, 7, 8), None, "JSON"),
    ),
)
def test_mariadb_resolve_mysql_json_type_threshold(
    version_info: VersionInfo | None, config: Any, expected: str
) -> None:
    """resolve_mysql_json_type enforces the supplied dialect config threshold."""
    assert resolve_mysql_json_type(version_info, config=config) == expected


def test_resolve_mysql_json_type_single_argument_compat() -> None:
    """Single-argument resolve_mysql_json_type calls default to the MySQL 5.7.8 threshold."""
    assert resolve_mysql_json_type(VersionInfo(5, 7, 8)) == "JSON"
    assert resolve_mysql_json_type(VersionInfo(5, 7, 7)) == "TEXT"
    assert resolve_mysql_json_type(None) == "TEXT"


@pytest.mark.parametrize(
    ("dictionary_type", "is_async", "version_text", "expected"),
    (
        (MariaDBSyncDataDictionary, False, "10.1.48-MariaDB", "TEXT"),
        (MariaDBSyncDataDictionary, False, "10.2.7-MariaDB", "JSON"),
        (MariaDBSyncDataDictionary, False, "5.7.8", "TEXT"),
        (MariaDBSyncDataDictionary, False, None, "TEXT"),
        (MariaDBAsyncDataDictionary, True, "10.1.48-MariaDB", "TEXT"),
        (MariaDBAsyncDataDictionary, True, "10.2.7-MariaDB", "JSON"),
        (MariaDBAsyncDataDictionary, True, "5.7.8", "TEXT"),
        (MariaDBAsyncDataDictionary, True, None, "TEXT"),
        (MySQLSyncDataDictionary, False, "5.7.7", "TEXT"),
        (MySQLSyncDataDictionary, False, "5.7.8", "JSON"),
        (MySQLAsyncDataDictionary, True, "5.7.7", "TEXT"),
        (MySQLAsyncDataDictionary, True, "5.7.8", "JSON"),
    ),
)
async def test_mariadb_and_mysql_get_optimal_type_json_threshold(
    dictionary_type: type[Any], is_async: bool, version_text: str | None, expected: str
) -> None:
    """get_optimal_type uses each dictionary dialect's supports_json threshold."""
    driver = Mock(
        select_value_or_none=AsyncMock(return_value=version_text) if is_async else Mock(return_value=version_text)
    )
    result = dictionary_type().get_optimal_type(driver, "json")
    if isawaitable(result):
        result = await result
    assert result == expected


@pytest.mark.parametrize(
    ("dictionary_type", "is_async"), ((MariaDBSyncDataDictionary, False), (MariaDBAsyncDataDictionary, True))
)
@pytest.mark.parametrize("raw_version", (None, "not-a-version"))
async def test_mariadb_get_ddl_success_without_parsed_version(
    dictionary_type: type[Any], is_async: bool, raw_version: str | None
) -> None:
    """Public get_ddl falls back to mariadb dialect when version cannot be parsed."""

    def select_one(statement: str, **kwargs: Any) -> dict[str, object]:
        _ = kwargs
        if "SELECT VERSION()" in statement:
            return {"raw_version": raw_version, "sql_mode": "STRICT_TRANS_TABLES", "sql_quote_show_create": "1"}
        return {"View": "v_orders", "Create View": "CREATE VIEW `v_orders` AS SELECT 1"}

    driver = Mock(select_one=AsyncMock(side_effect=select_one) if is_async else Mock(side_effect=select_one))
    result = dictionary_type().get_ddl(driver, "v_orders", schema="shop", object_type="view")
    if isawaitable(result):
        result = await result

    assert result.status == MetadataSupport.SUPPORTED
    assert result.identity.dialect == "mariadb"
    assert result.context["engine_family"] == "mariadb"
    assert result.ddl == "CREATE VIEW `v_orders` AS SELECT 1"


@pytest.mark.parametrize(
    ("dictionary_type", "is_async"), ((MariaDBSyncDataDictionary, False), (MariaDBAsyncDataDictionary, True))
)
@pytest.mark.parametrize("raw_version", (None, "not-a-version"))
async def test_mariadb_get_ddl_fallback_on_sqlspec_error(
    dictionary_type: type[Any], is_async: bool, raw_version: str | None
) -> None:
    """Public get_ddl error fallback preserves mariadb dialect when version cannot be parsed."""

    def select_one(statement: str, **kwargs: Any) -> dict[str, object]:
        _ = kwargs
        if "SELECT VERSION()" in statement:
            return {"raw_version": raw_version, "sql_mode": "STRICT_TRANS_TABLES", "sql_quote_show_create": "1"}
        msg = "View 'shop.v_orders' does not exist"
        raise SQLSpecError(msg)

    driver = Mock(select_one=AsyncMock(side_effect=select_one) if is_async else Mock(side_effect=select_one))
    result = dictionary_type().get_ddl(driver, "v_orders", schema="shop", object_type="view")
    if isawaitable(result):
        result = await result

    assert result.status == MetadataSupport.UNSUPPORTED
    assert result.identity.dialect == "mariadb"


def test_make_mysql_ddl_result_default_dialect() -> None:
    """make_mysql_ddl_result defaults fallback dialect to mysql and accepts default_dialect override."""
    row: dict[str, object] = {"View": "v_orders", "Create View": "CREATE VIEW `v_orders` AS SELECT 1"}
    default_result = make_mysql_ddl_result("v_orders", "shop", "view", row, None, None, None)
    mariadb_result = make_mysql_ddl_result("v_orders", "shop", "view", row, None, None, None, default_dialect="mariadb")

    assert default_result.identity.dialect == "mysql"
    assert default_result.context["engine_family"] == "mysql"
    assert mariadb_result.identity.dialect == "mariadb"
    assert mariadb_result.context["engine_family"] == "mariadb"


@pytest.mark.parametrize("dictionary_type", (MariaDBSyncDataDictionary, MariaDBAsyncDataDictionary))
def test_mariadb_dialect_config_lookup(dictionary_type: type[Any]) -> None:
    """MariaDB dictionary bases resolve MARIADB_CONFIG from the dialect registry."""
    config = dictionary_type().get_dialect_config()
    assert config.name == "mariadb"
    assert config is MARIADB_CONFIG


@pytest.mark.parametrize(
    ("dictionary_type", "is_async"), ((MariaDBSyncDataDictionary, False), (MariaDBAsyncDataDictionary, True))
)
@pytest.mark.parametrize("version_value", (None, "10.11.6-MariaDB"))
async def test_mariadb_get_metadata_capabilities_reports_mariadb(
    dictionary_type: type[Any], is_async: bool, version_value: str | None
) -> None:
    """get_metadata_capabilities reports mariadb with and without server version text."""
    driver = Mock(
        select_value_or_none=AsyncMock(return_value=version_value) if is_async else Mock(return_value=version_value)
    )
    profile = dictionary_type().get_metadata_capabilities(driver)
    if isawaitable(profile):
        profile = await profile
    assert profile.dialect == "mariadb"
