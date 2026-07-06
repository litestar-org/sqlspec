"""Unit tests for MySQL-family replacement data-dictionary metadata."""

from importlib import import_module
from typing import Any, cast

import pytest

from sqlspec.adapters.aiomysql.data_dictionary import AiomysqlDataDictionary
from sqlspec.adapters.asyncmy.data_dictionary import AsyncmyDataDictionary
from sqlspec.adapters.mysqlconnector.data_dictionary import MysqlConnectorSyncDataDictionary
from sqlspec.adapters.pymysql.data_dictionary import PyMysqlDataDictionary
from sqlspec.data_dictionary import (
    DataDictionaryLoader,
    DDLResult,
    MetadataFidelity,
    MetadataSource,
    MetadataSupport,
    get_dialect_config,
    normalize_dialect_name,
)


class _SyncDDLDriver:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def select_one(self, statement: str, *args: Any, **kwargs: Any) -> dict[str, str]:
        self.statements.append(str(statement))
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
        ("mysql", "ddl", "show_create_table", "SHOW CREATE TABLE"),
        ("mysql", "system", "performance_schema_tables", "performance_schema"),
        ("mariadb", "schemas", "list", "information_schema.schemata"),
        ("mariadb", "objects", "by_schema", "SEQUENCE"),
        ("mariadb", "plugins", "list", "information_schema.plugins"),
        ("mariadb", "ddl", "show_create_sequence", "SHOW CREATE SEQUENCE"),
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

    assert result.capability.support == MetadataSupport.SUPPORTED
    assert len(result.items) == 1
    ddl = cast(DDLResult, result.items[0])
    assert ddl.fidelity == MetadataFidelity.NATIVE
    assert ddl.source == MetadataSource.NATIVE_API
    assert ddl.context == {
        "engine_family": "mysql",
        "server_version": "8.4.0",
        "sql_mode": "STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION",
        "sql_quote_show_create": "1",
    }
    assert driver.statements[-1] == "SHOW CREATE TABLE `shop`.`orders`"
