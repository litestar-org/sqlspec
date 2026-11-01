"""SQL Server data-dictionary metadata tests."""

import sqlite3
from collections.abc import Iterator
from typing import Any, cast

import pytest

from sqlspec.adapters.mssql_python.data_dictionary import MssqlPythonSyncDataDictionary
from sqlspec.adapters.mssql_python.driver import MssqlPythonDriver
from sqlspec.adapters.pymssql.data_dictionary import PymssqlSyncDataDictionary
from sqlspec.adapters.pymssql.driver import PymssqlDriver
from sqlspec.data_dictionary import (
    DataDictionaryLoader,
    DDLResult,
    MetadataFidelity,
    MetadataRisk,
    MetadataSource,
    MetadataSupport,
    SystemMetadataRequest,
    SystemMetadataResult,
)
from sqlspec.driver import SyncDataDictionaryBase

REQUIRED_MSSQL_DOMAIN_QUERIES = {
    "schemas": ("list",),
    "objects": ("by_schema",),
    "tables": ("by_schema", "all_by_schema", "details_by_table"),
    "columns": ("by_schema", "by_table"),
    "constraints": ("by_schema", "by_table"),
    "foreign_keys": ("by_schema", "by_table"),
    "indexes": ("by_schema", "by_table"),
    "views": ("by_schema",),
    "modules": ("by_schema",),
    "routines": ("by_schema", "parameters_by_schema"),
    "triggers": ("by_schema",),
    "sequences": ("by_schema",),
    "extended_properties": ("by_schema",),
    "permissions": ("by_schema", "role_members"),
    "dependencies": ("by_schema",),
    "storage": ("by_schema",),
    "ddl": ("table_inputs_by_table", "index_inputs_by_table"),
    "system": ("dmv_exec_requests", "query_store_runtime"),
}


class FakeSyncDriver:
    """Minimal sync driver for SQL Server metadata behavior tests."""

    def __init__(self, driver_features: dict[str, Any] | None = None) -> None:
        self.driver_features = driver_features or {}
        self.select_calls: list[tuple[Any, dict[str, Any]]] = []

    def select_value_or_none(self, _statement: Any, **_kwargs: Any) -> str:
        return "dbo"

    def select(self, statement: Any, **kwargs: Any) -> list[dict[str, Any]]:
        self.select_calls.append((statement, kwargs))
        query_text = getattr(statement, "raw_sql", str(statement))
        if "sqlspec:mssql:ddl_table_inputs" in query_text:
            return _ddl_table_rows()
        if "sqlspec:mssql:ddl_index_inputs" in query_text:
            return []
        if "sqlspec:mssql:dmv_exec_requests" in query_text:
            return [{"session_id": 57, "login_name": "sa", "sql_text": "SELECT secret"}]
        return []


class CatalogCursor(sqlite3.Cursor):
    """Execute SDK-bound SQL against a local substitute SQL Server catalog."""

    def execute(self, sql: str, parameters: Any = ()) -> "CatalogCursor":
        super().execute(sql.replace("%s", "?"), parameters)
        return self


class CatalogConnection(sqlite3.Connection):
    """Provide the DB-API cursor surface shared by the SQL Server SDKs."""

    def cursor(self, *args: Any, **kwargs: Any) -> CatalogCursor:
        return super().cursor(factory=CatalogCursor)


@pytest.fixture(params=[MssqlPythonDriver, PymssqlDriver], ids=["mssql_python", "pymssql"])
def catalog_driver(request: pytest.FixtureRequest) -> Iterator[MssqlPythonDriver | PymssqlDriver]:
    """Run real drivers and dictionary SQL with only the external SDK substituted."""
    connection = sqlite3.connect(":memory:", factory=CatalogConnection)
    connection.create_function("DB_NAME", 0, lambda: "app")
    connection.create_function("SCHEMA_NAME", 0, lambda: "dbo")
    connection.executescript("""
        ATTACH DATABASE ':memory:' AS sys;
        CREATE TABLE sys.schemas (schema_id INTEGER, name TEXT);
        CREATE TABLE sys.tables (
            object_id INTEGER, schema_id INTEGER, name TEXT, create_date TEXT, modify_date TEXT,
            is_memory_optimized INTEGER, durability INTEGER, durability_desc TEXT,
            temporal_type INTEGER, temporal_type_desc TEXT, history_table_id INTEGER
        );
        INSERT INTO sys.schemas VALUES (1, 'dbo');
        INSERT INTO sys.tables VALUES
            (10, 1, 'accounts', '2026-01-01', '2026-02-01', 0, 0, 'SCHEMA_AND_DATA',
             2, 'SYSTEM_VERSIONED_TEMPORAL_TABLE', 11),
            (11, 1, 'accounts_history', '2026-01-01', '2026-01-01', 0, 0, 'SCHEMA_AND_DATA',
             1, 'HISTORY_TABLE', NULL);
    """)
    try:
        yield request.param(cast(Any, connection))
    finally:
        connection.close()


@pytest.mark.parametrize("schema", [None, "dbo"])
def test_table_details_selects_requested_table_with_rich_metadata(
    catalog_driver: MssqlPythonDriver | PymssqlDriver, schema: str | None
) -> None:
    """A detail lookup preserves table identity, rich fields, and missing-table behavior."""
    dictionary = cast("SyncDataDictionaryBase", catalog_driver.data_dictionary)
    result = dictionary.get_table_details(catalog_driver, "accounts", schema)

    assert result.items == (
        {
            "catalog_name": "app",
            "schema_name": "dbo",
            "table_name": "accounts",
            "table_type": "BASE TABLE",
            "create_date": "2026-01-01",
            "modify_date": "2026-02-01",
            "is_memory_optimized": 0,
            "durability": 0,
            "durability_desc": "SCHEMA_AND_DATA",
            "temporal_type": 2,
            "temporal_type_desc": "SYSTEM_VERSIONED_TEMPORAL_TABLE",
            "history_schema_name": "dbo",
            "history_table_name": "accounts_history",
        },
    )
    history = dictionary.get_table_details(catalog_driver, "accounts_history", schema)
    assert len(history.items) == 1
    history_row = cast("dict[str, object]", history.items[0])
    assert history_row["table_name"] == "accounts_history"
    assert history_row["history_table_name"] is None
    missing = dictionary.get_table_details(catalog_driver, "missing", schema)
    assert missing.items == ()


def test_mssql_domain_query_pack_supports_required_catalog_queries() -> None:
    """SQL Server query packs should load from direct domain paths."""
    loader = DataDictionaryLoader()

    for domain, query_names in REQUIRED_MSSQL_DOMAIN_QUERIES.items():
        for query_name in query_names:
            query = loader.get_domain_query("mssql", domain, query_name)

            assert query.is_supported is True, (domain, query_name, query.capability.warnings)
            assert query.query_text is not None
            assert "SELECT *" not in query.query_text.upper()


def test_mssql_column_and_index_query_shapes_cover_sql_server_specific_metadata() -> None:
    """Column and index catalog queries should expose SQL Server-only attributes."""
    loader = DataDictionaryLoader()

    columns_text = loader.get_domain_query_text("mssql", "columns", "by_table") or ""
    indexes_text = loader.get_domain_query_text("mssql", "indexes", "by_table") or ""

    for required_fragment in (
        "sys.computed_columns",
        "sys.identity_columns",
        "is_sparse",
        "is_masked",
        "encryption_type_desc",
        "generated_always_type",
        "COALESCE(:schema_name, SCHEMA_NAME())",
    ):
        assert required_fragment in columns_text

    for required_fragment in (
        "is_included_column",
        "filter_definition",
        "type_desc",
        "sys.data_spaces",
        "sys.partition_schemes",
    ):
        assert required_fragment in indexes_text


@pytest.mark.parametrize("dictionary_type", [MssqlPythonSyncDataDictionary, PymssqlSyncDataDictionary])
def test_sync_data_dictionary_reports_sql_server_metadata_capabilities(dictionary_type: type[Any]) -> None:
    """SQL Server adapters should report generated DDL and permission-sensitive system risks."""
    profile = dictionary_type().get_metadata_capabilities(cast(Any, FakeSyncDriver()))

    ddl = profile.get("ddl")
    assert ddl.support == MetadataSupport.SUPPORTED
    assert ddl.fidelity == MetadataFidelity.GENERATED
    assert ddl.source == MetadataSource.GENERATED
    assert "reconstructed from sys catalog views" in " ".join(ddl.warnings)

    permissions = profile.get("permissions")
    assert permissions.support == MetadataSupport.SUPPORTED
    assert MetadataRisk.PRIVILEGED in permissions.risks
    assert "VIEW DEFINITION" in " ".join(permissions.warnings)

    system = profile.get("system")
    assert system.support == MetadataSupport.SUPPORTED
    assert system.source == MetadataSource.SYSTEM_VIEW
    assert {MetadataRisk.PRIVILEGED, MetadataRisk.REDACTED} <= set(system.risks)
    assert "VIEW SERVER STATE" in " ".join(system.warnings)
    assert "VIEW SERVER PERFORMANCE STATE" in " ".join(system.warnings)
    assert "VIEW SERVER SECURITY STATE" in " ".join(system.warnings)


def test_mssql_generated_table_ddl_reports_generated_fidelity() -> None:
    """SQL Server table DDL should be generated from structured catalog rows."""
    driver = FakeSyncDriver()
    result = MssqlPythonSyncDataDictionary().get_ddl(cast(Any, driver), "accounts")

    assert isinstance(result, DDLResult)
    assert result.status == MetadataSupport.SUPPORTED
    assert result.fidelity == MetadataFidelity.GENERATED
    assert result.source == MetadataSource.GENERATED
    assert result.identity.schema == "dbo"
    assert result.identity.name == "accounts"
    assert result.ddl == (
        "CREATE TABLE [dbo].[accounts] (\n"
        "  [id] INTEGER NOT NULL IDENTITY(1, 1),\n"
        "  [name] NVARCHAR(100) NOT NULL DEFAULT 'unknown',\n"
        "  CONSTRAINT [pk_accounts] PRIMARY KEY ([id])\n"
        ");"
    )
    assert "reconstructed from sys catalog views" in " ".join(result.warnings)


def test_mssql_dmvs_disabled_without_permission_flags() -> None:
    """DMV metadata should require explicit opt-in and permission flags."""
    driver = FakeSyncDriver()
    result = MssqlPythonSyncDataDictionary().get_system_metadata(
        cast(Any, driver), SystemMetadataRequest("dmv_exec_requests", include_system=True)
    )

    assert isinstance(result, SystemMetadataResult)
    assert result.capability.support == MetadataSupport.UNSUPPORTED
    assert MetadataRisk.PRIVILEGED in result.capability.risks
    assert "enable_system_metadata" in " ".join(result.warnings)
    assert driver.select_calls == []


def test_mssql_dmvs_redact_sensitive_columns_by_default() -> None:
    """Opted-in DMV metadata should redact SQL text and principals unless requested."""
    driver = FakeSyncDriver({
        "data_dictionary": {
            "enable_system_metadata": True,
            "view_server_state": True,
            "view_server_performance_state": True,
        }
    })
    result = MssqlPythonSyncDataDictionary().get_system_metadata(
        cast(Any, driver), SystemMetadataRequest("dmv_exec_requests", include_system=True)
    )

    assert isinstance(result, SystemMetadataResult)
    assert result.capability.support == MetadataSupport.SUPPORTED
    assert result.rows
    row = result.rows[0]
    assert row["sql_text"] == "[REDACTED]"
    assert row["login_name"] == "[REDACTED]"
    assert row["session_id"] == 57


def _ddl_table_rows() -> list[dict[str, Any]]:
    return [
        {
            "schema_name": "dbo",
            "table_name": "accounts",
            "column_name": "id",
            "data_type": "int",
            "max_length": 4,
            "numeric_precision": 10,
            "numeric_scale": 0,
            "is_nullable": False,
            "ordinal_position": 1,
            "is_identity": True,
            "identity_seed": 1,
            "identity_increment": 1,
            "is_computed": False,
            "computed_definition": None,
            "column_default": None,
            "primary_key_name": "pk_accounts",
            "primary_key_ordinal": 1,
        },
        {
            "schema_name": "dbo",
            "table_name": "accounts",
            "column_name": "name",
            "data_type": "nvarchar",
            "max_length": 200,
            "numeric_precision": None,
            "numeric_scale": None,
            "is_nullable": False,
            "ordinal_position": 2,
            "is_identity": False,
            "identity_seed": None,
            "identity_increment": None,
            "is_computed": False,
            "computed_definition": None,
            "column_default": "('unknown')",
            "primary_key_name": "pk_accounts",
            "primary_key_ordinal": None,
        },
    ]
