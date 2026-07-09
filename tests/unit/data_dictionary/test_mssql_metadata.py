"""SQL Server replacement data-dictionary metadata tests."""

from typing import Any, cast

import pytest

from sqlspec.adapters.mssql_python.data_dictionary import MssqlPythonSyncDataDictionary
from sqlspec.adapters.pymssql.data_dictionary import PymssqlSyncDataDictionary
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

REQUIRED_MSSQL_DOMAIN_QUERIES = {
    "schemas": ("list",),
    "objects": ("by_schema",),
    "tables": ("by_schema", "all_by_schema", "details_by_table"),
    "columns": ("by_schema", "by_table"),
    "constraints": ("by_schema", "by_table", "foreign_keys_by_schema", "foreign_keys_by_table"),
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
        "generated_always_type_desc",
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
    assert result.ddl is not None
    assert "CREATE TABLE [dbo].[accounts]" in result.ddl
    assert "[id] int IDENTITY(1,1) NOT NULL" in result.ddl
    assert "CONSTRAINT [pk_accounts] PRIMARY KEY ([id])" in result.ddl
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
