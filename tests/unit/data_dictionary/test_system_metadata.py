"""Tests for opt-in system and performance metadata policy."""

from typing import Any
from unittest.mock import Mock

from sqlspec.adapters.adbc.data_dictionary import AdbcDataDictionary
from sqlspec.data_dictionary import (
    ColumnMetadata,
    ForeignKeyMetadata,
    IndexMetadata,
    MetadataRisk,
    MetadataSource,
    MetadataSupport,
    SystemMetadataCapability,
    SystemMetadataRedactionPolicy,
    SystemMetadataRequest,
    SystemMetadataResult,
    TableMetadata,
    system_metadata_gated_result,
)
from sqlspec.driver import AsyncDataDictionaryBase, SyncDataDictionaryBase


class ConcreteSyncDataDictionary(SyncDataDictionaryBase):
    """Concrete sync data dictionary for base behavior tests."""

    dialect = "test"

    def get_version(self, driver: Any) -> None:
        return None

    def get_feature_flag(self, driver: Any, feature: str) -> bool:
        return False

    def get_optimal_type(self, driver: Any, type_category: str) -> str:
        return "TEXT"

    def get_tables(self, driver: Any, schema: str | None = None) -> list[TableMetadata]:
        return []

    def get_columns(self, driver: Any, table: str | None = None, schema: str | None = None) -> list[ColumnMetadata]:
        return []

    def get_indexes(self, driver: Any, table: str | None = None, schema: str | None = None) -> list[IndexMetadata]:
        return []

    def get_foreign_keys(
        self, driver: Any, table: str | None = None, schema: str | None = None
    ) -> list[ForeignKeyMetadata]:
        return []


class ConcreteAsyncDataDictionary(AsyncDataDictionaryBase):
    """Concrete async data dictionary for base behavior tests."""

    dialect = "test"

    async def get_version(self, driver: Any) -> None:
        return None

    async def get_feature_flag(self, driver: Any, feature: str) -> bool:
        return False

    async def get_optimal_type(self, driver: Any, type_category: str) -> str:
        return "TEXT"

    async def get_tables(self, driver: Any, schema: str | None = None) -> list[TableMetadata]:
        return []

    async def get_columns(
        self, driver: Any, table: str | None = None, schema: str | None = None
    ) -> list[ColumnMetadata]:
        return []

    async def get_indexes(
        self, driver: Any, table: str | None = None, schema: str | None = None
    ) -> list[IndexMetadata]:
        return []

    async def get_foreign_keys(
        self, driver: Any, table: str | None = None, schema: str | None = None
    ) -> list[ForeignKeyMetadata]:
        return []


def test_system_metadata_disabled_by_default() -> None:
    """Base system metadata calls fail closed until explicitly opted in."""
    result = ConcreteSyncDataDictionary().get_system_metadata(Mock(), SystemMetadataRequest(domain="sessions"))

    assert isinstance(result, SystemMetadataResult)
    assert result.rows == ()
    assert result.capability.support == MetadataSupport.GATED
    assert result.capability.domain == "sessions"
    assert result.source == MetadataSource.SYSTEM_VIEW
    assert "disabled by default" in result.warnings[0]


def test_structural_metadata_capabilities_do_not_include_system_domain() -> None:
    """System metadata capability disclosure stays out of structural metadata profiles."""
    data_dictionary = ConcreteSyncDataDictionary()

    structural = data_dictionary.get_metadata_capabilities(Mock())
    system = data_dictionary.get_system_metadata_capabilities(Mock(), domains=("sessions",))

    assert structural.get("system").support == MetadataSupport.UNKNOWN
    assert system[0].domain == "sessions"
    assert system[0].support == MetadataSupport.UNSUPPORTED
    assert MetadataRisk.REDACTED in system[0].risks


async def test_async_system_metadata_disabled_by_default() -> None:
    """Async base system metadata follows the same fail-closed policy."""
    result = await ConcreteAsyncDataDictionary().get_system_metadata(Mock(), SystemMetadataRequest(domain="sessions"))

    assert result.rows == ()
    assert result.capability.support == MetadataSupport.GATED
    assert "disabled by default" in result.warnings[0]


def test_system_metadata_requires_explicit_billed_opt_in() -> None:
    """Billed metadata domains require their own explicit opt-in after system opt-in."""
    request = SystemMetadataRequest(domain="jobs", include_system=True)
    capability = SystemMetadataCapability(
        domain="jobs",
        support=MetadataSupport.SUPPORTED,
        source=MetadataSource.SYSTEM_VIEW,
        risks=(MetadataRisk.BILLED,),
        cost_implications="May bill INFORMATION_SCHEMA job timeline scans.",
    )

    result = system_metadata_gated_result(request, capability)

    assert result.capability.support == MetadataSupport.GATED
    assert result.rows == ()
    assert MetadataRisk.BILLED in result.capability.risks
    assert any("allow_billed_metadata=True" in warning for warning in result.warnings)


def test_system_metadata_requires_explicit_license_opt_in() -> None:
    """License-gated diagnostics require acknowledgement before any query can run."""
    request = SystemMetadataRequest(domain="awr", include_performance=True)
    capability = SystemMetadataCapability(
        domain="awr",
        support=MetadataSupport.SUPPORTED,
        source=MetadataSource.SYSTEM_VIEW,
        risks=(MetadataRisk.LICENSE_GATED,),
        license_gate="Oracle Diagnostics Pack",
    )

    result = system_metadata_gated_result(request, capability)

    assert result.capability.support == MetadataSupport.GATED
    assert MetadataRisk.LICENSE_GATED in result.capability.risks
    assert any("allow_license_gated_diagnostics=True" in warning for warning in result.warnings)


def test_system_metadata_capability_discloses_policy_concerns() -> None:
    """System capabilities carry privilege, cost, license, redaction, and managed-service disclosures."""
    capability = SystemMetadataCapability(
        domain="activity",
        support=MetadataSupport.SUPPORTED,
        required_privileges=("pg_monitor",),
        risks=(MetadataRisk.PRIVILEGED, MetadataRisk.EXPENSIVE, MetadataRisk.REDACTED),
        source=MetadataSource.SYSTEM_VIEW,
        cost_implications="May scan active session views.",
        license_gate="diagnostics option",
        managed_service_restricted=True,
        redaction_fields=("sql_text", "user", "host"),
    )

    payload = capability.to_dict()

    assert payload["required_privileges"] == ("pg_monitor",)
    assert payload["cost_implications"] == "May scan active session views."
    assert payload["license_gate"] == "diagnostics option"
    assert payload["managed_service_restricted"] is True
    assert payload["redaction_fields"] == ("sql_text", "user", "host")


def test_system_metadata_redacts_sensitive_fields_by_default() -> None:
    """Default redaction covers SQL text, principals, hosts, settings, connection strings, and grants."""
    row = {
        "sql_text": "select * from secret_table where token = 'abc'",
        "username": "alice",
        "client_host": "db-client.internal",
        "setting_name": "database_url",
        "setting_value": "postgresql://alice:secret@db.internal/app",
        "connection_string": "Server=db.internal;User Id=alice;Password=secret",
        "grantee": "reporting_role",
        "grant_sql": "GRANT SELECT ON secret_table TO reporting_role",
        "row_count": 42,
    }

    redacted, fields = SystemMetadataRedactionPolicy().redact_row(row)

    assert redacted["sql_text"] == "[REDACTED]"
    assert redacted["username"] == "[REDACTED]"
    assert redacted["client_host"] == "[REDACTED]"
    assert redacted["setting_value"] == "[REDACTED]"
    assert redacted["connection_string"] == "[REDACTED]"
    assert redacted["grantee"] == "[REDACTED]"
    assert redacted["grant_sql"] == "[REDACTED]"
    assert redacted["row_count"] == 42
    assert fields == (
        "client_host",
        "connection_string",
        "grant_sql",
        "grantee",
        "setting_value",
        "sql_text",
        "username",
    )


def test_system_metadata_result_redacts_rows_by_default() -> None:
    """Result construction applies the request redaction policy before exposing rows."""
    request = SystemMetadataRequest(domain="activity", include_system=True)
    capability = SystemMetadataCapability(
        domain="activity",
        support=MetadataSupport.SUPPORTED,
        source=MetadataSource.SYSTEM_VIEW,
        redaction_fields=("sql_text", "username"),
    )

    result = SystemMetadataResult.from_rows(
        request, capability, rows=({"sql_text": "select password from users", "username": "alice", "pid": 1},)
    )

    assert result.rows == ({"sql_text": "[REDACTED]", "username": "[REDACTED]", "pid": 1},)
    assert result.redactions == ("sql_text", "username")


def test_adbc_statistics_surface_as_transport_system_metadata() -> None:
    """ADBC table statistics can be requested through the gated system metadata namespace."""
    driver = Mock()
    driver.dialect = "duckdb"
    driver.connection.adbc_get_statistics.return_value.read_all.return_value.to_pylist.return_value = [
        {
            "catalog_name": "memory",
            "catalog_db_schemas": [
                {
                    "db_schema_name": "main",
                    "db_schema_statistics": [
                        {
                            "table_name": "items",
                            "column_name": None,
                            "statistic_key": 6,
                            "statistic_value": 3,
                            "statistic_is_approximate": True,
                        }
                    ],
                }
            ],
        }
    ]
    request = SystemMetadataRequest(domain="table_statistics", include_performance=True, table="items")

    result = AdbcDataDictionary().get_system_metadata(driver, request)

    assert result.capability.support == MetadataSupport.SUPPORTED
    assert result.capability.source == MetadataSource.DRIVER_METADATA
    assert result.rows[0]["table_name"] == "items"
    assert result.rows[0]["statistic_name"] == "adbc.statistic.row_count"
    assert MetadataRisk.PRIVILEGED in result.capability.risks
