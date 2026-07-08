"""PostgreSQL-family replacement data-dictionary adapter contracts."""

from typing import Any, cast

import pytest

from sqlspec.adapters.asyncpg.data_dictionary import AsyncpgDataDictionary
from sqlspec.adapters.cockroach_asyncpg.data_dictionary import CockroachAsyncpgDataDictionary
from sqlspec.adapters.cockroach_psycopg.data_dictionary import (
    CockroachPsycopgAsyncDataDictionary,
    CockroachPsycopgSyncDataDictionary,
)
from sqlspec.adapters.psqlpy.data_dictionary import PsqlpyDataDictionary
from sqlspec.adapters.psycopg.data_dictionary import PsycopgAsyncDataDictionary, PsycopgSyncDataDictionary
from sqlspec.data_dictionary import (
    DDLResult,
    MetadataFidelity,
    MetadataSource,
    MetadataSupport,
    SystemMetadataRequest,
    SystemMetadataResult,
)


class FakeSyncDriver:
    """Minimal sync driver recording metadata calls."""

    def __init__(self) -> None:
        self.select_calls: list[tuple[Any, dict[str, Any]]] = []

    def select(self, statement: Any, **kwargs: Any) -> list[dict[str, Any]]:
        self.select_calls.append((statement, kwargs))
        return [{"schema_name": kwargs.get("schema_name"), "table_name": kwargs.get("table_name", "app")}]


class FakeAsyncDriver:
    """Minimal async driver recording metadata calls."""

    def __init__(self) -> None:
        self.select_calls: list[tuple[Any, dict[str, Any]]] = []

    async def select(self, statement: Any, **kwargs: Any) -> list[dict[str, Any]]:
        self.select_calls.append((statement, kwargs))
        return [{"schema_name": kwargs.get("schema_name"), "table_name": kwargs.get("table_name", "app")}]


class FakeDdlSyncDriver(FakeSyncDriver):
    """Sync driver returning DDL-shaped metadata rows."""

    def select(self, statement: Any, **kwargs: Any) -> list[dict[str, Any]]:
        self.select_calls.append((statement, kwargs))
        return [
            {
                "schema_name": kwargs.get("schema_name"),
                "object_name": kwargs.get("object_name"),
                "ddl": "CREATE VIEW public.active_orders AS SELECT 1",
                "fidelity": "native",
            }
        ]


class FakeDdlAsyncDriver(FakeAsyncDriver):
    """Async driver returning DDL-shaped metadata rows."""

    async def select(self, statement: Any, **kwargs: Any) -> list[dict[str, Any]]:
        self.select_calls.append((statement, kwargs))
        return [
            {
                "schema_name": kwargs.get("schema_name"),
                "object_name": kwargs.get("object_name"),
                "ddl": "CREATE VIEW public.active_orders AS SELECT 1",
                "fidelity": "native",
            }
        ]


@pytest.mark.parametrize("data_dictionary", [PsycopgSyncDataDictionary()])
def test_postgres_sync_metadata_capabilities_report_supported_replacement_domains(data_dictionary: Any) -> None:
    """PostgreSQL sync adapters report rich domains as supported and system metadata as gated."""
    profile = data_dictionary.get_metadata_capabilities(cast(Any, FakeSyncDriver()))

    assert profile.dialect == "postgres"
    assert profile.get("schemas").support == MetadataSupport.SUPPORTED
    assert profile.get("constraints").fidelity == MetadataFidelity.NATIVE
    assert profile.get("dependencies").source == MetadataSource.CATALOG
    assert profile.get("ddl").support == MetadataSupport.SUPPORTED
    assert profile.get("system").support == MetadataSupport.UNSUPPORTED


@pytest.mark.parametrize(
    "data_dictionary", [AsyncpgDataDictionary(), PsqlpyDataDictionary(), PsycopgAsyncDataDictionary()]
)
async def test_postgres_async_metadata_capabilities_report_supported_replacement_domains(data_dictionary: Any) -> None:
    """PostgreSQL async adapters report rich domains as supported and system metadata as gated."""
    profile = await data_dictionary.get_metadata_capabilities(cast(Any, FakeAsyncDriver()))

    assert profile.dialect == "postgres"
    assert profile.get("schemas").support == MetadataSupport.SUPPORTED
    assert profile.get("constraints").fidelity == MetadataFidelity.NATIVE
    assert profile.get("dependencies").source == MetadataSource.CATALOG
    assert profile.get("ddl").support == MetadataSupport.SUPPORTED
    assert profile.get("system").support == MetadataSupport.UNSUPPORTED


def test_postgres_sync_domain_methods_delegate_to_direct_query_packs() -> None:
    """Sync replacement methods execute direct domain-pack SQL with schema/table binds."""
    driver = FakeSyncDriver()
    data_dictionary = PsycopgSyncDataDictionary()

    result = data_dictionary.get_constraints(cast(Any, driver), table="orders", schema="Reporting")

    assert result.domain == "constraints"
    assert result.capability.support == MetadataSupport.SUPPORTED
    assert result.items == ({"schema_name": "reporting", "table_name": "orders"},)
    statement, kwargs = driver.select_calls[0]
    assert "pg_catalog.pg_constraint" in statement.raw_sql
    assert kwargs["schema_name"] == "reporting"
    assert kwargs["table_name"] == "orders"


@pytest.mark.parametrize(
    "data_dictionary", [AsyncpgDataDictionary(), PsqlpyDataDictionary(), PsycopgAsyncDataDictionary()]
)
async def test_postgres_async_domain_methods_delegate_to_direct_query_packs(data_dictionary: Any) -> None:
    """Async replacement methods execute direct domain-pack SQL with schema/table binds."""
    driver = FakeAsyncDriver()

    result = await data_dictionary.get_constraints(cast(Any, driver), table="orders", schema="Reporting")

    assert result.domain == "constraints"
    assert result.capability.support == MetadataSupport.SUPPORTED
    assert result.items == ({"schema_name": "reporting", "table_name": "orders"},)
    statement, kwargs = driver.select_calls[0]
    assert "pg_catalog.pg_constraint" in statement.raw_sql
    assert kwargs["schema_name"] == "reporting"
    assert kwargs["table_name"] == "orders"


def test_postgres_sync_ddl_returns_contract_result() -> None:
    """Sync PostgreSQL DDL lookup returns the typed DDL envelope."""
    driver = FakeDdlSyncDriver()
    data_dictionary = PsycopgSyncDataDictionary()

    result = data_dictionary.get_ddl(cast(Any, driver), "active_orders", schema="Public", object_type="view")

    assert isinstance(result, DDLResult)
    assert result.identity.schema == "public"
    assert result.identity.name == "active_orders"
    assert result.fidelity == MetadataFidelity.NATIVE
    assert result.ddl == "CREATE VIEW public.active_orders AS SELECT 1"


@pytest.mark.parametrize(
    "data_dictionary", [AsyncpgDataDictionary(), PsqlpyDataDictionary(), PsycopgAsyncDataDictionary()]
)
async def test_postgres_async_ddl_returns_contract_result(data_dictionary: Any) -> None:
    """Async PostgreSQL DDL lookup returns the typed DDL envelope."""
    driver = FakeDdlAsyncDriver()

    result = await data_dictionary.get_ddl(cast(Any, driver), "active_orders", schema="Public", object_type="view")

    assert isinstance(result, DDLResult)
    assert result.identity.schema == "public"
    assert result.identity.name == "active_orders"
    assert result.fidelity == MetadataFidelity.NATIVE
    assert result.ddl == "CREATE VIEW public.active_orders AS SELECT 1"


def test_postgres_sync_system_metadata_returns_contract_result() -> None:
    """Sync PostgreSQL system metadata returns the typed opt-in envelope."""
    driver = FakeSyncDriver()
    data_dictionary = PsycopgSyncDataDictionary()

    result = data_dictionary.get_system_metadata(
        cast(Any, driver), SystemMetadataRequest("settings", include_system=True)
    )

    assert isinstance(result, SystemMetadataResult)
    assert result.capability.support == MetadataSupport.SUPPORTED
    assert result.source == MetadataSource.SYSTEM_VIEW
    assert result.rows == ({"schema_name": None, "table_name": "app"},)


@pytest.mark.parametrize(
    "data_dictionary", [AsyncpgDataDictionary(), PsqlpyDataDictionary(), PsycopgAsyncDataDictionary()]
)
async def test_postgres_async_system_metadata_returns_contract_result(data_dictionary: Any) -> None:
    """Async PostgreSQL system metadata returns the typed opt-in envelope."""
    driver = FakeAsyncDriver()

    result = await data_dictionary.get_system_metadata(
        cast(Any, driver), SystemMetadataRequest("settings", include_system=True)
    )

    assert isinstance(result, SystemMetadataResult)
    assert result.capability.support == MetadataSupport.SUPPORTED
    assert result.source == MetadataSource.SYSTEM_VIEW
    assert result.rows == ({"schema_name": None, "table_name": "app"},)


@pytest.mark.parametrize("data_dictionary", [CockroachPsycopgSyncDataDictionary()])
def test_cockroach_sync_capabilities_are_truthful_without_internal_metadata(data_dictionary: Any) -> None:
    """Cockroach sync adapters report stable-core support and no crdb_internal by default."""
    profile = data_dictionary.get_metadata_capabilities(
        cast(Any, FakeSyncDriver()), domains=("schemas", "ddl", "crdb_internal")
    )

    assert profile.dialect == "cockroachdb"
    assert profile.get("schemas").support == MetadataSupport.SUPPORTED
    assert profile.get("ddl").fidelity == MetadataFidelity.LOSSY
    assert profile.get("crdb_internal").support == MetadataSupport.UNSUPPORTED


@pytest.mark.parametrize("data_dictionary", [CockroachAsyncpgDataDictionary(), CockroachPsycopgAsyncDataDictionary()])
async def test_cockroach_async_capabilities_are_truthful_without_internal_metadata(data_dictionary: Any) -> None:
    """Cockroach async adapters report stable-core support and no crdb_internal by default."""
    profile = await data_dictionary.get_metadata_capabilities(
        cast(Any, FakeAsyncDriver()), domains=("schemas", "ddl", "crdb_internal")
    )

    assert profile.dialect == "cockroachdb"
    assert profile.get("schemas").support == MetadataSupport.SUPPORTED
    assert profile.get("ddl").fidelity == MetadataFidelity.LOSSY
    assert profile.get("crdb_internal").support == MetadataSupport.UNSUPPORTED


async def test_cockroach_async_domain_methods_use_stable_query_packs() -> None:
    """Cockroach async replacement methods use stable metadata query packs."""
    driver = FakeAsyncDriver()
    data_dictionary = CockroachAsyncpgDataDictionary()

    result = await data_dictionary.get_dependencies(cast(Any, driver), schema="Public")

    assert result.domain == "dependencies"
    assert result.capability.support == MetadataSupport.SUPPORTED
    statement, kwargs = driver.select_calls[0]
    assert "information_schema" in statement.raw_sql
    assert "crdb_internal" not in statement.raw_sql
    assert kwargs["schema_name"] == "public"
