"""PostgreSQL-family data-dictionary adapter contracts."""

import importlib
import subprocess
import sys
from typing import Any, cast

import pytest

import sqlspec.data_dictionary.dialects.postgres as postgres_dialect_pkg
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
    VersionInfo,
)
from sqlspec.data_dictionary.dialects.postgres import PostgresAsyncDataDictionary, PostgresSyncDataDictionary
from sqlspec.data_dictionary.dialects.postgres import dictionary as postgres_dictionary_module


class FakeSyncDriver:
    """Minimal sync driver recording metadata calls."""

    def __init__(self, *, connection: object | None = None, version_value: Any = None) -> None:
        self.connection = connection
        self.version_value = version_value
        self.select_calls: list[tuple[Any, dict[str, Any]]] = []
        self.select_value_calls: list[Any] = []

    def select(self, statement: Any, **kwargs: Any) -> list[dict[str, Any]]:
        self.select_calls.append((statement, kwargs))
        return [{"schema_name": kwargs.get("schema_name"), "table_name": kwargs.get("table_name", "app")}]

    def select_value_or_none(self, statement: Any, **kwargs: Any) -> Any:
        _ = kwargs
        self.select_value_calls.append(statement)
        return self.version_value


class FakeAsyncDriver:
    """Minimal async driver recording metadata calls."""

    def __init__(self, *, connection: object | None = None, version_value: Any = None) -> None:
        self.connection = connection
        self.version_value = version_value
        self.select_calls: list[tuple[Any, dict[str, Any]]] = []
        self.select_value_calls: list[Any] = []

    async def select(self, statement: Any, **kwargs: Any) -> list[dict[str, Any]]:
        self.select_calls.append((statement, kwargs))
        return [{"schema_name": kwargs.get("schema_name"), "table_name": kwargs.get("table_name", "app")}]

    async def select_value_or_none(self, statement: Any, **kwargs: Any) -> Any:
        _ = kwargs
        self.select_value_calls.append(statement)
        return self.version_value


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


def test_postgres_sync_base_capabilities_report_supported_domains() -> None:
    """Direct sync PostgreSQL base reports native domain capabilities and gated system metadata."""
    data_dictionary = PostgresSyncDataDictionary()
    profile = data_dictionary.get_metadata_capabilities(cast("Any", FakeSyncDriver()))

    assert profile.dialect == "postgres"
    assert profile.adapter == "PostgresSyncDataDictionary"
    assert profile.get("schemas").support == MetadataSupport.SUPPORTED
    assert profile.get("constraints").support == MetadataSupport.SUPPORTED
    assert profile.get("constraints").fidelity == MetadataFidelity.NATIVE
    assert profile.get("dependencies").source == MetadataSource.CATALOG
    assert profile.get("ddl").support == MetadataSupport.SUPPORTED
    assert profile.get("system").support == MetadataSupport.UNSUPPORTED


async def test_postgres_async_base_capabilities_report_supported_domains() -> None:
    """Direct async PostgreSQL base reports native domain capabilities and gated system metadata."""
    data_dictionary = PostgresAsyncDataDictionary()
    profile = await data_dictionary.get_metadata_capabilities(cast("Any", FakeAsyncDriver()))

    assert profile.dialect == "postgres"
    assert profile.adapter == "PostgresAsyncDataDictionary"
    assert profile.get("schemas").support == MetadataSupport.SUPPORTED
    assert profile.get("constraints").support == MetadataSupport.SUPPORTED
    assert profile.get("constraints").fidelity == MetadataFidelity.NATIVE
    assert profile.get("dependencies").source == MetadataSource.CATALOG
    assert profile.get("ddl").support == MetadataSupport.SUPPORTED
    assert profile.get("system").support == MetadataSupport.UNSUPPORTED


@pytest.mark.parametrize("cls_name", ["PostgresSyncDataDictionary", "PostgresAsyncDataDictionary"])
def test_postgres_dialect_package_lazy_exports_identity(cls_name: str) -> None:
    """PostgreSQL dialect package exports sync and async bases lazily via PEP 562 with exact identity."""
    exported_names = dir(postgres_dialect_pkg)
    assert cls_name in exported_names
    assert cls_name in postgres_dialect_pkg.__all__
    assert getattr(postgres_dialect_pkg, cls_name) is getattr(postgres_dictionary_module, cls_name)


def test_postgres_dialect_package_unknown_attribute_raises_attribute_error() -> None:
    """Accessing an unknown attribute on the PostgreSQL dialect package raises AttributeError."""
    with pytest.raises(AttributeError, match="has no attribute 'NonExistentPostgresExport'"):
        _ = getattr(postgres_dialect_pkg, "NonExistentPostgresExport")


def test_postgres_dialect_package_lazy_import_avoids_drivers_and_eager_dictionary() -> None:
    """Importing sqlspec.data_dictionary.dialects.postgres stays lazy and loads no third-party drivers."""
    probe = (
        "import sys; "
        "import sqlspec.data_dictionary.dialects.postgres as pkg; "
        "assert 'sqlspec.data_dictionary.dialects.postgres.dictionary' not in sys.modules; "
        "assert 'PostgresSyncDataDictionary' in dir(pkg); "
        "assert 'PostgresAsyncDataDictionary' in dir(pkg); "
        "sync_cls = getattr(pkg, 'PostgresSyncDataDictionary'); "
        "async_cls = getattr(pkg, 'PostgresAsyncDataDictionary'); "
        "import sqlspec.data_dictionary.dialects.postgres.dictionary as mod; "
        "assert sync_cls is mod.PostgresSyncDataDictionary; "
        "assert async_cls is mod.PostgresAsyncDataDictionary; "
        "blocked = ('psycopg', 'asyncpg', 'psqlpy', 'sqlspec.adapters'); "
        "loaded = [m for m in sys.modules if any(m == b or m.startswith(b + '.') for b in blocked)]; "
        "assert not loaded, loaded"
    )
    completed = subprocess.run([sys.executable, "-c", probe], capture_output=True, text=True, check=False)
    assert completed.returncode == 0, completed.stderr


@pytest.mark.parametrize(
    ("table", "schema", "expected_schema", "expected_table"),
    [(None, None, "public", None), ("users", None, None, "users"), ("Users", "Reporting", "reporting", "users")],
)
def test_postgres_sync_base_get_indexes_binds_schema_for_unqualified_and_qualified_tables(
    table: str | None, schema: str | None, expected_schema: str | None, expected_table: str | None
) -> None:
    """Sync get_indexes leaves schema_name unbound for unqualified tables so to_regclass follows search_path."""
    driver = FakeSyncDriver()
    data_dictionary = PostgresSyncDataDictionary()

    data_dictionary.get_indexes(cast("Any", driver), table=table, schema=schema)

    _, kwargs = driver.select_calls[0]
    assert kwargs["schema_name"] == expected_schema
    assert kwargs["table_name"] == expected_table


@pytest.mark.parametrize(
    ("table", "schema", "expected_schema", "expected_table"),
    [(None, None, "public", None), ("users", None, None, "users"), ("Users", "Reporting", "reporting", "users")],
)
async def test_postgres_async_base_get_indexes_binds_schema_for_unqualified_and_qualified_tables(
    table: str | None, schema: str | None, expected_schema: str | None, expected_table: str | None
) -> None:
    """Async get_indexes leaves schema_name unbound for unqualified tables so to_regclass follows search_path."""
    driver = FakeAsyncDriver()
    data_dictionary = PostgresAsyncDataDictionary()

    await data_dictionary.get_indexes(cast("Any", driver), table=table, schema=schema)

    _, kwargs = driver.select_calls[0]
    assert kwargs["schema_name"] == expected_schema
    assert kwargs["table_name"] == expected_table


@pytest.mark.parametrize(
    ("table", "schema", "expected_schema", "expected_table"),
    [(None, None, "public", None), ("users", None, None, "users"), ("Users", "Reporting", "reporting", "users")],
)
def test_postgres_sync_base_get_columns_binds_schema_for_unqualified_and_qualified_tables(
    table: str | None, schema: str | None, expected_schema: str | None, expected_table: str | None
) -> None:
    """Sync get_columns leaves schema_name unbound for unqualified tables and normalizes identifiers."""
    driver = FakeSyncDriver()
    data_dictionary = PostgresSyncDataDictionary()

    data_dictionary.get_columns(cast("Any", driver), table=table, schema=schema)

    _, kwargs = driver.select_calls[0]
    assert kwargs["schema_name"] == expected_schema
    assert kwargs["table_name"] == expected_table


@pytest.mark.parametrize(
    ("table", "schema", "expected_schema", "expected_table"),
    [(None, None, "public", None), ("users", None, None, "users"), ("Users", "Reporting", "reporting", "users")],
)
async def test_postgres_async_base_get_columns_binds_schema_for_unqualified_and_qualified_tables(
    table: str | None, schema: str | None, expected_schema: str | None, expected_table: str | None
) -> None:
    """Async get_columns leaves schema_name unbound for unqualified tables and normalizes identifiers."""
    driver = FakeAsyncDriver()
    data_dictionary = PostgresAsyncDataDictionary()

    await data_dictionary.get_columns(cast("Any", driver), table=table, schema=schema)

    _, kwargs = driver.select_calls[0]
    assert kwargs["schema_name"] == expected_schema
    assert kwargs["table_name"] == expected_table


def test_postgres_sync_base_version_caches_by_connection() -> None:
    """Sync get_version caches parsed VersionInfo per underlying physical connection object."""
    shared_connection = object()
    first_driver = FakeSyncDriver(connection=shared_connection, version_value="PostgreSQL 16.2 on x86_64")
    second_driver = FakeSyncDriver(connection=shared_connection, version_value="PostgreSQL 15.0 on x86_64")
    data_dictionary = PostgresSyncDataDictionary()

    first = data_dictionary.get_version(cast("Any", first_driver))
    second = data_dictionary.get_version(cast("Any", second_driver))

    assert first == VersionInfo(16, 2, 0)
    assert second == VersionInfo(16, 2, 0)
    assert len(first_driver.select_value_calls) == 1
    assert len(second_driver.select_value_calls) == 0


async def test_postgres_async_base_version_caches_by_connection() -> None:
    """Async get_version caches parsed VersionInfo per underlying physical connection object."""
    shared_connection = object()
    first_driver = FakeAsyncDriver(connection=shared_connection, version_value="PostgreSQL 16.2 on x86_64")
    second_driver = FakeAsyncDriver(connection=shared_connection, version_value="PostgreSQL 15.0 on x86_64")
    data_dictionary = PostgresAsyncDataDictionary()

    first = await data_dictionary.get_version(cast("Any", first_driver))
    second = await data_dictionary.get_version(cast("Any", second_driver))

    assert first == VersionInfo(16, 2, 0)
    assert second == VersionInfo(16, 2, 0)
    assert len(first_driver.select_value_calls) == 1
    assert len(second_driver.select_value_calls) == 0


@pytest.mark.parametrize("version_value", [None, "", "Unknown Engine"])
def test_postgres_sync_base_missing_or_unparseable_version_caches_none(version_value: str | None) -> None:
    """Sync get_version caches None for missing or unparsable version values without refetching."""
    driver = FakeSyncDriver(version_value=version_value)
    data_dictionary = PostgresSyncDataDictionary()

    assert data_dictionary.get_version(cast("Any", driver)) is None
    assert data_dictionary.get_version(cast("Any", driver)) is None
    assert len(driver.select_value_calls) == 1


@pytest.mark.parametrize("version_value", [None, "", "Unknown Engine"])
async def test_postgres_async_base_missing_or_unparseable_version_caches_none(version_value: str | None) -> None:
    """Async get_version caches None for missing or unparsable version values without refetching."""
    driver = FakeAsyncDriver(version_value=version_value)
    data_dictionary = PostgresAsyncDataDictionary()

    assert await data_dictionary.get_version(cast("Any", driver)) is None
    assert await data_dictionary.get_version(cast("Any", driver)) is None
    assert len(driver.select_value_calls) == 1


@pytest.mark.parametrize(
    ("version_value", "expected_type"),
    [
        ("PostgreSQL 9.3 on x86_64", "JSON"),
        ("PostgreSQL 16.0 on x86_64", "JSONB"),
        (None, "TEXT"),
        ("PostgreSQL 9.1 on x86_64", "TEXT"),
    ],
)
def test_postgres_sync_base_optimal_json_type(version_value: str | None, expected_type: str) -> None:
    """Sync get_optimal_type chooses JSONB, JSON, or TEXT based on detected PostgreSQL version."""
    driver = FakeSyncDriver(version_value=version_value)
    data_dictionary = PostgresSyncDataDictionary()

    assert data_dictionary.get_optimal_type(cast("Any", driver), "json") == expected_type


@pytest.mark.parametrize(
    ("version_value", "expected_type"),
    [
        ("PostgreSQL 9.3 on x86_64", "JSON"),
        ("PostgreSQL 16.0 on x86_64", "JSONB"),
        (None, "TEXT"),
        ("PostgreSQL 9.1 on x86_64", "TEXT"),
    ],
)
async def test_postgres_async_base_optimal_json_type(version_value: str | None, expected_type: str) -> None:
    """Async get_optimal_type chooses JSONB, JSON, or TEXT based on detected PostgreSQL version."""
    driver = FakeAsyncDriver(version_value=version_value)
    data_dictionary = PostgresAsyncDataDictionary()

    assert await data_dictionary.get_optimal_type(cast("Any", driver), "json") == expected_type


@pytest.mark.parametrize(
    ("request_obj", "expected_support", "expected_kwargs"),
    [
        (SystemMetadataRequest("settings", include_system=False), MetadataSupport.GATED, None),
        (SystemMetadataRequest("unknown_domain", include_system=True), MetadataSupport.UNSUPPORTED, None),
        (SystemMetadataRequest("pg_stat_statements", include_system=True), MetadataSupport.SUPPORTED, {"limit": 100}),
        (
            SystemMetadataRequest("table_stats", schema="Reporting", table="Orders", include_system=True),
            MetadataSupport.SUPPORTED,
            {"schema_name": "reporting", "table_name": "orders"},
        ),
        (
            SystemMetadataRequest("table_stats", schema=None, table=None, include_system=True),
            MetadataSupport.SUPPORTED,
            {"schema_name": "public", "table_name": None},
        ),
    ],
)
def test_postgres_sync_base_system_metadata_gating_and_binds(
    request_obj: SystemMetadataRequest, expected_support: MetadataSupport, expected_kwargs: dict[str, Any] | None
) -> None:
    """Sync get_system_metadata gates disabled requests and binds domain parameters when enabled."""
    driver = FakeSyncDriver()
    data_dictionary = PostgresSyncDataDictionary()

    result = data_dictionary.get_system_metadata(cast("Any", driver), request_obj)

    assert isinstance(result, SystemMetadataResult)
    assert result.capability.support == expected_support
    if expected_kwargs is None:
        assert result.rows == ()
        assert driver.select_calls == []
    else:
        assert len(driver.select_calls) == 1
        _, kwargs = driver.select_calls[0]
        assert kwargs == expected_kwargs


@pytest.mark.parametrize(
    ("request_obj", "expected_support", "expected_kwargs"),
    [
        (SystemMetadataRequest("settings", include_system=False), MetadataSupport.GATED, None),
        (SystemMetadataRequest("unknown_domain", include_system=True), MetadataSupport.UNSUPPORTED, None),
        (SystemMetadataRequest("pg_stat_statements", include_system=True), MetadataSupport.SUPPORTED, {"limit": 100}),
        (
            SystemMetadataRequest("table_stats", schema="Reporting", table="Orders", include_system=True),
            MetadataSupport.SUPPORTED,
            {"schema_name": "reporting", "table_name": "orders"},
        ),
        (
            SystemMetadataRequest("table_stats", schema=None, table=None, include_system=True),
            MetadataSupport.SUPPORTED,
            {"schema_name": "public", "table_name": None},
        ),
    ],
)
async def test_postgres_async_base_system_metadata_gating_and_binds(
    request_obj: SystemMetadataRequest, expected_support: MetadataSupport, expected_kwargs: dict[str, Any] | None
) -> None:
    """Async get_system_metadata gates disabled requests and binds domain parameters when enabled."""
    driver = FakeAsyncDriver()
    data_dictionary = PostgresAsyncDataDictionary()

    result = await data_dictionary.get_system_metadata(cast("Any", driver), request_obj)

    assert isinstance(result, SystemMetadataResult)
    assert result.capability.support == expected_support
    if expected_kwargs is None:
        assert result.rows == ()
        assert driver.select_calls == []
    else:
        assert len(driver.select_calls) == 1
        _, kwargs = driver.select_calls[0]
        assert kwargs == expected_kwargs


def test_postgres_dialect_package_reuses_same_cached_globals_entry() -> None:
    """Lazy PEP 562 lookup populates module globals on first access for repeated attribute access."""
    reloaded = importlib.import_module("sqlspec.data_dictionary.dialects.postgres")
    assert reloaded.PostgresSyncDataDictionary is postgres_dictionary_module.PostgresSyncDataDictionary
    assert reloaded.PostgresAsyncDataDictionary is postgres_dictionary_module.PostgresAsyncDataDictionary


def test_postgres_sync_base_missing_domain_query_raises_runtime_error() -> None:
    """Missing PostgreSQL query pack lookup raises a contextual RuntimeError."""
    with pytest.raises(RuntimeError, match="Missing PostgreSQL data-dictionary query: nonexistent/missing"):
        postgres_dictionary_module._postgres_domain_sql("nonexistent", "missing")
