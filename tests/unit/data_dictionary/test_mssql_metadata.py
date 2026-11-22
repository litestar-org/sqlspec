"""SQL Server data-dictionary metadata tests."""

import sqlite3
import subprocess
import sys
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, TypeVar, cast, overload

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
from sqlspec.data_dictionary.dialects import mssql as mssql_pkg
from sqlspec.data_dictionary.dialects.mssql import MssqlSyncDataDictionary, MssqlVersionInfo
from sqlspec.driver import SyncDataDictionaryBase

if TYPE_CHECKING:
    from collections.abc import Callable

_CursorT = TypeVar("_CursorT", bound=sqlite3.Cursor)

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

    @overload
    def cursor(self, factory: None = None) -> CatalogCursor: ...

    @overload
    def cursor(self, factory: "Callable[[sqlite3.Connection], _CursorT]") -> _CursorT: ...

    def cursor(self, factory: "Callable[[sqlite3.Connection], sqlite3.Cursor] | None" = None) -> sqlite3.Cursor:
        return super().cursor(factory=CatalogCursor if factory is None else factory)


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


@pytest.mark.parametrize(
    "dictionary_type", [MssqlSyncDataDictionary, MssqlPythonSyncDataDictionary, PymssqlSyncDataDictionary]
)
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


def test_mssql_sync_dictionary_construction_and_identity() -> None:
    """MssqlSyncDataDictionary constructs directly and matches the lazy package re-export."""
    from sqlspec.data_dictionary.dialects.mssql.dictionary import (
        MssqlSyncDataDictionary as DirectMssqlSyncDataDictionary,
    )

    dictionary = MssqlSyncDataDictionary()

    assert MssqlSyncDataDictionary is DirectMssqlSyncDataDictionary
    assert isinstance(dictionary, SyncDataDictionaryBase)
    assert dictionary.dialect == "mssql"
    assert getattr(mssql_pkg, "MssqlSyncDataDictionary") is DirectMssqlSyncDataDictionary
    assert "MssqlSyncDataDictionary" in mssql_pkg.__all__
    assert "MssqlSyncDataDictionary" in dir(mssql_pkg)
    assert "supports_native_json" in dictionary.list_available_features()


def test_mssql_package_unknown_attribute_raises_attribute_error() -> None:
    """Accessing an unknown attribute on sqlspec.data_dictionary.dialects.mssql raises AttributeError."""
    with pytest.raises(AttributeError, match="no attribute 'nonexistent_mssql_export'"):
        getattr(mssql_pkg, "nonexistent_mssql_export")


def test_mssql_package_lazy_export_avoids_eager_dictionary_or_adapter_imports() -> None:
    """Bare mssql package import keeps dictionary, driver, and optional adapters unloaded."""
    probe = (
        "import sys; "
        "import sqlspec.data_dictionary.dialects.mssql as mssql_pkg; "
        "assert 'sqlspec.data_dictionary.dialects.mssql.dictionary' not in sys.modules; "
        "assert 'sqlspec.driver' not in sys.modules; "
        "assert 'MssqlSyncDataDictionary' in dir(mssql_pkg); "
        "assert 'MssqlVersionInfo' in dir(mssql_pkg); "
        "assert mssql_pkg.MssqlVersionInfo is not None; "
        "assert 'sqlspec.data_dictionary.dialects.mssql.dictionary' not in sys.modules; "
        "cls = getattr(mssql_pkg, 'MssqlSyncDataDictionary'); "
        "from sqlspec.data_dictionary.dialects.mssql.dictionary import MssqlSyncDataDictionary as direct; "
        "assert cls is direct; "
        "assert mssql_pkg.__dict__['MssqlSyncDataDictionary'] is direct; "
        "instance = cls(); "
        "assert instance.dialect == 'mssql'; "
        "assert not any(name.startswith('sqlspec.adapters.') for name in sys.modules)"
    )
    completed = subprocess.run([sys.executable, "-c", probe], capture_output=True, text=True, check=False)
    assert completed.returncode == 0, completed.stderr or completed.stdout


class _ConfigurableSyncDriver:
    """Minimal sync driver double recording calls across SQL Server metadata queries."""

    def __init__(
        self,
        *,
        current_schema: str | None = "dbo",
        version_row: dict[str, Any] | None = None,
        query_rows: dict[str, list[dict[str, Any]]] | None = None,
        driver_features: dict[str, Any] | None = None,
    ) -> None:
        self.current_schema = current_schema
        self.version_row = version_row
        self.query_rows = query_rows or {}
        self.driver_features = driver_features or {}
        self.schema_calls: list[Any] = []
        self.version_calls: list[Any] = []
        self.select_calls: list[tuple[str, dict[str, Any]]] = []

    def select_value_or_none(self, statement: Any, **_kwargs: Any) -> str | None:
        self.schema_calls.append(statement)
        return self.current_schema

    def select_one_or_none(self, statement: Any, **_kwargs: Any) -> dict[str, Any] | None:
        self.version_calls.append(statement)
        return self.version_row

    def select(self, statement: Any, **kwargs: Any) -> list[dict[str, Any]]:
        query_text = getattr(statement, "raw_sql", str(statement))
        self.select_calls.append((query_text, kwargs))
        for marker, rows in self.query_rows.items():
            if marker in query_text:
                return list(rows)
        return []


@pytest.mark.parametrize(
    ("row", "expected_major", "expected_minor", "expected_build", "expected_revision", "expected_json_type"),
    [
        (
            {"product_version": "17.0.1000.7", "edition": "Developer Edition", "engine_edition": 3},
            17,
            0,
            1000,
            7,
            "JSON",
        ),
        (
            {"PRODUCT_VERSION": "17.0.1000.7", "EDITION": "Enterprise Edition", "ENGINE_EDITION": 3},
            17,
            0,
            1000,
            7,
            "JSON",
        ),
        (
            {"product_version": "16.0.4150.1", "edition": "Developer Edition", "engine_edition": 3},
            16,
            0,
            4150,
            1,
            "NVARCHAR(MAX)",
        ),
        (
            {
                "VERSION_STRING": "Microsoft SQL Server 2022 - 16.0.4150.1 (X64)",
                "EDITION": "Standard Edition",
                "ENGINE_EDITION": 2,
            },
            16,
            0,
            4150,
            1,
            "NVARCHAR(MAX)",
        ),
    ],
)
def test_mssql_sync_dictionary_version_parsing_and_json_optimal_type(
    row: dict[str, Any],
    expected_major: int,
    expected_minor: int,
    expected_build: int,
    expected_revision: int,
    expected_json_type: str,
) -> None:
    """Version parsing handles lower/upper keys and selects native JSON on SQL Server 2025+."""
    dictionary = MssqlSyncDataDictionary()
    driver = _ConfigurableSyncDriver(version_row=row)

    version_info = dictionary.get_version(cast(Any, driver))

    assert isinstance(version_info, MssqlVersionInfo)
    assert (version_info.major, version_info.minor, version_info.build, version_info.revision) == (
        expected_major,
        expected_minor,
        expected_build,
        expected_revision,
    )
    assert dictionary.get_optimal_type(cast(Any, driver), "json") == expected_json_type
    assert dictionary.get_optimal_type(cast(Any, driver), "jsonb") == expected_json_type
    assert dictionary.get_feature_flag(cast(Any, driver), "supports_native_json") is (expected_major >= 17)
    assert len(driver.version_calls) == 1


@pytest.mark.parametrize(
    "version_row", [None, {}, {"edition": "Developer Edition"}, {"product_version": None}, {"product_version": ""}]
)
def test_mssql_sync_dictionary_caches_none_on_missing_or_empty_version(version_row: dict[str, Any] | None) -> None:
    """Missing or empty version rows cache None and avoid repeat version queries."""
    dictionary = MssqlSyncDataDictionary()
    driver = _ConfigurableSyncDriver(version_row=version_row)

    assert dictionary.get_version(cast(Any, driver)) is None
    assert dictionary.get_version(cast(Any, driver)) is None
    assert dictionary.get_optimal_type(cast(Any, driver), "json") == "NVARCHAR(MAX)"
    assert len(driver.version_calls) == 1


@pytest.mark.parametrize(
    ("requested_schema", "current_schema", "expected_schema", "expected_queries"),
    [
        ("sales", "tenant_a", "sales", 0),
        (None, "tenant_a", "tenant_a", 1),
        (None, None, "dbo", 1),
        (None, "", "dbo", 1),
    ],
)
def test_mssql_sync_dictionary_resolve_connection_schema(
    requested_schema: str | None, current_schema: str | None, expected_schema: str, expected_queries: int
) -> None:
    """resolve_connection_schema resolves current connection schema and defaults to dbo when empty."""
    dictionary = MssqlSyncDataDictionary()
    driver = _ConfigurableSyncDriver(current_schema=current_schema)

    assert dictionary.resolve_connection_schema(driver, requested_schema) == expected_schema
    assert len(driver.schema_calls) == expected_queries


def test_mssql_sync_dictionary_non_table_ddl_returns_unsupported() -> None:
    """Public get_ddl on non-table objects returns UNSUPPORTED without running table-input queries."""
    dictionary = MssqlSyncDataDictionary()
    driver = _ConfigurableSyncDriver()

    result = dictionary.get_ddl(cast(Any, driver), "v_orders", schema="dbo", object_type="view")

    assert isinstance(result, DDLResult)
    assert result.status == MetadataSupport.UNSUPPORTED
    assert result.ddl is None
    assert result.identity.dialect == "mssql"
    assert result.identity.schema == "dbo"
    assert result.identity.name == "v_orders"
    assert not any("ddl_table_inputs" in query_text for query_text, _ in driver.select_calls)


def test_mssql_sync_dictionary_table_ddl_skips_index_inputs_when_columns_empty() -> None:
    """Table DDL returns without issuing ddl index input queries when column inputs are empty."""
    dictionary = MssqlSyncDataDictionary()
    driver = _ConfigurableSyncDriver(query_rows={"sqlspec:mssql:ddl_table_inputs": []})

    result = dictionary.get_ddl(cast(Any, driver), "missing_table", schema="dbo", object_type="table")

    assert isinstance(result, DDLResult)
    assert result.status == MetadataSupport.UNSUPPORTED
    assert not any("ddl_index_inputs" in query_text for query_text, _ in driver.select_calls)


def test_mssql_sync_dictionary_tables_merges_ordered_and_catalog_rows() -> None:
    """get_tables merges ordered FK-graph rows with unordered catalog rows."""
    dictionary = MssqlSyncDataDictionary()
    by_schema_sql = dictionary.get_domain_query("tables", "by_schema").raw_sql
    all_by_schema_sql = dictionary.get_domain_query("tables", "all_by_schema").raw_sql
    driver = _ConfigurableSyncDriver(
        query_rows={
            by_schema_sql: [{"schema_name": "dbo", "table_name": "parent", "table_type": "BASE TABLE"}],
            all_by_schema_sql: [
                {"schema_name": "dbo", "table_name": "parent", "table_type": "BASE TABLE"},
                {"schema_name": "dbo", "table_name": "orphan", "table_type": "BASE TABLE"},
            ],
        }
    )

    tables = dictionary.get_tables(cast(Any, driver), schema="dbo")

    assert [row["table_name"] for row in tables] == ["parent", "orphan"]
    assert len(driver.select_calls) == 2


@pytest.mark.parametrize(
    ("rows", "object_name", "expected_count"),
    [
        (
            [
                {"object_name": "orders", "permission_name": "SELECT"},
                {"object_name": "accounts", "permission_name": "INSERT"},
            ],
            "orders",
            1,
        ),
        (
            [
                {"OBJECT_NAME": "orders", "PERMISSION_NAME": "SELECT"},
                {"OBJECT_NAME": "accounts", "PERMISSION_NAME": "INSERT"},
            ],
            "orders",
            1,
        ),
        (
            [
                {"object_name": "orders", "permission_name": "SELECT"},
                {"object_name": "accounts", "permission_name": "INSERT"},
            ],
            None,
            2,
        ),
    ],
)
def test_mssql_sync_dictionary_privileges_filtering(
    rows: list[dict[str, Any]], object_name: str | None, expected_count: int
) -> None:
    """get_privileges filters matching object rows across lowercase and uppercase keys."""
    dictionary = MssqlSyncDataDictionary()
    permissions_sql = dictionary.get_domain_query("permissions", "by_schema").raw_sql
    driver = _ConfigurableSyncDriver(query_rows={permissions_sql: rows})

    result = dictionary.get_privileges(cast(Any, driver), object_name=object_name, schema="dbo")

    assert len(result.items) == expected_count
    if object_name is not None:
        item = cast("dict[str, Any]", result.items[0])
        assert (item.get("object_name") or item.get("OBJECT_NAME")) == object_name


@pytest.mark.parametrize(
    ("rows", "object_name", "expected_count"),
    [
        (
            [
                {"referencing_object_name": "orders", "referenced_object_name": "customers"},
                {"referencing_object_name": "invoices", "referenced_object_name": "orders"},
            ],
            "orders",
            1,
        ),
        (
            [
                {"REFERENCING_OBJECT_NAME": "orders", "REFERENCED_OBJECT_NAME": "customers"},
                {"REFERENCING_OBJECT_NAME": "invoices", "REFERENCED_OBJECT_NAME": "orders"},
            ],
            "orders",
            1,
        ),
        (
            [
                {"referencing_object_name": "orders", "referenced_object_name": "customers"},
                {"referencing_object_name": "invoices", "referenced_object_name": "orders"},
            ],
            None,
            2,
        ),
    ],
)
def test_mssql_sync_dictionary_dependencies_filtering(
    rows: list[dict[str, Any]], object_name: str | None, expected_count: int
) -> None:
    """get_dependencies filters matching referencing object rows across key casings."""
    dictionary = MssqlSyncDataDictionary()
    dependencies_sql = dictionary.get_domain_query("dependencies", "by_schema").raw_sql
    driver = _ConfigurableSyncDriver(query_rows={dependencies_sql: rows})

    result = dictionary.get_dependencies(cast(Any, driver), object_name=object_name, schema="dbo")

    assert len(result.items) == expected_count
    if object_name is not None:
        item = cast("dict[str, Any]", result.items[0])
        assert (item.get("referencing_object_name") or item.get("REFERENCING_OBJECT_NAME")) == object_name


def test_mssql_sync_dictionary_system_metadata_capabilities_and_gating() -> None:
    """MssqlSyncDataDictionary exposes system metadata capabilities and enforces opt-in gating."""
    dictionary = MssqlSyncDataDictionary()
    driver = FakeSyncDriver()

    capabilities = dictionary.get_system_metadata_capabilities(cast(Any, driver))
    assert tuple(cap.domain for cap in capabilities) == ("dmv_exec_requests", "query_store_runtime")

    denied = dictionary.get_system_metadata(
        cast(Any, driver), SystemMetadataRequest("dmv_exec_requests", include_system=True)
    )
    assert denied.capability.support == MetadataSupport.UNSUPPORTED
    assert driver.select_calls == []


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
