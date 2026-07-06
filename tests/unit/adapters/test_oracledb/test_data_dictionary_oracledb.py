"""Unit tests for Oracle data dictionary version handling."""

from typing import TYPE_CHECKING, Any, cast

import pytest

from sqlspec.adapters.oracledb.data_dictionary import OracledbAsyncDataDictionary, OracledbSyncDataDictionary
from sqlspec.data_dictionary import (
    DDLResult,
    MetadataFidelity,
    MetadataRisk,
    MetadataSource,
    MetadataSupport,
    TableMetadata,
    VersionInfo,
    get_data_dictionary_loader,
    get_dialect_config,
)
from sqlspec.data_dictionary.dialects.oracle import (
    extract_oracle_version_value,
    list_oracle_available_features,
    merge_oracle_table_lists,
    oracle_supports_json_blob,
    oracle_supports_native_json,
    oracle_supports_oson_blob,
    parse_oracle_compatible_major,
    parse_oracle_version_components,
    resolve_oracle_feature_flag,
    resolve_oracle_json_type,
)

if TYPE_CHECKING:
    from sqlspec.adapters.oracledb.driver import OracleAsyncDriver, OracleSyncDriver

ORACLE_AI_COMPONENT_ROW = {
    "product": "Oracle AI Database 26ai Free",
    "version": "23.26.0.0.0",
    "status": "Develop, Learn, and Run for Free",
}


class _FakeResult:
    """Return predetermined rows to mimic SQLResult."""

    def __init__(self, rows: "list[dict[str, Any]]") -> None:
        self._rows = rows

    def get_data(self, schema_type: "type[Any] | None" = None) -> "list[dict[str, Any]]":
        """Return stored rows."""

        _ = schema_type
        return self._rows


class _FakeSyncOracleDriver:
    """Minimal sync Oracle driver stub for data dictionary tests."""

    def __init__(
        self, rows: "list[dict[str, Any]]", compatible: str = "23.0.0.0.0", service: str = "AUTONOMOUS SHARED"
    ) -> None:
        self._rows = rows
        self._compatible = compatible
        self._service = service

    def execute(self, statement: str, *args: Any, **kwargs: Any) -> "_FakeResult":
        """Return stored rows regardless of SQL."""

        _ = (statement, args, kwargs)
        return _FakeResult(self._rows)

    def select_value(self, statement: str, *args: Any, **kwargs: Any) -> str:
        """Return compatible parameter when requested."""

        _ = (args, kwargs)
        if "v$parameter" in statement.lower():
            return self._compatible
        raise ValueError(f"Unexpected select_value SQL: {statement}")

    def select_value_or_none(self, statement: str, *args: Any, **kwargs: Any) -> str | None:
        """Return cloud service identifier when requested."""

        _ = (args, kwargs)
        if "sys_context" in statement.lower():
            return self._service
        return None

    def select_one_or_none(self, statement: str, *args: Any, **kwargs: Any) -> "dict[str, Any] | None":
        """Return the first row dict when requested."""

        _ = (statement, args, kwargs)
        if not self._rows:
            return None
        return dict(self._rows[0])


class _FakeAsyncOracleDriver:
    """Minimal async Oracle driver stub for data dictionary tests."""

    def __init__(
        self, rows: "list[dict[str, Any]]", compatible: str = "23.0.0.0.0", service: str = "AUTONOMOUS SHARED"
    ) -> None:
        self._rows = rows
        self._compatible = compatible
        self._service = service

    async def execute(self, statement: str, *args: Any, **kwargs: Any) -> "_FakeResult":
        """Return stored rows regardless of SQL (async)."""

        _ = (statement, args, kwargs)
        return _FakeResult(self._rows)

    async def select_value(self, statement: str, *args: Any, **kwargs: Any) -> str:
        """Return compatible parameter when requested (async)."""

        _ = (args, kwargs)
        if "v$parameter" in statement.lower():
            return self._compatible
        raise ValueError(f"Unexpected select_value SQL: {statement}")

    async def select_value_or_none(self, statement: str, *args: Any, **kwargs: Any) -> str | None:
        """Return cloud service identifier when requested (async)."""

        _ = (args, kwargs)
        if "sys_context" in statement.lower():
            return self._service
        return None

    async def select_one_or_none(self, statement: str, *args: Any, **kwargs: Any) -> "dict[str, Any] | None":
        """Return the first row dict when requested (async)."""

        _ = (statement, args, kwargs)
        if not self._rows:
            return None
        return dict(self._rows[0])


class _RecordingSyncOracleDriver(_FakeSyncOracleDriver):
    """Sync Oracle driver stub that records scalar query calls."""

    def __init__(self) -> None:
        super().__init__([dict(ORACLE_AI_COMPONENT_ROW)])
        self.select_value_calls: list[tuple[str, dict[str, Any]]] = []

    def select_value(self, statement: Any, *args: Any, **kwargs: Any) -> str:
        """Return DDL for DBMS_METADATA calls and record bind values."""

        statement_text = getattr(statement, "raw_sql", str(statement))
        self.select_value_calls.append((statement_text, kwargs))
        if "DBMS_METADATA.GET_DDL" in statement_text:
            return 'CREATE TABLE "APP"."ORDERS" ("ID" NUMBER)'
        return super().select_value(statement_text, *args, **kwargs)

    def select(self, statement: Any, *args: Any, **kwargs: Any) -> "list[dict[str, Any]]":
        """Return a predictable row and record domain query calls."""

        statement_text = getattr(statement, "raw_sql", str(statement))
        self.select_value_calls.append((statement_text, kwargs))
        statement_text_lower = statement_text.lower()
        if "all_objects" in statement_text_lower:
            return [{"object_name": "ORDERS", "object_type": "TABLE"}]
        if "all_constraints" in statement_text_lower:
            return [{"constraint_name": "ORDERS_PK", "constraint_type": "P"}]
        if "all_dependencies" in statement_text_lower:
            return [{"object_name": "ORDERS", "referenced_name": "CUSTOMERS"}]
        return [{"schema_name": kwargs.get("schema_name")}]


class _RecordingAsyncOracleDriver(_FakeAsyncOracleDriver):
    """Async Oracle driver stub that records scalar query calls."""

    def __init__(self) -> None:
        super().__init__([dict(ORACLE_AI_COMPONENT_ROW)])
        self.select_value_calls: list[tuple[str, dict[str, Any]]] = []

    async def select_value(self, statement: Any, *args: Any, **kwargs: Any) -> str:
        """Return DDL for DBMS_METADATA calls and record bind values."""

        statement_text = getattr(statement, "raw_sql", str(statement))
        self.select_value_calls.append((statement_text, kwargs))
        if "DBMS_METADATA.GET_DDL" in statement_text:
            return 'CREATE TABLE "APP"."ORDERS" ("ID" NUMBER)'
        return await super().select_value(statement_text, *args, **kwargs)

    async def select(self, statement: Any, *args: Any, **kwargs: Any) -> "list[dict[str, Any]]":
        """Return a predictable row and record domain query calls."""

        statement_text = getattr(statement, "raw_sql", str(statement))
        self.select_value_calls.append((statement_text, kwargs))
        if "all_objects" in statement_text.lower():
            return [{"object_name": "ORDERS", "object_type": "TABLE"}]
        return [{"schema_name": kwargs.get("schema_name")}]


@pytest.fixture
def oracle_component_rows() -> "list[dict[str, Any]]":
    """Return canonical Oracle component version rows for tests."""

    return [dict(ORACLE_AI_COMPONENT_ROW)]


@pytest.fixture
def oracle_sync_driver(oracle_component_rows: "list[dict[str, Any]]") -> "_FakeSyncOracleDriver":
    """Build a fake sync Oracle driver using the canonical component row."""

    return _FakeSyncOracleDriver(oracle_component_rows, compatible="23.20.0.0.0", service="AUTONOMOUS AI")


@pytest.fixture
def oracle_async_driver(oracle_component_rows: "list[dict[str, Any]]") -> "_FakeAsyncOracleDriver":
    """Build a fake async Oracle driver using the canonical component row."""

    return _FakeAsyncOracleDriver(oracle_component_rows, compatible="23.20.0.0.0", service="AUTONOMOUS AI")


def test_oracle_dialect_helpers_resolve_shared_version_rules() -> None:
    """Ensure Oracle dialect helpers cover sync and async dictionary rules."""

    config = get_dialect_config("oracle")

    assert extract_oracle_version_value({"VERSION": "23.26.0.0.0"}) == "23.26.0.0.0"
    assert extract_oracle_version_value(("19.20.0.0.0",)) == "19.20.0.0.0"
    assert parse_oracle_version_components("23.26.0.0.0") == (23, 26, 0)
    assert parse_oracle_compatible_major("23.20.0.0.0") == 23

    assert oracle_supports_native_json(23, 20) is True
    assert oracle_supports_native_json(21, 19) is False
    assert oracle_supports_oson_blob(19, True) is True
    assert oracle_supports_oson_blob(19, False) is False
    assert oracle_supports_json_blob(12) is True

    assert resolve_oracle_json_type(VersionInfo(23, 0, 0), compatible_major=20, is_autonomous=False) == "JSON"
    assert resolve_oracle_json_type(VersionInfo(21, 0, 0), compatible_major=19, is_autonomous=False) == "BLOB"
    assert resolve_oracle_json_type(VersionInfo(11, 0, 0), compatible_major=None, is_autonomous=False) == "CLOB"
    assert resolve_oracle_json_type(None, compatible_major=None, is_autonomous=False) == "CLOB"

    assert (
        resolve_oracle_feature_flag(
            config, VersionInfo(23, 0, 0), "supports_native_json", compatible_major=20, is_autonomous=False
        )
        is True
    )
    assert (
        resolve_oracle_feature_flag(
            config, VersionInfo(19, 0, 0), "supports_oson_blob", compatible_major=None, is_autonomous=True
        )
        is True
    )
    assert (
        resolve_oracle_feature_flag(
            config, VersionInfo(19, 0, 0), "is_autonomous", compatible_major=None, is_autonomous=True
        )
        is True
    )
    assert "supports_native_json" in list_oracle_available_features(config)


def test_oracle_dialect_helper_merges_ordered_tables_with_remainder() -> None:
    """Ensure Oracle table merge helper preserves dependency order and coverage."""

    ordered: list[TableMetadata] = [{"table_name": "parent"}]
    all_tables: list[TableMetadata] = [{"table_name": "child"}, {"table_name": "parent"}]

    assert merge_oracle_table_lists(ordered, all_tables) == [{"table_name": "parent"}, {"table_name": "child"}]
    assert merge_oracle_table_lists([], all_tables) == [{"table_name": "child"}, {"table_name": "parent"}]


def test_oracle_columns_uses_all_tab_cols_for_hidden_columns() -> None:
    """Oracle column discovery should include hidden and system-generated columns."""

    data_dictionary = OracledbSyncDataDictionary()

    columns_by_schema = data_dictionary.get_query_text("columns_by_schema").lower()
    columns_by_table = data_dictionary.get_query_text("columns_by_table").lower()

    assert "all_tab_cols" in columns_by_schema
    assert "all_tab_columns" not in columns_by_schema
    assert "all_tab_cols" in columns_by_table
    assert "all_tab_columns" not in columns_by_table


def test_oracle_domain_query_packs_cover_c5_sources() -> None:
    """Oracle should expose direct domain query packs for the C5 metadata family."""

    loader = get_data_dictionary_loader()
    expected_sources = {
        "objects": ("by_owner", "ALL_OBJECTS"),
        "schemas": ("by_owner", "ALL_USERS"),
        "tables": ("by_owner", "ALL_TABLES"),
        "columns": ("by_owner", "ALL_TAB_COLS"),
        "constraints": ("by_owner", "ALL_CONSTRAINTS"),
        "indexes": ("by_owner", "ALL_IND_EXPRESSIONS"),
        "views": ("by_owner", "ALL_VIEWS"),
        "materialized_views": ("by_owner", "ALL_MVIEWS"),
        "sequences": ("by_owner", "ALL_SEQUENCES"),
        "routines": ("by_owner", "ALL_PROCEDURES"),
        "arguments": ("by_owner", "ALL_ARGUMENTS"),
        "source": ("by_owner", "ALL_SOURCE"),
        "triggers": ("by_owner", "ALL_TRIGGERS"),
        "comments": ("by_owner", "ALL_COL_COMMENTS"),
        "grants": ("by_owner", "ALL_TAB_PRIVS"),
        "dependencies": ("by_owner", "ALL_DEPENDENCIES"),
        "partitions": ("by_owner", "ALL_TAB_PARTITIONS"),
        "lob_storage": ("by_owner", "ALL_LOBS"),
        "ddl": ("dbms_metadata", "DBMS_METADATA.GET_DDL"),
        "system": ("no_diagnostics", "diagnostics_enabled"),
    }

    for domain, (query_name, expected_source) in expected_sources.items():
        query = loader.get_domain_query("oracle", domain, query_name)

        assert query.is_supported, f"{domain}/{query_name} is not available: {query.warnings}"
        assert query.query_text is not None
        assert expected_source.lower() in query.query_text.lower()


def test_oracle_scope_modes_report_privileged_requirements() -> None:
    """Oracle capability profiles should make USER/ALL/DBA/CDB visibility explicit."""

    data_dictionary = OracledbSyncDataDictionary()
    profile = data_dictionary.get_metadata_capabilities(cast("OracleSyncDriver", _RecordingSyncOracleDriver()))

    assert profile.get("scope:user").support == MetadataSupport.SUPPORTED
    assert profile.get("scope:all").support == MetadataSupport.SUPPORTED
    assert profile.get("scope:all").warnings == ("ALL_* views can omit inaccessible objects.",)
    assert profile.get("scope:dba").support == MetadataSupport.UNSUPPORTED
    assert MetadataRisk.PRIVILEGED in profile.get("scope:dba").risks
    assert profile.get("scope:cdb").support == MetadataSupport.UNSUPPORTED
    assert MetadataRisk.PRIVILEGED in profile.get("scope:cdb").risks


def test_oracle_diagnostics_disabled_by_default() -> None:
    """Oracle diagnostics should stay disabled unless the caller explicitly accepts privilege and license gates."""

    data_dictionary = OracledbSyncDataDictionary()
    sync_driver = cast("OracleSyncDriver", _RecordingSyncOracleDriver())

    result = data_dictionary.get_system_metadata(sync_driver, "awr")

    assert result.domain == "system"
    assert result.capability.support == MetadataSupport.UNSUPPORTED
    assert MetadataRisk.LICENSE_GATED in result.capability.risks
    assert MetadataRisk.PRIVILEGED in result.capability.risks


def test_oracle_get_ddl_uses_dbms_metadata() -> None:
    """Oracle native DDL should come from DBMS_METADATA with bind values."""

    data_dictionary = OracledbSyncDataDictionary()
    sync_driver_stub = _RecordingSyncOracleDriver()
    sync_driver = cast("OracleSyncDriver", sync_driver_stub)

    result = data_dictionary.get_ddl(sync_driver, "orders", schema="app", object_type="TABLE")

    assert result.domain == "ddl"
    assert result.capability.source == MetadataSource.NATIVE_API
    assert result.capability.fidelity == MetadataFidelity.NATIVE
    assert result.capability.support == MetadataSupport.SUPPORTED
    assert len(result.items) == 1
    ddl = cast("DDLResult", result.items[0])
    assert ddl.ddl == 'CREATE TABLE "APP"."ORDERS" ("ID" NUMBER)'
    assert ddl.source == MetadataSource.NATIVE_API
    assert ddl.fidelity == MetadataFidelity.NATIVE
    assert ddl.warnings == result.warnings
    statement_text, kwargs = sync_driver_stub.select_value_calls[0]
    assert "DBMS_METADATA.GET_DDL" in statement_text
    assert kwargs == {"object_type": "TABLE", "object_name": "ORDERS", "owner": "APP"}


def test_oracle_replacement_methods_execute_domain_queries() -> None:
    """Oracle replacement methods should execute direct domain query packs."""

    data_dictionary = OracledbSyncDataDictionary()
    sync_driver_stub = _RecordingSyncOracleDriver()
    sync_driver = cast("OracleSyncDriver", sync_driver_stub)

    objects = data_dictionary.get_objects(sync_driver, schema="app")
    constraints = data_dictionary.get_constraints(sync_driver, table="orders", schema="app")
    dependencies = data_dictionary.get_dependencies(sync_driver, object_name="orders", schema="app")

    assert objects.domain == "objects"
    assert objects.items == ({"object_name": "ORDERS", "object_type": "TABLE"},)
    assert constraints.domain == "constraints"
    assert constraints.items == ({"constraint_name": "ORDERS_PK", "constraint_type": "P"},)
    assert dependencies.domain == "dependencies"
    assert dependencies.items == ({"object_name": "ORDERS", "referenced_name": "CUSTOMERS"},)
    assert any("all_objects" in statement.lower() for statement, _ in sync_driver_stub.select_value_calls)
    assert any("all_constraints" in statement.lower() for statement, _ in sync_driver_stub.select_value_calls)
    assert any("all_dependencies" in statement.lower() for statement, _ in sync_driver_stub.select_value_calls)


def test_sync_data_dictionary_detects_native_json_type(oracle_sync_driver: "_FakeSyncOracleDriver") -> None:
    """Ensure sync data dictionary maps Oracle 23ai to native JSON columns."""

    data_dictionary = OracledbSyncDataDictionary()
    sync_driver = cast("OracleSyncDriver", oracle_sync_driver)
    version_info = data_dictionary.get_version(sync_driver)

    assert version_info is not None
    assert version_info.supports_native_json()
    assert data_dictionary.get_optimal_type(sync_driver, "json") == "JSON"


@pytest.mark.anyio
async def test_async_data_dictionary_detects_native_json_type(oracle_async_driver: "_FakeAsyncOracleDriver") -> None:
    """Ensure async data dictionary maps Oracle 23ai to native JSON columns."""

    data_dictionary = OracledbAsyncDataDictionary()
    async_driver = cast("OracleAsyncDriver", oracle_async_driver)
    version_info = await data_dictionary.get_version(async_driver)

    assert version_info is not None
    assert version_info.supports_native_json()
    assert await data_dictionary.get_optimal_type(async_driver, "json") == "JSON"


@pytest.mark.anyio
async def test_async_oracle_get_ddl_uses_dbms_metadata() -> None:
    """Async Oracle native DDL should mirror the sync DBMS_METADATA path."""

    data_dictionary = OracledbAsyncDataDictionary()
    async_driver_stub = _RecordingAsyncOracleDriver()
    async_driver = cast("OracleAsyncDriver", async_driver_stub)

    result = await data_dictionary.get_ddl(async_driver, "orders", schema="app", object_type="TABLE")

    assert result.domain == "ddl"
    assert len(result.items) == 1
    ddl = cast("DDLResult", result.items[0])
    assert ddl.ddl == 'CREATE TABLE "APP"."ORDERS" ("ID" NUMBER)'
    statement_text, kwargs = async_driver_stub.select_value_calls[0]
    assert "DBMS_METADATA.GET_DDL" in statement_text
    assert kwargs == {"object_type": "TABLE", "object_name": "ORDERS", "owner": "APP"}


@pytest.mark.anyio
async def test_async_oracle_replacement_methods_execute_domain_queries() -> None:
    """Async Oracle replacement methods should execute direct domain query packs."""

    data_dictionary = OracledbAsyncDataDictionary()
    async_driver_stub = _RecordingAsyncOracleDriver()
    async_driver = cast("OracleAsyncDriver", async_driver_stub)

    objects = await data_dictionary.get_objects(async_driver, schema="app")

    assert objects.domain == "objects"
    assert objects.items == ({"object_name": "ORDERS", "object_type": "TABLE"},)
    statement_text, kwargs = async_driver_stub.select_value_calls[0]
    assert "all_objects" in statement_text.lower()
    assert kwargs == {"schema_name": "APP", "object_name": None}
