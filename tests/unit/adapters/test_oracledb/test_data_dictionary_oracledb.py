"""Unit tests for Oracle data dictionary version handling."""

from typing import TYPE_CHECKING, Any, cast

import pytest

from sqlspec.adapters.oracledb.data_dictionary import OracledbAsyncDataDictionary, OracledbSyncDataDictionary
from sqlspec.adapters.oracledb.driver import OracleAsyncDriver, OracleSyncDriver
from sqlspec.data_dictionary import (
    DDLResult,
    MetadataFidelity,
    MetadataRisk,
    MetadataSource,
    MetadataSupport,
    SystemMetadataRequest,
    SystemMetadataResult,
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
    from sqlspec.adapters.oracledb._typing import OracleAsyncConnection, OracleSyncConnection

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
    assert extract_oracle_version_value({"VERSION": "23.0.0.0.0", "VERSION_FULL": "23.26.0.0.0"}) == "23.26.0.0.0"
    assert extract_oracle_version_value({"version": "19.0.0.0.0", "version_full": None}) == "19.0.0.0.0"
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

    columns_by_schema = data_dictionary.get_query_text("columns", "by_schema").lower()
    columns_by_table = data_dictionary.get_query_text("columns", "by_table").lower()

    assert "all_tab_cols" in columns_by_schema
    assert "all_tab_columns" not in columns_by_schema
    assert "all_tab_identity_cols" in columns_by_schema
    assert "as identity_generation" in columns_by_schema
    assert "as sequence_name" in columns_by_schema
    assert "as is_primary" in columns_by_schema
    assert "all_tab_cols" in columns_by_table
    assert "all_tab_columns" not in columns_by_table
    assert "all_tab_identity_cols" in columns_by_table
    assert "as identity_generation" in columns_by_table
    assert "as sequence_name" in columns_by_table
    assert "as is_primary" in columns_by_table


def test_oracle_domain_query_packs_cover_c5_sources() -> None:
    """Oracle should expose direct domain query packs for the C5 metadata family."""

    loader = get_data_dictionary_loader()
    expected_sources = {
        "objects": ("by_schema", "ALL_OBJECTS"),
        "schemas": ("by_schema", "ALL_USERS"),
        "tables": ("by_schema", "ALL_TABLES"),
        "columns": ("by_schema", "ALL_TAB_COLS"),
        "constraints": ("by_schema", "ALL_CONSTRAINTS"),
        "indexes": ("by_schema", "ALL_INDEXES"),
        "views": ("by_schema", "ALL_VIEWS"),
        "materialized_views": ("by_schema", "ALL_MVIEWS"),
        "sequences": ("by_schema", "ALL_SEQUENCES"),
        "routines": ("by_schema", "ALL_PROCEDURES"),
        "arguments": ("by_schema", "ALL_ARGUMENTS"),
        "source": ("by_schema", "ALL_SOURCE"),
        "triggers": ("by_schema", "ALL_TRIGGERS"),
        "comments": ("by_schema", "ALL_COL_COMMENTS"),
        "grants": ("by_schema", "ALL_TAB_PRIVS"),
        "dependencies": ("by_schema", "ALL_DEPENDENCIES"),
        "partitions": ("by_schema", "ALL_TAB_PARTITIONS"),
        "lob_storage": ("by_schema", "ALL_LOBS"),
        "ddl": ("by_object", "DBMS_METADATA.GET_DDL"),
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

    result = data_dictionary.get_system_metadata(sync_driver, SystemMetadataRequest("awr"))

    assert isinstance(result, SystemMetadataResult)
    assert result.request.domain == "awr"
    assert result.capability.support == MetadataSupport.GATED
    assert MetadataRisk.LICENSE_GATED in result.capability.risks
    assert MetadataRisk.PRIVILEGED in result.capability.risks
    assert "disabled by default" in result.warnings[0]


def test_oracle_system_metadata_supports_legacy_license_opt_in() -> None:
    """Oracle diagnostics should translate legacy license acknowledgement kwargs to the request contract."""

    data_dictionary = OracledbSyncDataDictionary()
    sync_driver = cast("OracleSyncDriver", _RecordingSyncOracleDriver())

    result = data_dictionary.get_system_metadata(sync_driver, "awr", include_sensitive=True, acknowledge_license=True)

    assert isinstance(result, SystemMetadataResult)
    assert result.request.include_performance is True
    assert result.request.allow_license_gated_diagnostics is True
    assert result.capability.support == MetadataSupport.SUPPORTED
    assert result.rows == ({"diagnostics_enabled": True, "domain": "awr"},)


def test_oracle_get_ddl_uses_dbms_metadata() -> None:
    """Oracle native DDL should come from DBMS_METADATA with bind values."""

    data_dictionary = OracledbSyncDataDictionary()
    sync_driver_stub = _RecordingSyncOracleDriver()
    sync_driver = cast("OracleSyncDriver", sync_driver_stub)

    result = data_dictionary.get_ddl(sync_driver, "orders", schema="app", object_type="TABLE")

    assert isinstance(result, DDLResult)
    assert result.identity.schema == "APP"
    assert result.identity.name == "ORDERS"
    assert result.ddl == 'CREATE TABLE "APP"."ORDERS" ("ID" NUMBER)'
    assert result.source == MetadataSource.NATIVE_API
    assert result.fidelity == MetadataFidelity.NATIVE
    assert result.status == MetadataSupport.SUPPORTED
    assert result.warnings
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

    assert isinstance(result, DDLResult)
    assert result.identity.schema == "APP"
    assert result.identity.name == "ORDERS"
    assert result.ddl == 'CREATE TABLE "APP"."ORDERS" ("ID" NUMBER)'
    statement_text, kwargs = async_driver_stub.select_value_calls[0]
    assert "DBMS_METADATA.GET_DDL" in statement_text
    assert kwargs == {"object_type": "TABLE", "object_name": "ORDERS", "owner": "APP"}


@pytest.mark.anyio
async def test_async_oracle_system_metadata_returns_contract_result() -> None:
    """Async Oracle diagnostics should return the typed opt-in envelope."""

    data_dictionary = OracledbAsyncDataDictionary()
    async_driver = cast("OracleAsyncDriver", _RecordingAsyncOracleDriver())

    result = await data_dictionary.get_system_metadata(
        async_driver, SystemMetadataRequest("awr", include_performance=True, allow_license_gated_diagnostics=True)
    )

    assert isinstance(result, SystemMetadataResult)
    assert result.capability.support == MetadataSupport.SUPPORTED
    assert result.source == MetadataSource.SYSTEM_VIEW
    assert result.rows == ({"diagnostics_enabled": True, "domain": "awr"},)


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


def test_oracle_26ai_sync_data_dictionary_introspection_methods() -> None:
    data_dictionary = OracledbSyncDataDictionary()
    sync_driver_stub = _RecordingSyncOracleDriver()
    sync_driver = cast("OracleSyncDriver", sync_driver_stub)

    vpool = data_dictionary.get_vector_memory_pool(sync_driver)
    assert isinstance(vpool, list)

    has_ai = data_dictionary.has_cloud_ai(sync_driver)
    assert has_ai is True

    apex = data_dictionary.get_apex_release(sync_driver)
    assert isinstance(apex, dict)

    dviews = data_dictionary.get_json_duality_views(sync_driver, schema="app")
    assert isinstance(dviews, list)


@pytest.mark.anyio
async def test_oracle_26ai_async_data_dictionary_introspection_methods() -> None:
    data_dictionary = OracledbAsyncDataDictionary()
    async_driver_stub = _RecordingAsyncOracleDriver()
    async_driver = cast("OracleAsyncDriver", async_driver_stub)

    vpool = await data_dictionary.get_vector_memory_pool(async_driver)
    assert isinstance(vpool, list)

    has_ai = await data_dictionary.has_cloud_ai(async_driver)
    assert has_ai is True

    apex = await data_dictionary.get_apex_release(async_driver)
    assert isinstance(apex, dict)

    dviews = await data_dictionary.get_json_duality_views(async_driver, schema="app")
    assert isinstance(dviews, list)


def test_oracle_indexes_sql_does_not_listagg_column_expression() -> None:
    """Oracle indexes SQL text must not aggregate column_expression via LISTAGG."""
    data_dictionary = OracledbSyncDataDictionary()
    by_schema_sql = str(data_dictionary.get_query("indexes", "by_schema")).upper()
    by_table_sql = str(data_dictionary.get_query("indexes", "by_table")).upper()

    assert "LISTAGG(IE.COLUMN_EXPRESSION" not in by_schema_sql
    assert "COLUMN_EXPRESSION" not in by_schema_sql
    assert "LISTAGG(IE.COLUMN_EXPRESSION" not in by_table_sql


def test_oracle_sync_get_indexes_merges_expressions() -> None:
    """get_indexes must query expressions and merge them into index records."""
    data_dictionary = OracledbSyncDataDictionary()

    class _StubSyncDriver:
        def __init__(self) -> None:
            self.queries: list[str] = []

        def select(self, query: Any, **kwargs: Any) -> list[dict[str, Any]]:
            sql_text = str(query)
            self.queries.append(sql_text)
            if "all_ind_expressions" in sql_text:
                return [
                    {
                        "schema_name": "HR",
                        "table_name": "EMPLOYEES",
                        "index_name": "IDX_EMP_UPPER_EMAIL",
                        "column_position": 1,
                        "column_expression": "UPPER(EMAIL)",
                    }
                ]
            return [
                {
                    "schema_name": "HR",
                    "table_name": "EMPLOYEES",
                    "index_name": "IDX_EMP_UPPER_EMAIL",
                    "index_type": "NORMAL",
                    "uniqueness": "NONUNIQUE",
                    "columns": None,
                }
            ]

    driver = cast("OracleSyncDriver", _StubSyncDriver())
    indexes = data_dictionary.get_indexes(driver, schema="HR")
    assert len(indexes) == 1
    assert indexes[0]["index_name"] == "IDX_EMP_UPPER_EMAIL"
    assert indexes[0]["expressions"] == "UPPER(EMAIL)"
    assert indexes[0]["expression"] == "UPPER(EMAIL)"


@pytest.mark.anyio
async def test_oracle_async_get_indexes_merges_expressions() -> None:
    """Async get_indexes must query expressions and merge them into index records."""
    data_dictionary = OracledbAsyncDataDictionary()

    class _StubAsyncDriver:
        def __init__(self) -> None:
            self.queries: list[str] = []

        async def select(self, query: Any, **kwargs: Any) -> list[dict[str, Any]]:
            sql_text = str(query)
            self.queries.append(sql_text)
            if "all_ind_expressions" in sql_text:
                return [
                    {
                        "schema_name": "HR",
                        "table_name": "EMPLOYEES",
                        "index_name": "IDX_EMP_UPPER_EMAIL",
                        "column_position": 1,
                        "column_expression": "UPPER(EMAIL)",
                    }
                ]
            return [
                {
                    "schema_name": "HR",
                    "table_name": "EMPLOYEES",
                    "index_name": "IDX_EMP_UPPER_EMAIL",
                    "index_type": "NORMAL",
                    "uniqueness": "NONUNIQUE",
                    "columns": None,
                }
            ]

    driver = cast("OracleAsyncDriver", _StubAsyncDriver())
    indexes = await data_dictionary.get_indexes(driver, schema="HR")
    assert len(indexes) == 1
    assert indexes[0]["index_name"] == "IDX_EMP_UPPER_EMAIL"
    assert indexes[0]["expressions"] == "UPPER(EMAIL)"
    assert indexes[0]["expression"] == "UPPER(EMAIL)"


@pytest.mark.anyio
async def test_oracle_async_get_native_object_ddl_awaits_async_lob() -> None:
    """Async _get_native_object_ddl awaits an AsyncLOB read() instead of stringifying the coroutine."""
    data_dictionary = OracledbAsyncDataDictionary()

    class _AsyncLOB:
        async def read(self) -> str:
            return "CREATE TABLE FOO (ID NUMBER)"

    class _AsyncDriverStub:
        async def select_value(self, query: Any, **kwargs: Any) -> Any:
            return _AsyncLOB()

    driver = cast("OracleAsyncDriver", _AsyncDriverStub())
    result = await data_dictionary._get_native_object_ddl(
        driver, "FOO", schema="HR", object_type="TABLE", include_dependencies=False, redact=False
    )
    assert result is not None
    assert result.ddl == "CREATE TABLE FOO (ID NUMBER)"
    assert "<coroutine" not in str(result.ddl)


def test_oracle_get_tables_single_query() -> None:
    """get_tables issues a single catalog query (tables.by_schema) without a redundant names query."""
    data_dictionary = OracledbSyncDataDictionary()
    queries: list[str] = []

    class _StubSyncDriver:
        def select(self, query: Any, **kwargs: Any) -> list[dict[str, Any]]:
            queries.append(str(query))
            return [{"table_name": "T1"}]

    driver = cast("OracleSyncDriver", _StubSyncDriver())
    tables = data_dictionary.get_tables(driver, schema="HR")
    assert len(tables) == 1
    assert len(queries) == 1
    assert "names_by_schema" not in queries[0]


class _CatalogCursor:
    def __init__(self, connection: "_CatalogConnection") -> None:
        self.connection = connection
        self.description: list[tuple[str, object]] = []
        self.rows: list[tuple[object, ...]] = []
        self.rowcount = 0

    def execute(self, statement: str, parameters: object = None, **kwargs: object) -> None:
        import oracledb

        self.connection.statements.append(statement)
        if "product_component_version" in statement:
            columns = ["PRODUCT", "VERSION", "STATUS"]
            self.rows = [("Oracle Database", self.connection.catalog_version, "Production")]
            if int(self.connection.catalog_version.split(".", 1)[0]) >= 18:
                columns.insert(2, "VERSION_FULL")
                self.rows = [
                    ("Oracle Database", self.connection.catalog_version, self.connection.full_version, "Production")
                ]
        elif "v$parameter" in statement:
            columns = ["COMPATIBLE"]
            self.rows = [("23.4.0.0.0",)]
        elif "sys_context" in statement:
            columns = ["SERVICE"]
            self.rows = [(None,)]
        elif "all_ind_expressions" in statement:
            columns = ["SCHEMA_NAME", "TABLE_NAME", "INDEX_NAME", "COLUMN_EXPRESSION"]
            self.rows = [("HR", "EMPLOYEES", "IDX_EMAIL", 'UPPER("EMAIL")')]
        elif "all_indexes" in statement:
            columns = ["INDEX_NAME", "TABLE_NAME", "IS_UNIQUE", "IS_PRIMARY", "COLUMNS"]
            self.rows = [("IDX_EMAIL", "EMPLOYEES", 0, 0, "SYS_NC00003$")]
            if "i.owner AS schema_name" in statement:
                columns.insert(0, "SCHEMA_NAME")
                self.rows = [("HR", *row) for row in self.rows]
        else:
            raise ValueError(statement)
        self.description = [(name, oracledb.DB_TYPE_VARCHAR) for name in columns]
        self.rowcount = len(self.rows)

    def fetchall(self) -> list[tuple[object, ...]]:
        rows, self.rows = self.rows, []
        return rows

    def fetchone(self) -> tuple[object, ...] | None:
        return self.rows.pop(0) if self.rows else None

    def close(self) -> None:
        return None


class _AsyncCatalogCursor:
    def __init__(self, connection: "_CatalogConnection") -> None:
        self.cursor = _CatalogCursor(connection)

    @property
    def description(self) -> list[tuple[str, object]]:
        return self.cursor.description

    @property
    def rowcount(self) -> int:
        return self.cursor.rowcount

    async def execute(self, statement: str, parameters: object = None, **kwargs: object) -> None:
        self.cursor.execute(statement, parameters, **kwargs)

    async def fetchall(self) -> list[tuple[object, ...]]:
        return self.cursor.fetchall()

    async def fetchone(self) -> tuple[object, ...] | None:
        return self.cursor.fetchone()

    def close(self) -> None:
        self.cursor.close()


class _CatalogConnection:
    def __init__(
        self,
        *,
        is_async: bool,
        version: str = "23.0.0.0.0",
        full_version: str = "23.26.0.0.0",
        sdk_version_present: bool = True,
    ) -> None:
        self.is_async = is_async
        self.version = version if sdk_version_present else None
        self.catalog_version = version
        self.full_version = full_version
        self.statements: list[str] = []

    def cursor(self) -> _CatalogCursor | _AsyncCatalogCursor:
        return _AsyncCatalogCursor(self) if self.is_async else _CatalogCursor(self)


@pytest.mark.anyio
@pytest.mark.parametrize("is_async", [False, True])
async def test_oracle_default_schema_index_expression_uses_native_catalog_owner(is_async: bool) -> None:
    connection = _CatalogConnection(is_async=is_async)
    if is_async:
        driver = OracleAsyncDriver(
            cast("OracleAsyncConnection", connection), driver_features={"enable_lowercase_column_names": True}
        )
        indexes = await driver.data_dictionary.get_indexes(driver, table="employees")
    else:
        sync_driver = OracleSyncDriver(
            cast("OracleSyncConnection", connection), driver_features={"enable_lowercase_column_names": True}
        )
        indexes = sync_driver.data_dictionary.get_indexes(sync_driver, table="employees")
    assert len(indexes) == 1
    assert indexes[0]["index_name"] == "IDX_EMAIL"
    assert indexes[0]["expression"] == 'UPPER("EMAIL")'


@pytest.mark.anyio
@pytest.mark.parametrize("is_async", [False, True])
@pytest.mark.parametrize(
    ("major", "sdk_version_present", "lowercase_columns"),
    [(12, True, True), (18, True, False), (23, True, True), (23, False, False)],
)
async def test_oracle_version_preserves_release_update_and_legacy_query(
    is_async: bool, major: int, sdk_version_present: bool, lowercase_columns: bool
) -> None:
    base_version = f"{major}.0.0.0.0"
    full_version = f"{major}.26.0.0.0"
    connection = _CatalogConnection(
        is_async=is_async, version=base_version, full_version=full_version, sdk_version_present=sdk_version_present
    )
    if is_async:
        driver = OracleAsyncDriver(
            cast("OracleAsyncConnection", connection),
            driver_features={"enable_lowercase_column_names": lowercase_columns},
        )
        version = await driver.data_dictionary.get_version(driver)
        cached_version = await driver.data_dictionary.get_version(driver)
    else:
        sync_driver = OracleSyncDriver(
            cast("OracleSyncConnection", connection),
            driver_features={"enable_lowercase_column_names": lowercase_columns},
        )
        version = sync_driver.data_dictionary.get_version(sync_driver)
        cached_version = sync_driver.data_dictionary.get_version(sync_driver)
    assert version == VersionInfo(major, 26 if major >= 18 else 0, 0)
    assert cached_version is version
    version_statements = [query for query in connection.statements if "product_component_version" in query]
    assert len(version_statements) == 1
