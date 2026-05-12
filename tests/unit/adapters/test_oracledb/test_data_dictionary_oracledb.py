"""Unit tests for Oracle data dictionary version handling."""

from typing import TYPE_CHECKING, Any, cast

import pytest

from sqlspec.adapters.oracledb.data_dictionary import OracledbAsyncDataDictionary, OracledbSyncDataDictionary
from sqlspec.data_dictionary import get_dialect_config
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
from sqlspec.typing import TableMetadata, VersionInfo

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
