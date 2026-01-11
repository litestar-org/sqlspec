"""Unit tests for Oracle data dictionary version handling."""

from typing import TYPE_CHECKING, Any, cast

import pytest

from sqlspec.adapters.oracledb.data_dictionary import OracledbAsyncDataDictionary, OracledbSyncDataDictionary

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
