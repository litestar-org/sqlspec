"""psqlpy transaction state follows begin/commit/rollback."""

from types import SimpleNamespace
from typing import Any, cast

import pytest

from sqlspec.adapters.psqlpy._typing import PsqlpyDatabaseError
from sqlspec.adapters.psqlpy.config import PsqlpyConfig
from sqlspec.adapters.psqlpy.core import PsqlpyStreamSource, build_postgres_storage_params_sql
from sqlspec.adapters.psqlpy.driver import PsqlpyDriver
from sqlspec.exceptions import SQLSpecError

pytestmark = pytest.mark.anyio


class _FakeConnection:
    def __init__(self) -> None:
        self.statements: list[str] = []

    async def execute(self, sql: str, *args: Any, **kwargs: Any) -> None:
        self.statements.append(sql)

    def in_transaction(self) -> Any:
        msg = "in_transaction() is not a reliable transaction predicate"
        raise AssertionError(msg)


async def test_connection_in_transaction_tracks_begin_commit_rollback() -> None:
    connection = _FakeConnection()
    driver = PsqlpyDriver(cast("Any", connection))
    assert driver._connection_in_transaction() is False

    await driver.begin()
    assert driver._connection_in_transaction() is True
    await driver.commit()
    assert driver._connection_in_transaction() is False

    await driver.begin()
    assert driver._connection_in_transaction() is True
    await driver.rollback()
    assert driver._connection_in_transaction() is False
    assert connection.statements == ["BEGIN", "COMMIT", "BEGIN", "ROLLBACK"]


@pytest.mark.parametrize("operation", ["commit", "rollback"])
async def test_failed_transaction_end_clears_the_flag(operation: str) -> None:
    connection = _FakeConnection()
    driver = PsqlpyDriver(cast("Any", connection))
    await driver.begin()

    async def fail(sql: str, *args: Any, **kwargs: Any) -> None:
        raise PsqlpyDatabaseError(sql)

    connection.execute = fail  # type: ignore[method-assign]
    with pytest.raises(SQLSpecError):
        await getattr(driver, operation)()
    assert driver._connection_in_transaction() is False


class _FakeCursor:
    def __init__(self, owner: str) -> None:
        self.owner = owner
        self.closed = False

    async def start(self) -> None:
        return None

    def close(self) -> None:
        self.closed = True


class _FakeTransaction:
    def __init__(self, record: "list[str]") -> None:
        self._record = record

    async def begin(self) -> None:
        self._record.append("begin")

    async def commit(self) -> None:
        self._record.append("commit")

    async def rollback(self) -> None:
        self._record.append("rollback")

    def cursor(self, *_args: Any, **_kwargs: Any) -> _FakeCursor:
        return _FakeCursor("transaction")


class _StreamingConnection(_FakeConnection):
    def __init__(self) -> None:
        super().__init__()
        self.transaction_calls: list[str] = []

    def transaction(self) -> _FakeTransaction:
        return _FakeTransaction(self.transaction_calls)

    def cursor(self, *_args: Any, **_kwargs: Any) -> _FakeCursor:
        return _FakeCursor("connection")


async def test_stream_inside_a_user_transaction_does_not_commit_it() -> None:
    """A stream must never end a transaction it did not open."""
    connection = _StreamingConnection()
    driver = PsqlpyDriver(cast("Any", connection))
    await driver.begin()

    source = PsqlpyStreamSource(driver, "SELECT 1", None, 10)
    await source.start()
    await source.close()

    assert connection.transaction_calls == []
    assert driver._connection_in_transaction() is True
    assert connection.statements == ["BEGIN"]


async def test_stream_outside_a_transaction_manages_its_own() -> None:
    """With no caller transaction the stream still opens and commits its own."""
    connection = _StreamingConnection()
    driver = PsqlpyDriver(cast("Any", connection))

    source = PsqlpyStreamSource(driver, "SELECT 1", None, 10)
    await source.start()
    await source.close()

    assert connection.transaction_calls == ["begin", "commit"]


async def test_a_stream_transaction_is_not_offered_to_the_driver_as_its_own() -> None:
    """Claiming it would let the driver's own transaction block commit the stream away."""
    connection = _StreamingConnection()
    driver = PsqlpyDriver(cast("Any", connection))

    source = PsqlpyStreamSource(driver, "SELECT 1", None, 10)
    await source.start()
    try:
        assert driver._connection_in_transaction() is False
    finally:
        await source.close()


async def test_stream_error_close_rolls_back_only_its_own_transaction() -> None:
    """An errored stream rolls back the transaction it owns, not the caller's."""
    connection = _StreamingConnection()
    driver = PsqlpyDriver(cast("Any", connection))

    source = PsqlpyStreamSource(driver, "SELECT 1", None, 10)
    await source.start()
    await source.close(error=True)

    assert connection.transaction_calls == ["begin", "rollback"]
    assert driver._connection_in_transaction() is False


class _CopyConnection(_FakeConnection):
    """Records the native COPY call and fails loudly on a row-by-row fallback."""

    def __init__(self, json_columns: "tuple[str, ...]" = ()) -> None:
        super().__init__()
        self.copy_calls: list[tuple[str, list[Any], dict[str, Any]]] = []
        self._json_columns = json_columns

    async def fetch(self, _sql: str, _parameters: Any = None) -> Any:
        return SimpleNamespace(result=lambda: [{"column_name": name} for name in self._json_columns])

    async def copy_records_to_table(self, table_name: str, records: Any, **kwargs: Any) -> int:
        self.copy_calls.append((table_name, list(records), kwargs))
        return len(self.copy_calls)

    def execute_many(self, *_args: Any, **_kwargs: Any) -> None:
        msg = "load_from_arrow must not fall back to row-by-row inserts"
        raise AssertionError(msg)


async def test_load_from_arrow_uses_the_native_copy_path() -> None:
    """Arrow ingest must go through copy_records_to_table, never execute_many."""
    pa = pytest.importorskip("pyarrow")
    connection = _CopyConnection()
    config = PsqlpyConfig()
    driver = PsqlpyDriver(
        cast("Any", connection), driver_features={"storage_capabilities": config.storage_capabilities()}
    )
    table = pa.table({"id": [1, 2], "name": ["a", "b"]})

    await driver.load_from_arrow("analytics.events", table)

    assert len(connection.copy_calls) == 1
    table_name, records, kwargs = connection.copy_calls[0]
    assert table_name == "events"
    assert records == [(1, "a"), (2, "b")]
    assert kwargs == {"columns": ["id", "name"], "schema_name": "analytics"}


async def test_load_from_arrow_decodes_json_text_for_json_columns() -> None:
    """psqlpy binds by destination column type, so a JSON column needs an object."""
    pa = pytest.importorskip("pyarrow")
    connection = _CopyConnection(json_columns=("payload",))
    config = PsqlpyConfig()
    driver = PsqlpyDriver(
        cast("Any", connection), driver_features={"storage_capabilities": config.storage_capabilities()}
    )
    table = pa.table({"id": [1], "payload": ['{"name": "alpha"}'], "note": ['{"not": "json"}']})

    await driver.load_from_arrow("events", table)

    _table_name, records, _kwargs = connection.copy_calls[0]
    assert records == [(1, {"name": "alpha"}, '{"not": "json"}')]


def test_psqlpy_build_postgres_storage_params_sql() -> None:
    """build_postgres_storage_params_sql consolidates storage and autovacuum clauses."""
    settings = {"fillfactor": 85, "autovacuum_vacuum_scale_factor": 0.05, "autovacuum_analyze_scale_factor": 0.02}
    clause = build_postgres_storage_params_sql(settings, include_autovacuum=True)
    assert "WITH (" in clause
    assert "fillfactor = 85" in clause
    assert "autovacuum_vacuum_scale_factor = 0.05" in clause
    assert "autovacuum_analyze_scale_factor = 0.02" in clause


def test_psqlpy_events_validate_listener_pool_capacity_skips_when_direct_dsn() -> None:
    """_validate_listener_pool_capacity should not enforce pool size when direct_dsn is configured."""
    from sqlspec.adapters.psqlpy.events.backend import _validate_listener_pool_capacity

    config = PsqlpyConfig(
        connection_config={"dsn": "postgresql://localhost/test", "max_db_pool_size": 1},
        driver_features={"direct_dsn": "postgresql://localhost/direct"},
    )
    _validate_listener_pool_capacity(config)


def test_psqlpy_events_parse_notify_payload_handles_malformed_json() -> None:
    """Malformed NOTIFY payload does not raise and returns None safely."""
    from sqlspec.adapters.psqlpy.events.backend import (
        _extract_batch_size,
        _extract_event_id,
        _extract_marker_id,
        _parse_notify_payload,
    )

    bad_payload = "not valid json {{"
    assert _parse_notify_payload(bad_payload) is None
    assert _extract_event_id(bad_payload) is None
    assert _extract_batch_size(bad_payload) == 0
    assert _extract_marker_id(bad_payload) is None

    good_payload = '{"event_id": "e1", "batch_size": 5, "marker_id": "m1"}'
    parsed = _parse_notify_payload(good_payload)
    assert parsed is not None
    assert _extract_event_id(parsed) == "e1"
    assert _extract_batch_size(parsed) == 5
    assert _extract_marker_id(parsed) == "m1"


def test_psqlpy_events_resolve_listener_config_allows_mapping_direct_dsn() -> None:
    """_resolve_listener_config accepts Mapping for direct_dsn without raising."""
    from sqlspec.adapters.psqlpy.events._hub import PsqlpyListenerHub

    config = PsqlpyConfig(
        connection_config={"dsn": "postgresql://localhost/test"},
        driver_features={"pooler_mode": "transaction", "direct_dsn": {"host": "localhost", "database": "direct_db"}},
    )
    hub = PsqlpyListenerHub(config)
    resolved = hub._resolve_listener_config()
    assert resolved is not None


def test_psqlpy_events_backend_logger_namespace() -> None:
    """Psqlpy events backend logger namespace follows standard naming."""
    from sqlspec.adapters.psqlpy.events.backend import logger

    assert logger.name == "sqlspec.adapters.psqlpy.events.backend"


async def test_psqlpy_litestar_store_operations_handle_none_query_result() -> None:
    """exists, delete, delete_expired handle None query result safely."""
    from unittest.mock import AsyncMock

    from sqlspec.adapters.psqlpy.litestar.store import PsqlpyStore

    mock_conn = AsyncMock()
    mock_conn.fetch = AsyncMock(return_value=None)
    mock_conn.execute = AsyncMock(return_value=None)

    config = PsqlpyConfig(connection_config={"dsn": "postgresql://localhost/test"})
    store = PsqlpyStore(config)
    object.__setattr__(
        config,
        "provide_connection",
        lambda: AsyncMock(__aenter__=AsyncMock(return_value=mock_conn), __aexit__=AsyncMock()),
    )

    assert await store.exists("sess_1") is False
    assert await store.expires_in("sess_1") is None
    assert await store.delete_expired() == 0


async def test_psqlpy_get_indexes_with_none_schema_resolves_via_search_path() -> None:
    """get_indexes passes schema_name=None when schema is None to resolve table on search_path."""
    from unittest.mock import AsyncMock, MagicMock

    from sqlspec.adapters.psqlpy.data_dictionary import PsqlpyDataDictionary

    calls: list[dict[str, Any]] = []
    driver = MagicMock()

    async def fake_select(query: Any, **kwargs: Any) -> list[Any]:
        calls.append(kwargs)
        return []

    driver.select = AsyncMock(side_effect=fake_select)

    dd = PsqlpyDataDictionary()
    await dd.get_indexes(driver, table="users", schema=None)
    assert len(calls) == 1
    assert calls[0]["schema_name"] is None
    assert calls[0]["table_name"] == "users"
