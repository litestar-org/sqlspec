"""Unit tests for the aiomysql driver transaction-state contract."""

from typing import Any, Self, cast

import pytest

from sqlspec.adapters.aiomysql.driver import AiomysqlDriver

pytest.importorskip("aiomysql", reason="aiomysql adapter requires the aiomysql package")


class _FakeConnection:
    def __init__(self, in_transaction: bool) -> None:
        self._in_transaction = in_transaction

    def get_transaction_status(self) -> bool:
        return self._in_transaction


@pytest.mark.parametrize("in_transaction", [True, False])
def test_connection_in_transaction_reflects_driver_state(in_transaction: bool) -> None:
    """_connection_in_transaction() must reflect the connection's real transaction status."""
    driver = AiomysqlDriver(connection=cast("Any", _FakeConnection(in_transaction)))
    assert driver._connection_in_transaction() is in_transaction


class _TransactionConnection:
    """Records native transaction calls and any statement executed through a cursor."""

    def __init__(self) -> None:
        self.begins = 0
        self.executed: list[str] = []

    async def begin(self) -> None:
        self.begins += 1

    def cursor(self, *_args: Any, **_kwargs: Any) -> "_TransactionCursor":
        return _TransactionCursor(self)

    def get_transaction_status(self) -> bool:
        return self.begins > 0


class _TransactionCursor:
    def __init__(self, connection: _TransactionConnection) -> None:
        self._connection = connection

    async def __aenter__(self) -> "Self":
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def execute(self, sql: str, parameters: Any = None) -> None:
        _ = parameters
        self._connection.executed.append(sql)

    async def close(self) -> None:
        return None


async def test_begin_uses_the_native_call_instead_of_a_cursor_statement() -> None:
    """The driver's own begin avoids allocating a cursor for a single command."""
    connection = _TransactionConnection()
    driver = AiomysqlDriver(connection=cast("Any", connection))

    await driver.begin()

    assert connection.begins == 1
    assert connection.executed == []


async def test_aiomysql_create_pool_resolves_database_and_db_conflict(monkeypatch: pytest.MonkeyPatch) -> None:
    """AiomysqlConfig._create_pool normalizes database to db without raising duplicate keyword conflicts."""
    import aiomysql

    from sqlspec.adapters.aiomysql.config import AiomysqlConfig

    captured_kwargs: dict[str, Any] = {}

    async def _fake_create_pool(**kwargs: Any) -> Any:
        captured_kwargs.update(kwargs)
        return object()

    monkeypatch.setattr(aiomysql, "create_pool", _fake_create_pool)
    config = AiomysqlConfig(connection_config={"host": "localhost", "database": "canonical_db", "db": "legacy_db"})
    await config._create_pool()
    assert captured_kwargs["db"] == "legacy_db"
    assert "database" not in captured_kwargs


def test_aiomysql_signature_namespace_includes_cursors() -> None:
    """AiomysqlConfig.get_signature_namespace includes AiomysqlDictCursor and AiomysqlRawCursor."""
    from sqlspec.adapters.aiomysql.config import AiomysqlConfig

    config = AiomysqlConfig(connection_config={"host": "localhost", "db": "test"})
    ns = config.get_signature_namespace()
    assert "AiomysqlDictCursor" in ns
    assert "AiomysqlRawCursor" in ns


async def test_aiomysql_execute_cache_hit_invokes_collect_rows_on_clean_query(monkeypatch: pytest.MonkeyPatch) -> None:
    """Cached query without unescaped literal percent delegates to fast path and invokes collect_rows."""
    from unittest.mock import AsyncMock, MagicMock

    from sqlspec.core import OperationProfile, ParameterProfile, ProcessedState
    from sqlspec.core.cache import CachedQuery

    cursor = MagicMock()
    cursor.description = [("id", 3, None, None, None, None, None)]
    cursor.fetchall = AsyncMock(return_value=[(1,)])
    cursor.execute = AsyncMock(return_value=None)

    conn = MagicMock()
    conn.cursor = AsyncMock(return_value=cursor)

    driver = AiomysqlDriver(connection=cast("Any", conn))
    collect_called: list[bool] = []
    original_collect_rows = AiomysqlDriver.collect_rows

    def _spy_collect_rows(*args: Any, **kwargs: Any) -> Any:
        collect_called.append(True)
        return original_collect_rows(*args, **kwargs)

    monkeypatch.setattr(AiomysqlDriver, "collect_rows", _spy_collect_rows)

    cached = CachedQuery(
        compiled_sql="SELECT 1",
        parameter_profile=ParameterProfile(),
        input_named_parameters=(),
        applied_wrap_types=False,
        parameter_casts={},
        operation_type="SELECT",
        operation_profile=OperationProfile(returns_rows=True, modifies_rows=False),
        param_count=0,
        processed_state=ProcessedState(compiled_sql="SELECT 1", execution_parameters=[], operation_type="SELECT"),
    )

    result = await driver._execute_cache_hit("SELECT 1", (), cached)
    assert bool(collect_called) is True
    assert result.data == [(1,)]
