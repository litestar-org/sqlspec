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


async def test_aiomysql_dispatch_execute_uses_statement_statement_config() -> None:
    """dispatch_execute compiles using statement.statement_config."""
    from unittest.mock import AsyncMock, MagicMock

    from sqlspec import SQL
    from sqlspec.core import StatementConfig

    custom_config = StatementConfig(enable_caching=False)
    statement = SQL("SELECT 1", statement_config=custom_config)
    cursor = MagicMock()
    cursor.description = [("1", 3, None, None, None, None, None)]
    cursor.execute = AsyncMock()
    cursor.fetchall = AsyncMock(return_value=[(1,)])
    cursor.rowcount = 1
    cursor.lastrowid = None

    driver = AiomysqlDriver(connection=MagicMock())
    result = await driver.dispatch_execute(cursor, statement)
    assert cursor.execute.called
    assert result.data_row_count == 1


async def test_aiomysql_dispatch_select_stream_uses_statement_config() -> None:
    """dispatch_select_stream compiles using statement.statement_config."""
    from unittest.mock import MagicMock

    from sqlspec import SQL
    from sqlspec.core import StatementConfig

    custom_config = StatementConfig(enable_caching=False)
    statement = SQL("SELECT 1", statement_config=custom_config)
    driver = AiomysqlDriver(connection=MagicMock())
    stream = driver.dispatch_select_stream(statement, chunk_size=10)
    assert stream is not None


async def test_aiomysql_dispatch_execute_script_delegates_to_super() -> None:
    """dispatch_execute_script emits script statement events via super()."""
    from unittest.mock import AsyncMock, MagicMock

    from sqlspec import SQL

    cursor = MagicMock()
    cursor.execute = AsyncMock()
    cursor.description = None
    cursor.rowcount = 0
    cursor.lastrowid = None

    driver = AiomysqlDriver(connection=MagicMock())
    statement = SQL("SELECT 1; SELECT 2").as_script()
    result = await driver.dispatch_execute_script(cursor, statement)
    assert result.statement_count == 2
    assert result.successful_statements == 2


async def test_aiomysql_transaction_methods_catch_pymysql_error() -> None:
    """begin, commit, and rollback catch AiomysqlPymysqlError and raise mapped exception."""
    from unittest.mock import AsyncMock, MagicMock

    from sqlspec.adapters.aiomysql._typing import AiomysqlPymysqlError
    from sqlspec.exceptions import SQLSpecError

    conn = MagicMock()
    conn.begin = AsyncMock(side_effect=AiomysqlPymysqlError(1045, "Access denied"))
    conn.commit = AsyncMock(side_effect=AiomysqlPymysqlError(1045, "Access denied"))
    conn.rollback = AsyncMock(side_effect=AiomysqlPymysqlError(1045, "Access denied"))

    driver = AiomysqlDriver(connection=conn)
    with pytest.raises(SQLSpecError):
        await driver.begin()
    with pytest.raises(SQLSpecError):
        await driver.commit()
    with pytest.raises(SQLSpecError):
        await driver.rollback()
