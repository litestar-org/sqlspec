"""Unit tests for the aiomysql driver transaction-state contract."""

from typing import Any, cast

import pytest
from typing_extensions import Self

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
