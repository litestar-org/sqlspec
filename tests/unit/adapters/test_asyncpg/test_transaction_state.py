"""asyncpg transaction control uses the native transaction handle."""

import inspect
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import asyncpg
import pytest

from sqlspec.adapters.asyncpg.config import AsyncpgConfig
from sqlspec.adapters.asyncpg.driver import AsyncpgDriver

pytestmark = pytest.mark.anyio


class _FakeTransaction:
    def __init__(self, record: "list[str]", nested: bool = False) -> None:
        self._record = record
        self.nested = nested

    async def start(self) -> None:
        self._record.append("savepoint" if self.nested else "start")

    async def commit(self) -> None:
        self._record.append("commit")

    async def rollback(self) -> None:
        self._record.append("rollback")


class _FakeConnection:
    """Mirrors asyncpg's refusal to mix manual SQL with transaction handles."""

    def __init__(self) -> None:
        self.statements: list[str] = []
        self.transaction_calls: list[str] = []
        self._depth = 0

    async def execute(self, sql: str, *_args: Any, **_kwargs: Any) -> str:
        self.statements.append(sql)
        return "SELECT 0"

    def transaction(self) -> _FakeTransaction:
        nested = self._depth > 0
        self._depth += 1
        return _FakeTransaction(self.transaction_calls, nested=nested)

    def is_in_transaction(self) -> bool:
        return self._depth > 0


async def test_begin_uses_the_native_transaction_handle() -> None:
    connection = _FakeConnection()
    driver = AsyncpgDriver(cast("Any", connection))

    await driver.begin()

    assert connection.transaction_calls == ["start"]
    assert connection.statements == []


async def test_a_second_begin_does_not_open_a_savepoint() -> None:
    connection = _FakeConnection()
    driver = AsyncpgDriver(cast("Any", connection))

    await driver.begin()
    await driver.begin()

    assert connection.transaction_calls == ["start"]


async def test_commit_after_a_repeated_begin_commits_the_outer_transaction() -> None:
    connection = _FakeConnection()
    driver = AsyncpgDriver(cast("Any", connection))

    await driver.begin()
    await driver.begin()
    await driver.commit()

    assert connection.transaction_calls == ["start", "commit"]
    assert connection.statements == []


async def test_rollback_releases_the_handle_so_a_later_begin_starts_fresh() -> None:
    connection = _FakeConnection()
    driver = AsyncpgDriver(cast("Any", connection))

    await driver.begin()
    await driver.rollback()
    connection._depth = 0
    await driver.begin()

    assert connection.transaction_calls == ["start", "rollback", "start"]


async def test_a_stream_can_open_a_transaction_after_driver_begin() -> None:
    """asyncpg refuses transaction() inside a manually started transaction."""
    connection = _FakeConnection()
    driver = AsyncpgDriver(cast("Any", connection))

    await driver.begin()

    assert connection.statements == []
    assert connection.is_in_transaction() is True


async def test_create_connection_consumes_no_pool_slot(monkeypatch: pytest.MonkeyPatch) -> None:
    sentinel = MagicMock()
    connect = AsyncMock(return_value=sentinel)
    monkeypatch.setattr("sqlspec.adapters.asyncpg.config.asyncpg_connect", connect)
    monkeypatch.setattr(AsyncpgConfig, "_init_connection", AsyncMock())
    config = AsyncpgConfig(connection_config={"host": "localhost", "min_size": 1, "max_size": 1})

    connection = await config.create_connection()

    assert connection is sentinel
    assert config.connection_instance is None
    assert "min_size" not in connect.call_args.kwargs
    assert "max_size" not in connect.call_args.kwargs


async def test_create_connection_drops_every_pool_only_setting(monkeypatch: pytest.MonkeyPatch) -> None:
    """asyncpg.connect() takes no **kwargs, so a pool-only setting reaching it is a TypeError."""
    connect = AsyncMock(return_value=MagicMock())
    monkeypatch.setattr("sqlspec.adapters.asyncpg.config.asyncpg_connect", connect)
    monkeypatch.setattr(AsyncpgConfig, "_init_connection", AsyncMock())
    config = AsyncpgConfig(
        connection_config={
            "host": "localhost",
            "min_size": 1,
            "max_size": 2,
            "reset": AsyncMock(),
            "setup": AsyncMock(),
        }
    )

    await config.create_connection()

    accepted = set(inspect.signature(asyncpg.connect).parameters)
    assert set(connect.call_args.kwargs) <= accepted
