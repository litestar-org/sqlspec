"""Driver transaction() context manager contracts."""

import sqlite3
from collections.abc import AsyncIterator, Iterator
from contextlib import closing
from pathlib import Path

import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig, AiosqliteDriver
from sqlspec.adapters.sqlite import SqliteConfig, SqliteDriver
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.service import SQLSpecAsyncService, SQLSpecSyncService


class _BoomError(Exception):
    pass


def _committed_ids(config: "SqliteConfig | AiosqliteConfig") -> list[int]:
    with closing(sqlite3.connect(config.connection_config["database"])) as outside:
        return [row[0] for row in outside.execute("SELECT id FROM items ORDER BY id")]


@pytest.fixture
async def async_config(tmp_path: Path) -> AsyncIterator[AiosqliteConfig]:
    config = AiosqliteConfig(connection_config={"database": str(tmp_path / "transaction.sqlite")})
    async with config.provide_session() as session:
        await session.execute_script("CREATE TABLE items (id INTEGER PRIMARY KEY)")
        await session.commit()
    yield config
    await config.close_pool()


@pytest.fixture
def sync_config(tmp_path: Path) -> Iterator[SqliteConfig]:
    config = SqliteConfig(connection_config={"database": str(tmp_path / "transaction.sqlite")})
    with config.provide_session() as session:
        session.execute_script("CREATE TABLE items (id INTEGER PRIMARY KEY)")
        session.commit()
    yield config
    config.close_pool()


async def test_async_commit_on_exit(async_config: AiosqliteConfig) -> None:
    async with async_config.provide_session() as session:
        async with session.transaction() as tx:
            assert tx is session
            assert session.connection.in_transaction
            await tx.execute("INSERT INTO items (id) VALUES (1)")
        assert not session.connection.in_transaction

    async with async_config.provide_session() as session:
        assert await session.select_value("SELECT COUNT(*) FROM items") == 1


async def test_async_rollback_on_error(async_config: AiosqliteConfig) -> None:
    async with async_config.provide_session() as session:
        with pytest.raises(_BoomError):
            async with session.transaction():
                await session.execute("INSERT INTO items (id) VALUES (1)")
                raise _BoomError
        assert not session.connection.in_transaction
        assert await session.select_value("SELECT COUNT(*) FROM items") == 0

    async with async_config.provide_session() as session:
        assert await session.select_value("SELECT COUNT(*) FROM items") == 0


def test_sync_commit_on_exit(sync_config: SqliteConfig) -> None:
    with sync_config.provide_session() as session:
        with session.transaction() as tx:
            assert tx is session
            assert session.connection.in_transaction
            tx.execute("INSERT INTO items (id) VALUES (1)")
        assert not session.connection.in_transaction

    with sync_config.provide_session() as session:
        assert session.select_value("SELECT COUNT(*) FROM items") == 1


def test_sync_rollback_on_error(sync_config: SqliteConfig) -> None:
    with sync_config.provide_session() as session:
        with pytest.raises(_BoomError):
            with session.transaction():
                session.execute("INSERT INTO items (id) VALUES (1)")
                raise _BoomError
        assert not session.connection.in_transaction
        assert session.select_value("SELECT COUNT(*) FROM items") == 0

    with sync_config.provide_session() as session:
        assert session.select_value("SELECT COUNT(*) FROM items") == 0


async def test_async_nested_transaction_uses_savepoint(async_config: AiosqliteConfig) -> None:
    async with async_config.provide_session() as session:
        with pytest.raises(_BoomError):
            async with session.transaction():
                await session.execute("INSERT INTO items (id) VALUES (1)")
                async with session.transaction() as inner:
                    assert inner is session
                    await session.execute("INSERT INTO items (id) VALUES (2)")
                assert session.connection.in_transaction
                assert _committed_ids(async_config) == []
                raise _BoomError
        assert _committed_ids(async_config) == []

        async with session.transaction():
            await session.execute("INSERT INTO items (id) VALUES (3)")
            with pytest.raises(_BoomError):
                async with session.transaction():
                    await session.execute("INSERT INTO items (id) VALUES (4)")
                    raise _BoomError
            assert await session.select_value("SELECT COUNT(*) FROM items WHERE id = 4") == 0
            await session.execute("INSERT INTO items (id) VALUES (5)")
    assert _committed_ids(async_config) == [3, 5]


def test_sync_nested_transaction_uses_savepoint(sync_config: SqliteConfig) -> None:
    with sync_config.provide_session() as session:
        with pytest.raises(_BoomError):
            with session.transaction():
                session.execute("INSERT INTO items (id) VALUES (1)")
                with session.transaction() as inner:
                    assert inner is session
                    session.execute("INSERT INTO items (id) VALUES (2)")
                assert session.connection.in_transaction
                assert _committed_ids(sync_config) == []
                raise _BoomError
        assert _committed_ids(sync_config) == []

        with session.transaction():
            session.execute("INSERT INTO items (id) VALUES (3)")
            with pytest.raises(_BoomError):
                with session.transaction():
                    session.execute("INSERT INTO items (id) VALUES (4)")
                    raise _BoomError
            assert session.select_value("SELECT COUNT(*) FROM items WHERE id = 4") == 0
            session.execute("INSERT INTO items (id) VALUES (5)")
    assert _committed_ids(sync_config) == [3, 5]


@pytest.mark.parametrize("kind", ["config", "session"])
async def test_async_transaction_inside_service_transaction(async_config: AiosqliteConfig, kind: str) -> None:
    async with async_config.provide_session() as bound:
        service = SQLSpecAsyncService(config=async_config) if kind == "config" else SQLSpecAsyncService(bound)
        with pytest.raises(_BoomError):
            async with service.begin_transaction() as session:
                await session.execute("INSERT INTO items (id) VALUES (1)")
                async with session.transaction():
                    await session.execute("INSERT INTO items (id) VALUES (2)")
                assert _committed_ids(async_config) == []
                raise _BoomError
        async with service.begin_transaction() as session:
            await session.execute("INSERT INTO items (id) VALUES (3)")
            with pytest.raises(_BoomError):
                async with session.transaction():
                    await session.execute("INSERT INTO items (id) VALUES (4)")
                    raise _BoomError
    assert _committed_ids(async_config) == [3]


@pytest.mark.parametrize("kind", ["config", "session"])
def test_sync_transaction_inside_service_transaction(sync_config: SqliteConfig, kind: str) -> None:
    with sync_config.provide_session() as bound:
        service = SQLSpecSyncService(config=sync_config) if kind == "config" else SQLSpecSyncService(bound)
        with pytest.raises(_BoomError):
            with service.begin_transaction() as session:
                session.execute("INSERT INTO items (id) VALUES (1)")
                with session.transaction():
                    session.execute("INSERT INTO items (id) VALUES (2)")
                assert _committed_ids(sync_config) == []
                raise _BoomError
        with service.begin_transaction() as session:
            session.execute("INSERT INTO items (id) VALUES (3)")
            with pytest.raises(_BoomError):
                with session.transaction():
                    session.execute("INSERT INTO items (id) VALUES (4)")
                    raise _BoomError
    assert _committed_ids(sync_config) == [3]


@pytest.mark.parametrize("rollback_fails", [False, True])
async def test_async_commit_failure_rolls_back(
    async_config: AiosqliteConfig, monkeypatch: pytest.MonkeyPatch, rollback_fails: bool
) -> None:
    error = RuntimeError("commit")
    calls: list[str] = []

    async def commit(self: AiosqliteDriver) -> None:
        calls.append("commit")
        raise error

    original_rollback = AiosqliteDriver.rollback

    async def rollback(self: AiosqliteDriver) -> None:
        calls.append("rollback")
        await original_rollback(self)
        if rollback_fails:
            raise _BoomError

    monkeypatch.setattr(AiosqliteDriver, "commit", commit)
    monkeypatch.setattr(AiosqliteDriver, "rollback", rollback)
    async with async_config.provide_session() as session:
        with pytest.raises(RuntimeError) as raised:
            async with session.transaction():
                await session.execute("INSERT INTO items (id) VALUES (1)")
        assert raised.value is error
        assert calls == ["commit", "rollback"]
        assert not session.connection.in_transaction
    assert _committed_ids(async_config) == []


@pytest.mark.parametrize("rollback_fails", [False, True])
def test_sync_commit_failure_rolls_back(
    sync_config: SqliteConfig, monkeypatch: pytest.MonkeyPatch, rollback_fails: bool
) -> None:
    error = RuntimeError("commit")
    calls: list[str] = []

    def commit(self: SqliteDriver) -> None:
        calls.append("commit")
        raise error

    original_rollback = SqliteDriver.rollback

    def rollback(self: SqliteDriver) -> None:
        calls.append("rollback")
        original_rollback(self)
        if rollback_fails:
            raise _BoomError

    monkeypatch.setattr(SqliteDriver, "commit", commit)
    monkeypatch.setattr(SqliteDriver, "rollback", rollback)
    with sync_config.provide_session() as session:
        with pytest.raises(RuntimeError) as raised:
            with session.transaction():
                session.execute("INSERT INTO items (id) VALUES (1)")
        assert raised.value is error
        assert calls == ["commit", "rollback"]
        assert not session.connection.in_transaction
    assert _committed_ids(sync_config) == []


async def test_async_implicit_transaction_before_block_is_committed(async_config: AiosqliteConfig) -> None:
    async with async_config.provide_session() as session:
        await session.execute("INSERT INTO items (id) VALUES (1)")
        assert session.connection.in_transaction
        async with session.transaction():
            await session.execute("INSERT INTO items (id) VALUES (2)")
        assert not session.connection.in_transaction
        assert _committed_ids(async_config) == [1, 2]


def test_sync_implicit_transaction_before_block_is_committed(sync_config: SqliteConfig) -> None:
    with sync_config.provide_session() as session:
        session.execute("INSERT INTO items (id) VALUES (1)")
        assert session.connection.in_transaction
        with session.transaction():
            session.execute("INSERT INTO items (id) VALUES (2)")
        assert not session.connection.in_transaction
        assert _committed_ids(sync_config) == [1, 2]


async def test_async_nested_transaction_without_savepoints_is_refused(
    async_config: AiosqliteConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def unsupported(self: AiosqliteDriver, name: str) -> None:
        raise NotImplementedError

    monkeypatch.setattr(AiosqliteDriver, "create_savepoint", unsupported)
    async with async_config.provide_session() as session:
        async with session.transaction():
            await session.execute("INSERT INTO items (id) VALUES (1)")
            with pytest.raises(ImproperConfigurationError, match="savepoints") as raised:
                async with session.transaction():
                    pytest.fail("nested block entered")
            assert isinstance(raised.value.__cause__, NotImplementedError)
        async with session.transaction():
            await session.execute("INSERT INTO items (id) VALUES (2)")
    assert _committed_ids(async_config) == [1, 2]


def test_sync_nested_transaction_without_savepoints_is_refused(
    sync_config: SqliteConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    def unsupported(self: SqliteDriver, name: str) -> None:
        raise NotImplementedError

    monkeypatch.setattr(SqliteDriver, "create_savepoint", unsupported)
    with sync_config.provide_session() as session:
        with session.transaction():
            session.execute("INSERT INTO items (id) VALUES (1)")
            with pytest.raises(ImproperConfigurationError, match="savepoints") as raised:
                with session.transaction():
                    pytest.fail("nested block entered")
            assert isinstance(raised.value.__cause__, NotImplementedError)
        with session.transaction():
            session.execute("INSERT INTO items (id) VALUES (2)")
    assert _committed_ids(sync_config) == [1, 2]


async def test_async_service_transaction_inside_driver_transaction(async_config: AiosqliteConfig) -> None:
    async with async_config.provide_session() as session:
        service = SQLSpecAsyncService(session)
        with pytest.raises(_BoomError):
            async with session.transaction():
                await session.execute("INSERT INTO items (id) VALUES (1)")
                async with service.begin_transaction():
                    await session.execute("INSERT INTO items (id) VALUES (2)")
                assert _committed_ids(async_config) == []
                raise _BoomError
        async with session.transaction():
            await session.execute("INSERT INTO items (id) VALUES (3)")
            with pytest.raises(_BoomError):
                async with service.begin_transaction():
                    await session.execute("INSERT INTO items (id) VALUES (4)")
                    raise _BoomError
    assert _committed_ids(async_config) == [3]


def test_sync_service_transaction_inside_driver_transaction(sync_config: SqliteConfig) -> None:
    with sync_config.provide_session() as session:
        service = SQLSpecSyncService(session)
        with pytest.raises(_BoomError):
            with session.transaction():
                session.execute("INSERT INTO items (id) VALUES (1)")
                with service.begin_transaction():
                    session.execute("INSERT INTO items (id) VALUES (2)")
                assert _committed_ids(sync_config) == []
                raise _BoomError
        with session.transaction():
            session.execute("INSERT INTO items (id) VALUES (3)")
            with pytest.raises(_BoomError):
                with service.begin_transaction():
                    session.execute("INSERT INTO items (id) VALUES (4)")
                    raise _BoomError
    assert _committed_ids(sync_config) == [3]
