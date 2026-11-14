"""Driver transaction() context manager contracts."""

import sqlite3
from collections.abc import AsyncIterator, Iterator
from contextlib import closing
from pathlib import Path
from typing import Any

import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig, AiosqliteDriver
from sqlspec.adapters.aiosqlite._typing import AiosqliteSessionContext
from sqlspec.adapters.duckdb import DuckDBConfig
from sqlspec.adapters.sqlite import SqliteConfig, SqliteDriver
from sqlspec.adapters.sqlite._typing import SqliteSessionContext
from sqlspec.base import SQLSpec
from sqlspec.config import LifecycleConfig
from sqlspec.core import OperationType
from sqlspec.driver._common import _DETACHED_CONNECTION, operation_requires_transaction
from sqlspec.driver._sync import SyncPoolSessionFactory
from sqlspec.exceptions import ImproperConfigurationError, SQLSpecError
from sqlspec.observability import ObservabilityConfig, ObservabilityRuntime
from sqlspec.service import SQLSpecAsyncService, SQLSpecSyncService


class _BoomError(Exception):
    pass


async def _acquire_nothing() -> object:
    return object()


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


def test_sync_session_context_forwards_exception_to_release(sync_config: SqliteConfig) -> None:
    released: list[dict[str, object]] = []

    context = SqliteSessionContext(
        acquire_connection=lambda: object(),
        release_connection=lambda _conn, **kwargs: released.append(kwargs),
        statement_config=sync_config.statement_config,
        driver_features={},
        prepare_driver=lambda driver: driver,
    )

    with pytest.raises(_BoomError), context:
        raise _BoomError

    assert released == [{"exc_type": _BoomError, "exc_val": released[0]["exc_val"], "exc_tb": released[0]["exc_tb"]}]
    assert isinstance(released[0]["exc_val"], _BoomError)
    assert released[0]["exc_tb"] is not None


async def test_async_session_context_forwards_exception_to_release(async_config: AiosqliteConfig) -> None:
    released: list[dict[str, object]] = []

    async def _release(_conn: object, **kwargs: object) -> None:
        released.append(kwargs)

    context = AiosqliteSessionContext(
        acquire_connection=_acquire_nothing,
        release_connection=_release,
        statement_config=async_config.statement_config,
        driver_features={},
        prepare_driver=lambda driver: driver,
    )

    with pytest.raises(_BoomError):
        async with context:
            raise _BoomError

    assert isinstance(released[0]["exc_val"], _BoomError)
    assert released[0]["exc_type"] is _BoomError
    assert released[0]["exc_tb"] is not None


def test_sync_session_factory_forwards_exception_to_pool_context() -> None:
    recorded: list[tuple[object, object, object]] = []

    class _RecordingCtx:
        def __enter__(self) -> object:
            return object()

        def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
            recorded.append((exc_type, exc_val, exc_tb))

    class _RecordingPool:
        def get_connection(self) -> _RecordingCtx:
            return _RecordingCtx()

    class _RecordingConfig:
        def provide_pool(self) -> _RecordingPool:
            return _RecordingPool()

        def _create_tenant_lease(self) -> None:
            return None

    factory = SyncPoolSessionFactory(_RecordingConfig())
    connection = factory.acquire_connection()
    error = _BoomError()
    factory.release_connection(connection, exc_type=_BoomError, exc_val=error, exc_tb=None)

    assert recorded == [(_BoomError, error, None)]


def test_failed_sqlite_session_leaves_transaction_uncommitted(sync_config: SqliteConfig) -> None:
    with pytest.raises(_BoomError), sync_config.provide_session() as session:
        session.begin()
        session.execute("INSERT INTO items (id) VALUES (1)")
        raise _BoomError

    assert _committed_ids(sync_config) == []


def test_failed_duckdb_session_keeps_an_in_memory_connection() -> None:
    """An in-memory database lives in its connection, so a failed session must not discard it."""
    config = DuckDBConfig(connection_config={"database": ":memory:"})
    try:
        with config.provide_session() as session:
            session.execute_script("CREATE TABLE items (id INTEGER)")
        pool = config.provide_pool()
        assert pool.size() == 1

        with pytest.raises(_BoomError), config.provide_session():
            raise _BoomError

        assert pool.size() == 1
        with config.provide_session() as session:
            assert session.execute("SELECT id FROM items").get_data() == []
    finally:
        config.close_pool()


def test_failed_duckdb_session_discards_a_file_connection(tmp_path: Path) -> None:
    """A file-backed connection holds DuckDB's file lock, which a failed session must release."""
    config = DuckDBConfig(connection_config={"database": str(tmp_path / "items.duckdb")})
    try:
        with config.provide_session() as session:
            session.execute_script("CREATE TABLE items (id INTEGER)")
        pool = config.provide_pool()

        with pytest.raises(_BoomError), config.provide_session():
            raise _BoomError

        assert pool.size() == 0
        with config.provide_session() as session:
            assert session.execute("SELECT id FROM items").get_data() == []
    finally:
        config.close_pool()


def test_detached_connection_sentinel_semantics() -> None:
    assert bool(_DETACHED_CONNECTION) is False
    assert repr(_DETACHED_CONNECTION) == "<DetachedConnection>"
    with pytest.raises(AttributeError):
        _ = _DETACHED_CONNECTION.__deepcopy__
    with pytest.raises(ImproperConfigurationError, match="already closed"):
        _ = _DETACHED_CONNECTION.cursor
    with pytest.raises(ImproperConfigurationError, match="already closed"):
        hasattr(_DETACHED_CONNECTION, "cursor")


@pytest.mark.parametrize(
    ("op_type", "modifies_rows", "expected"),
    [
        ("SELECT", False, False),
        ("SELECT", True, True),
        ("PRAGMA", False, False),
        ("PRAGMA", True, True),
        ("COPY_TO", False, False),
        ("COPY_TO", True, True),
        ("INSERT", False, True),
        ("UPDATE", False, True),
        ("DELETE", False, True),
        ("MERGE", False, True),
        ("COPY", False, True),
        ("COPY_FROM", False, True),
        ("EXECUTE", False, True),
        ("SCRIPT", False, True),
        ("DDL", False, True),
        ("COMMAND", False, True),
    ],
)
def test_operation_requires_transaction_classification(
    op_type: OperationType, modifies_rows: bool, expected: bool
) -> None:
    assert operation_requires_transaction(op_type, modifies_rows) is expected


@pytest.mark.parametrize("tx_method", ["begin", "commit", "rollback"])
def test_sync_detached_driver_rejects_operations_and_keeps_cache_disabled(
    sync_config: SqliteConfig, tx_method: str
) -> None:
    with sync_config.provide_session() as session:
        session._detach_connection()  # pyright: ignore[reportPrivateUsage]
        assert session.connection is _DETACHED_CONNECTION
        session.attach_observability(ObservabilityRuntime())
        assert session._stmt_cache_enabled is False  # pyright: ignore[reportPrivateUsage]
        with pytest.raises(ImproperConfigurationError, match="already closed"):
            session.execute("SELECT 1")
        with pytest.raises(SQLSpecError):
            getattr(session, tx_method)()


@pytest.mark.parametrize("tx_method", ["begin", "commit", "rollback"])
async def test_async_detached_driver_rejects_operations_and_keeps_cache_disabled(
    async_config: AiosqliteConfig, tx_method: str
) -> None:
    async with async_config.provide_session() as session:
        session._detach_connection()  # pyright: ignore[reportPrivateUsage]
        assert session.connection is _DETACHED_CONNECTION
        session.attach_observability(ObservabilityRuntime())
        assert session._stmt_cache_enabled is False  # pyright: ignore[reportPrivateUsage]
        with pytest.raises(ImproperConfigurationError, match="already closed"):
            await session.execute("SELECT 1")
        with pytest.raises(SQLSpecError):
            await getattr(session, tx_method)()


def test_sqlspec_runtime_sync_provide_session_detaches_and_emits_raw_connection_destroy(tmp_path: Path) -> None:
    destroyed: list[dict[str, Any]] = []
    lifecycle: LifecycleConfig = {"on_connection_destroy": [destroyed.append]}
    spec = SQLSpec(observability_config=ObservabilityConfig(lifecycle=lifecycle))
    cfg = spec.add_config(SqliteConfig(connection_config={"database": str(tmp_path / "rt_sync.sqlite")}))
    try:
        with spec.provide_session(cfg) as session:
            live_conn = session.connection
            assert live_conn is not _DETACHED_CONNECTION
        assert session.connection is _DETACHED_CONNECTION
        assert len(destroyed) == 1
        assert destroyed[0]["connection"] is live_conn
        with pytest.raises(ImproperConfigurationError, match="already closed"):
            session.execute("SELECT 1")
    finally:
        cfg.close_pool()


async def test_sqlspec_runtime_async_provide_session_detaches_and_emits_raw_connection_destroy(tmp_path: Path) -> None:
    destroyed: list[dict[str, Any]] = []
    lifecycle: LifecycleConfig = {"on_connection_destroy": [destroyed.append]}
    spec = SQLSpec(observability_config=ObservabilityConfig(lifecycle=lifecycle))
    cfg = spec.add_config(AiosqliteConfig(connection_config={"database": str(tmp_path / "rt_async.sqlite")}))
    try:
        async with spec.provide_session(cfg) as session:
            live_conn = session.connection
            assert live_conn is not _DETACHED_CONNECTION
        assert session.connection is _DETACHED_CONNECTION
        assert len(destroyed) == 1
        assert destroyed[0]["connection"] is live_conn
        with pytest.raises(ImproperConfigurationError, match="already closed"):
            await session.execute("SELECT 1")
    finally:
        await cfg.close_pool()


@pytest.mark.parametrize(
    ("sql_text", "expects_begin"),
    [
        ("SELECT 1", False),
        ("PRAGMA user_version", False),
        ("INSERT INTO items (id) VALUES (10)", True),
        ("INSERT INTO items (id) VALUES (11) RETURNING id", True),
        ("UPDATE items SET id = 12 WHERE id = 10", True),
        ("DELETE FROM items WHERE id = 12", True),
    ],
)
def test_sync_auto_begin_on_write_starts_transaction_lazily(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, sql_text: str, expects_begin: bool
) -> None:
    cfg = SqliteConfig(connection_config={"database": str(tmp_path / "lazy_sync.sqlite"), "autocommit": True})
    try:
        with cfg.provide_session() as setup:
            setup.execute_script("CREATE TABLE items (id INTEGER PRIMARY KEY)")
        begins = 0
        with cfg.provide_session() as session:
            driver_cls = type(session)
            orig_begin = driver_cls.begin

            def tracked_begin(driver: Any) -> None:
                nonlocal begins
                if driver is session:
                    begins += 1
                orig_begin(driver)

            monkeypatch.setattr(driver_cls, "begin", tracked_begin)
            session._stmt_cache_enabled = False  # pyright: ignore[reportPrivateUsage]
            session._auto_begin_on_write = True  # pyright: ignore[reportPrivateUsage]
            session.execute(sql_text)
            if expects_begin:
                assert begins == 1
                session.execute("INSERT INTO items (id) VALUES (99)")
                assert begins == 1
                session.rollback()
            else:
                assert begins == 0
    finally:
        cfg.close_pool()


@pytest.mark.parametrize(
    ("sql_text", "expects_begin"),
    [
        ("SELECT 1", False),
        ("PRAGMA user_version", False),
        ("INSERT INTO items (id) VALUES (10)", True),
        ("INSERT INTO items (id) VALUES (11) RETURNING id", True),
        ("UPDATE items SET id = 12 WHERE id = 10", True),
        ("DELETE FROM items WHERE id = 12", True),
    ],
)
async def test_async_auto_begin_on_write_starts_transaction_lazily(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, sql_text: str, expects_begin: bool
) -> None:
    cfg = AiosqliteConfig(connection_config={"database": str(tmp_path / "lazy_async.sqlite"), "autocommit": True})
    try:
        async with cfg.provide_session() as setup:
            await setup.execute_script("CREATE TABLE items (id INTEGER PRIMARY KEY)")
        begins = 0
        async with cfg.provide_session() as session:
            driver_cls = type(session)
            orig_begin = driver_cls.begin

            async def tracked_begin(driver: Any) -> None:
                nonlocal begins
                if driver is session:
                    begins += 1
                await orig_begin(driver)

            monkeypatch.setattr(driver_cls, "begin", tracked_begin)
            session._stmt_cache_enabled = False  # pyright: ignore[reportPrivateUsage]
            session._auto_begin_on_write = True  # pyright: ignore[reportPrivateUsage]
            await session.execute(sql_text)
            if expects_begin:
                assert begins == 1
                await session.execute("INSERT INTO items (id) VALUES (99)")
                assert begins == 1
                await session.rollback()
            else:
                assert begins == 0
    finally:
        await cfg.close_pool()
