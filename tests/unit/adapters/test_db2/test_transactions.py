"""Tests for the Db2 autocommit baseline and transaction lifecycle.

Driver and session-context behaviors run in both driver modes through ``db2_mode``.
"""

from typing import Any

import pytest

import sqlspec.adapters.db2._typing as typing_module
from sqlspec.exceptions import MissingDependencyError, SQLSpecError
from tests.unit.adapters.test_db2._fakes import (
    DriverMode,
    FakeDb2Connection,
    FakeDb2OperationalError,
    FakeIbmDbDbiModule,
    FakeIbmDbModule,
)

FakeModules = tuple[FakeIbmDbModule, FakeIbmDbDbiModule]
INSERT = "INSERT INTO t (a) VALUES (1)"


def _config(
    fake_ibm_db: FakeModules, db2_mode: DriverMode, *, autocommit: "bool | None" = None
) -> "tuple[Any, FakeDb2Connection]":
    """Build a config of the given mode whose pool hands out one scripted fake connection.

    Returns:
        The config and the connection its pool will open.
    """
    _, fake_module = fake_ibm_db
    connection = FakeDb2Connection()
    fake_module.pending_connections.append(connection)
    connection_config: dict[str, Any] = {"database": "d"}
    if autocommit is not None:
        connection_config["autocommit"] = autocommit
    return db2_mode.config(connection_config=connection_config), connection


def _registered_connection(fake_ibm_db: FakeModules, *, autocommit: bool = True) -> FakeDb2Connection:
    """Build a fake connection whose autocommit mode the ``ibm_db`` fake can read.

    Returns:
        The registered connection.
    """
    ibm_db, _ = fake_ibm_db
    connection = FakeDb2Connection(autocommit=autocommit)
    ibm_db.register(connection)
    return connection


def _record_autocommit_calls(monkeypatch: pytest.MonkeyPatch, connection: FakeDb2Connection) -> "list[bool]":
    """Record every ``set_autocommit`` call while keeping the fake's behavior.

    Returns:
        The list that receives each requested mode.
    """
    calls: list[bool] = []
    original = connection.set_autocommit

    def record(is_on: bool) -> None:
        calls.append(is_on)
        original(is_on)

    monkeypatch.setattr(connection, "set_autocommit", record)
    return calls


@pytest.mark.anyio
@pytest.mark.parametrize(("autocommit", "expected"), [(None, {102: 1}), (True, {102: 1}), (False, {102: 0})])
async def test_pool_connects_with_autocommit_on_by_default(
    fake_ibm_db: FakeModules, db2_mode: DriverMode, autocommit: "bool | None", expected: "dict[int, int]"
) -> None:
    """Connections open with autocommit on unless the config turns it off."""
    _, fake_module = fake_ibm_db
    config, connection = _config(fake_ibm_db, db2_mode, autocommit=autocommit)

    await db2_mode.call(config.create_connection)

    assert fake_module.connect_calls[0][5] == expected
    assert connection.autocommit is (autocommit is not False)
    await db2_mode.call(config.close_pool)


@pytest.mark.anyio
async def test_implicit_write_commits_without_begin(fake_ibm_db: FakeModules, db2_mode: DriverMode) -> None:
    """A write outside ``begin()`` is durable immediately on a default connection."""
    config, connection = _config(fake_ibm_db, db2_mode)

    async with db2_mode.enter(config.provide_session()) as driver:
        await db2_mode.call(driver.execute, INSERT)

    assert [sql for sql, _ in connection.committed] == [INSERT]
    assert connection.pending == []
    assert connection.rollbacks == 0
    await db2_mode.call(config.close_pool)


@pytest.mark.anyio
@pytest.mark.parametrize(("finish", "persisted"), [("commit", True), ("rollback", False)])
async def test_begin_commit_restores_autocommit(
    fake_ibm_db: FakeModules, db2_mode: DriverMode, finish: str, persisted: bool
) -> None:
    """``commit()`` and ``rollback()`` end the transaction and restore autocommit."""
    connection = _registered_connection(fake_ibm_db)
    driver = db2_mode.driver(connection)

    await db2_mode.call(driver.begin)
    assert connection.autocommit is False
    await db2_mode.call(driver.execute, INSERT)
    await db2_mode.call(getattr(driver, finish))

    assert connection.autocommit is True
    assert driver._connection_in_transaction() is False
    assert [sql for sql, _ in connection.committed] == ([INSERT] if persisted else [])
    assert connection.pending == []


@pytest.mark.anyio
async def test_begin_is_idempotent_while_active(
    fake_ibm_db: FakeModules, db2_mode: DriverMode, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A second ``begin()`` inside an active transaction changes nothing."""
    connection = _registered_connection(fake_ibm_db)
    calls = _record_autocommit_calls(monkeypatch, connection)
    driver = db2_mode.driver(connection)

    await db2_mode.call(driver.begin)
    await db2_mode.call(driver.begin)
    assert calls == [False]
    await db2_mode.call(driver.commit)

    assert calls == [False, True]


@pytest.mark.anyio
async def test_begin_on_autocommit_off_connection_does_not_toggle(
    fake_ibm_db: FakeModules, db2_mode: DriverMode, monkeypatch: pytest.MonkeyPatch
) -> None:
    """On an autocommit-off baseline, transactions never switch autocommit on."""
    connection = _registered_connection(fake_ibm_db, autocommit=False)
    calls = _record_autocommit_calls(monkeypatch, connection)
    driver = db2_mode.driver(connection)

    await db2_mode.call(driver.begin)
    await db2_mode.call(driver.execute, INSERT)
    await db2_mode.call(driver.commit)

    assert calls == []
    assert connection.autocommit is False
    assert [sql for sql, _ in connection.committed] == [INSERT]


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("autocommit", "begin"), [(True, True), (False, False), (False, True)], ids=["transaction", "implicit", "both"]
)
async def test_session_context_exit_rolls_back_open_work(
    fake_ibm_db: FakeModules, db2_mode: DriverMode, autocommit: bool, begin: bool
) -> None:
    """Session exit rolls back an active transaction or autocommit-off work, then releases."""
    connection = _registered_connection(fake_ibm_db, autocommit=autocommit)
    released: list[tuple[object, object]] = []
    context = db2_mode.session_context(connection, released, autocommit_baseline=autocommit, begin_transaction=begin)

    async with db2_mode.enter(context) as driver:
        assert driver._connection_in_transaction() is begin
        await db2_mode.call(driver.execute, INSERT)

    assert connection.rollbacks == 1
    assert connection.committed == []
    assert connection.pending == []
    assert connection.autocommit is autocommit
    assert released == [(connection, None)]


@pytest.mark.anyio
async def test_session_context_exit_keeps_committed_autocommit_work(
    fake_ibm_db: FakeModules, db2_mode: DriverMode
) -> None:
    """On an autocommit baseline with no open transaction, session exit issues no rollback."""
    connection = _registered_connection(fake_ibm_db)
    released: list[tuple[object, object]] = []
    context = db2_mode.session_context(connection, released)

    async with db2_mode.enter(context) as driver:
        await db2_mode.call(driver.execute, INSERT)

    assert connection.rollbacks == 0
    assert [sql for sql, _ in connection.committed] == [INSERT]
    assert released == [(connection, None)]


@pytest.mark.anyio
async def test_session_exit_rolls_back_active_transaction(fake_ibm_db: FakeModules, db2_mode: DriverMode) -> None:
    """Leaving a session with an open transaction rolls it back and restores autocommit."""
    config, connection = _config(fake_ibm_db, db2_mode)

    async with db2_mode.enter(config.provide_session()) as driver:
        await db2_mode.call(driver.begin)
        await db2_mode.call(driver.execute, INSERT)

    assert connection.rollbacks == 1
    assert connection.committed == []
    assert connection.pending == []
    assert connection.autocommit is True
    await db2_mode.call(config.close_pool)


@pytest.mark.anyio
@pytest.mark.parametrize(("commit", "persisted"), [(False, False), (True, True)], ids=["uncommitted", "committed"])
async def test_transaction_session_begins_and_rolls_back_uncommitted_work(
    fake_ibm_db: FakeModules, db2_mode: DriverMode, commit: bool, persisted: bool
) -> None:
    """``provide_session(transaction=True)`` opens a transaction the session must commit."""
    config, connection = _config(fake_ibm_db, db2_mode)

    async with db2_mode.enter(config.provide_session(transaction=True)) as driver:
        assert driver._connection_in_transaction() is True
        await db2_mode.call(driver.execute, INSERT)
        if commit:
            await db2_mode.call(driver.commit)

    assert [sql for sql, _ in connection.committed] == ([INSERT] if persisted else [])
    assert connection.pending == []
    assert connection.autocommit is True
    await db2_mode.call(config.close_pool)


@pytest.mark.anyio
async def test_session_exit_rolls_back_autocommit_off_work(fake_ibm_db: FakeModules, db2_mode: DriverMode) -> None:
    """On an autocommit-off baseline, implicit work left open is rolled back on release."""
    config, connection = _config(fake_ibm_db, db2_mode, autocommit=False)

    async with db2_mode.enter(config.provide_session()) as driver:
        await db2_mode.call(driver.execute, INSERT)

    assert connection.rollbacks == 1
    assert connection.committed == []
    assert connection.pending == []
    await db2_mode.call(config.close_pool)


@pytest.mark.anyio
async def test_session_exit_releases_connection_when_rollback_fails(
    fake_ibm_db: FakeModules, db2_mode: DriverMode, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failing release rollback neither masks the session error nor skips the release."""
    connection = _registered_connection(fake_ibm_db)

    def failing_rollback() -> None:
        raise FakeDb2OperationalError("SQL30081N  A communication error has been detected.")

    monkeypatch.setattr(connection, "rollback", failing_rollback)
    released: list[tuple[object, object]] = []
    context = db2_mode.session_context(connection, released)

    with pytest.raises(RuntimeError, match="body failed"):
        async with db2_mode.enter(context) as driver:
            await db2_mode.call(driver.begin)
            raise RuntimeError("body failed")

    assert released == [(connection, RuntimeError)]


@pytest.mark.anyio
@pytest.mark.parametrize("method", ["commit", "rollback"])
async def test_transaction_error_wraps_only_vendor_errors(
    fake_ibm_db: FakeModules, db2_mode: DriverMode, monkeypatch: pytest.MonkeyPatch, method: str
) -> None:
    """Driver errors from commit/rollback become ``SQLSpecError``; other errors propagate unchanged."""
    connection = _registered_connection(fake_ibm_db)
    driver = db2_mode.driver(connection)
    vendor_error = FakeDb2OperationalError("SQL0911N  The current transaction has been rolled back.")

    def raise_vendor() -> None:
        raise vendor_error

    monkeypatch.setattr(connection, method, raise_vendor)
    await db2_mode.call(driver.begin)
    with pytest.raises(SQLSpecError) as exc_info:
        await db2_mode.call(getattr(driver, method))
    assert exc_info.value.__cause__ is vendor_error

    def raise_runtime() -> None:
        raise RuntimeError("not a driver error")

    monkeypatch.setattr(connection, method, raise_runtime)
    with pytest.raises(RuntimeError, match="not a driver error"):
        await db2_mode.call(getattr(driver, method))


@pytest.mark.anyio
async def test_transaction_session_releases_connection_when_begin_fails(
    fake_ibm_db: FakeModules, db2_mode: DriverMode, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A session that cannot begin its transaction still releases the connection."""
    connection = _registered_connection(fake_ibm_db)

    def failing_set_autocommit(is_on: bool) -> None:
        raise FakeDb2OperationalError("SQL30081N  A communication error has been detected.")

    monkeypatch.setattr(connection, "set_autocommit", failing_set_autocommit)
    released: list[tuple[object, object]] = []
    context = db2_mode.session_context(connection, released, begin_transaction=True)

    with pytest.raises(SQLSpecError, match="Failed to begin Db2 transaction"):
        async with db2_mode.enter(context):
            pytest.fail("the session body must not run")

    assert released == [(connection, SQLSpecError)]


@pytest.mark.anyio
async def test_restoring_autocommit_wraps_vendor_errors(
    fake_ibm_db: FakeModules, db2_mode: DriverMode, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A driver error while switching autocommit back on is reported as ``SQLSpecError``."""
    connection = _registered_connection(fake_ibm_db)
    driver = db2_mode.driver(connection)
    await db2_mode.call(driver.begin)

    def failing_set_autocommit(is_on: bool) -> None:
        raise FakeDb2OperationalError("SQL30081N  A communication error has been detected.")

    monkeypatch.setattr(connection, "set_autocommit", failing_set_autocommit)

    with pytest.raises(SQLSpecError, match="Failed to restore Db2 autocommit"):
        await db2_mode.call(driver.commit)
    assert driver._connection_in_transaction() is False


@pytest.mark.anyio
async def test_begin_requires_ibm_db(db2_mode: DriverMode, monkeypatch: pytest.MonkeyPatch) -> None:
    """Reading the autocommit mode needs ``ibm_db``; without it ``begin()`` reports the missing extra."""
    monkeypatch.setattr(typing_module, "ibm_db", None)
    driver = db2_mode.driver(FakeDb2Connection(autocommit=True))

    with pytest.raises(MissingDependencyError):
        await db2_mode.call(driver.begin)
