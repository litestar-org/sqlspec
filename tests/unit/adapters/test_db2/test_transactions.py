"""Tests for the Db2 autocommit baseline and transaction lifecycle."""

from typing import Any

import pytest

import sqlspec.adapters.db2._typing as typing_module
from sqlspec.adapters.db2._typing import Db2SyncSessionContext
from sqlspec.adapters.db2.config import Db2SyncConfig
from sqlspec.adapters.db2.core import default_statement_config
from sqlspec.adapters.db2.driver import Db2SyncDriver
from sqlspec.exceptions import MissingDependencyError, SQLSpecError
from tests.unit.adapters.test_db2._fakes import (
    FakeDb2Connection,
    FakeDb2OperationalError,
    FakeIbmDbDbiModule,
    FakeIbmDbModule,
)

FakeModules = tuple[FakeIbmDbModule, FakeIbmDbDbiModule]
INSERT = "INSERT INTO t (a) VALUES (1)"


def _config(fake_ibm_db: FakeModules, *, autocommit: "bool | None" = None) -> "tuple[Db2SyncConfig, FakeDb2Connection]":
    """Build a config whose pool hands out one scripted fake connection.

    Returns:
        The config and the connection its pool will open.
    """
    _, fake_module = fake_ibm_db
    connection = FakeDb2Connection()
    fake_module.pending_connections.append(connection)
    connection_config: dict[str, Any] = {"database": "d"}
    if autocommit is not None:
        connection_config["autocommit"] = autocommit
    return Db2SyncConfig(connection_config=connection_config), connection


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


@pytest.mark.parametrize(("autocommit", "expected"), [(None, {102: 1}), (True, {102: 1}), (False, {102: 0})])
def test_pool_connects_with_autocommit_on_by_default(
    fake_ibm_db: FakeModules, autocommit: "bool | None", expected: "dict[int, int]"
) -> None:
    """Connections open with autocommit on unless the config turns it off."""
    _, fake_module = fake_ibm_db
    config, connection = _config(fake_ibm_db, autocommit=autocommit)

    config.create_connection()

    assert fake_module.connect_calls[0][5] == expected
    assert connection.autocommit is (autocommit is not False)


def test_implicit_write_commits_without_begin(fake_ibm_db: FakeModules) -> None:
    """A write outside ``begin()`` is durable immediately on a default connection."""
    config, connection = _config(fake_ibm_db)

    with config.provide_session() as driver:
        driver.execute(INSERT)

    assert [sql for sql, _ in connection.committed] == [INSERT]
    assert connection.pending == []
    assert connection.rollbacks == 0


@pytest.mark.parametrize(("finish", "persisted"), [("commit", True), ("rollback", False)])
def test_begin_commit_restores_autocommit(fake_ibm_db: FakeModules, finish: str, persisted: bool) -> None:
    """``commit()`` and ``rollback()`` end the transaction and restore autocommit."""
    config, connection = _config(fake_ibm_db)

    with config.provide_session() as driver:
        driver.begin()
        assert connection.autocommit is False
        driver.execute(INSERT)
        getattr(driver, finish)()
        assert connection.autocommit is True
        assert driver._connection_in_transaction() is False

    assert [sql for sql, _ in connection.committed] == ([INSERT] if persisted else [])
    assert connection.pending == []


def test_begin_is_idempotent_while_active(fake_ibm_db: FakeModules, monkeypatch: pytest.MonkeyPatch) -> None:
    """A second ``begin()`` inside an active transaction changes nothing."""
    config, connection = _config(fake_ibm_db)
    calls = _record_autocommit_calls(monkeypatch, connection)

    with config.provide_session() as driver:
        driver.begin()
        driver.begin()
        assert calls == [False]
        driver.commit()

    assert calls == [False, True]


def test_begin_on_autocommit_off_connection_does_not_toggle(
    fake_ibm_db: FakeModules, monkeypatch: pytest.MonkeyPatch
) -> None:
    """On an autocommit-off baseline, transactions never switch autocommit on."""
    config, connection = _config(fake_ibm_db, autocommit=False)
    calls = _record_autocommit_calls(monkeypatch, connection)

    with config.provide_session() as driver:
        driver.begin()
        driver.execute(INSERT)
        driver.commit()

    assert calls == []
    assert connection.autocommit is False
    assert [sql for sql, _ in connection.committed] == [INSERT]


def test_session_exit_rolls_back_active_transaction(fake_ibm_db: FakeModules) -> None:
    """Leaving a session with an open transaction rolls it back and restores autocommit."""
    config, connection = _config(fake_ibm_db)

    with config.provide_session() as driver:
        driver.begin()
        driver.execute(INSERT)

    assert connection.rollbacks == 1
    assert connection.committed == []
    assert connection.pending == []
    assert connection.autocommit is True


@pytest.mark.parametrize(("commit", "persisted"), [(False, False), (True, True)], ids=["uncommitted", "committed"])
def test_transaction_session_begins_and_rolls_back_uncommitted_work(
    fake_ibm_db: FakeModules, commit: bool, persisted: bool
) -> None:
    """``provide_session(transaction=True)`` opens a transaction the session must commit."""
    config, connection = _config(fake_ibm_db)

    with config.provide_session(transaction=True) as driver:
        assert driver._connection_in_transaction() is True
        driver.execute(INSERT)
        if commit:
            driver.commit()

    assert [sql for sql, _ in connection.committed] == ([INSERT] if persisted else [])
    assert connection.pending == []
    assert connection.autocommit is True


def test_session_exit_rolls_back_autocommit_off_work(fake_ibm_db: FakeModules) -> None:
    """On an autocommit-off baseline, implicit work left open is rolled back on release."""
    config, connection = _config(fake_ibm_db, autocommit=False)

    with config.provide_session() as driver:
        driver.execute(INSERT)

    assert connection.rollbacks == 1
    assert connection.committed == []
    assert connection.pending == []


def test_session_exit_releases_connection_when_rollback_fails(
    fake_ibm_db: FakeModules, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failing release rollback neither masks the session error nor skips the release."""
    ibm_db, _ = fake_ibm_db
    connection = FakeDb2Connection(autocommit=True)
    ibm_db.register(connection)

    def failing_rollback() -> None:
        raise FakeDb2OperationalError("SQL30081N  A communication error has been detected.")

    monkeypatch.setattr(connection, "rollback", failing_rollback)
    released: list[tuple[object, object]] = []
    context = Db2SyncSessionContext(
        acquire_connection=lambda: connection,
        release_connection=lambda conn, **kwargs: released.append((conn, kwargs.get("exc_type"))),
        statement_config=default_statement_config,
        driver_features={},
        prepare_driver=lambda driver: driver,
    )

    with pytest.raises(RuntimeError, match="body failed"):
        with context as driver:
            driver.begin()
            raise RuntimeError("body failed")

    assert released == [(connection, RuntimeError)]


@pytest.mark.parametrize("method", ["commit", "rollback"])
def test_transaction_error_wraps_only_vendor_errors(
    fake_ibm_db: FakeModules, monkeypatch: pytest.MonkeyPatch, method: str
) -> None:
    """Driver errors from commit/rollback become ``SQLSpecError``; other errors propagate unchanged."""
    ibm_db, _ = fake_ibm_db
    connection = FakeDb2Connection(autocommit=True)
    ibm_db.register(connection)
    driver = Db2SyncDriver(connection)
    vendor_error = FakeDb2OperationalError("SQL0911N  The current transaction has been rolled back.")

    def raise_vendor() -> None:
        raise vendor_error

    monkeypatch.setattr(connection, method, raise_vendor)
    driver.begin()
    with pytest.raises(SQLSpecError) as exc_info:
        getattr(driver, method)()
    assert exc_info.value.__cause__ is vendor_error

    def raise_runtime() -> None:
        raise RuntimeError("not a driver error")

    monkeypatch.setattr(connection, method, raise_runtime)
    with pytest.raises(RuntimeError, match="not a driver error"):
        getattr(driver, method)()


def test_transaction_session_releases_connection_when_begin_fails(
    fake_ibm_db: FakeModules, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A session that cannot begin its transaction still releases the connection."""
    ibm_db, _ = fake_ibm_db
    connection = FakeDb2Connection(autocommit=True)
    ibm_db.register(connection)

    def failing_set_autocommit(is_on: bool) -> None:
        raise FakeDb2OperationalError("SQL30081N  A communication error has been detected.")

    monkeypatch.setattr(connection, "set_autocommit", failing_set_autocommit)
    released: list[tuple[object, object]] = []
    context = Db2SyncSessionContext(
        acquire_connection=lambda: connection,
        release_connection=lambda conn, **kwargs: released.append((conn, kwargs.get("exc_type"))),
        statement_config=default_statement_config,
        driver_features={},
        prepare_driver=lambda driver: driver,
        begin_transaction=True,
    )

    with pytest.raises(SQLSpecError, match="Failed to begin Db2 transaction"):
        with context:
            pytest.fail("the session body must not run")

    assert released == [(connection, SQLSpecError)]


def test_restoring_autocommit_wraps_vendor_errors(fake_ibm_db: FakeModules, monkeypatch: pytest.MonkeyPatch) -> None:
    """A driver error while switching autocommit back on is reported as ``SQLSpecError``."""
    ibm_db, _ = fake_ibm_db
    connection = FakeDb2Connection(autocommit=True)
    ibm_db.register(connection)
    driver = Db2SyncDriver(connection)
    driver.begin()

    def failing_set_autocommit(is_on: bool) -> None:
        raise FakeDb2OperationalError("SQL30081N  A communication error has been detected.")

    monkeypatch.setattr(connection, "set_autocommit", failing_set_autocommit)

    with pytest.raises(SQLSpecError, match="Failed to restore Db2 autocommit"):
        driver.commit()
    assert driver._connection_in_transaction() is False


def test_begin_requires_ibm_db(monkeypatch: pytest.MonkeyPatch) -> None:
    """Reading the autocommit mode needs ``ibm_db``; without it ``begin()`` reports the missing extra."""
    monkeypatch.setattr(typing_module, "ibm_db", None)
    driver = Db2SyncDriver(FakeDb2Connection(autocommit=True))

    with pytest.raises(MissingDependencyError):
        driver.begin()
