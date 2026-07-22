"""Unit tests for psycopg driver transaction behavior."""

from types import SimpleNamespace
from typing import TYPE_CHECKING, cast

import psycopg
import pytest

from sqlspec.adapters.psycopg.driver import PsycopgAsyncDriver, PsycopgSyncDriver
from sqlspec.exceptions import SQLSpecError

if TYPE_CHECKING:
    from sqlspec.adapters.psycopg._typing import PsycopgAsyncConnection, PsycopgSyncConnection


class _SyncTransactionConnection:
    def __init__(
        self,
        *,
        autocommit: bool = True,
        begin_error: Exception | None = None,
        commit_error: Exception | None = None,
        rollback_error: Exception | None = None,
        restore_error: Exception | None = None,
        transaction_status: int = 0,
    ) -> None:
        self._autocommit = autocommit
        self.autocommit_calls: list[bool] = []
        self.begin_error = begin_error
        self.commit_calls = 0
        self.commit_error = commit_error
        self.info = SimpleNamespace(transaction_status=transaction_status)
        self.restore_error = restore_error
        self.rollback_calls = 0
        self.rollback_error = rollback_error

    @property
    def autocommit(self) -> bool:
        return self._autocommit

    @autocommit.setter
    def autocommit(self, value: bool) -> None:
        self.autocommit_calls.append(value)
        error = self.begin_error if value is False else self.restore_error
        if error is not None:
            raise error
        self._autocommit = value

    def commit(self) -> None:
        self.commit_calls += 1
        if self.commit_error is not None:
            raise self.commit_error

    def rollback(self) -> None:
        self.rollback_calls += 1
        if self.rollback_error is not None:
            raise self.rollback_error


class _AsyncTransactionConnection:
    def __init__(
        self,
        *,
        autocommit: bool = True,
        begin_error: Exception | None = None,
        commit_error: Exception | None = None,
        rollback_error: Exception | None = None,
        restore_error: Exception | None = None,
        transaction_status: int = 0,
    ) -> None:
        self.autocommit = autocommit
        self.autocommit_calls: list[bool] = []
        self.begin_error = begin_error
        self.commit_calls = 0
        self.commit_error = commit_error
        self.info = SimpleNamespace(transaction_status=transaction_status)
        self.restore_error = restore_error
        self.rollback_calls = 0
        self.rollback_error = rollback_error

    async def set_autocommit(self, value: bool) -> None:
        self.autocommit_calls.append(value)
        error = self.begin_error if value is False else self.restore_error
        if error is not None:
            raise error
        self.autocommit = value

    async def commit(self) -> None:
        self.commit_calls += 1
        if self.commit_error is not None:
            raise self.commit_error

    async def rollback(self) -> None:
        self.rollback_calls += 1
        if self.rollback_error is not None:
            raise self.rollback_error


def _sync_driver(connection: _SyncTransactionConnection) -> PsycopgSyncDriver:
    return PsycopgSyncDriver(cast("PsycopgSyncConnection", connection))


def _async_driver(connection: _AsyncTransactionConnection) -> PsycopgAsyncDriver:
    return PsycopgAsyncDriver(cast("PsycopgAsyncConnection", connection))


def test_sync_transaction_state_is_inactive_initially() -> None:
    driver = _sync_driver(_SyncTransactionConnection())

    assert driver._transaction_active is False  # pyright: ignore[reportPrivateUsage]
    assert driver._restore_autocommit is False  # pyright: ignore[reportPrivateUsage]


async def test_async_transaction_state_is_inactive_initially() -> None:
    driver = _async_driver(_AsyncTransactionConnection())

    assert driver._transaction_active is False  # pyright: ignore[reportPrivateUsage]
    assert driver._restore_autocommit is False  # pyright: ignore[reportPrivateUsage]


@pytest.mark.parametrize("method_name", ["begin", "commit", "rollback"])
def test_sync_transaction_control_propagates_non_native_errors(method_name: str) -> None:
    error_kwargs = {f"{method_name}_error": RuntimeError("internal bug")}
    driver = _sync_driver(_SyncTransactionConnection(**error_kwargs))  # type: ignore[arg-type]

    with pytest.raises(RuntimeError, match="internal bug"):
        getattr(driver, method_name)()


@pytest.mark.parametrize("method_name", ["begin", "commit", "rollback"])
def test_sync_transaction_control_wraps_native_errors(method_name: str) -> None:
    error_kwargs = {f"{method_name}_error": psycopg.Error("native failure")}
    driver = _sync_driver(_SyncTransactionConnection(**error_kwargs))  # type: ignore[arg-type]

    with pytest.raises(SQLSpecError, match=f"Failed to {method_name} transaction"):
        getattr(driver, method_name)()


@pytest.mark.parametrize("method_name", ["begin", "commit", "rollback"])
async def test_async_transaction_control_propagates_non_native_errors(method_name: str) -> None:
    error_kwargs = {f"{method_name}_error": RuntimeError("internal bug")}
    driver = _async_driver(_AsyncTransactionConnection(**error_kwargs))  # type: ignore[arg-type]

    with pytest.raises(RuntimeError, match="internal bug"):
        await getattr(driver, method_name)()


@pytest.mark.parametrize("method_name", ["begin", "commit", "rollback"])
async def test_async_transaction_control_wraps_native_errors(method_name: str) -> None:
    error_kwargs = {f"{method_name}_error": psycopg.Error("native failure")}
    driver = _async_driver(_AsyncTransactionConnection(**error_kwargs))  # type: ignore[arg-type]

    with pytest.raises(SQLSpecError, match=f"Failed to {method_name} transaction"):
        await getattr(driver, method_name)()


@pytest.mark.parametrize("autocommit", [True, False])
def test_sync_commit_restores_only_changed_autocommit(autocommit: bool) -> None:
    connection = _SyncTransactionConnection(autocommit=autocommit)
    driver = _sync_driver(connection)

    driver.begin()
    driver.commit()

    assert connection.autocommit_calls == ([False, True] if autocommit else [])
    assert connection.autocommit is autocommit
    assert driver._connection_in_transaction() is False  # pyright: ignore[reportPrivateUsage]


@pytest.mark.parametrize("autocommit", [True, False])
async def test_async_commit_restores_only_changed_autocommit(autocommit: bool) -> None:
    connection = _AsyncTransactionConnection(autocommit=autocommit)
    driver = _async_driver(connection)

    await driver.begin()
    await driver.commit()

    assert connection.autocommit_calls == ([False, True] if autocommit else [])
    assert connection.autocommit is autocommit
    assert driver._connection_in_transaction() is False  # pyright: ignore[reportPrivateUsage]


def test_sync_rollback_restores_changed_autocommit() -> None:
    connection = _SyncTransactionConnection()
    driver = _sync_driver(connection)

    driver.begin()
    driver.rollback()

    assert connection.autocommit_calls == [False, True]
    assert connection.autocommit is True
    assert driver._connection_in_transaction() is False  # pyright: ignore[reportPrivateUsage]


async def test_async_rollback_restores_changed_autocommit() -> None:
    connection = _AsyncTransactionConnection()
    driver = _async_driver(connection)

    await driver.begin()
    await driver.rollback()

    assert connection.autocommit_calls == [False, True]
    assert connection.autocommit is True
    assert driver._connection_in_transaction() is False  # pyright: ignore[reportPrivateUsage]


def test_sync_begin_is_idempotent_for_logically_owned_transaction() -> None:
    connection = _SyncTransactionConnection()
    driver = _sync_driver(connection)

    driver.begin()
    driver.begin()

    assert connection.autocommit_calls == [False]
    assert driver._connection_in_transaction() is True  # pyright: ignore[reportPrivateUsage]


async def test_async_begin_is_idempotent_for_logically_owned_transaction() -> None:
    connection = _AsyncTransactionConnection()
    driver = _async_driver(connection)

    await driver.begin()
    await driver.begin()

    assert connection.autocommit_calls == [False]
    assert driver._connection_in_transaction() is True  # pyright: ignore[reportPrivateUsage]


def test_sync_begin_is_idempotent_for_libpq_owned_transaction() -> None:
    connection = _SyncTransactionConnection(transaction_status=2)
    driver = _sync_driver(connection)

    driver.begin()

    assert connection.autocommit_calls == []
    assert driver._transaction_active is False  # pyright: ignore[reportPrivateUsage]


async def test_async_begin_is_idempotent_for_libpq_owned_transaction() -> None:
    connection = _AsyncTransactionConnection(transaction_status=2)
    driver = _async_driver(connection)

    await driver.begin()

    assert connection.autocommit_calls == []
    assert driver._transaction_active is False  # pyright: ignore[reportPrivateUsage]


def test_sync_begin_failure_preserves_inactive_state() -> None:
    driver = _sync_driver(_SyncTransactionConnection(begin_error=psycopg.Error("setter failed")))

    with pytest.raises(SQLSpecError, match="Failed to begin transaction"):
        driver.begin()

    assert driver._transaction_active is False  # pyright: ignore[reportPrivateUsage]
    assert driver._restore_autocommit is False  # pyright: ignore[reportPrivateUsage]


async def test_async_begin_failure_preserves_inactive_state() -> None:
    driver = _async_driver(_AsyncTransactionConnection(begin_error=psycopg.Error("setter failed")))

    with pytest.raises(SQLSpecError, match="Failed to begin transaction"):
        await driver.begin()

    assert driver._transaction_active is False  # pyright: ignore[reportPrivateUsage]
    assert driver._restore_autocommit is False  # pyright: ignore[reportPrivateUsage]


@pytest.mark.parametrize("method_name", ["commit", "rollback"])
def test_sync_native_completion_failure_preserves_transaction_state(method_name: str) -> None:
    error_kwargs = {f"{method_name}_error": psycopg.Error("native failure")}
    driver = _sync_driver(_SyncTransactionConnection(**error_kwargs))  # type: ignore[arg-type]
    driver.begin()

    with pytest.raises(SQLSpecError, match=f"Failed to {method_name} transaction"):
        getattr(driver, method_name)()

    assert driver._transaction_active is True  # pyright: ignore[reportPrivateUsage]
    assert driver._restore_autocommit is True  # pyright: ignore[reportPrivateUsage]
    assert driver._connection_in_transaction() is True  # pyright: ignore[reportPrivateUsage]


@pytest.mark.parametrize("method_name", ["commit", "rollback"])
async def test_async_native_completion_failure_preserves_transaction_state(method_name: str) -> None:
    error_kwargs = {f"{method_name}_error": psycopg.Error("native failure")}
    driver = _async_driver(_AsyncTransactionConnection(**error_kwargs))  # type: ignore[arg-type]
    await driver.begin()

    with pytest.raises(SQLSpecError, match=f"Failed to {method_name} transaction"):
        await getattr(driver, method_name)()

    assert driver._transaction_active is True  # pyright: ignore[reportPrivateUsage]
    assert driver._restore_autocommit is True  # pyright: ignore[reportPrivateUsage]
    assert driver._connection_in_transaction() is True  # pyright: ignore[reportPrivateUsage]


@pytest.mark.parametrize("method_name", ["commit", "rollback"])
def test_sync_restoration_failure_reports_completed_transaction(method_name: str) -> None:
    connection = _SyncTransactionConnection(restore_error=psycopg.Error("restore failed"))
    driver = _sync_driver(connection)
    driver.begin()

    with pytest.raises(SQLSpecError, match="Failed to restore autocommit: restore failed"):
        getattr(driver, method_name)()

    assert connection.commit_calls == (1 if method_name == "commit" else 0)
    assert connection.rollback_calls == (1 if method_name == "rollback" else 0)
    assert driver._transaction_active is False  # pyright: ignore[reportPrivateUsage]
    assert driver._restore_autocommit is False  # pyright: ignore[reportPrivateUsage]
    assert driver._connection_in_transaction() is False  # pyright: ignore[reportPrivateUsage]


@pytest.mark.parametrize("method_name", ["commit", "rollback"])
async def test_async_restoration_failure_reports_completed_transaction(method_name: str) -> None:
    connection = _AsyncTransactionConnection(restore_error=psycopg.Error("restore failed"))
    driver = _async_driver(connection)
    await driver.begin()

    with pytest.raises(SQLSpecError, match="Failed to restore autocommit: restore failed"):
        await getattr(driver, method_name)()

    assert connection.commit_calls == (1 if method_name == "commit" else 0)
    assert connection.rollback_calls == (1 if method_name == "rollback" else 0)
    assert driver._transaction_active is False  # pyright: ignore[reportPrivateUsage]
    assert driver._restore_autocommit is False  # pyright: ignore[reportPrivateUsage]
    assert driver._connection_in_transaction() is False  # pyright: ignore[reportPrivateUsage]


def test_sync_logical_transaction_is_reported_while_libpq_is_idle() -> None:
    driver = _sync_driver(_SyncTransactionConnection(transaction_status=0))

    driver.begin()

    assert driver._connection_in_transaction() is True  # pyright: ignore[reportPrivateUsage]


async def test_async_logical_transaction_is_reported_while_libpq_is_idle() -> None:
    driver = _async_driver(_AsyncTransactionConnection(transaction_status=0))

    await driver.begin()

    assert driver._connection_in_transaction() is True  # pyright: ignore[reportPrivateUsage]
