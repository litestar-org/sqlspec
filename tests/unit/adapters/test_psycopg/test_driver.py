"""Unit tests for psycopg driver transaction behavior."""

from typing import TYPE_CHECKING, cast

import psycopg
import pytest

from sqlspec.adapters.psycopg.driver import PsycopgAsyncDriver, PsycopgSyncDriver
from sqlspec.exceptions import SQLSpecError

if TYPE_CHECKING:
    from sqlspec.adapters.psycopg._typing import PsycopgAsyncConnection, PsycopgSyncConnection


class _SyncTransactionConnection:
    def __init__(self, method_name: str, error: Exception | None, *, autocommit: bool = True) -> None:
        self.method_name = method_name
        self.error = error
        self._autocommit = autocommit

    @property
    def autocommit(self) -> bool:
        return self._autocommit

    @autocommit.setter
    def autocommit(self, value: bool) -> None:
        if self.method_name == "begin" and self.error is not None:
            raise self.error
        self._autocommit = value

    def commit(self) -> None:
        if self.method_name == "commit" and self.error is not None:
            raise self.error

    def rollback(self) -> None:
        if self.method_name == "rollback" and self.error is not None:
            raise self.error


class _AsyncTransactionConnection:
    def __init__(self, method_name: str, error: Exception | None, *, autocommit: bool = True) -> None:
        self.method_name = method_name
        self.error = error
        self.autocommit = autocommit
        self.autocommit_calls: list[bool] = []

    async def set_autocommit(self, value: bool) -> None:
        self.autocommit_calls.append(value)
        if self.method_name == "begin" and self.error is not None:
            raise self.error
        self.autocommit = value

    async def commit(self) -> None:
        if self.method_name == "commit" and self.error is not None:
            raise self.error

    async def rollback(self) -> None:
        if self.method_name == "rollback" and self.error is not None:
            raise self.error


@pytest.mark.parametrize("method_name", ["begin", "commit", "rollback"])
def test_sync_transaction_control_propagates_non_native_errors(method_name: str) -> None:
    connection = _SyncTransactionConnection(method_name, RuntimeError("internal bug"))
    driver = PsycopgSyncDriver(cast("PsycopgSyncConnection", connection))

    with pytest.raises(RuntimeError, match="internal bug"):
        getattr(driver, method_name)()


@pytest.mark.parametrize("method_name", ["begin", "commit", "rollback"])
def test_sync_transaction_control_wraps_native_errors(method_name: str) -> None:
    connection = _SyncTransactionConnection(method_name, psycopg.Error("native failure"))
    driver = PsycopgSyncDriver(cast("PsycopgSyncConnection", connection))

    with pytest.raises(SQLSpecError, match=f"Failed to {method_name} transaction"):
        getattr(driver, method_name)()


@pytest.mark.anyio
@pytest.mark.parametrize("method_name", ["begin", "commit", "rollback"])
async def test_async_transaction_control_propagates_non_native_errors(method_name: str) -> None:
    connection = _AsyncTransactionConnection(method_name, RuntimeError("internal bug"))
    driver = PsycopgAsyncDriver(cast("PsycopgAsyncConnection", connection))

    with pytest.raises(RuntimeError, match="internal bug"):
        await getattr(driver, method_name)()


@pytest.mark.anyio
@pytest.mark.parametrize("method_name", ["begin", "commit", "rollback"])
async def test_async_transaction_control_wraps_native_errors(method_name: str) -> None:
    connection = _AsyncTransactionConnection(method_name, psycopg.Error("native failure"))
    driver = PsycopgAsyncDriver(cast("PsycopgAsyncConnection", connection))

    with pytest.raises(SQLSpecError, match=f"Failed to {method_name} transaction"):
        await getattr(driver, method_name)()


@pytest.mark.anyio
@pytest.mark.parametrize(("autocommit", "expected_calls"), [(True, [False]), (False, [])])
async def test_async_begin_uses_autocommit_property_before_setter(autocommit: bool, expected_calls: list[bool]) -> None:
    connection = _AsyncTransactionConnection("begin", None, autocommit=autocommit)
    driver = PsycopgAsyncDriver(cast("PsycopgAsyncConnection", connection))

    await driver.begin()

    assert connection.autocommit_calls == expected_calls
