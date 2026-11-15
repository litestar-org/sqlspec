"""Stream sources finalize transactions only on the connection that started them."""

import inspect
from typing import Any, Self

import pytest

from sqlspec.adapters.asyncpg.core import AsyncpgStreamSource
from sqlspec.adapters.cockroach_asyncpg.driver import CockroachAsyncpgStreamSource
from sqlspec.adapters.cockroach_psycopg.driver import (
    CockroachPsycopgAsyncStreamSource,
    CockroachPsycopgSyncStreamSource,
)
from sqlspec.adapters.psqlpy.core import PsqlpyStreamSource
from sqlspec.adapters.psycopg.core import PsycopgAsyncStreamSource, PsycopgSyncStreamSource

pytestmark = pytest.mark.anyio


class FakeTransaction:
    def __init__(self, calls: "list[str]", label: str) -> None:
        self.calls = calls
        self.label = label

    def __exit__(self, *args: Any) -> None:
        self.calls.append(f"{self.label}:tx_exit")

    async def __aexit__(self, *args: Any) -> None:
        self.calls.append(f"{self.label}:tx_exit")

    def __enter__(self) -> Self:
        return self

    async def __aenter__(self) -> Self:
        return self

    async def start(self) -> None:
        return None

    async def begin(self) -> None:
        return None

    async def commit(self) -> None:
        self.calls.append(f"{self.label}:commit")

    async def rollback(self) -> None:
        self.calls.append(f"{self.label}:rollback")


class NullHandler:
    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: Any) -> None:
        return None

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *args: Any) -> None:
        return None


class FakeCursor:
    description = ()
    _portal_name = None

    def __init__(self, calls: "list[str]", label: str) -> None:
        self.calls = calls
        self.label = label
        self.itersize = 0

    async def start(self) -> None:
        return None

    def execute(self, *args: Any, **kwargs: Any) -> Any:
        return _Done()

    def close(self) -> Any:
        self.calls.append(f"{self.label}:cursor_close")
        return _Done()

    async def _close_portal(self, timeout: Any) -> None:
        self.calls.append(f"{self.label}:portal_close")

    def __await__(self) -> Any:
        return _Done().__await__()


class _Done:
    def __await__(self) -> Any:
        return iter(())


class FakeConnection:
    """Connection double recording every call made against it under a label."""

    def __init__(self, label: str, calls: "list[str]", fail_commit: bool = False, in_transaction: bool = False) -> None:
        self.label = label
        self.calls = calls
        self.fail_commit = fail_commit
        self.in_transaction = in_transaction

    def transaction(self) -> FakeTransaction:
        return FakeTransaction(self.calls, self.label)

    def cursor(self, *args: Any, **kwargs: Any) -> Any:
        return FakeCursor(self.calls, self.label)

    async def close(self) -> None:
        self.calls.append(f"{self.label}:connection_close")


class FailingCommitTransaction(FakeTransaction):
    async def commit(self) -> None:
        msg = "commit failed"
        raise RuntimeError(msg)


class FakeDriver:
    """Driver double exposing the hooks the stream sources call."""

    def __init__(self, connection: FakeConnection, calls: "list[str]") -> None:
        self.connection = connection
        self.calls = calls

    def handle_database_exceptions(self) -> Any:
        return NullHandler()

    async def _run_with_exception_handler(self, handler: Any, func: Any, *args: Any, **kwargs: Any) -> Any:
        result = func(*args, **kwargs)
        if inspect.isawaitable(result):
            return await result
        return result

    def _check_pending_exception(self, handler: Any) -> None:
        return None

    def _connection_in_transaction(self) -> bool:
        return bool(self.connection.in_transaction)

    async def commit(self) -> None:
        self.calls.append(f"{self.connection.label}:commit")

    async def rollback(self) -> None:
        self.calls.append(f"{self.connection.label}:rollback")

    async def _begin_follower_read_transaction(self) -> bool:
        self.connection.in_transaction = True
        return True

    def _wants_follower_read(self, statement: Any) -> bool:
        return True


class SyncFakeDriver(FakeDriver):
    def _run_with_exception_handler(self, handler: Any, func: Any, *args: Any, **kwargs: Any) -> Any:  # type: ignore[override]
        return func(*args, **kwargs)

    def commit(self) -> None:  # type: ignore[override]
        self.calls.append(f"{self.connection.label}:commit")

    def rollback(self) -> None:  # type: ignore[override]
        self.calls.append(f"{self.connection.label}:rollback")


@pytest.fixture
def calls() -> "list[str]":
    return []


def swap_owner(driver: FakeDriver, calls: "list[str]") -> FakeConnection:
    successor = FakeConnection("B", calls, in_transaction=True)
    driver.connection = successor
    return successor


def foreign_calls(calls: "list[str]") -> "list[str]":
    return [call for call in calls if call.startswith("B:")]


def test_psycopg_sync_late_close_skips_foreign_transaction_exit(calls: "list[str]") -> None:
    driver = SyncFakeDriver(FakeConnection("A", calls), calls)
    source = PsycopgSyncStreamSource(driver, "select 1", None, 10)
    source.start()
    swap_owner(driver, calls)
    source.close()
    assert foreign_calls(calls) == []
    assert "A:tx_exit" not in calls


async def test_psycopg_async_late_close_skips_foreign_transaction_exit(calls: "list[str]") -> None:
    driver = FakeDriver(FakeConnection("A", calls), calls)
    source = PsycopgAsyncStreamSource(driver, "select 1", None, 10)
    await source.start()
    swap_owner(driver, calls)
    await source.close()
    assert foreign_calls(calls) == []
    assert "A:tx_exit" not in calls


def test_psycopg_sync_owned_close_exits_transaction_once(calls: "list[str]") -> None:
    driver = SyncFakeDriver(FakeConnection("A", calls), calls)
    source = PsycopgSyncStreamSource(driver, "select 1", None, 10)
    source.start()
    source.close()
    source.close()
    assert calls.count("A:tx_exit") == 1


async def test_asyncpg_finalize_error_closes_own_connection_only(calls: "list[str]") -> None:
    connection = FakeConnection("A", calls)
    driver = FakeDriver(connection, calls)
    source = AsyncpgStreamSource(driver, "select 1", (), 10)
    await source.start()
    source._transaction = FailingCommitTransaction(calls, "A")
    with pytest.raises(RuntimeError):
        await source.close()
    assert "A:connection_close" in calls
    assert foreign_calls(calls) == []


async def test_asyncpg_late_close_skips_foreign_commit(calls: "list[str]") -> None:
    driver = FakeDriver(FakeConnection("A", calls), calls)
    source = AsyncpgStreamSource(driver, "select 1", (), 10)
    await source.start()
    swap_owner(driver, calls)
    await source.close()
    assert calls == []


async def test_psqlpy_late_close_skips_foreign_transaction(calls: "list[str]") -> None:
    driver = FakeDriver(FakeConnection("A", calls), calls)
    source = PsqlpyStreamSource(driver, "select 1", None, 10)
    await source.start()
    swap_owner(driver, calls)
    await source.close()
    assert foreign_calls(calls) == []
    assert "A:commit" not in calls


@pytest.mark.parametrize("error", [False, True], ids=["clean", "error"])
async def test_owned_close_still_finalizes_stream_transaction(calls: "list[str]", error: bool) -> None:
    asyncpg_driver = FakeDriver(FakeConnection("A", calls), calls)
    asyncpg_source = AsyncpgStreamSource(asyncpg_driver, "select 1", (), 10)
    await asyncpg_source.start()
    await asyncpg_source.close(error=error)
    psqlpy_source = PsqlpyStreamSource(FakeDriver(FakeConnection("A", calls), calls), "select 1", None, 10)
    await psqlpy_source.start()
    await psqlpy_source.close(error=error)
    expected = "A:rollback" if error else "A:commit"
    assert calls.count(expected) == 2


class StubUnderlying:
    column_types = None

    def __init__(self) -> None:
        self.closed = 0

    def start(self) -> Any:
        return None

    def fetch_chunk(self) -> Any:
        return []

    def close(self, error: bool = False) -> Any:
        self.closed += 1


class AsyncStubUnderlying(StubUnderlying):
    async def start(self) -> None:
        return None

    async def fetch_chunk(self) -> Any:
        return []

    async def close(self, error: bool = False) -> None:
        self.closed += 1


def test_cockroach_psycopg_sync_late_close_skips_foreign_commit(calls: "list[str]") -> None:
    driver = SyncFakeDriver(FakeConnection("A", calls, in_transaction=True), calls)
    source = CockroachPsycopgSyncStreamSource(driver, StubUnderlying(), True)  # type: ignore[arg-type]
    swap_owner(driver, calls)
    source.close()
    assert foreign_calls(calls) == []


@pytest.mark.parametrize("source_type", [CockroachPsycopgAsyncStreamSource, CockroachAsyncpgStreamSource])
async def test_cockroach_async_late_close_skips_foreign_commit(calls: "list[str]", source_type: Any) -> None:
    driver = FakeDriver(FakeConnection("A", calls), calls)
    underlying = AsyncStubUnderlying()
    source = source_type(driver, "select 1", underlying)
    await source.start()
    swap_owner(driver, calls)
    await source.close()
    assert foreign_calls(calls) == []
    assert underlying.closed == 1


@pytest.mark.parametrize("error", [False, True], ids=["clean", "error"])
async def test_cockroach_owned_close_finalizes_follower_read(calls: "list[str]", error: bool) -> None:
    expected = "A:rollback" if error else "A:commit"

    sync_driver = SyncFakeDriver(FakeConnection("A", calls, in_transaction=True), calls)
    CockroachPsycopgSyncStreamSource(sync_driver, StubUnderlying(), True).close(error=error)  # type: ignore[arg-type]
    assert calls == [expected]

    for source_type in (CockroachPsycopgAsyncStreamSource, CockroachAsyncpgStreamSource):
        calls.clear()
        driver = FakeDriver(FakeConnection("A", calls), calls)
        source = source_type(driver, "select 1", AsyncStubUnderlying())
        await source.start()
        await source.close(error=error)
        assert calls == [expected]
