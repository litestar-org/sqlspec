"""Stream sources finalize their transaction only on the connection they started on."""

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from sqlspec.adapters.asyncpg.core import AsyncpgStreamSource
from sqlspec.adapters.cockroach_asyncpg.driver import CockroachAsyncpgStreamSource
from sqlspec.adapters.cockroach_psycopg.driver import (
    CockroachPsycopgAsyncStreamSource,
    CockroachPsycopgSyncStreamSource,
)
from sqlspec.adapters.psqlpy.core import PsqlpyStreamSource
from sqlspec.adapters.psycopg.core import PsycopgAsyncStreamSource, PsycopgSyncStreamSource


def make_driver(connection: Any, *, in_transaction: bool = False, is_async: bool = True) -> Any:
    handler = MagicMock()
    driver = MagicMock()
    driver.connection = connection
    driver.handle_database_exceptions.return_value = handler
    driver._connection_in_transaction.return_value = in_transaction
    driver._wants_follower_read.return_value = True
    driver._begin_follower_read_transaction = AsyncMock(return_value=True) if is_async else MagicMock(return_value=True)
    driver.commit = AsyncMock() if is_async else MagicMock()
    driver.rollback = AsyncMock() if is_async else MagicMock()

    if is_async:

        async def run(_handler: Any, func: Any, *args: Any, **kwargs: Any) -> Any:
            return await func(*args, **kwargs)

    else:

        def run(_handler: Any, func: Any, *args: Any, **kwargs: Any) -> Any:
            return func(*args, **kwargs)

    driver._run_with_exception_handler = run
    return driver


def psycopg_connection(*, is_async: bool) -> "tuple[Any, Any]":
    transaction = MagicMock()
    transaction.__aenter__ = AsyncMock()
    transaction.__aexit__ = AsyncMock()
    cursor = MagicMock()
    cursor.close = AsyncMock() if is_async else MagicMock()
    cursor.description = [SimpleNamespace(name="x")]
    connection = MagicMock()
    connection.transaction.return_value = transaction
    connection.cursor.return_value = cursor
    return connection, transaction


@pytest.mark.anyio
@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
async def test_psycopg_late_close_skips_foreign_transaction_exit(is_async: bool) -> None:
    owner, owner_transaction = psycopg_connection(is_async=is_async)
    next_owner, next_transaction = psycopg_connection(is_async=is_async)
    driver = make_driver(owner, is_async=is_async)
    source_cls = PsycopgAsyncStreamSource if is_async else PsycopgSyncStreamSource
    source = source_cls(driver, "select 1", None, 10)
    if is_async:
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr("sqlspec.adapters.psycopg.core.execute_with_optional_parameters_async", AsyncMock())
            await source.start()
        driver.connection = next_owner
        await source.close()
    else:
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr("sqlspec.adapters.psycopg.core.execute_with_optional_parameters", MagicMock())
            source.start()
        driver.connection = next_owner
        source.close()
    exit_mock = owner_transaction.__aexit__ if is_async else owner_transaction.__exit__
    assert exit_mock.call_count == 0
    assert next_transaction.mock_calls == []
    assert next_owner.mock_calls == []


@pytest.mark.anyio
@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
async def test_psycopg_owned_close_exits_stream_transaction_once(is_async: bool) -> None:
    owner, transaction = psycopg_connection(is_async=is_async)
    driver = make_driver(owner, is_async=is_async)
    source_cls = PsycopgAsyncStreamSource if is_async else PsycopgSyncStreamSource
    source = source_cls(driver, "select 1", None, 10)
    with pytest.MonkeyPatch.context() as patch:
        if is_async:
            patch.setattr("sqlspec.adapters.psycopg.core.execute_with_optional_parameters_async", AsyncMock())
            await source.start()
            await source.close()
        else:
            patch.setattr("sqlspec.adapters.psycopg.core.execute_with_optional_parameters", MagicMock())
            source.start()
            source.close()
    exit_mock = transaction.__aexit__ if is_async else transaction.__exit__
    assert exit_mock.call_count == 1


@pytest.mark.anyio
async def test_asyncpg_finalize_error_closes_own_connection_only() -> None:
    owner = MagicMock()
    owner.transaction.return_value = MagicMock(start=AsyncMock(), commit=AsyncMock(side_effect=RuntimeError("boom")))
    owner.cursor = AsyncMock(return_value=MagicMock(_portal_name=None))
    owner.close = AsyncMock()
    next_owner = MagicMock()
    next_owner.close = AsyncMock()
    driver = make_driver(owner)
    source = AsyncpgStreamSource(driver, "select 1", (), 10)
    await source._start()
    with pytest.raises(RuntimeError, match="boom"):
        await source.close()
    assert owner.close.await_count == 1
    assert next_owner.close.await_count == 0


@pytest.mark.anyio
async def test_asyncpg_owned_close_commits_once_and_late_close_skips() -> None:
    transaction = MagicMock(start=AsyncMock(), commit=AsyncMock(), rollback=AsyncMock())
    owner = MagicMock()
    owner.transaction.return_value = transaction
    owner.cursor = AsyncMock(return_value=MagicMock(_portal_name=None))
    driver = make_driver(owner)
    owned = AsyncpgStreamSource(driver, "select 1", (), 10)
    await owned._start()
    await owned.close()
    assert transaction.commit.await_count == 1

    late = AsyncpgStreamSource(driver, "select 1", (), 10)
    await late._start()
    driver.connection = MagicMock()
    await late.close()
    assert transaction.commit.await_count == 1
    assert driver.connection.mock_calls == []


@pytest.mark.anyio
async def test_psqlpy_late_close_skips_foreign_transaction() -> None:
    transaction = MagicMock(begin=AsyncMock(), commit=AsyncMock(), rollback=AsyncMock())
    cursor = MagicMock(start=AsyncMock())
    transaction.cursor.return_value = cursor
    owner = MagicMock()
    owner.transaction.return_value = transaction
    next_owner = MagicMock()
    driver = make_driver(owner)
    source = PsqlpyStreamSource(driver, "select 1", None, 10)
    await source._start()
    driver.connection = next_owner
    await source.close()
    assert transaction.commit.await_count == 0
    assert transaction.rollback.await_count == 0
    assert next_owner.mock_calls == []


@pytest.mark.anyio
async def test_psqlpy_owned_close_commits_once() -> None:
    transaction = MagicMock(begin=AsyncMock(), commit=AsyncMock(), rollback=AsyncMock())
    transaction.cursor.return_value = MagicMock(start=AsyncMock())
    owner = MagicMock()
    owner.transaction.return_value = transaction
    source = PsqlpyStreamSource(make_driver(owner), "select 1", None, 10)
    await source._start()
    await source.close()
    assert transaction.commit.await_count == 1


def cockroach_underlying(*, is_async: bool) -> Any:
    underlying = MagicMock()
    underlying.start = AsyncMock() if is_async else MagicMock()
    underlying.close = AsyncMock() if is_async else MagicMock()
    return underlying


@pytest.mark.anyio
@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
async def test_cockroach_psycopg_late_close_skips_foreign_commit(is_async: bool) -> None:
    owner = MagicMock()
    driver = make_driver(owner, in_transaction=True, is_async=is_async)
    underlying = cockroach_underlying(is_async=is_async)
    if is_async:
        source = CockroachPsycopgAsyncStreamSource(driver, MagicMock(), underlying)
        await source.start()
        driver.connection = MagicMock()
        await source.close()
    else:
        source = CockroachPsycopgSyncStreamSource(driver, underlying, True)
        source.start()
        driver.connection = MagicMock()
        source.close()
    assert driver.commit.call_count == 0
    assert driver.rollback.call_count == 0


@pytest.mark.anyio
@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize("error", [False, True], ids=["clean", "error"])
async def test_cockroach_psycopg_owned_close_finalizes_follower_read(is_async: bool, error: bool) -> None:
    owner = MagicMock()
    driver = make_driver(owner, in_transaction=True, is_async=is_async)
    underlying = cockroach_underlying(is_async=is_async)
    if is_async:
        source = CockroachPsycopgAsyncStreamSource(driver, MagicMock(), underlying)
        await source.start()
        await source.close(error=error)
    else:
        source = CockroachPsycopgSyncStreamSource(driver, underlying, True)
        source.start()
        source.close(error=error)
    assert driver.commit.call_count == (0 if error else 1)
    assert driver.rollback.call_count == (1 if error else 0)


@pytest.mark.anyio
async def test_cockroach_asyncpg_late_close_skips_foreign_commit() -> None:
    driver = make_driver(MagicMock(), in_transaction=True)
    source = CockroachAsyncpgStreamSource(driver, MagicMock(), cockroach_underlying(is_async=True))
    await source.start()
    driver.connection = MagicMock()
    await source.close()
    assert driver.commit.await_count == 0
    assert driver.rollback.await_count == 0
