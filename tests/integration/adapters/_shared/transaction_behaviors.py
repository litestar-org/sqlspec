"""Nested transaction block behaviors shared by adapter contract tests."""

import contextlib
from collections.abc import AsyncIterator, Iterator
from typing import Any, cast

import pytest

from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.service import SQLSpecAsyncService, SQLSpecSyncService
from tests.integration.adapters._shared._cases import DriverCase

_ROWS = {
    name: (f"nested-{name}", value, None) for name, value in zip("abcdefghijklmnop", range(10, 170, 10), strict=True)
}


class NestedBlockError(Exception):
    """Raised inside a nested block to trigger its rollback."""


class _SyncSessionConfig:
    """Config stand-in that lends one live driver to a config-built service."""

    is_async = False

    def __init__(self, driver: Any) -> None:
        self.driver = driver
        self.sessions = 0

    @contextlib.contextmanager
    def provide_session(self) -> Iterator[Any]:
        self.sessions += 1
        yield self.driver


class _AsyncSessionConfig:
    """Config stand-in that lends one live driver to a config-built service."""

    is_async = True

    def __init__(self, driver: Any) -> None:
        self.driver = driver
        self.sessions = 0

    @contextlib.asynccontextmanager
    async def provide_session(self) -> AsyncIterator[Any]:
        self.sessions += 1
        yield self.driver


def _sync_names(driver: Any, case: DriverCase) -> list[str]:
    return [
        row[0] for row in _ROWS.values() if driver.select_one_or_none(case.table.select_by_name_qmark_sql, (row[0],))
    ]


async def _async_names(driver: Any, case: DriverCase) -> list[str]:
    return [
        row[0]
        for row in _ROWS.values()
        if await driver.select_one_or_none(case.table.select_by_name_qmark_sql, (row[0],))
    ]


def _sync_committed_names(driver: Any, case: DriverCase) -> list[str]:
    names = _sync_names(driver, case)
    driver.commit()
    return names


async def _async_committed_names(driver: Any, case: DriverCase) -> list[str]:
    names = await _async_names(driver, case)
    await driver.commit()
    return names


def _expected(*keys: str) -> list[str]:
    return [_ROWS[key][0] for key in keys]


def assert_sync_nested_transaction_contract(driver: object, case: DriverCase) -> None:
    """Assert nested service and driver transaction blocks on a sync adapter."""
    if not case.supports_transactions:
        pytest.skip(f"{case.adapter} has no verified transaction support")
    sync_driver = cast("Any", driver)
    table = case.table
    service = SQLSpecSyncService(sync_driver)
    session_config = _SyncSessionConfig(sync_driver)
    config_service = SQLSpecSyncService(config=cast("Any", session_config))

    sync_driver.execute(table.delete_sql)
    sync_driver.commit()
    try:
        sync_driver.execute(table.insert_qmark_sql, _ROWS["a"])
        with sync_driver.transaction():
            sync_driver.execute(table.insert_qmark_sql, _ROWS["b"])
        assert _sync_committed_names(sync_driver, case) == _expected("a", "b")

        sync_driver.begin()
        sync_driver.execute(table.insert_qmark_sql, _ROWS["n"])
        with pytest.raises(NestedBlockError):
            with sync_driver.transaction():
                sync_driver.execute(table.insert_qmark_sql, _ROWS["d"])
                raise NestedBlockError
        assert _sync_committed_names(sync_driver, case) == _expected("a", "b")

        sync_driver.execute(table.insert_qmark_sql, _ROWS["o"])
        with service.begin_transaction():
            sync_driver.execute(table.insert_qmark_sql, _ROWS["p"])
        assert _sync_committed_names(sync_driver, case) == _expected("a", "b", "o", "p")

        sync_driver.begin()
        sync_driver.execute(table.insert_qmark_sql, _ROWS["n"])
        with pytest.raises(NestedBlockError):
            with service.begin_transaction():
                sync_driver.execute(table.insert_qmark_sql, _ROWS["d"])
                raise NestedBlockError
        assert _sync_committed_names(sync_driver, case) == _expected("a", "b", "o", "p")

        if not case.supports_savepoints:
            with service.begin_transaction() as session:
                with pytest.raises(ImproperConfigurationError, match="savepoint"):
                    with service.begin_transaction():
                        pytest.fail("nested block entered without savepoint support")
                session.execute(table.insert_qmark_sql, _ROWS["c"])
            with sync_driver.transaction():
                with pytest.raises(ImproperConfigurationError, match="savepoint"):
                    with sync_driver.transaction():
                        pytest.fail("nested block entered without savepoint support")
            with config_service.begin_transaction():
                with pytest.raises(ImproperConfigurationError, match="savepoint"):
                    with config_service.begin_transaction():
                        pytest.fail("nested block entered without savepoint support")
            assert _sync_committed_names(sync_driver, case) == _expected("a", "b", "c", "o", "p")
            return

        with service.begin_transaction() as session:
            with service.begin_transaction():
                session.execute(table.insert_qmark_sql, _ROWS["c"])
            with pytest.raises(NestedBlockError):
                with service.begin_transaction():
                    session.execute(table.insert_qmark_sql, _ROWS["d"])
                    raise NestedBlockError
            with service.begin_transaction():
                session.execute(table.insert_qmark_sql, _ROWS["e"])
            assert _sync_names(session, case) == _expected("a", "b", "c", "e", "o", "p")
        assert _sync_committed_names(sync_driver, case) == _expected("a", "b", "c", "e", "o", "p")

        with pytest.raises(NestedBlockError):
            with service.begin_transaction() as session:
                with service.begin_transaction():
                    session.execute(table.insert_qmark_sql, _ROWS["f"])
                raise NestedBlockError
        assert _sync_committed_names(sync_driver, case) == _expected("a", "b", "c", "e", "o", "p")

        with sync_driver.transaction():
            with sync_driver.transaction():
                sync_driver.execute(table.insert_qmark_sql, _ROWS["g"])
            with pytest.raises(NestedBlockError):
                with sync_driver.transaction():
                    sync_driver.execute(table.insert_qmark_sql, _ROWS["h"])
                    raise NestedBlockError
        assert _sync_committed_names(sync_driver, case) == _expected("a", "b", "c", "e", "g", "o", "p")

        with pytest.raises(NestedBlockError):
            with service.begin_transaction():
                with sync_driver.transaction():
                    sync_driver.execute(table.insert_qmark_sql, _ROWS["i"])
                raise NestedBlockError
        with pytest.raises(NestedBlockError):
            with sync_driver.transaction():
                with service.begin_transaction():
                    sync_driver.execute(table.insert_qmark_sql, _ROWS["j"])
                raise NestedBlockError
        with sync_driver.transaction():
            with pytest.raises(NestedBlockError):
                with service.begin_transaction():
                    sync_driver.execute(table.insert_qmark_sql, _ROWS["k"])
                    raise NestedBlockError
            sync_driver.execute(table.insert_qmark_sql, _ROWS["l"])
        assert _sync_committed_names(sync_driver, case) == _expected("a", "b", "c", "e", "g", "l", "o", "p")

        with config_service.begin_transaction() as session:
            with config_service.begin_transaction() as inner:
                assert inner is session
                session.execute(table.insert_qmark_sql, _ROWS["m"])
            with pytest.raises(NestedBlockError):
                with config_service.begin_transaction():
                    session.execute(table.insert_qmark_sql, _ROWS["n"])
                    raise NestedBlockError
        assert session_config.sessions == 1
        assert _sync_committed_names(sync_driver, case) == _expected("a", "b", "c", "e", "g", "l", "m", "o", "p")
    finally:
        with contextlib.suppress(Exception):
            sync_driver.rollback()
        with contextlib.suppress(Exception):
            sync_driver.execute(table.delete_sql)
            sync_driver.commit()


async def assert_async_nested_transaction_contract(driver: object, case: DriverCase) -> None:
    """Assert nested service and driver transaction blocks on an async adapter."""
    if not case.supports_transactions:
        pytest.skip(f"{case.adapter} has no verified transaction support")
    async_driver = cast("Any", driver)
    table = case.table
    service = SQLSpecAsyncService(async_driver)
    session_config = _AsyncSessionConfig(async_driver)
    config_service = SQLSpecAsyncService(config=cast("Any", session_config))

    await async_driver.execute(table.delete_sql)
    await async_driver.commit()
    try:
        await async_driver.execute(table.insert_qmark_sql, _ROWS["a"])
        async with async_driver.transaction():
            await async_driver.execute(table.insert_qmark_sql, _ROWS["b"])
        assert await _async_committed_names(async_driver, case) == _expected("a", "b")

        await async_driver.begin()
        await async_driver.execute(table.insert_qmark_sql, _ROWS["n"])
        with pytest.raises(NestedBlockError):
            async with async_driver.transaction():
                await async_driver.execute(table.insert_qmark_sql, _ROWS["d"])
                raise NestedBlockError
        assert await _async_committed_names(async_driver, case) == _expected("a", "b")

        await async_driver.execute(table.insert_qmark_sql, _ROWS["o"])
        async with service.begin_transaction():
            await async_driver.execute(table.insert_qmark_sql, _ROWS["p"])
        assert await _async_committed_names(async_driver, case) == _expected("a", "b", "o", "p")

        await async_driver.begin()
        await async_driver.execute(table.insert_qmark_sql, _ROWS["n"])
        with pytest.raises(NestedBlockError):
            async with service.begin_transaction():
                await async_driver.execute(table.insert_qmark_sql, _ROWS["d"])
                raise NestedBlockError
        assert await _async_committed_names(async_driver, case) == _expected("a", "b", "o", "p")

        if not case.supports_savepoints:
            async with service.begin_transaction() as session:
                with pytest.raises(ImproperConfigurationError, match="savepoint"):
                    async with service.begin_transaction():
                        pytest.fail("nested block entered without savepoint support")
                await session.execute(table.insert_qmark_sql, _ROWS["c"])
            async with async_driver.transaction():
                with pytest.raises(ImproperConfigurationError, match="savepoint"):
                    async with async_driver.transaction():
                        pytest.fail("nested block entered without savepoint support")
            async with config_service.begin_transaction():
                with pytest.raises(ImproperConfigurationError, match="savepoint"):
                    async with config_service.begin_transaction():
                        pytest.fail("nested block entered without savepoint support")
            assert await _async_committed_names(async_driver, case) == _expected("a", "b", "c", "o", "p")
            return

        async with service.begin_transaction() as session:
            async with service.begin_transaction():
                await session.execute(table.insert_qmark_sql, _ROWS["c"])
            with pytest.raises(NestedBlockError):
                async with service.begin_transaction():
                    await session.execute(table.insert_qmark_sql, _ROWS["d"])
                    raise NestedBlockError
            async with service.begin_transaction():
                await session.execute(table.insert_qmark_sql, _ROWS["e"])
            assert await _async_names(session, case) == _expected("a", "b", "c", "e", "o", "p")
        assert await _async_committed_names(async_driver, case) == _expected("a", "b", "c", "e", "o", "p")

        with pytest.raises(NestedBlockError):
            async with service.begin_transaction() as session:
                async with service.begin_transaction():
                    await session.execute(table.insert_qmark_sql, _ROWS["f"])
                raise NestedBlockError
        assert await _async_committed_names(async_driver, case) == _expected("a", "b", "c", "e", "o", "p")

        async with async_driver.transaction():
            async with async_driver.transaction():
                await async_driver.execute(table.insert_qmark_sql, _ROWS["g"])
            with pytest.raises(NestedBlockError):
                async with async_driver.transaction():
                    await async_driver.execute(table.insert_qmark_sql, _ROWS["h"])
                    raise NestedBlockError
        assert await _async_committed_names(async_driver, case) == _expected("a", "b", "c", "e", "g", "o", "p")

        with pytest.raises(NestedBlockError):
            async with service.begin_transaction():
                async with async_driver.transaction():
                    await async_driver.execute(table.insert_qmark_sql, _ROWS["i"])
                raise NestedBlockError
        with pytest.raises(NestedBlockError):
            async with async_driver.transaction():
                async with service.begin_transaction():
                    await async_driver.execute(table.insert_qmark_sql, _ROWS["j"])
                raise NestedBlockError
        async with async_driver.transaction():
            with pytest.raises(NestedBlockError):
                async with service.begin_transaction():
                    await async_driver.execute(table.insert_qmark_sql, _ROWS["k"])
                    raise NestedBlockError
            await async_driver.execute(table.insert_qmark_sql, _ROWS["l"])
        assert await _async_committed_names(async_driver, case) == _expected("a", "b", "c", "e", "g", "l", "o", "p")

        async with config_service.begin_transaction() as session:
            async with config_service.begin_transaction() as inner:
                assert inner is session
                await session.execute(table.insert_qmark_sql, _ROWS["m"])
            with pytest.raises(NestedBlockError):
                async with config_service.begin_transaction():
                    await session.execute(table.insert_qmark_sql, _ROWS["n"])
                    raise NestedBlockError
        assert session_config.sessions == 1
        assert await _async_committed_names(async_driver, case) == _expected(
            "a", "b", "c", "e", "g", "l", "m", "o", "p"
        )
    finally:
        with contextlib.suppress(Exception):
            await async_driver.rollback()
        with contextlib.suppress(Exception):
            await async_driver.execute(table.delete_sql)
            await async_driver.commit()
