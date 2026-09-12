"""Nested transaction block behaviors shared by adapter contract tests."""

import contextlib
from typing import Any, cast

import pytest

from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.service import SQLSpecAsyncService, SQLSpecSyncService
from tests.integration.adapters._shared._cases import DriverCase

_ROWS = {name: (f"nested-{name}", value, None) for name, value in zip("abcdefgh", range(10, 90, 10), strict=True)}


class NestedBlockError(Exception):
    """Raised inside a nested block to trigger its rollback."""


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

    sync_driver.execute(table.delete_sql)
    sync_driver.commit()
    try:
        if not case.supports_savepoints:
            with service.begin_transaction() as session:
                with pytest.raises(ImproperConfigurationError, match="savepoint"):
                    with service.begin_transaction():
                        pytest.fail("nested block entered without savepoint support")
                session.execute(table.insert_qmark_sql, _ROWS["a"])
            with pytest.raises(NotImplementedError):
                with sync_driver.transaction():
                    with sync_driver.transaction():
                        pytest.fail("nested block entered without savepoint support")
            assert _sync_committed_names(sync_driver, case) == _expected("a")
            return

        with service.begin_transaction() as session:
            with service.begin_transaction():
                session.execute(table.insert_qmark_sql, _ROWS["a"])
            with pytest.raises(NestedBlockError):
                with service.begin_transaction():
                    session.execute(table.insert_qmark_sql, _ROWS["b"])
                    raise NestedBlockError
            with service.begin_transaction():
                session.execute(table.insert_qmark_sql, _ROWS["c"])
            assert _sync_names(session, case) == _expected("a", "c")
        assert _sync_committed_names(sync_driver, case) == _expected("a", "c")

        with pytest.raises(NestedBlockError):
            with service.begin_transaction() as session:
                with service.begin_transaction():
                    session.execute(table.insert_qmark_sql, _ROWS["d"])
                raise NestedBlockError
        assert _sync_committed_names(sync_driver, case) == _expected("a", "c")

        with sync_driver.transaction():
            with sync_driver.transaction():
                sync_driver.execute(table.insert_qmark_sql, _ROWS["e"])
            with pytest.raises(NestedBlockError):
                with sync_driver.transaction():
                    sync_driver.execute(table.insert_qmark_sql, _ROWS["f"])
                    raise NestedBlockError
        assert _sync_committed_names(sync_driver, case) == _expected("a", "c", "e")

        with pytest.raises(NestedBlockError):
            with service.begin_transaction():
                with sync_driver.transaction():
                    sync_driver.execute(table.insert_qmark_sql, _ROWS["g"])
                raise NestedBlockError
        assert _sync_committed_names(sync_driver, case) == _expected("a", "c", "e")
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

    await async_driver.execute(table.delete_sql)
    await async_driver.commit()
    try:
        if not case.supports_savepoints:
            async with service.begin_transaction() as session:
                with pytest.raises(ImproperConfigurationError, match="savepoint"):
                    async with service.begin_transaction():
                        pytest.fail("nested block entered without savepoint support")
                await session.execute(table.insert_qmark_sql, _ROWS["a"])
            with pytest.raises(NotImplementedError):
                async with async_driver.transaction():
                    async with async_driver.transaction():
                        pytest.fail("nested block entered without savepoint support")
            assert await _async_committed_names(async_driver, case) == _expected("a")
            return

        async with service.begin_transaction() as session:
            async with service.begin_transaction():
                await session.execute(table.insert_qmark_sql, _ROWS["a"])
            with pytest.raises(NestedBlockError):
                async with service.begin_transaction():
                    await session.execute(table.insert_qmark_sql, _ROWS["b"])
                    raise NestedBlockError
            async with service.begin_transaction():
                await session.execute(table.insert_qmark_sql, _ROWS["c"])
            assert await _async_names(session, case) == _expected("a", "c")
        assert await _async_committed_names(async_driver, case) == _expected("a", "c")

        with pytest.raises(NestedBlockError):
            async with service.begin_transaction() as session:
                async with service.begin_transaction():
                    await session.execute(table.insert_qmark_sql, _ROWS["d"])
                raise NestedBlockError
        assert await _async_committed_names(async_driver, case) == _expected("a", "c")

        async with async_driver.transaction():
            async with async_driver.transaction():
                await async_driver.execute(table.insert_qmark_sql, _ROWS["e"])
            with pytest.raises(NestedBlockError):
                async with async_driver.transaction():
                    await async_driver.execute(table.insert_qmark_sql, _ROWS["f"])
                    raise NestedBlockError
        assert await _async_committed_names(async_driver, case) == _expected("a", "c", "e")

        with pytest.raises(NestedBlockError):
            async with service.begin_transaction():
                async with async_driver.transaction():
                    await async_driver.execute(table.insert_qmark_sql, _ROWS["g"])
                raise NestedBlockError
        assert await _async_committed_names(async_driver, case) == _expected("a", "c", "e")
    finally:
        with contextlib.suppress(Exception):
            await async_driver.rollback()
        with contextlib.suppress(Exception):
            await async_driver.execute(table.delete_sql)
            await async_driver.commit()
