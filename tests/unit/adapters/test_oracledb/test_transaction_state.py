"""Oracle transaction-honesty: _connection_in_transaction reflects begin/commit/rollback."""

from typing import Any, cast

import pytest

from sqlspec.adapters.oracledb.driver import OracleAsyncDriver, OracleSyncDriver


class _FakeSyncConnection:
    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass


class _FakeAsyncConnection:
    async def commit(self) -> None:
        pass

    async def rollback(self) -> None:
        pass


def test_oracle_sync_connection_in_transaction_tracks_begin_commit_rollback() -> None:
    """The sync transaction predicate should follow the real begin/commit/rollback state."""
    driver = OracleSyncDriver(cast("Any", _FakeSyncConnection()))
    assert driver._connection_in_transaction() is False

    driver.begin()
    assert driver._connection_in_transaction() is True
    driver.commit()
    assert driver._connection_in_transaction() is False

    driver.begin()
    assert driver._connection_in_transaction() is True
    driver.rollback()
    assert driver._connection_in_transaction() is False


@pytest.mark.anyio
async def test_oracle_async_connection_in_transaction_tracks_begin_commit_rollback() -> None:
    """The async transaction predicate should follow the real begin/commit/rollback state."""
    driver = OracleAsyncDriver(cast("Any", _FakeAsyncConnection()))
    assert driver._connection_in_transaction() is False

    await driver.begin()
    assert driver._connection_in_transaction() is True
    await driver.commit()
    assert driver._connection_in_transaction() is False

    await driver.begin()
    assert driver._connection_in_transaction() is True
    await driver.rollback()
    assert driver._connection_in_transaction() is False
