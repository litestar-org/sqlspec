"""Oracle transaction-honesty: _connection_in_transaction reflects begin/commit/rollback."""

from typing import Any, cast

import pytest

from sqlspec.adapters.oracledb.driver import OracleAsyncDriver, OracleSyncDriver
from sqlspec.exceptions import TransactionError


class _FakeSyncConnection:
    def __init__(self, transaction_in_progress: bool = False) -> None:
        self.transaction_in_progress = transaction_in_progress

    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass


class _FakeAsyncConnection:
    def __init__(self, transaction_in_progress: bool = False) -> None:
        self.transaction_in_progress = transaction_in_progress

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


def test_oracle_sync_release_savepoint_validates_without_sql() -> None:
    """Oracle has no RELEASE SAVEPOINT statement, so release only validates the name."""
    driver = OracleSyncDriver(cast("Any", _FakeSyncConnection()))

    driver.release_savepoint("sqlspec_sp_1")
    with pytest.raises(TransactionError):
        driver.release_savepoint("sp; DROP TABLE users")


@pytest.mark.anyio
async def test_oracle_async_release_savepoint_validates_without_sql() -> None:
    """Oracle has no RELEASE SAVEPOINT statement, so release only validates the name."""
    driver = OracleAsyncDriver(cast("Any", _FakeAsyncConnection()))

    await driver.release_savepoint("sqlspec_sp_1")
    with pytest.raises(TransactionError):
        await driver.release_savepoint("sp; DROP TABLE users")


def test_sync_predicate_sees_an_implicitly_opened_transaction() -> None:
    """DML without an explicit begin still opens a transaction Oracle reports natively."""
    driver = OracleSyncDriver(cast("Any", _FakeSyncConnection(transaction_in_progress=True)))

    assert driver._connection_in_transaction() is True


async def test_async_predicate_sees_an_implicitly_opened_transaction() -> None:
    """The async twin must consult the same native signal."""
    driver = OracleAsyncDriver(cast("Any", _FakeAsyncConnection(transaction_in_progress=True)))

    assert driver._connection_in_transaction() is True


def test_sync_predicate_sees_an_explicit_begin_before_any_dml() -> None:
    """begin() issues no SQL, so only the ownership flag can report it."""
    driver = OracleSyncDriver(cast("Any", _FakeSyncConnection(transaction_in_progress=False)))
    driver.begin()

    assert driver._connection_in_transaction() is True


def test_execute_stack_does_not_commit_after_begin_without_dml() -> None:
    """Ownership after an explicit begin must not flip just because no DML has run."""
    driver = OracleSyncDriver(cast("Any", _FakeSyncConnection(transaction_in_progress=False)))
    driver.begin()

    assert driver._connection_in_transaction() is True

    driver.commit()

    assert driver._connection_in_transaction() is False
