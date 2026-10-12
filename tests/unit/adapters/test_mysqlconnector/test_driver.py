"""Unit tests for mysql-connector driver transaction behavior."""

from types import SimpleNamespace
from typing import Any, cast

import pytest

from sqlspec.adapters.mysqlconnector._typing import MysqlConnectorError
from sqlspec.adapters.mysqlconnector.driver import MysqlConnectorAsyncDriver, MysqlConnectorSyncDriver
from sqlspec.exceptions import DeadlockError


class _InvalidBoolean:
    def __bool__(self) -> bool:
        raise RuntimeError("invalid boolean")


@pytest.mark.parametrize("driver_cls", [MysqlConnectorSyncDriver, MysqlConnectorAsyncDriver])
def test_connection_in_transaction_propagates_invalid_boolean(driver_cls: type[Any]) -> None:
    connection = SimpleNamespace(in_transaction=_InvalidBoolean())
    driver = driver_cls(cast(Any, connection))

    with pytest.raises(RuntimeError, match="invalid boolean"):
        driver._connection_in_transaction()


@pytest.mark.parametrize("driver_cls", [MysqlConnectorSyncDriver, MysqlConnectorAsyncDriver])
def test_connection_in_transaction_does_not_infer_from_autocommit(driver_cls: type[Any]) -> None:
    """Disabled autocommit suggests a transaction, but only the connector's own flag decides."""
    connection = SimpleNamespace(autocommit=False, in_transaction=False)
    driver = driver_cls(cast(Any, connection))

    assert driver._connection_in_transaction() is False


def test_sync_commit_maps_mysqlconnector_deadlock_error() -> None:
    def _fail_commit() -> None:
        raise MysqlConnectorError(errno=1213, msg="Deadlock found when trying to get lock")

    connection = SimpleNamespace(commit=_fail_commit, in_transaction=True)
    driver = MysqlConnectorSyncDriver(cast(Any, connection))
    with pytest.raises(DeadlockError):
        driver.commit()
