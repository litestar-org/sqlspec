"""Unit tests for mysql-connector driver transaction behavior."""

from types import SimpleNamespace
from typing import Any, cast

import pytest

from sqlspec.adapters.mysqlconnector.driver import MysqlConnectorAsyncDriver, MysqlConnectorSyncDriver


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
    connection = SimpleNamespace(autocommit=False)
    driver = driver_cls(cast(Any, connection))

    assert driver._connection_in_transaction() is False
