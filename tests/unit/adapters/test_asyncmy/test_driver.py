"""Unit tests for the asyncmy driver transaction-state contract."""

from typing import Any, cast

import pytest

from sqlspec.adapters.asyncmy.driver import AsyncmyDriver

pytest.importorskip("asyncmy", reason="asyncmy adapter requires the asyncmy package")


class _FakeConnection:
    def __init__(self, in_transaction: bool) -> None:
        self._in_transaction = in_transaction

    def get_transaction_status(self) -> bool:
        return self._in_transaction


@pytest.mark.parametrize("in_transaction", [True, False])
def test_connection_in_transaction_reflects_driver_state(in_transaction: bool) -> None:
    """_connection_in_transaction() must reflect the connection's real transaction status."""
    driver = AsyncmyDriver(connection=cast("Any", _FakeConnection(in_transaction)))
    assert driver._connection_in_transaction() is in_transaction
