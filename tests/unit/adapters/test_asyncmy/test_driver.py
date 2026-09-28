"""Unit tests for the asyncmy driver transaction-state contract."""

from typing import Any, cast

import pytest

from sqlspec.adapters.asyncmy.driver import AsyncmyDriver

pytest.importorskip("asyncmy", reason="asyncmy adapter requires the asyncmy package")


class _FakeConnection:
    def __init__(self, server_status: int) -> None:
        self.server_status = server_status


class _StatelessConnection:
    pass


@pytest.mark.parametrize(("server_status", "expected"), [(0, False), (1, True), (2, False), (3, True)])
def test_connection_in_transaction_reflects_driver_state(server_status: int, expected: bool) -> None:
    """_connection_in_transaction() must reflect the SERVER_STATUS_IN_TRANS bit on server_status."""
    driver = AsyncmyDriver(connection=cast("Any", _FakeConnection(server_status)))
    assert driver._connection_in_transaction() is expected


def test_connection_in_transaction_defaults_false_without_server_status() -> None:
    """_connection_in_transaction() returns False when connection has no server_status attribute."""
    driver = AsyncmyDriver(connection=cast("Any", _StatelessConnection()))
    assert driver._connection_in_transaction() is False
