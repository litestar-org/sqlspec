"""ADBC transaction-honesty: _connection_in_transaction reflects begin/commit/rollback."""

from unittest.mock import MagicMock

from sqlspec.adapters.adbc.driver import AdbcDriver


def _driver() -> AdbcDriver:
    connection = MagicMock()
    connection.adbc_get_info.return_value = {"vendor_name": "sqlite", "driver_name": "sqlite"}
    return AdbcDriver(connection)


def test_adbc_connection_in_transaction_tracks_begin_commit_rollback() -> None:
    """The transaction predicate should follow the real begin/commit/rollback state."""
    driver = _driver()
    assert driver._connection_in_transaction() is False

    driver.begin()
    assert driver._connection_in_transaction() is True
    driver.commit()
    assert driver._connection_in_transaction() is False

    driver.begin()
    assert driver._connection_in_transaction() is True
    driver.rollback()
    assert driver._connection_in_transaction() is False
