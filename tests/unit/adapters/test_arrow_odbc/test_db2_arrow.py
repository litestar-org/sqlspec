"""Unit tests for arrow-odbc Db2 adapter and native Arrow streaming."""

import pytest

pytest.importorskip("arrow_odbc")

from sqlspec.adapters.arrow_odbc import ArrowOdbcConfig, ArrowOdbcDriver
from sqlspec.core import LimitOffsetFilter
from sqlspec.exceptions import DeadlockError, QueryTimeoutError, UniqueViolationError
from tests.unit.adapters.test_arrow_odbc._db2_fakes import (
    DB2_CONNECTION_STRING,
    FakeArrowOdbcConnection,
    FakeOdbcError,
    as_connection,
    db2_error_message,
)


def _db2_driver(connection: FakeArrowOdbcConnection) -> ArrowOdbcDriver:
    config = ArrowOdbcConfig(connection_config={"connection_string": DB2_CONNECTION_STRING})
    return ArrowOdbcDriver(
        as_connection(connection), statement_config=config.statement_config, driver_features=config.driver_features
    )


def test_arrow_odbc_db2_select_to_arrow_streaming() -> None:
    """Verify ArrowOdbcDriver streams Arrow batches using db2 configuration."""
    connection = FakeArrowOdbcConnection()
    driver = _db2_driver(connection)

    result = driver.select_to_arrow("SELECT id, name FROM sysibm.sysdummy1")
    data = result.get_data()

    assert data.num_rows == 2
    assert data.to_pydict() == {"id": [1, 2], "name": ["Ada", "Grace"]}
    assert connection.read_calls[-1]["query"] == "SELECT id, name FROM sysibm.sysdummy1"


def test_arrow_odbc_db2_select_to_arrow_with_parameters() -> None:
    """Verify ArrowOdbcDriver compiles named parameters into positional ? markers for Db2."""
    connection = FakeArrowOdbcConnection()
    driver = _db2_driver(connection)

    driver.select_to_arrow("SELECT id, name FROM users WHERE id = :user_id", user_id=42)

    call = connection.read_calls[-1]
    assert call["query"] == "SELECT id, name FROM users WHERE id = ?"
    assert call["parameters"] == ["42"]


def test_arrow_odbc_db2_pagination_offset_fetch() -> None:
    """Verify ArrowOdbcDriver compiles LimitOffsetFilter to Db2 OFFSET FETCH syntax with positional parameters."""
    connection = FakeArrowOdbcConnection()
    driver = _db2_driver(connection)

    driver.select_to_arrow("SELECT id, name FROM users", LimitOffsetFilter(limit=10, offset=5))

    call = connection.read_calls[-1]
    assert "OFFSET ? ROWS FETCH NEXT ? ROWS ONLY" in call["query"]
    assert call["parameters"] == ["5", "10"]


def test_arrow_odbc_db2_batch_sizes_forwarded() -> None:
    """Verify batch_size parameter is forwarded to connection read_arrow_batches."""
    connection = FakeArrowOdbcConnection()
    driver = _db2_driver(connection)

    driver.select_to_arrow("SELECT * FROM users", batch_size=250)

    call = connection.read_calls[-1]
    assert call["batch_size"] == 250


@pytest.mark.parametrize(
    ("sqlstate", "expected_exception"),
    [("23505", UniqueViolationError), ("40001", DeadlockError), ("57014", QueryTimeoutError)],
)
def test_arrow_odbc_db2_error_mapping_on_select_to_arrow(
    monkeypatch: pytest.MonkeyPatch, sqlstate: str, expected_exception: type[Exception]
) -> None:
    """Verify Db2 ODBC errors during select_to_arrow map to specific SQLSpec exceptions."""
    monkeypatch.setattr("sqlspec.adapters.arrow_odbc.driver.ArrowOdbcError", FakeOdbcError)
    connection = FakeArrowOdbcConnection(error=FakeOdbcError(db2_error_message(sqlstate, -803)))
    driver = _db2_driver(connection)

    with pytest.raises(expected_exception):
        driver.select_to_arrow("SELECT * FROM users")
