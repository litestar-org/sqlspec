"""Unit tests for arrow-odbc Db2 adapter and native Arrow streaming."""

from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, cast

import pyarrow as pa
import pytest

pytest.importorskip("arrow_odbc")

from sqlspec.adapters.arrow_odbc import ArrowOdbcConfig, ArrowOdbcDriver
from sqlspec.core import LimitOffsetFilter
from sqlspec.exceptions import DeadlockError, QueryTimeoutError, UniqueViolationError

if TYPE_CHECKING:
    from sqlspec.adapters.arrow_odbc._typing import ArrowOdbcConnection


class FakeReader:
    """Minimal Arrow batch reader for Db2 arrow-odbc tests."""

    def __init__(self, table: pa.Table) -> None:
        self._batches = table.to_batches(max_chunksize=2)
        self.schema = table.schema

    def __iter__(self) -> Iterator[pa.RecordBatch]:
        return iter(self._batches)

    def into_pyarrow_record_batch_reader(self) -> pa.RecordBatchReader:
        return pa.RecordBatchReader.from_batches(self.schema, self._batches)


class FakeConnection:
    """Connection stub for Db2 arrow-odbc tests."""

    def __init__(self) -> None:
        self.closed = False
        self.commit_calls = 0
        self.rollback_calls = 0
        self.read_calls: list[dict[str, Any]] = []
        self.executed: list[tuple[str, Any]] = []

    def read_arrow_batches(self, **kwargs: Any) -> FakeReader:
        self.read_calls.append(kwargs)
        return FakeReader(pa.table({"id": [1, 2], "name": ["Ada", "Grace"]}))

    def execute(self, query: str, parameters: Any = None) -> None:
        self.executed.append((query, parameters))

    def commit(self) -> None:
        self.commit_calls += 1

    def rollback(self) -> None:
        self.rollback_calls += 1

    def close(self) -> None:
        self.closed = True


class FakeOdbcError(Exception):
    """Constructible stand-in for arrow_odbc.Error."""


class ErrorConnection(FakeConnection):
    """Connection stub that raises Db2 ODBC driver errors."""

    def __init__(self, sqlstate: str, native_error: int = -803) -> None:
        super().__init__()
        self.sqlstate = sqlstate
        self.native_error = native_error

    def read_arrow_batches(self, **kwargs: Any) -> FakeReader:
        raise FakeOdbcError(
            f"ODBC error: State: {self.sqlstate}, Native error: {self.native_error}, "
            "[IBM][CLI Driver][DB2/LINUXX8664] Error condition"
        )


@pytest.mark.parametrize(
    "conn_str",
    [
        "Driver={IBM DB2 ODBC DRIVER};Database=SAMPLE;Hostname=localhost;Port=50000;Protocol=TCPIP;",
        "Driver=/opt/ibm/clidriver/lib/libdb2o.so;Database=TESTDB;Hostname=db2;",
    ],
)
def test_arrow_odbc_db2_connection_string_configuration(conn_str: str) -> None:
    """Verify ArrowOdbcConfig resolves db2 dialect and qmark paramstyle from connection strings."""
    config = ArrowOdbcConfig(connection_config={"connection_string": conn_str})

    assert config.statement_config.dialect == "db2"
    assert config.statement_config.parameter_config.default_parameter_style == "qmark"
    assert config.driver_features.get("connection_string") == conn_str


def test_arrow_odbc_db2_select_to_arrow_streaming() -> None:
    """Verify ArrowOdbcDriver streams Arrow batches using db2 configuration."""
    connection = FakeConnection()
    config = ArrowOdbcConfig(
        connection_config={"connection_string": "Driver={IBM DB2 ODBC DRIVER};Database=SAMPLE;"}
    )
    driver = ArrowOdbcDriver(
        cast("ArrowOdbcConnection", connection),
        statement_config=config.statement_config,
        driver_features=config.driver_features,
    )

    result = driver.select_to_arrow("SELECT id, name FROM sysibm.sysdummy1")
    data = result.get_data()

    assert data.num_rows == 2
    assert data.to_pydict() == {"id": [1, 2], "name": ["Ada", "Grace"]}
    assert connection.read_calls[-1]["query"] == "SELECT id, name FROM sysibm.sysdummy1"


def test_arrow_odbc_db2_select_to_arrow_with_parameters() -> None:
    """Verify ArrowOdbcDriver compiles named parameters into positional ? markers for Db2."""
    connection = FakeConnection()
    config = ArrowOdbcConfig(
        connection_config={"connection_string": "Driver={IBM DB2 ODBC DRIVER};Database=SAMPLE;"}
    )
    driver = ArrowOdbcDriver(
        cast("ArrowOdbcConnection", connection),
        statement_config=config.statement_config,
        driver_features=config.driver_features,
    )

    driver.select_to_arrow("SELECT id, name FROM users WHERE id = :user_id", user_id=42)

    call = connection.read_calls[-1]
    assert call["query"] == "SELECT id, name FROM users WHERE id = ?"
    assert call["parameters"] == ["42"]


def test_arrow_odbc_db2_pagination_offset_fetch() -> None:
    """Verify ArrowOdbcDriver compiles LimitOffsetFilter to Db2 OFFSET FETCH syntax with positional parameters."""
    connection = FakeConnection()
    config = ArrowOdbcConfig(
        connection_config={"connection_string": "Driver={IBM DB2 ODBC DRIVER};Database=SAMPLE;"}
    )
    driver = ArrowOdbcDriver(
        cast("ArrowOdbcConnection", connection),
        statement_config=config.statement_config,
        driver_features=config.driver_features,
    )

    driver.select_to_arrow("SELECT id, name FROM users", LimitOffsetFilter(limit=10, offset=5))

    call = connection.read_calls[-1]
    assert "OFFSET ? ROWS FETCH NEXT ? ROWS ONLY" in call["query"]
    assert call["parameters"] == ["5", "10"]


def test_arrow_odbc_db2_batch_sizes_forwarded() -> None:
    """Verify batch_size parameter is forwarded to connection read_arrow_batches."""
    connection = FakeConnection()
    config = ArrowOdbcConfig(
        connection_config={"connection_string": "Driver={IBM DB2 ODBC DRIVER};Database=SAMPLE;"}
    )
    driver = ArrowOdbcDriver(
        cast("ArrowOdbcConnection", connection),
        statement_config=config.statement_config,
        driver_features=config.driver_features,
    )

    driver.select_to_arrow("SELECT * FROM users", batch_size=250)

    call = connection.read_calls[-1]
    assert call["batch_size"] == 250


@pytest.mark.parametrize(
    ("sqlstate", "expected_exception"),
    [
        ("23505", UniqueViolationError),
        ("40001", DeadlockError),
        ("57014", QueryTimeoutError),
    ],
)
def test_arrow_odbc_db2_error_mapping_on_select_to_arrow(
    monkeypatch: pytest.MonkeyPatch, sqlstate: str, expected_exception: type[Exception]
) -> None:
    """Verify Db2 ODBC errors during select_to_arrow map to specific SQLSpec exceptions."""
    monkeypatch.setattr("sqlspec.adapters.arrow_odbc.driver.ArrowOdbcError", FakeOdbcError)
    connection = ErrorConnection(sqlstate=sqlstate)
    config = ArrowOdbcConfig(
        connection_config={"connection_string": "Driver={IBM DB2 ODBC DRIVER};Database=SAMPLE;"}
    )
    driver = ArrowOdbcDriver(
        cast("ArrowOdbcConnection", connection),
        statement_config=config.statement_config,
        driver_features=config.driver_features,
    )

    with pytest.raises(expected_exception):
        driver.select_to_arrow("SELECT * FROM users")
