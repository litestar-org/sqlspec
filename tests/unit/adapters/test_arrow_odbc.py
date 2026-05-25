"""Unit tests for the arrow_odbc adapter."""

from typing import Any, cast

import pyarrow as pa
import pytest

pytest.importorskip("arrow_odbc")

from sqlspec.adapters.arrow_odbc import ArrowOdbcConfig, ArrowOdbcDriver, resolve_dialect_from_dbms_name
from sqlspec.exceptions import SQLSpecError


class FakeReader:
    """Minimal Arrow batch reader."""

    def __init__(self, table: pa.Table) -> None:
        self._batches = table.to_batches(max_chunksize=2)
        self.schema = table.schema

    def __iter__(self) -> Any:
        return iter(self._batches)


class FakeConnection:
    """Connection stub for arrow_odbc driver tests."""

    def __init__(self) -> None:
        self.closed = False
        self.read_calls: list[dict[str, Any]] = []
        self.insert_calls: list[tuple[str, int, pa.Table]] = []
        self.executed: list[tuple[str, Any]] = []

    def read_arrow_batches(self, **kwargs: Any) -> FakeReader:
        self.read_calls.append(kwargs)
        return FakeReader(pa.table({"x": [1, 2, 3]}))

    def from_table_to_db(self, source: pa.Table, target: str, chunk_size: int = 1000) -> None:
        self.insert_calls.append((target, chunk_size, source))

    def execute(self, query: str, parameters: Any = None) -> None:
        self.executed.append((query, parameters))

    def commit(self) -> None:
        return None

    def rollback(self) -> None:
        return None

    def close(self) -> None:
        self.closed = True


class NoCloseConnection:
    """Connection stub matching arrow-odbc 10.4's no-close public surface."""

    dbms_name = "Microsoft SQL Server"


class FakeOdbcError(Exception):
    """Constructible stand-in for arrow_odbc.Error."""


class ErrorConnection(FakeConnection):
    """Connection stub that raises an ODBC driver error."""

    def read_arrow_batches(self, **kwargs: Any) -> FakeReader:
        raise FakeOdbcError("read failed")

    def from_table_to_db(self, source: pa.Table, target: str, chunk_size: int = 1000) -> None:
        raise FakeOdbcError("insert failed")


def test_resolve_dialect_from_dbms_name() -> None:
    """ODBC DBMS names and driver strings should map to SQLSpec dialects."""
    assert resolve_dialect_from_dbms_name("Microsoft SQL Server") == "mssql"
    assert resolve_dialect_from_dbms_name("ODBC Driver 18 for SQL Server") == "mssql"
    assert resolve_dialect_from_dbms_name("Oracle in instantclient_19_24") == "oracle"
    assert resolve_dialect_from_dbms_name("MySQL ODBC 8.0 Unicode Driver") == "mysql"


def test_arrow_odbc_select_to_arrow_uses_native_reader() -> None:
    """select_to_arrow should materialize batches returned by arrow-odbc."""
    connection = FakeConnection()
    driver = ArrowOdbcDriver(
        cast("Any", connection),
        driver_features={"connection_string": "Driver={ODBC Driver 18 for SQL Server};", "chunk_size": 2},
    )

    result = driver.select_to_arrow("SELECT 1 AS x")

    assert driver.dialect == "tsql"
    assert driver.data_dictionary.get_dialect_config().name == "mssql"
    assert result.get_data().num_rows == 3
    assert connection.read_calls[0]["query"] == "SELECT 1 AS x"
    assert connection.read_calls[0]["batch_size"] == 2


def test_arrow_odbc_select_to_arrow_batches_returns_batches() -> None:
    """select_to_arrow should use SQLSpec's canonical batches return format."""
    connection = FakeConnection()
    driver = ArrowOdbcDriver(cast("Any", connection), driver_features={"chunk_size": 2})

    result = driver.select_to_arrow("SELECT 1 AS x", return_format="batches", batch_size=2)
    batches = result.get_data()

    assert [batch.num_rows for batch in batches] == [2, 1]


def test_arrow_odbc_select_to_arrow_raises_mapped_driver_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    """Native read failures should use SQLSpec's deferred exception mapping."""
    monkeypatch.setattr("sqlspec.adapters.arrow_odbc.driver.ArrowOdbcError", FakeOdbcError)
    connection = ErrorConnection()
    driver = ArrowOdbcDriver(cast("Any", connection), driver_features={"chunk_size": 2})

    with pytest.raises(SQLSpecError, match="ODBC database error"):
        driver.select_to_arrow("SELECT 1 AS x")


def test_arrow_odbc_bulk_insert_arrow_uses_from_table_to_db() -> None:
    """bulk_insert_arrow should use the table-specific write API when given a Table."""
    connection = FakeConnection()
    driver = ArrowOdbcDriver(cast("Any", connection), driver_features={"chunk_size": 10})
    table = pa.table({"x": [1, 2, 3]})

    driver.bulk_insert_arrow("dbo.target", table)

    assert connection.insert_calls == [("dbo.target", 10, table)]


def test_arrow_odbc_bulk_insert_arrow_raises_mapped_driver_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    """Native write failures should not be swallowed by the deferred exception handler."""
    monkeypatch.setattr("sqlspec.adapters.arrow_odbc.driver.ArrowOdbcError", FakeOdbcError)
    connection = ErrorConnection()
    driver = ArrowOdbcDriver(cast("Any", connection), driver_features={"chunk_size": 10})

    with pytest.raises(SQLSpecError, match="ODBC database error"):
        driver.bulk_insert_arrow("dbo.target", pa.table({"x": [1]}))


def test_arrow_odbc_config_connects_with_verified_keyword_names(monkeypatch: Any) -> None:
    """Config should call arrow_odbc.connect with 10.4 keyword names."""
    calls: list[tuple[str, dict[str, Any]]] = []
    connection = FakeConnection()

    def fake_connect(connection_string: str, **kwargs: Any) -> FakeConnection:
        calls.append((connection_string, kwargs))
        return connection

    monkeypatch.setattr("sqlspec.adapters.arrow_odbc.config.arrow_odbc_connect", fake_connect)

    config = ArrowOdbcConfig(
        connection_config={
            "connection_string": "Driver={ODBC Driver 18 for SQL Server};",
            "login_timeout": 3,
            "autocommit": False,
        }
    )

    with config.provide_session() as session:
        assert isinstance(session, ArrowOdbcDriver)

    assert calls == [("Driver={ODBC Driver 18 for SQL Server};", {"login_timeout_sec": 3, "autocommit": False})]
    assert connection.closed is True


def test_arrow_odbc_session_release_allows_connections_without_close(monkeypatch: Any) -> None:
    """arrow-odbc 10.4 Connection has no close() method, so release should be a no-op."""
    connection = NoCloseConnection()

    monkeypatch.setattr("sqlspec.adapters.arrow_odbc.config.arrow_odbc_connect", lambda *_args, **_kwargs: connection)

    config = ArrowOdbcConfig(connection_config={"connection_string": "Driver={ODBC Driver 18 for SQL Server};"})

    with config.provide_session() as session:
        assert isinstance(session, ArrowOdbcDriver)


def test_arrow_odbc_field_config_uses_driver_name_for_dialect(monkeypatch: Any) -> None:
    """Field-based configs should still infer dialect from the ODBC driver name."""
    connection = NoCloseConnection()

    monkeypatch.setattr("sqlspec.adapters.arrow_odbc.config.arrow_odbc_connect", lambda *_args, **_kwargs: connection)

    config = ArrowOdbcConfig(
        connection_config={"driver": "ODBC Driver 18 for SQL Server", "server": "localhost", "database": "app"}
    )

    with config.provide_session() as session:
        assert session.dialect == "tsql"


def test_arrow_odbc_mssql_driver_uses_tsql_statement_dialect() -> None:
    """SQL Server ODBC connections should compile with sqlglot's tsql dialect."""
    connection = FakeConnection()
    driver = ArrowOdbcDriver(cast("Any", connection), driver_features={"dbms_name": "Microsoft SQL Server"})

    prepared = driver.prepare_statement("SELECT :value", (), kwargs={"value": 1})
    sql, parameters = driver._get_compiled_sql(prepared, driver.statement_config)

    assert driver.dialect == "tsql"
    assert driver.statement_config.dialect == "tsql"
    assert sql == "SELECT ?"
    assert parameters == (1,)
