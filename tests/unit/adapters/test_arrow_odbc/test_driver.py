"""Unit tests for the arrow_odbc adapter."""

from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, cast

import pyarrow as pa
import pytest

pytest.importorskip("arrow_odbc")

from sqlspec.adapters.arrow_odbc import (
    ArrowOdbcConfig,
    ArrowOdbcConnectionParams,
    ArrowOdbcDriver,
    ArrowOdbcDriverFeatures,
    build_connection_config,
    create_mapped_exception,
    odbc_type_to_arrow,
    resolve_dialect_from_dbms_name,
)
from sqlspec.adapters.arrow_odbc.data_dictionary import ArrowOdbcDataDictionary
from sqlspec.core import LimitOffsetFilter, OrderByFilter
from sqlspec.exceptions import SQLFileNotFoundError, SQLParsingError, SQLSpecError

if TYPE_CHECKING:
    from sqlspec.adapters.arrow_odbc._typing import ArrowOdbcConnection


class FakeReader:
    """Minimal Arrow batch reader."""

    def __init__(self, table: pa.Table) -> None:
        self._batches = table.to_batches(max_chunksize=2)
        self.schema = table.schema

    def __iter__(self) -> Iterator[pa.RecordBatch]:
        return iter(self._batches)

    def into_pyarrow_record_batch_reader(self) -> pa.RecordBatchReader:
        return pa.RecordBatchReader.from_batches(self.schema, self._batches)


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
        self.read_calls.append(kwargs)
        raise FakeOdbcError("read failed")

    def from_table_to_db(self, source: pa.Table, target: str, chunk_size: int = 1000) -> None:
        raise FakeOdbcError("insert failed")


def _empty_table_reader() -> FakeReader:
    return FakeReader(pa.table({"id": pa.array([], type=pa.int64()), "name": pa.array([], type=pa.string())}))


def test_get_columns_probes_arrow_schema_when_query_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    """Missing bundled column SQL should fall back to a zero-row Arrow schema probe."""
    connection = FakeConnection()
    driver = ArrowOdbcDriver(cast("ArrowOdbcConnection", connection), driver_features={"chunk_size": 2})

    def read_arrow_batches(**kwargs: Any) -> FakeReader:
        connection.read_calls.append(kwargs)
        return _empty_table_reader()

    connection.read_arrow_batches = read_arrow_batches  # type: ignore[assignment]

    def fail_get_query(self: ArrowOdbcDataDictionary, name: str) -> Any:
        raise SQLFileNotFoundError(name)

    monkeypatch.setattr(ArrowOdbcDataDictionary, "get_query", fail_get_query)

    result = driver.data_dictionary.get_columns(driver, table="items")

    assert [entry["data_type"] for entry in result] == ["BIGINT", "VARCHAR"]
    assert [entry["ordinal_position"] for entry in result] == [1, 2]
    assert connection.read_calls[-1]["query"] == 'SELECT * FROM "items" WHERE 1=0'


def test_get_columns_probe_quotes_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    """Schema-qualified probes should quote both schema and table identifiers."""
    connection = FakeConnection()
    driver = ArrowOdbcDriver(cast("ArrowOdbcConnection", connection), driver_features={"chunk_size": 2})

    def read_arrow_batches(**kwargs: Any) -> FakeReader:
        connection.read_calls.append(kwargs)
        return _empty_table_reader()

    connection.read_arrow_batches = read_arrow_batches  # type: ignore[assignment]

    def fail_get_query(self: ArrowOdbcDataDictionary, name: str) -> Any:
        raise SQLFileNotFoundError(name)

    monkeypatch.setattr(ArrowOdbcDataDictionary, "get_query", fail_get_query)

    result = driver.data_dictionary.get_columns(driver, table="items", schema="dbo")

    assert [entry["schema_name"] for entry in result] == ["dbo", "dbo"]
    assert connection.read_calls[-1]["query"] == 'SELECT * FROM "dbo"."items" WHERE 1=0'


def test_get_columns_schema_wide_does_not_probe(monkeypatch: pytest.MonkeyPatch) -> None:
    """Schema-wide missing SQL should stay on the empty SQL fallback and skip probing."""
    connection = FakeConnection()
    driver = ArrowOdbcDriver(cast("ArrowOdbcConnection", connection), driver_features={"chunk_size": 2})

    def fail_get_query(self: ArrowOdbcDataDictionary, name: str) -> Any:
        raise SQLFileNotFoundError(name)

    monkeypatch.setattr(ArrowOdbcDataDictionary, "get_query", fail_get_query)

    result = driver.data_dictionary.get_columns(driver)

    assert result == []
    assert connection.read_calls == []


def test_get_columns_probe_failure_returns_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    """Probe failures should preserve the existing empty-result contract."""
    connection = ErrorConnection()
    driver = ArrowOdbcDriver(cast("ArrowOdbcConnection", connection), driver_features={"chunk_size": 2})

    def fail_get_query(self: ArrowOdbcDataDictionary, name: str) -> Any:
        raise SQLFileNotFoundError(name)

    monkeypatch.setattr(ArrowOdbcDataDictionary, "get_query", fail_get_query)

    assert driver.data_dictionary.get_columns(driver, table="items") == []
    assert connection.read_calls[-1]["query"] == 'SELECT * FROM "items" WHERE 1=0'


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
        cast("ArrowOdbcConnection", connection),
        driver_features={"connection_string": "Driver={ODBC Driver 18 for SQL Server};", "chunk_size": 2},
    )

    result = driver.select_to_arrow("SELECT 1 AS x")

    assert driver.dialect == "tsql"
    assert driver.data_dictionary.get_dialect_config().name == "mssql"
    assert result.get_data().num_rows == 3
    assert connection.read_calls[0]["query"] == "SELECT 1 AS x"
    assert connection.read_calls[0]["batch_size"] == 2


def test_arrow_odbc_select_to_arrow_precompiles_prepared_statement(monkeypatch: pytest.MonkeyPatch) -> None:
    connection = FakeConnection()
    driver = ArrowOdbcDriver(
        cast("ArrowOdbcConnection", connection),
        driver_features={"connection_string": "Driver={ODBC Driver 18 for SQL Server};", "chunk_size": 2},
    )
    original = ArrowOdbcDriver._get_compiled_sql
    captured: list[bool] = []

    def get_compiled_sql(self: ArrowOdbcDriver, statement: Any, config: Any) -> Any:
        captured.append(statement.is_processed)
        return original(self, statement, config)

    monkeypatch.setattr(ArrowOdbcDriver, "_get_compiled_sql", get_compiled_sql)

    driver.select_to_arrow("SELECT 1 AS x")

    assert captured == [True]


def test_arrow_odbc_select_to_arrow_batches_returns_batches() -> None:
    """select_to_arrow should use SQLSpec's canonical batches return format."""
    connection = FakeConnection()
    driver = ArrowOdbcDriver(cast("ArrowOdbcConnection", connection), driver_features={"chunk_size": 2})

    result = driver.select_to_arrow("SELECT 1 AS x", return_format="batches", batch_size=2)
    batches = result.get_data()

    assert [batch.num_rows for batch in batches] == [2, 1]


def test_arrow_odbc_select_to_arrow_reader_skips_table_materialization(monkeypatch: pytest.MonkeyPatch) -> None:
    """reader formats should transfer the native BatchReader without _reader_to_table."""
    connection = FakeConnection()
    driver = ArrowOdbcDriver(cast("ArrowOdbcConnection", connection), driver_features={"chunk_size": 2})

    def fail_reader_to_table(_reader: object) -> pa.Table:
        msg = "reader formats should not materialize via _reader_to_table"
        raise AssertionError(msg)

    monkeypatch.setattr("sqlspec.adapters.arrow_odbc.driver._reader_to_table", fail_reader_to_table)

    result = driver.select_to_arrow("SELECT 1 AS x", return_format="reader")

    assert result.rows_affected == -1
    assert result.get_data().read_all().to_pydict() == {"x": [1, 2, 3]}


def test_arrow_odbc_select_to_arrow_raises_mapped_driver_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    """Native read failures should use SQLSpec's deferred exception mapping."""
    monkeypatch.setattr("sqlspec.adapters.arrow_odbc.driver.ArrowOdbcError", FakeOdbcError)
    connection = ErrorConnection()
    driver = ArrowOdbcDriver(cast("ArrowOdbcConnection", connection), driver_features={"chunk_size": 2})

    with pytest.raises(SQLSpecError, match="ODBC database error"):
        driver.select_to_arrow("SELECT 1 AS x")


def test_arrow_odbc_bulk_insert_arrow_uses_from_table_to_db() -> None:
    """bulk_insert_arrow should use the table-specific write API when given a Table."""
    connection = FakeConnection()
    driver = ArrowOdbcDriver(cast("ArrowOdbcConnection", connection), driver_features={"chunk_size": 10})
    table = pa.table({"x": [1, 2, 3]})

    driver.bulk_insert_arrow("dbo.target", table)

    assert connection.insert_calls == [("dbo.target", 10, table)]


def test_arrow_odbc_bulk_insert_arrow_raises_mapped_driver_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    """Native write failures should not be swallowed by the deferred exception handler."""
    monkeypatch.setattr("sqlspec.adapters.arrow_odbc.driver.ArrowOdbcError", FakeOdbcError)
    connection = ErrorConnection()
    driver = ArrowOdbcDriver(cast("ArrowOdbcConnection", connection), driver_features={"chunk_size": 10})

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


def test_arrow_odbc_config_runs_connection_create_callback(monkeypatch: Any) -> None:
    """arrow-odbc should run the connection hook when it creates a physical connection."""
    connection = FakeConnection()
    seen: list[Any] = []

    def fake_connect(_connection_string: str, **_kwargs: Any) -> FakeConnection:
        return connection

    monkeypatch.setattr("sqlspec.adapters.arrow_odbc.config.arrow_odbc_connect", fake_connect)

    config = ArrowOdbcConfig(
        connection_config={"connection_string": "Driver={ODBC Driver 18 for SQL Server};"},
        driver_features={"on_connection_create": seen.append},
    )

    assert config.create_connection() is connection
    assert seen == [connection]
    assert "on_connection_create" not in config.driver_features


def test_arrow_odbc_connection_params_declares_routed_security_keys() -> None:
    """Connection params should type the ODBC security keys routed by core."""
    expected_keys = {"trusted_connection", "trust_server_certificate", "encrypt"}

    assert expected_keys.issubset(ArrowOdbcConnectionParams.__annotations__)


def test_arrow_odbc_driver_features_declares_json_serializers() -> None:
    """Driver features should type the JSON hooks already defaulted by core."""
    expected_keys = {"json_serializer", "json_deserializer"}

    assert expected_keys.issubset(ArrowOdbcDriverFeatures.__annotations__)


def test_arrow_odbc_build_connection_config_formats_security_fields_and_kwargs() -> None:
    """Routed security fields should become canonical ODBC string keys."""
    connection_string, kwargs = build_connection_config({
        "driver": "ODBC Driver 18 for SQL Server",
        "server": "localhost",
        "database": "app",
        "uid": "sa",
        "pwd": "secret",
        "trusted_connection": True,
        "trust_server_certificate": True,
        "encrypt": "mandatory",
        "login_timeout": 7,
        "packet_size": 8192,
        "autocommit": True,
    })

    assert connection_string == (
        "Driver=ODBC Driver 18 for SQL Server;Server=localhost;Database=app;UID=sa;PWD=secret;"
        "Trusted_Connection=yes;TrustServerCertificate=yes;Encrypt=mandatory;"
    )
    assert kwargs == {"login_timeout_sec": 7, "packet_size": 8192, "autocommit": True}


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


def test_arrow_odbc_config_init_no_pre_super_assign_connection_string() -> None:
    """Connection-string input should normalize and populate driver features."""
    connection_string = "Driver={ODBC Driver 18 for SQL Server};Server=localhost;Database=db;"

    config = ArrowOdbcConfig(connection_config={"connection_string": connection_string})

    assert config.connection_config == {"connection_string": connection_string}
    assert config.driver_features.get("connection_string") == connection_string
    assert config.statement_config.dialect == "tsql"


def test_arrow_odbc_config_init_no_pre_super_assign_driver_key() -> None:
    """Driver-key input should populate dbms_name without early slot writes."""
    config = ArrowOdbcConfig(
        connection_config={"driver": "ODBC Driver 17 for SQL Server", "server": "myhost", "database": "mydb"}
    )

    assert config.connection_config["driver"] == "ODBC Driver 17 for SQL Server"
    assert config.driver_features.get("dbms_name") == "ODBC Driver 17 for SQL Server"
    assert config.statement_config.dialect == "tsql"


def test_arrow_odbc_config_init_no_pre_super_assign_none_input() -> None:
    """None input should normalize to an empty connection_config dict."""
    config = ArrowOdbcConfig(connection_config=None)

    assert config.connection_config == {}


def test_arrow_odbc_mssql_driver_uses_tsql_statement_dialect() -> None:
    """SQL Server ODBC connections should compile with sqlglot's tsql dialect."""
    connection = FakeConnection()
    driver = ArrowOdbcDriver(
        cast("ArrowOdbcConnection", connection), driver_features={"dbms_name": "Microsoft SQL Server"}
    )

    prepared = driver.prepare_statement("SELECT :value", (), kwargs={"value": 1})
    sql, parameters = driver._get_compiled_sql(prepared, driver.statement_config)

    assert driver.dialect == "tsql"
    assert driver.statement_config.dialect == "tsql"
    assert sql == "SELECT ?"
    assert parameters == (1,)


def test_arrow_odbc_mssql_pagination_inlines_offset_fetch_integers() -> None:
    """SQL Server ODBC requires literal integer OFFSET/FETCH control values."""
    connection = FakeConnection()
    driver = ArrowOdbcDriver(
        cast("ArrowOdbcConnection", connection), driver_features={"dbms_name": "Microsoft SQL Server"}
    )

    driver.execute("SELECT name, value FROM dbo.items", OrderByFilter("value", "desc"), LimitOffsetFilter(2, 1))

    call = connection.read_calls[-1]
    assert "OFFSET 1 ROWS FETCH FIRST 2 ROWS ONLY" in call["query"]
    assert call["parameters"] is None


def test_arrow_odbc_mssql_syntax_error_maps_to_sql_parsing_error() -> None:
    """SQL Server syntax errors should satisfy the shared parsing-error contract."""
    mapped = create_mapped_exception(
        FakeOdbcError(
            "ODBC emitted an error calling 'SQLExecDirect': State: 42000, Native error: 102, "
            "Message: [Microsoft][ODBC Driver 18 for SQL Server][SQL Server]Incorrect syntax near '*'."
        )
    )

    assert isinstance(mapped, SQLParsingError)


def test_arrow_odbc_driver_dialect_set_from_dbms_name() -> None:
    """The public dialect slot stores the resolved statement dialect without a private mirror slot."""
    connection = FakeConnection()
    driver = ArrowOdbcDriver(
        cast("ArrowOdbcConnection", connection), driver_features={"dbms_name": "Microsoft SQL Server"}
    )

    assert driver.dialect == "tsql"
    assert "_statement_dialect" not in ArrowOdbcDriver.__slots__
    assert not hasattr(driver, "_statement_dialect")


def test_arrow_odbc_connection_in_transaction_uses_explicit_management_fallback() -> None:
    """Generic ODBC connections do not expose a portable transaction-state API."""
    connection = FakeConnection()
    driver = ArrowOdbcDriver(cast("ArrowOdbcConnection", connection))

    assert driver._connection_in_transaction() is False  # pyright: ignore[reportPrivateUsage]


def test_arrow_odbc_execute_marks_dml_rowcount_unknown() -> None:
    """arrow-odbc does not expose portable rows-affected metadata for DML."""
    connection = FakeConnection()
    driver = ArrowOdbcDriver(cast("ArrowOdbcConnection", connection))

    result = driver.execute("DELETE FROM items WHERE id = ?", (1,))

    assert result.rows_affected == -1
    assert connection.executed == [("DELETE FROM items WHERE id = ?", ["1"])]


def test_arrow_odbc_mssql_execute_script_sends_batch_without_splitting() -> None:
    """SQL Server IF/BEGIN batches should stay intact for ODBC execution."""
    connection = FakeConnection()
    driver = ArrowOdbcDriver(
        cast("ArrowOdbcConnection", connection), driver_features={"dbms_name": "Microsoft SQL Server"}
    )
    script = "IF OBJECT_ID(N'dbo.items', N'U') IS NULL BEGIN CREATE TABLE dbo.items (id INT); END;"

    result = driver.execute_script(script)

    assert result.rows_affected == -1
    assert connection.executed == [(script, None)]


def test_arrow_odbc_driver_slots_populated_from_features() -> None:
    """All driver_features values are cached as typed slots at initialization."""
    connection = FakeConnection()
    features = {
        "chunk_size": 4096,
        "max_bytes_per_batch": 1_000_000,
        "max_text_size": 512,
        "max_binary_size": 256,
        "fetch_concurrently": False,
        "query_timeout_sec": 30,
    }
    driver = ArrowOdbcDriver(cast("ArrowOdbcConnection", connection), driver_features=features)

    assert driver._chunk_size_val == 4096
    assert driver._max_batch_bytes == 1_000_000
    assert driver._max_text_size_val == 512
    assert driver._max_binary_size_val == 256
    assert driver._use_concurrent_fetch is False
    assert driver._query_timeout_sec_val == 30


def test_arrow_odbc_type_helper_public_import() -> None:
    import sqlspec.adapters.arrow_odbc as arrow_odbc_adapter

    assert arrow_odbc_adapter.odbc_type_to_arrow is odbc_type_to_arrow
    assert "odbc_type_to_arrow" in arrow_odbc_adapter.__all__
    assert odbc_type_to_arrow("bigint") == pa.int64()


def test_arrow_odbc_driver_slots_default_values() -> None:
    """Slot defaults match the previous driver_features.get() fallbacks."""
    connection = FakeConnection()
    driver = ArrowOdbcDriver(cast("ArrowOdbcConnection", connection), driver_features={})

    assert driver._chunk_size_val == 65_536
    assert driver._max_batch_bytes is None
    assert driver._max_text_size_val is None
    assert driver._max_binary_size_val is None
    assert driver._use_concurrent_fetch is True
    assert driver._query_timeout_sec_val is None


def test_arrow_odbc_driver_chunk_size_returns_slot() -> None:
    """_chunk_size() returns the pre-computed slot value, not a live dict lookup."""
    connection = FakeConnection()
    driver = ArrowOdbcDriver(cast("ArrowOdbcConnection", connection), driver_features={"chunk_size": 8192})

    assert driver._chunk_size() == 8192
    driver.driver_features["chunk_size"] = 99999  # pyright: ignore[reportPrivateUsage]
    assert driver._chunk_size() == 8192


def test_arrow_odbc_read_batches_uses_cached_slots() -> None:
    """_read_arrow_batches passes slot values, not live driver_features lookups."""
    connection = FakeConnection()
    features = {
        "chunk_size": 100,
        "max_bytes_per_batch": 500_000,
        "max_text_size": 200,
        "max_binary_size": 100,
        "fetch_concurrently": True,
        "query_timeout_sec": 10,
    }
    driver = ArrowOdbcDriver(cast("ArrowOdbcConnection", connection), driver_features=features)

    driver._read_arrow_batches("SELECT 1", None, 100)

    assert len(connection.read_calls) == 1
    call = connection.read_calls[0]
    assert call["max_bytes_per_batch"] == 500_000
    assert call["max_text_size"] == 200
    assert call["max_binary_size"] == 100
    assert call["fetch_concurrently"] is True
    assert call["query_timeout_sec"] == 10


def test_arrow_odbc_read_batches_omits_query_timeout_when_none() -> None:
    """query_timeout_sec key is absent from kwargs when slot is None."""
    connection = FakeConnection()
    driver = ArrowOdbcDriver(cast("ArrowOdbcConnection", connection), driver_features={"chunk_size": 50})

    driver._read_arrow_batches("SELECT 1", None, 50)

    call = connection.read_calls[0]
    assert "query_timeout_sec" not in call


def test_arrow_odbc_driver_slots_in_class_definition() -> None:
    """All new slots are declared on the class and remain alphabetically sorted."""
    slots = ArrowOdbcDriver.__slots__
    expected_new = {
        "_chunk_size_val",
        "_max_batch_bytes",
        "_max_binary_size_val",
        "_max_text_size_val",
        "_query_timeout_sec_val",
        "_use_concurrent_fetch",
    }
    assert expected_new.issubset(set(slots))
    assert list(slots) == sorted(slots)
