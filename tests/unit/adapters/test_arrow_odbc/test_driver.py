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
from sqlspec.adapters.arrow_odbc._typing import ArrowOdbcError
from sqlspec.adapters.arrow_odbc.data_dictionary import ArrowOdbcDataDictionary
from sqlspec.adapters.arrow_odbc.driver import ArrowOdbcStreamSource, _reader_to_table
from sqlspec.core import SQL, ArrowResult, LimitOffsetFilter, OrderByFilter
from sqlspec.data_dictionary import DDLResult, MetadataFidelity, MetadataSource, MetadataSupport
from sqlspec.exceptions import (
    DatabaseConnectionError,
    DataError,
    DeadlockError,
    ForeignKeyViolationError,
    ImproperConfigurationError,
    NotNullViolationError,
    OperationalError,
    PermissionDeniedError,
    QueryTimeoutError,
    SQLFileNotFoundError,
    SQLParsingError,
    SQLSpecError,
    TransactionError,
    UniqueViolationError,
)

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
        self.commit_calls = 0
        self.read_calls: list[dict[str, Any]] = []
        self.insert_calls: list[tuple[str, int, pa.Table]] = []
        self.executed: list[tuple[str, Any]] = []
        self.rollback_calls = 0

    def read_arrow_batches(self, **kwargs: Any) -> FakeReader:
        self.read_calls.append(kwargs)
        return FakeReader(pa.table({"x": [1, 2, 3]}))

    def from_table_to_db(self, source: pa.Table, target: str, chunk_size: int = 1000) -> None:
        self.insert_calls.append((target, chunk_size, source))

    def execute(self, query: str, parameters: Any = None) -> None:
        self.executed.append((query, parameters))

    def commit(self) -> None:
        self.commit_calls += 1

    def rollback(self) -> None:
        self.rollback_calls += 1

    def close(self) -> None:
        self.closed = True


def test_arrow_odbc_config_applies_driver_feature_json_serializer_to_statement_config() -> None:
    """Custom JSON serializers should reach the statement parameter config."""

    def serializer(value: object) -> str:
        return f"json:{value!r}"

    config = ArrowOdbcConfig(
        connection_config={"connection_string": "Driver={ODBC Driver 18 for SQL Server};"},
        driver_features={"json_serializer": serializer},
    )

    parameter_config = config.statement_config.parameter_config
    assert parameter_config.json_serializer is serializer
    assert parameter_config.type_coercion_map[dict] is serializer
    assert parameter_config.type_coercion_map[list] is serializer


class NoCloseConnection:
    """Connection stub matching arrow-odbc 10.4's no-close public surface."""


class FakeOdbcError(Exception):
    """Constructible stand-in for arrow_odbc.Error."""


class ErrorConnection(FakeConnection):
    """Connection stub that raises an ODBC driver error."""

    def read_arrow_batches(self, **kwargs: Any) -> FakeReader:
        self.read_calls.append(kwargs)
        raise FakeOdbcError("read failed")

    def from_table_to_db(self, source: pa.Table, target: str, chunk_size: int = 1000) -> None:
        raise FakeOdbcError("insert failed")


@pytest.mark.parametrize("method_name", ["commit", "rollback"])
def test_transaction_control_propagates_non_native_errors(method_name: str) -> None:
    connection = FakeConnection()

    def raise_runtime_error() -> None:
        raise RuntimeError("internal bug")

    setattr(connection, method_name, raise_runtime_error)
    driver = ArrowOdbcDriver(cast("ArrowOdbcConnection", connection))

    with pytest.raises(RuntimeError, match="internal bug"):
        getattr(driver, method_name)()


@pytest.mark.parametrize("method_name", ["commit", "rollback"])
def test_transaction_control_wraps_native_errors(method_name: str) -> None:
    connection = FakeConnection()
    native_error = ArrowOdbcError.__new__(ArrowOdbcError)
    native_error.args = ("native failure",)

    def raise_native_error() -> None:
        raise native_error

    setattr(connection, method_name, raise_native_error)
    driver = ArrowOdbcDriver(cast("ArrowOdbcConnection", connection))

    with pytest.raises(SQLSpecError, match=f"Failed to {method_name} transaction"):
        getattr(driver, method_name)()


@pytest.mark.parametrize(
    ("method_name", "statement"), [("commit", "COMMIT TRANSACTION"), ("rollback", "ROLLBACK TRANSACTION")]
)
def test_mssql_owned_transaction_uses_tsql_boundary(method_name: str, statement: str) -> None:
    """Owned MSSQL transactions should bypass native controls that are no-ops under autocommit."""
    connection = FakeConnection()
    driver = ArrowOdbcDriver(
        cast("ArrowOdbcConnection", connection), driver_features={"dbms_name": "Microsoft SQL Server"}
    )

    driver.begin()
    getattr(driver, method_name)()

    assert connection.executed == [("BEGIN TRANSACTION", None), (statement, None)]
    assert connection.commit_calls == 0
    assert connection.rollback_calls == 0
    assert driver._connection_in_transaction() is False  # pyright: ignore[reportPrivateUsage]


@pytest.mark.parametrize(("method_name", "active_after_error"), [("commit", True), ("rollback", False)])
def test_mssql_owned_transaction_tsql_error_preserves_state_semantics(
    method_name: str, active_after_error: bool
) -> None:
    """Failed owned controls should retain commit state but always clear rollback state."""
    connection = FakeConnection()
    native_error = ArrowOdbcError.__new__(ArrowOdbcError)
    native_error.args = ("native failure",)

    def execute(query: str, parameters: Any = None) -> None:
        connection.executed.append((query, parameters))
        if query != "BEGIN TRANSACTION":
            raise native_error

    connection.execute = execute  # type: ignore[method-assign]
    driver = ArrowOdbcDriver(
        cast("ArrowOdbcConnection", connection), driver_features={"dbms_name": "Microsoft SQL Server"}
    )
    driver.begin()

    with pytest.raises(SQLSpecError, match=f"Failed to {method_name} transaction"):
        getattr(driver, method_name)()

    assert driver._connection_in_transaction() is active_after_error  # pyright: ignore[reportPrivateUsage]


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

    def fail_get_query(self: ArrowOdbcDataDictionary, domain: str, operation: str) -> Any:
        raise SQLFileNotFoundError(f"{domain}/{operation}")

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

    def fail_get_query(self: ArrowOdbcDataDictionary, domain: str, operation: str) -> Any:
        raise SQLFileNotFoundError(f"{domain}/{operation}")

    monkeypatch.setattr(ArrowOdbcDataDictionary, "get_query", fail_get_query)

    result = driver.data_dictionary.get_columns(driver, table="items", schema="dbo")

    assert [entry["schema_name"] for entry in result] == ["dbo", "dbo"]
    assert connection.read_calls[-1]["query"] == 'SELECT * FROM "dbo"."items" WHERE 1=0'


def test_get_columns_schema_wide_does_not_probe(monkeypatch: pytest.MonkeyPatch) -> None:
    """Schema-wide missing SQL should stay on the empty SQL fallback and skip probing."""
    connection = FakeConnection()
    driver = ArrowOdbcDriver(cast("ArrowOdbcConnection", connection), driver_features={"chunk_size": 2})

    def fail_get_query(self: ArrowOdbcDataDictionary, domain: str, operation: str) -> Any:
        raise SQLFileNotFoundError(f"{domain}/{operation}")

    monkeypatch.setattr(ArrowOdbcDataDictionary, "get_query", fail_get_query)

    result = driver.data_dictionary.get_columns(driver)

    assert result == []
    assert connection.read_calls == []


def test_get_columns_probe_failure_returns_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    """Probe failures should preserve the existing empty-result contract."""
    connection = ErrorConnection()
    driver = ArrowOdbcDriver(cast("ArrowOdbcConnection", connection), driver_features={"chunk_size": 2})

    def fail_get_query(self: ArrowOdbcDataDictionary, domain: str, operation: str) -> Any:
        raise SQLFileNotFoundError(f"{domain}/{operation}")

    monkeypatch.setattr(ArrowOdbcDataDictionary, "get_query", fail_get_query)

    assert driver.data_dictionary.get_columns(driver, table="items") == []
    assert connection.read_calls[-1]["query"] == 'SELECT * FROM "items" WHERE 1=0'


def test_arrow_odbc_data_dictionary_catalog_support_is_explicitly_unavailable_without_bridge() -> None:
    """arrow-odbc should not claim raw ODBC catalog APIs without a bridge."""
    connection = FakeConnection()
    driver = ArrowOdbcDriver(cast("ArrowOdbcConnection", connection), driver_features={"chunk_size": 2})

    profile = driver.data_dictionary.get_metadata_capabilities(driver, domains=("odbc_catalog", "columns", "ddl"))

    catalog = profile.get("odbc_catalog")
    assert profile.adapter == "arrow_odbc"
    assert catalog.support == MetadataSupport.UNSUPPORTED
    assert catalog.fidelity == MetadataFidelity.UNSUPPORTED
    assert catalog.source == MetadataSource.DRIVER_METADATA
    assert catalog.warnings
    assert profile.get("columns").support == MetadataSupport.SUPPORTED
    assert profile.get("columns").fidelity == MetadataFidelity.NATIVE
    assert profile.get("columns").source == MetadataSource.CATALOG
    assert profile.get("ddl").support == MetadataSupport.UNSUPPORTED


def test_arrow_odbc_get_ddl_returns_unsupported_ddl_result() -> None:
    """arrow-odbc DDL requests should follow the shared DDLResult contract."""
    connection = FakeConnection()
    driver = ArrowOdbcDriver(cast("ArrowOdbcConnection", connection), driver_features={"chunk_size": 2})

    result = driver.data_dictionary.get_ddl(
        driver, "items", schema="dbo", object_type="view", include_dependencies=False, prefer_native=False, redact=False
    )

    assert isinstance(result, DDLResult)
    assert result.identity.name == "items"
    assert result.identity.object_type == "view"
    assert result.identity.schema == "dbo"
    assert result.status == MetadataSupport.UNSUPPORTED
    assert result.fidelity == MetadataFidelity.UNSUPPORTED
    assert result.source == MetadataSource.DRIVER_METADATA
    assert result.ddl is None
    assert result.warnings


UNSAFE_SAVEPOINT_NAMES = ["1; DROP TABLE users", "sp-1", "sp 1", "", '"sp"']


@pytest.mark.parametrize("bad_name", UNSAFE_SAVEPOINT_NAMES)
def test_arrow_odbc_savepoint_overrides_reject_unsafe_names(bad_name: str) -> None:
    """The savepoint overrides must reject unsafe identifiers before interpolation."""
    connection = FakeConnection()
    driver = ArrowOdbcDriver(cast("ArrowOdbcConnection", connection))

    with pytest.raises(TransactionError):
        driver.create_savepoint(bad_name)
    with pytest.raises(TransactionError):
        driver.release_savepoint(bad_name)
    with pytest.raises(TransactionError):
        driver.rollback_to_savepoint(bad_name)


def test_arrow_odbc_savepoint_overrides_accept_valid_name() -> None:
    """A safe savepoint name should pass validation and reach the underlying execute path."""
    connection = FakeConnection()
    driver = ArrowOdbcDriver(cast("ArrowOdbcConnection", connection))

    driver.create_savepoint("sp1")
    driver.release_savepoint("sp1")
    driver.rollback_to_savepoint("sp1")

    executed_sql = [call[0] for call in connection.executed]
    assert "SAVEPOINT sp1" in executed_sql
    assert "RELEASE SAVEPOINT sp1" in executed_sql
    assert "ROLLBACK TO SAVEPOINT sp1" in executed_sql


@pytest.mark.parametrize("bad_name", UNSAFE_SAVEPOINT_NAMES)
def test_arrow_odbc_mssql_savepoint_overrides_reject_unsafe_names(bad_name: str) -> None:
    """The SQL Server savepoint overrides must reject unsafe identifiers before interpolation."""
    connection = FakeConnection()
    driver = ArrowOdbcDriver(
        cast("ArrowOdbcConnection", connection), driver_features={"dbms_name": "Microsoft SQL Server"}
    )

    with pytest.raises(TransactionError):
        driver.create_savepoint(bad_name)
    with pytest.raises(TransactionError):
        driver.rollback_to_savepoint(bad_name)


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


def test_arrow_odbc_select_stream_uses_native_batches(monkeypatch: pytest.MonkeyPatch) -> None:
    """select_stream should stream Arrow batches instead of materializing the whole table."""
    connection = FakeConnection()
    driver = ArrowOdbcDriver(cast("ArrowOdbcConnection", connection), driver_features={"chunk_size": 2})

    def fail_reader_to_table(_reader: object) -> pa.Table:
        msg = "select_stream should not materialize via _reader_to_table"
        raise AssertionError(msg)

    monkeypatch.setattr("sqlspec.adapters.arrow_odbc.driver._reader_to_table", fail_reader_to_table)

    with driver.select_stream("SELECT 1 AS x", native_only=True, chunk_size=2) as stream:
        rows = list(stream)

    assert rows == [{"x": 1}, {"x": 2}, {"x": 3}]
    assert connection.read_calls[0]["query"] == "SELECT 1 AS x"
    assert connection.read_calls[0]["batch_size"] == 2


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
    sql, parameters = driver._compiled_sql(prepared, driver.statement_config)

    assert driver.dialect == "tsql"
    assert driver.statement_config.dialect == "tsql"
    assert sql == "SELECT ?"
    assert parameters == (1,)


@pytest.mark.parametrize("method", ["execute", "select_to_arrow"])
def test_arrow_odbc_mssql_pagination_inlines_offset_fetch_integers(method: str) -> None:
    """SQL Server ODBC requires literal integer OFFSET/FETCH control values."""
    connection = FakeConnection()
    driver = ArrowOdbcDriver(
        cast("ArrowOdbcConnection", connection), driver_features={"dbms_name": "Microsoft SQL Server"}
    )

    getattr(driver, method)(
        "SELECT name, value FROM dbo.items", OrderByFilter("value", "desc"), LimitOffsetFilter(2, 1)
    )

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


def test_arrow_odbc_syntax_fallback_without_native_error_number() -> None:
    """Syntax diagnostics without a native error number retain the fallback mapping."""
    mapped = create_mapped_exception(FakeOdbcError("Incorrect syntax near '*'."))

    assert isinstance(mapped, SQLParsingError)


@pytest.mark.parametrize(
    ("message", "expected_type"),
    [
        (
            "State: 23000, Native error: 2601, Message: [SQL Server]Cannot insert duplicate key row.",
            UniqueViolationError,
        ),
        (
            "State: 23000, Native error: 2627, Message: [SQL Server]Violation of UNIQUE KEY constraint.",
            UniqueViolationError,
        ),
        (
            "State: 23000, Native error: 547, Message: [SQL Server]FOREIGN KEY constraint conflict.",
            ForeignKeyViolationError,
        ),
        ("State: 23000, Native error: 515, Message: [SQL Server]Cannot insert NULL.", NotNullViolationError),
        ("State: 28000, Native error: 18456, Message: [SQL Server]Login failed.", PermissionDeniedError),
        (
            "State: 08001, Native error: 4060, Message: [SQL Server]Cannot open database requested by the login.",
            DatabaseConnectionError,
        ),
        ("State: 08001, Native error: 53, Message: [SQL Server]Network path not found.", DatabaseConnectionError),
        ("State: 40001, Native error: 1205, Message: [SQL Server]Transaction was deadlocked.", DeadlockError),
        ("State: HYT00, Native error: -2, Message: [SQL Server]Query timeout expired.", QueryTimeoutError),
        ("State: 22018, Native error: 8114, Message: [SQL Server]Conversion failed.", DataError),
        ("State: HY000, Native error: 1105, Message: [SQL Server]Could not allocate space.", OperationalError),
        ("State: 42000, Native error: 102, Message: [SQL Server]Incorrect syntax near '*'.", SQLParsingError),
    ],
)
def test_arrow_odbc_error_number_maps_to_sqlspec_exception(message: str, expected_type: "type[SQLSpecError]") -> None:
    """SQL Server native error numbers in arrow-odbc diagnostics map to SQLSpec exceptions."""
    mapped = create_mapped_exception(FakeOdbcError(message))

    assert isinstance(mapped, expected_type)


def test_arrow_odbc_non_mssql_native_code_does_not_use_sql_server_mapping() -> None:
    """Native error numbers from other ODBC vendors do not use the SQL Server table."""
    mapped = create_mapped_exception(
        FakeOdbcError("State: HY000, Native error: 1205, Message: [MySQL]Lock wait timeout exceeded.")
    )

    assert type(mapped) is SQLSpecError


def test_arrow_odbc_driver_dialect_set_from_dbms_name() -> None:
    """The public dialect slot stores the resolved statement dialect without a private mirror slot."""
    connection = FakeConnection()
    driver = ArrowOdbcDriver(
        cast("ArrowOdbcConnection", connection), driver_features={"dbms_name": "Microsoft SQL Server"}
    )

    assert driver.dialect == "tsql"
    assert "_statement_dialect" not in ArrowOdbcDriver.__slots__
    assert not hasattr(driver, "_statement_dialect")


def test_arrow_odbc_connection_in_transaction_tracks_begin_commit_rollback() -> None:
    """The transaction predicate should follow the real begin/commit/rollback state."""
    connection = FakeConnection()
    driver = ArrowOdbcDriver(cast("ArrowOdbcConnection", connection))

    assert driver._connection_in_transaction() is False  # pyright: ignore[reportPrivateUsage]

    driver.begin()
    assert connection.executed == [("BEGIN", None)]
    assert driver._connection_in_transaction() is True  # pyright: ignore[reportPrivateUsage]

    driver.commit()
    assert driver._connection_in_transaction() is False  # pyright: ignore[reportPrivateUsage]

    driver.begin()
    assert driver._connection_in_transaction() is True  # pyright: ignore[reportPrivateUsage]
    driver.rollback()
    assert driver._connection_in_transaction() is False  # pyright: ignore[reportPrivateUsage]


def test_arrow_odbc_mssql_begin_issues_tsql_and_tracks_state() -> None:
    """SQL Server ODBC transactions should issue BEGIN TRANSACTION and drive the state predicate."""
    connection = FakeConnection()
    driver = ArrowOdbcDriver(
        cast("ArrowOdbcConnection", connection), driver_features={"dbms_name": "Microsoft SQL Server"}
    )

    driver.begin()

    assert connection.executed == [("BEGIN TRANSACTION", None)]
    assert driver._connection_in_transaction() is True  # pyright: ignore[reportPrivateUsage]

    driver.commit()
    assert driver._connection_in_transaction() is False  # pyright: ignore[reportPrivateUsage]


def test_arrow_odbc_execute_marks_dml_rowcount_zero() -> None:
    """arrow-odbc has no portable DML rows-affected metadata; it reports zero, not the -1 sentinel."""
    connection = FakeConnection()
    driver = ArrowOdbcDriver(cast("ArrowOdbcConnection", connection))

    result = driver.execute("DELETE FROM items WHERE id = ?", (1,))

    assert result.rows_affected == 0
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
    """Slot defaults match apply_driver_features defaults."""
    connection = FakeConnection()
    driver = ArrowOdbcDriver(cast("ArrowOdbcConnection", connection), driver_features={})

    assert driver._chunk_size_val == 65_536
    assert driver._max_batch_bytes == 512 * 1024 * 1024
    assert driver._max_text_size_val == 1024 * 1024
    assert driver._max_binary_size_val == 1024 * 1024
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


@pytest.mark.parametrize(
    "sql,parameters,expected_sql,expected_parameters",
    [
        (
            "SELECT TOP (?) id FROM t WHERE name = ?",
            [3, "hostile'; --"],
            "SELECT TOP (3) id FROM t WHERE name = ?",
            ["hostile'; --"],
        ),
        (
            "WITH c AS (SELECT ? AS id) SELECT TOP (?) id FROM c WHERE id > ?",
            [7, 3, 0],
            "WITH c AS (SELECT ? AS id) SELECT TOP (3) id FROM c WHERE id > ?",
            [7, 0],
        ),
        ("SELECT TOP /* limit */ (?) '?' AS marker FROM t", [3], "SELECT TOP /* limit */ (3) '?' AS marker FROM t", []),
        ("SELECT 'TOP (?)' AS label FROM t WHERE id = ?", [7], "SELECT 'TOP (?)' AS label FROM t WHERE id = ?", [7]),
        (
            "WITH c AS (SELECT id FROM t ORDER BY id OFFSET ? ROWS FETCH NEXT ? ROWS ONLY) SELECT * FROM c WHERE id > ?",
            [10, 5, 42],
            "WITH c AS (SELECT id FROM t ORDER BY id OFFSET 10 ROWS FETCH NEXT 5 ROWS ONLY) SELECT * FROM c WHERE id > ?",
            [42],
        ),
        (
            "SELECT 'OFFSET ? ROWS FETCH NEXT ? ROWS ONLY' AS label FROM t WHERE id = ?",
            [7],
            "SELECT 'OFFSET ? ROWS FETCH NEXT ? ROWS ONLY' AS label FROM t WHERE id = ?",
            [7],
        ),
        (
            "SELECT id FROM t WHERE active = ? ORDER BY id OFFSET ? ROWS",
            [1, 10],
            "SELECT id FROM t WHERE active = ? ORDER BY id OFFSET 10 ROWS",
            [1],
        ),
        (
            "SELECT id FROM t WHERE active = ? ORDER BY id OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY",
            [1, 25],
            "SELECT id FROM t WHERE active = ? ORDER BY id OFFSET 0 ROWS FETCH NEXT 25 ROWS ONLY",
            [1],
        ),
    ],
)
def test_mssql_top_preserves_data_bindings(
    sql: str, parameters: list[Any], expected_sql: str, expected_parameters: list[Any]
) -> None:
    from sqlspec.adapters.arrow_odbc.driver import _inline_mssql_pagination_parameters

    statement = SQL(sql, parameters)
    statement.compile()
    assert _inline_mssql_pagination_parameters(sql, parameters, statement.get_processed_state().parameter_profile) == (
        expected_sql,
        expected_parameters,
    )


def test_mssql_top_rejects_noninteger_limit() -> None:
    from sqlspec.adapters.arrow_odbc.driver import _inline_mssql_pagination_parameters

    statement = SQL("SELECT TOP (?) id FROM t", ["3); DROP TABLE t; --"])
    sql, parameters = statement.compile()
    with pytest.raises(ValueError):
        _inline_mssql_pagination_parameters(sql, parameters, statement.get_processed_state().parameter_profile)


@pytest.mark.parametrize("suffix", ["", " PERCENT"])
def test_mssql_top_rejects_fractional_limits(suffix: str) -> None:
    from sqlspec.adapters.arrow_odbc.driver import _inline_mssql_pagination_parameters

    statement = SQL("SELECT TOP (?)" + suffix + " id FROM t", [12.5])
    sql, parameters = statement.compile()
    with pytest.raises(ValueError, match="whole integers"):
        _inline_mssql_pagination_parameters(sql, parameters, statement.get_processed_state().parameter_profile)


def test_arrow_odbc_select_to_arrow_reader_maps_exceptions(monkeypatch: pytest.MonkeyPatch) -> None:
    """select_to_arrow with return_format='reader' should raise mapped SQLSpec exceptions."""
    monkeypatch.setattr("sqlspec.adapters.arrow_odbc.driver.ArrowOdbcError", FakeOdbcError)
    connection = ErrorConnection()
    driver = ArrowOdbcDriver(cast("ArrowOdbcConnection", connection))

    with pytest.raises(SQLSpecError, match="ODBC database error"):
        driver.select_to_arrow("SELECT * FROM items", return_format="reader")


@pytest.mark.parametrize("method", ["execute", "select_to_arrow"])
def test_mssql_named_pagination_preserves_cte_binding_order(method: str) -> None:
    connection = FakeConnection()
    driver = ArrowOdbcDriver(
        cast("ArrowOdbcConnection", connection), driver_features={"dbms_name": "Microsoft SQL Server"}
    )
    for offset, limit, value in [(10, 5, 42), (20, 7, 99)]:
        getattr(driver, method)(
            "WITH c AS (SELECT id FROM t ORDER BY id OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY) "
            "SELECT * FROM c WHERE id > :value",
            {"offset": offset, "limit": limit, "value": value},
        )
        call = connection.read_calls[-1]
        assert f"OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY" in call["query"]
        assert call["parameters"] == [str(value)]


def test_mssql_prepared_pagination_reuses_compiler_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    from sqlglot import Dialect

    connection = FakeConnection()
    driver = ArrowOdbcDriver(
        cast("ArrowOdbcConnection", connection), driver_features={"dbms_name": "Microsoft SQL Server"}
    )
    statement = driver.prepare_statement(
        "SELECT TOP (:limit) id FROM t WHERE id > :value", (), kwargs={"limit": 2, "value": 7}
    )
    statement.compile()

    def unexpected_tokenize(*args: Any, **kwargs: Any) -> Any:
        pytest.fail("Executing an already compiled statement must not tokenize again")

    monkeypatch.setattr(Dialect, "tokenize", unexpected_tokenize)
    with driver.with_cursor(driver.connection) as cursor:
        driver.dispatch_execute(cursor, statement)
        driver.dispatch_execute(cursor, statement)
    assert [(call["query"], call["parameters"]) for call in connection.read_calls] == [
        ("SELECT TOP (2) id FROM t WHERE id > ?", ["7"]),
        ("SELECT TOP (2) id FROM t WHERE id > ?", ["7"]),
    ]


def test_arrow_odbc_execute_many_preserves_sql_and_reports_unknown_rowcount() -> None:
    connection = FakeConnection()
    driver = ArrowOdbcDriver(cast("Any", connection))
    result = driver.execute_many("UPDATE items SET name = ? WHERE id > ?", [("alpha", 0), ("beta", 2)])
    assert connection.executed == [
        ("UPDATE items SET name = ? WHERE id > ?", ["alpha", "0"]),
        ("UPDATE items SET name = ? WHERE id > ?", ["beta", "2"]),
    ]
    assert result.rows_affected == -1


@pytest.mark.parametrize("parameter_transformer", [False, True])
def test_mssql_pagination_after_output_transformer(parameter_transformer: bool) -> None:
    connection = FakeConnection()
    driver = ArrowOdbcDriver(
        cast("ArrowOdbcConnection", connection), driver_features={"dbms_name": "Microsoft SQL Server"}
    )

    def prepend_comment(sql: str, parameters: Any) -> tuple[str, Any]:
        return "/* trace */ " + sql, parameters

    config = driver.statement_config
    if parameter_transformer:
        config = config.replace(parameter_config=config.parameter_config.replace(output_transformer=prepend_comment))
    else:
        config = config.replace(output_transformer=prepend_comment)
    driver.select_to_arrow(
        "SELECT TOP (:limit) id FROM t WHERE id > :value", {"limit": 2, "value": 7}, statement_config=config
    )
    call = connection.read_calls[-1]
    assert call["query"] == "/* trace */ SELECT TOP (2) id FROM t WHERE id > ?"
    assert call["parameters"] == ["7"]


@pytest.mark.parametrize(
    ("dbms_name", "expected_begin", "expected_commit", "expected_rollback"),
    [
        ("Microsoft SQL Server", "BEGIN TRANSACTION", "COMMIT TRANSACTION", "ROLLBACK TRANSACTION"),
        ("PostgreSQL Unicode", "BEGIN", "COMMIT", "ROLLBACK"),
        ("SQLite", "BEGIN", "COMMIT", "ROLLBACK"),
    ],
)
def test_autocommit_transactions_emit_dialect_sql_statements(
    dbms_name: str, expected_begin: str, expected_commit: str, expected_rollback: str
) -> None:
    """Under autocommit=True, begin/commit/rollback emit dialect-appropriate SQL statements."""
    connection = FakeConnection()
    driver = ArrowOdbcDriver(
        cast("ArrowOdbcConnection", connection), driver_features={"dbms_name": dbms_name, "connection_autocommit": True}
    )

    driver.begin()
    assert driver._transaction_active is True
    driver.commit()
    assert driver._transaction_active is False

    driver.begin()
    driver.rollback()
    assert driver._transaction_active is False

    assert connection.executed == [
        (expected_begin, None),
        (expected_commit, None),
        (expected_begin, None),
        (expected_rollback, None),
    ]
    assert connection.commit_calls == 0
    assert connection.rollback_calls == 0


@pytest.mark.parametrize("dbms_name", ["IBM DB2", "Oracle in OraClient21Home1"])
def test_autocommit_transactions_rejected_for_db2_and_oracle(dbms_name: str) -> None:
    """Db2 and Oracle reject explicit begin() when connection_autocommit=True."""
    connection = FakeConnection()
    driver = ArrowOdbcDriver(
        cast("ArrowOdbcConnection", connection), driver_features={"dbms_name": dbms_name, "connection_autocommit": True}
    )

    with pytest.raises(ImproperConfigurationError, match="autocommit"):
        driver.begin()


@pytest.mark.parametrize(
    "dbms_name", ["Microsoft SQL Server", "PostgreSQL Unicode", "SQLite", "IBM DB2", "Oracle in OraClient21Home1"]
)
def test_manual_commit_transactions_use_odbc_end_tran_without_sql_begin(dbms_name: str) -> None:
    """Under connection_autocommit=False, begin() is a state no-op and commit/rollback use ODBC API."""
    connection = FakeConnection()
    driver = ArrowOdbcDriver(
        cast("ArrowOdbcConnection", connection),
        driver_features={"dbms_name": dbms_name, "connection_autocommit": False},
    )

    driver.begin()
    assert driver._transaction_active is True
    assert connection.executed == []

    driver.commit()
    assert driver._transaction_active is False
    assert connection.commit_calls == 1

    driver.begin()
    driver.rollback()
    assert driver._transaction_active is False
    assert connection.rollback_calls == 1
    assert connection.executed == []


def test_arrow_exception_mapped_in_exception_handler_and_stream_source() -> None:
    """pyarrow.ArrowException during batch reading or streaming is mapped to SQLSpecError."""

    class ArrowBrokenReader:
        schema = pa.schema([("id", pa.int64())])

        def __init__(self) -> None:
            self.closed = False

        def __iter__(self) -> Iterator[pa.RecordBatch]:
            return self

        def __next__(self) -> pa.RecordBatch:
            msg = "State: 22018, Native error: 0, Message: invalid character value for cast"
            raise pa.ArrowInvalid(msg)

        def close(self) -> None:
            self.closed = True

    raw_reader = ArrowBrokenReader()

    class BrokenStreamConnection(FakeConnection):
        def read_arrow_batches(self, **kwargs: Any) -> Any:
            self.read_calls.append(kwargs)
            return raw_reader

    connection = BrokenStreamConnection()
    driver = ArrowOdbcDriver(cast("ArrowOdbcConnection", connection))
    source = ArrowOdbcStreamSource(driver, "SELECT id FROM items", None, 10)
    source.start()

    with pytest.raises(DataError):
        source.fetch_chunk()

    source.close()
    assert raw_reader.closed is True


def test_select_to_storage_writes_arrow_table_to_destination(tmp_path: Any) -> None:
    """select_to_storage exports query results via the Arrow storage pipeline."""
    connection = FakeConnection()
    driver = ArrowOdbcDriver(cast("ArrowOdbcConnection", connection))
    destination = str(tmp_path / "export.parquet")

    job = driver.select_to_storage("SELECT x FROM items", destination, format_hint="parquet")

    assert job.status == "completed"
    assert job.telemetry["rows_processed"] == 3


@pytest.mark.parametrize("binary_val", [b"raw", bytearray(b"raw"), memoryview(b"raw")])
def test_binary_parameter_raises_data_error(binary_val: Any) -> None:
    """Passing raw binary parameters raises DataError instead of binding repr() text."""
    connection = FakeConnection()
    driver = ArrowOdbcDriver(cast("ArrowOdbcConnection", connection))

    with pytest.raises(DataError, match="Binary parameter binding is not supported"):
        driver.execute("INSERT INTO blobs (payload) VALUES (?)", (binary_val,))


def test_execute_statement_forwards_query_timeout_sec() -> None:
    """_execute_statement passes query_timeout_sec when configured in driver_features."""
    recorded: list[tuple[str, Any, Any]] = []

    class TimeoutAwareConnection(FakeConnection):
        def execute(self, query: str, parameters: Any = None, query_timeout_sec: Any = None) -> None:
            recorded.append((query, parameters, query_timeout_sec))

    connection = TimeoutAwareConnection()
    driver = ArrowOdbcDriver(cast("ArrowOdbcConnection", connection), driver_features={"query_timeout_sec": 15})

    driver.execute("DELETE FROM items WHERE x = ?", (1,))

    assert recorded == [("DELETE FROM items WHERE x = ?", ["1"], 15)]


def test_dispatch_execute_script_does_not_rebind_parameters_across_multiple_statements() -> None:
    """Multi-statement execute_script does not pass the full parameter list to each split statement."""
    connection = FakeConnection()
    driver = ArrowOdbcDriver(
        cast("ArrowOdbcConnection", connection), driver_features={"dbms_name": "PostgreSQL Unicode"}
    )

    driver.execute_script("DELETE FROM items; DELETE FROM archive;")
    assert connection.executed == [("DELETE FROM items", None), ("DELETE FROM archive", None)]


def test_reader_to_table_preserves_schema_on_empty_iterable_reader() -> None:
    """_reader_to_table preserves schema metadata when an iterable reader yields zero batches."""

    class EmptyIterableReader:
        schema = pa.Table.from_pydict({
            "id": pa.array([], type=pa.int64()),
            "name": pa.array([], type=pa.string()),
        }).schema

        def __iter__(self) -> Iterator[pa.RecordBatch]:
            return iter(())

    table = _reader_to_table(EmptyIterableReader())
    assert table.num_rows == 0
    assert table.column_names == ["id", "name"]


def test_data_dictionary_dialect_and_lowercase_column_probe(monkeypatch: pytest.MonkeyPatch) -> None:
    """Data dictionary tracks runtime dialect, normalizes probed columns, and disables concurrent fetch on probes."""

    class UpperSchemaConnection(FakeConnection):
        def read_arrow_batches(self, **kwargs: Any) -> FakeReader:
            self.read_calls.append(kwargs)
            empty_table = pa.Table.from_pydict({
                "ITEM_ID": pa.array([], type=pa.int64()),
                "ITEM_NAME": pa.array([], type=pa.string()),
            })
            return FakeReader(empty_table)

    connection = UpperSchemaConnection()
    driver = ArrowOdbcDriver(
        cast("ArrowOdbcConnection", connection),
        driver_features={"dbms_name": "IBM DB2", "enable_lowercase_column_names": True},
    )

    def fail_get_query(self: ArrowOdbcDataDictionary, domain: str, operation: str) -> Any:
        raise SQLFileNotFoundError(f"{domain}/{operation}")

    monkeypatch.setattr(ArrowOdbcDataDictionary, "get_query", fail_get_query)

    assert driver.data_dictionary.dialect == "db2"
    columns = driver.data_dictionary.get_columns(driver, table="items")
    assert [col["column_name"] for col in columns] == ["item_id", "item_name"]
    assert connection.read_calls[-1]["fetch_concurrently"] is False

    profile = driver.data_dictionary.get_metadata_capabilities(driver, domains=("columns",))
    assert profile.get("columns").fidelity == MetadataFidelity.PARTIAL


def test_odbc_type_to_arrow_handles_wide_and_out_of_range_decimals() -> None:
    """Decimals up to 38 use decimal128, 39..76 use decimal256, and out-of-range fall back to string."""
    assert odbc_type_to_arrow("decimal", precision=18, scale=4) == pa.decimal128(18, 4)
    assert odbc_type_to_arrow("decimal", precision=50, scale=10) == pa.decimal256(50, 10)
    assert odbc_type_to_arrow("decimal", precision=100, scale=10) == pa.string()
    assert odbc_type_to_arrow("numeric", precision=0, scale=0) == pa.string()
    assert odbc_type_to_arrow("numeric", precision=10, scale=20) == pa.string()


def test_load_from_arrow_streams_record_batch_reader_and_bulk_insert_unwraps_arrow_result() -> None:
    """load_from_arrow passes RecordBatchReader directly to insert_into_table and bulk_insert_arrow unwraps ArrowResult."""
    inserted_readers: list[tuple[str, Any]] = []

    class StreamingInsertConnection(FakeConnection):
        def insert_into_table(self, **kwargs: Any) -> None:
            inserted_readers.append((kwargs["table"], kwargs["reader"]))

    connection = StreamingInsertConnection()
    driver = ArrowOdbcDriver(cast("ArrowOdbcConnection", connection))
    table = pa.table({"id": [1, 2]})
    reader = pa.RecordBatchReader.from_batches(table.schema, table.to_batches())

    driver.load_from_arrow("items", reader)
    assert len(inserted_readers) == 1
    assert inserted_readers[0][1] is reader

    arrow_result = ArrowResult(statement=SQL("SELECT 1"), data=table, rows_affected=2)
    driver.bulk_insert_arrow("items", arrow_result)
    assert connection.insert_calls == [("items", 65536, table)]


def test_ambient_exception_isolation_select_to_arrow() -> None:
    """select_to_arrow executes cleanly inside an active exception block without ambient exception leakage."""
    connection = FakeConnection()
    driver = ArrowOdbcDriver(cast("ArrowOdbcConnection", connection))

    try:
        raise ValueError("ambient error")
    except ValueError:
        result = driver.select_to_arrow("SELECT 1")
        assert result.num_rows == 3


def test_transaction_lifecycle_maps_generic_exception() -> None:
    """begin, commit, and rollback map native exceptions with appropriate transaction failure messages."""
    connection = FakeConnection()
    native_error = ArrowOdbcError.__new__(ArrowOdbcError)
    native_error.args = ("native disk error",)

    def raise_native() -> None:
        raise native_error

    setattr(connection, "commit", raise_native)
    setattr(connection, "rollback", raise_native)
    driver = ArrowOdbcDriver(cast("ArrowOdbcConnection", connection))

    with pytest.raises(SQLSpecError, match="Failed to commit transaction"):
        driver.commit()

    with pytest.raises(SQLSpecError, match="Failed to rollback transaction"):
        driver.rollback()
