"""Parameterized tests for real adapter implementations."""

import base64
import importlib
import inspect
import operator
import sqlite3
import types
from collections.abc import AsyncIterator, Generator, Iterator
from contextlib import asynccontextmanager, contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Self

import pytest

import sqlspec.adapters
from sqlspec.adapters.sqlite.driver import SqliteDriver
from sqlspec.config import DatabaseConfigProtocol
from sqlspec.core import SQL, ParameterStyle, ParameterStyleConfig, SQLResult, StatementConfig
from sqlspec.core.capabilities import TypeCoercionCapabilities
from sqlspec.driver import ExecutionResult
from sqlspec.exceptions import SQLParsingError, SQLSpecError

__all__ = ()


ADAPTER_CONFIGS = [
    {
        "name": "SQLite",
        "parameter_style": ParameterStyle.QMARK,
        "dialect": "sqlite",
        "example_queries": {
            "select": "SELECT * FROM users WHERE id = ?",
            "insert": "INSERT INTO users (name) VALUES (?)",
            "update": "UPDATE users SET name = ? WHERE id = ?",
            "delete": "DELETE FROM users WHERE id = ?",
        },
    },
    {
        "name": "PostgreSQL",
        "parameter_style": ParameterStyle.NUMERIC,
        "dialect": "postgres",
        "example_queries": {
            "select": "SELECT * FROM users WHERE id = $1",
            "insert": "INSERT INTO users (name) VALUES ($1)",
            "update": "UPDATE users SET name = $1 WHERE id = $2",
            "delete": "DELETE FROM users WHERE id = $1",
        },
    },
    {
        "name": "MySQL",
        "parameter_style": ParameterStyle.QMARK,
        "execution_style": ParameterStyle.POSITIONAL_PYFORMAT,
        "dialect": "mysql",
        "example_queries": {
            "select": "SELECT * FROM users WHERE id = ?",
            "insert": "INSERT INTO users (name) VALUES (?)",
            "update": "UPDATE users SET name = ? WHERE id = ?",
            "delete": "DELETE FROM users WHERE id = ?",
        },
    },
]

ADAPTER_CORE_MODULES = (
    "sqlspec.adapters.adbc.core",
    "sqlspec.adapters.aiomysql.core",
    "sqlspec.adapters.aiosqlite.core",
    "sqlspec.adapters.arrow_odbc.core",
    "sqlspec.adapters.asyncmy.core",
    "sqlspec.adapters.asyncpg.core",
    "sqlspec.adapters.bigquery.core",
    "sqlspec.adapters.db2.core",
    "sqlspec.adapters.duckdb.core",
    "sqlspec.adapters.mssql_python.core",
    "sqlspec.adapters.mysqlconnector.core",
    "sqlspec.adapters.oracledb.core",
    "sqlspec.adapters.psqlpy.core",
    "sqlspec.adapters.psycopg.core",
    "sqlspec.adapters.pymssql.core",
    "sqlspec.adapters.pymysql.core",
    "sqlspec.adapters.spanner.core",
    "sqlspec.adapters.sqlite.core",
    "sqlspec.adapters.teradata.core",
)
CREATE_MAPPED_EXCEPTION_MODULES = ADAPTER_CORE_MODULES
APPLY_DRIVER_FEATURES_MODULES = ADAPTER_CORE_MODULES
SQLITE_EXCEPTION_HANDLER_CLASSES = (
    ("sqlspec.adapters.aiosqlite.driver", "AiosqliteExceptionHandler"),
    ("sqlspec.adapters.sqlite.driver", "SqliteExceptionHandler"),
)


@pytest.fixture(params=ADAPTER_CONFIGS, ids=operator.itemgetter("name"))
def adapter_config(request: pytest.FixtureRequest) -> "dict[str, Any]":
    """Parameterized fixture providing different adapter configurations."""
    from typing import cast

    return cast("dict[str, Any]", request.param)


@pytest.fixture
def statement_config_for_adapter(adapter_config: "dict[str, Any]") -> StatementConfig:
    """Create statement config separating parse and execution parameter styles."""
    parse_style = adapter_config["parameter_style"]
    exec_style = adapter_config.get("execution_style", parse_style)

    return StatementConfig(
        dialect=adapter_config["dialect"],
        enable_caching=False,
        parameter_config=ParameterStyleConfig(
            default_parameter_style=parse_style,
            supported_parameter_styles={parse_style},
            default_execution_parameter_style=exec_style,
            supported_execution_parameter_styles={exec_style},
        ),
    )


@pytest.mark.parametrize("module_name", CREATE_MAPPED_EXCEPTION_MODULES)
def test_adapter_create_mapped_exception_signature_is_uniform(module_name: str) -> None:
    module = pytest.importorskip(module_name)

    signature = inspect.signature(module.create_mapped_exception)

    assert tuple(signature.parameters) == ("error", "logger")
    assert signature.parameters["error"].kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
    assert signature.parameters["logger"].kind is inspect.Parameter.KEYWORD_ONLY
    assert signature.parameters["logger"].default is None


def test_duckdb_create_mapped_exception_accepts_standard_shape() -> None:
    from sqlspec.adapters.duckdb.core import create_mapped_exception

    class ParserException(Exception):
        pass

    mapped = create_mapped_exception(ParserException("syntax error"))

    assert isinstance(mapped, SQLParsingError)


def test_arrow_odbc_create_mapped_exception_accepts_standard_shape() -> None:
    from sqlspec.adapters.arrow_odbc.core import create_mapped_exception

    mapped = create_mapped_exception(Exception("Native error: 102 Incorrect syntax near 'FROM'"), logger=None)

    assert isinstance(mapped, SQLParsingError)


@pytest.mark.parametrize(("module_name", "class_name"), SQLITE_EXCEPTION_HANDLER_CLASSES)
def test_sqlite_exception_handlers_declare_empty_slots(module_name: str, class_name: str) -> None:
    module = importlib.import_module(module_name)
    exception_handler = getattr(module, class_name)

    assert exception_handler.__slots__ == ()


@pytest.mark.parametrize("module_name", APPLY_DRIVER_FEATURES_MODULES)
def test_adapter_apply_driver_features_signature_and_return_shape(module_name: str) -> None:
    module = pytest.importorskip(module_name)
    statement_config = StatementConfig()

    signature = inspect.signature(module.apply_driver_features)
    assert tuple(signature.parameters) == ("statement_config", "driver_features")

    resolved_statement_config, features = module.apply_driver_features(statement_config, {"custom": "value"})

    assert isinstance(resolved_statement_config, StatementConfig)
    assert isinstance(features, dict)
    assert features["custom"] == "value"


def test_mssql_python_pool_is_implemented_in_pool_module() -> None:
    from sqlspec.adapters.mssql_python import MssqlPythonConnectionPool

    assert MssqlPythonConnectionPool.__module__ == "sqlspec.adapters.mssql_python.pool"


def test_adapter_parameter_style_handling(
    adapter_config: "dict[str, Any]", statement_config_for_adapter: StatementConfig
) -> None:
    """Test parameter style handling across different adapters."""
    config = statement_config_for_adapter

    assert config.parameter_config.default_parameter_style == adapter_config["parameter_style"]
    assert adapter_config["parameter_style"] in config.parameter_config.supported_parameter_styles

    queries = adapter_config["example_queries"]

    if adapter_config["parameter_style"] == ParameterStyle.QMARK:
        statement = SQL(queries["select"], 1, statement_config=config)
        assert "?" in statement.raw_sql
    elif adapter_config["parameter_style"] == ParameterStyle.NUMERIC:
        statement = SQL(queries["select"], 1, statement_config=config)
        assert "$1" in statement.raw_sql or "?" in statement.raw_sql


def test_adapter_sql_compilation(
    adapter_config: "dict[str, Any]", statement_config_for_adapter: StatementConfig
) -> None:
    """Test SQL compilation for different adapters."""
    config = statement_config_for_adapter
    queries = adapter_config["example_queries"]

    select_stmt = SQL(queries["select"], 1, statement_config=config)
    compiled_sql, compiled_params = select_stmt.compile()

    assert isinstance(compiled_sql, str)
    assert len(compiled_sql) > 0
    assert compiled_params is not None

    insert_stmt = SQL(queries["insert"], "test_user", statement_config=config)
    compiled_sql, compiled_params = insert_stmt.compile()

    assert isinstance(compiled_sql, str)
    assert "INSERT" in compiled_sql.upper()
    assert compiled_params is not None


def test_adapter_query_type_detection(
    adapter_config: "dict[str, Any]", statement_config_for_adapter: StatementConfig
) -> None:
    """Test query type detection across adapters."""
    config = statement_config_for_adapter
    queries = adapter_config["example_queries"]

    select_stmt = SQL(queries["select"], 1, statement_config=config)
    assert select_stmt.returns_rows() is True

    insert_stmt = SQL(queries["insert"], "test", statement_config=config)
    assert insert_stmt.returns_rows() is False

    update_stmt = SQL(queries["update"], "new_name", 1, statement_config=config)
    assert update_stmt.returns_rows() is False

    delete_stmt = SQL(queries["delete"], 1, statement_config=config)
    assert delete_stmt.returns_rows() is False


@pytest.mark.parametrize("query_type", ["select", "insert", "update", "delete"])
def test_adapter_query_types(
    adapter_config: "dict[str, Any]", statement_config_for_adapter: StatementConfig, query_type: str
) -> None:
    """Test different query types for each adapter."""
    config = statement_config_for_adapter
    query = adapter_config["example_queries"][query_type]

    if query_type == "select":
        statement = SQL(query, 1, statement_config=config)
        assert statement.returns_rows() is True
    elif query_type == "insert":
        statement = SQL(query, "test", statement_config=config)
        assert statement.returns_rows() is False
    elif query_type == "update":
        statement = SQL(query, "new_name", 1, statement_config=config)
        assert statement.returns_rows() is False
    elif query_type == "delete":
        statement = SQL(query, 1, statement_config=config)
        assert statement.returns_rows() is False
    else:
        statement = SQL(query, statement_config=config)

    assert statement
    compiled_sql, _compiled_params = statement.compile()
    assert isinstance(compiled_sql, str)
    assert query_type.upper() in compiled_sql.upper()


def test_sqlite_driver_real_implementation() -> None:
    """Test SQLite driver with real SQLite connection."""

    connection = sqlite3.connect(":memory:")

    try:
        connection.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        connection.execute("INSERT INTO users (name) VALUES ('test_user')")
        connection.commit()

        from sqlspec.core import ParameterStyleConfig

        simple_config = StatementConfig(
            dialect="sqlite",
            enable_caching=False,
            parameter_config=ParameterStyleConfig(
                default_parameter_style=ParameterStyle.QMARK, supported_parameter_styles={ParameterStyle.QMARK}
            ),
        )

        driver = SqliteDriver(connection, simple_config)

        result = driver.execute("SELECT * FROM users WHERE name = ?", "test_user")
        assert isinstance(result, SQLResult)
        assert result.operation_type == "SELECT"
        assert len(result.get_data()) == 1
        assert result.get_data()[0]["name"] == "test_user"

        result = driver.execute("INSERT INTO users (name) VALUES (?)", "new_user")
        assert isinstance(result, SQLResult)
        assert result.operation_type == "INSERT"
        assert result.rows_affected == 1

        result = driver.execute_many("INSERT INTO users (name) VALUES (?)", [["user1"], ["user2"], ["user3"]])
        assert isinstance(result, SQLResult)
        assert result.rows_affected == 3

        script = """
        UPDATE users SET name = 'updated_' || name WHERE id > 1;
        DELETE FROM users WHERE name = 'test_user';
        """
        result = driver.execute_script(script)
        assert isinstance(result, SQLResult)
        assert result.operation_type == "SCRIPT"
        assert result.total_statements == 2

        driver.begin()
        driver.execute("INSERT INTO users (name) VALUES (?)", "transaction_test")
        driver.commit()

        result = driver.execute("SELECT COUNT(*) as count FROM users WHERE name = ?", "transaction_test")
        count = result.get_data()[0]["count"]
        assert count == 1

        driver.begin()
        driver.execute("INSERT INTO users (name) VALUES (?)", "rollback_test")
        driver.rollback()

        result = driver.execute("SELECT COUNT(*) as count FROM users WHERE name = ?", "rollback_test")
        count = result.get_data()[0]["count"]
        assert count == 0

    finally:
        connection.close()


def test_sqlite_driver_exception_handling() -> None:
    """Test SQLite driver exception handling."""
    connection = sqlite3.connect(":memory:")

    simple_config = StatementConfig(
        dialect="sqlite",
        enable_caching=False,
        parameter_config=ParameterStyleConfig(
            default_parameter_style=ParameterStyle.QMARK, supported_parameter_styles={ParameterStyle.QMARK}
        ),
    )

    driver = SqliteDriver(connection, simple_config)

    try:
        with pytest.raises(SQLSpecError):
            driver.execute("INVALID SQL SYNTAX")

        connection.execute("CREATE TABLE test_table (id INTEGER PRIMARY KEY, unique_col TEXT UNIQUE)")
        connection.commit()

        driver.execute("INSERT INTO test_table (unique_col) VALUES (?)", "unique_value")

        with pytest.raises(SQLSpecError):
            driver.execute("INSERT INTO test_table (unique_col) VALUES (?)", "unique_value")

    finally:
        connection.close()


def test_sqlite_driver_cursor_management() -> None:
    """Test SQLite driver cursor management."""
    connection = sqlite3.connect(":memory:")

    simple_config = StatementConfig(
        dialect="sqlite",
        enable_caching=False,
        parameter_config=ParameterStyleConfig(
            default_parameter_style=ParameterStyle.QMARK, supported_parameter_styles={ParameterStyle.QMARK}
        ),
    )

    driver = SqliteDriver(connection, simple_config)

    try:
        with driver.with_cursor(connection) as cursor:
            assert cursor is not None
            assert hasattr(cursor, "execute")
            assert hasattr(cursor, "fetchall")

    finally:
        connection.close()


@pytest.mark.parametrize("script_statements", [1, 2, 5])
def test_adapter_script_execution_counts(statement_config_for_adapter: StatementConfig, script_statements: int) -> None:
    """Test script execution with different statement counts."""
    config = statement_config_for_adapter

    statements = [f"INSERT INTO users (name) VALUES ('user_{i}')" for i in range(script_statements)]

    script = "; ".join(statements) + ";"
    statement = SQL(script, statement_config=config)
    statement_as_script = statement.as_script()

    assert statement_as_script.is_script is True

    connection = sqlite3.connect(":memory:")
    try:
        sqlite_config = StatementConfig(
            enable_caching=False, parameter_config=ParameterStyleConfig(default_parameter_style=ParameterStyle.QMARK)
        )
        driver = SqliteDriver(connection, sqlite_config)
        split_statements = driver.split_script_statements(script, sqlite_config, strip_trailing_semicolon=True)
    finally:
        connection.close()

    non_empty_statements = [stmt for stmt in split_statements if stmt.strip()]
    assert len(non_empty_statements) == script_statements


@pytest.mark.parametrize("parameter_count", [0, 1, 3, 5])
def test_adapter_parameter_handling(
    adapter_config: "dict[str, Any]", statement_config_for_adapter: StatementConfig, parameter_count: int
) -> None:
    """Test parameter handling with different parameter counts."""
    config = statement_config_for_adapter

    if adapter_config["parameter_style"] == ParameterStyle.QMARK:
        placeholders = ", ".join(["?"] * parameter_count)
    elif adapter_config["parameter_style"] == ParameterStyle.NUMERIC:
        placeholders = ", ".join([f"${i + 1}" for i in range(parameter_count)])
    else:
        placeholders = ", ".join(["%s"] * parameter_count)

    if parameter_count == 0:
        query = "SELECT * FROM users"
        parameters = ()
    else:
        query = f"SELECT * FROM users WHERE id IN ({placeholders})"
        parameters = tuple(range(1, parameter_count + 1))

    statement = SQL(query, *parameters, statement_config=config)

    compiled_sql, compiled_params = statement.compile()
    assert isinstance(compiled_sql, str)

    if parameter_count == 0:
        assert compiled_params is None or len(compiled_params) == 0
    else:
        assert compiled_params is not None
        assert len(compiled_params) == parameter_count


def test_execution_result_creation() -> None:
    """Test ExecutionResult creation and properties."""
    connection = sqlite3.connect(":memory:")
    config = StatementConfig(
        enable_caching=False, parameter_config=ParameterStyleConfig(default_parameter_style=ParameterStyle.QMARK)
    )
    driver = SqliteDriver(connection, config)

    try:
        select_result = driver.create_execution_result(
            cursor_result="mock_cursor",
            selected_data=[(1,), (2,)],
            column_names=["id"],
            data_row_count=2,
            is_select_result=True,
            row_format="tuple",
        )

        assert isinstance(select_result, ExecutionResult)
        assert select_result.is_select_result is True
        assert select_result.selected_data == [(1,), (2,)]
        assert select_result.column_names == ["id"]
        assert select_result.data_row_count == 2

        insert_result = driver.create_execution_result(cursor_result="mock_cursor", rowcount_override=1)

        assert insert_result.is_select_result is False
        assert insert_result.rowcount_override == 1

        script_result = driver.create_execution_result(
            cursor_result="mock_cursor", statement_count=3, successful_statements=3, is_script_result=True
        )

        assert script_result.is_script_result is True
        assert script_result.statement_count == 3
        assert script_result.successful_statements == 3

        many_result = driver.create_execution_result(
            cursor_result="mock_cursor", rowcount_override=5, is_many_result=True
        )

        assert many_result.is_many_result is True
        assert many_result.rowcount_override == 5
    finally:
        connection.close()


def test_sql_result_building() -> None:
    """Test SQLResult building from ExecutionResult."""
    connection = sqlite3.connect(":memory:")
    config = StatementConfig(
        enable_caching=False, parameter_config=ParameterStyleConfig(default_parameter_style=ParameterStyle.QMARK)
    )
    driver = SqliteDriver(connection, config)

    try:
        statement = SQL("SELECT * FROM users", statement_config=config)
        execution_result = driver.create_execution_result(
            cursor_result="mock",
            selected_data=[(1, "test")],
            column_names=["id", "name"],
            data_row_count=1,
            is_select_result=True,
            row_format="tuple",
        )

        sql_result = driver.build_statement_result(statement, execution_result)
        assert isinstance(sql_result, SQLResult)
        assert sql_result.operation_type == "SELECT"
        assert sql_result.get_data() == [{"id": 1, "name": "test"}]
        assert sql_result.column_names == ["id", "name"]

        script_statement = SQL("INSERT INTO users VALUES (1, 'test');", statement_config=config, is_script=True)
        script_execution_result = driver.create_execution_result(
            cursor_result="mock", statement_count=1, successful_statements=1, is_script_result=True
        )

        script_sql_result = driver.build_statement_result(script_statement, script_execution_result)
        assert script_sql_result.operation_type == "SCRIPT"
        assert script_sql_result.total_statements == 1
        assert script_sql_result.successful_statements == 1
    finally:
        connection.close()


ADAPTER_CONFIG_MODULES = (
    "sqlspec.adapters.adbc.config",
    "sqlspec.adapters.aiomysql.config",
    "sqlspec.adapters.aiosqlite.config",
    "sqlspec.adapters.arrow_odbc.config",
    "sqlspec.adapters.asyncmy.config",
    "sqlspec.adapters.asyncpg.config",
    "sqlspec.adapters.bigquery.config",
    "sqlspec.adapters.cockroach_asyncpg.config",
    "sqlspec.adapters.cockroach_psycopg.config",
    "sqlspec.adapters.db2.config",
    "sqlspec.adapters.duckdb.config",
    "sqlspec.adapters.mssql_python.config",
    "sqlspec.adapters.mysqlconnector.config",
    "sqlspec.adapters.oracledb.config",
    "sqlspec.adapters.psqlpy.config",
    "sqlspec.adapters.psycopg.config",
    "sqlspec.adapters.pymssql.config",
    "sqlspec.adapters.pymysql.config",
    "sqlspec.adapters.spanner.config",
    "sqlspec.adapters.sqlite.config",
    "sqlspec.adapters.teradata.config",
)


def test_every_config_declares_type_coercion_capabilities() -> None:
    """Assert every concrete config class defines type_coercion_capabilities and it is not the base default object."""
    concrete_configs: list[type[DatabaseConfigProtocol[Any, Any, Any]]] = []

    for module_name in ADAPTER_CONFIG_MODULES:
        mod = importlib.import_module(module_name)
        for _, obj in inspect.getmembers(mod):
            if (
                isinstance(obj, type)
                and not isinstance(obj, (types.GenericAlias, getattr(types, "UnionType", ())))
                and issubclass(obj, DatabaseConfigProtocol)
                and obj is not DatabaseConfigProtocol
                and not inspect.isabstract(obj)
                and obj.__module__ == module_name
            ):
                concrete_configs.append(obj)

    assert len(concrete_configs) == 27

    for config_cls in concrete_configs:
        assert "type_coercion_capabilities" in config_cls.__dict__, (
            f"{config_cls.__name__} does not explicitly declare type_coercion_capabilities"
        )
        caps = config_cls.type_coercion_capabilities
        assert isinstance(caps, TypeCoercionCapabilities), (
            f"{config_cls.__name__}.type_coercion_capabilities is not TypeCoercionCapabilities"
        )
        assert caps is not DatabaseConfigProtocol.type_coercion_capabilities, (
            f"{config_cls.__name__}.type_coercion_capabilities is the base default object"
        )


def _litestar_store_targets() -> "list[tuple[str, str]]":
    """Return ``(adapter, config class)`` pairs for adapters that ship a Litestar store."""
    targets: list[tuple[str, str]] = []
    adapters_root = Path(sqlspec.adapters.__file__).parent
    for config_path in sorted(adapters_root.glob("*/config.py")):
        adapter = config_path.parent.name
        if not (config_path.parent / "litestar" / "store.py").exists():
            continue
        module = importlib.import_module(f"sqlspec.adapters.{adapter}.config")
        for name, value in vars(module).items():
            if (
                isinstance(value, type)
                and value.__module__ == module.__name__
                and name.endswith("Config")
                and issubclass(value, DatabaseConfigProtocol)
            ):
                targets.append((adapter, name))
    return targets


@pytest.mark.parametrize(("adapter", "config_name"), _litestar_store_targets())
def test_litestar_session_migration_resolves_store_class(adapter: str, config_name: str) -> None:
    """The Litestar session migration derives the store class name from the config class name."""
    store_name = config_name.replace("Config", "Store")
    store_module = importlib.import_module(f"sqlspec.adapters.{adapter}.litestar.store")

    assert hasattr(store_module, store_name), f"{adapter}: expected {store_name} in litestar/store.py"


LITESTAR_RENEWAL_STORE_SPECS: tuple[tuple[str, str, str], ...] = (
    ("sqlspec.adapters.asyncpg.litestar.store", "AsyncpgStore", "async_fetch"),
    ("sqlspec.adapters.psycopg.litestar.store", "PsycopgAsyncStore", "async_dict_cursor"),
    ("sqlspec.adapters.psycopg.litestar.store", "PsycopgSyncStore", "sync_dict_cursor"),
    ("sqlspec.adapters.psqlpy.litestar.store", "PsqlpyStore", "async_fetch"),
    ("sqlspec.adapters.cockroach_asyncpg.litestar.store", "CockroachAsyncpgStore", "async_fetch"),
    ("sqlspec.adapters.cockroach_psycopg.litestar.store", "CockroachPsycopgAsyncStore", "async_dict_cursor"),
    ("sqlspec.adapters.cockroach_psycopg.litestar.store", "CockroachPsycopgSyncStore", "sync_dict_cursor"),
    ("sqlspec.adapters.sqlite.litestar.store", "SqliteStore", "sqlite_conn"),
    ("sqlspec.adapters.aiosqlite.litestar.store", "AiosqliteStore", "aiosqlite_conn"),
    ("sqlspec.adapters.aiomysql.litestar.store", "AiomysqlStore", "async_tuple_cursor"),
    ("sqlspec.adapters.asyncmy.litestar.store", "AsyncmyStore", "async_tuple_cursor"),
    ("sqlspec.adapters.pymysql.litestar.store", "PyMysqlStore", "sync_dict_cursor"),
    ("sqlspec.adapters.mysqlconnector.litestar.store", "MysqlConnectorAsyncStore", "async_tuple_cursor"),
    ("sqlspec.adapters.mysqlconnector.litestar.store", "MysqlConnectorSyncStore", "sync_tuple_cursor"),
    ("sqlspec.adapters.mssql_python.litestar.store", "MssqlPythonStore", "sync_tuple_cursor"),
    ("sqlspec.adapters.pymssql.litestar.store", "PymssqlStore", "sync_tuple_cursor"),
    ("sqlspec.adapters.sybase.litestar.store", "SybaseStore", "sync_tuple_cursor"),
    ("sqlspec.adapters.sqlanywhere.litestar.store", "SqlanywhereStore", "sync_tuple_cursor"),
    ("sqlspec.adapters.oracledb.litestar.store", "OracleAsyncStore", "async_tuple_cursor"),
    ("sqlspec.adapters.oracledb.litestar.store", "OracleSyncStore", "sync_tuple_cursor"),
    ("sqlspec.adapters.db2.litestar.store", "Db2SyncStore", "sync_session"),
    ("sqlspec.adapters.db2.litestar.store", "Db2AsyncStore", "async_session"),
    ("sqlspec.adapters.bigquery.litestar.store", "BigQueryStore", "sync_session"),
    ("sqlspec.adapters.snowflake.litestar.store", "SnowflakeStore", "sync_session"),
    ("sqlspec.adapters.teradata.litestar.store", "TeradataStore", "sync_tuple_cursor"),
    ("sqlspec.adapters.duckdb.litestar.store", "DuckDBStore", "duckdb_conn"),
    ("sqlspec.adapters.arrow_odbc.litestar.store", "ArrowOdbcStore", "arrow_odbc_session"),
)


def _decode_sql(sql: object) -> str:
    return sql.decode("utf-8") if isinstance(sql, bytes) else str(sql)


class _RecordingSyncCursor:
    def __init__(self, executed: "list[str]", row: Any) -> None:
        self._executed = executed
        self._row = row

    def execute(self, sql: object, *_args: Any, **_kwargs: Any) -> Self:
        self._executed.append(_decode_sql(sql))
        return self

    def fetchone(self) -> Any:
        return self._row

    def close(self) -> None:
        return None

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_: object) -> None:
        return None


class _RecordingAsyncCursor:
    def __init__(self, executed: "list[str]", row: Any, *, record_on_init_sql: "str | None" = None) -> None:
        self._executed = executed
        self._row = row
        if record_on_init_sql is not None:
            self._executed.append(record_on_init_sql)

    def __await__(self) -> Generator[Any, None, Self]:
        async def _coro() -> Self:
            return self

        return _coro().__await__()

    async def execute(self, sql: object, *_args: Any, **_kwargs: Any) -> Self:
        self._executed.append(_decode_sql(sql))
        return self

    async def fetchone(self) -> Any:
        return self._row

    async def close(self) -> None:
        return None

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_: object) -> None:
        return None

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_: object) -> None:
        return None


def _build_litestar_renewal_config(mode: str, executed: "list[str]") -> Any:
    future = datetime.now(UTC) + timedelta(minutes=5)
    dict_row = {"data": b"payload", "expires_at": future}
    tuple_row = (b"payload", future)

    if mode == "async_fetch":

        class _AsyncFetchConn:
            async def fetchrow(self, sql: object, *_args: Any) -> dict[str, Any]:
                executed.append(_decode_sql(sql))
                return dict_row

            async def fetch(self, sql: object, *_args: Any) -> Any:
                executed.append(_decode_sql(sql))
                return SimpleNamespace(result=lambda: [dict_row])

            async def execute(self, sql: object, *_args: Any) -> None:
                executed.append(_decode_sql(sql))

        @asynccontextmanager
        async def provide_async_fetch(*_a: Any, **_kw: Any) -> AsyncIterator[Any]:
            yield _AsyncFetchConn()

        return SimpleNamespace(extension_config={"litestar": {}}, provide_connection=provide_async_fetch)

    if mode in {"async_dict_cursor", "async_tuple_cursor"}:
        row = dict_row if mode == "async_dict_cursor" else tuple_row

        class _AsyncCursorConn:
            def cursor(self, *_args: Any, **_kwargs: Any) -> _RecordingAsyncCursor:
                return _RecordingAsyncCursor(executed, row)

            async def execute(self, sql: object, *_args: Any) -> None:
                executed.append(_decode_sql(sql))

            async def commit(self) -> None:
                return None

            async def rollback(self) -> None:
                return None

        @asynccontextmanager
        async def provide_async_cursor(*_a: Any, **_kw: Any) -> AsyncIterator[Any]:
            yield _AsyncCursorConn()

        return SimpleNamespace(extension_config={"litestar": {}}, provide_connection=provide_async_cursor)

    if mode in {"sync_dict_cursor", "sync_tuple_cursor"}:
        row = dict_row if mode == "sync_dict_cursor" else tuple_row

        class _SyncCursorConn:
            autocommit = False

            def cursor(self, *_args: Any, **_kwargs: Any) -> _RecordingSyncCursor:
                return _RecordingSyncCursor(executed, row)

            def execute(self, sql: object, *_args: Any) -> _RecordingSyncCursor:
                executed.append(_decode_sql(sql))
                return _RecordingSyncCursor(executed, row)

            def commit(self) -> None:
                return None

            def rollback(self) -> None:
                return None

        @contextmanager
        def provide_sync_cursor(*_a: Any, **_kw: Any) -> Iterator[Any]:
            yield _SyncCursorConn()

        return SimpleNamespace(extension_config={"litestar": {}}, provide_connection=provide_sync_cursor)

    if mode in {"sqlite_conn", "duckdb_conn"}:
        direct_row = (b"payload", 2461500.5) if mode == "sqlite_conn" else (b"payload", future.isoformat())

        class _DirectSyncConn:
            in_transaction = False

            def execute(self, sql: object, *_args: Any) -> _RecordingSyncCursor:
                executed.append(_decode_sql(sql))
                return _RecordingSyncCursor(executed, direct_row)

            def commit(self) -> None:
                return None

        @contextmanager
        def provide_direct_sync(*_a: Any, **_kw: Any) -> Iterator[Any]:
            yield _DirectSyncConn()

        return SimpleNamespace(extension_config={"litestar": {}}, provide_connection=provide_direct_sync)

    if mode == "aiosqlite_conn":

        class _AiosqliteConn:
            in_transaction = False

            def execute(self, sql: object, *_args: Any) -> _RecordingAsyncCursor:
                return _RecordingAsyncCursor(executed, (b"payload", 2461500.5), record_on_init_sql=_decode_sql(sql))

            async def commit(self) -> None:
                return None

        @asynccontextmanager
        async def provide_aiosqlite(*_a: Any, **_kw: Any) -> AsyncIterator[Any]:
            yield _AiosqliteConn()

        return SimpleNamespace(extension_config={"litestar": {}}, provide_connection=provide_aiosqlite)

    if mode == "async_session":

        class _AsyncDriver:
            async def select_one_or_none(self, sql: object, *_args: Any, **_kwargs: Any) -> dict[str, Any]:
                executed.append(_decode_sql(sql))
                return dict_row

            async def execute(self, sql: object, *_args: Any, **_kwargs: Any) -> None:
                executed.append(_decode_sql(sql))

            async def commit(self) -> None:
                return None

        @asynccontextmanager
        async def provide_async_session(*_a: Any, **_kw: Any) -> AsyncIterator[Any]:
            yield _AsyncDriver()

        return SimpleNamespace(extension_config={"litestar": {}}, provide_session=provide_async_session)

    session_row = (
        {"data": base64.b64encode(b"payload").decode("ascii"), "expires_at": future}
        if mode == "arrow_odbc_session"
        else dict_row
    )

    class _SyncDriver:
        def select_one_or_none(self, sql: object, *_args: Any, **_kwargs: Any) -> dict[str, Any]:
            executed.append(_decode_sql(sql))
            return session_row

        def select_one(self, sql: object, *_args: Any, **_kwargs: Any) -> dict[str, Any]:
            executed.append(_decode_sql(sql))
            return session_row

        def execute(self, sql: object, *_args: Any, **_kwargs: Any) -> None:
            executed.append(_decode_sql(sql))

        def commit(self) -> None:
            return None

    @contextmanager
    def provide_sync_session(*_a: Any, **_kw: Any) -> Iterator[Any]:
        yield _SyncDriver()

    return SimpleNamespace(
        extension_config={"litestar": {}},
        statement_config=SimpleNamespace(dialect="mssql"),
        provide_session=provide_sync_session,
    )


@pytest.mark.anyio
@pytest.mark.parametrize(("module_name", "class_name", "mode"), LITESTAR_RENEWAL_STORE_SPECS)
@pytest.mark.parametrize(
    ("renew_for", "expect_renew"),
    [
        pytest.param(0, False, id="zero"),
        pytest.param(timedelta(0), False, id="zero-timedelta"),
        pytest.param(timedelta(milliseconds=500), True, id="subsecond-timedelta"),
        pytest.param(30, True, id="positive-int"),
    ],
)
async def test_litestar_stores_renew_only_for_positive_duration(
    module_name: str, class_name: str, mode: str, renew_for: "int | timedelta", expect_renew: bool
) -> None:
    """Every Litestar session store executes a renewal UPDATE only when renew_for is positive."""
    module = pytest.importorskip(module_name)
    store_cls = getattr(module, class_name)
    executed: list[str] = []
    config = _build_litestar_renewal_config(mode, executed)
    store = store_cls(config)

    result = await store.get("session-1", renew_for=renew_for)

    assert result == b"payload"
    update_count = sum(1 for sql in executed if sql.lstrip().upper().startswith("UPDATE"))
    assert update_count == (1 if expect_renew else 0)
