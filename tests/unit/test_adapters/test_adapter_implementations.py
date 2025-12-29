"""Parameterized tests for real adapter implementations."""

import operator
import sqlite3
from typing import Any

import pytest

from sqlspec.adapters.sqlite.driver import SqliteDriver
from sqlspec.core import SQL, ParameterStyle, ParameterStyleConfig, SQLResult, StatementConfig
from sqlspec.driver import ExecutionResult
from sqlspec.exceptions import SQLSpecError

pytestmark = pytest.mark.xdist_group("unit")


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
        "parameter_style": ParameterStyle.POSITIONAL_PYFORMAT,
        "dialect": "mysql",
        "example_queries": {
            "select": "SELECT * FROM users WHERE id = %s",
            "insert": "INSERT INTO users (name) VALUES (%s)",
            "update": "UPDATE users SET name = %s WHERE id = %s",
            "delete": "DELETE FROM users WHERE id = %s",
        },
    },
]


@pytest.fixture(params=ADAPTER_CONFIGS, ids=operator.itemgetter("name"))
def adapter_config(request: pytest.FixtureRequest) -> "dict[str, Any]":
    """Parameterized fixture providing different adapter configurations."""
    from typing import cast

    return cast("dict[str, Any]", request.param)


@pytest.fixture
def statement_config_for_adapter(adapter_config: "dict[str, Any]") -> StatementConfig:
    """Create statement config for specific adapter."""
    return StatementConfig(
        dialect=adapter_config["dialect"],
        enable_caching=False,
        parameter_config=ParameterStyleConfig(
            default_parameter_style=adapter_config["parameter_style"],
            supported_parameter_styles={adapter_config["parameter_style"]},
            default_execution_parameter_style=adapter_config["parameter_style"],
            supported_execution_parameter_styles={adapter_config["parameter_style"]},
        ),
    )


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
        assert "?" in statement.sql
    elif adapter_config["parameter_style"] == ParameterStyle.NUMERIC:
        statement = SQL(queries["select"], 1, statement_config=config)
        assert "$1" in statement.sql or "?" in statement.sql
    elif adapter_config["parameter_style"] == ParameterStyle.POSITIONAL_PYFORMAT:
        statement = SQL(queries["select"], 1, statement_config=config)
        assert "%s" in statement.sql or "?" in statement.sql


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
    sqlite_config = StatementConfig(
        enable_caching=False, parameter_config=ParameterStyleConfig(default_parameter_style=ParameterStyle.QMARK)
    )
    driver = SqliteDriver(connection, sqlite_config)
    split_statements = driver.split_script_statements(script, sqlite_config, strip_trailing_semicolon=True)
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
            selected_data=[{"id": 1}, {"id": 2}],
            column_names=["id"],
            data_row_count=2,
            is_select_result=True,
        )

        assert isinstance(select_result, ExecutionResult)
        assert select_result.is_select_result is True
        assert select_result.selected_data == [{"id": 1}, {"id": 2}]
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
            selected_data=[{"id": 1, "name": "test"}],
            column_names=["id", "name"],
            data_row_count=1,
            is_select_result=True,
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
