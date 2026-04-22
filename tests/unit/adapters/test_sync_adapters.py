# pyright: reportPrivateImportUsage = false, reportPrivateUsage = false
"""Tests for synchronous database adapters."""

import sqlite3
from typing import Any
from unittest.mock import patch

import pytest

from sqlspec.adapters.sqlite import SqliteDriver
from sqlspec.core import SQL, ParameterStyle, ParameterStyleConfig, SQLResult, StatementConfig, get_default_config
from sqlspec.driver import ExecutionResult
from sqlspec.exceptions import NotFoundError, SQLSpecError
from sqlspec.observability import ObservabilityConfig, ObservabilityRuntime
from sqlspec.typing import Empty

pytestmark = pytest.mark.xdist_group("adapter_unit")
__all__ = ()


def test_sync_driver_initialization() -> None:
    """Test basic sync driver initialization."""
    conn = sqlite3.connect(":memory:")
    driver = SqliteDriver(conn)

    assert driver.connection is conn
    assert driver.dialect == "sqlite"
    assert driver.statement_config.dialect == "sqlite"
    assert driver.statement_config.parameter_config.default_parameter_style == ParameterStyle.QMARK
    conn.close()


def test_sync_driver_with_custom_config() -> None:
    """Test sync driver initialization with custom statement config."""
    conn = sqlite3.connect(":memory:")
    custom_config = StatementConfig(
        dialect="postgresql",
        parameter_config=ParameterStyleConfig(
            default_parameter_style=ParameterStyle.NUMERIC, supported_parameter_styles={ParameterStyle.NUMERIC}
        ),
    )

    driver = SqliteDriver(conn, custom_config)
    assert driver.statement_config.dialect == "postgresql"
    assert driver.statement_config.parameter_config.default_parameter_style == ParameterStyle.NUMERIC
    conn.close()


def test_sync_driver_fast_path_flag_default() -> None:
    conn = sqlite3.connect(":memory:")
    driver = SqliteDriver(conn)

    assert driver._stmt_cache_enabled is True
    conn.close()


def test_sync_driver_fast_path_flag_disabled_by_transformer() -> None:
    conn = sqlite3.connect(":memory:")

    def transformer(expression: Any, context: Any) -> "tuple[Any, Any]":
        return expression, context

    custom_config = StatementConfig(
        dialect="sqlite",
        parameter_config=ParameterStyleConfig(
            default_parameter_style=ParameterStyle.QMARK, supported_parameter_styles={ParameterStyle.QMARK}
        ),
        statement_transformers=(transformer,),
    )
    driver = SqliteDriver(conn, custom_config)

    assert driver._stmt_cache_enabled is False
    conn.close()


def test_sync_driver_fast_path_flag_disabled_by_observability() -> None:
    conn = sqlite3.connect(":memory:")
    driver = SqliteDriver(conn)
    runtime = ObservabilityRuntime(ObservabilityConfig(print_sql=True))

    driver.attach_observability(runtime)

    assert driver._stmt_cache_enabled is False
    conn.close()


def test_sync_driver_with_cursor(sqlite_sync_driver: SqliteDriver) -> None:
    """Test cursor context manager functionality."""
    with sqlite_sync_driver.with_cursor(sqlite_sync_driver.connection) as cursor:
        assert hasattr(cursor, "execute")
        assert hasattr(cursor, "fetchall")
        assert cursor.connection is sqlite_sync_driver.connection


def test_sync_driver_database_exception_handling(sqlite_sync_driver: SqliteDriver) -> None:
    """Test database exception handling with deferred exception pattern."""
    exc_handler = sqlite_sync_driver.handle_database_exceptions()
    with exc_handler:
        pass
    assert exc_handler.pending_exception is None

    exc_handler = sqlite_sync_driver.handle_database_exceptions()
    with exc_handler:
        raise sqlite3.Error("Test error")

    assert exc_handler.pending_exception is not None
    assert isinstance(exc_handler.pending_exception, SQLSpecError)


def test_sync_driver_dispatch_execute_select(sqlite_sync_driver: SqliteDriver) -> None:
    """Test dispatch_execute method with SELECT query."""
    statement = SQL("SELECT id, name FROM users", statement_config=sqlite_sync_driver.statement_config)

    with sqlite_sync_driver.with_cursor(sqlite_sync_driver.connection) as cursor:
        result = sqlite_sync_driver.dispatch_execute(cursor, statement)

    assert isinstance(result, ExecutionResult)
    assert result.is_select_result is True
    assert result.is_script_result is False
    assert result.is_many_result is False
    assert result.selected_data == [(1, "test"), (2, "example")]
    assert result.column_names == ["id", "name"]
    assert result.data_row_count == 2


def test_sync_driver_dispatch_execute_insert(sqlite_sync_driver: SqliteDriver) -> None:
    """Test dispatch_execute method with INSERT query."""
    statement = SQL(
        "INSERT INTO users (name) VALUES (?)", "new_user", statement_config=sqlite_sync_driver.statement_config
    )

    with sqlite_sync_driver.with_cursor(sqlite_sync_driver.connection) as cursor:
        result = sqlite_sync_driver.dispatch_execute(cursor, statement)

    assert isinstance(result, ExecutionResult)
    assert result.is_select_result is False
    assert result.is_script_result is False
    assert result.is_many_result is False
    assert result.rowcount_override == 1


def test_sync_driver_execute_many(sqlite_sync_driver: SqliteDriver) -> None:
    """Test _execute_many method."""
    statement = SQL(
        "INSERT INTO users (name) VALUES (?)",
        [["alice"], ["bob"], ["charlie"]],
        statement_config=sqlite_sync_driver.statement_config,
        is_many=True,
    )
    with sqlite_sync_driver.with_cursor(sqlite_sync_driver.connection) as cursor:
        result = sqlite_sync_driver.dispatch_execute_many(cursor, statement)

    assert isinstance(result, ExecutionResult)
    assert result.is_many_result is True
    assert result.rowcount_override == 3


def test_sync_driver_execute_many_no_parameters(sqlite_sync_driver: SqliteDriver) -> None:
    """Test _execute_many method fails without parameters."""
    statement = SQL(
        "INSERT INTO users (name) VALUES (?)", statement_config=sqlite_sync_driver.statement_config, is_many=True
    )
    with sqlite_sync_driver.with_cursor(sqlite_sync_driver.connection) as cursor:
        with pytest.raises(ValueError, match="execute_many requires parameters"):
            sqlite_sync_driver.dispatch_execute_many(cursor, statement)


def test_sync_driver_execute_script(sqlite_sync_driver: SqliteDriver) -> None:
    """Test _execute_script method."""
    script = """
    INSERT INTO users (name) VALUES ('alice');
    INSERT INTO users (name) VALUES ('bob');
    UPDATE users SET name = 'updated';
    """
    statement = SQL(script, statement_config=sqlite_sync_driver.statement_config, is_script=True)

    with sqlite_sync_driver.with_cursor(sqlite_sync_driver.connection) as cursor:
        result = sqlite_sync_driver.dispatch_execute_script(cursor, statement)

    assert isinstance(result, ExecutionResult)
    assert result.is_script_result is True
    assert result.statement_count == 3
    assert result.successful_statements == 3


def test_sync_driver_dispatch_statement_execution_select(sqlite_sync_driver: SqliteDriver) -> None:
    """Test dispatch_statement_execution with SELECT statement."""
    statement = SQL("SELECT * FROM users", statement_config=sqlite_sync_driver.statement_config)

    result = sqlite_sync_driver.dispatch_statement_execution(statement, sqlite_sync_driver.connection)

    assert isinstance(result, SQLResult)
    assert result.operation_type == "SELECT"
    assert len(result.get_data()) == 2
    assert result.get_data()[0]["id"] == 1
    assert result.get_data()[0]["name"] == "test"


def test_sync_driver_dispatch_statement_execution_insert(sqlite_sync_driver: SqliteDriver) -> None:
    """Test dispatch_statement_execution with INSERT statement."""
    statement = SQL(
        "INSERT INTO users (name) VALUES (?)", "new_user", statement_config=sqlite_sync_driver.statement_config
    )

    result = sqlite_sync_driver.dispatch_statement_execution(statement, sqlite_sync_driver.connection)

    assert isinstance(result, SQLResult)
    assert result.operation_type == "INSERT"
    assert result.rows_affected == 1


def test_sync_driver_dispatch_statement_execution_script(sqlite_sync_driver: SqliteDriver) -> None:
    """Test dispatch_statement_execution with script."""
    script = "INSERT INTO users (name) VALUES ('alice'); INSERT INTO users (name) VALUES ('bob');"
    statement = SQL(script, statement_config=sqlite_sync_driver.statement_config, is_script=True)

    result = sqlite_sync_driver.dispatch_statement_execution(statement, sqlite_sync_driver.connection)

    assert isinstance(result, SQLResult)
    assert result.operation_type == "SCRIPT"
    assert result.total_statements == 2
    assert result.successful_statements == 2


def test_sync_driver_dispatch_statement_execution_many(sqlite_sync_driver: SqliteDriver) -> None:
    """Test dispatch_statement_execution with execute_many."""
    statement = SQL(
        "INSERT INTO users (name) VALUES (?)",
        [["alice"], ["bob"]],
        statement_config=sqlite_sync_driver.statement_config,
        is_many=True,
    )

    result = sqlite_sync_driver.dispatch_statement_execution(statement, sqlite_sync_driver.connection)

    assert isinstance(result, SQLResult)
    assert result.operation_type == "INSERT"
    assert result.rows_affected == 2


def test_sync_driver_releases_pooled_statement(sqlite_sync_driver: SqliteDriver) -> None:
    """Pooled statements should be reset after dispatch execution."""
    seed = "SELECT * FROM users WHERE id = ?"
    sqlite_sync_driver.prepare_statement(seed, (1,), statement_config=sqlite_sync_driver.statement_config, kwargs={})
    pooled = sqlite_sync_driver.prepare_statement(
        seed, (2,), statement_config=sqlite_sync_driver.statement_config, kwargs={}
    )

    assert pooled._pooled is True

    sqlite_sync_driver.dispatch_statement_execution(pooled, sqlite_sync_driver.connection)

    assert pooled._raw_sql == ""
    assert pooled._processed_state is Empty
    assert pooled._filters == []
    assert pooled._statement_config is get_default_config()


def test_sync_driver_transaction_management(sqlite_sync_driver: SqliteDriver) -> None:
    """Test transaction management methods."""
    sqlite_sync_driver.begin()
    sqlite_sync_driver.execute("INSERT INTO users (name) VALUES ('trans')")
    sqlite_sync_driver.commit()

    res = sqlite_sync_driver.select_value("SELECT COUNT(*) FROM users WHERE name = 'trans'")
    assert res == 1

    sqlite_sync_driver.begin()
    sqlite_sync_driver.execute("INSERT INTO users (name) VALUES ('rolledback')")
    sqlite_sync_driver.rollback()

    res = sqlite_sync_driver.select_value("SELECT COUNT(*) FROM users WHERE name = 'rolledback'")
    assert res == 0


def test_sync_driver_execute_method(sqlite_sync_driver: SqliteDriver) -> None:
    """Test high-level execute method."""
    result = sqlite_sync_driver.execute("SELECT * FROM users WHERE id = ?", 1)

    assert isinstance(result, SQLResult)
    assert result.operation_type == "SELECT"
    assert len(result.get_data()) == 1


def test_sync_driver_execute_many_method(sqlite_sync_driver: SqliteDriver) -> None:
    """Test high-level execute_many method."""
    parameters = [["alice"], ["bob"], ["charlie"]]
    result = sqlite_sync_driver.execute_many("INSERT INTO users (name) VALUES (?)", parameters)

    assert isinstance(result, SQLResult)
    assert result.operation_type == "INSERT"
    assert result.rows_affected == 3


def test_sync_driver_execute_script_method(sqlite_sync_driver: SqliteDriver) -> None:
    """Test high-level execute_script method."""
    script = "INSERT INTO users (name) VALUES ('alice'); UPDATE users SET name = 'updated';"
    result = sqlite_sync_driver.execute_script(script)

    assert isinstance(result, SQLResult)
    assert result.operation_type == "SCRIPT"
    assert result.total_statements == 2
    assert result.successful_statements == 2


@pytest.mark.parametrize(
    ("method_name", "call_args"),
    [
        pytest.param("execute", ("SELECT * FROM users WHERE id = ?", 1), id="execute"),
        pytest.param("execute_script", ("INSERT INTO users (name) VALUES ('alice');",), id="execute_script"),
    ],
)
def test_sync_driver_execution_wrappers_reraise_deferred_database_errors(
    sqlite_sync_driver: SqliteDriver, method_name: str, call_args: tuple[Any, ...]
) -> None:
    """Test wrapper methods re-raise mapped errors after the exception context exits."""
    # Patch all potential entry points for the different method types
    with (
        patch.object(
            sqlite_sync_driver, "dispatch_statement_execution", side_effect=sqlite3.Error("Test wrapper error")
        ),
        patch.object(sqlite_sync_driver, "dispatch_execute_many", side_effect=sqlite3.Error("Test wrapper error")),
        patch.object(sqlite_sync_driver, "dispatch_execute_script", side_effect=sqlite3.Error("Test wrapper error")),
    ):
        method = getattr(sqlite_sync_driver, method_name)

        with pytest.raises(SQLSpecError):
            method(*call_args)


def test_sync_driver_select_one(sqlite_sync_driver: SqliteDriver) -> None:
    """Test select_one method - expects error when multiple rows returned."""
    with pytest.raises(ValueError, match="Multiple results found"):
        sqlite_sync_driver.select_one("SELECT * FROM users")


def test_sync_driver_select_one_no_results(sqlite_sync_driver: SqliteDriver) -> None:
    """Test select_one method with no results."""
    with pytest.raises(NotFoundError, match="No rows found"):
        sqlite_sync_driver.select_one("SELECT * FROM users WHERE id = ?", 999)


def test_sync_driver_select_one_or_none(sqlite_sync_driver: SqliteDriver) -> None:
    """Test select_one_or_none method - expects error when multiple rows returned."""
    with pytest.raises(ValueError, match="Multiple results found"):
        sqlite_sync_driver.select_one_or_none("SELECT * FROM users")


def test_sync_driver_select_one_or_none_no_results(sqlite_sync_driver: SqliteDriver) -> None:
    """Test select_one_or_none method with no results."""
    result = sqlite_sync_driver.select_one_or_none("SELECT * FROM users WHERE id = ?", 999)
    assert result is None


def test_sync_driver_select(sqlite_sync_driver: SqliteDriver) -> None:
    """Test select method."""
    result: list[Any] = sqlite_sync_driver.select("SELECT * FROM users")

    assert isinstance(result, list)
    assert len(result) == 2
    assert result[0]["id"] == 1
    assert result[1]["id"] == 2


def test_sync_driver_select_value(sqlite_sync_driver: SqliteDriver) -> None:
    """Test select_value method."""
    result = sqlite_sync_driver.select_value("SELECT COUNT(*) FROM users")
    assert result == 2


def test_sync_driver_select_value_no_results(sqlite_sync_driver: SqliteDriver) -> None:
    """Test select_value method with no results."""
    with pytest.raises(NotFoundError, match="No rows found"):
        sqlite_sync_driver.select_value("SELECT id FROM users WHERE id = 999")


def test_sync_driver_select_value_or_none_no_results(sqlite_sync_driver: SqliteDriver) -> None:
    """Test select_value_or_none method with no results."""
    result = sqlite_sync_driver.select_value_or_none("SELECT id FROM users WHERE id = 999")
    assert result is None


@pytest.mark.parametrize(
    "parameter_style,expected_style",
    [
        pytest.param(ParameterStyle.QMARK, ParameterStyle.QMARK, id="qmark"),
        pytest.param(ParameterStyle.NAMED_COLON, ParameterStyle.NAMED_COLON, id="named_colon"),
    ],
)
def test_sync_driver_parameter_styles(
    sqlite_sync_driver: SqliteDriver, parameter_style: ParameterStyle, expected_style: ParameterStyle
) -> None:
    """Test different parameter styles are handled correctly."""
    config = StatementConfig(
        dialect="sqlite",
        parameter_config=ParameterStyleConfig(
            default_parameter_style=parameter_style,
            supported_parameter_styles={parameter_style},
            default_execution_parameter_style=ParameterStyle.QMARK,
            supported_execution_parameter_styles={ParameterStyle.QMARK},
        ),
    )

    sqlite_sync_driver.statement_config = config
    assert sqlite_sync_driver.statement_config.parameter_config.default_parameter_style == expected_style

    if parameter_style == ParameterStyle.QMARK:
        statement = SQL("SELECT * FROM users WHERE id = ?", 1, statement_config=config)
    else:
        statement = SQL("SELECT * FROM users WHERE id = :id", {"id": 1}, statement_config=config)

    result = sqlite_sync_driver.dispatch_statement_execution(statement, sqlite_sync_driver.connection)
    assert isinstance(result, SQLResult)


def test_sync_driver_different_dialects(sqlite_sync_driver: SqliteDriver) -> None:
    """Test sync driver works with different SQL dialects."""
    config = StatementConfig(
        dialect="sqlite",
        parameter_config=ParameterStyleConfig(
            default_parameter_style=ParameterStyle.QMARK, supported_parameter_styles={ParameterStyle.QMARK}
        ),
    )

    sqlite_sync_driver.statement_config = config
    result = sqlite_sync_driver.execute("SELECT 1 as test")
    assert isinstance(result, SQLResult)


def test_sync_driver_create_execution_result(sqlite_sync_driver: SqliteDriver) -> None:
    """Test create_execution_result method."""
    with sqlite_sync_driver.with_cursor(sqlite_sync_driver.connection) as cursor:
        result = sqlite_sync_driver.create_execution_result(
            cursor,
            selected_data=[(1,), (2,)],
            column_names=["id"],
            data_row_count=2,
            is_select_result=True,
            row_format="tuple",
        )

        assert result.is_select_result is True
        assert result.selected_data == [(1,), (2,)]
        assert result.column_names == ["id"]
        assert result.data_row_count == 2

        result = sqlite_sync_driver.create_execution_result(cursor, rowcount_override=1)
        assert result.is_select_result is False
        assert result.rowcount_override == 1


def test_sync_driver_build_statement_result(sqlite_sync_driver: SqliteDriver) -> None:
    """Test build_statement_result method."""
    statement = SQL("SELECT * FROM users", statement_config=sqlite_sync_driver.statement_config)
    with sqlite_sync_driver.with_cursor(sqlite_sync_driver.connection) as cursor:
        execution_result = sqlite_sync_driver.create_execution_result(
            cursor,
            selected_data=[(1,)],
            column_names=["id"],
            data_row_count=1,
            is_select_result=True,
            row_format="tuple",
        )

        sql_result = sqlite_sync_driver.build_statement_result(statement, execution_result)
        assert isinstance(sql_result, SQLResult)
        assert sql_result.operation_type == "SELECT"
        assert sql_result.get_data() == [{"id": 1}]
        assert sql_result.column_names == ["id"]


def test_sync_driver_special_handling_integration(sqlite_sync_driver: SqliteDriver) -> None:
    """Test that dispatch_special_handling is called during dispatch."""
    statement = SQL("SELECT * FROM users", statement_config=sqlite_sync_driver.statement_config)

    with patch.object(sqlite_sync_driver, "dispatch_special_handling", return_value=None) as mock_special:
        result = sqlite_sync_driver.dispatch_statement_execution(statement, sqlite_sync_driver.connection)

        assert isinstance(result, SQLResult)
        mock_special.assert_called_once()


def test_sync_driver_error_handling_in_dispatch(sqlite_sync_driver: SqliteDriver) -> None:
    """Test error handling during statement dispatch."""
    statement = SQL("SELECT * FROM users", statement_config=sqlite_sync_driver.statement_config)

    with patch.object(sqlite_sync_driver, "dispatch_execute", side_effect=sqlite3.Error("Test error")):
        with pytest.raises(SQLSpecError):
            sqlite_sync_driver.dispatch_statement_execution(statement, sqlite_sync_driver.connection)
