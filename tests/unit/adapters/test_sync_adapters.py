# pyright: reportPrivateImportUsage = false, reportPrivateUsage = false
"""Tests for synchronous database adapters."""

from typing import Any
from unittest.mock import Mock, patch

import pytest

from sqlspec.core import SQL, ParameterStyle, ParameterStyleConfig, SQLResult, StatementConfig
from sqlspec.driver import ExecutionResult
from sqlspec.exceptions import NotFoundError, SQLSpecError
from tests.unit.adapters.conftest import MockSyncConnection, MockSyncDriver

pytestmark = pytest.mark.xdist_group("adapter_unit")
__all__ = ()


def test_sync_driver_initialization(mock_sync_connection: MockSyncConnection) -> None:
    """Test basic sync driver initialization."""
    driver = MockSyncDriver(mock_sync_connection)

    assert driver.connection is mock_sync_connection
    assert driver.dialect == "sqlite"
    assert driver.statement_config.dialect == "sqlite"
    assert driver.statement_config.parameter_config.default_parameter_style == ParameterStyle.QMARK


def test_sync_driver_with_custom_config(mock_sync_connection: MockSyncConnection) -> None:
    """Test sync driver initialization with custom statement config."""
    custom_config = StatementConfig(
        dialect="postgresql",
        parameter_config=ParameterStyleConfig(
            default_parameter_style=ParameterStyle.NUMERIC, supported_parameter_styles={ParameterStyle.NUMERIC}
        ),
    )

    driver = MockSyncDriver(mock_sync_connection, custom_config)
    assert driver.statement_config.dialect == "postgresql"
    assert driver.statement_config.parameter_config.default_parameter_style == ParameterStyle.NUMERIC


def test_sync_driver_with_cursor(mock_sync_driver: MockSyncDriver) -> None:
    """Test cursor context manager functionality."""
    with mock_sync_driver.with_cursor(mock_sync_driver.connection) as cursor:
        assert hasattr(cursor, "connection")
        assert hasattr(cursor, "execute")
        assert hasattr(cursor, "fetchall")
        assert cursor.connection is mock_sync_driver.connection


def test_sync_driver_database_exception_handling(mock_sync_driver: MockSyncDriver) -> None:
    """Test database exception handling with deferred exception pattern.

    The deferred pattern stores exceptions in `pending_exception` instead of
    raising from `__exit__`, allowing compiled code to raise safely.
    """
    exc_handler = mock_sync_driver.handle_database_exceptions()
    with exc_handler:
        pass
    assert exc_handler.pending_exception is None

    exc_handler = mock_sync_driver.handle_database_exceptions()
    with exc_handler:
        raise ValueError("Test error")

    assert exc_handler.pending_exception is not None
    assert isinstance(exc_handler.pending_exception, SQLSpecError)
    assert "Mock database error" in str(exc_handler.pending_exception)

    with pytest.raises(SQLSpecError, match="Mock database error"):
        raise exc_handler.pending_exception


def test_sync_driverdispatch_execute_select(mock_sync_driver: MockSyncDriver) -> None:
    """Test dispatch_execute method with SELECT query."""
    statement = SQL("SELECT id, name FROM users", statement_config=mock_sync_driver.statement_config)

    with mock_sync_driver.with_cursor(mock_sync_driver.connection) as cursor:
        result = mock_sync_driver.dispatch_execute(cursor, statement)

    assert isinstance(result, ExecutionResult)
    assert result.is_select_result is True
    assert result.is_script_result is False
    assert result.is_many_result is False
    assert result.selected_data == [{"id": 1, "name": "test"}, {"id": 2, "name": "example"}]
    assert result.column_names == ["id", "name"]
    assert result.data_row_count == 2


def test_sync_driverdispatch_execute_insert(mock_sync_driver: MockSyncDriver) -> None:
    """Test dispatch_execute method with INSERT query."""
    statement = SQL("INSERT INTO users (name) VALUES (?)", "test", statement_config=mock_sync_driver.statement_config)

    with mock_sync_driver.with_cursor(mock_sync_driver.connection) as cursor:
        result = mock_sync_driver.dispatch_execute(cursor, statement)

    assert isinstance(result, ExecutionResult)
    assert result.is_select_result is False
    assert result.is_script_result is False
    assert result.is_many_result is False
    assert result.rowcount_override == 1
    assert result.selected_data is None


def test_sync_driver_execute_many(mock_sync_driver: MockSyncDriver) -> None:
    """Test _execute_many method."""
    statement = SQL(
        "INSERT INTO users (name) VALUES (?)",
        [["alice"], ["bob"], ["charlie"]],
        statement_config=mock_sync_driver.statement_config,
        is_many=True,
    )
    with mock_sync_driver.with_cursor(mock_sync_driver.connection) as cursor:
        result = mock_sync_driver.dispatch_execute_many(cursor, statement)

    assert isinstance(result, ExecutionResult)
    assert result.is_many_result is True
    assert result.is_select_result is False
    assert result.is_script_result is False
    assert result.rowcount_override == 3
    assert mock_sync_driver.connection.execute_many_count == 1


def test_sync_driver_execute_many_no_parameters(mock_sync_driver: MockSyncDriver) -> None:
    """Test _execute_many method fails without parameters."""
    statement = SQL(
        "INSERT INTO users (name) VALUES (?)", statement_config=mock_sync_driver.statement_config, is_many=True
    )
    with mock_sync_driver.with_cursor(mock_sync_driver.connection) as cursor:
        with pytest.raises(ValueError, match="execute_many requires parameters"):
            mock_sync_driver.dispatch_execute_many(cursor, statement)


def test_sync_driver_execute_script(mock_sync_driver: MockSyncDriver) -> None:
    """Test _execute_script method."""
    script = """
    INSERT INTO users (name) VALUES ('alice');
    INSERT INTO users (name) VALUES ('bob');
    UPDATE users SET active = 1;
    """
    statement = SQL(script, statement_config=mock_sync_driver.statement_config, is_script=True)

    with mock_sync_driver.with_cursor(mock_sync_driver.connection) as cursor:
        result = mock_sync_driver.dispatch_execute_script(cursor, statement)

    assert isinstance(result, ExecutionResult)
    assert result.is_script_result is True
    assert result.is_select_result is False
    assert result.is_many_result is False
    assert result.statement_count == 3
    assert result.successful_statements == 3


def test_sync_driver_dispatch_statement_execution_select(mock_sync_driver: MockSyncDriver) -> None:
    """Test dispatch_statement_execution with SELECT statement."""
    statement = SQL("SELECT * FROM users", statement_config=mock_sync_driver.statement_config)

    result = mock_sync_driver.dispatch_statement_execution(statement, mock_sync_driver.connection)

    assert isinstance(result, SQLResult)
    assert result.operation_type == "SELECT"
    assert len(result.get_data()) == 2
    assert result.get_data()[0]["id"] == 1
    assert result.get_data()[0]["name"] == "test"


def test_sync_driver_dispatch_statement_execution_insert(mock_sync_driver: MockSyncDriver) -> None:
    """Test dispatch_statement_execution with INSERT statement."""
    statement = SQL("INSERT INTO users (name) VALUES (?)", "test", statement_config=mock_sync_driver.statement_config)

    result = mock_sync_driver.dispatch_statement_execution(statement, mock_sync_driver.connection)

    assert isinstance(result, SQLResult)
    assert result.operation_type == "INSERT"
    assert result.rows_affected == 1
    assert len(result.get_data()) == 0


def test_sync_driver_dispatch_statement_execution_script(mock_sync_driver: MockSyncDriver) -> None:
    """Test dispatch_statement_execution with script."""
    script = "INSERT INTO users (name) VALUES ('alice'); INSERT INTO users (name) VALUES ('bob');"
    statement = SQL(script, statement_config=mock_sync_driver.statement_config, is_script=True)

    result = mock_sync_driver.dispatch_statement_execution(statement, mock_sync_driver.connection)

    assert isinstance(result, SQLResult)
    assert result.operation_type == "SCRIPT"
    assert result.total_statements == 2
    assert result.successful_statements == 2


def test_sync_driver_dispatch_statement_execution_many(mock_sync_driver: MockSyncDriver) -> None:
    """Test dispatch_statement_execution with execute_many."""
    statement = SQL(
        "INSERT INTO users (name) VALUES (?)",
        [["alice"], ["bob"]],
        statement_config=mock_sync_driver.statement_config,
        is_many=True,
    )

    result = mock_sync_driver.dispatch_statement_execution(statement, mock_sync_driver.connection)

    assert isinstance(result, SQLResult)
    assert result.operation_type == "INSERT"
    assert result.rows_affected == 2


def test_sync_driver_transaction_management(mock_sync_driver: MockSyncDriver) -> None:
    """Test transaction management methods."""
    connection = mock_sync_driver.connection

    mock_sync_driver.begin()
    assert connection.in_transaction is True

    mock_sync_driver.commit()
    assert connection.in_transaction is False

    mock_sync_driver.begin()
    assert connection.in_transaction is True
    mock_sync_driver.rollback()
    assert connection.in_transaction is False


def test_sync_driver_execute_method(mock_sync_driver: MockSyncDriver) -> None:
    """Test high-level execute method."""
    result = mock_sync_driver.execute("SELECT * FROM users WHERE id = ?", 1)

    assert isinstance(result, SQLResult)
    assert result.operation_type == "SELECT"
    assert len(result.get_data()) == 2


def test_sync_driver_execute_many_method(mock_sync_driver: MockSyncDriver) -> None:
    """Test high-level execute_many method."""
    parameters = [["alice"], ["bob"], ["charlie"]]
    result = mock_sync_driver.execute_many("INSERT INTO users (name) VALUES (?)", parameters)

    assert isinstance(result, SQLResult)
    assert result.operation_type == "INSERT"
    assert result.rows_affected == 3


def test_sync_driver_execute_script_method(mock_sync_driver: MockSyncDriver) -> None:
    """Test high-level execute_script method."""
    script = "INSERT INTO users (name) VALUES ('alice'); UPDATE users SET active = 1;"
    result = mock_sync_driver.execute_script(script)

    assert isinstance(result, SQLResult)
    assert result.operation_type == "SCRIPT"
    assert result.total_statements == 2
    assert result.successful_statements == 2


def test_sync_driver_select_one(mock_sync_driver: MockSyncDriver) -> None:
    """Test select_one method - expects error when multiple rows returned."""
    with pytest.raises(ValueError, match="Multiple results found"):
        mock_sync_driver.select_one("SELECT * FROM users WHERE id = ?", 1)


def test_sync_driver_select_one_no_results(mock_sync_driver: MockSyncDriver) -> None:
    """Test select_one method with no results."""

    with patch.object(mock_sync_driver, "execute") as mock_execute:
        mock_result = Mock(spec=SQLResult)
        mock_result.one.side_effect = ValueError("No result found, exactly one row expected")
        mock_execute.return_value = mock_result

        with pytest.raises(NotFoundError, match="No rows found"):
            mock_sync_driver.select_one("SELECT * FROM users WHERE id = ?", 999)


def test_sync_driver_select_one_multiple_results(mock_sync_driver: MockSyncDriver) -> None:
    """Test select_one method with multiple results."""

    with patch.object(mock_sync_driver, "execute") as mock_execute:
        mock_result = Mock(spec=SQLResult)
        mock_result.one.side_effect = ValueError("Multiple results found (3), exactly one row expected")
        mock_execute.return_value = mock_result

        with pytest.raises(ValueError, match="Multiple results found"):
            mock_sync_driver.select_one("SELECT * FROM users")


def test_sync_driver_select_one_or_none(mock_sync_driver: MockSyncDriver) -> None:
    """Test select_one_or_none method - expects error when multiple rows returned."""
    with pytest.raises(ValueError, match="Multiple results found"):
        mock_sync_driver.select_one_or_none("SELECT * FROM users WHERE id = ?", 1)


def test_sync_driver_select_one_or_none_no_results(mock_sync_driver: MockSyncDriver) -> None:
    """Test select_one_or_none method with no results."""
    with patch.object(mock_sync_driver, "execute") as mock_execute:
        mock_result = Mock(spec=SQLResult)
        mock_result.one_or_none.return_value = None
        mock_execute.return_value = mock_result

        result = mock_sync_driver.select_one_or_none("SELECT * FROM users WHERE id = ?", 999)
        assert result is None


def test_sync_driver_select_one_or_none_multiple_results(mock_sync_driver: MockSyncDriver) -> None:
    """Test select_one_or_none method with multiple results."""
    with patch.object(mock_sync_driver, "execute") as mock_execute:
        mock_result = Mock(spec=SQLResult)
        mock_result.one_or_none.side_effect = ValueError("Multiple results found (2), at most one row expected")
        mock_execute.return_value = mock_result

        with pytest.raises(ValueError, match="Multiple results found"):
            mock_sync_driver.select_one_or_none("SELECT * FROM users")


def test_sync_driver_select(mock_sync_driver: MockSyncDriver) -> None:
    """Test select method."""
    result: list[Any] = mock_sync_driver.select("SELECT * FROM users")

    assert isinstance(result, list)
    assert len(result) == 2
    assert result[0]["id"] == 1
    assert result[1]["id"] == 2


def test_sync_driver_select_value(mock_sync_driver: MockSyncDriver) -> None:
    """Test select_value method."""

    with patch.object(mock_sync_driver, "execute") as mock_execute:
        mock_result = Mock(spec=SQLResult)
        mock_result.scalar.return_value = 42
        mock_execute.return_value = mock_result

        result = mock_sync_driver.select_value("SELECT COUNT(*) as count FROM users")
        assert result == 42


def test_sync_driver_select_value_no_results(mock_sync_driver: MockSyncDriver) -> None:
    """Test select_value method with no results."""
    with patch.object(mock_sync_driver, "execute") as mock_execute:
        mock_result = Mock(spec=SQLResult)
        mock_result.scalar.side_effect = ValueError("No result found, exactly one row expected")
        mock_execute.return_value = mock_result

        with pytest.raises(NotFoundError, match="No rows found"):
            mock_sync_driver.select_value("SELECT COUNT(*) FROM users WHERE id = 999")


def test_sync_driver_select_value_or_none(mock_sync_driver: MockSyncDriver) -> None:
    """Test select_value_or_none method - expects error when multiple rows returned."""
    with pytest.raises(ValueError, match="Multiple results found"):
        mock_sync_driver.select_value_or_none("SELECT * FROM users WHERE id = ?", 1)


def test_sync_driver_select_value_or_none_no_results(mock_sync_driver: MockSyncDriver) -> None:
    """Test select_value_or_none method with no results."""
    with patch.object(mock_sync_driver, "execute") as mock_execute:
        mock_result = Mock(spec=SQLResult)
        mock_result.scalar_or_none.return_value = None
        mock_execute.return_value = mock_result

        result = mock_sync_driver.select_value_or_none("SELECT COUNT(*) FROM users WHERE id = 999")
        assert result is None


@pytest.mark.parametrize(
    "parameter_style,expected_style",
    [
        pytest.param(ParameterStyle.QMARK, ParameterStyle.QMARK, id="qmark"),
        pytest.param(ParameterStyle.NUMERIC, ParameterStyle.NUMERIC, id="numeric"),
        pytest.param(ParameterStyle.NAMED_COLON, ParameterStyle.NAMED_COLON, id="named_colon"),
        pytest.param(ParameterStyle.NAMED_PYFORMAT, ParameterStyle.NAMED_PYFORMAT, id="pyformat_named"),
    ],
)
def test_sync_driver_parameter_styles(
    mock_sync_connection: MockSyncConnection, parameter_style: ParameterStyle, expected_style: ParameterStyle
) -> None:
    """Test different parameter styles are handled correctly."""
    config = StatementConfig(
        dialect="sqlite",
        parameter_config=ParameterStyleConfig(
            default_parameter_style=parameter_style,
            supported_parameter_styles={parameter_style},
            default_execution_parameter_style=parameter_style,
            supported_execution_parameter_styles={parameter_style},
        ),
    )

    driver = MockSyncDriver(mock_sync_connection, config)
    assert driver.statement_config.parameter_config.default_parameter_style == expected_style

    if parameter_style == ParameterStyle.QMARK:
        statement = SQL("SELECT * FROM users WHERE id = ?", 1, statement_config=config)
    elif parameter_style == ParameterStyle.NUMERIC:
        statement = SQL("SELECT * FROM users WHERE id = $1", 1, statement_config=config)
    elif parameter_style == ParameterStyle.NAMED_COLON:
        statement = SQL("SELECT * FROM users WHERE id = :id", {"id": 1}, statement_config=config)
    else:
        statement = SQL("SELECT * FROM users WHERE id = %(id)s", {"id": 1}, statement_config=config)

    result = driver.dispatch_statement_execution(statement, driver.connection)
    assert isinstance(result, SQLResult)


@pytest.mark.parametrize("dialect", ["sqlite", "postgres", "mysql"])
def test_sync_driver_different_dialects(mock_sync_connection: MockSyncConnection, dialect: str) -> None:
    """Test sync driver works with different SQL dialects."""
    config = StatementConfig(
        dialect=dialect,
        parameter_config=ParameterStyleConfig(
            default_parameter_style=ParameterStyle.QMARK, supported_parameter_styles={ParameterStyle.QMARK}
        ),
    )

    driver = MockSyncDriver(mock_sync_connection, config)
    assert driver.statement_config.dialect == dialect

    result = driver.execute("SELECT 1 as test")
    assert isinstance(result, SQLResult)


def test_sync_driver_create_execution_result(mock_sync_driver: MockSyncDriver) -> None:
    """Test create_execution_result method."""
    cursor = mock_sync_driver.with_cursor(mock_sync_driver.connection)

    result = mock_sync_driver.create_execution_result(
        cursor, selected_data=[{"id": 1}, {"id": 2}], column_names=["id"], data_row_count=2, is_select_result=True
    )

    assert result.is_select_result is True
    assert result.selected_data == [{"id": 1}, {"id": 2}]
    assert result.column_names == ["id"]
    assert result.data_row_count == 2

    result = mock_sync_driver.create_execution_result(cursor, rowcount_override=1)
    assert result.is_select_result is False
    assert result.rowcount_override == 1

    result = mock_sync_driver.create_execution_result(
        cursor, statement_count=3, successful_statements=3, is_script_result=True
    )
    assert result.is_script_result is True
    assert result.statement_count == 3
    assert result.successful_statements == 3


def test_sync_driver_build_statement_result(mock_sync_driver: MockSyncDriver) -> None:
    """Test build_statement_result method."""
    statement = SQL("SELECT * FROM users", statement_config=mock_sync_driver.statement_config)
    cursor = mock_sync_driver.with_cursor(mock_sync_driver.connection)

    execution_result = mock_sync_driver.create_execution_result(
        cursor, selected_data=[{"id": 1}], column_names=["id"], data_row_count=1, is_select_result=True
    )

    sql_result = mock_sync_driver.build_statement_result(statement, execution_result)
    assert isinstance(sql_result, SQLResult)
    assert sql_result.operation_type == "SELECT"
    assert sql_result.get_data() == [{"id": 1}]
    assert sql_result.column_names == ["id"]

    script_statement = SQL(
        "INSERT INTO users (name) VALUES ('test');", statement_config=mock_sync_driver.statement_config, is_script=True
    )
    script_execution_result = mock_sync_driver.create_execution_result(
        cursor, statement_count=1, successful_statements=1, is_script_result=True
    )

    script_sql_result = mock_sync_driver.build_statement_result(script_statement, script_execution_result)
    assert script_sql_result.operation_type == "SCRIPT"
    assert script_sql_result.total_statements == 1
    assert script_sql_result.successful_statements == 1


def test_sync_driver_special_handling_integration(mock_sync_driver: MockSyncDriver) -> None:
    """Test that dispatch_special_handling is called during dispatch."""
    statement = SQL("SELECT * FROM users", statement_config=mock_sync_driver.statement_config)

    with patch.object(mock_sync_driver, "dispatch_special_handling", return_value=None) as mock_special:
        result = mock_sync_driver.dispatch_statement_execution(statement, mock_sync_driver.connection)

        assert isinstance(result, SQLResult)
        mock_special.assert_called_once()


def test_sync_driver_error_handling_in_dispatch(mock_sync_driver: MockSyncDriver) -> None:
    """Test error handling during statement dispatch."""
    statement = SQL("SELECT * FROM users", statement_config=mock_sync_driver.statement_config)

    with patch.object(mock_sync_driver, "dispatch_execute", side_effect=ValueError("Test error")):
        with pytest.raises(SQLSpecError, match="Mock database error"):
            mock_sync_driver.dispatch_statement_execution(statement, mock_sync_driver.connection)
