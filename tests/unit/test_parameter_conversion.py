"""Test parameter conversion logic follows core principles.

Tests validate that parameter conversion ONLY occurs when:
1. Required format for execution (e.g., psycopg specific requirements)
2. Mapping to supplied format handling transformations

Parameter style conversion (QMARK vs NUMERIC vs NAMED) should be handled
by SQLGlot compilation, not by adapter-specific logic.
"""

from typing import Any
from unittest.mock import Mock

import pytest

from sqlspec.driver._common import ExecutionResult
from sqlspec.driver._sync import SyncDriverAdapterBase
from sqlspec.parameters import ParameterStyle
from sqlspec.parameters.config import ParameterStyleConfig
from sqlspec.statement.sql import SQL, StatementConfig


class MockAdapter(SyncDriverAdapterBase):
    """Mock adapter for testing parameter conversion logic."""

    def __init__(self, statement_config: StatementConfig) -> None:
        self.connection = Mock()
        self.statement_config = statement_config
        self.driver_features = {}

    def with_cursor(self, connection: Any) -> Any:
        return Mock()

    def _try_special_handling(self, cursor: Any, statement: Any) -> Any:
        return None

    def _execute_many(self, cursor: Any, sql: str, prepared_params: Any, statement: "SQL") -> ExecutionResult:
        return self.create_execution_result(cursor, is_many_result=True)

    def _execute_statement(self, cursor: Any, sql: str, prepared_params: Any, statement: "SQL") -> ExecutionResult:
        return self.create_execution_result(cursor)

    def _execute_script(
        self, cursor: Any, sql: str, prepared_params: Any, statement_config: "StatementConfig", statement: "SQL"
    ) -> ExecutionResult:
        return self.create_execution_result(cursor, is_script_result=True)

    def _get_selected_data(self, cursor: Any) -> "tuple[list[dict[str, Any]], list[str], int]":
        return [], [], 0

    def _get_row_count(self, cursor: Any) -> int:
        return 0

    def begin(self) -> None:
        pass

    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass


def test_parameter_conversion_only_when_necessary() -> None:
    """Test that parameter conversion only occurs when required."""
    # Create adapter with QMARK style (same as default)
    parameter_config = ParameterStyleConfig(
        default_parameter_style=ParameterStyle.QMARK,
        supported_parameter_styles={ParameterStyle.QMARK},
        supported_execution_parameter_styles={ParameterStyle.QMARK},
        default_execution_parameter_style=ParameterStyle.QMARK,
    )
    statement_config = StatementConfig(dialect="sqlite", parameter_config=parameter_config)
    adapter = MockAdapter(statement_config)

    # Mock SQL statement with proper attributes
    mock_statement = Mock(spec=SQL)
    mock_statement.compile.return_value = ("SELECT * FROM test WHERE id = ?", [1])
    mock_statement.parameters = [1]  # Mock parameters attribute
    mock_statement.is_many = False  # Mock is_many attribute
    # Mock internal attributes used by hash_sql_statement
    mock_statement._positional_params = [1]
    mock_statement._named_params = {}
    mock_statement._original_parameters = [1]
    mock_statement._raw_sql = "SELECT * FROM test WHERE id = ?"
    mock_statement._statement = None  # Indicates raw SQL
    mock_statement._filters = []

    # Test that _get_compiled_sql uses explicit placeholder_style
    sql, params = adapter._get_compiled_sql(mock_statement, statement_config)

    # Should call compile with explicit style even when it matches default
    mock_statement.compile.assert_called_once_with(placeholder_style=ParameterStyle.QMARK, flatten_single_params=False)
    assert sql == "SELECT * FROM test WHERE id = ?"
    assert params == [1]


def test_parameter_style_conversion_when_different() -> None:
    """Test parameter style conversion when execution style differs from default."""
    # Create adapter where execution style differs from default
    parameter_config = ParameterStyleConfig(
        default_parameter_style=ParameterStyle.QMARK,
        supported_parameter_styles={ParameterStyle.QMARK, ParameterStyle.NUMERIC},
        supported_execution_parameter_styles={ParameterStyle.NUMERIC},
        default_execution_parameter_style=ParameterStyle.NUMERIC,
    )
    statement_config = StatementConfig(dialect="postgres", parameter_config=parameter_config)
    adapter = MockAdapter(statement_config)

    # Mock SQL statement with proper attributes
    mock_statement = Mock(spec=SQL)
    mock_statement.compile.return_value = ("SELECT * FROM test WHERE id = $1", [1])
    mock_statement.parameters = [1]  # Mock parameters attribute
    mock_statement.is_many = False  # Mock is_many attribute
    # Mock internal attributes used by hash_sql_statement
    mock_statement._positional_params = [1]
    mock_statement._named_params = {}
    mock_statement._original_parameters = [1]
    mock_statement._raw_sql = "SELECT * FROM test WHERE id = $1"
    mock_statement._statement = None  # Indicates raw SQL
    mock_statement._filters = []

    # Test compilation with different execution style
    sql, params = adapter._get_compiled_sql(mock_statement, statement_config)

    # Should call compile with explicit execution style
    mock_statement.compile.assert_called_once_with(
        placeholder_style=ParameterStyle.NUMERIC, flatten_single_params=False
    )
    assert sql == "SELECT * FROM test WHERE id = $1"
    assert params == [1]


def test_no_parameter_conversion_without_target_style() -> None:
    """Test that new dual parameter system works without explicit execution configuration."""
    # Create adapter without explicit execution style
    parameter_config = ParameterStyleConfig(
        default_parameter_style=ParameterStyle.QMARK,
        supported_parameter_styles={ParameterStyle.QMARK},
        # Don't set execution parameter styles - should use default behavior
    )

    # Check that execution style configuration is None (uses default behavior)
    assert parameter_config.supported_execution_parameter_styles is None
    assert parameter_config.default_execution_parameter_style is None

    statement_config = StatementConfig(dialect="sqlite", parameter_config=parameter_config)
    adapter = MockAdapter(statement_config)

    # Mock SQL statement with proper attributes
    mock_statement = Mock(spec=SQL)
    mock_statement.compile.return_value = ("SELECT * FROM test WHERE id = ?", [1])
    mock_statement.parameters = [1]  # Mock parameters attribute
    mock_statement.is_many = False  # Mock is_many attribute
    # Mock internal attributes used by hash_sql_statement
    mock_statement._positional_params = [1]
    mock_statement._named_params = {}
    mock_statement._original_parameters = [1]
    mock_statement._raw_sql = "SELECT * FROM test WHERE id = ?"
    mock_statement._statement = None  # Indicates raw SQL
    mock_statement._filters = []

    # Test compilation - should use explicit style from default parameter style
    sql, params = adapter._get_compiled_sql(mock_statement, statement_config)

    # Should call compile with explicit style (which equals default in this case)
    mock_statement.compile.assert_called_once_with(placeholder_style=ParameterStyle.QMARK, flatten_single_params=False)
    assert sql == "SELECT * FROM test WHERE id = ?"
    assert params == [1]


def test_parameter_preparation_preserves_values() -> None:
    """Test that parameter preparation preserves original values when no conversion needed."""
    parameter_config = ParameterStyleConfig(
        default_parameter_style=ParameterStyle.QMARK,
        supported_parameter_styles={ParameterStyle.QMARK},
        supported_execution_parameter_styles={ParameterStyle.QMARK},
        default_execution_parameter_style=ParameterStyle.QMARK,
    )
    statement_config = StatementConfig(dialect="sqlite", parameter_config=parameter_config)
    adapter = MockAdapter(statement_config)

    # Test various parameter types
    test_params = ["string_value", 123, 45.67, True, None, [1, 2, 3], {"key": "value"}]

    # Should preserve all values unchanged when no conversion needed
    prepared = adapter.prepare_driver_parameters(test_params, statement_config)
    assert prepared == test_params


def test_parameter_style_configurations() -> None:
    """Test that different adapters have correct parameter style configurations."""
    # Test cases for different database parameter styles
    test_cases = [
        # (adapter_name, default_style, supported_styles)
        ("sqlite", ParameterStyle.QMARK, {ParameterStyle.QMARK, ParameterStyle.NAMED_COLON}),
        ("duckdb", ParameterStyle.QMARK, {ParameterStyle.QMARK, ParameterStyle.NUMERIC, ParameterStyle.NAMED_DOLLAR}),
        ("postgres_asyncpg", ParameterStyle.NUMERIC, {ParameterStyle.NUMERIC}),
        (
            "postgres_psycopg",
            ParameterStyle.POSITIONAL_PYFORMAT,
            {ParameterStyle.POSITIONAL_PYFORMAT, ParameterStyle.NAMED_PYFORMAT, ParameterStyle.NUMERIC},
        ),
        (
            "mysql",
            ParameterStyle.POSITIONAL_PYFORMAT,
            {ParameterStyle.POSITIONAL_PYFORMAT, ParameterStyle.NAMED_PYFORMAT},
        ),
        ("bigquery", ParameterStyle.NAMED_AT, {ParameterStyle.NAMED_AT}),
        ("oracle", ParameterStyle.NAMED_COLON, {ParameterStyle.NAMED_COLON, ParameterStyle.POSITIONAL_COLON}),
    ]

    for adapter_name, default_style, supported_styles in test_cases:
        parameter_config = ParameterStyleConfig(
            default_parameter_style=default_style,
            supported_parameter_styles=supported_styles,
            supported_execution_parameter_styles={default_style},
            default_execution_parameter_style=default_style,
        )

        # Verify configuration is valid
        assert parameter_config.default_parameter_style == default_style
        assert parameter_config.supported_parameter_styles == supported_styles
        assert parameter_config.supported_execution_parameter_styles == {default_style}
        assert parameter_config.default_execution_parameter_style == default_style

        # Verify default style is in supported styles
        assert default_style in supported_styles


def test_base_get_compiled_sql_always_explicit() -> None:
    """Test that base _get_compiled_sql always uses explicit placeholder_style when configured."""
    # This test ensures the fix for SQLGlot default style issues
    parameter_config = ParameterStyleConfig(
        default_parameter_style=ParameterStyle.QMARK,
        supported_parameter_styles={ParameterStyle.QMARK},
        supported_execution_parameter_styles={ParameterStyle.QMARK},
        default_execution_parameter_style=ParameterStyle.QMARK,
    )
    statement_config = StatementConfig(dialect="sqlite", parameter_config=parameter_config)
    adapter = MockAdapter(statement_config)

    # Mock SQL statement with proper attributes
    mock_statement = Mock(spec=SQL)
    mock_statement.compile.return_value = ("SELECT * FROM test WHERE id = ?", [1])
    mock_statement.parameters = [1]  # Mock parameters attribute
    mock_statement.is_many = False  # Mock is_many attribute
    # Mock internal attributes used by hash_sql_statement
    mock_statement._positional_params = [1]
    mock_statement._named_params = {}
    mock_statement._original_parameters = [1]
    mock_statement._raw_sql = "SELECT * FROM test WHERE id = ?"
    mock_statement._statement = None  # Indicates raw SQL
    mock_statement._filters = []

    # Call base implementation
    _ = adapter._get_compiled_sql(mock_statement, statement_config)

    # Should ALWAYS call with explicit placeholder_style when target_style is set
    # This prevents SQLGlot from using its internal default which may differ
    mock_statement.compile.assert_called_once_with(placeholder_style=ParameterStyle.QMARK, flatten_single_params=False)


@pytest.mark.parametrize(
    "adapter_style,expected_style",
    [
        (ParameterStyle.QMARK, ParameterStyle.QMARK),
        (ParameterStyle.NUMERIC, ParameterStyle.NUMERIC),
        (ParameterStyle.NAMED_COLON, ParameterStyle.NAMED_COLON),
        (ParameterStyle.NAMED_DOLLAR, ParameterStyle.NAMED_DOLLAR),
        (ParameterStyle.NAMED_AT, ParameterStyle.NAMED_AT),
        (ParameterStyle.POSITIONAL_PYFORMAT, ParameterStyle.POSITIONAL_PYFORMAT),
        (ParameterStyle.NAMED_PYFORMAT, ParameterStyle.NAMED_PYFORMAT),
        (ParameterStyle.POSITIONAL_COLON, ParameterStyle.POSITIONAL_COLON),
    ],
)
def test_parameter_style_compilation(adapter_style: ParameterStyle, expected_style: ParameterStyle) -> None:
    """Test that each parameter style is compiled correctly."""
    parameter_config = ParameterStyleConfig(
        default_parameter_style=adapter_style,
        supported_parameter_styles={adapter_style},
        supported_execution_parameter_styles={adapter_style},
        default_execution_parameter_style=adapter_style,
    )
    statement_config = StatementConfig(dialect="test", parameter_config=parameter_config)
    adapter = MockAdapter(statement_config)

    # Mock SQL statement with proper attributes
    mock_statement = Mock(spec=SQL)
    mock_statement.compile.return_value = ("SELECT * FROM test", [])
    mock_statement.parameters = []  # Mock parameters attribute (empty for this test)
    mock_statement.is_many = False  # Mock is_many attribute
    # Mock internal attributes used by hash_sql_statement
    mock_statement._positional_params = []
    mock_statement._named_params = {}
    mock_statement._original_parameters = []
    mock_statement._raw_sql = "SELECT * FROM test"
    mock_statement._statement = None  # Indicates raw SQL
    mock_statement._filters = []

    # Test compilation
    adapter._get_compiled_sql(mock_statement, statement_config)

    # Should use the correct parameter style
    mock_statement.compile.assert_called_once_with(placeholder_style=expected_style, flatten_single_params=False)
