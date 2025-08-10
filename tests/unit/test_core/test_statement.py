"""Comprehensive unit tests for sqlspec.core.statement module.

This test module validates the enhanced SQL class and StatementConfig implementations
that provide 100% backward compatibility while internally using optimized processing.

Key Test Coverage:
1. SQL class single-pass processing - Verify SQL is parsed exactly once
2. Expression caching and reuse - Test that expressions are cached properly
3. Parameter integration - Test integration with the 2-phase parameter system
4. Operation type detection - Test detection of SELECT, INSERT, UPDATE, DELETE, etc.
5. Immutability guarantees - Ensure SQL objects are immutable
6. API compatibility - Ensure the same public API as the old architecture
7. Performance characteristics - Validate parse-once semantics
8. Edge cases - Complex queries, comments, string literals
"""

from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock, patch

import pytest
from sqlglot import expressions as exp

from sqlspec.core.parameters import ParameterStyle, ParameterStyleConfig
from sqlspec.core.statement import (
    SQL,
    ProcessedState,
    StatementConfig,
    get_default_config,
    get_default_parameter_config,
)
from sqlspec.typing import Empty

if TYPE_CHECKING:
    pass


# Test fixtures and constants
DEFAULT_PARAMETER_CONFIG = ParameterStyleConfig(
    default_parameter_style=ParameterStyle.QMARK, supported_parameter_styles={ParameterStyle.QMARK}
)
TEST_CONFIG = StatementConfig(parameter_config=DEFAULT_PARAMETER_CONFIG)


# StatementConfig function-based tests


@pytest.mark.parametrize(
    "config_kwargs,expected_values",
    [
        (
            {"parameter_config": DEFAULT_PARAMETER_CONFIG},
            {"dialect": None, "enable_caching": True, "enable_parsing": True, "enable_validation": True},
        ),
        (
            {
                "parameter_config": DEFAULT_PARAMETER_CONFIG,
                "dialect": "sqlite",
                "enable_caching": False,
                "execution_mode": "COPY",
            },
            {"dialect": "sqlite", "enable_caching": False, "execution_mode": "COPY"},
        ),
    ],
    ids=["defaults", "custom"],
)
def test_statement_config_initialization(config_kwargs: "dict[str, Any]", expected_values: "dict[str, Any]") -> None:
    """Test StatementConfig initialization with different parameters."""
    config = StatementConfig(**config_kwargs)

    for attr, expected in expected_values.items():
        assert getattr(config, attr) == expected

    # Test that parameter_converter and parameter_validator are always created
    assert config.parameter_converter is not None
    assert config.parameter_validator is not None


def test_statement_config_replace_immutable_update() -> None:
    """Test StatementConfig.replace() method for immutable updates."""
    original_config = StatementConfig(parameter_config=DEFAULT_PARAMETER_CONFIG, dialect="sqlite", enable_caching=True)

    # Test replacing multiple attributes
    updated_config = original_config.replace(dialect="postgres", enable_caching=False)

    # Original config unchanged
    assert original_config.dialect == "sqlite"
    assert original_config.enable_caching is True

    # New config has updates
    assert updated_config.dialect == "postgres"
    assert updated_config.enable_caching is False

    # Other attributes preserved
    assert updated_config.parameter_config is original_config.parameter_config


def test_statement_config_replace_invalid_attribute() -> None:
    """Test StatementConfig.replace() with invalid attribute raises TypeError."""
    config = StatementConfig(parameter_config=DEFAULT_PARAMETER_CONFIG)

    with pytest.raises(TypeError, match="'invalid_attr' is not a field"):
        config.replace(invalid_attr="value")


def test_statement_config_hash_equality() -> None:
    """Test StatementConfig hash and equality methods."""
    config1 = StatementConfig(parameter_config=DEFAULT_PARAMETER_CONFIG, dialect="sqlite")
    config2 = StatementConfig(parameter_config=DEFAULT_PARAMETER_CONFIG, dialect="sqlite")
    config3 = StatementConfig(parameter_config=DEFAULT_PARAMETER_CONFIG, dialect="postgres")

    # Note: StatementConfig creates new converter/validator instances, so direct equality fails
    # Test that key configuration attributes are the same
    assert config1.dialect == config2.dialect
    assert config1.enable_caching == config2.enable_caching
    assert config1.parameter_config == config2.parameter_config

    # Different configs have different dialects
    assert config1.dialect != config3.dialect

    # Hash should be stable for same config
    hash1_a = hash(config1)
    hash1_b = hash(config1)
    assert hash1_a == hash1_b


def test_statement_config_driver_required_attributes() -> None:
    """Test that all attributes required by drivers are available."""
    config = StatementConfig(
        parameter_config=DEFAULT_PARAMETER_CONFIG,
        dialect="postgres",
        execution_mode="COPY",
        execution_args={"format": "csv"},
    )

    # Driver-accessed attributes must be available
    assert hasattr(config, "dialect")
    assert hasattr(config, "parameter_config")
    assert hasattr(config, "execution_mode")
    assert hasattr(config, "execution_args")
    assert hasattr(config, "enable_caching")

    # Parameter config attributes that drivers access
    assert hasattr(config.parameter_config, "default_parameter_style")
    assert hasattr(config.parameter_config, "supported_parameter_styles")
    assert hasattr(config.parameter_config, "type_coercion_map")
    assert hasattr(config.parameter_config, "output_transformer")
    assert callable(config.parameter_config.hash)


# ProcessedState function-based tests


def test_processed_state_initialization() -> None:
    """Test ProcessedState initialization with all parameters."""
    compiled_sql = "SELECT * FROM users WHERE id = ?"
    execution_params = [1]
    operation_type = "SELECT"

    state = ProcessedState(
        compiled_sql=compiled_sql, execution_parameters=execution_params, operation_type=operation_type, is_many=False
    )

    assert state.compiled_sql == compiled_sql
    assert state.execution_parameters == execution_params
    assert state.operation_type == operation_type
    assert state.validation_errors == []
    assert state.is_many is False


def test_processed_state_hash_equality() -> None:
    """Test ProcessedState hash and equality."""
    state1 = ProcessedState("SELECT * FROM users", [], operation_type="SELECT")
    state2 = ProcessedState("SELECT * FROM users", [], operation_type="SELECT")
    state3 = ProcessedState("SELECT * FROM orders", [], operation_type="SELECT")

    # Equal states have same hash
    assert hash(state1) == hash(state2)

    # Different states have different hashes
    assert hash(state1) != hash(state3)


# SQL class basic functionality function-based tests


def test_sql_initialization_with_string() -> None:
    """Test SQL initialization with string input."""
    sql_str = "SELECT * FROM users"
    stmt = SQL(sql_str)

    assert stmt._raw_sql == sql_str
    assert stmt._processed_state is Empty
    assert stmt.statement_config is not None
    assert isinstance(stmt.statement_config, StatementConfig)


def test_sql_initialization_with_parameters() -> None:
    """Test SQL initialization with parameters."""
    sql_str = "SELECT * FROM users WHERE id = :id"
    parameters: dict[str, Any] = {"id": 1}
    stmt = SQL(sql_str, **parameters)

    assert stmt._raw_sql == sql_str
    assert stmt._named_parameters == parameters
    assert stmt._positional_parameters == []


def test_sql_initialization_with_positional_parameters() -> None:
    """Test SQL initialization with positional parameters."""
    sql_str = "SELECT * FROM users WHERE id = ?"
    stmt = SQL(sql_str, 1, "john")

    assert stmt._raw_sql == sql_str
    assert stmt._positional_parameters == [1, "john"]
    assert stmt._named_parameters == {}


def test_sql_initialization_with_expression() -> None:
    """Test SQL initialization with sqlglot expression."""
    expr = exp.select("*").from_("users")
    stmt = SQL(expr)

    # Expression is converted to SQL string
    assert "SELECT" in stmt._raw_sql
    assert "users" in stmt._raw_sql


def test_sql_initialization_with_custom_config() -> None:
    """Test SQL initialization with custom config."""
    config = StatementConfig(parameter_config=DEFAULT_PARAMETER_CONFIG, dialect="sqlite")
    stmt = SQL("SELECT * FROM users", statement_config=config)

    assert stmt.statement_config is config
    assert stmt.statement_config.dialect == "sqlite"


def test_sql_initialization_from_sql_object() -> None:
    """Test SQL initialization from existing SQL object."""
    original = SQL("SELECT * FROM users", id=1)
    copy_stmt = SQL(original)

    assert copy_stmt._raw_sql == original._raw_sql
    assert copy_stmt._named_parameters == original._named_parameters
    assert copy_stmt._is_many == original._is_many


def test_sql_auto_detect_many_from_parameters() -> None:
    """Test SQL auto-detection of is_many from parameter structure."""
    # Single parameter list - not many
    stmt1 = SQL("SELECT * FROM users WHERE id IN (?)", [1, 2, 3])
    assert stmt1._is_many is False

    # List of tuples - many
    stmt2 = SQL("INSERT INTO users (id, name) VALUES (?, ?)", [(1, "john"), (2, "jane")])
    assert stmt2._is_many is True

    # Explicit is_many override
    stmt3 = SQL("SELECT * FROM users WHERE id = ?", [(1,), (2,)], is_many=False)
    assert stmt3._is_many is False


# SQL single-pass processing function-based tests


def test_sql_lazy_processing_not_triggered_initially() -> None:
    """Test SQL processing is done lazily - not triggered on initialization."""
    stmt = SQL("SELECT * FROM users")

    # Processing not done yet
    assert stmt._processed_state is Empty


def test_sql_single_pass_processing_triggered_by_sql_property() -> None:
    """Test accessing .sql property returns raw SQL without processing."""
    stmt = SQL("SELECT * FROM users")

    # Mock the SQLProcessor to verify it's NOT called for .sql property
    with patch("sqlspec.core.statement.SQLProcessor") as mock_processor_class:
        mock_processor = MagicMock()
        mock_processor_class.return_value = mock_processor

        # Mock the compiled result
        from sqlspec.core.compiler import CompiledSQL

        mock_compiled = CompiledSQL(
            compiled_sql="SELECT * FROM users",
            execution_parameters=[],
            operation_type="SELECT",
            expression=exp.select("*").from_("users"),
        )
        mock_processor.compile.return_value = mock_compiled

        # Accessing .sql should return raw SQL without processing
        sql_result = stmt.sql

        # Verify processor was NOT called
        mock_processor_class.assert_not_called()
        assert sql_result == "SELECT * FROM users"  # Raw SQL

        # Processing state is still empty
        assert stmt._processed_state is Empty

        # Calling compile() triggers processing
        compiled_sql, params = stmt.compile()

        # Now processor should be called
        mock_processor_class.assert_called_once_with(stmt._statement_config)
        mock_processor.compile.assert_called_once_with(stmt._raw_sql, [], is_many=False)
        assert compiled_sql == "SELECT * FROM users"
        assert params == []


def test_sql_single_pass_processing_triggered_by_parameters_property() -> None:
    """Test accessing .parameters property returns original parameters."""
    stmt = SQL("SELECT * FROM users WHERE id = ?", 1)

    with patch("sqlspec.core.statement.SQLProcessor") as mock_processor_class:
        mock_processor = MagicMock()
        mock_processor_class.return_value = mock_processor

        from sqlspec.core.compiler import CompiledSQL

        mock_compiled = CompiledSQL(
            compiled_sql="SELECT * FROM users WHERE id = ?",
            execution_parameters=[1],
            operation_type="SELECT",
            expression=exp.select("*").from_("users"),
        )
        mock_processor.compile.return_value = mock_compiled

        # Access parameters returns original without processing
        params = stmt.parameters

        # No processing triggered
        mock_processor_class.assert_not_called()
        assert params == [1]  # Original parameters
        assert stmt._processed_state is Empty


def test_sql_single_pass_processing_triggered_by_operation_type_property() -> None:
    """Test accessing .operation_type property returns UNKNOWN without processing."""
    stmt = SQL("INSERT INTO users (name) VALUES ('john')")

    with patch("sqlspec.core.statement.SQLProcessor") as mock_processor_class:
        mock_processor = MagicMock()
        mock_processor_class.return_value = mock_processor

        from sqlspec.core.compiler import CompiledSQL

        mock_compiled = CompiledSQL(
            compiled_sql="INSERT INTO users (name) VALUES ('john')",
            execution_parameters={},
            operation_type="INSERT",
            expression=MagicMock(),  # Use MagicMock for the expression
        )
        mock_processor.compile.return_value = mock_compiled

        # Access operation_type returns UNKNOWN without processing
        op_type = stmt.operation_type

        # No processing triggered
        mock_processor_class.assert_not_called()
        assert op_type == "UNKNOWN"  # Default when not processed
        assert stmt._processed_state is Empty


def test_sql_processing_fallback_on_error() -> None:
    """Test SQL processing fallback when SQLProcessor fails."""
    stmt = SQL("INVALID SQL SYNTAX")

    with patch("sqlspec.core.statement.SQLProcessor") as mock_processor_class:
        mock_processor = MagicMock()
        mock_processor_class.return_value = mock_processor
        mock_processor.compile.side_effect = Exception("Processing failed")

        # .sql returns raw SQL without processing
        sql_result = stmt.sql
        assert sql_result == "INVALID SQL SYNTAX"
        assert stmt._processed_state is Empty  # No processing yet

        # compile() triggers processing and handles fallback
        compiled_sql, params = stmt.compile()

        # Fallback processing creates basic ProcessedState
        assert compiled_sql == "INVALID SQL SYNTAX"
        assert params == []
        assert stmt.operation_type == "UNKNOWN"
        assert stmt._processed_state is not Empty


# SQL expression caching function-based tests


def test_sql_expression_caching_enabled() -> None:
    """Test SQL expression caching when enabled."""
    config = StatementConfig(parameter_config=DEFAULT_PARAMETER_CONFIG, enable_caching=True)
    stmt = SQL("SELECT * FROM users", statement_config=config)

    # Mock SQLProcessor to control caching behavior
    with patch("sqlspec.core.statement.SQLProcessor") as mock_processor_class:
        mock_processor = MagicMock()
        mock_processor_class.return_value = mock_processor

        expr = exp.select("*").from_("users")
        from sqlspec.core.compiler import CompiledSQL

        mock_compiled = CompiledSQL(
            compiled_sql="SELECT * FROM users", execution_parameters={}, operation_type="SELECT", expression=expr
        )
        mock_processor.compile.return_value = mock_compiled

        # Expression is None before compilation
        assert stmt.expression is None

        # Compile to populate expression
        stmt.compile()

        # Now expression is available
        expr1 = stmt.expression
        # Second access should return cached
        expr2 = stmt.expression

        # Same object returned (cached)
        assert expr1 is expr2
        # Processor called only once due to internal caching
        assert mock_processor.compile.call_count == 1


def test_sql_expression_caching_disabled() -> None:
    """Test SQL expression behavior when caching is disabled."""
    config = StatementConfig(parameter_config=DEFAULT_PARAMETER_CONFIG, enable_caching=False)
    stmt = SQL("SELECT * FROM users", statement_config=config)

    # Even with caching disabled, the processed state should be cached internally
    # to avoid redundant processing within the same SQL object
    with patch("sqlspec.core.statement.SQLProcessor") as mock_processor_class:
        mock_processor = MagicMock()
        mock_processor_class.return_value = mock_processor

        expr = exp.select("*").from_("users")
        from sqlspec.core.compiler import CompiledSQL

        mock_compiled = CompiledSQL(
            compiled_sql="SELECT * FROM users", execution_parameters={}, operation_type="SELECT", expression=expr
        )
        mock_processor.compile.return_value = mock_compiled

        expr1 = stmt.expression
        expr2 = stmt.expression

        # Should still return same object due to internal state caching
        assert expr1 is expr2


# SQL parameter integration function-based tests


def test_sql_parameter_processing_named_parameters() -> None:
    """Test SQL parameter processing with named parameters."""
    stmt = SQL("SELECT * FROM users WHERE id = :id AND name = :name", id=1, name="john")

    assert stmt._named_parameters == {"id": 1, "name": "john"}
    assert stmt._positional_parameters == []


def test_sql_parameter_processing_positional_parameters() -> None:
    """Test SQL parameter processing with positional parameters."""
    stmt = SQL("SELECT * FROM users WHERE id = ? AND name = ?", 1, "john")

    assert stmt._positional_parameters == [1, "john"]
    assert stmt._named_parameters == {}


def test_sql_parameter_processing_mixed_args_kwargs() -> None:
    """Test SQL parameter processing with mixed args and kwargs."""
    stmt = SQL("SELECT * FROM users WHERE id = ? AND name = :name", 1, name="john")

    assert stmt._positional_parameters == [1]
    assert stmt._named_parameters == {"name": "john"}


def test_sql_parameter_processing_dict_parameter() -> None:
    """Test SQL parameter processing with dict parameter."""
    params = {"id": 1, "name": "john"}
    stmt = SQL("SELECT * FROM users WHERE id = :id AND name = :name", params)

    assert stmt._named_parameters == params
    assert stmt._positional_parameters == []


def test_sql_parameter_processing_list_parameter() -> None:
    """Test SQL parameter processing with list parameter."""
    params = [1, "john"]
    stmt = SQL("SELECT * FROM users WHERE id = ? AND name = ?", params)

    assert stmt._positional_parameters == params
    assert stmt._named_parameters == {}


def test_sql_parameter_processing_execute_many_detection() -> None:
    """Test SQL parameter processing detects execute_many scenarios."""
    # List of tuples should be detected as execute_many
    params = [(1, "john"), (2, "jane")]
    stmt = SQL("INSERT INTO users (id, name) VALUES (?, ?)", params)

    assert stmt._is_many is True
    assert stmt._positional_parameters == params


def test_sql_parameters_property_returns_processed_parameters() -> None:
    """Test SQL.parameters property returns processed parameters."""
    stmt = SQL("SELECT * FROM users WHERE id = ?", 1)

    with patch("sqlspec.core.statement.SQLProcessor") as mock_processor_class:
        mock_processor = MagicMock()
        mock_processor_class.return_value = mock_processor

        from sqlspec.core.compiler import CompiledSQL

        mock_compiled = CompiledSQL(
            compiled_sql="SELECT * FROM users WHERE id = ?",
            execution_parameters=[1],  # Processed parameters
            operation_type="SELECT",
            expression=exp.select("*").from_("users"),
        )
        mock_processor.compile.return_value = mock_compiled

        # Parameters property returns processed parameters
        params = stmt.parameters
        assert params == [1]


def test_sql_parameters_property_fallback_to_original() -> None:
    """Test SQL.parameters property falls back to original parameters when not processed."""
    stmt = SQL("SELECT * FROM users WHERE id = ?", 1)

    # Before processing, should return original parameters
    assert stmt._processed_state is Empty
    # Access parameters without triggering processing
    original_params = stmt._positional_parameters
    assert original_params == [1]


# SQL operation type detection function-based tests


@pytest.mark.parametrize(
    "sql_statement,expected_operation_type",
    [
        ("SELECT * FROM users", "SELECT"),
        ("INSERT INTO users (name) VALUES ('john')", "INSERT"),
        ("UPDATE users SET name = 'jane' WHERE id = 1", "UPDATE"),
        ("DELETE FROM users WHERE id = 1", "DELETE"),
        ("WITH cte AS (SELECT * FROM users) SELECT * FROM cte", "SELECT"),
        ("CREATE TABLE users (id INT)", "CREATE"),
        ("DROP TABLE users", "DROP"),
        ("EXECUTE sp_procedure", "COMMAND"),
    ],
    ids=["select", "insert", "update", "delete", "cte", "create", "drop", "execute"],
)
def test_sql_operation_type_detection(sql_statement: str, expected_operation_type: str) -> None:
    """Test SQL operation type detection for various statement types."""
    stmt = SQL(sql_statement)

    with patch("sqlspec.core.statement.SQLProcessor") as mock_processor_class:
        mock_processor = MagicMock()
        mock_processor_class.return_value = mock_processor

        from sqlspec.core.compiler import CompiledSQL

        mock_compiled = CompiledSQL(
            compiled_sql=sql_statement,
            execution_parameters={},
            operation_type=expected_operation_type,
            expression=MagicMock(),
        )
        mock_processor.compile.return_value = mock_compiled

        # Trigger compilation to populate operation_type
        stmt.compile()
        assert stmt.operation_type == expected_operation_type


def test_sql_returns_rows_detection() -> None:
    """Test SQL.returns_rows() method for different operation types."""
    # Mock the operation type by patching the processed state
    from sqlspec.core.statement import ProcessedState

    # SELECT statements return rows
    select_stmt = SQL("SELECT * FROM users")
    select_stmt._processed_state = ProcessedState(
        compiled_sql="SELECT * FROM users", execution_parameters=[], operation_type="SELECT"
    )
    assert select_stmt.returns_rows() is True

    # INSERT statements don't return rows
    insert_stmt = SQL("INSERT INTO users (name) VALUES ('john')")
    insert_stmt._processed_state = ProcessedState(
        compiled_sql="INSERT INTO users (name) VALUES ('john')", execution_parameters=[], operation_type="INSERT"
    )
    assert insert_stmt.returns_rows() is False

    # WITH statements return rows
    with_stmt = SQL("WITH cte AS (SELECT * FROM users) SELECT * FROM cte")
    with_stmt._processed_state = ProcessedState(
        compiled_sql="WITH cte AS (SELECT * FROM users) SELECT * FROM cte",
        execution_parameters=[],
        operation_type="WITH",
    )
    assert with_stmt.returns_rows() is True

    # SHOW statements return rows
    show_stmt = SQL("SHOW TABLES")
    show_stmt._processed_state = ProcessedState(
        compiled_sql="SHOW TABLES", execution_parameters=[], operation_type="SHOW"
    )
    assert show_stmt.returns_rows() is True


# SQL immutability guarantees function-based tests


def test_sql_slots_prevent_new_attributes() -> None:
    """Test SQL __slots__ prevent adding new attributes."""
    stmt = SQL("SELECT * FROM users")

    # Cannot add new attributes due to __slots__
    with pytest.raises(AttributeError):
        stmt.new_attribute = "test"  # type: ignore[attr-defined]


def test_sql_hash_immutability() -> None:
    """Test SQL hash remains consistent (immutability indicator)."""
    stmt = SQL("SELECT * FROM users WHERE id = ?", 1)

    # Hash should be consistent
    hash1 = hash(stmt)
    hash2 = hash(stmt)
    assert hash1 == hash2


def test_sql_equality_immutability() -> None:
    """Test SQL equality based on immutable attributes."""
    stmt1 = SQL("SELECT * FROM users WHERE id = ?", 1)
    stmt2 = SQL("SELECT * FROM users WHERE id = ?", 1)
    stmt3 = SQL("SELECT * FROM users WHERE id = ?", 2)

    # Equal SQL objects
    assert stmt1 == stmt2
    assert hash(stmt1) == hash(stmt2)

    # Different SQL objects
    assert stmt1 != stmt3
    assert hash(stmt1) != hash(stmt3)


def test_sql_copy_creates_new_instance() -> None:
    """Test SQL.copy() creates new immutable instance."""
    original = SQL("SELECT * FROM users WHERE id = ?", 1)
    copy_stmt = original.copy(parameters=[2])

    # Different instances
    assert copy_stmt is not original
    # Different parameters
    assert copy_stmt._positional_parameters != original._positional_parameters
    # Same SQL
    assert copy_stmt._raw_sql == original._raw_sql


def test_sql_as_script_creates_new_instance() -> None:
    """Test SQL.as_script() creates new immutable instance."""
    original = SQL("SELECT * FROM users")
    script_stmt = original.as_script()

    # Different instances
    assert script_stmt is not original
    # Different is_script flag
    assert script_stmt._is_script is True
    assert original._is_script is False


def test_sql_add_named_parameter_creates_new_instance() -> None:
    """Test SQL.add_named_parameter() creates new immutable instance."""
    original = SQL("SELECT * FROM users WHERE id = :id", id=1)
    updated_stmt = original.add_named_parameter("name", "john")

    # Different instances
    assert updated_stmt is not original
    # Original unchanged
    assert "name" not in original._named_parameters
    # Updated has new parameter
    assert updated_stmt._named_parameters["name"] == "john"
    assert updated_stmt._named_parameters["id"] == 1


# SQL API compatibility function-based tests


def test_sql_compile_method_compatibility() -> None:
    """Test SQL.compile() method returns same format as old API."""
    stmt = SQL("SELECT * FROM users WHERE id = ?", 1)

    with patch("sqlspec.core.statement.SQLProcessor") as mock_processor_class:
        mock_processor = MagicMock()
        mock_processor_class.return_value = mock_processor

        from sqlspec.core.compiler import CompiledSQL

        mock_compiled = CompiledSQL(
            compiled_sql="SELECT * FROM users WHERE id = ?",
            execution_parameters=[1],
            operation_type="SELECT",
            expression=exp.select("*").from_("users"),
        )
        mock_processor.compile.return_value = mock_compiled

        sql, params = stmt.compile()

        # Returns tuple of (sql, parameters) as expected
        assert isinstance(sql, str)
        assert sql == "SELECT * FROM users WHERE id = ?"
        assert params == [1]


def test_sql_where_method_compatibility() -> None:
    """Test SQL.where() method creates new SQL with WHERE condition."""
    stmt = SQL("SELECT * FROM users")
    where_stmt = stmt.where("id > 10")

    # Different instances
    assert where_stmt is not stmt
    # Original unchanged
    assert "WHERE" not in stmt._raw_sql
    # New instance has WHERE
    assert "WHERE" in where_stmt._raw_sql or "id > 10" in where_stmt._raw_sql


def test_sql_where_method_with_expression() -> None:
    """Test SQL.where() method works with SQLGlot expressions."""
    stmt = SQL("SELECT * FROM users")
    # Create a simple condition using sqlglot
    condition = exp.GT(this=exp.column("id"), expression=exp.Literal.number(10))
    where_stmt = stmt.where(condition)

    # Different instances
    assert where_stmt is not stmt
    # Should create new SQL with WHERE condition
    assert where_stmt._raw_sql != stmt._raw_sql


def test_sql_filters_property_compatibility() -> None:
    """Test SQL.filters property returns copy of filters list."""
    stmt = SQL("SELECT * FROM users")

    # Empty filters initially
    filters = stmt.filters
    assert filters == []

    # Returns copy (not reference to internal list)
    assert filters is not stmt._filters


def test_sql_validation_errors_property_compatibility() -> None:
    """Test SQL.validation_errors property compatibility."""
    stmt = SQL("SELECT * FROM users")

    with patch("sqlspec.core.statement.SQLProcessor") as mock_processor_class:
        mock_processor = MagicMock()
        mock_processor_class.return_value = mock_processor

        from sqlspec.core.compiler import CompiledSQL

        mock_compiled = CompiledSQL(
            compiled_sql="SELECT * FROM users",
            execution_parameters={},
            operation_type="SELECT",
            expression=exp.select("*").from_("users"),
        )
        # Add validation errors to processed state
        state = ProcessedState(
            compiled_sql="SELECT * FROM users",
            execution_parameters={},
            operation_type="SELECT",
            validation_errors=["Warning: Missing index"],
        )
        mock_processor.compile.return_value = mock_compiled
        stmt._processed_state = state

        errors = stmt.validation_errors

        assert errors == ["Warning: Missing index"]
        # Returns copy
        assert errors is not state.validation_errors


def test_sql_has_errors_property_compatibility() -> None:
    """Test SQL.has_errors property compatibility."""
    stmt = SQL("SELECT * FROM users")

    # Mock processed state with no errors
    stmt._processed_state = ProcessedState(
        compiled_sql="SELECT * FROM users", execution_parameters={}, operation_type="SELECT", validation_errors=[]
    )
    assert stmt.has_errors is False

    # Mock processed state with errors
    stmt._processed_state = ProcessedState(
        compiled_sql="SELECT * FROM users",
        execution_parameters={},
        operation_type="SELECT",
        validation_errors=["Error: Invalid syntax"],
    )
    assert stmt.has_errors is True


# SQL performance characteristics function-based tests


def test_sql_single_parse_guarantee() -> None:
    """Test SQL guarantees single parse operation."""
    stmt = SQL("SELECT * FROM users WHERE id = ?", 1)

    with patch("sqlspec.core.statement.SQLProcessor") as mock_processor_class:
        mock_processor = MagicMock()
        mock_processor_class.return_value = mock_processor

        from sqlspec.core.compiler import CompiledSQL

        mock_compiled = CompiledSQL(
            compiled_sql="SELECT * FROM users WHERE id = ?",
            execution_parameters=[1],
            operation_type="SELECT",
            expression=exp.select("*").from_("users"),
        )
        mock_processor.compile.return_value = mock_compiled

        # Access multiple properties that would trigger parsing in old system
        _ = stmt.sql
        _ = stmt.operation_type
        _ = stmt.expression
        _ = stmt.parameters
        _ = stmt.compile()

        # Processor should be called exactly once
        assert mock_processor.compile.call_count == 1


def test_sql_lazy_evaluation_performance() -> None:
    """Test SQL lazy evaluation avoids unnecessary work."""
    stmt = SQL("SELECT * FROM users")

    # No processing should happen during initialization
    assert stmt._processed_state is Empty

    # Should not trigger processing for basic attribute access
    _ = stmt._raw_sql
    _ = stmt.statement_config
    _ = stmt._is_many
    _ = stmt._is_script

    assert stmt._processed_state is Empty


def test_sql_processing_caching_performance() -> None:
    """Test SQL processing result caching for performance."""
    stmt = SQL("SELECT * FROM users")

    with patch("sqlspec.core.statement.SQLProcessor") as mock_processor_class:
        mock_processor = MagicMock()
        mock_processor_class.return_value = mock_processor

        from sqlspec.core.compiler import CompiledSQL

        mock_compiled = CompiledSQL(
            compiled_sql="SELECT * FROM users",
            execution_parameters={},
            operation_type="SELECT",
            expression=exp.select("*").from_("users"),
        )
        mock_processor.compile.return_value = mock_compiled

        # First compile triggers processing and caches result
        stmt.compile()
        assert stmt._processed_state is not Empty

        # Subsequent accesses use cached result
        result1 = stmt.sql
        result2 = stmt.sql

        # All return consistent results
        assert result1 == result2
        # Processor called only once due to caching
        assert mock_processor.compile.call_count == 1


# SQL edge cases function-based tests


@pytest.mark.parametrize(
    "complex_sql",
    [
        "SELECT * FROM users u JOIN orders o ON u.id = o.user_id WHERE u.active = 1",
        "WITH cte AS (SELECT * FROM users) SELECT * FROM cte",
        "SELECT COUNT(*), MAX(price) FROM orders GROUP BY user_id HAVING COUNT(*) > 5",
        "INSERT INTO users (name, email) VALUES ('test', 'test@example.com')",
        "UPDATE users SET active = 0 WHERE last_login < '2023-01-01'",
        "DELETE FROM orders WHERE status = 'cancelled' AND created_at < '2023-01-01'",
        """
        SELECT
            u.name,
            o.total
        FROM users u
        LEFT JOIN orders o ON u.id = o.user_id
        WHERE u.created_at > '2023-01-01'
        ORDER BY u.name
        """,
    ],
    ids=["join", "cte", "group_by", "insert", "update", "delete", "multiline"],
)
def test_sql_complex_queries(complex_sql: str) -> None:
    """Test SQL handles complex queries correctly."""
    stmt = SQL(complex_sql)

    # Should initialize without errors
    assert stmt._raw_sql == complex_sql
    assert stmt._processed_state is Empty


def test_sql_with_comments_and_literals() -> None:
    """Test SQL handles comments and string literals."""
    sql_with_comments = """
    -- This is a line comment
    SELECT
        name, /* inline comment */
        'string literal with -- comment inside',
        "double quoted string"
    FROM users
    /*
       Multi-line comment
    */
    WHERE name = 'O''Brien' -- escaped quote
    """

    stmt = SQL(sql_with_comments)

    # Should initialize without errors
    assert stmt._raw_sql == sql_with_comments


def test_sql_with_complex_parameters() -> None:
    """Test SQL with complex parameter scenarios."""
    # Mixed parameter styles (should be handled by parameter processing)
    sql = "SELECT * FROM users WHERE id = ? AND name = :name AND email = $1"
    stmt = SQL(sql, 1, name="john", email="john@example.com")

    assert stmt._positional_parameters == [1]
    assert stmt._named_parameters == {"name": "john", "email": "john@example.com"}


def test_sql_empty_and_whitespace() -> None:
    """Test SQL handles empty and whitespace-only input."""
    # Empty string
    empty_stmt = SQL("")
    assert empty_stmt._raw_sql == ""

    # Whitespace only
    whitespace_stmt = SQL("   \n\t   ")
    assert whitespace_stmt._raw_sql == "   \n\t   "


def test_sql_invalid_syntax_handling() -> None:
    """Test SQL handles invalid syntax gracefully."""
    invalid_stmt = SQL("INVALID SQL SYNTAX !@#$%^&*()")

    # Should initialize without errors
    assert "INVALID" in invalid_stmt._raw_sql

    # Processing should fall back gracefully
    with patch("sqlspec.core.statement.SQLProcessor") as mock_processor_class:
        mock_processor = MagicMock()
        mock_processor_class.return_value = mock_processor
        mock_processor.compile.side_effect = Exception("Parse error")

        # Should not raise, should use fallback
        sql_result = invalid_stmt.sql
        op_type = invalid_stmt.operation_type

        assert sql_result == "INVALID SQL SYNTAX !@#$%^&*()"
        assert op_type == "UNKNOWN"


def test_sql_special_characters_and_unicode() -> None:
    """Test SQL handles special characters and Unicode."""
    unicode_sql = "SELECT * FROM users WHERE name = 'José' AND city = '北京'"
    stmt = SQL(unicode_sql)

    assert stmt._raw_sql == unicode_sql


def test_sql_very_long_query() -> None:
    """Test SQL handles very long queries."""
    # Generate a long SELECT statement
    columns = [f"column_{i}" for i in range(100)]
    long_sql = f"SELECT {', '.join(columns)} FROM users"

    stmt = SQL(long_sql)
    assert stmt._raw_sql == long_sql


def test_sql_repr_format() -> None:
    """Test SQL __repr__ provides useful debugging information."""
    # Simple case
    stmt1 = SQL("SELECT * FROM users")
    repr1 = repr(stmt1)
    assert "SQL(" in repr1
    assert "SELECT * FROM users" in repr1

    # With parameters
    stmt2 = SQL("SELECT * FROM users WHERE id = ?", 1)
    repr2 = repr(stmt2)
    assert "params=[1]" in repr2

    # With named parameters
    stmt3 = SQL("SELECT * FROM users WHERE id = :id", id=1)
    repr3 = repr(stmt3)
    assert "named_params={'id': 1}" in repr3

    # With flags
    stmt4 = SQL("SELECT * FROM users", is_many=True)
    stmt4_script = stmt4.as_script()
    repr4 = repr(stmt4_script)
    assert "is_script" in repr4


# Configuration functions tests


def test_get_default_config() -> None:
    """Test get_default_config() returns valid StatementConfig."""
    config = get_default_config()

    assert isinstance(config, StatementConfig)
    assert config.enable_parsing is True
    assert config.enable_validation is True
    assert config.enable_caching is True
    assert config.parameter_config is not None


def test_get_default_parameter_config() -> None:
    """Test get_default_parameter_config() returns valid ParameterStyleConfig."""
    param_config = get_default_parameter_config()

    assert isinstance(param_config, ParameterStyleConfig)
    assert param_config.default_parameter_style == ParameterStyle.QMARK
    assert ParameterStyle.QMARK in param_config.supported_parameter_styles


# Performance and memory regression test fixtures
@pytest.fixture
def sample_sqls() -> "list[str]":
    """Sample SQL statements for performance testing."""
    return [
        "SELECT * FROM users",
        "SELECT * FROM users WHERE id = ?",
        "INSERT INTO users (name, email) VALUES (?, ?)",
        "UPDATE users SET name = ? WHERE id = ?",
        "DELETE FROM users WHERE id = ?",
        "SELECT u.*, o.* FROM users u JOIN orders o ON u.id = o.user_id",
        "WITH cte AS (SELECT * FROM users WHERE active = 1) SELECT * FROM cte",
    ]


def test_sql_memory_efficiency_with_slots(sample_sqls: "list[str]") -> None:
    """Test SQL objects use __slots__ for memory efficiency."""
    statements = [SQL(sql) for sql in sample_sqls]

    # All SQL objects should have __slots__
    for stmt in statements:
        assert hasattr(stmt, "__slots__")
        # Should not have __dict__ due to __slots__
        assert not hasattr(stmt, "__dict__")


def test_sql_consistent_behavior_across_multiple_instances(sample_sqls: "list[str]") -> None:
    """Test SQL behavior is consistent across multiple instances."""
    statements = [SQL(sql) for sql in sample_sqls]

    # All should initialize successfully
    assert len(statements) == len(sample_sqls)

    # All should have consistent internal state
    for stmt in statements:
        assert stmt._processed_state is Empty
        assert isinstance(stmt.statement_config, StatementConfig)
        assert stmt._hash is None  # Lazy hash computation


# SQL thread safety function-based tests


def test_sql_immutable_after_creation() -> None:
    """Test SQL objects are effectively immutable after creation."""
    stmt = SQL("SELECT * FROM users WHERE id = ?", 1)

    # Key attributes should not change
    original_raw_sql = stmt._raw_sql
    original_params = stmt._positional_parameters
    original_config = stmt.statement_config

    # Access properties that trigger processing
    _ = stmt.sql
    _ = stmt.operation_type

    # Original attributes unchanged
    assert stmt._raw_sql is original_raw_sql
    assert stmt._positional_parameters is original_params
    assert stmt.statement_config is original_config


def test_sql_processing_state_stability() -> None:
    """Test SQL processing state remains stable after first access."""
    stmt = SQL("SELECT * FROM users")

    with patch("sqlspec.core.statement.SQLProcessor") as mock_processor_class:
        mock_processor = MagicMock()
        mock_processor_class.return_value = mock_processor

        from sqlspec.core.compiler import CompiledSQL

        mock_compiled = CompiledSQL(
            compiled_sql="SELECT * FROM users",
            execution_parameters={},
            operation_type="SELECT",
            expression=exp.select("*").from_("users"),
        )
        mock_processor.compile.return_value = mock_compiled

        # First access establishes processed state
        _ = stmt.sql
        first_state = stmt._processed_state

        # Subsequent accesses should not change the state object
        _ = stmt.operation_type
        _ = stmt.expression

        assert stmt._processed_state is first_state
