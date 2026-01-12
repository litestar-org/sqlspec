# pyright: reportPrivateImportUsage = false, reportPrivateUsage = false
# pyright: reportPrivateImportUsage = false, reportPrivateUsage = false
"""Unit tests for sqlspec.core.statement module.

This test module validates the SQL class and StatementConfig implementations.

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

import os
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from sqlglot import expressions as exp

from sqlspec.core import (
    SQL,
    CompiledSQL,
    OperationType,
    ParameterStyle,
    ParameterStyleConfig,
    ProcessedState,
    StatementConfig,
    get_default_config,
    get_default_parameter_config,
    get_pipeline_metrics,
    reset_pipeline_registry,
)
from sqlspec.typing import Empty
from tests.conftest import requires_interpreted

pytestmark = pytest.mark.xdist_group("core")

DEFAULT_PARAMETER_CONFIG = ParameterStyleConfig(
    default_parameter_style=ParameterStyle.QMARK, supported_parameter_styles={ParameterStyle.QMARK}
)
TEST_CONFIG = StatementConfig(parameter_config=DEFAULT_PARAMETER_CONFIG)


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

    assert config.parameter_converter is not None
    assert config.parameter_validator is not None


def test_statement_config_replace_immutable_update() -> None:
    """Test StatementConfig.replace() method for immutable updates."""
    original_config = StatementConfig(parameter_config=DEFAULT_PARAMETER_CONFIG, dialect="sqlite", enable_caching=True)

    updated_config = original_config.replace(dialect="postgres", enable_caching=False)

    assert original_config.dialect == "sqlite"
    assert original_config.enable_caching is True

    assert updated_config.dialect == "postgres"
    assert updated_config.enable_caching is False

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

    assert config1.dialect == config2.dialect
    assert config1.enable_caching == config2.enable_caching
    assert config1.parameter_config == config2.parameter_config

    assert config1.dialect != config3.dialect

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

    assert hasattr(config, "dialect")
    assert hasattr(config, "parameter_config")
    assert hasattr(config, "execution_mode")
    assert hasattr(config, "execution_args")
    assert hasattr(config, "enable_caching")

    assert hasattr(config.parameter_config, "default_parameter_style")
    assert hasattr(config.parameter_config, "supported_parameter_styles")
    assert hasattr(config.parameter_config, "type_coercion_map")
    assert hasattr(config.parameter_config, "output_transformer")
    assert callable(config.parameter_config.hash)


def test_processed_state_initialization() -> None:
    """Test ProcessedState initialization with all parameters."""
    compiled_sql = "SELECT * FROM users WHERE id = ?"
    execution_params = [1]
    operation_type: OperationType = "SELECT"

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

    assert hash(state1) == hash(state2)

    assert hash(state1) != hash(state3)


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

    stmt1 = SQL("SELECT * FROM users WHERE id IN (?)", [1, 2, 3])
    assert stmt1._is_many is False

    stmt2 = SQL("INSERT INTO users (id, name) VALUES (?, ?)", [(1, "john"), (2, "jane")])
    assert stmt2._is_many is True

    stmt3 = SQL("SELECT * FROM users WHERE id = ?", [(1,), (2,)], is_many=False)
    assert stmt3._is_many is False


def test_sql_lazy_processing_not_triggered_initially() -> None:
    """Test SQL processing is done lazily - not triggered on initialization."""
    stmt = SQL("SELECT * FROM users")

    assert stmt._processed_state is Empty


@requires_interpreted
def test_sql_single_pass_processing_triggered_by_sql_property() -> None:
    """Test accessing .sql property returns raw SQL without processing."""
    stmt = SQL("SELECT * FROM users")

    with patch("sqlspec.core.pipeline.compile_with_pipeline") as mock_compile:
        mock_compiled = CompiledSQL(
            compiled_sql="SELECT * FROM users",
            execution_parameters=[],
            operation_type="SELECT",
            expression=exp.select("*").from_("users"),
        )
        mock_compile.return_value = mock_compiled

        sql_result = stmt.sql

        mock_compile.assert_not_called()
        assert sql_result == "SELECT * FROM users"

        assert stmt._processed_state is Empty

        compiled_sql, params = stmt.compile()

        mock_compile.assert_called_once_with(stmt._statement_config, stmt._raw_sql, [], is_many=False, expression=None)
        assert compiled_sql == "SELECT * FROM users"
        assert params == []


@requires_interpreted
def test_sql_compilation_uses_raw_expression() -> None:
    """Ensure raw expressions are reused when provided."""
    expression = exp.select("*").from_("users")
    stmt = SQL(expression)

    with patch("sqlspec.core.pipeline.compile_with_pipeline") as mock_compile:
        mock_compiled = CompiledSQL(
            compiled_sql="SELECT * FROM users", execution_parameters=[], operation_type="SELECT", expression=expression
        )
        mock_compile.return_value = mock_compiled

        compiled_sql, params = stmt.compile()

        mock_compile.assert_called_once_with(
            stmt._statement_config, stmt._raw_sql, [], is_many=False, expression=expression
        )
        assert compiled_sql == "SELECT * FROM users"
        assert params == []


@requires_interpreted
def test_sql_single_pass_processing_triggered_by_parameters_property() -> None:
    """Test accessing .parameters property returns original parameters."""
    stmt = SQL("SELECT * FROM users WHERE id = ?", 1)

    with patch("sqlspec.core.pipeline.compile_with_pipeline") as mock_compile:
        mock_compiled = CompiledSQL(
            compiled_sql="SELECT * FROM users WHERE id = ?",
            execution_parameters=[1],
            operation_type="SELECT",
            expression=exp.select("*").from_("users"),
        )
        mock_compile.return_value = mock_compiled

        params = stmt.parameters

        mock_compile.assert_not_called()
        assert params == [1]
        assert stmt._processed_state is Empty


@requires_interpreted
def test_sql_single_pass_processing_triggered_by_operation_type_property() -> None:
    """Test accessing .operation_type property returns UNKNOWN without processing."""
    stmt = SQL("INSERT INTO users (name) VALUES ('john')")

    with patch("sqlspec.core.pipeline.compile_with_pipeline") as mock_compile:
        mock_compiled = CompiledSQL(
            compiled_sql="INSERT INTO users (name) VALUES ('john')",
            execution_parameters={},
            operation_type="INSERT",
            expression=MagicMock(),
        )
        mock_compile.return_value = mock_compiled

        op_type = stmt.operation_type

        mock_compile.assert_not_called()
        assert op_type == "UNKNOWN"
        assert stmt._processed_state is Empty


@requires_interpreted
def test_sql_processing_fallback_on_error() -> None:
    """Test SQL processing fallback when SQLProcessor fails."""
    stmt = SQL("INVALID SQL SYNTAX")

    with patch("sqlspec.core.pipeline.compile_with_pipeline") as mock_compile:
        mock_compile.side_effect = Exception("Processing failed")

        sql_result = stmt.sql
        assert sql_result == "INVALID SQL SYNTAX"
        assert stmt._processed_state is Empty

        compiled_sql, params = stmt.compile()

        assert compiled_sql == "INVALID SQL SYNTAX"
        assert params == []
        assert stmt.operation_type == "UNKNOWN"
        assert stmt._processed_state is not Empty


@requires_interpreted
def test_sql_expression_caching_enabled() -> None:
    """Test SQL expression caching when enabled."""
    config = StatementConfig(parameter_config=DEFAULT_PARAMETER_CONFIG, enable_caching=True)
    stmt = SQL("SELECT * FROM users", statement_config=config)

    with patch("sqlspec.core.pipeline.compile_with_pipeline") as mock_compile:
        expr = exp.select("*").from_("users")

        mock_compiled = CompiledSQL(
            compiled_sql="SELECT * FROM users", execution_parameters={}, operation_type="SELECT", expression=expr
        )
        mock_compile.return_value = mock_compiled

        assert stmt.expression is None

        stmt.compile()

        expr1 = stmt.expression

        expr2 = stmt.expression

        assert expr1 is expr2

        assert mock_compile.call_count == 1


@requires_interpreted
def test_sql_expression_caching_disabled() -> None:
    """Test SQL expression behavior when caching is disabled."""
    config = StatementConfig(parameter_config=DEFAULT_PARAMETER_CONFIG, enable_caching=False)
    stmt = SQL("SELECT * FROM users", statement_config=config)

    with patch("sqlspec.core.pipeline.compile_with_pipeline") as mock_compile:
        expr = exp.select("*").from_("users")

        mock_compiled = CompiledSQL(
            compiled_sql="SELECT * FROM users", execution_parameters={}, operation_type="SELECT", expression=expr
        )
        mock_compile.return_value = mock_compiled

        expr1 = stmt.expression
        expr2 = stmt.expression

        assert expr1 is expr2


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

    params = [(1, "john"), (2, "jane")]
    stmt = SQL("INSERT INTO users (id, name) VALUES (?, ?)", params)

    assert stmt._is_many is True
    assert stmt._positional_parameters == params


@requires_interpreted
def test_sql_parameters_property_returns_processed_parameters() -> None:
    """Test SQL.parameters property returns processed parameters."""
    stmt = SQL("SELECT * FROM users WHERE id = ?", 1)

    with patch("sqlspec.core.pipeline.compile_with_pipeline") as mock_compile:
        mock_compiled = CompiledSQL(
            compiled_sql="SELECT * FROM users WHERE id = ?",
            execution_parameters=[1],
            operation_type="SELECT",
            expression=exp.select("*").from_("users"),
        )
        mock_compile.return_value = mock_compiled

        params = stmt.parameters
        assert params == [1]


def test_sql_parameters_property_fallback_to_original() -> None:
    """Test SQL.parameters property falls back to original parameters when not processed."""
    stmt = SQL("SELECT * FROM users WHERE id = ?", 1)

    assert stmt._processed_state is Empty

    original_params = stmt._positional_parameters
    assert original_params == [1]


@pytest.mark.parametrize(
    "sql_statement,expected_operation_type",
    [
        ("SELECT * FROM users", "SELECT"),
        ("INSERT INTO users (name) VALUES ('john')", "INSERT"),
        ("UPDATE users SET name = 'jane' WHERE id = 1", "UPDATE"),
        ("DELETE FROM users WHERE id = 1", "DELETE"),
        ("WITH cte AS (SELECT * FROM users) SELECT * FROM cte", "SELECT"),
        ("CREATE TABLE users (id INT)", "DDL"),
        ("DROP TABLE users", "DDL"),
        ("EXECUTE sp_procedure", "EXECUTE"),
    ],
    ids=["select", "insert", "update", "delete", "cte", "create", "drop", "execute"],
)
@requires_interpreted
def test_sql_operation_type_detection(sql_statement: str, expected_operation_type: OperationType) -> None:
    """Test SQL operation type detection for various statement types."""
    stmt = SQL(sql_statement)

    with patch("sqlspec.core.pipeline.compile_with_pipeline") as mock_compile:
        mock_compiled = CompiledSQL(
            compiled_sql=sql_statement,
            execution_parameters={},
            operation_type=expected_operation_type,
            expression=MagicMock(),
        )
        mock_compile.return_value = mock_compiled

        stmt.compile()
        assert stmt.operation_type == expected_operation_type


def test_sql_returns_rows_detection() -> None:
    """Test SQL.returns_rows() method for different operation types."""

    select_stmt = SQL("SELECT * FROM users")
    select_stmt._processed_state = ProcessedState(
        compiled_sql="SELECT * FROM users", execution_parameters=[], operation_type="SELECT"
    )
    assert select_stmt.returns_rows() is True

    insert_stmt = SQL("INSERT INTO users (name) VALUES ('john')")
    insert_stmt._processed_state = ProcessedState(
        compiled_sql="INSERT INTO users (name) VALUES ('john')", execution_parameters=[], operation_type="INSERT"
    )
    assert insert_stmt.returns_rows() is False

    returning_stmt = SQL("INSERT INTO users (name) VALUES (:name) RETURNING id", name="alice")
    returning_stmt.compile()
    assert returning_stmt.returns_rows() is True
    assert returning_stmt.is_modifying_operation() is True

    with_stmt = SQL("WITH cte AS (SELECT * FROM users) SELECT * FROM cte")
    with_stmt._processed_state = ProcessedState(
        compiled_sql="WITH cte AS (SELECT * FROM users) SELECT * FROM cte",
        execution_parameters=[],
        operation_type="SELECT",
    )
    assert with_stmt.returns_rows() is True

    show_stmt = SQL("SHOW TABLES")
    show_stmt._processed_state = ProcessedState(
        compiled_sql="SHOW TABLES", execution_parameters=[], operation_type="SELECT"
    )
    assert show_stmt.returns_rows() is True


@pytest.mark.parametrize(
    "sql_text",
    [
        "SELECT 1 UNION ALL SELECT 2",
        "SELECT 1 EXCEPT SELECT 2",
        "SELECT 1 INTERSECT SELECT 1",
        "WITH cte AS (SELECT 1 AS id) SELECT * FROM cte",
    ],
    ids=["union", "except", "intersect", "cte_select"],
)
def test_sql_set_and_cte_operations_detect_as_select(sql_text: str) -> None:
    """Ensure set operations and CTE queries are detected as SELECT and return rows."""

    stmt = SQL(sql_text)
    stmt.compile()

    assert stmt.operation_type == "SELECT"
    assert stmt.returns_rows() is True
    assert stmt.is_modifying_operation() is False


def test_sql_slots_prevent_new_attributes() -> None:
    """Test SQL __slots__ prevent adding new attributes."""
    stmt = SQL("SELECT * FROM users")

    with pytest.raises(AttributeError):
        stmt.new_attribute = "test"  # pyright: ignore


def test_sql_hash_immutability() -> None:
    """Test SQL hash remains consistent (immutability indicator)."""
    stmt = SQL("SELECT * FROM users WHERE id = ?", 1)

    hash1 = hash(stmt)
    hash2 = hash(stmt)
    assert hash1 == hash2


def test_sql_equality_immutability() -> None:
    """Test SQL equality based on immutable attributes."""
    stmt1 = SQL("SELECT * FROM users WHERE id = ?", 1)
    stmt2 = SQL("SELECT * FROM users WHERE id = ?", 1)
    stmt3 = SQL("SELECT * FROM users WHERE id = ?", 2)

    assert stmt1 == stmt2
    assert hash(stmt1) == hash(stmt2)

    assert stmt1 != stmt3
    assert hash(stmt1) != hash(stmt3)


def test_sql_copy_creates_new_instance() -> None:
    """Test SQL.copy() creates new immutable instance."""
    original = SQL("SELECT * FROM users WHERE id = ?", 1)
    copy_stmt = original.copy(parameters=[2])

    assert copy_stmt is not original

    assert copy_stmt._positional_parameters != original._positional_parameters

    assert copy_stmt._raw_sql == original._raw_sql


def test_sql_as_script_creates_new_instance() -> None:
    """Test SQL.as_script() creates new immutable instance."""
    original = SQL("SELECT * FROM users")
    script_stmt = original.as_script()

    assert script_stmt is not original

    assert script_stmt._is_script is True
    assert original._is_script is False


def test_sql_add_named_parameter_creates_new_instance() -> None:
    """Test SQL.add_named_parameter() creates new immutable instance."""
    original = SQL("SELECT * FROM users WHERE id = :id", id=1)
    updated_stmt = original.add_named_parameter("name", "john")

    assert updated_stmt is not original

    assert "name" not in original._named_parameters

    assert updated_stmt._named_parameters["name"] == "john"
    assert updated_stmt._named_parameters["id"] == 1


@requires_interpreted
def test_sql_compile_method_compatibility() -> None:
    """Test SQL.compile() method returns same format as old API."""
    stmt = SQL("SELECT * FROM users WHERE id = ?", 1)

    with patch("sqlspec.core.pipeline.compile_with_pipeline") as mock_compile:
        mock_compiled = CompiledSQL(
            compiled_sql="SELECT * FROM users WHERE id = ?",
            execution_parameters=[1],
            operation_type="SELECT",
            expression=exp.select("*").from_("users"),
        )
        mock_compile.return_value = mock_compiled

        sql, params = stmt.compile()

        assert isinstance(sql, str)
        assert sql == "SELECT * FROM users WHERE id = ?"
        assert params == [1]


def test_sql_where_method_compatibility() -> None:
    """Test SQL.where() method creates new SQL with WHERE condition."""
    stmt = SQL("SELECT * FROM users")
    where_stmt = stmt.where("id > 10")

    assert where_stmt is not stmt

    assert "WHERE" not in stmt._raw_sql

    assert "WHERE" in where_stmt._raw_sql or "id > 10" in where_stmt._raw_sql


def test_sql_where_method_with_expression() -> None:
    """Test SQL.where() method works with SQLGlot expressions."""
    stmt = SQL("SELECT * FROM users")

    condition = exp.GT(this=exp.column("id"), expression=exp.Literal.number(10))
    where_stmt = stmt.where(condition)

    assert where_stmt is not stmt

    assert where_stmt._raw_sql != stmt._raw_sql


def test_sql_order_by_method_compatibility() -> None:
    """Test SQL.order_by() method creates new SQL with ORDER BY clause."""
    stmt = SQL("SELECT * FROM users")
    order_stmt = stmt.order_by("id")

    assert order_stmt is not stmt

    assert "ORDER BY" not in stmt._raw_sql

    assert "ORDER BY" in order_stmt._raw_sql or "id" in order_stmt._raw_sql


def test_sql_order_by_method_with_expression() -> None:
    """Test SQL.order_by() method works with SQLGlot expressions."""
    stmt = SQL("SELECT * FROM users")

    order_stmt = stmt.order_by(exp.column("name").desc())

    assert order_stmt is not stmt

    assert order_stmt._raw_sql != stmt._raw_sql


def test_sql_order_by_method_without_parsing() -> None:
    """Test SQL.order_by() when parsing is disabled."""
    config = StatementConfig(parameter_config=DEFAULT_PARAMETER_CONFIG, enable_parsing=False)
    stmt = SQL("SELECT * FROM users", statement_config=config)

    order_stmt = stmt.order_by("id DESC")

    assert "ORDER BY" in order_stmt._raw_sql or "DESC" in order_stmt._raw_sql


def test_sql_filters_property_compatibility() -> None:
    """Test SQL.filters property returns copy of filters list."""
    stmt = SQL("SELECT * FROM users")

    filters = stmt.filters
    assert filters == []

    assert filters is not stmt._filters


@requires_interpreted
def test_sql_validation_errors_property_compatibility() -> None:
    """Test SQL.validation_errors property compatibility."""
    stmt = SQL("SELECT * FROM users")

    with patch("sqlspec.core.pipeline.compile_with_pipeline") as mock_compile:
        mock_compiled = CompiledSQL(
            compiled_sql="SELECT * FROM users",
            execution_parameters={},
            operation_type="SELECT",
            expression=exp.select("*").from_("users"),
        )

        state = ProcessedState(
            compiled_sql="SELECT * FROM users",
            execution_parameters={},
            operation_type="SELECT",
            validation_errors=["Warning: Missing index"],
        )
        mock_compile.return_value = mock_compiled
        stmt._processed_state = state

        errors = stmt.validation_errors

        assert errors == ["Warning: Missing index"]

        assert errors is not state.validation_errors


def test_sql_has_errors_property_compatibility() -> None:
    """Test SQL.has_errors property compatibility."""
    stmt = SQL("SELECT * FROM users")

    stmt._processed_state = ProcessedState(
        compiled_sql="SELECT * FROM users", execution_parameters={}, operation_type="SELECT", validation_errors=[]
    )
    assert stmt.has_errors is False

    stmt._processed_state = ProcessedState(
        compiled_sql="SELECT * FROM users",
        execution_parameters={},
        operation_type="SELECT",
        validation_errors=["Error: Invalid syntax"],
    )
    assert stmt.has_errors is True


@requires_interpreted
def test_sql_single_parse_guarantee() -> None:
    """Test SQL guarantees single parse operation."""
    stmt = SQL("SELECT * FROM users WHERE id = ?", 1)

    with patch("sqlspec.core.pipeline.compile_with_pipeline") as mock_compile:
        mock_compiled = CompiledSQL(
            compiled_sql="SELECT * FROM users WHERE id = ?",
            execution_parameters=[1],
            operation_type="SELECT",
            expression=exp.select("*").from_("users"),
        )
        mock_compile.return_value = mock_compiled

        _ = stmt.sql
        _ = stmt.operation_type
        _ = stmt.expression
        _ = stmt.parameters
        _ = stmt.compile()

        assert mock_compile.call_count == 1


def test_sql_lazy_evaluation_performance() -> None:
    """Test SQL lazy evaluation avoids unnecessary work."""
    stmt = SQL("SELECT * FROM users")

    assert stmt._processed_state is Empty

    _ = stmt._raw_sql
    _ = stmt.statement_config
    _ = stmt._is_many
    _ = stmt._is_script

    assert stmt._processed_state is Empty


@requires_interpreted
def test_sql_processing_caching_performance() -> None:
    """Test SQL processing result caching for performance."""
    stmt = SQL("SELECT * FROM users")

    with patch("sqlspec.core.pipeline.compile_with_pipeline") as mock_compile:
        mock_compiled = CompiledSQL(
            compiled_sql="SELECT * FROM users",
            execution_parameters={},
            operation_type="SELECT",
            expression=exp.select("*").from_("users"),
        )
        mock_compile.return_value = mock_compiled

        stmt.compile()
        assert stmt._processed_state is not Empty

        result1 = stmt.sql
        result2 = stmt.sql

        assert result1 == result2

        assert mock_compile.call_count == 1


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

    assert stmt._raw_sql == sql_with_comments


def test_sql_with_complex_parameters() -> None:
    """Test SQL with complex parameter scenarios."""

    sql = "SELECT * FROM users WHERE id = ? AND name = :name AND email = $1"
    stmt = SQL(sql, 1, name="john", email="john@example.com")

    assert stmt._positional_parameters == [1]
    assert stmt._named_parameters == {"name": "john", "email": "john@example.com"}


def test_sql_empty_and_whitespace() -> None:
    """Test SQL handles empty and whitespace-only input."""

    empty_stmt = SQL("")
    assert empty_stmt._raw_sql == ""

    whitespace_stmt = SQL("   \n\t   ")
    assert whitespace_stmt._raw_sql == "   \n\t   "


@requires_interpreted
def test_sql_invalid_syntax_handling() -> None:
    """Test SQL handles invalid syntax gracefully."""
    invalid_stmt = SQL("INVALID SQL SYNTAX !@#$%")
    assert "INVALID" in invalid_stmt._raw_sql

    with patch("sqlspec.core.pipeline.compile_with_pipeline") as mock_compile:
        mock_compile.side_effect = Exception("Parse error")

        sql_result = invalid_stmt.sql
        op_type = invalid_stmt.operation_type

        assert sql_result == "INVALID SQL SYNTAX !@#$%"
        assert op_type == "UNKNOWN"


def test_sql_special_characters_and_unicode() -> None:
    """Test SQL handles special characters and Unicode."""
    unicode_sql = "SELECT * FROM users WHERE name = 'José' AND city = '北京'"
    stmt = SQL(unicode_sql)

    assert stmt._raw_sql == unicode_sql


def test_sql_very_long_query() -> None:
    """Test SQL handles very long queries."""

    columns = [f"column_{i}" for i in range(100)]
    long_sql = f"SELECT {', '.join(columns)} FROM users"

    stmt = SQL(long_sql)
    assert stmt._raw_sql == long_sql


def test_sql_repr_format() -> None:
    """Test SQL __repr__ provides useful debugging information."""

    stmt1 = SQL("SELECT * FROM users")
    repr1 = repr(stmt1)
    assert "SQL(" in repr1
    assert "SELECT * FROM users" in repr1

    stmt2 = SQL("SELECT * FROM users WHERE id = ?", 1)
    repr2 = repr(stmt2)
    assert "params=[1]" in repr2

    stmt3 = SQL("SELECT * FROM users WHERE id = :id", id=1)
    repr3 = repr(stmt3)
    assert "named_params={'id': 1}" in repr3

    stmt4 = SQL("SELECT * FROM users", is_many=True)
    stmt4_script = stmt4.as_script()
    repr4 = repr(stmt4_script)
    assert "is_script" in repr4


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

    for stmt in statements:
        slots = getattr(type(stmt), "__slots__", None)
        if slots is not None:
            assert "__dict__" not in slots
        assert not hasattr(stmt, "__dict__")


def test_sql_consistent_behavior_across_multiple_instances(sample_sqls: "list[str]") -> None:
    """Test SQL behavior is consistent across multiple instances."""
    statements = [SQL(sql) for sql in sample_sqls]

    assert len(statements) == len(sample_sqls)

    for stmt in statements:
        assert stmt._processed_state is Empty
        assert isinstance(stmt.statement_config, StatementConfig)
        assert stmt._hash is None


def test_sql_immutable_after_creation() -> None:
    """Test SQL objects are effectively immutable after creation."""
    stmt = SQL("SELECT * FROM users WHERE id = ?", 1)

    original_raw_sql = stmt._raw_sql
    original_params = stmt._positional_parameters
    original_config = stmt.statement_config

    _ = stmt.sql
    _ = stmt.operation_type

    assert stmt._raw_sql is original_raw_sql
    assert stmt._positional_parameters is original_params
    assert stmt.statement_config is original_config


@requires_interpreted
def test_sql_processing_state_stability() -> None:
    """Test SQL processing state remains stable after first access."""
    stmt = SQL("SELECT * FROM users")

    with patch("sqlspec.core.pipeline.compile_with_pipeline") as mock_compile:
        mock_compiled = CompiledSQL(
            compiled_sql="SELECT * FROM users",
            execution_parameters={},
            operation_type="SELECT",
            expression=exp.select("*").from_("users"),
        )
        mock_compile.return_value = mock_compiled

        _ = stmt.sql
        first_state = stmt._processed_state

        _ = stmt.operation_type
        _ = stmt.expression

        assert stmt._processed_state is first_state


def test_processed_state_parameter_profile_exposed() -> None:
    """Processed state exposes parameter metadata for downstream adapters."""

    sql_instance = SQL("SELECT :id::int", {"id": 7})
    sql_instance.compile()

    processed_state = sql_instance.get_processed_state()
    profile = processed_state.parameter_profile

    assert profile.total_count == 1
    assert profile.styles == (ParameterStyle.QMARK.value,)
    assert profile.placeholder_count("?") == 1


def test_shared_pipeline_metrics_respects_debug_flag() -> None:
    """Shared pipeline metrics emit data only when debug flag is enabled."""

    previous = os.environ.get("SQLSPEC_DEBUG_PIPELINE_CACHE")
    os.environ["SQLSPEC_DEBUG_PIPELINE_CACHE"] = "1"
    try:
        reset_pipeline_registry()

        SQL("SELECT 1").compile()
        SQL("SELECT 1").compile()

        metrics = get_pipeline_metrics()
        total_hits = sum(entry.get("hits", 0) for entry in metrics)
        assert total_hits >= 1
    finally:
        if previous is None:
            os.environ.pop("SQLSPEC_DEBUG_PIPELINE_CACHE", None)
        else:
            os.environ["SQLSPEC_DEBUG_PIPELINE_CACHE"] = previous
        reset_pipeline_registry()


def test_sql_builder_from_named_parameters() -> None:
    """SQL.builder returns a builder seeded with named parameters."""
    stmt = SQL("SELECT * FROM users WHERE id = :id", id=1)

    builder = stmt.builder()
    built_query = builder.build()

    assert built_query.parameters == {"id": 1}
    assert ":id" in built_query.sql


def test_sql_builder_from_positional_parameters() -> None:
    """SQL.builder normalizes positional parameters to named placeholders."""
    stmt = SQL("SELECT * FROM users WHERE id = ?", 42)

    builder = stmt.builder()
    built_query = builder.build()

    assert built_query.parameters == {"param_0": 42}
    assert ":param_0" in built_query.sql


def test_sql_builder_handles_cte_statement() -> None:
    """SQL.builder preserves WITH clauses."""
    stmt = SQL("WITH cte AS (SELECT 1) SELECT * FROM cte")

    builder = stmt.builder()
    built_query = builder.build()

    assert "WITH" in built_query.sql.upper()


def test_sql_builder_handles_ddl_statement() -> None:
    """SQL.builder returns a builder for DDL statements."""
    stmt = SQL("CREATE TABLE test_table (id INT)")

    builder = stmt.builder()
    built_query = builder.build()

    assert "CREATE TABLE" in built_query.sql.upper()
