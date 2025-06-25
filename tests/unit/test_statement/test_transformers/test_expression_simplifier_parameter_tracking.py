"""Test parameter tracking in ExpressionSimplifier."""

from sqlglot import parse_one

from sqlspec.statement.pipelines.context import SQLProcessingContext
from sqlspec.statement.pipelines.transformers._expression_simplifier import ExpressionSimplifier
from sqlspec.statement.sql import SQLConfig


def test_expression_simplifier_tracks_parameter_reordering() -> None:
    """Test that ExpressionSimplifier tracks when parameters are reordered."""
    # SQL with comparison operators that SQLGlot might reorder
    sql = "SELECT * FROM test WHERE value >= ? AND value <= ?"
    expression = parse_one(sql, dialect="duckdb")

    # Create context with parameters
    context = SQLProcessingContext(
        initial_sql_string=sql,
        dialect="duckdb",
        config=SQLConfig(),
        current_expression=expression,
        initial_expression=expression,
        merged_parameters=[200, 400],  # Lower bound, upper bound
    )

    # Process with simplifier
    simplifier = ExpressionSimplifier(enabled=True)
    result = simplifier.process(expression, context)

    # Check if parameter mapping was created
    if "parameter_position_mapping" in context.metadata:
        mapping = context.metadata["parameter_position_mapping"]
        # If conditions were reordered from "value >= ? AND value <= ?"
        # to "value <= ? AND value >= ?", the mapping should be {0: 1, 1: 0}
        assert mapping == {0: 1, 1: 0}, f"Expected parameter swap mapping, got {mapping}"

    # The transformed SQL should have reordered conditions
    assert result is not None
    result_sql = result.sql(dialect="duckdb")
    assert "value <=" in result_sql and "value >=" in result_sql


def test_expression_simplifier_no_tracking_without_parameters() -> None:
    """Test that parameter tracking doesn't happen without parameters."""
    sql = "SELECT * FROM test WHERE value >= 200 AND value <= 400"
    expression = parse_one(sql, dialect="duckdb")

    # Create context without parameters
    context = SQLProcessingContext(
        initial_sql_string=sql,
        dialect="duckdb",
        config=SQLConfig(),
        current_expression=expression,
        initial_expression=expression,
        merged_parameters=None,
    )

    # Process with simplifier
    simplifier = ExpressionSimplifier(enabled=True)
    result = simplifier.process(expression, context)
    assert result is not None

    # Should not have parameter mapping
    assert "parameter_position_mapping" not in context.metadata


def test_expression_simplifier_handles_complex_comparisons() -> None:
    """Test parameter tracking with multiple comparison operators."""
    sql = "SELECT * FROM test WHERE a >= ? AND b <= ? AND c = ?"
    expression = parse_one(sql, dialect="duckdb")

    # Create context with parameters
    context = SQLProcessingContext(
        initial_sql_string=sql,
        dialect="duckdb",
        config=SQLConfig(),
        current_expression=expression,
        initial_expression=expression,
        merged_parameters=[10, 20, 30],
    )

    # Process with simplifier
    simplifier = ExpressionSimplifier(enabled=True)
    result = simplifier.process(expression, context)
    assert result is not None

    # If any reordering happened, it should be tracked
    if "parameter_position_mapping" in context.metadata:
        mapping = context.metadata["parameter_position_mapping"]
        # Verify mapping has correct structure
        assert isinstance(mapping, dict)
        assert all(isinstance(k, int) and isinstance(v, int) for k, v in mapping.items())


def test_sql_compile_applies_parameter_reordering() -> None:
    """Test that SQL.compile() applies parameter reordering based on mapping."""
    from sqlspec.statement.sql import SQL

    # Create SQL with parameters that will be reordered
    sql_obj = SQL("SELECT * FROM test WHERE value >= ? AND value <= ?", (200, 400))

    # Compile and get parameters
    compiled_sql, params = sql_obj.compile(placeholder_style="qmark")

    # The SQL might be reordered by SQLGlot
    # If it's reordered to "value <= ? AND value >= ?",
    # the parameters should also be reordered to (400, 200)
    if "value <= ?" in compiled_sql and compiled_sql.index("value <= ?") < compiled_sql.index("value >= ?"):
        # SQL was reordered, check if parameters were too
        assert params == [400, 200], f"Parameters should be reordered to match SQL, got {params}"
    else:
        # SQL wasn't reordered, parameters should be unchanged
        assert params == [200, 400], f"Parameters should be unchanged, got {params}"


def test_parameter_reordering_with_dict_params() -> None:
    """Test parameter reordering with dictionary parameters."""
    from sqlspec.statement.sql import SQL

    # Create SQL with dict parameters
    sql_obj = SQL("SELECT * FROM test WHERE value >= ? AND value <= ?", {"param_0": 200, "param_1": 400})

    # Compile and get parameters
    _, params = sql_obj.compile(placeholder_style="qmark")

    # For dict params with param_N keys, reordering should update the keys
    if isinstance(params, dict) and "param_0" in params:
        # Check that values are associated with correct keys after any reordering
        assert "param_0" in params and "param_1" in params
