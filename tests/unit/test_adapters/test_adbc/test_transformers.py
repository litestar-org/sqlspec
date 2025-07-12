"""Unit tests for ADBC pipeline steps."""

from sqlglot import parse_one

from sqlspec.adapters.adbc.pipeline_steps import adbc_null_transform_step
from sqlspec.statement.pipeline import SQLTransformContext


def test_adbc_postgres_transformer_empty_params() -> None:
    """Test transformer handles empty parameter list."""
    sql = "INSERT INTO test_table (col1, col2) VALUES ($1, $2)"
    expression = parse_one(sql, dialect="postgres")

    context = SQLTransformContext(
        current_expression=expression,
        original_expression=expression,
        dialect="postgres",
        parameters={},  # Empty parameter dict
    )

    result = adbc_null_transform_step(context)

    # Should not transform for empty params (driver handles it)
    assert "$1" in result.current_expression.sql(dialect="postgres")
    assert "$2" in result.current_expression.sql(dialect="postgres")
    assert result.parameters == {}


def test_adbc_postgres_transformer_all_null_params() -> None:
    """Test transformer handles all NULL parameters."""
    sql = "INSERT INTO test_table (col1, col2, col3) VALUES ($1, $2, $3)"
    expression = parse_one(sql, dialect="postgres")

    context = SQLTransformContext(
        current_expression=expression,
        original_expression=expression,
        dialect="postgres",
        parameters={"param_0": None, "param_1": None, "param_2": None},  # All NULL parameters
    )

    result = adbc_null_transform_step(context)

    # Should transform placeholders to NULL
    result_sql = result.current_expression.sql(dialect="postgres")
    assert "NULL" in result_sql
    assert "$1" not in result_sql
    assert "$2" not in result_sql
    assert "$3" not in result_sql

    # Parameters should be cleared
    assert result.parameters == {}
    assert result.metadata["adbc_null_transform_applied"] is True
    assert result.metadata["null_parameter_count"] == 3


def test_adbc_postgres_transformer_mixed_params() -> None:
    """Test transformer with mixed NULL and non-NULL parameters."""
    sql = "INSERT INTO test_table (col1, col2) VALUES ($1, $2)"
    expression = parse_one(sql, dialect="postgres")

    context = SQLTransformContext(
        current_expression=expression,
        original_expression=expression,
        dialect="postgres",
        parameters={"param_0": "value1", "param_1": None},  # Mixed parameters
    )

    result = adbc_null_transform_step(context)

    # Should transform NULL parameters to NULL nodes
    result_sql = result.current_expression.sql(dialect="postgres")
    assert "$1" in result_sql  # First parameter stays as $1
    assert "$2" not in result_sql  # Second parameter becomes NULL
    assert "NULL" in result_sql  # NULL is in the SQL

    # Parameters should be modified to remove NULLs
    assert result.parameters == {"param_0": "value1"}
    assert result.metadata["adbc_null_transform_applied"] is True
    assert result.metadata["null_parameter_count"] == 1


def test_adbc_postgres_transformer_dict_params_all_null() -> None:
    """Test transformer with dictionary parameters where all values are NULL."""
    sql = "INSERT INTO test_table (col1, col2) VALUES (:param1, :param2)"
    expression = parse_one(sql, dialect="postgres")

    context = SQLTransformContext(
        current_expression=expression,
        original_expression=expression,
        dialect="postgres",
        parameters={"param1": None, "param2": None},  # All NULL dict params
    )

    result = adbc_null_transform_step(context)

    # Should detect all NULL and clear parameters
    assert result.parameters == {}
    assert result.metadata["adbc_null_transform_applied"] is True
    assert result.metadata["null_parameter_count"] == 2


def test_adbc_postgres_transformer_complex_mixed_nulls() -> None:
    """Test transformer with complex mixed NULL scenario."""
    sql = "INSERT INTO test_table (col1, col2, col3, col4, col5) VALUES ($1, $2, $3, $4, $5)"
    expression = parse_one(sql, dialect="postgres")

    context = SQLTransformContext(
        current_expression=expression,
        original_expression=expression,
        dialect="postgres",
        parameters={
            "param_0": "value1",
            "param_1": None,
            "param_2": "value3",
            "param_3": None,
            "param_4": "value5",
        },  # Mixed NULLs
    )

    result = adbc_null_transform_step(context)

    # Should transform NULL parameters to NULL nodes
    result_sql = result.current_expression.sql(dialect="postgres")

    # Check the SQL contains correct parameter placeholders and NULLs
    # The transformer replaces NULL parameters with NULL and renumbers remaining parameters
    assert "$1" in result_sql  # value1 stays as $1
    assert "$2" in result_sql  # value3 renumbered to $2
    assert "$3" in result_sql  # value5 renumbered to $3
    assert "$4" not in result_sql  # No $4 after renumbering
    assert "$5" not in result_sql  # No $5 after renumbering
    assert result_sql.count("NULL") == 2  # Two NULLs in the SQL

    # Parameters should be modified to remove NULLs
    assert result.parameters == {"param_0": "value1", "param_2": "value3", "param_4": "value5"}
    assert result.metadata["null_parameter_count"] == 2
