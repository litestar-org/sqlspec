"""Unit tests for ADBC transformers."""

from sqlglot import parse_one

from sqlspec.adapters.adbc.transformers import AdbcPostgresTransformer
from sqlspec.statement.pipelines.context import SQLProcessingContext
from sqlspec.statement.sql import SQLConfig


def test_adbc_postgres_transformer_empty_params() -> None:
    """Test transformer handles empty parameter list."""
    sql = "INSERT INTO test_table (col1, col2) VALUES ($1, $2)"
    expression = parse_one(sql, dialect="postgres")

    config = SQLConfig(dialect="postgres")
    context = SQLProcessingContext(
        initial_sql_string=sql,
        dialect="postgres",
        config=config,
        merged_parameters=[],  # Empty parameter list
    )

    transformer = AdbcPostgresTransformer()
    result = transformer.process(expression, context)

    # Should not transform for empty params (driver handles it)
    assert result is not None
    assert "$1" in result.sql(dialect="postgres")
    assert "$2" in result.sql(dialect="postgres")
    assert transformer.is_empty_params is True
    assert transformer.all_params_null is False


def test_adbc_postgres_transformer_all_null_params() -> None:
    """Test transformer handles all NULL parameters."""
    sql = "INSERT INTO test_table (col1, col2, col3) VALUES ($1, $2, $3)"
    expression = parse_one(sql, dialect="postgres")

    config = SQLConfig(dialect="postgres")
    context = SQLProcessingContext(
        initial_sql_string=sql,
        dialect="postgres",
        config=config,
        merged_parameters=[None, None, None],  # All NULL parameters
    )

    transformer = AdbcPostgresTransformer()
    result = transformer.process(expression, context)

    # Should transform placeholders to NULL
    assert result is not None
    result_sql = result.sql(dialect="postgres")
    assert "NULL" in result_sql
    assert "$1" not in result_sql
    assert "$2" not in result_sql
    assert "$3" not in result_sql

    # Parameters should be cleared
    assert context.merged_parameters == []
    assert transformer.all_params_null is True
    assert transformer.is_empty_params is False


def test_adbc_postgres_transformer_mixed_params() -> None:
    """Test transformer with mixed NULL and non-NULL parameters."""
    sql = "INSERT INTO test_table (col1, col2) VALUES ($1, $2)"
    expression = parse_one(sql, dialect="postgres")

    config = SQLConfig(dialect="postgres")
    context = SQLProcessingContext(
        initial_sql_string=sql,
        dialect="postgres",
        config=config,
        merged_parameters=["value1", None],  # Mixed parameters
    )

    transformer = AdbcPostgresTransformer()
    result = transformer.process(expression, context)

    # Should transform NULL parameters and renumber remaining ones
    assert result is not None
    result_sql = result.sql(dialect="postgres")
    assert "$1" in result_sql  # First parameter stays as $1
    assert "$2" not in result_sql  # Second parameter becomes NULL
    assert "NULL" in result_sql  # NULL is in the SQL

    # Parameters should be modified to remove NULLs
    assert context.merged_parameters == ["value1"]
    assert transformer.all_params_null is False
    assert transformer.is_empty_params is False
    assert transformer.has_null_params is True
    assert transformer.null_param_indices == [1]  # Second parameter (index 1) is NULL


def test_adbc_postgres_transformer_dict_params_all_null() -> None:
    """Test transformer with dictionary parameters where all values are NULL."""
    sql = "INSERT INTO test_table (col1, col2) VALUES (:param1, :param2)"
    expression = parse_one(sql, dialect="postgres")

    config = SQLConfig(dialect="postgres")
    context = SQLProcessingContext(
        initial_sql_string=sql,
        dialect="postgres",
        config=config,
        merged_parameters={"param1": None, "param2": None},  # All NULL dict params
    )

    transformer = AdbcPostgresTransformer()
    transformer.process(expression, context)

    # Should detect all NULL and clear parameters to empty dict
    assert transformer.all_params_null is True
    assert context.merged_parameters == {}  # Dict params become empty dict, not empty list
    assert transformer.has_null_params is True


def test_adbc_postgres_transformer_complex_mixed_nulls() -> None:
    """Test transformer with complex mixed NULL scenario."""
    sql = "INSERT INTO test_table (col1, col2, col3, col4, col5) VALUES ($1, $2, $3, $4, $5)"
    expression = parse_one(sql, dialect="postgres")

    config = SQLConfig(dialect="postgres")
    context = SQLProcessingContext(
        initial_sql_string=sql,
        dialect="postgres",
        config=config,
        merged_parameters=["value1", None, "value3", None, "value5"],  # Mixed NULLs
    )

    transformer = AdbcPostgresTransformer()
    result = transformer.process(expression, context)

    # Should transform NULL parameters and renumber remaining ones
    assert result is not None
    result_sql = result.sql(dialect="postgres")

    # Check the SQL contains correct parameter numbering and NULLs
    assert "$1" in result_sql  # value1 stays as $1
    assert "$2" in result_sql  # value3 becomes $2
    assert "$3" in result_sql  # value5 becomes $3
    assert "$4" not in result_sql  # No $4 anymore
    assert "$5" not in result_sql  # No $5 anymore
    assert result_sql.count("NULL") == 2  # Two NULLs in the SQL

    # Parameters should be modified to remove NULLs
    assert context.merged_parameters == ["value1", "value3", "value5"]
    assert transformer.null_param_indices == [1, 3]  # Indices 1 and 3 (0-based) are NULL
