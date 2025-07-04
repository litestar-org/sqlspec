"""Unit tests for SQL.as_many() method."""

from sqlspec.statement.sql import SQL


def test_as_many_with_parameters() -> None:
    """Test as_many() method with parameters list."""
    parameters = [("Item 1", 100, "A"), ("Item 2", 200, "B"), ("Item 3", 300, "A")]

    sql = SQL("INSERT INTO test_table (name, value, category) VALUES (?, ?, ?)")
    sql_many = sql.as_many(parameters)

    # Check properties
    assert sql_many.is_many is True
    assert sql_many.parameters == parameters
    assert sql_many._original_parameters == parameters

    # Check that compile returns the correct parameters
    compiled_sql, compiled_params = sql_many.compile()
    assert compiled_params == parameters


def test_as_many_with_empty_list() -> None:
    """Test as_many() method with empty list."""
    sql = SQL("INSERT INTO test_table (name, value, category) VALUES (?, ?, ?)")
    sql_many = sql.as_many([])

    assert sql_many.is_many is True
    assert sql_many.parameters == []
    assert sql_many._original_parameters == []

    # Check compile
    _compiled_sql, compiled_params = sql_many.compile()
    assert compiled_params == []


def test_as_many_without_parameters() -> None:
    """Test as_many() method without parameters."""
    sql = SQL("INSERT INTO test_table (name, value, category) VALUES (?, ?, ?)")
    sql_many = sql.as_many()

    assert sql_many.is_many is True
    # When no parameters are provided, original_parameters is None
    assert sql_many._original_parameters is None

    # Parameters property should process normally
    params = sql_many.parameters
    assert params == {} or params is None or params == []


def test_as_many_with_placeholder_conversion() -> None:
    """Test as_many() with placeholder style conversion."""
    parameters = [("Item 1", 100, "A"), ("Item 2", 200, "B")]

    # Original SQL with numeric placeholders
    sql = SQL("INSERT INTO test_table (name, value, category) VALUES ($1, $2, $3)")
    sql_many = sql.as_many(parameters)

    # Convert to qmark style
    compiled_sql, compiled_params = sql_many.compile(placeholder_style="qmark")
    assert "?" in compiled_sql
    assert "$1" not in compiled_sql
    assert compiled_params == parameters


def test_as_many_preserves_list_of_tuples() -> None:
    """Test that as_many() preserves the list of tuples structure for execute_many."""
    # Different parameter formats
    tuple_params = [("Item 1", 100), ("Item 2", 200)]

    list_params = [["Item 3", 300], ["Item 4", 400]]

    # Test with tuples
    sql1 = SQL("INSERT INTO test_table (name, value) VALUES (?, ?)").as_many(tuple_params)
    assert sql1.parameters == tuple_params
    assert isinstance(sql1.parameters[0], tuple)

    # Test with lists
    sql2 = SQL("INSERT INTO test_table (name, value) VALUES (?, ?)").as_many(list_params)
    assert sql2.parameters == list_params
    assert isinstance(sql2.parameters[0], list)


def test_as_many_chaining() -> None:
    """Test that as_many() can be chained with other methods."""
    base_sql = SQL("INSERT INTO test_table (name, value) VALUES (?, ?)")

    # Create as_many first
    sql_many = base_sql.as_many([("Item 1", 100)])

    # Should still be marked as many
    assert sql_many.is_many is True

    # Copy should preserve is_many flag and parameters
    sql_copy = sql_many.copy()
    assert sql_copy.is_many is True
    # Check that parameters property returns the correct value
    assert sql_copy.parameters == [("Item 1", 100)]

    # Check that compile also returns correct parameters
    _, params = sql_copy.compile()
    assert params == [("Item 1", 100)]
