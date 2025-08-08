"""Unit tests for SQL is_many parameter functionality."""

from sqlspec.core.statement import SQL


def test_is_many_with_parameters() -> None:
    """Test is_many=True with parameters list."""
    parameters = [("Item 1", 100, "A"), ("Item 2", 200, "B"), ("Item 3", 300, "A")]

    sql_many = SQL("INSERT INTO test_table (name, value, category) VALUES (?, ?, ?)", parameters, is_many=True)

    # Check properties
    assert sql_many.is_many is True
    assert sql_many.parameters == parameters
    assert sql_many._original_parameters == parameters

    # Check that compile returns the correct parameters
    compiled_sql, compiled_parameters = sql_many.compile()
    assert compiled_parameters == parameters


def test_is_many_with_empty_list() -> None:
    """Test is_many=True with empty list."""
    sql_many = SQL("INSERT INTO test_table (name, value, category) VALUES (?, ?, ?)", [], is_many=True)

    assert sql_many.is_many is True
    assert sql_many.parameters == []
    assert sql_many._original_parameters == []

    # Check compile
    _compiled_sql, compiled_parameters = sql_many.compile()
    assert compiled_parameters == []


def test_is_many_without_parameters() -> None:
    """Test is_many=True without parameters."""
    sql_many = SQL("INSERT INTO test_table (name, value, category) VALUES (?, ?, ?)", is_many=True)

    assert sql_many.is_many is True
    # When no parameters are provided, original_parameters is None
    assert sql_many._original_parameters is None

    # Parameters property should process normally
    parameters = sql_many.parameters
    assert parameters == {} or parameters is None or parameters == []


def test_is_many_with_placeholder_conversion() -> None:
    """Test is_many=True with placeholder style conversion."""
    parameters = [("Item 1", 100, "A"), ("Item 2", 200, "B")]

    # Original SQL with numeric placeholders
    sql_many = SQL("INSERT INTO test_table (name, value, category) VALUES ($1, $2, $3)", parameters, is_many=True)

    # Convert to qmark style
    compiled_sql, compiled_parameters = sql_many.compile(placeholder_style="qmark")
    assert "?" in compiled_sql
    assert "$1" not in compiled_sql
    assert compiled_parameters == parameters


def test_is_many_preserves_list_of_tuples() -> None:
    """Test that is_many=True preserves the list of tuples structure for execute_many."""
    # Different parameter formats
    tuple_parameters = [("Item 1", 100), ("Item 2", 200)]

    list_parameters = [["Item 3", 300], ["Item 4", 400]]

    # Test with tuples
    sql1 = SQL("INSERT INTO test_table (name, value) VALUES (?, ?)", tuple_parameters, is_many=True)
    assert sql1.parameters == tuple_parameters
    assert isinstance(sql1.parameters[0], tuple)

    # Test with lists
    sql2 = SQL("INSERT INTO test_table (name, value) VALUES (?, ?)", list_parameters, is_many=True)
    assert sql2.parameters == list_parameters
    assert isinstance(sql2.parameters[0], list)


def test_is_many_chaining() -> None:
    """Test that is_many=True works with other methods."""
    # Create SQL with is_many=True
    sql_many = SQL("INSERT INTO test_table (name, value) VALUES (?, ?)", [("Item 1", 100)], is_many=True)

    # Should be marked as many
    assert sql_many.is_many is True

    # Copy should preserve is_many flag and parameters
    sql_copy = sql_many.copy()
    assert sql_copy.is_many is True
    # Check that parameters property returns the correct value
    assert sql_copy.parameters == [("Item 1", 100)]

    # Check that compile also returns correct parameters
    _, parameters = sql_copy.compile()
    assert parameters == [("Item 1", 100)]
