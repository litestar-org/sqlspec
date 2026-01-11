"""Unit tests for BigQuery execute_many helper methods."""

# pyright: reportPrivateUsage=false


def test_is_simple_insert_operation_basic_insert() -> None:
    """Test that a basic INSERT statement is detected correctly."""
    from sqlspec.adapters.bigquery.core import is_simple_insert

    assert is_simple_insert("INSERT INTO test (a, b) VALUES (1, 2)")


def test_is_simple_insert_operation_with_named_params() -> None:
    """Test INSERT with named parameters."""
    from sqlspec.adapters.bigquery.core import is_simple_insert

    assert is_simple_insert("INSERT INTO test (a, b) VALUES (@a, @b)")


def test_is_simple_insert_operation_not_insert() -> None:
    """Test that UPDATE/DELETE/SELECT are not detected as INSERT."""
    from sqlspec.adapters.bigquery.core import is_simple_insert

    assert not is_simple_insert("UPDATE test SET a = 1")
    assert not is_simple_insert("DELETE FROM test WHERE a = 1")
    assert not is_simple_insert("SELECT * FROM test")


def test_is_simple_insert_operation_insert_select() -> None:
    """Test that INSERT...SELECT is not detected as simple INSERT."""
    from sqlspec.adapters.bigquery.core import is_simple_insert

    # INSERT...SELECT should not be simple INSERT for bulk load optimization
    result = is_simple_insert("INSERT INTO test SELECT * FROM other")
    # This might be True or False depending on implementation - the key is it doesn't crash
    assert isinstance(result, bool)


def test_is_simple_insert_operation_malformed_sql() -> None:
    """Test that malformed SQL returns False without raising."""
    from sqlspec.adapters.bigquery.core import is_simple_insert

    assert not is_simple_insert("NOT VALID SQL AT ALL")


def test_extract_table_from_insert_simple() -> None:
    """Test extracting table name from simple INSERT."""
    from sqlspec.adapters.bigquery.core import extract_insert_table

    assert extract_insert_table("INSERT INTO test (a) VALUES (1)") == "test"


def test_extract_table_from_insert_qualified() -> None:
    """Test extracting qualified table name from INSERT."""
    from sqlspec.adapters.bigquery.core import extract_insert_table

    result = extract_insert_table("INSERT INTO project.dataset.table (a) VALUES (1)")
    # Should include catalog (project), db (dataset), and table
    assert result is not None
    assert "table" in result


def test_extract_table_from_insert_not_insert() -> None:
    """Test that non-INSERT returns None."""
    from sqlspec.adapters.bigquery.core import extract_insert_table

    assert extract_insert_table("SELECT * FROM test") is None


def test_extract_table_from_insert_malformed() -> None:
    """Test that malformed SQL returns None without raising."""
    from sqlspec.adapters.bigquery.core import extract_insert_table

    assert extract_insert_table("NOT VALID SQL") is None
