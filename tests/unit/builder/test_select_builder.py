"""Unit tests for SELECT builder methods."""

import pytest

from sqlspec import sql


def test_select_only_replaces_columns() -> None:
    """Test select_only() replaces existing selected columns."""
    query = sql.select("id", "name").from_("users")

    assert "id" in query.to_sql()
    assert "name" in query.to_sql()

    query.select_only("email")

    sql_str = query.to_sql()
    assert "id" not in sql_str
    assert "name" not in sql_str
    assert "email" in sql_str

def test_select_only_multiple_columns() -> None:
    """Test select_only() handles multiple columns."""
    query = sql.select("id").from_("users")

    query.select_only("name", "email")

    sql_str = query.to_sql()
    assert "id" not in sql_str
    assert "name" in sql_str
    assert "email" in sql_str

def test_select_only_on_new_query() -> None:
    """Test select_only() works on fresh query."""
    query = sql.select("id").from_("users")
    # Fresh query from same builder if possible, but here we modify in place
    # so we just test basic functionality again to be sure
    query.select_only("active")
    assert "active" in query.to_sql()

def test_select_only_with_expressions() -> None:
    """Test select_only() works with SQL expressions (strings)."""
    query = sql.select("id").from_("users")

    # Using string expressions which are definitely supported
    query.select_only("COUNT(id) AS count")

    sql_str = query.to_sql().upper()
    assert "COUNT" in sql_str
    assert "ID" in sql_str

def test_select_only_not_available_on_update() -> None:
    """Test select_only() is not available on UPDATE statements."""
    query = sql.update("users").set(active=True)

    # The method shouldn't exist on Update builder, so AttributeError is expected
    with pytest.raises(AttributeError):
        query.select_only("id")  # type: ignore
