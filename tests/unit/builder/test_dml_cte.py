"""Unit tests for CTE rendering on UPDATE and DELETE statements."""

import pytest

from sqlspec import sql
from sqlspec.exceptions import SQLBuilderError


def test_update_cte_renders_with_returning() -> None:
    """Test that CTEs attached to UPDATE statements render in SQL with RETURNING."""
    cte = sql.select("id").from_("source")
    query = sql.update("t").with_cte("c", cte).set(a=1).returning("id")

    stmt = query.build(dialect="postgres")
    assert stmt.sql.startswith("WITH")
    assert ('"c" AS (' in stmt.sql or "c AS (" in stmt.sql) and "SELECT" in stmt.sql
    assert "UPDATE" in stmt.sql
    assert "RETURNING" in stmt.sql
    assert stmt.parameters["a"] == 1


def test_update_with_alias_renders_cte() -> None:
    """Test that with_() method on UPDATE works as an alias for with_cte."""
    cte = sql.select("id").from_("source")
    query = sql.update("t").with_("c", cte).set(a=1).returning("id")

    stmt = query.build(dialect="postgres")
    assert stmt.sql.startswith("WITH")
    assert "UPDATE" in stmt.sql
    assert "RETURNING" in stmt.sql


def test_delete_cte_renders() -> None:
    """Test that CTEs attached to DELETE statements render in SQL with RETURNING."""
    cte = sql.select("id").from_("source")
    query = sql.delete("t").with_cte("c", cte).where("t.id in (select id from c)").returning("id")

    stmt = query.build(dialect="postgres")
    assert stmt.sql.startswith("WITH")
    assert ('"c" AS (' in stmt.sql or "c AS (" in stmt.sql) and "SELECT" in stmt.sql
    assert "DELETE FROM" in stmt.sql
    assert "RETURNING" in stmt.sql


def test_delete_with_alias_renders_cte() -> None:
    """Test that with_() method on DELETE works as an alias for with_cte."""
    cte = sql.select("id").from_("source")
    query = sql.delete("t").with_("c", cte).where("t.id in (select id from c)").returning("id")

    stmt = query.build(dialect="postgres")
    assert stmt.sql.startswith("WITH")
    assert "DELETE FROM" in stmt.sql
    assert "RETURNING" in stmt.sql


def test_cte_parameter_merge_and_collision() -> None:
    """Test that CTE parameters merge properly on UPDATE and DELETE with collision handling."""
    cte1 = sql.select("id").from_("x").where_eq("status", "pending")
    cte2 = sql.select("id").from_("y").where_eq("status", "archived")

    query = (
        sql
        .update("t")
        .with_cte("c1", cte1)
        .with_cte("c2", cte2)
        .set(status="active")
        .where("t.id in (select id from c1)")
    )

    stmt = query.build(dialect="postgres")
    param_values = list(stmt.parameters.values())
    assert "pending" in param_values
    assert "archived" in param_values
    assert "active" in param_values
    assert len(stmt.parameters) == 3


def test_duplicate_cte_alias_raises_error() -> None:
    """Test that registering duplicate CTE aliases on UPDATE raises SQLBuilderError."""
    cte = sql.select("id").from_("x")
    query = sql.update("t").with_cte("c", cte)

    with pytest.raises(SQLBuilderError, match=r"CTE with alias 'c' already exists"):
        query.with_cte("c", cte)


@pytest.mark.parametrize("operation", ["update", "delete", "select"])
def test_recursive_cte_flag_is_rendered(operation: str) -> None:
    query = getattr(sql, operation)("t")
    if operation == "update":
        query = query.set(a=1)
    query = query.with_cte("c", sql.select("id").from_("source"), recursive=True)
    assert query.build(dialect="postgres").sql.startswith("WITH RECURSIVE")
