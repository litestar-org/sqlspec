"""Render regressions for DML builders that silently dropped clauses.

UPDATE ... FROM lost its FROM clause while keeping the correlated WHERE,
UPDATE ... JOIN lost the join and its condition, DELETE with a CTE lost the
DELETE statement entirely, and every builder CTE rendered with the alias and
query swapped. Each came from passing arguments sqlglot's generator ignores
or from calling ``with_`` with reversed arguments.
"""

import re

from sqlspec import sql


def test_update_from_renders_from_clause() -> None:
    stmt = sql.update("t").set("a", 1).from_("other").where("t.id = other.id").build(dialect="postgres")
    assert "FROM" in stmt.sql
    assert "other" in stmt.sql


def test_update_from_multiple_tables_renders_all() -> None:
    stmt = sql.update("t").set("a", 1).from_("x").from_("y").where("t.id = x.id").build(dialect="postgres")
    assert "FROM" in stmt.sql
    assert "x" in stmt.sql
    assert "y" in stmt.sql


def test_update_join_renders_join_and_condition() -> None:
    stmt = sql.update("t").join("j", on="t.id = j.id").set("a", 1).build(dialect="mysql")
    assert "JOIN" in stmt.sql
    assert "j" in stmt.sql.split("JOIN")[-1]


def test_delete_with_cte_renders_delete_statement() -> None:
    cte = sql.select("id").from_("src")
    stmt = sql.delete().from_("t").where_in("id", cte).with_cte("x", cte).build()
    assert "WITH" in stmt.sql
    assert "DELETE FROM" in stmt.sql


def test_cte_renders_alias_before_query() -> None:
    cte = sql.select("id").from_("users")
    builder = sql.select("*").from_("active").with_cte("active", cte)
    builder.enable_optimization = False
    stmt = builder.build()
    assert re.search(r'WITH\s+"?active"?\s+AS\s+\(', stmt.sql), stmt.sql


def test_update_with_cte_renders_alias_before_query() -> None:
    cte = sql.select("id").from_("users")
    builder = sql.update("t").set("a", 1).with_cte("u", cte).where("t.id = 1")
    builder.enable_optimization = False
    stmt = builder.build(dialect="postgres")
    assert re.search(r'WITH\s+"?u"?\s+AS\s+\(', stmt.sql), stmt.sql
