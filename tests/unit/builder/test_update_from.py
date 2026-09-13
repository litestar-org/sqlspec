"""Unit tests for UPDATE ... FROM clause support and dialect validation."""

import pytest
from sqlglot import exp

from sqlspec import sql
from sqlspec.exceptions import SQLBuilderError


def test_update_from_select_builder_matrix() -> None:
    """Test UPDATE FROM with Select builder across supported dialects."""
    subquery = sql.select("id").from_("t").limit(1)
    query = (
        sql.update("t")
        .set(a=1)
        .from_(subquery, alias="s")
        .where("t.id = s.id")
        .returning("id")
    )

    stmt_pg = query.build(dialect="postgres")
    assert "FROM (" in stmt_pg.sql and "SELECT" in stmt_pg.sql
    assert "AS s" in stmt_pg.sql or 'AS "s"' in stmt_pg.sql
    assert "RETURNING" in stmt_pg.sql
    assert stmt_pg.parameters["a"] == 1

    stmt_sqlite = query.build(dialect="sqlite")
    assert "FROM (" in stmt_sqlite.sql and "SELECT" in stmt_sqlite.sql
    assert "RETURNING" in stmt_sqlite.sql
    assert stmt_sqlite.parameters["a"] == 1

    stmt_duckdb = query.build(dialect="duckdb")
    assert "FROM (" in stmt_duckdb.sql and "SELECT" in stmt_duckdb.sql
    assert "RETURNING" in stmt_duckdb.sql
    assert stmt_duckdb.parameters["a"] == 1

    stmt_tsql = query.build(dialect="tsql")
    assert "FROM (" in stmt_tsql.sql and "TOP 1" in stmt_tsql.sql
    assert "OUTPUT" in stmt_tsql.sql
    assert stmt_tsql.parameters["a"] == 1


@pytest.mark.parametrize("dialect", ["oracle", "mysql", "mariadb", "spanner", "bigquery"])
def test_update_from_raises_oracle_mysql(dialect: str) -> None:
    """Test UPDATE FROM raises SQLBuilderError on unsupported dialects."""
    subquery = sql.select("id").from_("t").limit(1)
    query = (
        sql.update("t")
        .set(a=1)
        .from_(subquery, alias="s")
        .where("t.id = s.id")
    )

    with pytest.raises(SQLBuilderError, match=r"(?i)MERGE|join"):
        query.build(dialect=dialect)


def test_update_from_string_table() -> None:
    """Test UPDATE FROM with string table name and alias."""
    query = (
        sql.update("t")
        .set(a=1)
        .from_("source_table", alias="s")
        .where("t.id = s.id")
    )
    stmt = query.build(dialect="postgres")
    assert 'FROM "source_table" AS "s"' in stmt.sql or 'FROM "source_table" AS s' in stmt.sql
    assert stmt.parameters["a"] == 1


def test_update_from_expression_table() -> None:
    """Test UPDATE FROM with sqlglot expression."""
    table_expr = exp.to_table("source_table")
    query = (
        sql.update("t")
        .set(a=1)
        .from_(table_expr, alias="s")
        .where("t.id = s.id")
    )
    stmt = query.build(dialect="postgres")
    assert 'FROM "source_table" AS "s"' in stmt.sql or 'FROM "source_table" AS s' in stmt.sql


def test_update_from_parameter_merge_and_collision() -> None:
    """Test parameter collision resolution when subquery shares parameter names with main query."""
    subquery = sql.select("id").from_("source").where_eq("status", "pending")
    query = (
        sql.update("t")
        .set(status="active")
        .from_(subquery, alias="s")
        .where("t.id = s.id")
    )
    stmt = query.build(dialect="postgres")
    assert len(stmt.parameters) >= 2
    param_values = list(stmt.parameters.values())
    assert "pending" in param_values
    assert "active" in param_values


def test_update_from_multiple_sources() -> None:
    """Test adding multiple FROM sources creates join clauses."""
    s1 = sql.select("id").from_("src1")
    s2 = sql.select("id").from_("src2")
    query = (
        sql.update("t")
        .set(a=1)
        .from_(s1, alias="s1")
        .from_(s2, alias="s2")
        .where("t.id = s1.id")
    )
    stmt = query.build(dialect="postgres")
    assert "AS s1" in stmt.sql and "AS s2" in stmt.sql
