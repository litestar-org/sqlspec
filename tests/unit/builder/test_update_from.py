"""Unit tests for UPDATE ... FROM clause support and dialect validation."""

from types import SimpleNamespace

import pytest
from sqlglot import exp

from sqlspec import sql
from sqlspec.core import StatementConfig
from sqlspec.exceptions import SQLBuilderError


def test_update_from_select_builder_matrix() -> None:
    """Test UPDATE FROM with Select builder across supported dialects."""
    subquery = sql.select("id").from_("t").limit(1)
    query = sql.update("t").set(a=1).from_(subquery, alias="s").where("t.id = s.id").returning("id")

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


@pytest.mark.parametrize("dialect", ["oracle", "mysql", "mariadb", "spanner", "spangres"])
def test_update_from_raises_oracle_mysql(dialect: str) -> None:
    """Test UPDATE FROM raises SQLBuilderError on unsupported dialects."""
    subquery = sql.select("id").from_("t").limit(1)
    query = sql.update("t").set(a=1).from_(subquery, alias="s").where("t.id = s.id")

    with pytest.raises(SQLBuilderError, match=r"(?i)MERGE|join"):
        query.build(dialect=dialect)


def test_update_from_string_table() -> None:
    """Test UPDATE FROM with string table name and alias."""
    query = sql.update("t").set(a=1).from_("source_table", alias="s").where("t.id = s.id")
    stmt = query.build(dialect="postgres")
    assert 'FROM "source_table" AS "s"' in stmt.sql or 'FROM "source_table" AS s' in stmt.sql
    assert stmt.parameters["a"] == 1


def test_update_from_expression_table() -> None:
    """Test UPDATE FROM with sqlglot expression."""
    table_expr = exp.to_table("source_table")
    query = sql.update("t").set(a=1).from_(table_expr, alias="s").where("t.id = s.id")
    stmt = query.build(dialect="postgres")
    assert 'FROM "source_table" AS "s"' in stmt.sql or 'FROM "source_table" AS s' in stmt.sql


def test_update_from_parameter_merge_and_collision() -> None:
    """Test parameter collision resolution when subquery shares parameter names with main query."""
    subquery = sql.select("id").from_("source").where_eq("status", "pending")
    query = sql.update("t").set(status="active").from_(subquery, alias="s").where("t.id = s.id")
    stmt = query.build(dialect="postgres")
    assert len(stmt.parameters) >= 2
    param_values = list(stmt.parameters.values())
    assert "pending" in param_values
    assert "active" in param_values


def test_update_from_multiple_sources() -> None:
    """Test adding multiple FROM sources creates join clauses."""
    s1 = sql.select("id").from_("src1")
    s2 = sql.select("id").from_("src2")
    query = sql.update("t").set(a=1).from_(s1, alias="s1").from_(s2, alias="s2").where("t.id = s1.id")
    stmt = query.build(dialect="postgres")
    assert 'AS "s1"' in stmt.sql and 'AS "s2"' in stmt.sql


@pytest.mark.parametrize("dialect", ["mysql", "oracle", "mariadb", "spangres"])
def test_update_from_statement_config_rejects_unsupported_dialect(dialect: str) -> None:
    query = sql.update("t").set(a=1).from_("source")
    with pytest.raises(SQLBuilderError, match="MERGE"):
        query.to_statement(StatementConfig(dialect=dialect))


def test_update_from_select_expression_is_parenthesized() -> None:
    source = exp.select("id").from_("source")
    query = sql.update("t").set(a=1).from_(source, alias="s")
    expression = query.get_expression()
    assert expression is not None
    assert isinstance(expression.args["from_"].this, exp.Subquery)
    assert source.args.get("alias") is None


def test_bigquery_update_from_is_supported() -> None:
    query = sql.update("t").set(a=1).from_("source", alias="s").where("t.id = s.id")
    assert "FROM" in query.build(dialect="bigquery").sql
    assert "FROM" in query.to_statement(StatementConfig(dialect="bigquery")).sql


@pytest.mark.parametrize(("major", "expected"), [(19, False), (23, True)])
def test_oracle_update_from_runtime_capability(major: int, expected: bool) -> None:
    from sqlspec.data_dictionary import VersionInfo
    from sqlspec.data_dictionary.dialects.oracle.config import ORACLE_CONFIG, resolve_oracle_feature_flag

    assert (
        resolve_oracle_feature_flag(
            ORACLE_CONFIG, VersionInfo(major, 0, 0), "supports_update_from", compatible_major=major, is_autonomous=False
        )
        is expected
    )


@pytest.mark.parametrize("getter", [False, True])
def test_update_from_expression_provider_preserves_parameters(getter: bool) -> None:
    expression = exp.select("id").from_("source").where(exp.column("id").eq(exp.Placeholder(this="id")))
    source = SimpleNamespace(parameters={"id": 7}, alias="candidate")
    if getter:
        source.get_expression = lambda: expression
    else:
        source._expression = expression
    query = sql.update("target").set(id=8).from_(source).where("target.id = candidate.id")
    built = query.build(dialect="postgres")

    assert built.parameters == {"id": 8, "candidate_id": 7}
    assert 'AS "candidate"' in built.sql
    placeholder = expression.find(exp.Placeholder)
    assert placeholder is not None
    assert placeholder.name == "id"


@pytest.mark.parametrize("expression", [None, exp.delete("source")])
def test_update_from_rejects_invalid_expression_provider(expression: exp.Expr | None) -> None:
    source = SimpleNamespace(get_expression=lambda: expression)
    with pytest.raises(SQLBuilderError, match=r"no expression|must be SELECT"):
        sql.update("target").set(id=1).from_(source)


@pytest.mark.parametrize("source_alias", [None, "original"])
def test_update_from_values_alias_preserves_columns(source_alias: str | None) -> None:
    values = sql.values([(1, "updated")], alias=source_alias, columns=["id", "name"])
    query = sql.update("target").set(name=exp.column("name", table="v")).from_(values, alias="v")
    built = query.build(dialect="postgres")

    assert 'AS "v"("id", "name")' in built.sql
    assert built.parameters == {"v_id": 1, "v_name": "updated"}
    assert values.alias_name == source_alias


def test_update_from_subquery_provider_replaces_alias() -> None:
    expression = exp.select("id").from_("source").subquery("original")
    source = SimpleNamespace(get_expression=lambda: expression)
    query = sql.update("target").set(id=1).from_(source, alias="candidate")

    assert 'AS "candidate"' in query.build(dialect="postgres").sql
    assert expression.alias == "original"
