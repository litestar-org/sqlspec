"""Unit tests for preserving declared T-SQL local variables during parameter extraction."""

import pytest

from sqlspec import SQL
from sqlspec.adapters.mssql_python.core import default_statement_config as mssql_python_config
from sqlspec.adapters.pymssql.core import default_statement_config as pymssql_config
from sqlspec.adapters.sybase.core import default_statement_config as sybase_config
from sqlspec.core import ParameterValidator


@pytest.mark.parametrize("dialect", [None, "tsql", "mssql", "sqlserver", "sybase", "ase", "sqlanywhere"])
@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        ("DECLARE @x int; SET @x = 1; SELECT @x;", []),
        ("DECLARE @x AS INT = 1; SELECT @x;", []),
        ("DECLARE @x int\nSET @x = 1\nSELECT @x", []),
        ("DECLARE @X int; SET @x = 1; SELECT @X;", []),
        ("DECLARE @a int, @b varchar(50), @c decimal(10, 2) = 1.5; SELECT @a, @b, @c;", []),
        (
            "DECLARE @t TABLE (id int, name nvarchar(100)); INSERT INTO @t (id, name) VALUES (:id, :name); SELECT * FROM @t;",
            [":id", ":name"],
        ),
        ("DECLARE @x int = @init_val; SET @x = @x + :delta; SELECT @x;", ["@init_val", ":delta"]),
        ("DECLARE @x int = @p1, @y int = @x + @p2; SELECT @x, @y;", ["@p1", "@p2"]),
        ("DECLARE @x int = COALESCE(@p1, @p2), @y int = 2; SELECT @x, @y;", ["@p1", "@p2"]),
        ("DECLARE @x int = CASE WHEN @cond = 1 THEN @a ELSE @b END, @y int = 2; SELECT @x, @y;", ["@cond", "@a", "@b"]),
        ("DECLARE @x int = (SELECT col FROM t WHERE id = @id); SELECT @x;", ["@id"]),
        ("DECLARE @x int\nSELECT @a, @b", ["@a", "@b"]),
        ("IF @flag = 1 BEGIN DECLARE @x int; SET @x = 1; SELECT @x; END", ["@flag"]),
        ("-- DECLARE @x int\nSELECT @x;", ["@x"]),
        ("/* DECLARE @x int */ SELECT @x;", ["@x"]),
        ("SELECT 'DECLARE @x int', @x;", ["@x"]),
        ('SELECT "DECLARE @x int", @x;', ["@x"]),
        ("SELECT @@ROWCOUNT, @x;", ["@x"]),
        ("SELECT t.declare, @a, @b FROM t;", ["@a", "@b"]),
        ("SELECT 1 AS declare, @a FROM t;", ["@a"]),
        ("DECLARE cur CURSOR FOR SELECT @a, @b;", ["@a", "@b"]),
        ("DECLARE @x int = t.select, @y int; SELECT @x, @y;", []),
    ],
)
def test_extract_parameters_preserves_declared_tsql_local_variables(
    dialect: str | None, sql: str, expected: list[str]
) -> None:
    validator = ParameterValidator()
    assert [info.placeholder_text for info in validator.extract_parameters(sql, dialect=dialect)] == expected


@pytest.mark.parametrize("dialect", ["tsql", "mssql", "sqlserver", "sybase", "ase", "sqlanywhere"])
def test_extract_parameters_ignores_declare_inside_bracket_identifiers(dialect: str) -> None:
    validator = ParameterValidator()
    assert [
        info.placeholder_text for info in validator.extract_parameters("SELECT [DECLARE @x int], @x;", dialect=dialect)
    ] == ["@x"]


def test_extract_parameters_does_not_skip_declare_in_non_tsql_dialects() -> None:
    validator = ParameterValidator()
    sql = "DECLARE @x int; SELECT @x;"
    assert [info.placeholder_text for info in validator.extract_parameters(sql, dialect="sqlite")] == ["@x", "@x"]
    assert [info.placeholder_text for info in validator.extract_parameters(sql, dialect="mysql")] == ["@x", "@x"]


def test_compile_preserves_declared_tsql_local_variables() -> None:
    sql = "DECLARE @x int; SET @x = 1; SELECT @x;"
    for config in (mssql_python_config, pymssql_config, sybase_config):
        compiled_sql, compiled_params = SQL(sql, statement_config=config).compile()
        assert compiled_sql == sql
        assert not compiled_params

        script_sql, script_params = SQL(sql, statement_config=config).as_script().compile()
        assert script_sql == sql
        assert not script_params

    default_sql, default_params = SQL(sql).compile()
    assert default_sql == sql
    assert not default_params


def test_compile_binds_external_params_while_preserving_declared_local_variables() -> None:
    sql = "DECLARE @x int; SET @x = :val; SELECT @x;"
    compiled_sql, compiled_params = SQL(sql, {"val": 42}, statement_config=mssql_python_config).compile()
    assert compiled_sql == "DECLARE @x int; SET @x = ?; SELECT @x;"
    assert tuple(compiled_params) == (42,)

    pymssql_sql, pymssql_params = SQL(sql, {"val": 42}, statement_config=pymssql_config).compile()
    assert pymssql_sql == "DECLARE @x int; SET @x = %s; SELECT @x;"
    assert tuple(pymssql_params) == (42,)

    script_sql, script_params = SQL(sql, {"val": 42}, statement_config=mssql_python_config).as_script().compile()
    assert script_sql == "DECLARE @x int; SET @x = 42; SELECT @x;"
    assert script_params is None
