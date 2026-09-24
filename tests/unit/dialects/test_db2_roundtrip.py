"""Db2 round-trip fidelity for SQL that SQLSpec re-renders."""

import pytest
import sqlglot
from sqlglot import ErrorLevel, exp
from sqlglot.errors import UnsupportedError


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        ("SELECT CURRENT TIMESTAMP FROM SYSIBM.SYSDUMMY1", "SELECT CURRENT TIMESTAMP FROM SYSIBM.SYSDUMMY1"),
        ("SELECT CURRENT_TIMESTAMP", "SELECT CURRENT TIMESTAMP FROM SYSIBM.SYSDUMMY1"),
        ("SELECT a + 1 DAYS FROM t", "SELECT a + 1 DAYS FROM t"),
        ("CREATE TABLE t (c CLOB(1M))", "CREATE TABLE t (c CLOB(1M))"),
        ("VALUES 1", "VALUES (1)"),
        (
            "SELECT CURRENT DATE, CURRENT TIME, CURRENT SCHEMA, CURRENT USER FROM SYSIBM.SYSDUMMY1",
            "SELECT CURRENT DATE, CURRENT TIME, CURRENT SCHEMA, CURRENT USER FROM SYSIBM.SYSDUMMY1",
        ),
        (
            "SELECT CURRENT TIMEZONE, CURRENT TIME ZONE, CURRENT SERVER FROM SYSIBM.SYSDUMMY1",
            "SELECT CURRENT TIMEZONE, CURRENT TIME ZONE, CURRENT SERVER FROM SYSIBM.SYSDUMMY1",
        ),
        (
            "SELECT a + ? DAYS - :n MONTHS, b - (c + 1) HOURS, f(x) + 2 YEARS FROM t",
            "SELECT a + ? DAYS - :n MONTHS, b - (c + 1) HOURS, F(x) + 2 YEARS FROM t",
        ),
        ("SELECT t.ts + 30 SECONDS FROM t", "SELECT t.ts + 30 SECONDS FROM t"),
        ("CREATE TABLE t (b BLOB(10M), c CLOB(2G))", "CREATE TABLE t (b BLOB(10M), c CLOB(2G))"),
        ("VALUES (1, 'a'), (2, 'b')", "VALUES (1, 'a'), (2, 'b')"),
        ("VALUES CURRENT SCHEMA", "VALUES (CURRENT SCHEMA)"),
        ("SELECT col days FROM t", "SELECT col AS days FROM t"),
    ],
)
def test_db2_round_trip_is_stable(sql: str, expected: str) -> None:
    assert sqlglot.transpile(sql, read="db2", write="db2") == [expected]


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        ("SELECT CURRENT_TIMESTAMP", "SELECT CURRENT TIMESTAMP FROM SYSIBM.SYSDUMMY1"),
        ("SELECT a + INTERVAL '1 day' FROM t", "SELECT a + 1 DAY FROM t"),
    ],
)
def test_transpile_interval_to_db2(source: str, expected: str) -> None:
    assert sqlglot.transpile(source, read="postgres", write="db2") == [expected]


def test_non_numeric_interval_literal_is_reported() -> None:
    with pytest.raises(UnsupportedError):
        sqlglot.transpile(
            "SELECT a + INTERVAL '1 day 2 hours' FROM t",
            read="postgres",
            write="db2",
            unsupported_level=ErrorLevel.RAISE,
        )


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        ("SELECT a + @x HOURS, a + b.c DAYS, a - DAY(x) FROM t", "SELECT a + ? HOURS, a + b.c DAYS, a - DAY(x) FROM t"),
        ("VALUES COALESCE(1, 2), 3", "VALUES (COALESCE(1, 2)), (3)"),
    ],
)
def test_db2_token_normalization_edges(sql: str, expected: str) -> None:
    assert sqlglot.transpile(sql, read="db2", write="db2") == [expected]


def test_interval_amounts_render_as_db2_operands() -> None:
    compound = exp.Interval(this=exp.Add(this=exp.column("x"), expression=exp.Literal.number(1)), unit=exp.var("DAY"))
    numeric = exp.Interval(this=exp.Literal.number(2), unit=exp.var("DAY"))
    assert compound.sql(dialect="db2") == "(x + 1) DAY"
    assert numeric.sql(dialect="db2") == "2 DAY"
