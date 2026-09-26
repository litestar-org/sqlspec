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


@pytest.mark.parametrize(
    ("source", "read", "expected"),
    [
        (
            "SELECT LISTAGG(a, ',') WITHIN GROUP (ORDER BY a) FROM t",
            "db2",
            "SELECT LISTAGG(a, ',') WITHIN GROUP (ORDER BY a) FROM t",
        ),
        (
            "CREATE TABLE t (j JSON, v VARCHAR, b VARBINARY(10), z TIMESTAMPTZ)",
            "postgres",
            "CREATE TABLE t (j CLOB, v VARCHAR(32672), b VARBINARY(10), z TIMESTAMP)",
        ),
        (
            "CREATE TABLE t (u UUID, n NVARCHAR, vb VARBINARY, jb JSONB, ti TINYINT, dt DATETIME)",
            "mysql",
            "CREATE TABLE t (u VARCHAR(36), n VARGRAPHIC(16336), vb VARBINARY(32672), jb CLOB, ti SMALLINT, dt TIMESTAMP)",
        ),
        ("CREATE TABLE t (d DBCLOB(10))", "db2", "CREATE TABLE t (d DBCLOB(10))"),
        ("CREATE TABLE t (d DBCLOB(1M))", "db2", "CREATE TABLE t (d DBCLOB(1M))"),
        (
            "CREATE TABLE t (g GRAPHIC(5), vg VARGRAPHIC(10), x XML, df DECFLOAT(34), bn BINARY(4))",
            "db2",
            "CREATE TABLE t (g GRAPHIC(5), vg VARGRAPHIC(10), x XML, df DECFLOAT(34), bn BINARY(4))",
        ),
        ("SELECT DATE_ADD(a, INTERVAL '1' DAY)", "mysql", "SELECT a + 1 DAY FROM SYSIBM.SYSDUMMY1"),
        ("SELECT DATE_ADD(a, INTERVAL x + 1 DAY) FROM t", "mysql", "SELECT a + (x + 1) DAY FROM t"),
    ],
)
def test_db2_types_functions_and_date_arithmetic(source: str, read: str, expected: str) -> None:
    assert sqlglot.transpile(source, read=read, write="db2") == [expected]


def test_timestamptz_is_reported() -> None:
    with pytest.raises(UnsupportedError):
        sqlglot.transpile(
            "CREATE TABLE t (z TIMESTAMPTZ)", read="postgres", write="db2", unsupported_level=ErrorLevel.RAISE
        )


def test_time_mapping_to_db2() -> None:
    assert sqlglot.transpile("SELECT STRFTIME(ts, '%Y-%m-%d %H:%M:%S.%f') FROM t", read="duckdb", write="db2") == [
        "SELECT VARCHAR_FORMAT(ts, 'YYYY-MM-DD HH24:MI:SS.FF6') FROM t"
    ]


def test_time_mapping_from_db2() -> None:
    assert sqlglot.transpile("SELECT VARCHAR_FORMAT(ts, 'HH12:MI NNNNNN') FROM t", read="db2", write="duckdb") == [
        "SELECT STRFTIME(ts, '%I:%M %f') FROM t"
    ]


def test_exists_subselect_gets_dummy_table() -> None:
    assert sqlglot.transpile("SELECT 1 FROM t WHERE EXISTS (SELECT 1)", read="db2", write="db2") == [
        "SELECT 1 FROM t WHERE EXISTS(SELECT 1 FROM SYSIBM.SYSDUMMY1)"
    ]


def test_date_add_never_interpolates_literal_text() -> None:
    expression = exp.DateAdd(
        this=exp.column("a"), expression=exp.Literal.string("1; DROP TABLE t"), unit=exp.var("DAY")
    )
    assert expression.sql(dialect="db2") == "a + '1; DROP TABLE t' DAY"
    with pytest.raises(UnsupportedError):
        expression.sql(dialect="db2", unsupported_level=ErrorLevel.RAISE)


def test_date_arithmetic_sign_and_embedded_unit() -> None:
    negated = exp.DateSub(this=exp.column("a"), expression=exp.Neg(this=exp.Literal.number(2)), unit=exp.var("DAY"))
    embedded = exp.DateAdd(this=exp.column("a"), expression=exp.Literal.string("-3 months"))
    interval = exp.DateAdd(
        this=exp.column("a"),
        expression=exp.Interval(this=exp.Literal.string("4"), unit=exp.var("HOUR")),
        unit=exp.var("DAY"),
    )
    assert negated.sql(dialect="db2") == "a + 2 DAY"
    assert embedded.sql(dialect="db2") == "a - 3 MONTHS"
    assert interval.sql(dialect="db2") == "a + 4 HOUR"
