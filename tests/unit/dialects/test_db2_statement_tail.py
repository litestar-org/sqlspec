"""Db2 select-statement tail parsing, rendering, and lock translation."""

import pytest
import sqlglot
from sqlglot import ErrorLevel
from sqlglot.errors import ParseError, UnsupportedError


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM t WITH UR",
        "SELECT * FROM t WITH CS",
        "SELECT a FROM t WHERE b = 1 FOR READ ONLY WITH RS",
        "SELECT a FROM t ORDER BY a FETCH FIRST 1 ROWS ONLY WITH RS USE AND KEEP UPDATE LOCKS SKIP LOCKED DATA",
        "SELECT a FROM t FOR UPDATE OF a WITH RS SKIP LOCKED DATA",
        "SELECT a FROM t FOR UPDATE",
        "SELECT a FROM t OPTIMIZE FOR 10 ROWS WITH UR",
        "SELECT a FROM t UNION ALL SELECT a FROM u WITH UR",
    ],
)
def test_db2_statement_tail_round_trips(sql: str) -> None:
    assert sqlglot.transpile(sql, read="db2", write="db2") == [sql]


def test_db2_statement_tails_stay_with_their_statements() -> None:
    sql = "SELECT 1 FROM SYSIBM.SYSDUMMY1 WITH UR; SELECT 2 FROM SYSIBM.SYSDUMMY1 WITH CS"
    assert sqlglot.transpile(sql, read="db2", write="db2") == [
        "SELECT 1 FROM SYSIBM.SYSDUMMY1 WITH UR",
        "SELECT 2 FROM SYSIBM.SYSDUMMY1 WITH CS",
    ]


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        ("SELECT * FROM t FOR UPDATE", "SELECT * FROM t WITH RS USE AND KEEP UPDATE LOCKS"),
        (
            "SELECT * FROM t FOR UPDATE SKIP LOCKED",
            "SELECT * FROM t WITH RS USE AND KEEP UPDATE LOCKS SKIP LOCKED DATA",
        ),
        ("SELECT * FROM t FOR SHARE", "SELECT * FROM t WITH RS USE AND KEEP SHARE LOCKS"),
        (
            "SELECT * FROM t ORDER BY a LIMIT 1 FOR UPDATE SKIP LOCKED",
            "SELECT * FROM t ORDER BY a FETCH FIRST 1 ROWS ONLY WITH RS USE AND KEEP UPDATE LOCKS SKIP LOCKED DATA",
        ),
    ],
)
def test_foreign_locks_render_db2_isolation_form(source: str, expected: str) -> None:
    assert sqlglot.transpile(source, read="postgres", write="db2") == [expected]


@pytest.mark.parametrize(
    "source",
    [
        "SELECT * FROM t FOR UPDATE NOWAIT",
        "SELECT * FROM t FOR UPDATE OF t",
        "SELECT * FROM t FOR UPDATE OF t FOR SHARE OF u",
    ],
)
def test_inexpressible_foreign_lock_options_are_reported(source: str) -> None:
    with pytest.raises(UnsupportedError):
        sqlglot.transpile(source, read="postgres", write="db2", unsupported_level=ErrorLevel.RAISE)


def test_statement_tail_on_non_query_is_rejected() -> None:
    with pytest.raises(ParseError):
        sqlglot.transpile("INSERT INTO t SELECT a FROM u WITH UR", read="db2", write="db2")


def test_statement_tails_align_across_empty_and_comment_statements() -> None:
    sql = "SELECT 1 FROM x WITH UR;; /* note */; SELECT 2 FROM y WITH RR USE AND KEEP EXCLUSIVE LOCKS"
    rendered = sqlglot.transpile(sql, read="db2", write="db2")
    assert rendered[0] == "SELECT 1 FROM x WITH UR"
    assert rendered[-1] == "SELECT 2 FROM y WITH RR USE AND KEEP EXCLUSIVE LOCKS"


def test_fetch_only_and_single_row_optimize_render_canonical_spelling() -> None:
    assert sqlglot.transpile("SELECT a FROM t FOR FETCH ONLY OPTIMIZE FOR 1 ROW", read="db2", write="db2") == [
        "SELECT a FROM t FOR READ ONLY OPTIMIZE FOR 1 ROWS"
    ]
