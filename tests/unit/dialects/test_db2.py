"""Unit tests for the custom IBM Db2 sqlglot dialect."""

import sqlglot
from sqlglot import exp, parse_one, transpile

from sqlspec.dialects.db2 import DB2, DB2Tokenizer


def test_db2_dialect_registration() -> None:
    """Verify Db2 dialect is registered and resolvable by name."""
    dialect = sqlglot.Dialect.get_or_raise("db2")
    assert dialect.__class__ is DB2
    assert dialect.tokenizer_class is DB2Tokenizer
    assert dialect.generate(parse_one("SELECT STRPOS(a, 'b')", read="postgres")) == (
        "SELECT POSSTR(a, 'b') FROM SYSIBM.SYSDUMMY1"
    )


def test_select_without_from_adds_sysibm_dummy() -> None:
    """Verify SELECT 1 transpiles to include FROM SYSIBM.SYSDUMMY1."""
    result = transpile("SELECT 1", read="postgres", write="db2")[0]
    assert result == "SELECT 1 FROM SYSIBM.SYSDUMMY1"


def test_select_with_from_preserves_table() -> None:
    """Verify SELECT col FROM tbl retains table source without dummy table."""
    result = transpile("SELECT col FROM tbl", read="postgres", write="db2")[0]
    assert "SYSDUMMY1" not in result
    assert result == "SELECT col FROM tbl"


def test_limit_only_generates_fetch_first() -> None:
    """Verify SELECT ... LIMIT 10 transpiles to FETCH FIRST 10 ROWS ONLY."""
    result = transpile("SELECT * FROM tbl LIMIT 10", read="postgres", write="db2")[0]
    assert result == "SELECT * FROM tbl FETCH FIRST 10 ROWS ONLY"


def test_limit_and_offset_generates_offset_fetch_next() -> None:
    """Verify SELECT ... LIMIT 10 OFFSET 5 transpiles to OFFSET 5 ROWS FETCH NEXT 10 ROWS ONLY."""
    result = transpile("SELECT * FROM tbl LIMIT 10 OFFSET 5", read="postgres", write="db2")[0]
    assert result == "SELECT * FROM tbl OFFSET 5 ROWS FETCH NEXT 10 ROWS ONLY"


def test_posstr_transpilation_from_strpos() -> None:
    """Verify STRPOS, INSTR, and POSITION transpile to POSSTR(haystack, needle)."""
    result = transpile("SELECT STRPOS('haystack', 'needle')", read="postgres", write="db2")[0]
    assert "POSSTR('haystack', 'needle')" in result
    assert "FROM SYSIBM.SYSDUMMY1" in result


def test_posstr_transpilation_from_position() -> None:
    """Verify ANSI POSITION(needle IN haystack) transpiles to POSSTR(haystack, needle)."""
    result = transpile("SELECT POSITION('needle' IN 'haystack')", read="postgres", write="db2")[0]
    assert "POSSTR('haystack', 'needle')" in result


def test_posstr_db2_roundtrip() -> None:
    """Verify POSSTR parses and roundtrips in Db2 dialect."""
    parsed = parse_one("SELECT POSSTR('haystack', 'needle') FROM SYSIBM.SYSDUMMY1", dialect="db2")
    rendered = parsed.sql(dialect="db2")
    assert "POSSTR('haystack', 'needle')" in rendered


def test_date_add_day_transpilation() -> None:
    """Verify DATEADD / interval additions transpile to Db2 labeled duration syntax."""
    result = transpile("SELECT DATEADD(day, 1, created_at) FROM orders", read="postgres", write="db2")[0]
    assert "created_at + 1 DAY" in result


def test_date_sub_month_transpilation() -> None:
    """Verify DATEADD negative or interval subtraction transpiles to Db2 labeled duration."""
    parsed = exp.DateSub(this=exp.column("created_at"), expression=exp.Literal.number(3), unit=exp.var("MONTH"))
    rendered = parsed.sql(dialect="db2")
    assert rendered == "created_at - 3 MONTH"


def test_varchar_format_transpilation() -> None:
    """Verify TO_CHAR / DATE_FORMAT transpiles to VARCHAR_FORMAT."""
    result = transpile("SELECT TO_CHAR(created_at, 'YYYY-MM-DD') FROM orders", read="oracle", write="db2")[0]
    assert "VARCHAR_FORMAT(created_at, 'YYYY-MM-DD')" in result


def test_type_mappings() -> None:
    """Verify Db2 dialect type mappings for BOOLEAN, CLOB, BLOB."""
    bool_sql = parse_one("CAST(x AS BOOLEAN)", dialect="postgres").sql(dialect="db2")
    assert "CAST(x AS BOOLEAN)" in bool_sql

    text_sql = parse_one("CAST(x AS TEXT)", dialect="postgres").sql(dialect="db2")
    assert "CAST(x AS CLOB)" in text_sql

    blob_sql = parse_one("CAST(x AS BYTEA)", dialect="postgres").sql(dialect="db2")
    assert "CAST(x AS BLOB)" in blob_sql


def test_parameter_binding_qmark() -> None:
    """Verify parameters render as positional question mark placeholders."""
    expr = exp.Select(expressions=[exp.Parameter()]).from_("tbl")
    rendered = expr.sql(dialect="db2")
    assert rendered == "SELECT ? FROM tbl"


def test_quoted_identifier_preservation() -> None:
    """Verify quoted identifiers preserve exact case with double quotes."""
    result = transpile('SELECT "myCol" FROM "mySchema"."myTable"', read="postgres", write="db2")[0]
    assert '"myCol"' in result
    assert '"mySchema"."myTable"' in result


def test_string_concatenation() -> None:
    """Verify string concatenation renders with double pipe operator."""
    result = transpile("SELECT 'a' || 'b'", read="postgres", write="db2")[0]
    assert "'a' || 'b'" in result
    assert "FROM SYSIBM.SYSDUMMY1" in result


def test_select_current_date_adds_dummy() -> None:
    """Verify SELECT CURRENT_DATE adds dummy table."""
    result = transpile("SELECT CURRENT_DATE", read="postgres", write="db2")[0]
    assert "FROM SYSIBM.SYSDUMMY1" in result


def test_concat_multi_arg() -> None:
    """Verify multi-argument CONCAT transpiles to double-pipe concatenation."""
    result = transpile("SELECT CONCAT('a', 'b', 'c')", read="postgres", write="db2")[0]
    assert "'a' || 'b' || 'c'" in result


def test_mod_operator() -> None:
    """Verify modulo operator % transpiles to MOD(a, b)."""
    result = transpile("SELECT a % b FROM tbl", read="postgres", write="db2")[0]
    assert "MOD(a, b)" in result


def test_ilike_operator() -> None:
    """Verify ILIKE operator transpiles to LOWER(a) LIKE LOWER(b)."""
    result = transpile("SELECT a ILIKE b FROM tbl", read="postgres", write="db2")[0]
    assert "LOWER(a) LIKE LOWER(b)" in result


def test_dateadd_negative_interval() -> None:
    """Verify negative intervals transpile to subtraction without double signs."""
    result = transpile("SELECT DATEADD(day, -5, x)", read="tsql", write="db2")[0]
    assert "x - 5 DAY" in result


def test_dbclob_tokenization() -> None:
    """Verify DBCLOB tokenizes as text."""
    tokens = DB2Tokenizer().tokenize("DBCLOB")
    assert tokens[0].token_type == sqlglot.TokenType.TEXT


def test_parse_into_normalizes_posstr() -> None:
    """Verify parsing into a target type rewrites POSSTR into a string position node."""
    parsed = parse_one("SELECT POSSTR(a, 'b') FROM t", read="db2", into=exp.Select)
    assert parsed.find(exp.StrPosition) is not None
    assert parsed.sql(dialect="postgres") == "SELECT POSITION('b' IN a) FROM t"


def test_parameterized_and_unmapped_types() -> None:
    """Verify type parameters are kept and types without a Db2 mapping render unchanged."""
    result = transpile("SELECT CAST(a AS VARCHAR(10)), CAST(b AS CHAR(3)) FROM t", read="postgres", write="db2")[0]
    assert result == "SELECT CAST(a AS VARCHAR(10)), CAST(b AS CHAR(3)) FROM t"


def test_interval_renders_labeled_duration() -> None:
    """Verify numeric and column intervals render as Db2 labeled durations."""
    result = transpile("SELECT a + INTERVAL 1 DAY, b - INTERVAL c HOUR FROM t", read="mysql", write="db2")[0]
    assert result == "SELECT a + 1 DAY, b - c HOUR FROM t"
