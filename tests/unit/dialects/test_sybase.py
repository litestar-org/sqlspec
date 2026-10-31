"""Unit tests for the custom SAP Adaptive Server Enterprise (Sybase ASE) sqlglot dialect."""

import sqlglot
from sqlglot import exp, parse_one, transpile
from sqlglot.dialects.dialect import NormalizationStrategy

from sqlspec.dialects.sybase import Sybase, SybaseTokenizer

__all__ = ()


def test_sybase_dialect_registration() -> None:
    """Verify Sybase dialect is registered and resolvable by name."""
    dialect = sqlglot.Dialect.get_or_raise("sybase")
    assert dialect.__class__ is Sybase
    assert dialect.tokenizer_class is SybaseTokenizer
    assert dialect.NORMALIZATION_STRATEGY == NormalizationStrategy.CASE_SENSITIVE


def test_sybase_top_limit_without_offset() -> None:
    """Verify literal integer LIMIT without OFFSET renders unparenthesized SELECT TOP n."""
    result = transpile("SELECT id, name FROM users LIMIT 10", read="postgres", write="sybase")[0]
    assert result == "SELECT TOP 10 id, name FROM users"


def test_sybase_rows_limit_offset_rendering() -> None:
    """Verify LIMIT with OFFSET renders SAP ASE 16.0 SP03+ ROWS LIMIT n OFFSET m."""
    result = transpile("SELECT id, name FROM users ORDER BY id LIMIT 10 OFFSET 20", write="sybase")[0]
    assert result == "SELECT id, name FROM users ORDER BY id ROWS LIMIT 10 OFFSET 20"


def test_sybase_rows_offset_only_rendering() -> None:
    """Verify OFFSET without LIMIT renders ROWS OFFSET m."""
    result = transpile("SELECT id FROM users ORDER BY id OFFSET 15", write="sybase")[0]
    assert result == "SELECT id FROM users ORDER BY id ROWS OFFSET 15"


def test_sybase_parameterized_limit_renders_rows_limit() -> None:
    """Verify non-literal LIMIT (placeholder/parameter) renders ROWS LIMIT instead of TOP."""
    expr = exp.select("id").from_("users").limit(exp.Placeholder(this="limit"))
    rendered = expr.sql(dialect="sybase")
    assert rendered == "SELECT id FROM users ROWS LIMIT :limit"


def test_sybase_parse_rows_limit_offset_roundtrip() -> None:
    """Verify SAP ASE ROWS LIMIT n OFFSET m parses into canonical Limit/Offset AST nodes."""
    parsed = parse_one("SELECT id, name FROM users ORDER BY id ROWS LIMIT 10 OFFSET 20", dialect="sybase")
    assert isinstance(parsed, exp.Select)
    limit_node = parsed.args.get("limit")
    offset_node = parsed.args.get("offset")
    assert isinstance(limit_node, exp.Limit)
    assert isinstance(offset_node, exp.Offset)
    assert parsed.sql(dialect="sybase") == "SELECT id, name FROM users ORDER BY id ROWS LIMIT 10 OFFSET 20"


def test_sybase_copy_false_preserves_ast_pagination() -> None:
    """Verify expression.sql('sybase', copy=False) never mutates limit or offset in place."""
    select_limit = parse_one("SELECT id FROM users LIMIT 5", read="postgres")
    first_limit_sql = select_limit.sql(dialect="sybase", copy=False)
    second_limit_sql = select_limit.sql(dialect="sybase", copy=False)
    assert first_limit_sql == "SELECT TOP 5 id FROM users"
    assert second_limit_sql == first_limit_sql
    assert isinstance(select_limit.args.get("limit"), exp.Limit)

    select_page = parse_one("SELECT id FROM users ORDER BY id LIMIT 5 OFFSET 10")
    first_page_sql = select_page.sql(dialect="sybase", copy=False)
    second_page_sql = select_page.sql(dialect="sybase", copy=False)
    assert first_page_sql == "SELECT id FROM users ORDER BY id ROWS LIMIT 5 OFFSET 10"
    assert second_page_sql == first_page_sql
    assert isinstance(select_page.args.get("limit"), exp.Limit)
    assert isinstance(select_page.args.get("offset"), exp.Offset)


def test_sybase_set_operation_pagination() -> None:
    """Verify UNION pagination renders ROWS LIMIT n OFFSET m and preserves AST under copy=False."""
    union_expr = parse_one("SELECT 1 AS a UNION ALL SELECT 2 AS a LIMIT 5 OFFSET 2")
    first_sql = union_expr.sql(dialect="sybase", copy=False)
    second_sql = union_expr.sql(dialect="sybase", copy=False)
    assert first_sql == "SELECT 1 AS a UNION ALL SELECT 2 AS a ROWS LIMIT 5 OFFSET 2"
    assert second_sql == first_sql


def test_sybase_type_mappings() -> None:
    """Verify Sybase ASE type mappings for TEXT, IMAGE, BIGDATETIME, BIGTIME, UUID, and VARCHAR."""
    assert parse_one("CAST(x AS TEXT)", read="postgres").sql("sybase") == "CAST(x AS TEXT)"
    assert parse_one("CAST(x AS JSONB)", read="postgres").sql("sybase") == "CAST(x AS TEXT)"
    assert parse_one("CAST(x AS BLOB)", dialect="sybase").sql("sybase") == "CAST(x AS IMAGE)"
    assert parse_one("CAST(x AS BYTEA)", read="postgres").sql("sybase") == "CAST(x AS VARBINARY(16384))"
    assert parse_one("CAST(x AS TIMESTAMPTZ)", read="postgres").sql("sybase") == "CAST(x AS BIGDATETIME)"
    assert parse_one("CAST(x AS TIMESTAMP)", read="postgres").sql("sybase") == "CAST(x AS BIGDATETIME)"
    assert parse_one("CAST(x AS TIME)", read="postgres").sql("sybase") == "CAST(x AS BIGTIME)"
    assert parse_one("CAST(x AS UUID)", read="postgres").sql("sybase") == "CAST(x AS VARCHAR(36))"
    assert parse_one("CAST(x AS VARCHAR)", read="postgres").sql("sybase") == "CAST(x AS VARCHAR(16384))"
    assert parse_one("CAST(x AS VARCHAR(MAX))", read="tsql").sql("sybase") == "CAST(x AS TEXT)"
    assert parse_one("CAST(x AS NVARCHAR(MAX))", read="tsql").sql("sybase") == "CAST(x AS UNITEXT)"
    assert parse_one("CAST(x AS VARBINARY(MAX))", read="tsql").sql("sybase") == "CAST(x AS IMAGE)"


def test_sybase_columndef_identity_and_nullable_boolean() -> None:
    """Verify Sybase ASE renders bare IDENTITY and converts nullable BIT/BOOLEAN to TINYINT NULL."""
    ddl = parse_one(
        "CREATE TABLE items (id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, active BOOLEAN NOT NULL, flag BOOLEAN NULL)",
        read="postgres",
    ).sql("sybase")
    assert "id INT IDENTITY PRIMARY KEY" in ddl
    assert "IDENTITY(1, 1)" not in ddl
    assert "active BIT NOT NULL" in ddl
    assert "flag TINYINT NULL" in ddl


def test_sybase_functions_and_operators() -> None:
    """Verify CHAR_LENGTH, CHARINDEX, GETUTCDATE, and ILIKE rendering and parsing in Sybase."""
    assert transpile("SELECT LENGTH(name) FROM users", read="postgres", write="sybase")[0] == (
        "SELECT CHAR_LENGTH(name) FROM users"
    )
    assert transpile("SELECT STRPOS(name, 'abc') FROM users", read="postgres", write="sybase")[0] == (
        "SELECT CHARINDEX('abc', name) FROM users"
    )
    assert transpile("SELECT CURRENT_TIMESTAMP", read="postgres", write="sybase")[0] == "SELECT GETDATE()"
    assert transpile("SELECT GETUTCDATE()", read="tsql", write="sybase")[0] == "SELECT GETUTCDATE()"
    assert transpile("SELECT * FROM users WHERE name ILIKE '%alice%'", read="postgres", write="sybase")[0] == (
        "SELECT * FROM users WHERE LOWER(name) LIKE LOWER('%alice%')"
    )

    parsed_len = parse_one("SELECT CHAR_LENGTH(name) FROM users", dialect="sybase")
    assert parsed_len.find(exp.Length) is not None
    parsed_pos = parse_one("SELECT CHARINDEX('abc', name) FROM users", dialect="sybase")
    pos_node = parsed_pos.find(exp.StrPosition)
    assert pos_node is not None
    assert pos_node.sql(dialect="sybase") == "CHARINDEX('abc', name)"


def test_sybase_fetch_pagination_preservation() -> None:
    """Verify FETCH FIRST n ROWS ONLY and OFFSET m ROWS FETCH NEXT n are preserved and not dropped."""
    fetch_expr = parse_one("SELECT id FROM users ORDER BY id OFFSET 5 ROWS FETCH NEXT 10 ROWS ONLY", read="tsql")
    rendered = fetch_expr.sql("sybase")
    assert rendered == "SELECT id FROM users ORDER BY id ROWS LIMIT 10 OFFSET 5"
