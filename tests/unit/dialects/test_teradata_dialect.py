"""Teradata generation contracts."""

import pytest
from sqlglot import parse_one, transpile


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        ("SELECT id FROM users LIMIT 5", "SELECT TOP 5 id FROM users"),
        (
            "SELECT id FROM users ORDER BY id LIMIT 10 OFFSET 20",
            "SELECT id FROM users QUALIFY ROW_NUMBER() OVER (ORDER BY id) BETWEEN 21 AND 30",
        ),
        ("SELECT id FROM users OFFSET 2", "SELECT id FROM users QUALIFY ROW_NUMBER() OVER (ORDER BY 1) >= 3"),
        ("SELECT CAST(x AS BOOLEAN)", "SELECT CAST(x AS BYTEINT)"),
        ("SELECT CAST(x AS VARCHAR)", "SELECT CAST(x AS VARCHAR(32000))"),
    ],
)
def test_teradata_generation(source: str, expected: str) -> None:
    from sqlspec.dialects.teradata import Vantage

    assert transpile(source, write=Vantage)[0] == expected


def test_teradata_pagination_preserves_ast() -> None:
    from sqlspec.dialects.teradata import Vantage

    node = parse_one("SELECT DISTINCT id FROM users ORDER BY id LIMIT 5 OFFSET 10")
    original = node.copy()
    rendered = node.sql(dialect=Vantage)
    assert node == original
    assert (
        rendered
        == "SELECT * FROM (SELECT DISTINCT id FROM users) AS _sqlspec_paged QUALIFY ROW_NUMBER() OVER (ORDER BY id) BETWEEN 11 AND 15"
    )


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        ("SELECT CAST(x AS TEXT)", "SELECT CAST(x AS CLOB)"),
        ("SELECT CAST(x AS VARBINARY)", "SELECT CAST(x AS VARBYTE(32000))"),
        ("SELECT CAST(x AS TIMESTAMP(6) WITH TIME ZONE)", "SELECT CAST(x AS TIMESTAMP(6) WITH TIME ZONE)"),
        (
            "SELECT id FROM users LIMIT :limit OFFSET :offset",
            "SELECT id FROM users QUALIFY ROW_NUMBER() OVER (ORDER BY 1) BETWEEN :offset + 1 AND :offset + :limit",
        ),
        (
            "SELECT id FROM a UNION ALL SELECT id FROM b LIMIT 2 OFFSET 3",
            "SELECT * FROM (SELECT id FROM a UNION ALL SELECT id FROM b) AS _sqlspec_paged QUALIFY ROW_NUMBER() OVER (ORDER BY 1) BETWEEN 4 AND 5",
        ),
    ],
)
def test_teradata_types_and_set_pagination(source: str, expected: str) -> None:
    from sqlspec.dialects.teradata import Vantage

    assert transpile(source, write=Vantage)[0] == expected


def test_teradata_native_byte_types() -> None:
    from sqlspec.dialects.teradata import Vantage

    assert (
        transpile("SELECT CAST(x AS BYTEINT), CAST(y AS VARBYTE(12))", read=Vantage, write=Vantage)[0]
        == "SELECT CAST(x AS BYTEINT), CAST(y AS VARBYTE(12))"
    )


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        (
            "SELECT id FROM a UNION ALL SELECT id FROM b LIMIT 2",
            "SELECT TOP 2 * FROM (SELECT id FROM a UNION ALL SELECT id FROM b) AS _sqlspec_paged",
        ),
        (
            "SELECT id FROM a UNION ALL SELECT id FROM b ORDER BY id LIMIT 2",
            "SELECT TOP 2 * FROM (SELECT id FROM a UNION ALL SELECT id FROM b) AS _sqlspec_paged ORDER BY id",
        ),
        (
            "SELECT t1.id FROM t1 UNION SELECT t2.id FROM t2 ORDER BY t1.id LIMIT 5",
            "SELECT TOP 5 * FROM (SELECT t1.id FROM t1 UNION SELECT t2.id FROM t2) AS _sqlspec_paged ORDER BY id",
        ),
        (
            "SELECT db1.t1.id FROM db1.t1 INTERSECT SELECT db1.t2.id FROM db1.t2 ORDER BY db1.t1.id LIMIT 5",
            "SELECT TOP 5 * FROM (SELECT db1.t1.id FROM db1.t1 INTERSECT SELECT db1.t2.id FROM db1.t2) AS _sqlspec_paged ORDER BY id",
        ),
        (
            "SELECT a FROM t1 UNION SELECT a FROM t2 OFFSET 5 ROWS FETCH FIRST ROW ONLY",
            "SELECT * FROM (SELECT a FROM t1 UNION SELECT a FROM t2) AS _sqlspec_paged QUALIFY ROW_NUMBER() OVER (ORDER BY 1) BETWEEN 6 AND 6",
        ),
        (
            "SELECT a FROM t1 UNION SELECT a FROM t2 OFFSET :offset ROWS FETCH FIRST ROW ONLY",
            "SELECT * FROM (SELECT a FROM t1 UNION SELECT a FROM t2) AS _sqlspec_paged QUALIFY ROW_NUMBER() OVER (ORDER BY 1) BETWEEN :offset + 1 AND :offset + 1",
        ),
        ("SELECT id FROM a UNION ALL SELECT id FROM b", "SELECT id FROM a UNION ALL SELECT id FROM b"),
        ("SELECT id FROM a FETCH FIRST 2 ROWS ONLY", "SELECT TOP 2 id FROM a"),
        ("SELECT id FROM a FETCH FIRST 50 PERCENT ROWS ONLY", "SELECT TOP 50 PERCENT id FROM a"),
        ("SELECT id FROM a ORDER BY id FETCH FIRST 5 ROWS WITH TIES", "SELECT TOP 5 WITH TIES id FROM a ORDER BY id"),
        (
            "SELECT id FROM a ORDER BY id OFFSET 2 ROWS FETCH FIRST 50 PERCENT ROWS ONLY",
            "SELECT id FROM a QUALIFY ROW_NUMBER() OVER (ORDER BY id) BETWEEN 3 AND 2 + CEIL(CAST(COUNT(*) OVER () * 50 AS DOUBLE PRECISION) / 100.0)",
        ),
        (
            "SELECT id FROM a ORDER BY id OFFSET 2 ROWS FETCH FIRST 5 ROWS WITH TIES",
            "SELECT id FROM a QUALIFY ROW_NUMBER() OVER (ORDER BY id) >= 3 AND RANK() OVER (ORDER BY id) <= 7",
        ),
        (
            "SELECT id FROM a ORDER BY id OFFSET 2 ROWS FETCH FIRST 50 PERCENT ROWS WITH TIES",
            "SELECT id FROM a QUALIFY ROW_NUMBER() OVER (ORDER BY id) >= 3 AND RANK() OVER (ORDER BY id) <= 2 + CEIL(CAST(COUNT(*) OVER () * 50 AS DOUBLE PRECISION) / 100.0)",
        ),
        ("SELECT id FROM a ORDER BY id OFFSET 2 ROWS FETCH FIRST 0 ROWS WITH TIES", "SELECT id FROM a QUALIFY 1 = 0"),
        (
            "SELECT id FROM a ORDER BY id OFFSET :off ROWS FETCH FIRST :lim ROWS WITH TIES",
            "SELECT id FROM a QUALIFY :lim > 0 AND ROW_NUMBER() OVER (ORDER BY id) >= :off + 1 AND RANK() OVER (ORDER BY id) <= :off + :lim",
        ),
        (
            "SELECT id FROM a UNION ALL SELECT id FROM b ORDER BY id OFFSET 2 ROWS FETCH FIRST 5 ROWS WITH TIES",
            "SELECT * FROM (SELECT id FROM a UNION ALL SELECT id FROM b) AS _sqlspec_paged QUALIFY ROW_NUMBER() OVER (ORDER BY id) >= 3 AND RANK() OVER (ORDER BY id) <= 7",
        ),
        (
            "SELECT DISTINCT id FROM a ORDER BY id OFFSET 2 ROWS FETCH FIRST 50 PERCENT ROWS ONLY",
            "SELECT * FROM (SELECT DISTINCT id FROM a) AS _sqlspec_paged QUALIFY ROW_NUMBER() OVER (ORDER BY id) BETWEEN 3 AND 2 + CEIL(CAST(COUNT(*) OVER () * 50 AS DOUBLE PRECISION) / 100.0)",
        ),
    ],
)
def test_teradata_set_limits_and_fetch(source: str, expected: str) -> None:
    from sqlspec.dialects.teradata import Vantage

    node = parse_one(source)
    original = node.copy()
    assert node.sql(dialect=Vantage) == expected
    assert node == original


def test_teradata_stock_dialect_isolation() -> None:
    from sqlglot.dialects.dialect import Dialect
    from sqlglot.dialects.teradata import Teradata as StockTeradata
    from sqlglot.generators.teradata import TeradataGenerator

    from sqlspec.dialects.teradata import Vantage

    transforms_before = dict(TeradataGenerator.TRANSFORMS)
    type_mapping_before = dict(TeradataGenerator.TYPE_MAPPING)

    assert transpile("SELECT CAST(x AS BOOLEAN)", write=Vantage)[0] == "SELECT CAST(x AS BYTEINT)"
    assert (
        transpile("SELECT id FROM users ORDER BY id LIMIT 10 OFFSET 20", write=Vantage)[0]
        == "SELECT id FROM users QUALIFY ROW_NUMBER() OVER (ORDER BY id) BETWEEN 21 AND 30"
    )

    assert TeradataGenerator.TRANSFORMS == transforms_before
    assert TeradataGenerator.TYPE_MAPPING == type_mapping_before
    assert Dialect.get("teradata") is StockTeradata
    assert Dialect.get("vantage") is Vantage
    assert transpile("SELECT CAST(x AS BOOLEAN)", write=StockTeradata)[0] == "SELECT CAST(x AS BOOLEAN)"
    assert transpile("SELECT CAST(x AS BOOLEAN)", write="teradata")[0] == "SELECT CAST(x AS BOOLEAN)"
    assert transpile("SELECT CAST(x AS BOOLEAN)", write="postgres")[0] == "SELECT CAST(x AS BOOLEAN)"
    assert (
        transpile("SELECT id FROM users ORDER BY id LIMIT 10 OFFSET 20", write="postgres")[0]
        == "SELECT id FROM users ORDER BY id NULLS FIRST LIMIT 10 OFFSET 20"
    )
    assert transpile("SELECT id FROM users LIMIT 5", write="tsql")[0] == "SELECT TOP 5 id FROM users"
