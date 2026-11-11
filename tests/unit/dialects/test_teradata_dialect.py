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
    from sqlspec.dialects.teradata import Teradata

    assert transpile(source, write=Teradata)[0] == expected


def test_teradata_pagination_preserves_ast() -> None:
    from sqlspec.dialects.teradata import Teradata

    node = parse_one("SELECT DISTINCT id FROM users ORDER BY id LIMIT 5 OFFSET 10")
    original = node.copy()
    rendered = node.sql(dialect=Teradata)
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
    from sqlspec.dialects.teradata import Teradata

    assert transpile(source, write=Teradata)[0] == expected


def test_teradata_native_byte_types() -> None:
    from sqlspec.dialects.teradata import Teradata

    assert (
        transpile("SELECT CAST(x AS BYTEINT), CAST(y AS VARBYTE(12))", read=Teradata, write=Teradata)[0]
        == "SELECT CAST(x AS BYTEINT), CAST(y AS VARBYTE(12))"
    )
