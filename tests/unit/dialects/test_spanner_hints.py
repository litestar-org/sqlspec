"""Unit tests for Cloud Spanner GoogleSQL hints."""

from sqlglot import exp, parse_one


def test_single_statement_hint() -> None:
    """Verify single statement-level hint preceding SELECT."""
    sql = "@{LOCK_SCANNED_RANGES=exclusive} SELECT * FROM Singers"
    parsed = parse_one(sql, dialect="spanner")
    assert not isinstance(parsed, exp.Command)
    hint = parsed.args.get("hint")
    assert hint is not None
    assert isinstance(hint, exp.Hint)
    rendered = parsed.sql(dialect="spanner")
    assert "@{LOCK_SCANNED_RANGES=exclusive} SELECT * FROM Singers" in rendered


def test_multiple_statement_hints() -> None:
    """Verify multiple statement hints in @{...} preceding SELECT."""
    sql = "@{LOCK_SCANNED_RANGES=exclusive, OPTIMIZER_VERSION=6} SELECT * FROM Singers"
    parsed = parse_one(sql, dialect="spanner")
    hint = parsed.args.get("hint")
    assert hint is not None
    assert len(hint.expressions) == 2
    rendered = parsed.sql(dialect="spanner")
    assert "LOCK_SCANNED_RANGES=exclusive" in rendered
    assert "OPTIMIZER_VERSION=6" in rendered
    assert "SELECT * FROM Singers" in rendered


def test_table_hints() -> None:
    """Verify table-level hints following table and join names."""
    sql = (
        "SELECT * FROM Albums @{FORCE_INDEX=AlbumsBySinger} "
        "JOIN Singers @{JOIN_METHOD=HASH_JOIN} ON Albums.SingerId = Singers.Id"
    )
    parsed = parse_one(sql, dialect="spanner")
    table = parsed.find(exp.Table)
    assert table is not None
    assert table.args.get("hints") is not None
    rendered = parsed.sql(dialect="spanner")
    assert "Albums @{FORCE_INDEX=AlbumsBySinger}" in rendered
    assert "Singers @{JOIN_METHOD=HASH_JOIN}" in rendered


def test_combined_statement_and_table_hints() -> None:
    """Verify query containing both statement and table hints."""
    sql = "@{LOCK_SCANNED_RANGES=exclusive} SELECT * FROM Albums @{FORCE_INDEX=AlbumsBySinger}"
    parsed = parse_one(sql, dialect="spanner")
    assert parsed.args.get("hint") is not None
    table = parsed.find(exp.Table)
    assert table is not None
    assert table.args.get("hints") is not None
    rendered = parsed.sql(dialect="spanner")
    assert "@{LOCK_SCANNED_RANGES=exclusive}" in rendered
    assert "Albums @{FORCE_INDEX=AlbumsBySinger}" in rendered


def test_regular_parameters_preserved() -> None:
    """Verify query parameters like @param are not affected by hint parsing."""
    sql = "SELECT * FROM Singers WHERE Id = @singer_id"
    parsed = parse_one(sql, dialect="spanner")
    param = parsed.find(exp.Parameter)
    assert param is not None
    rendered = parsed.sql(dialect="spanner")
    assert "@singer_id" in rendered
