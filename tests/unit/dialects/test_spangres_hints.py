"""Unit tests for Cloud Spanner PostgreSQL-interface (Spangres) comment-based hints and transpilation."""

from sqlglot import exp, parse_one


def test_spangres_statement_hint() -> None:
    """Verify /*@ ... */ statement hint parsing and generation in Spangres."""
    sql = "/*@ LOCK_SCANNED_RANGES=exclusive */ SELECT * FROM albums"
    parsed = parse_one(sql, dialect="spangres")
    assert not isinstance(parsed, exp.Command)
    hint = parsed.args.get("hint")
    assert hint is not None
    assert isinstance(hint, exp.Hint)
    rendered = parsed.sql(dialect="spangres")
    assert "/*@ LOCK_SCANNED_RANGES=exclusive */ SELECT * FROM albums" in rendered


def test_spangres_table_hint() -> None:
    """Verify /*@ ... */ table hint parsing and generation in Spangres."""
    sql = "SELECT * FROM albums /*@ FORCE_INDEX=albums_idx */"
    parsed = parse_one(sql, dialect="spangres")
    table = parsed.find(exp.Table)
    assert table is not None
    assert table.args.get("hints") is not None
    rendered = parsed.sql(dialect="spangres")
    assert "albums /*@ FORCE_INDEX=albums_idx */" in rendered


def test_transpile_statement_hint_spanner_to_spangres() -> None:
    """Verify statement hint transpiles from Spanner @{...} to Spangres /*@ ... */."""
    spanner_sql = "@{LOCK_SCANNED_RANGES=exclusive} SELECT * FROM albums"
    spangres_sql = parse_one(spanner_sql, dialect="spanner").sql(dialect="spangres")
    assert "/*@ LOCK_SCANNED_RANGES=exclusive */ SELECT * FROM albums" in spangres_sql


def test_transpile_statement_hint_spangres_to_spanner() -> None:
    """Verify statement hint transpiles from Spangres /*@ ... */ to Spanner @{...}."""
    spangres_sql = "/*@ LOCK_SCANNED_RANGES=exclusive */ SELECT * FROM albums"
    spanner_sql = parse_one(spangres_sql, dialect="spangres").sql(dialect="spanner")
    assert "@{LOCK_SCANNED_RANGES=exclusive} SELECT * FROM albums" in spanner_sql


def test_transpile_table_hint_bidirectional() -> None:
    """Verify table hint transpiles cleanly between Spanner and Spangres."""
    spanner_sql = "SELECT * FROM albums @{FORCE_INDEX=albums_idx}"
    spangres_sql = parse_one(spanner_sql, dialect="spanner").sql(dialect="spangres")
    assert "albums /*@ FORCE_INDEX=albums_idx */" in spangres_sql

    roundtrip_spanner = parse_one(spangres_sql, dialect="spangres").sql(dialect="spanner")
    assert "albums @{FORCE_INDEX=albums_idx}" in roundtrip_spanner
