"""Unit tests for Cloud Spanner Sequences (CREATE, ALTER, DROP, GET_NEXT_SEQUENCE_VALUE)."""

from sqlglot import exp, parse_one

from sqlspec.dialects.spanner._expressions import GetNextSequenceValue


def test_create_sequence_with_options() -> None:
    """Verify CREATE SEQUENCE with OPTIONS parses and generates canonical DDL."""
    sql = "CREATE SEQUENCE CustomerSequence OPTIONS (sequence_kind = 'bit_reversed_positive')"
    parsed = parse_one(sql, dialect="spanner")
    assert not isinstance(parsed, exp.Command)
    assert isinstance(parsed, exp.Create)
    assert parsed.kind == "SEQUENCE"
    rendered = parsed.sql(dialect="spanner")
    assert "CREATE SEQUENCE CustomerSequence" in rendered
    assert "sequence_kind = 'bit_reversed_positive'" in rendered


def test_create_sequence_full_options() -> None:
    """Verify CREATE SEQUENCE with full options set."""
    sql = (
        "CREATE SEQUENCE FullSeq OPTIONS ("
        "sequence_kind = 'bit_reversed_positive', "
        "skip_range_min = 1, "
        "skip_range_max = 1000, "
        "start_with_counter = 10)"
    )
    parsed = parse_one(sql, dialect="spanner")
    assert isinstance(parsed, exp.Create)
    rendered = parsed.sql(dialect="spanner")
    assert "CREATE SEQUENCE FullSeq" in rendered
    assert "skip_range_min = 1" in rendered
    assert "skip_range_max = 1000" in rendered
    assert "start_with_counter = 10" in rendered


def test_alter_sequence_set_options() -> None:
    """Verify ALTER SEQUENCE SET OPTIONS parses and generates."""
    sql = "ALTER SEQUENCE CustomerSequence SET OPTIONS (skip_range_min = 100)"
    parsed = parse_one(sql, dialect="spanner")
    assert not isinstance(parsed, exp.Command)
    assert isinstance(parsed, exp.Alter)
    rendered = parsed.sql(dialect="spanner")
    assert "ALTER SEQUENCE CustomerSequence SET OPTIONS" in rendered
    assert "skip_range_min = 100" in rendered


def test_drop_sequence() -> None:
    """Verify DROP SEQUENCE parses and generates."""
    sql = "DROP SEQUENCE CustomerSequence"
    parsed = parse_one(sql, dialect="spanner")
    assert not isinstance(parsed, exp.Command)
    assert isinstance(parsed, exp.Drop)
    rendered = parsed.sql(dialect="spanner")
    assert "DROP SEQUENCE CustomerSequence" in rendered


def test_get_next_sequence_value() -> None:
    """Verify GET_NEXT_SEQUENCE_VALUE(SEQUENCE name) parses and round-trips."""
    sql = "SELECT GET_NEXT_SEQUENCE_VALUE(SEQUENCE CustomerSequence) AS next_id"
    parsed = parse_one(sql, dialect="spanner")
    node = parsed.find(GetNextSequenceValue)
    assert node is not None
    assert isinstance(node, GetNextSequenceValue)
    rendered = parsed.sql(dialect="spanner")
    assert "GET_NEXT_SEQUENCE_VALUE(SEQUENCE CustomerSequence)" in rendered
