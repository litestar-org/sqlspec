"""Dialect unit tests for the custom Spanner (GoogleSQL) dialect.

DDL fixtures follow the official GoogleSQL grammar: table-level PRIMARY KEY
after the closing paren, comma-separated INTERLEAVE and ROW DELETION POLICY
clauses, and DAY-based row deletion intervals.
"""

from pathlib import Path

from sqlglot import Dialect, exp, parse_one

from sqlspec.dialects.spanner import _generators, _parsers
from sqlspec.dialects.spanner._generators import _bq_create_transform, _is_post_schema_spanner_property

OFFICIAL_INTERLEAVE_DDL = """
CREATE TABLE Albums (
  SingerId     INT64 NOT NULL,
  AlbumId      INT64 NOT NULL,
  AlbumTitle   STRING(MAX),
) PRIMARY KEY (SingerId, AlbumId),
  INTERLEAVE IN PARENT Singers ON DELETE CASCADE
"""

OFFICIAL_INTERLEAVE_IN_DDL = """
CREATE TABLE Resources (
  ProjectId    INT64 NOT NULL,
  ResourceId   INT64 NOT NULL,
  ResourceName STRING(1024),
) PRIMARY KEY (ProjectId, ResourceId),
  INTERLEAVE IN Projects
"""

OFFICIAL_ROW_DELETION_DDL = """
CREATE TABLE MyTable (
  Key INT64,
  CreatedAt TIMESTAMP,
) PRIMARY KEY (Key),
  ROW DELETION POLICY (OLDER_THAN(CreatedAt, INTERVAL 30 DAY))
"""

COMBINED_DDL = """
CREATE TABLE child (
  parent_id STRING(36),
  child_id INT64,
  expires_at TIMESTAMP,
) PRIMARY KEY (parent_id, child_id),
  INTERLEAVE IN PARENT parent_table ON DELETE NO ACTION,
  ROW DELETION POLICY (OLDER_THAN(expires_at, INTERVAL 7 DAY))
"""


def _render(sql: str) -> str:
    return parse_one(sql, dialect="spanner").sql(dialect="spanner")


def test_official_interleave_example_parses_and_renders() -> None:
    rendered = _render(OFFICIAL_INTERLEAVE_DDL)
    assert "PRIMARY KEY (SingerId, AlbumId), INTERLEAVE IN PARENT Singers ON DELETE CASCADE" in rendered


def test_official_interleave_without_parent_keyword() -> None:
    rendered = _render(OFFICIAL_INTERLEAVE_IN_DDL)
    assert ", INTERLEAVE IN Projects" in rendered
    assert "PARENT" not in rendered


def test_official_row_deletion_policy_example() -> None:
    rendered = _render(OFFICIAL_ROW_DELETION_DDL)
    assert ", ROW DELETION POLICY (OLDER_THAN(CreatedAt, INTERVAL 30 DAY))" in rendered


def test_combined_clauses_are_comma_separated_and_ordered() -> None:
    rendered = _render(COMBINED_DDL)
    assert ", INTERLEAVE IN PARENT parent_table ON DELETE NO ACTION" in rendered
    assert ", ROW DELETION POLICY (OLDER_THAN(expires_at, INTERVAL 7 DAY))" in rendered
    assert rendered.find("INTERLEAVE IN PARENT") < rendered.find("ROW DELETION POLICY")


def test_combined_clauses_reverse_input_order_preserved() -> None:
    sql = """
    CREATE TABLE child (
      parent_id STRING(36),
      child_id INT64,
      expires_at TIMESTAMP,
    ) PRIMARY KEY (parent_id, child_id),
      ROW DELETION POLICY (OLDER_THAN(expires_at, INTERVAL 7 DAY)),
      INTERLEAVE IN PARENT parent_table ON DELETE NO ACTION
    """
    rendered = _render(sql)
    assert rendered.find("ROW DELETION POLICY") < rendered.find("INTERLEAVE IN PARENT")


def test_roundtrip_is_stable() -> None:
    for sql in (OFFICIAL_INTERLEAVE_DDL, OFFICIAL_INTERLEAVE_IN_DDL, OFFICIAL_ROW_DELETION_DDL, COMBINED_DDL):
        first = _render(sql)
        assert _render(first) == first


def test_interleave_create_repairs_command_fallback() -> None:
    expression = parse_one(OFFICIAL_INTERLEAVE_DDL, dialect="spanner")
    assert isinstance(expression, exp.Create)
    assert "INTERLEAVE IN PARENT Singers ON DELETE CASCADE" in expression.sql(dialect="spanner")


def test_inline_primary_key_style_still_parses() -> None:
    sql = """
    CREATE TABLE child (
        parent_id STRING(36),
        child_id INT64,
        PRIMARY KEY (parent_id, child_id)
    ) INTERLEAVE IN PARENT parent_table ON DELETE CASCADE
    """
    rendered = _render(sql)
    assert "INTERLEAVE IN PARENT parent_table ON DELETE CASCADE" in rendered
    assert _render(rendered) == rendered


def test_interleave_without_on_delete() -> None:
    sql = """
    CREATE TABLE child (
        parent_id STRING(36),
        child_id INT64,
    ) PRIMARY KEY (parent_id, child_id),
      INTERLEAVE IN PARENT parent_table
    """
    rendered = _render(sql)
    assert "INTERLEAVE IN PARENT parent_table" in rendered
    assert "ON DELETE" not in rendered


def test_string_interval_normalizes_to_googlesql_day_form() -> None:
    sql = """
    CREATE TABLE logs (
        id STRING(36),
        created_at TIMESTAMP,
    ) PRIMARY KEY (id),
      ROW DELETION POLICY (OLDER_THAN(created_at, INTERVAL '90 days'))
    """
    rendered = _render(sql)
    assert "ROW DELETION POLICY (OLDER_THAN(created_at, INTERVAL 90 DAY))" in rendered


def test_pg_ttl_form_normalizes_to_row_deletion_policy() -> None:
    sql = """
    CREATE TABLE ttl_table (
        id INT64,
        expires_at TIMESTAMP,
        PRIMARY KEY (id)
    ) TTL INTERVAL '5 days' ON expires_at
    """
    rendered = _render(sql)
    assert "TTL INTERVAL" not in rendered
    assert "ROW DELETION POLICY (OLDER_THAN(expires_at, INTERVAL 5 DAY))" in rendered


def test_transpile_googlesql_policies_to_spangres() -> None:
    rendered = parse_one(COMBINED_DDL, dialect="spanner").sql(dialect="spangres")
    assert "INTERLEAVE IN PARENT parent_table ON DELETE NO ACTION" in rendered
    assert "TTL INTERVAL '7 days' ON expires_at" in rendered
    assert "ROW DELETION POLICY" not in rendered


def test_spanner_property_moves_after_non_spanner_property() -> None:
    parsed = parse_one(OFFICIAL_INTERLEAVE_DDL, dialect="spanner")
    assert isinstance(parsed, exp.Create)
    properties = parsed.args["properties"]
    spanner_property = next(p for p in properties.expressions if _is_post_schema_spanner_property(p))
    non_spanner_property = exp.Property(
        this=exp.Literal.string("enable_change_stream_capture"), value=exp.Boolean(this=True)
    )
    assert not _is_post_schema_spanner_property(non_spanner_property)
    properties.set("expressions", [spanner_property, non_spanner_property])
    generator = Dialect.get_or_raise("spanner").generator()
    _bq_create_transform(generator, parsed)
    assert properties.expressions == [non_spanner_property, spanner_property]


def test_dedup_normalize_interval_expression_has_single_definition() -> None:
    package_root = Path("sqlspec/dialects/spanner")
    matches = [path for path in package_root.glob("_*.py") if "def _normalize_interval_expression" in path.read_text()]
    assert matches == [package_root / "_generators.py"]


def test_dedup_spanner_constants_have_single_definition() -> None:
    package_root = Path("sqlspec/dialects/spanner")
    for name in ("_TTL_MIN_COMPONENTS", "_ROW_DELETION_NAME", "_INTERLEAVE_NAME", "_INTERLEAVE_IN_NAME"):
        matches = [path for path in package_root.glob("_*.py") if f"{name} =" in path.read_text()]
        assert matches == [package_root / "_generators.py"]


def test_dedup_parsers_import_canonical_helpers() -> None:
    assert _parsers._normalize_interval_expression is _generators._normalize_interval_expression
    assert _parsers._ROW_DELETION_NAME is _generators._ROW_DELETION_NAME
    assert _parsers._INTERLEAVE_NAME is _generators._INTERLEAVE_NAME
    assert _parsers._INTERLEAVE_IN_NAME is _generators._INTERLEAVE_IN_NAME
