"""Dialect unit tests for the custom Spanner dialect."""

from pathlib import Path

from sqlglot import Dialect, exp, parse_one

from sqlspec.dialects.spanner import _generators, _spangres, _spanner
from sqlspec.dialects.spanner._generators import _bq_create_transform, _is_post_schema_spanner_property


def _render(sql: str) -> str:
    return parse_one(sql, dialect="spanner").sql(dialect="spanner")


def test_parse_and_generate_interleave_clause() -> None:
    sql = "\n    CREATE TABLE child (\n        parent_id STRING(36),\n        child_id INT64,\n        PRIMARY KEY (parent_id, child_id)\n    ) INTERLEAVE IN PARENT parent_table\n    "
    rendered = _render(sql)
    assert "INTERLEAVE IN PARENT parent_table" in rendered


def test_parse_interleave_with_on_delete_cascade() -> None:
    sql = "\n    CREATE TABLE child (\n        parent_id STRING(36),\n        child_id INT64,\n        PRIMARY KEY (parent_id, child_id)\n    ) INTERLEAVE IN PARENT parent_table ON DELETE CASCADE\n    "
    rendered = _render(sql)
    assert "INTERLEAVE IN PARENT parent_table ON DELETE CASCADE" in rendered


def test_interleave_create_repairs_command_fallback() -> None:
    sql = "\n    CREATE TABLE child (\n        parent_id STRING(36),\n        child_id INT64,\n        PRIMARY KEY (parent_id, child_id)\n    ) INTERLEAVE IN PARENT parent_table ON DELETE CASCADE\n    "
    expression = parse_one(sql, dialect="spanner")
    assert isinstance(expression, exp.Create)
    assert "INTERLEAVE IN PARENT parent_table ON DELETE CASCADE" in expression.sql(dialect="spanner")


def test_parse_ttl_clause_roundtrip() -> None:
    sql = "\n    CREATE TABLE orders (\n        order_id INT64,\n        created_at TIMESTAMP,\n        PRIMARY KEY (order_id)\n    ) ROW DELETION POLICY (OLDER_THAN(created_at, INTERVAL 30 DAY))\n    "
    rendered = _render(sql)
    assert "ROW DELETION POLICY" in rendered
    assert "OLDER_THAN(created_at" in rendered


def test_roundtrip_interleave_and_ttl_together() -> None:
    sql = "\n    CREATE TABLE child (\n        parent_id STRING(36),\n        child_id INT64,\n        expires_at TIMESTAMP,\n        PRIMARY KEY (parent_id, child_id)\n    ) INTERLEAVE IN PARENT parent_table ON DELETE NO ACTION\n      ROW DELETION POLICY (OLDER_THAN(expires_at, INTERVAL 7 DAY))\n    "
    rendered = _render(sql)
    assert "INTERLEAVE IN PARENT parent_table ON DELETE NO ACTION" in rendered
    assert "ROW DELETION POLICY (OLDER_THAN(expires_at, INTERVAL 7 DAY))" in rendered


def test_interleave_on_delete_cascade() -> None:
    sql = "\n    CREATE TABLE child (\n        parent_id STRING(36),\n        child_id INT64,\n        PRIMARY KEY (parent_id, child_id)\n    ) INTERLEAVE IN PARENT parent_table ON DELETE CASCADE\n    "
    rendered = _render(sql)
    assert "INTERLEAVE IN PARENT parent_table ON DELETE CASCADE" in rendered


def test_interleave_without_on_delete() -> None:
    sql = "\n    CREATE TABLE child (\n        parent_id STRING(36),\n        child_id INT64,\n        PRIMARY KEY (parent_id, child_id)\n    ) INTERLEAVE IN PARENT parent_table\n    "
    rendered = _render(sql)
    assert "INTERLEAVE IN PARENT parent_table" in rendered
    assert "ON DELETE" not in rendered


def test_row_deletion_policy_interval_literal() -> None:
    sql = "\n    CREATE TABLE logs (\n        id STRING(36),\n        created_at TIMESTAMP,\n        PRIMARY KEY (id)\n    ) ROW DELETION POLICY (OLDER_THAN(created_at, INTERVAL '90 days'))\n    "
    rendered = _render(sql)
    assert "ROW DELETION POLICY" in rendered
    assert "INTERVAL '90 days'" in rendered


def test_legacy_ttl_pg_style_still_parses() -> None:
    sql = "\n    CREATE TABLE ttl_table (\n        id INT64,\n        expires_at TIMESTAMP,\n        PRIMARY KEY (id)\n    ) TTL INTERVAL '5 days' ON expires_at\n    "
    rendered = _render(sql)
    assert "TTL INTERVAL" in rendered
    assert "expires_at" in rendered


def test_spanner_c19_dedup_normalize_interval_expression_has_single_definition() -> None:
    package_root = Path("sqlspec/dialects/spanner")
    matches = [path for path in package_root.glob("_*.py") if "def _normalize_interval_expression" in path.read_text()]
    assert matches == [package_root / "_generators.py"]


def test_spanner_c19_dedup_spanner_constants_have_single_definition() -> None:
    package_root = Path("sqlspec/dialects/spanner")
    for name in ("_TTL_MIN_COMPONENTS", "_ROW_DELETION_NAME", "_INTERLEAVE_NAME"):
        matches = [path for path in package_root.glob("_*.py") if f"{name} =" in path.read_text()]
        assert matches == [package_root / "_generators.py"]


def test_spanner_c19_dedup_spanner_modules_import_canonical_helpers() -> None:
    assert _spanner._normalize_interval_expression is _generators._normalize_interval_expression
    assert _spangres._normalize_interval_expression is _generators._normalize_interval_expression
    assert _spanner._TTL_MIN_COMPONENTS is _generators._TTL_MIN_COMPONENTS
    assert _spangres._TTL_MIN_COMPONENTS is _generators._TTL_MIN_COMPONENTS
    assert _spanner._ROW_DELETION_NAME is _generators._ROW_DELETION_NAME
    assert _spangres._ROW_DELETION_NAME is _generators._ROW_DELETION_NAME
    assert _spanner._INTERLEAVE_NAME is _generators._INTERLEAVE_NAME
    assert _spangres._INTERLEAVE_NAME is _generators._INTERLEAVE_NAME


def _render_spanner(sql: str) -> str:
    return parse_one(sql, dialect="spanner").sql(dialect="spanner")


def _assert_token_after_column_list(rendered: str, token: str) -> None:
    assert f") {token}" in rendered


def test_spanner_ordering_interleave_appears_after_column_list() -> None:
    rendered = _render_spanner(
        "\n        CREATE TABLE child (\n            parent_id STRING(36),\n            child_id INT64,\n            PRIMARY KEY (parent_id, child_id)\n        ) INTERLEAVE IN PARENT parent_table ON DELETE CASCADE\n        "
    )
    assert "INTERLEAVE IN PARENT parent_table ON DELETE CASCADE" in rendered
    _assert_token_after_column_list(rendered, "INTERLEAVE IN PARENT")


def test_spanner_ordering_interleave_roundtrip_is_stable() -> None:
    sql = "\n    CREATE TABLE child (\n        parent_id STRING(36),\n        child_id INT64,\n        PRIMARY KEY (parent_id, child_id)\n    ) INTERLEAVE IN PARENT parent_table ON DELETE CASCADE\n    "
    first = _render_spanner(sql)
    assert _render_spanner(first) == first


def test_spanner_ordering_row_deletion_policy_appears_after_column_list() -> None:
    rendered = _render_spanner(
        "\n        CREATE TABLE logs (\n            id STRING(36),\n            created_at TIMESTAMP,\n            PRIMARY KEY (id)\n        ) ROW DELETION POLICY (OLDER_THAN(created_at, INTERVAL 30 DAY))\n        "
    )
    assert "ROW DELETION POLICY" in rendered
    _assert_token_after_column_list(rendered, "ROW DELETION POLICY")


def test_spanner_ordering_row_deletion_policy_roundtrip_is_stable() -> None:
    sql = "\n    CREATE TABLE logs (\n        id STRING(36),\n        created_at TIMESTAMP,\n        PRIMARY KEY (id)\n    ) ROW DELETION POLICY (OLDER_THAN(created_at, INTERVAL 30 DAY))\n    "
    first = _render_spanner(sql)
    assert _render_spanner(first) == first


def test_spanner_ordering_spanner_property_moves_after_non_spanner_property() -> None:
    parsed = parse_one(
        "\n        CREATE TABLE child (\n            parent_id STRING(36),\n            child_id INT64,\n            PRIMARY KEY (parent_id, child_id)\n        ) INTERLEAVE IN PARENT parent_table ON DELETE CASCADE\n        ",
        dialect="spanner",
    )
    assert isinstance(parsed, exp.Create)
    properties = parsed.args["properties"]
    spanner_property = properties.expressions[0]
    non_spanner_property = exp.Property(
        this=exp.Literal.string("enable_change_stream_capture"), value=exp.Boolean(this=True)
    )
    assert _is_post_schema_spanner_property(spanner_property)
    assert not _is_post_schema_spanner_property(non_spanner_property)
    properties.set("expressions", [spanner_property, non_spanner_property])
    generator = Dialect.get_or_raise("spanner").generator()
    _bq_create_transform(generator, parsed)
    assert properties.expressions == [non_spanner_property, spanner_property]


def test_spanner_ordering_two_spanner_properties_order_preserved() -> None:
    rendered = _render_spanner(
        "\n        CREATE TABLE child (\n            parent_id STRING(36),\n            child_id INT64,\n            expires_at TIMESTAMP,\n            PRIMARY KEY (parent_id, child_id)\n        ) INTERLEAVE IN PARENT parent_table ON DELETE NO ACTION\n          ROW DELETION POLICY (OLDER_THAN(expires_at, INTERVAL 7 DAY))\n        "
    )
    assert rendered.find("INTERLEAVE IN PARENT") < rendered.find("ROW DELETION POLICY")


def test_spanner_ordering_two_spanner_properties_order_preserved_reverse_input() -> None:
    rendered = _render_spanner(
        "\n        CREATE TABLE child (\n            parent_id STRING(36),\n            child_id INT64,\n            expires_at TIMESTAMP,\n            PRIMARY KEY (parent_id, child_id)\n        ) ROW DELETION POLICY (OLDER_THAN(expires_at, INTERVAL 7 DAY))\n          INTERLEAVE IN PARENT parent_table ON DELETE NO ACTION\n        "
    )
    assert rendered.find("ROW DELETION POLICY") < rendered.find("INTERLEAVE IN PARENT")
