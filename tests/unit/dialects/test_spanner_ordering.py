"""Regression tests for Spanner CREATE TABLE property ordering."""

from sqlglot import Dialect, exp, parse_one

import sqlspec.dialects  # noqa: F401
from sqlspec.dialects.spanner._generators import _bq_create_transform, _is_post_schema_spanner_property


def _render_spanner(sql: str) -> str:
    return parse_one(sql, dialect="spanner").sql(dialect="spanner")


def _assert_token_after_column_list(rendered: str, token: str) -> None:
    assert f") {token}" in rendered


def test_interleave_appears_after_column_list() -> None:
    rendered = _render_spanner(
        """
        CREATE TABLE child (
            parent_id STRING(36),
            child_id INT64,
            PRIMARY KEY (parent_id, child_id)
        ) INTERLEAVE IN PARENT parent_table ON DELETE CASCADE
        """
    )

    assert "INTERLEAVE IN PARENT parent_table ON DELETE CASCADE" in rendered
    _assert_token_after_column_list(rendered, "INTERLEAVE IN PARENT")


def test_interleave_roundtrip_is_stable() -> None:
    sql = """
    CREATE TABLE child (
        parent_id STRING(36),
        child_id INT64,
        PRIMARY KEY (parent_id, child_id)
    ) INTERLEAVE IN PARENT parent_table ON DELETE CASCADE
    """

    first = _render_spanner(sql)
    assert _render_spanner(first) == first


def test_row_deletion_policy_appears_after_column_list() -> None:
    rendered = _render_spanner(
        """
        CREATE TABLE logs (
            id STRING(36),
            created_at TIMESTAMP,
            PRIMARY KEY (id)
        ) ROW DELETION POLICY (OLDER_THAN(created_at, INTERVAL 30 DAY))
        """
    )

    assert "ROW DELETION POLICY" in rendered
    _assert_token_after_column_list(rendered, "ROW DELETION POLICY")


def test_row_deletion_policy_roundtrip_is_stable() -> None:
    sql = """
    CREATE TABLE logs (
        id STRING(36),
        created_at TIMESTAMP,
        PRIMARY KEY (id)
    ) ROW DELETION POLICY (OLDER_THAN(created_at, INTERVAL 30 DAY))
    """

    first = _render_spanner(sql)
    assert _render_spanner(first) == first


def test_spanner_property_moves_after_non_spanner_property() -> None:
    parsed = parse_one(
        """
        CREATE TABLE child (
            parent_id STRING(36),
            child_id INT64,
            PRIMARY KEY (parent_id, child_id)
        ) INTERLEAVE IN PARENT parent_table ON DELETE CASCADE
        """,
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


def test_two_spanner_properties_order_preserved() -> None:
    rendered = _render_spanner(
        """
        CREATE TABLE child (
            parent_id STRING(36),
            child_id INT64,
            expires_at TIMESTAMP,
            PRIMARY KEY (parent_id, child_id)
        ) INTERLEAVE IN PARENT parent_table ON DELETE NO ACTION
          ROW DELETION POLICY (OLDER_THAN(expires_at, INTERVAL 7 DAY))
        """
    )

    assert rendered.find("INTERLEAVE IN PARENT") < rendered.find("ROW DELETION POLICY")


def test_two_spanner_properties_order_preserved_reverse_input() -> None:
    rendered = _render_spanner(
        """
        CREATE TABLE child (
            parent_id STRING(36),
            child_id INT64,
            expires_at TIMESTAMP,
            PRIMARY KEY (parent_id, child_id)
        ) ROW DELETION POLICY (OLDER_THAN(expires_at, INTERVAL 7 DAY))
          INTERLEAVE IN PARENT parent_table ON DELETE NO ACTION
        """
    )

    assert rendered.find("ROW DELETION POLICY") < rendered.find("INTERLEAVE IN PARENT")
