"""Tests for sqlspec.core.sqlcommenter — Google SQLCommenter spec compliance via sqlglot AST."""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

import sqlglot
from sqlglot import exp

from sqlspec.core.sqlcommenter import (
    SQLCommenterContext,
    append_comment,
    create_sqlcommenter_statement_transformer,
    generate_comment,
    parse_comment,
)

# ── generate_comment ──────────────────────────────────────────────────────


def test_generate_comment_basic() -> None:
    attrs: dict[str, str | None] = {"db_driver": "asyncpg", "route": "/users"}
    result = generate_comment(attrs)
    assert result == "db_driver='asyncpg',route='%2Fusers'"


def test_generate_comment_empty_attrs() -> None:
    result = generate_comment({})
    assert result == ""


def test_generate_comment_lexicographic_sort() -> None:
    attrs: dict[str, str | None] = {"z_key": "z", "a_key": "a", "m_key": "m"}
    result = generate_comment(attrs)
    assert result == "a_key='a',m_key='m',z_key='z'"


def test_generate_comment_url_encodes_values() -> None:
    attrs: dict[str, str | None] = {"route": "/polls 1000"}
    result = generate_comment(attrs)
    assert result == "route='%2Fpolls%201000'"


def test_generate_comment_url_encodes_keys() -> None:
    attrs: dict[str, str | None] = {"route parameter": "value"}
    result = generate_comment(attrs)
    assert result == "route%20parameter='value'"


def test_generate_comment_encodes_single_quotes_in_values() -> None:
    attrs: dict[str, str | None] = {"key": "it's a test"}
    result = generate_comment(attrs)
    assert result == "key='it%27s%20a%20test'"


def test_generate_comment_encodes_single_quotes_in_keys() -> None:
    attrs: dict[str, str | None] = {"it's": "value"}
    result = generate_comment(attrs)
    assert result == "it%27s='value'"


def test_generate_comment_special_characters() -> None:
    attrs: dict[str, str | None] = {"key": "a=b&c"}
    result = generate_comment(attrs)
    assert result == "key='a%3Db%26c'"


def test_generate_comment_none_values_skipped() -> None:
    attrs: dict[str, str | None] = {"key1": "val1", "key2": None}  # type: ignore[typeddict-item]
    result = generate_comment(attrs)
    assert result == "key1='val1'"


def test_empty_string_value() -> None:
    attrs: dict[str, str | None] = {"key": ""}
    result = generate_comment(attrs)
    assert result == "key=''"


# ── append_comment (AST-level) ────────────────────────────────────────────


def test_append_comment_basic() -> None:
    expr = sqlglot.parse_one("SELECT * FROM users")
    append_comment(expr, {"db_driver": "asyncpg"})
    sql = expr.sql()
    assert "db_driver='asyncpg'" in sql
    assert "/*" in sql


def test_append_comment_empty_attrs_no_change() -> None:
    expr = sqlglot.parse_one("SELECT * FROM users")
    append_comment(expr, {})
    assert not expr.comments


def test_append_comment_coexists_with_existing_comments() -> None:
    expr = sqlglot.parse_one("SELECT /* existing note */ * FROM users")
    append_comment(expr, {"db_driver": "asyncpg"})
    sql = expr.sql()
    assert "existing note" in sql
    assert "db_driver='asyncpg'" in sql


def test_append_comment_coexists_with_hints() -> None:
    expr = sqlglot.parse_one("SELECT /*+ IndexScan(t) */ * FROM users")
    append_comment(expr, {"db_driver": "asyncpg"})
    sql = expr.sql()
    assert "db_driver='asyncpg'" in sql
    # Hint should be preserved as a separate node
    assert "INDEXSCAN" in sql.upper()


def test_append_comment_multiple_attrs_sorted() -> None:
    expr = sqlglot.parse_one("SELECT 1")
    append_comment(expr, {"framework": "litestar", "db_driver": "asyncpg", "action": "list"})
    sql = expr.sql()
    assert "action='list',db_driver='asyncpg',framework='litestar'" in sql


def test_append_comment_works_on_insert() -> None:
    expr = sqlglot.parse_one("INSERT INTO users (name) VALUES ('alice')")
    append_comment(expr, {"db_driver": "asyncpg"})
    assert "db_driver='asyncpg'" in expr.sql()


def test_append_comment_works_on_update() -> None:
    expr = sqlglot.parse_one("UPDATE users SET name = 'bob' WHERE id = 1")
    append_comment(expr, {"db_driver": "asyncpg"})
    assert "db_driver='asyncpg'" in expr.sql()


def test_append_comment_works_on_delete() -> None:
    expr = sqlglot.parse_one("DELETE FROM users WHERE id = 1")
    append_comment(expr, {"db_driver": "asyncpg"})
    assert "db_driver='asyncpg'" in expr.sql()


# ── parse_comment (AST-level) ─────────────────────────────────────────────


def test_parse_comment_basic() -> None:
    expr = sqlglot.parse_one("SELECT * FROM users")
    append_comment(expr, {"db_driver": "asyncpg", "route": "/users"})
    _, attrs = parse_comment(expr)
    assert attrs == {"db_driver": "asyncpg", "route": "/users"}


def test_parse_comment_no_comments() -> None:
    expr = sqlglot.parse_one("SELECT * FROM users")
    _, attrs = parse_comment(expr)
    assert attrs == {}


def test_parse_comment_preserves_non_sqlcommenter_comments() -> None:
    expr = sqlglot.parse_one("SELECT /* just a note */ * FROM users")
    _, attrs = parse_comment(expr)
    assert attrs == {}
    assert expr.comments is not None
    assert any("just a note" in c for c in expr.comments)


def test_parse_comment_url_decodes() -> None:
    expr = sqlglot.parse_one("SELECT 1")
    append_comment(expr, {"route": "/polls 1000"})
    _, attrs = parse_comment(expr)
    assert attrs == {"route": "/polls 1000"}


def test_parse_comment_round_trip() -> None:
    original_attrs: dict[str, str] = {
        "db_driver": "asyncpg",
        "framework": "litestar",
        "route": "/api/users",
        "controller": "UserController",
    }
    expr = sqlglot.parse_one("SELECT * FROM users WHERE id = :id")
    append_comment(expr, original_attrs)
    # Re-parse the generated SQL
    expr2 = sqlglot.parse_one(expr.sql())
    _, parsed_attrs = parse_comment(expr2)
    assert parsed_attrs == original_attrs


def test_parse_comment_separates_sqlcommenter_from_regular() -> None:
    expr = sqlglot.parse_one("SELECT * FROM users")
    expr.add_comments(["regular note"])
    append_comment(expr, {"db_driver": "asyncpg"})
    assert expr.comments is not None
    assert len(expr.comments) == 2

    _, attrs = parse_comment(expr)
    assert attrs == {"db_driver": "asyncpg"}
    # Regular comment should be preserved
    assert expr.comments is not None
    assert any("regular note" in c for c in expr.comments)


def test_traceparent_format_round_trip() -> None:
    tp = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
    attrs: dict[str, str | None] = {"traceparent": tp}
    expr = sqlglot.parse_one("SELECT 1")
    append_comment(expr, attrs)
    expr2 = sqlglot.parse_one(expr.sql())
    _, parsed = parse_comment(expr2)
    assert parsed["traceparent"] == tp


# ── create_sqlcommenter_statement_transformer ─────────────────────────────


def test_transformer_appends_static_attrs() -> None:
    transformer = create_sqlcommenter_statement_transformer(
        attributes={"db_driver": "asyncpg", "framework": "litestar"}
    )
    expr = sqlglot.parse_one("SELECT * FROM users")
    expr, params = transformer(expr, {"id": 1})
    sql = expr.sql()
    assert "db_driver='asyncpg'" in sql
    assert "framework='litestar'" in sql
    assert params == {"id": 1}


def test_transformer_empty_attrs_noop() -> None:
    transformer = create_sqlcommenter_statement_transformer(attributes={})
    expr = sqlglot.parse_one("SELECT 1")
    result_expr, _ = transformer(expr, None)
    assert not result_expr.comments


def test_transformer_preserves_params() -> None:
    params_in = [1, 2, 3]
    transformer = create_sqlcommenter_statement_transformer(attributes={"key": "val"})
    expr = sqlglot.parse_one("SELECT 1")
    _, params_out = transformer(expr, params_in)
    assert params_out is params_in


def test_transformer_with_traceparent() -> None:
    with patch(
        "sqlspec.core.sqlcommenter.get_trace_context",
        return_value=("0af7651916cd43dd8448eb211c80319c", "b7ad6b7169203331"),
    ):
        transformer = create_sqlcommenter_statement_transformer(
            attributes={"db_driver": "asyncpg"}, enable_traceparent=True
        )
        expr = sqlglot.parse_one("SELECT 1")
        expr, _ = transformer(expr, None)
        sql = expr.sql()
        assert "traceparent=" in sql
        assert "db_driver='asyncpg'" in sql


def test_transformer_traceparent_disabled_by_default() -> None:
    transformer = create_sqlcommenter_statement_transformer(attributes={"db_driver": "asyncpg"})
    expr = sqlglot.parse_one("SELECT 1")
    expr, _ = transformer(expr, None)
    assert "traceparent" not in expr.sql()


def test_transformer_traceparent_no_otel() -> None:
    with patch("sqlspec.core.sqlcommenter.get_trace_context", return_value=(None, None)):
        transformer = create_sqlcommenter_statement_transformer(
            attributes={"db_driver": "asyncpg"}, enable_traceparent=True
        )
        expr = sqlglot.parse_one("SELECT 1")
        expr, _ = transformer(expr, None)
        sql = expr.sql()
        assert "db_driver='asyncpg'" in sql
        assert "traceparent" not in sql


def test_transformer_reads_context_when_enabled() -> None:
    transformer = create_sqlcommenter_statement_transformer(attributes={"db_driver": "asyncpg"}, enable_context=True)
    with SQLCommenterContext.scope({"route": "/users", "framework": "litestar"}):
        expr = sqlglot.parse_one("SELECT 1")
        expr, _ = transformer(expr, None)
        sql = expr.sql()
        assert "db_driver='asyncpg'" in sql
        assert "route='%2Fusers'" in sql
        assert "framework='litestar'" in sql


def test_transformer_context_disabled_by_default() -> None:
    transformer = create_sqlcommenter_statement_transformer(attributes={"db_driver": "asyncpg"})
    with SQLCommenterContext.scope({"route": "/users"}):
        expr = sqlglot.parse_one("SELECT 1")
        expr, _ = transformer(expr, None)
        sql = expr.sql()
        assert "db_driver='asyncpg'" in sql
        assert "route" not in sql


def test_transformer_static_attrs_override_context() -> None:
    transformer = create_sqlcommenter_statement_transformer(attributes={"db_driver": "asyncpg"}, enable_context=True)
    with SQLCommenterContext.scope({"db_driver": "psycopg", "route": "/users"}):
        expr = sqlglot.parse_one("SELECT 1")
        expr, _ = transformer(expr, None)
        sql = expr.sql()
        assert "db_driver='asyncpg'" in sql
        assert "db_driver='psycopg'" not in sql
        assert "route='%2Fusers'" in sql


def test_transformer_coexists_with_existing_comments() -> None:
    transformer = create_sqlcommenter_statement_transformer(attributes={"db_driver": "asyncpg"})
    expr = sqlglot.parse_one("SELECT /* existing */ * FROM users")
    expr, _ = transformer(expr, None)
    sql = expr.sql()
    assert "existing" in sql
    assert "db_driver='asyncpg'" in sql


def test_transformer_coexists_with_hints() -> None:
    transformer = create_sqlcommenter_statement_transformer(attributes={"db_driver": "asyncpg"})
    expr = sqlglot.parse_one("SELECT /*+ IndexScan(t) */ * FROM users")
    expr, _ = transformer(expr, None)
    sql = expr.sql()
    assert "db_driver='asyncpg'" in sql
    assert "INDEXSCAN" in sql.upper()


# ── Correlation ID integration ────────────────────────────────────────────


def test_transformer_includes_correlation_id_when_context_enabled() -> None:
    from sqlspec.utils.correlation import CorrelationContext

    transformer = create_sqlcommenter_statement_transformer(attributes={"db_driver": "asyncpg"}, enable_context=True)
    with CorrelationContext.context("abc-123"):
        expr = sqlglot.parse_one("SELECT 1")
        expr, _ = transformer(expr, None)
        sql = expr.sql()
        assert "correlation_id='abc-123'" in sql
        assert "db_driver='asyncpg'" in sql


def test_transformer_no_correlation_id_without_context_enabled() -> None:
    from sqlspec.utils.correlation import CorrelationContext

    transformer = create_sqlcommenter_statement_transformer(attributes={"db_driver": "asyncpg"})
    with CorrelationContext.context("abc-123"):
        expr = sqlglot.parse_one("SELECT 1")
        expr, _ = transformer(expr, None)
        sql = expr.sql()
        assert "correlation_id" not in sql


def test_transformer_no_correlation_id_when_not_set() -> None:
    transformer = create_sqlcommenter_statement_transformer(attributes={"db_driver": "asyncpg"}, enable_context=True)
    expr = sqlglot.parse_one("SELECT 1")
    expr, _ = transformer(expr, None)
    sql = expr.sql()
    assert "correlation_id" not in sql


def test_transformer_explicit_correlation_id_in_context_overrides() -> None:
    from sqlspec.utils.correlation import CorrelationContext

    transformer = create_sqlcommenter_statement_transformer(enable_context=True)
    with CorrelationContext.context("from-middleware"):
        with SQLCommenterContext.scope({"correlation_id": "explicit-value"}):
            expr = sqlglot.parse_one("SELECT 1")
            expr, _ = transformer(expr, None)
            sql = expr.sql()
            assert "correlation_id='explicit-value'" in sql
            assert "from-middleware" not in sql


# ── SQLCommenterContext ───────────────────────────────────────────────────


def test_context_get_returns_none_by_default() -> None:
    assert SQLCommenterContext.get() is None


def test_context_set_and_get() -> None:
    SQLCommenterContext.set({"route": "/users", "controller": "UserController"})
    try:
        attrs = SQLCommenterContext.get()
        assert attrs is not None
        assert attrs["route"] == "/users"
    finally:
        SQLCommenterContext.set(None)


def test_context_scope_restores_previous() -> None:
    SQLCommenterContext.set({"route": "/outer"})
    try:
        with SQLCommenterContext.scope({"route": "/inner"}):
            attrs = SQLCommenterContext.get()
            assert attrs is not None
            assert attrs["route"] == "/inner"
        attrs = SQLCommenterContext.get()
        assert attrs is not None
        assert attrs["route"] == "/outer"
    finally:
        SQLCommenterContext.set(None)


# ── StatementConfig integration ───────────────────────────────────────────


def test_statement_config_enable_sqlcommenter() -> None:
    from sqlspec.core import StatementConfig

    config = StatementConfig(
        enable_sqlcommenter=True, sqlcommenter_attributes={"db_driver": "sqlite", "framework": "litestar"}
    )
    # Should be added as a statement_transformer, not output_transformer
    assert len(config.statement_transformers) == 1
    assert config.output_transformer is None


def test_statement_config_auto_sets_db_driver_from_dialect() -> None:
    from sqlspec.core import StatementConfig

    config = StatementConfig(enable_sqlcommenter=True, dialect="postgres")
    assert config.sqlcommenter_attributes is not None
    assert config.sqlcommenter_attributes["db_driver"] == "postgresql"


def test_statement_config_db_driver_not_overridden_when_explicit() -> None:
    from sqlspec.core import StatementConfig

    config = StatementConfig(
        enable_sqlcommenter=True, dialect="postgres", sqlcommenter_attributes={"db_driver": "custom-pg"}
    )
    assert config.sqlcommenter_attributes is not None
    assert config.sqlcommenter_attributes["db_driver"] == "custom-pg"


def test_statement_config_sqlcommenter_disabled_by_default() -> None:
    from sqlspec.core import StatementConfig

    config = StatementConfig()
    assert len(config.statement_transformers) == 0


def test_statement_config_sqlcommenter_appends_to_existing_transformers() -> None:
    from sqlspec.core import StatementConfig

    def my_transformer(expr: exp.Expr, params: Any) -> tuple[exp.Expr, Any]:
        return expr, params

    config = StatementConfig(
        statement_transformers=[my_transformer],
        enable_sqlcommenter=True,
        sqlcommenter_attributes={"db_driver": "sqlite"},
    )
    # Should have both: user transformer + sqlcommenter
    assert len(config.statement_transformers) == 2
    assert config.statement_transformers[0] is my_transformer


def test_statement_config_replace_preserves_sqlcommenter() -> None:
    from sqlspec.core import StatementConfig

    config = StatementConfig(enable_sqlcommenter=True, sqlcommenter_attributes={"db_driver": "sqlite"})
    replaced = config.replace(enable_caching=False)
    assert len(replaced.statement_transformers) == 1
