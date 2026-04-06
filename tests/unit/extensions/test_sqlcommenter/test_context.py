"""Tests for SQLCommenterContext and framework-enriched transformers."""

from __future__ import annotations

from unittest.mock import patch

from sqlspec.extensions.sqlcommenter import SQLCommenterContext, create_sqlcommenter_transformer

# ── SQLCommenterContext ───────────────────────────────────────────────────


def test_context_get_returns_none_by_default() -> None:
    assert SQLCommenterContext.get() is None


def test_context_set_and_get() -> None:
    SQLCommenterContext.set({"route": "/users", "controller": "UserController"})
    try:
        attrs = SQLCommenterContext.get()
        assert attrs is not None
        assert attrs["route"] == "/users"
        assert attrs["controller"] == "UserController"
    finally:
        SQLCommenterContext.set(None)


def test_context_clear() -> None:
    SQLCommenterContext.set({"route": "/users"})
    SQLCommenterContext.set(None)
    assert SQLCommenterContext.get() is None


def test_context_manager() -> None:
    assert SQLCommenterContext.get() is None
    with SQLCommenterContext.scope({"route": "/api/v1"}):
        attrs = SQLCommenterContext.get()
        assert attrs is not None
        assert attrs["route"] == "/api/v1"
    assert SQLCommenterContext.get() is None


def test_context_manager_restores_previous() -> None:
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


# ── Transformer with context ─────────────────────────────────────────────


def test_transformer_reads_context_when_enabled() -> None:
    transformer = create_sqlcommenter_transformer(attributes={"db_driver": "asyncpg"}, enable_context=True)
    with SQLCommenterContext.scope({"route": "/users", "framework": "litestar"}):
        sql, _ = transformer("SELECT 1", None)
        assert "db_driver='asyncpg'" in sql
        assert "route='%2Fusers'" in sql
        assert "framework='litestar'" in sql


def test_transformer_context_disabled_by_default() -> None:
    transformer = create_sqlcommenter_transformer(attributes={"db_driver": "asyncpg"})
    with SQLCommenterContext.scope({"route": "/users"}):
        sql, _ = transformer("SELECT 1", None)
        assert "db_driver='asyncpg'" in sql
        assert "route" not in sql


def test_transformer_static_attrs_override_context() -> None:
    transformer = create_sqlcommenter_transformer(attributes={"db_driver": "asyncpg"}, enable_context=True)
    # Context sets db_driver too, but static should win
    with SQLCommenterContext.scope({"db_driver": "psycopg", "route": "/users"}):
        sql, _ = transformer("SELECT 1", None)
        assert "db_driver='asyncpg'" in sql
        assert "db_driver='psycopg'" not in sql
        assert "route='%2Fusers'" in sql


def test_transformer_context_and_traceparent() -> None:
    with patch("sqlspec.extensions.sqlcommenter.get_trace_context", return_value=("abc123", "def456")):
        transformer = create_sqlcommenter_transformer(
            attributes={"db_driver": "asyncpg"}, enable_context=True, enable_traceparent=True
        )
        with SQLCommenterContext.scope({"route": "/users"}):
            sql, _ = transformer("SELECT 1", None)
            assert "db_driver='asyncpg'" in sql
            assert "route='%2Fusers'" in sql
            assert "traceparent=" in sql
