"""Tests for sqlcommenter pipeline integration — transformer factory and StatementConfig wiring."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from sqlspec.extensions.sqlcommenter import create_sqlcommenter_transformer

# ── create_sqlcommenter_transformer ───────────────────────────────────────


def test_transformer_appends_static_attrs() -> None:
    transformer = create_sqlcommenter_transformer(attributes={"db_driver": "asyncpg", "framework": "litestar"})
    sql, params = transformer("SELECT * FROM users", {"id": 1})
    assert sql == "SELECT * FROM users /*db_driver='asyncpg',framework='litestar'*/"
    assert params == {"id": 1}


def test_transformer_empty_attrs_returns_unchanged() -> None:
    transformer = create_sqlcommenter_transformer(attributes={})
    sql, params = transformer("SELECT 1", None)
    assert sql == "SELECT 1"
    assert params is None


def test_transformer_skips_existing_comments() -> None:
    transformer = create_sqlcommenter_transformer(attributes={"db_driver": "asyncpg"})
    original = "SELECT /* hint */ * FROM users"
    sql, _params = transformer(original, None)
    assert sql == original


def test_transformer_preserves_params_unchanged() -> None:
    params_in = [1, 2, 3]
    transformer = create_sqlcommenter_transformer(attributes={"key": "val"})
    _, params_out = transformer("SELECT 1", params_in)
    assert params_out is params_in


def test_transformer_with_traceparent_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_ctx = MagicMock(
        is_valid=True,
        trace_id=0x0AF7651916CD43DD8448EB211C80319C,
        span_id=0xB7AD6B7169203331,
        trace_flags=MagicMock(default=1),
    )

    mock_span = MagicMock()
    mock_span.is_recording.return_value = True
    mock_span.get_span_context.return_value = mock_ctx

    with patch(
        "sqlspec.extensions.sqlcommenter.get_trace_context",
        return_value=("0af7651916cd43dd8448eb211c80319c", "b7ad6b7169203331"),
    ):
        transformer = create_sqlcommenter_transformer(attributes={"db_driver": "asyncpg"}, enable_traceparent=True)
        sql, _ = transformer("SELECT 1", None)
        # traceparent should be present in the comment
        assert "traceparent=" in sql
        assert "db_driver='asyncpg'" in sql


def test_transformer_traceparent_disabled_by_default() -> None:
    transformer = create_sqlcommenter_transformer(attributes={"db_driver": "asyncpg"})
    sql, _ = transformer("SELECT 1", None)
    assert "traceparent" not in sql


def test_transformer_traceparent_no_otel_available() -> None:
    with patch("sqlspec.extensions.sqlcommenter.get_trace_context", return_value=(None, None)):
        transformer = create_sqlcommenter_transformer(attributes={"db_driver": "asyncpg"}, enable_traceparent=True)
        sql, _ = transformer("SELECT 1", None)
        # Should still have db_driver but no traceparent
        assert "db_driver='asyncpg'" in sql
        assert "traceparent" not in sql


# ── StatementConfig integration ───────────────────────────────────────────


def test_statement_config_enable_sqlcommenter() -> None:
    from sqlspec.core import StatementConfig

    config = StatementConfig(
        enable_sqlcommenter=True, sqlcommenter_attributes={"db_driver": "sqlite", "framework": "litestar"}
    )
    assert config.output_transformer is not None


def test_statement_config_sqlcommenter_disabled_by_default() -> None:
    from sqlspec.core import StatementConfig

    config = StatementConfig()
    assert config.output_transformer is None


def test_statement_config_sqlcommenter_chains_with_existing_transformer() -> None:
    from sqlspec.core import StatementConfig

    def my_transformer(sql: str, params: object) -> tuple[str, object]:
        return f"/* tenant=acme */ {sql}", params

    config = StatementConfig(
        output_transformer=my_transformer, enable_sqlcommenter=True, sqlcommenter_attributes={"db_driver": "sqlite"}
    )
    assert config.output_transformer is not None
    # User transformer runs first; sqlcommenter sees existing comment and skips
    sql, _params = config.output_transformer("SELECT 1", None)
    assert "/* tenant=acme */" in sql
    # Per spec: existing comments → do not mutate
    assert "db_driver" not in sql


def test_statement_config_sqlcommenter_chains_no_conflict() -> None:
    from sqlspec.core import StatementConfig

    def my_transformer(sql: str, params: object) -> tuple[str, object]:
        return sql.upper(), params

    config = StatementConfig(
        output_transformer=my_transformer, enable_sqlcommenter=True, sqlcommenter_attributes={"db_driver": "sqlite"}
    )
    assert config.output_transformer is not None
    sql, _ = config.output_transformer("select 1", None)
    # User transformer uppercases, then sqlcommenter appends
    assert sql == "SELECT 1 /*db_driver='sqlite'*/"


def test_statement_config_replace_preserves_sqlcommenter() -> None:
    from sqlspec.core import StatementConfig

    config = StatementConfig(enable_sqlcommenter=True, sqlcommenter_attributes={"db_driver": "sqlite"})
    replaced = config.replace(enable_caching=False)
    assert replaced.output_transformer is not None
