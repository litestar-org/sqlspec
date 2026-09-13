"""Unit tests for sqlspec.core.config_runtime."""

from typing import Any

import pytest
from sqlglot import parse_one

import sqlspec.dialects.postgres  # noqa: F401
from sqlspec.core.config_runtime import (
    build_postgres_extension_probe_names,
    is_postgres_extension_active,
    resolve_postgres_extension_state,
)
from sqlspec.core.statement import StatementConfig


def test_build_postgres_extension_probe_names_pg_textsearch() -> None:
    features = {"enable_pgvector": True, "enable_paradedb": True, "enable_pg_textsearch": True}
    probes = build_postgres_extension_probe_names(features)
    assert probes == ["vector", "pg_search", "pg_textsearch"]


def test_resolve_postgres_extension_state_active_extensions() -> None:
    features = {"enable_pgvector": True, "enable_pg_textsearch": True}
    config = StatementConfig(dialect="postgres")
    detected = {"vector", "pg_textsearch"}

    updated_config, pgvector_avail, paradedb_avail = resolve_postgres_extension_state(config, features, detected)

    assert pgvector_avail is True
    assert paradedb_avail is False
    assert updated_config.dialect == "pg_textsearch"
    assert "active_extensions" in features
    assert features["active_extensions"] == {"vector", "pg_textsearch"}
    assert is_postgres_extension_active(features, "pg_textsearch") is True
    assert is_postgres_extension_active(features, "vector") is True
    assert is_postgres_extension_active(features, "pg_search") is False


@pytest.mark.parametrize(
    "detected",
    [
        set(),
        {"vector"},
        {"pg_search"},
        {"pg_textsearch"},
        {"vector", "pg_search"},
        {"vector", "pg_textsearch"},
        {"pg_search", "pg_textsearch"},
        {"vector", "pg_search", "pg_textsearch"},
    ],
)
def test_enabled_extension_combinations_preserve_all_operators(detected: set[str]) -> None:
    features: dict[str, Any] = {"enable_pgvector": True, "enable_paradedb": True, "enable_pg_textsearch": True}
    config, _, _ = resolve_postgres_extension_state(StatementConfig(dialect="postgres"), features, detected)
    expressions = ["1"]
    if "vector" in detected:
        expressions.append("embedding <=> '[1,2,3]'")
    if "pg_search" in detected:
        expressions.append("content @@@ 'query'")
    if "pg_textsearch" in detected:
        expressions.append("content <@> 'query'")
    query = "SELECT " + ", ".join(expressions) + " FROM documents"
    rendered = parse_one(query, read=config.dialect).sql(dialect=config.dialect)
    for operator in ("<=>", "@@@", "<@>"):
        if operator in query:
            assert operator in rendered
    assert features["active_extensions"] == detected
