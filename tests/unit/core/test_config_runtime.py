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
    features: dict[str, Any] = {"enable_pgvector": True, "enable_pg_textsearch": True}
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
    ("detected", "expected_dialect"),
    [
        (set(), "postgres"),
        ({"vector"}, "pgvector"),
        ({"pg_search"}, "paradedb"),
        ({"pg_textsearch"}, "pg_textsearch"),
        ({"vector", "pg_search"}, "paradedb"),
        ({"vector", "pg_textsearch"}, "pg_textsearch"),
        ({"pg_search", "pg_textsearch"}, "paradedb"),
        ({"vector", "pg_search", "pg_textsearch"}, "paradedb"),
    ],
)
def test_enabled_extension_combinations_promoted_dialect_and_active_extensions(
    detected: set[str], expected_dialect: str
) -> None:
    """Verify dialect promotion hierarchy and active extension recording for all combinations."""
    features: dict[str, Any] = {"enable_pgvector": True, "enable_paradedb": True, "enable_pg_textsearch": True}
    config, pgv_avail, pdb_avail = resolve_postgres_extension_state(
        StatementConfig(dialect="postgres"), features, detected
    )
    assert config.dialect == expected_dialect
    assert features["active_extensions"] == detected
    assert pgv_avail is ("vector" in detected)
    assert pdb_avail is ("pg_search" in detected)
    assert is_postgres_extension_active(features, "pg_textsearch") is ("pg_textsearch" in detected)
    assert is_postgres_extension_active(features, "vector") is ("vector" in detected)
    assert is_postgres_extension_active(features, "pg_search") is ("pg_search" in detected)

    expressions = ["1"]
    if config.dialect == "paradedb":
        expressions.append("content @@@ 'query'")
        expressions.append("embedding <=> '[1,2,3]'")
    elif config.dialect == "pg_textsearch":
        expressions.append("content <@> 'query'")
        expressions.append("embedding <=> '[1,2,3]'")
    elif config.dialect == "pgvector":
        expressions.append("embedding <=> '[1,2,3]'")

    query = "SELECT " + ", ".join(expressions) + " FROM documents"
    rendered = parse_one(query, read=config.dialect).sql(dialect=config.dialect)
    for part in expressions[1:]:
        op = part.split()[1]
        assert op in rendered


def test_resolve_postgres_extension_state_preserves_explicit_dialect() -> None:
    """Verify an explicit non-postgres dialect is preserved even when other extensions are detected."""
    features: dict[str, Any] = {"enable_pgvector": True, "enable_paradedb": True, "enable_pg_textsearch": True}
    config = StatementConfig(dialect="pg_textsearch")
    detected = {"vector", "pg_search", "pg_textsearch"}

    updated_config, pgv_avail, pdb_avail = resolve_postgres_extension_state(config, features, detected)

    assert updated_config.dialect == "pg_textsearch"
    assert pgv_avail is True
    assert pdb_avail is True
    assert features["active_extensions"] == detected
    assert is_postgres_extension_active(features, "pg_textsearch") is True
