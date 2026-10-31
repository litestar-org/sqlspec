"""Unit tests for SQLSpec QueryBuilder with Sybase ASE and SAP SQL Anywhere dialects."""

import pytest

from sqlspec import sql
from sqlspec.core import get_cache
from sqlspec.exceptions import DialectNotSupportedError

__all__ = ()


def test_sybase_builder_select_limit_offset_and_optimizer_cache() -> None:
    """Verify QueryBuilder with dialect='sybase' renders ROWS LIMIT/OFFSET and populates QueryCache."""
    cache = get_cache()
    cache.clear()

    builder = (
        sql
        .select("id", "name", dialect="sybase")
        .from_("users")
        .where_eq("status", "active")
        .order_by("id")
        .limit(10)
        .offset(20)
    )
    first = builder.build()
    second = builder.build()

    assert "ROWS LIMIT" in first.sql
    assert "OFFSET" in first.sql
    assert first.sql == second.sql
    assert cache.get_stats().hits >= 1


def test_sqlanywhere_builder_select_limit_offset_and_optimizer_cache() -> None:
    """Verify QueryBuilder with dialect='sqlanywhere' renders LIMIT/OFFSET and populates QueryCache."""
    cache = get_cache()
    cache.clear()

    builder = (
        sql
        .select("id", "name", dialect="sqlanywhere")
        .from_("users")
        .where_eq("status", "active")
        .order_by("id")
        .limit(10)
        .offset(20)
    )
    first = builder.build()
    second = builder.build()

    assert "LIMIT" in first.sql
    assert "OFFSET" in first.sql
    assert "FETCH" not in first.sql
    assert first.sql == second.sql
    assert cache.get_stats().hits >= 1


def test_sybase_merge_raises_dialect_not_supported() -> None:
    """Verify sql.merge(dialect='sybase').build() raises DialectNotSupportedError while sqlanywhere is supported."""
    with pytest.raises(DialectNotSupportedError):
        (
            sql
            .merge(dialect="sybase")
            .into("users", alias="t")
            .using("staging_users", alias="s")
            .on("t.id = s.id")
            .when_matched_then_update(name="s.name")
            .build()
        )

    merge_builder = (
        sql
        .merge(dialect="sqlanywhere")
        .into("users", alias="t")
        .using("staging_users", alias="s")
        .on("t.id = s.id")
        .when_matched_then_update(name="s.name")
        .when_not_matched_then_insert(id="s.id", name="s.name")
    )
    compiled = merge_builder.build()
    assert "MERGE INTO" in compiled.sql
