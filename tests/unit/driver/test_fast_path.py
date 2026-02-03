# pyright: reportPrivateImportUsage = false, reportPrivateUsage = false
"""Unit tests for fast-path query cache behavior."""

from sqlspec.driver._common import CachedQuery, _QueryCache


def test_query_cache_lru_eviction() -> None:
    cache = _QueryCache(max_size=2)

    cache.set("a", CachedQuery("SQL_A", (), 1))
    cache.set("b", CachedQuery("SQL_B", (), 1))
    assert cache.get("a") is not None

    cache.set("c", CachedQuery("SQL_C", (), 1))

    assert cache.get("b") is None
    assert cache.get("a") is not None
    assert cache.get("c") is not None


def test_query_cache_update_moves_to_end() -> None:
    cache = _QueryCache(max_size=2)

    cache.set("a", CachedQuery("SQL_A", (), 1))
    cache.set("b", CachedQuery("SQL_B", (), 1))
    cache.set("a", CachedQuery("SQL_A2", (), 2))
    cache.set("c", CachedQuery("SQL_C", (), 1))

    assert cache.get("b") is None
    entry = cache.get("a")
    assert entry is not None
    assert entry.driver_sql == "SQL_A2"
    assert entry.param_count == 2
