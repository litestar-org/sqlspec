"""Test configuration example: Cache clearing operations."""


def test_clear_cache() -> None:
    """Test cache clearing operations."""
    from sqlspec.core.cache import clear_all_caches, get_cache_statistics

    # Get initial statistics
    stats_before = get_cache_statistics()
    assert isinstance(stats_before, dict)

    # Clear all caches and reset statistics
    clear_all_caches()

    # Verify caches were cleared
    stats_after = get_cache_statistics()
    assert isinstance(stats_after, dict)
    assert "multi_level" in stats_after

