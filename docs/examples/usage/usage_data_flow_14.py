"""Example 14: Multi-Tier Caching."""

__all__ = ("test_multi_tier_caching",)


def test_multi_tier_caching() -> None:
    """Test cache types in SQLSpec."""
    # start-example
    from sqlglot import Expression

    # Cache types and their purposes:
    sql_cache: dict[str, str] = {}  # Compiled SQL strings
    optimized_cache: dict[str, Expression] = {}  # Post-optimization AST
    # end-example

    # Verify cache dictionaries were created
    assert isinstance(sql_cache, dict)
    assert isinstance(optimized_cache, dict)
