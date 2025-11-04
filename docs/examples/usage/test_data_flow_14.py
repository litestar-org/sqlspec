"""Example 14: Multi-Tier Caching."""


def test_multi_tier_caching() -> None:
    """Test cache types in SQLSpec."""
    # start-example
    from sqlglot import Expression

    # Cache types and their purposes:
    sql_cache: dict[str, str] = {}             # Compiled SQL strings
    optimized_cache: dict[str, Expression] = {}  # Post-optimization AST
    builder_cache: dict[str, bytes] = {}       # QueryBuilder serialization
    file_cache: dict[str, dict] = {}   # File loading with checksums
    analysis_cache: dict[str, object] = {}         # Pipeline step results
    # end-example

    # Verify cache dictionaries were created
    assert isinstance(sql_cache, dict)
    assert isinstance(optimized_cache, dict)

