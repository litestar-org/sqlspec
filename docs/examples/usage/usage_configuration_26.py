"""Test configuration example: Best practice - Enable caching."""

__all__ = ("test_enable_caching_best_practice",)


def test_enable_caching_best_practice() -> None:
    """Test caching best practice configuration."""
    from sqlspec import StatementConfig

    statement_config = StatementConfig(dialect="postgres", enable_caching=True)

    assert statement_config.enable_caching is True
    assert statement_config.dialect == "postgres"
