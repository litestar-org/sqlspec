"""Test configuration example: Best practice - Tune pool sizes."""

__all__ = ("test_disable_security_checks_best_practice", )


def test_disable_security_checks_best_practice() -> None:
    """Test disabling security checks when necessary."""

    from sqlspec import StatementConfig

    # Example: Disabling security checks for trusted internal queries
    statement_config = StatementConfig(
        dialect="postgres",
        enable_validation=False,  # Skip security checks
    )
    assert statement_config.enable_validation is False
