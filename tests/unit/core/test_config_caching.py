from sqlspec.core.statement import get_default_config


def test_get_default_config_is_cached() -> None:
    config1 = get_default_config()
    config2 = get_default_config()

    # Current behavior: False (new instance)
    # Target behavior: True (same instance)
    assert config1 is config2


def test_default_config_immutability_check() -> None:
    # This test verifies if modifying the default config affects subsequent calls
    # If we share the instance, we must be careful.
    config1 = get_default_config()
    original_parsing = config1.enable_parsing

    try:
        config1.enable_parsing = not original_parsing
        config2 = get_default_config()
        # If shared, config2 sees the change
        assert config2.enable_parsing == config1.enable_parsing
    finally:
        # Restore state to not break other tests if running in same process
        config1.enable_parsing = original_parsing
