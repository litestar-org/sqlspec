"""Example 15: Configuration-Driven Processing."""

__all__ = ("test_configuration_driven_processing",)


def test_configuration_driven_processing() -> None:
    """Test StatementConfig for controlling pipeline behavior."""
    # start-example
    from sqlspec import ParameterStyle, ParameterStyleConfig, StatementConfig

    config = StatementConfig(
        dialect="postgres",
        enable_parsing=True,  # AST generation
        enable_validation=True,  # Security/performance checks
        enable_transformations=True,  # AST transformations
        enable_caching=True,  # Namespaced caching
        parameter_config=ParameterStyleConfig(
            default_parameter_style=ParameterStyle.NUMERIC, has_native_list_expansion=False
        ),
    )
    # end-example

    # Verify config was created
    assert config is not None
    assert config.dialect == "postgres"
    assert config.enable_parsing is True
