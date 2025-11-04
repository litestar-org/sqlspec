def test_basic_statement_config() -> None:
    from sqlspec.adapters.asyncpg import AsyncpgConfig
    from sqlspec.core.statement import StatementConfig

    statement_config = StatementConfig(
        dialect="postgres",  # SQLGlot dialect
        enable_parsing=True,  # Parse SQL into AST
        enable_validation=True,  # Run security/performance validators
        enable_transformations=True,  # Apply AST transformations
        enable_caching=True,  # Enable multi-tier caching
    )

    # Apply to adapter
    config = AsyncpgConfig(pool_config={"dsn": "postgresql://localhost/db"}, statement_config=statement_config)
    assert config.statement_config.dialect == "postgres"
    assert config.statement_config.enable_parsing is True
