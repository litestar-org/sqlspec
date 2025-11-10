def test_basic_statement_config() -> None:
__all__ = ("test_basic_statement_config", )


    import os

    from sqlspec import StatementConfig
    from sqlspec.adapters.asyncpg import AsyncpgConfig

    statement_config = StatementConfig(
        dialect="postgres",  # SQLGlot dialect
        enable_parsing=True,  # Parse SQL into AST
        enable_validation=True,  # Run security/performance validators
        enable_transformations=True,  # Apply AST transformations
        enable_caching=True,  # Enable multi-tier caching
    )

    # Apply to adapter
    dsn = os.getenv("SQLSPEC_USAGE_PG_DSN", "postgresql://localhost/db")
    config = AsyncpgConfig(pool_config={"dsn": dsn}, statement_config=statement_config)
    assert config.statement_config.dialect == "postgres"
    assert config.statement_config.enable_parsing is True
