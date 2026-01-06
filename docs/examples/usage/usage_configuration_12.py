import pytest

pytestmark = pytest.mark.xdist_group("postgres")

__all__ = ("test_basic_statement_config",)


def test_basic_statement_config() -> None:

    # start-example
    import os

    from sqlspec import StatementConfig
    from sqlspec.adapters.asyncpg import AsyncpgConfig

    statement_config = StatementConfig(
        dialect="postgres",  # SQLGlot dialect
        enable_parsing=True,  # Parse SQL into AST
        enable_validation=True,  # Run security/performance validators
        enable_transformations=True,  # Apply AST transformations
        enable_caching=True,  # Enable namespaced caching
    )

    # Apply to adapter
    dsn = os.getenv("SQLSPEC_USAGE_PG_DSN", "postgresql://localhost/db")
    config = AsyncpgConfig(connection_config={"dsn": dsn}, statement_config=statement_config)
    # end-example
    assert config.statement_config.dialect == "postgres"
    assert config.statement_config.enable_parsing is True
