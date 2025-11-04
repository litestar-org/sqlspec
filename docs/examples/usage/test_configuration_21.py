"""Test configuration example: Basic migration configuration."""

import pytest


@pytest.mark.skipif(
    not pytest.importorskip("asyncpg", reason="AsyncPG not installed"), reason="AsyncPG integration tests disabled"
)
def test_basic_migration_config() -> None:
    """Test basic migration configuration."""
    from sqlspec.adapters.asyncpg import AsyncpgConfig

    config = AsyncpgConfig(
        pool_config={"dsn": "postgresql://localhost/db"},
        extension_config={
            "litestar": {"session_table": "custom_sessions"}  # Extension settings
        },
        migration_config={
            "script_location": "migrations",  # Migration directory
            "version_table": "alembic_version",  # Version tracking table
            "include_extensions": ["litestar"],  # Simple string list only
        },
    )

    assert config.migration_config["script_location"] == "migrations"
    assert "litestar" in config.migration_config["include_extensions"]
