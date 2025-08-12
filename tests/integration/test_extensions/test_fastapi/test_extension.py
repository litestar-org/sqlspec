"""Tests for SQLSpec FastAPI extension."""

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.extensions.fastapi import DatabaseConfig, SQLSpec


@pytest.fixture
def sqlite_config() -> SqliteConfig:
    """Create an in-memory SQLite configuration for testing."""
    return SqliteConfig(database=":memory:")


@pytest.fixture
def database_config(sqlite_config: SqliteConfig) -> DatabaseConfig:
    """Create a database configuration for testing."""
    return DatabaseConfig(config=sqlite_config)


@pytest.fixture
def sqlspec_extension(database_config: DatabaseConfig) -> SQLSpec:
    """Create a SQLSpec extension for testing."""
    return SQLSpec(config=database_config)


@pytest.fixture
def fastapi_app(sqlspec_extension: SQLSpec, database_config: DatabaseConfig) -> FastAPI:
    """Create a FastAPI application with SQLSpec configured."""
    app = FastAPI()

    # Initialize SQLSpec with the app
    sqlspec_extension.init_app(app)

    @app.get("/test")
    async def test_endpoint():
        """Test endpoint that uses the database connection."""
        # Check if connection is available in app state
        has_pool = hasattr(app.state, database_config.pool_key)
        return {"has_pool": has_pool}

    return app


def test_sqlspec_fastapi_initialization(fastapi_app: FastAPI, sqlspec_extension: SQLSpec):
    """Test that SQLSpec initializes properly with FastAPI."""
    assert sqlspec_extension._app == fastapi_app
    assert len(sqlspec_extension.config) == 1


def test_sqlspec_fastapi_configuration(database_config: DatabaseConfig):
    """Test database configuration properties."""
    assert database_config.connection_key == "db_connection"
    assert database_config.pool_key == "db_pool"
    assert database_config.session_key == "db_session"
    assert database_config.commit_mode == "manual"


def test_fastapi_app_with_middleware(fastapi_app: FastAPI):
    """Test that the FastAPI app works with SQLSpec middleware."""
    client = TestClient(fastapi_app)

    # Make a request to test the middleware
    response = client.get("/test")
    assert response.status_code == 200

    # The response should indicate the pool is available
    data = response.json()
    assert "has_pool" in data


def test_provide_session_context_manager(sqlspec_extension: SQLSpec, sqlite_config: SqliteConfig):
    """Test the provide_session context manager."""

    async def test_session():
        async with sqlspec_extension.provide_session(sqlite_config) as session:
            assert session is not None
            # Test that we can execute a simple query
            result = await session.execute("SELECT 1 as test")
            assert result is not None

    import asyncio

    asyncio.run(test_session())


def test_provide_connection_context_manager(sqlspec_extension: SQLSpec, sqlite_config: SqliteConfig):
    """Test the provide_connection context manager."""

    async def test_connection():
        async with sqlspec_extension.provide_connection(sqlite_config) as connection:
            assert connection is not None

    import asyncio

    asyncio.run(test_connection())


def test_multiple_database_configs():
    """Test SQLSpec with multiple database configurations."""
    config1 = DatabaseConfig(
        config=SqliteConfig(database=":memory:"), connection_key="db1_connection", pool_key="db1_pool"
    )
    config2 = DatabaseConfig(
        config=SqliteConfig(database=":memory:"), connection_key="db2_connection", pool_key="db2_pool"
    )

    sqlspec = SQLSpec(config=[config1, config2])
    app = FastAPI()
    sqlspec.init_app(app)

    assert len(sqlspec.config) == 2
    assert sqlspec.config[0].connection_key == "db1_connection"
    assert sqlspec.config[1].connection_key == "db2_connection"


def test_database_config_validation():
    """Test database configuration validation."""
    config = SqliteConfig(database=":memory:")

    # Test invalid commit mode
    with pytest.raises(Exception):  # Should raise ImproperConfigurationError
        DatabaseConfig(config=config, commit_mode="invalid")  # type: ignore[arg-type]

    # Test conflicting status codes
    with pytest.raises(Exception):  # Should raise ImproperConfigurationError
        DatabaseConfig(config=config, extra_commit_statuses={200, 201}, extra_rollback_statuses={200, 500})


def test_cli_integration():
    """Test CLI integration functions."""
    from sqlspec.extensions.fastapi.cli import register_database_commands

    app = FastAPI()

    # Register database commands
    db_group = register_database_commands(app)
    assert db_group.name == "db"

    # Check that commands were added
    assert len(db_group.commands) > 0
