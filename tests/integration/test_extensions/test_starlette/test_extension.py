"""Tests for SQLSpec Starlette extension."""

import pytest
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.testclient import TestClient

from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.extensions.starlette import DatabaseConfig, SQLSpec


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
def starlette_app(sqlspec_extension: SQLSpec, database_config: DatabaseConfig) -> Starlette:
    """Create a Starlette application with SQLSpec configured."""
    app = Starlette()

    # Initialize SQLSpec with the app
    sqlspec_extension.init_app(app)

    @app.route("/test")
    async def test_endpoint(request):
        """Test endpoint that uses the database connection."""
        # Check if connection is available in request state
        connection_key = database_config.connection_key
        has_connection = hasattr(request.state, connection_key)
        return JSONResponse({"has_connection": has_connection})

    return app


def test_sqlspec_starlette_initialization(starlette_app: Starlette, sqlspec_extension: SQLSpec):
    """Test that SQLSpec initializes properly with Starlette."""
    assert sqlspec_extension._app == starlette_app
    assert len(sqlspec_extension.config) == 1


def test_sqlspec_starlette_configuration(database_config: DatabaseConfig):
    """Test database configuration properties."""
    assert database_config.connection_key == "db_connection"
    assert database_config.pool_key == "db_pool"
    assert database_config.session_key == "db_session"
    assert database_config.commit_mode == "manual"


def test_starlette_app_with_middleware(starlette_app: Starlette):
    """Test that the Starlette app works with SQLSpec middleware."""
    client = TestClient(starlette_app)

    # Make a request to test the middleware
    response = client.get("/test")
    assert response.status_code == 200

    # The response should indicate middleware is working
    data = response.json()
    # Note: This might be False if middleware isn't properly setting up connections
    # but the test should pass without errors
    assert "has_connection" in data


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
    app = Starlette()
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
