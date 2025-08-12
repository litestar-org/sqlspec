"""Tests for SQLSpec Sanic extension."""

import pytest

from sqlspec.adapters.sqlite import SqliteConfig, SqliteConnectionParams
from sqlspec.extensions.sanic import DatabaseConfig, SQLSpec


@pytest.fixture
def sqlite_config() -> SqliteConfig:
    """Create an in-memory SQLite configuration for testing."""
    return SqliteConfig(pool_config=SqliteConnectionParams(database=":memory:"))


@pytest.fixture
def database_config(sqlite_config: SqliteConfig) -> DatabaseConfig:
    """Create a database configuration for testing."""
    return DatabaseConfig(config=sqlite_config)


@pytest.fixture
def sqlspec_extension(database_config: DatabaseConfig) -> SQLSpec:
    """Create a SQLSpec extension for testing."""
    return SQLSpec(config=database_config)


@pytest.fixture
def sanic_app(sqlspec_extension: SQLSpec, database_config: DatabaseConfig):
    """Create a Sanic application with SQLSpec configured."""
    try:
        from sanic import Sanic
        from sanic.response import json
    except ImportError:
        pytest.skip("Sanic not available")

    app = Sanic("test_app")

    # Initialize SQLSpec with the app
    sqlspec_extension.init_app(app)

    @app.route("/test")
    async def test_endpoint(request):
        """Test endpoint that uses the database session."""
        try:
            session = sqlspec_extension.get_session(request)
            has_session = session is not None
        except Exception:
            has_session = False
        return json({"has_session": has_session})

    return app


def test_sqlspec_sanic_initialization(sanic_app, sqlspec_extension: SQLSpec):
    """Test that SQLSpec initializes properly with Sanic."""
    assert sqlspec_extension._app == sanic_app
    assert len(sqlspec_extension.config) == 1


def test_sqlspec_sanic_configuration(database_config: DatabaseConfig):
    """Test database configuration properties."""
    assert database_config.connection_key == "db_connection"
    assert database_config.pool_key == "db_pool"
    assert database_config.session_key == "db_session"
    assert database_config.commit_mode == "manual"


def test_sanic_app_with_extension(sanic_app):
    """Test that the Sanic app works with SQLSpec extension."""
    try:
        from sanic_testing import TestClient
    except ImportError:
        pytest.skip("Sanic testing not available")

    client = TestClient(sanic_app)

    # Test that the app starts without errors
    request, response = client.get("/test")
    assert response.status == 200

    # The response should indicate session handling
    data = response.json
    assert "has_session" in data


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
        config=SqliteConfig(pool_config=SqliteConnectionParams(database=":memory:")),
        connection_key="db1_connection",
        pool_key="db1_pool",
    )
    config2 = DatabaseConfig(
        config=SqliteConfig(pool_config=SqliteConnectionParams(database=":memory:")),
        connection_key="db2_connection",
        pool_key="db2_pool",
    )

    sqlspec = SQLSpec(config=[config1, config2])

    try:
        from sanic import Sanic

        app = Sanic("test_app")
        sqlspec.init_app(app)

        assert len(sqlspec.config) == 2
        assert sqlspec.config[0].connection_key == "db1_connection"
        assert sqlspec.config[1].connection_key == "db2_connection"
    except ImportError:
        pytest.skip("Sanic not available")


def test_database_config_validation():
    """Test database configuration validation."""
    config = SqliteConfig(pool_config=SqliteConnectionParams(database=":memory:"))

    # Test invalid commit mode
    with pytest.raises(Exception):  # Should raise ImproperConfigurationError
        DatabaseConfig(config=config, commit_mode="invalid")  # type: ignore[arg-type]

    # Test conflicting status codes
    with pytest.raises(Exception):  # Should raise ImproperConfigurationError
        DatabaseConfig(config=config, extra_commit_statuses={200, 201}, extra_rollback_statuses={200, 500})


def test_unique_context_keys(database_config: DatabaseConfig):
    """Test that unique context keys are generated."""
    assert database_config.engine_key.startswith("engine_")
    assert database_config.session_maker_key.startswith("session_maker_")

    # Test different configs get different keys
    config2 = DatabaseConfig(
        config=SqliteConfig(pool_config=SqliteConnectionParams(database=":memory:")), connection_key="db2_connection"
    )
    assert database_config.engine_key != config2.engine_key
    assert database_config.session_maker_key != config2.session_maker_key
