"""Tests for SQLSpec Flask extension."""

import pytest

from sqlspec.adapters.sqlite import SqliteConfig, SqliteConnectionParams
from sqlspec.extensions.flask import DatabaseConfig, FlaskServiceMixin, SQLSpec


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
def flask_app(sqlspec_extension: SQLSpec, database_config: DatabaseConfig):
    """Create a Flask application with SQLSpec configured."""
    try:
        from flask import Flask
    except ImportError:
        pytest.skip("Flask not available")

    app = Flask(__name__)

    # Initialize SQLSpec with the app
    sqlspec_extension.init_app(app)

    @app.route("/test")
    def test_endpoint():
        """Test endpoint that uses the database session."""
        session = sqlspec_extension.get_session()
        return {"has_session": session is not None}

    return app


def test_sqlspec_flask_initialization(flask_app, sqlspec_extension: SQLSpec):
    """Test that SQLSpec initializes properly with Flask."""
    assert sqlspec_extension._app == flask_app
    assert len(sqlspec_extension.config) == 1


def test_sqlspec_flask_configuration(database_config: DatabaseConfig):
    """Test database configuration properties."""
    assert database_config.connection_key == "db_connection"
    assert database_config.pool_key == "db_pool"
    assert database_config.session_key == "db_session"
    assert database_config.commit_mode == "manual"


def test_flask_app_with_extension(flask_app):
    """Test that the Flask app works with SQLSpec extension."""
    with flask_app.test_client() as client:
        # Test that the app starts without errors
        response = client.get("/test")
        assert response.status_code == 200

        # The response should indicate session is available
        data = response.get_json()
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
        from flask import Flask

        app = Flask(__name__)
        sqlspec.init_app(app)

        assert len(sqlspec.config) == 2
        assert sqlspec.config[0].connection_key == "db1_connection"
        assert sqlspec.config[1].connection_key == "db2_connection"
    except ImportError:
        pytest.skip("Flask not available")


def test_database_config_validation():
    """Test database configuration validation."""
    config = SqliteConfig(pool_config=SqliteConnectionParams(database=":memory:"))

    # Test invalid commit mode
    with pytest.raises(Exception):  # Should raise ImproperConfigurationError
        DatabaseConfig(config=config, commit_mode="invalid")  # type: ignore[arg-type]

    # Test conflicting status codes
    with pytest.raises(Exception):  # Should raise ImproperConfigurationError
        DatabaseConfig(config=config, extra_commit_statuses={200, 201}, extra_rollback_statuses={200, 500})


def test_flask_service_mixin():
    """Test FlaskServiceMixin functionality."""

    class TestService(FlaskServiceMixin):
        def get_data(self):
            return {"message": "test"}

    service = TestService()

    # Test without Flask context (should not crash)
    assert hasattr(service, "jsonify")

    try:
        from flask import Flask

        app = Flask(__name__)
        with app.app_context():
            # Test jsonify method
            response = service.jsonify({"test": "data"})
            assert response.status_code == 200
    except ImportError:
        pytest.skip("Flask not available")


def test_cli_integration():
    """Test CLI integration functions."""
    try:
        from sqlspec.extensions.flask.cli import database_group

        assert database_group.name == "db"
        # Check that commands were added
        assert len(database_group.commands) > 0
    except ImportError:
        pytest.skip("Flask not available")
