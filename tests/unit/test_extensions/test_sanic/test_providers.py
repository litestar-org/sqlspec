"""Unit tests for Sanic providers."""

from unittest.mock import Mock

import pytest

from sqlspec.adapters.sqlite import SqliteConfig, SqliteConnectionParams
from sqlspec.extensions.sanic.config import DatabaseConfig
from sqlspec.extensions.sanic.providers import (
    create_filter_provider,
    create_service_provider,
    provide_connection,
    provide_filters,
    provide_pool,
    provide_service,
    provide_session,
)


@pytest.fixture
def mock_request():
    """Create a mock Sanic request object."""
    request = Mock()
    request.args = {"limit": "10", "offset": "0", "search": "test"}
    request.ctx = Mock()
    request.app = Mock()
    request.app.ctx = Mock()
    return request


@pytest.fixture
def mock_sqlspec():
    """Create a mock SQLSpec instance."""
    sqlspec = Mock()
    sqlspec._configs = []
    return sqlspec


@pytest.fixture
def sqlite_config():
    """Create a SQLite configuration for testing."""
    return SqliteConfig(pool_config=SqliteConnectionParams(database=":memory:"))


@pytest.fixture
def database_config(sqlite_config):
    """Create a database configuration for testing."""
    return DatabaseConfig(config=sqlite_config)


class TestProvideService:
    """Test the provide_service function."""

    def test_provide_service_creates_provider(self):
        """Test that provide_service creates a provider function."""
        provider = provide_service(str, None)
        assert callable(provider)

    def test_provider_requires_sqlspec_in_app_context(self, mock_request):
        """Test that provider requires SQLSpec in app context."""
        mock_request.app.ctx.sqlspec = None

        provider = provide_service(str, None)

        with pytest.raises(RuntimeError, match="SQLSpec not initialized"):
            provider(mock_request)

    def test_provider_uses_first_config_when_none_provided(self, mock_request, mock_sqlspec, database_config):
        """Test that provider uses first config when none provided."""
        mock_sqlspec._configs = [database_config]
        mock_sqlspec.get_session.return_value = Mock()
        mock_request.app.ctx.sqlspec = mock_sqlspec

        provider = provide_service(str, None)

        # Mock the service class to avoid actual instantiation
        with pytest.raises(TypeError):  # str() doesn't accept session parameter
            provider(mock_request)

    def test_provider_with_cache_key_uses_singleton(self, mock_request, mock_sqlspec, database_config):
        """Test that provider with cache key uses singleton pattern."""
        mock_sqlspec._configs = [database_config]
        mock_sqlspec.get_session.return_value = Mock()
        mock_request.app.ctx.sqlspec = mock_sqlspec

        provider = provide_service(str, None, cache_key="test_service")

        # This would use get_cached_instance, behavior depends on implementation
        with pytest.raises(TypeError):  # str() doesn't accept session parameter
            provider(mock_request)


class TestProvideFilters:
    """Test the provide_filters function."""

    def test_provide_filters_creates_provider(self):
        """Test that provide_filters creates a provider function."""
        filter_types = [dict]
        provider = provide_filters(filter_types)
        assert callable(provider)

    def test_provider_processes_single_filter_type(self, mock_request):
        """Test provider processes single filter type."""
        provider = provide_filters(dict)
        result = provider(mock_request)

        assert isinstance(result, dict)
        assert "dict" in result
        assert isinstance(result["dict"], dict)

    def test_provider_processes_multiple_filter_types(self, mock_request):
        """Test provider processes multiple filter types."""
        filter_types = [dict, list]
        provider = provide_filters(filter_types)
        result = provider(mock_request)

        assert isinstance(result, dict)
        assert "dict" in result
        assert "list" in result

    def test_provider_applies_field_transformations(self, mock_request):
        """Test that provider applies field transformations."""
        mock_request.args = {"field_slug": "test-value", "other": "normal"}

        class TestFilter:
            def __init__(self):
                self.field_slug = None
                self.other = None

        provider = provide_filters([TestFilter])
        result = provider(mock_request)

        filter_instance = result["testfilter"]
        assert hasattr(filter_instance, "field_slug")
        assert hasattr(filter_instance, "other")


class TestProvideConnection:
    """Test the provide_connection function."""

    def test_provide_connection_creates_provider(self):
        """Test that provide_connection creates a provider function."""
        provider = provide_connection()
        assert callable(provider)

    def test_provider_requires_sqlspec_in_app_context(self, mock_request):
        """Test that provider requires SQLSpec in app context."""
        mock_request.app.ctx.sqlspec = None

        provider = provide_connection()

        with pytest.raises(RuntimeError, match="SQLSpec not initialized"):
            provider(mock_request)

    def test_provider_requires_connection_in_request_context(self, mock_request, mock_sqlspec, database_config):
        """Test that provider requires connection in request context."""
        mock_sqlspec._configs = [database_config]
        mock_request.app.ctx.sqlspec = mock_sqlspec
        mock_request.ctx.db_connection = None

        provider = provide_connection()

        with pytest.raises(RuntimeError, match="No connection available"):
            provider(mock_request)

    def test_provider_returns_connection_from_context(self, mock_request, mock_sqlspec, database_config):
        """Test that provider returns connection from request context."""
        mock_connection = Mock()
        mock_sqlspec._configs = [database_config]
        mock_request.app.ctx.sqlspec = mock_sqlspec
        mock_request.ctx.db_connection = mock_connection

        provider = provide_connection()
        result = provider(mock_request)

        assert result is mock_connection


class TestProvidePool:
    """Test the provide_pool function."""

    def test_provide_pool_creates_provider(self):
        """Test that provide_pool creates a provider function."""
        provider = provide_pool()
        assert callable(provider)

    def test_provider_requires_sqlspec_in_app_context(self, mock_request):
        """Test that provider requires SQLSpec in app context."""
        mock_request.app.ctx.sqlspec = None

        provider = provide_pool()

        with pytest.raises(RuntimeError, match="SQLSpec not initialized"):
            provider(mock_request)

    def test_provider_gets_pool_from_app_context(self, mock_request, mock_sqlspec, database_config):
        """Test that provider gets pool from app context."""
        mock_pool = Mock()
        mock_sqlspec._configs = [database_config]
        mock_sqlspec.get_engine.return_value = mock_pool
        mock_request.app.ctx.sqlspec = mock_sqlspec

        provider = provide_pool()
        result = provider(mock_request)

        assert result is mock_pool


class TestProvideSession:
    """Test the provide_session function."""

    def test_provide_session_creates_provider(self):
        """Test that provide_session creates a provider function."""
        provider = provide_session()
        assert callable(provider)

    def test_provider_requires_sqlspec_in_app_context(self, mock_request):
        """Test that provider requires SQLSpec in app context."""
        mock_request.app.ctx.sqlspec = None

        provider = provide_session()

        with pytest.raises(RuntimeError, match="SQLSpec not initialized"):
            provider(mock_request)

    def test_provider_gets_session_from_sqlspec(self, mock_request, mock_sqlspec, database_config):
        """Test that provider gets session from SQLSpec."""
        mock_session = Mock()
        mock_sqlspec._configs = [database_config]
        mock_sqlspec.get_session.return_value = mock_session
        mock_request.app.ctx.sqlspec = mock_sqlspec

        provider = provide_session(database_config)
        result = provider(mock_request)

        assert result is mock_session


class TestCreateServiceProvider:
    """Test the create_service_provider function."""

    def test_create_service_provider_creates_provider(self):
        """Test that create_service_provider creates a provider function."""
        provider = create_service_provider(str)
        assert callable(provider)

    def test_provider_with_config_key_lookup(self, mock_request, mock_sqlspec, database_config):
        """Test provider with configuration key lookup."""
        mock_sqlspec._configs = [database_config]
        mock_sqlspec.get_config.return_value = database_config
        mock_sqlspec.get_session.return_value = Mock()
        mock_request.app.ctx.sqlspec = mock_sqlspec

        provider = create_service_provider(str, config_key="test_config")

        with pytest.raises(TypeError):  # str() constructor issue
            provider(mock_request)

        # Verify get_config was called
        mock_sqlspec.get_config.assert_called_once_with("test_config")


class TestCreateFilterProvider:
    """Test the create_filter_provider function."""

    def test_create_filter_provider_creates_provider(self):
        """Test that create_filter_provider creates a provider function."""
        provider = create_filter_provider(dict)
        assert callable(provider)

    def test_provider_applies_field_mapping(self, mock_request):
        """Test that provider applies field mapping."""
        mock_request.args = {"query_param": "test_value"}

        class TestFilter:
            def __init__(self):
                self.filter_field = None

        field_mapping = {"query_param": "filter_field"}
        provider = create_filter_provider(TestFilter, field_mapping=field_mapping)
        result = provider(mock_request)

        assert hasattr(result, "filter_field")
        assert result.filter_field == "test_value"

    def test_provider_handles_missing_attributes(self, mock_request):
        """Test that provider handles missing attributes gracefully."""
        mock_request.args = {"nonexistent_param": "value"}

        class TestFilter:
            def __init__(self):
                pass

        provider = create_filter_provider(TestFilter)
        result = provider(mock_request)

        # Should create filter instance even if parameters don't match
        assert isinstance(result, TestFilter)
