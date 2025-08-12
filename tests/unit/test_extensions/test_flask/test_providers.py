"""Tests for SQLSpec Flask provider functions."""

from unittest.mock import AsyncMock, Mock, patch

import pytest

from sqlspec.adapters.sqlite import SqliteConfig, SqliteConnectionParams
from sqlspec.extensions.flask._providers import (
    create_connection_provider,
    create_pool_provider,
    create_session_provider,
)

# Test fixtures


@pytest.fixture
def mock_config() -> Mock:
    """Create a mock database configuration."""
    config = Mock()
    config.create_pool = AsyncMock(return_value="mock_pool")
    config.close_pool = AsyncMock()
    config.provide_connection = Mock()
    config.driver_type = Mock()
    return config


@pytest.fixture
def sqlite_config() -> SqliteConfig:
    """Create a real SQLite configuration for testing."""
    return SqliteConfig(pool_config=SqliteConnectionParams(database=":memory:"))


# Basic provider creation tests


def test_create_pool_provider(mock_config: Mock) -> None:
    """Test pool provider creation returns sync function for Flask."""
    pool_provider = create_pool_provider(mock_config, "test_pool_key")

    assert callable(pool_provider)

    # For Flask, this should return a sync function that uses portal
    # Test the provider function returns expected result
    result = pool_provider()
    # The result should be whatever the portal.call returns
    assert result is not None


def test_create_connection_provider(mock_config: Mock) -> None:
    """Test connection provider creation returns sync function for Flask."""
    # Setup mock connection context manager
    mock_connection = Mock()
    mock_cm = AsyncMock()
    mock_cm.__aenter__ = AsyncMock(return_value=mock_connection)
    mock_cm.__aexit__ = AsyncMock()
    mock_config.provide_connection.return_value = mock_cm

    connection_provider = create_connection_provider(mock_config, "test_pool_key", "test_connection_key")

    assert callable(connection_provider)

    # For Flask, this should return a sync function that uses portal
    result = connection_provider()
    assert result is not None


def test_create_session_provider(mock_config: Mock) -> None:
    """Test session provider creation returns sync function for Flask."""
    mock_connection = Mock()
    mock_driver = Mock()
    mock_config.driver_type.return_value = mock_driver

    session_provider = create_session_provider(mock_config, "test_connection_key")

    assert callable(session_provider)

    # For Flask, this should return a sync function that uses portal
    result = session_provider(mock_connection)
    assert result is not None


# Portal integration tests


@patch("sqlspec.extensions.flask._providers.PortalProvider")
def test_pool_provider_uses_portal(mock_portal_class: Mock, mock_config: Mock) -> None:
    """Test that pool provider uses portal for sync operation."""
    # Setup mock portal
    mock_portal = Mock()
    mock_portal.is_running = False
    mock_portal.start = Mock()
    mock_portal.call = Mock(return_value="mock_pool")
    mock_portal_class.return_value = mock_portal

    # Create the provider
    pool_provider = create_pool_provider(mock_config, "test_pool")

    # Call the provider (which should be the sync wrapper)
    result = pool_provider()

    assert result == "mock_pool"
    mock_portal_class.assert_called_once()
    mock_portal.start.assert_called_once()
    mock_portal.call.assert_called_once()


@patch("sqlspec.extensions.flask._providers.PortalProvider")
def test_connection_provider_uses_portal(mock_portal_class: Mock, mock_config: Mock) -> None:
    """Test that connection provider uses portal for sync operation."""
    # Setup mock portal
    mock_portal = Mock()
    mock_portal.is_running = True  # Already running
    mock_portal.start = Mock()
    mock_portal.call = Mock(return_value="mock_connection")
    mock_portal_class.return_value = mock_portal

    # Create the provider
    connection_provider = create_connection_provider(mock_config, "pool", "conn")

    # Call the provider
    result = connection_provider()

    assert result == "mock_connection"
    mock_portal_class.assert_called_once()
    mock_portal.start.assert_not_called()  # Already running
    mock_portal.call.assert_called_once()


@patch("sqlspec.extensions.flask._providers.PortalProvider")
def test_session_provider_uses_portal(mock_portal_class: Mock, mock_config: Mock) -> None:
    """Test that session provider uses portal for sync operation."""
    # Setup mock portal
    mock_portal = Mock()
    mock_portal.is_running = False
    mock_portal.start = Mock()
    mock_portal.call = Mock(return_value="mock_session")
    mock_portal_class.return_value = mock_portal

    mock_connection = Mock()

    # Create the provider
    session_provider = create_session_provider(mock_config, "conn")

    # Call the provider
    result = session_provider(mock_connection)

    assert result == "mock_session"
    mock_portal_class.assert_called_once()
    mock_portal.start.assert_called_once()
    mock_portal.call.assert_called_once()


# Error handling tests


@patch("sqlspec.extensions.flask._providers.PortalProvider")
def test_provider_error_handling(mock_portal_class: Mock, mock_config: Mock) -> None:
    """Test provider error handling."""
    # Setup portal to raise exception
    mock_portal = Mock()
    mock_portal.is_running = False
    mock_portal.start = Mock()
    mock_portal.call = Mock(side_effect=Exception("Portal error"))
    mock_portal_class.return_value = mock_portal

    pool_provider = create_pool_provider(mock_config, "test_pool")

    with pytest.raises(Exception, match="Portal error"):
        pool_provider()


# Type annotation tests


def test_pool_provider_type_hints() -> None:
    """Test that pool provider has correct type hints."""
    import inspect

    # Get the function signature
    sig = inspect.signature(create_pool_provider)

    # Check parameter types
    assert "config" in sig.parameters
    assert "pool_key" in sig.parameters

    # The return type should be a callable
    provider = create_pool_provider(Mock(), "test")
    assert callable(provider)


def test_connection_provider_type_hints() -> None:
    """Test that connection provider has correct type hints."""
    import inspect

    sig = inspect.signature(create_connection_provider)

    assert "config" in sig.parameters
    assert "pool_key" in sig.parameters
    assert "connection_key" in sig.parameters

    provider = create_connection_provider(Mock(), "pool", "conn")
    assert callable(provider)


def test_session_provider_type_hints() -> None:
    """Test that session provider has correct type hints."""
    import inspect

    sig = inspect.signature(create_session_provider)

    assert "config" in sig.parameters
    assert "connection_key" in sig.parameters

    provider = create_session_provider(Mock(), "conn")
    assert callable(provider)
