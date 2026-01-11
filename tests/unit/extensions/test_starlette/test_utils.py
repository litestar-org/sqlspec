"""Test utility functions for Starlette extension."""

from unittest.mock import MagicMock

import pytest

from sqlspec.extensions.starlette import SQLSpecConfigState, get_connection_from_request, get_or_create_session


def test_get_connection_from_request() -> None:
    """Test get_connection_from_request retrieves connection from request state."""
    mock_request = MagicMock()
    mock_connection = MagicMock()
    mock_config = MagicMock()

    config_state = SQLSpecConfigState(
        config=mock_config,
        connection_key="db_connection",
        pool_key="db_pool",
        session_key="db_session",
        commit_mode="manual",
        extra_commit_statuses=None,
        extra_rollback_statuses=None,
        disable_di=False,
    )

    setattr(mock_request.state, "db_connection", mock_connection)

    result = get_connection_from_request(mock_request, config_state)

    assert result is mock_connection


def test_get_connection_from_request_raises_when_missing() -> None:
    """Test get_connection_from_request raises AttributeError when connection missing."""
    mock_request = MagicMock()
    mock_config = MagicMock()
    mock_request.state = MagicMock(spec=[])

    config_state = SQLSpecConfigState(
        config=mock_config,
        connection_key="db_connection",
        pool_key="db_pool",
        session_key="db_session",
        commit_mode="manual",
        extra_commit_statuses=None,
        extra_rollback_statuses=None,
        disable_di=False,
    )

    with pytest.raises(AttributeError):
        get_connection_from_request(mock_request, config_state)


def test_get_or_create_session_creates_new_session() -> None:
    """Test get_or_create_session creates new session when not cached."""
    mock_request = MagicMock()
    mock_connection = MagicMock()
    mock_config = MagicMock()
    mock_driver = MagicMock()

    mock_config.driver_type = mock_driver
    mock_config.statement_config = {"test": "config"}
    mock_config.driver_features = {"feature": True}

    config_state = SQLSpecConfigState(
        config=mock_config,
        connection_key="db_connection",
        pool_key="db_pool",
        session_key="db_session",
        commit_mode="manual",
        extra_commit_statuses=None,
        extra_rollback_statuses=None,
        disable_di=False,
    )

    setattr(mock_request.state, "db_connection", mock_connection)
    mock_request.state.db_session_instance = None

    session = get_or_create_session(mock_request, config_state)

    assert session is not None
    mock_driver.assert_called_once_with(
        connection=mock_connection, statement_config={"test": "config"}, driver_features={"feature": True}
    )


def test_get_or_create_session_returns_cached_session() -> None:
    """Test get_or_create_session returns cached session on second call."""
    mock_request = MagicMock()
    mock_connection = MagicMock()
    mock_config = MagicMock()
    mock_driver = MagicMock()
    mock_session = MagicMock()

    mock_config.driver_type = mock_driver
    mock_config.statement_config = {}
    mock_config.driver_features = {}

    config_state = SQLSpecConfigState(
        config=mock_config,
        connection_key="db_connection",
        pool_key="db_pool",
        session_key="db_session",
        commit_mode="manual",
        extra_commit_statuses=None,
        extra_rollback_statuses=None,
        disable_di=False,
    )

    setattr(mock_request.state, "db_connection", mock_connection)
    setattr(mock_request.state, "db_session_instance", mock_session)

    result = get_or_create_session(mock_request, config_state)

    assert result is mock_session
    mock_driver.assert_not_called()


def test_get_or_create_session_uses_unique_cache_key() -> None:
    """Test get_or_create_session uses unique cache key per session_key."""
    mock_request = MagicMock()
    mock_connection = MagicMock()
    mock_config = MagicMock()
    mock_driver = MagicMock()

    mock_config.driver_type = mock_driver
    mock_config.statement_config = {}
    mock_config.driver_features = {}

    config_state = SQLSpecConfigState(
        config=mock_config,
        connection_key="db_connection",
        pool_key="db_pool",
        session_key="custom_db",
        commit_mode="manual",
        extra_commit_statuses=None,
        extra_rollback_statuses=None,
        disable_di=False,
    )

    setattr(mock_request.state, "db_connection", mock_connection)

    session = get_or_create_session(mock_request, config_state)

    assert hasattr(mock_request.state, "custom_db_instance")
    assert session is not None
