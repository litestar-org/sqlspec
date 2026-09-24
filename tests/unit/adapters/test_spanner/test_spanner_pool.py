"""Unit tests for Spanner session pool configuration and multiplexed pooling."""

from unittest.mock import MagicMock, patch

from google.cloud.spanner_v1.pool import BurstyPool

from sqlspec.adapters.spanner.config import SpannerSyncConfig


def test_multiplexed_session_pool_default() -> None:
    """Verify that default SpannerSyncConfig uses multiplexed pooling and omits pool from database args."""
    config = SpannerSyncConfig(connection_config={"project": "p", "instance_id": "i", "database_id": "d"})
    assert config.connection_config.get("pool_type") is None

    mock_db = MagicMock()
    mock_instance = MagicMock()
    mock_instance.database.return_value = mock_db
    mock_client = MagicMock()
    mock_client.instance.return_value = mock_instance

    with patch.object(config, "_get_client", return_value=mock_client):
        db = config.get_database()

    assert db is mock_db
    mock_instance.database.assert_called_once()
    _, kwargs = mock_instance.database.call_args
    assert "pool" not in kwargs


def test_explicit_pool_type_preserved() -> None:
    """Verify that explicit pool_type in connection_config is instantiated and forwarded."""
    config = SpannerSyncConfig(
        connection_config={"project": "p", "instance_id": "i", "database_id": "d", "pool_type": BurstyPool}
    )
    assert config.connection_config.get("pool_type") is BurstyPool

    mock_db = MagicMock()
    mock_instance = MagicMock()
    mock_instance.database.return_value = mock_db
    mock_client = MagicMock()
    mock_client.instance.return_value = mock_instance

    with patch.object(config, "_get_client", return_value=mock_client):
        db = config.get_database()

    assert db is mock_db
    mock_instance.database.assert_called_once()
    _, kwargs = mock_instance.database.call_args
    assert "pool" in kwargs
    assert isinstance(kwargs["pool"], BurstyPool)


def test_disable_multiplexed_sessions_uses_legacy_pool() -> None:
    """Verify that enable_multiplexed_sessions=False constructs an explicit session pool."""
    config = SpannerSyncConfig(
        connection_config={"project": "p", "instance_id": "i", "database_id": "d", "enable_multiplexed_sessions": False}
    )

    mock_db = MagicMock()
    mock_instance = MagicMock()
    mock_instance.database.return_value = mock_db
    mock_client = MagicMock()
    mock_client.instance.return_value = mock_instance

    with patch.object(config, "_get_client", return_value=mock_client):
        db = config.get_database()

    assert db is mock_db
    mock_instance.database.assert_called_once()
    _, kwargs = mock_instance.database.call_args
    assert "pool" in kwargs
