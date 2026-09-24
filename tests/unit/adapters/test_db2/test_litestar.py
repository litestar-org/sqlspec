"""Unit tests for Db2 Litestar extension integration."""

from unittest.mock import MagicMock

from sqlspec.adapters.db2.litestar import Db2LitestarConfig, Db2SyncStore


def test_db2_litestar_config() -> None:
    """Test Db2LitestarConfig type."""
    config = Db2LitestarConfig()
    assert isinstance(config, dict)


def test_db2_litestar_store() -> None:
    """Test Db2SyncStore initialization."""
    mock_config = MagicMock()
    mock_config.extension_config = {}
    store = Db2SyncStore(mock_config)
    assert store._config is mock_config
    assert "session_id VARCHAR(255)" in store._table_ddl()
    assert "BLOB(10M)" in store._table_ddl()
