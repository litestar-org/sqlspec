"""Unit tests for SQLSpec session backend."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from sqlspec.extensions.litestar.session import SQLSpecSessionBackend, SQLSpecSessionConfig


@pytest.fixture
def mock_store() -> MagicMock:
    """Create a mock Litestar Store."""
    store = MagicMock()
    store.get = AsyncMock()
    store.set = AsyncMock()
    store.delete = AsyncMock()
    store.exists = AsyncMock()
    store.delete_all = AsyncMock()
    return store


@pytest.fixture
def session_config() -> SQLSpecSessionConfig:
    """Create a session config instance."""
    return SQLSpecSessionConfig()


@pytest.fixture
def session_backend(session_config: SQLSpecSessionConfig) -> SQLSpecSessionBackend:
    """Create a session backend instance."""
    return SQLSpecSessionBackend(config=session_config)


def test_sqlspec_session_config_defaults() -> None:
    """Test SQLSpecSessionConfig default values."""
    config = SQLSpecSessionConfig()

    # Test inherited ServerSideSessionConfig defaults
    assert config.key == "session"
    assert config.max_age == 1209600  # 14 days
    assert config.path == "/"
    assert config.domain is None
    assert config.secure is False
    assert config.httponly is True
    assert config.samesite == "lax"
    assert config.exclude is None
    assert config.exclude_opt_key == "skip_session"
    assert config.scopes == frozenset({"http", "websocket"})

    # Test SQLSpec-specific defaults
    assert config.table_name == "litestar_sessions"
    assert config.session_id_column == "session_id"
    assert config.data_column == "data"
    assert config.expires_at_column == "expires_at"
    assert config.created_at_column == "created_at"

    # Test backend class is set correctly
    assert config.backend_class is SQLSpecSessionBackend


def test_sqlspec_session_config_custom_values() -> None:
    """Test SQLSpecSessionConfig with custom values."""
    config = SQLSpecSessionConfig(
        key="custom_session",
        max_age=3600,
        table_name="custom_sessions",
        session_id_column="id",
        data_column="payload",
        expires_at_column="expires",
        created_at_column="created",
    )

    # Test inherited config
    assert config.key == "custom_session"
    assert config.max_age == 3600

    # Test SQLSpec-specific config
    assert config.table_name == "custom_sessions"
    assert config.session_id_column == "id"
    assert config.data_column == "payload"
    assert config.expires_at_column == "expires"
    assert config.created_at_column == "created"


def test_session_backend_init(session_config: SQLSpecSessionConfig) -> None:
    """Test SQLSpecSessionBackend initialization."""
    backend = SQLSpecSessionBackend(config=session_config)

    assert backend.config is session_config
    assert isinstance(backend.config, SQLSpecSessionConfig)


@pytest.mark.asyncio
async def test_get_session_data_found(session_backend: SQLSpecSessionBackend, mock_store: MagicMock) -> None:
    """Test getting session data when session exists and data is dict/list."""
    session_id = "test_session_123"
    stored_data = {"user_id": 456, "username": "testuser"}

    mock_store.get.return_value = stored_data

    result = await session_backend.get(session_id, mock_store)

    # The data should be JSON-serialized to bytes
    expected_bytes = b'{"user_id":456,"username":"testuser"}'
    assert result == expected_bytes

    # Should call store.get with renew_for=None since renew_on_access is False by default
    mock_store.get.assert_called_once_with(session_id, renew_for=None)


@pytest.mark.asyncio
async def test_get_session_data_already_bytes(session_backend: SQLSpecSessionBackend, mock_store: MagicMock) -> None:
    """Test getting session data when store returns bytes directly."""
    session_id = "test_session_123"
    stored_bytes = b'{"user_id": 456, "username": "testuser"}'

    mock_store.get.return_value = stored_bytes

    result = await session_backend.get(session_id, mock_store)

    # Should return bytes as-is
    assert result == stored_bytes

    # Should call store.get with renew_for=None since renew_on_access is False by default
    mock_store.get.assert_called_once_with(session_id, renew_for=None)


@pytest.mark.asyncio
async def test_get_session_not_found(session_backend: SQLSpecSessionBackend, mock_store: MagicMock) -> None:
    """Test getting session data when session doesn't exist."""
    session_id = "nonexistent_session"

    mock_store.get.return_value = None

    result = await session_backend.get(session_id, mock_store)

    assert result is None
    # Should call store.get with renew_for=None since renew_on_access is False by default
    mock_store.get.assert_called_once_with(session_id, renew_for=None)


@pytest.mark.asyncio
async def test_get_session_with_renew_enabled() -> None:
    """Test getting session data when renew_on_access is enabled."""
    config = SQLSpecSessionConfig(renew_on_access=True)
    backend = SQLSpecSessionBackend(config=config)
    mock_store = MagicMock()
    mock_store.get = AsyncMock(return_value={"data": "test"})

    session_id = "test_session_123"

    await backend.get(session_id, mock_store)

    # Should call store.get with max_age when renew_on_access is True
    expected_max_age = int(backend.config.max_age)
    mock_store.get.assert_called_once_with(session_id, renew_for=expected_max_age)


@pytest.mark.asyncio
async def test_get_session_with_no_max_age() -> None:
    """Test getting session data when max_age is None."""
    config = SQLSpecSessionConfig()
    # Directly manipulate the dataclass field
    object.__setattr__(config, "max_age", None)
    backend = SQLSpecSessionBackend(config=config)
    mock_store = MagicMock()
    mock_store.get = AsyncMock(return_value={"data": "test"})

    session_id = "test_session_123"

    await backend.get(session_id, mock_store)

    # Should call store.get with renew_for=None when max_age is None
    mock_store.get.assert_called_once_with(session_id, renew_for=None)


@pytest.mark.asyncio
async def test_set_session_data(session_backend: SQLSpecSessionBackend, mock_store: MagicMock) -> None:
    """Test setting session data."""
    session_id = "test_session_123"
    # Litestar sends JSON bytes to the backend
    session_data_bytes = b'{"user_id": 789, "username": "newuser"}'

    await session_backend.set(session_id, session_data_bytes, mock_store)

    # Should deserialize the bytes and pass Python object to store
    expected_data = {"user_id": 789, "username": "newuser"}
    expected_expires_in = int(session_backend.config.max_age)

    mock_store.set.assert_called_once_with(session_id, expected_data, expires_in=expected_expires_in)


@pytest.mark.asyncio
async def test_set_session_data_with_no_max_age() -> None:
    """Test setting session data when max_age is None."""
    config = SQLSpecSessionConfig()
    # Directly manipulate the dataclass field
    object.__setattr__(config, "max_age", None)
    backend = SQLSpecSessionBackend(config=config)
    mock_store = MagicMock()
    mock_store.set = AsyncMock()

    session_id = "test_session_123"
    session_data_bytes = b'{"user_id": 789}'

    await backend.set(session_id, session_data_bytes, mock_store)

    # Should call store.set with expires_in=None when max_age is None
    expected_data = {"user_id": 789}
    mock_store.set.assert_called_once_with(session_id, expected_data, expires_in=None)


@pytest.mark.asyncio
async def test_set_session_data_complex_types(session_backend: SQLSpecSessionBackend, mock_store: MagicMock) -> None:
    """Test setting session data with complex data types."""
    session_id = "test_session_complex"
    # Complex JSON data with nested objects and lists
    complex_data_bytes = (
        b'{"user": {"id": 123, "roles": ["admin", "user"]}, "settings": {"theme": "dark", "notifications": true}}'
    )

    await session_backend.set(session_id, complex_data_bytes, mock_store)

    expected_data = {
        "user": {"id": 123, "roles": ["admin", "user"]},
        "settings": {"theme": "dark", "notifications": True},
    }
    expected_expires_in = int(session_backend.config.max_age)

    mock_store.set.assert_called_once_with(session_id, expected_data, expires_in=expected_expires_in)


@pytest.mark.asyncio
async def test_delete_session(session_backend: SQLSpecSessionBackend, mock_store: MagicMock) -> None:
    """Test deleting a session."""
    session_id = "test_session_to_delete"

    await session_backend.delete(session_id, mock_store)

    mock_store.delete.assert_called_once_with(session_id)


@pytest.mark.asyncio
async def test_get_store_exception(session_backend: SQLSpecSessionBackend, mock_store: MagicMock) -> None:
    """Test that store exceptions propagate correctly on get."""
    session_id = "test_session_123"
    mock_store.get.side_effect = Exception("Store connection failed")

    with pytest.raises(Exception, match="Store connection failed"):
        await session_backend.get(session_id, mock_store)


@pytest.mark.asyncio
async def test_set_store_exception(session_backend: SQLSpecSessionBackend, mock_store: MagicMock) -> None:
    """Test that store exceptions propagate correctly on set."""
    session_id = "test_session_123"
    session_data_bytes = b'{"user_id": 123}'
    mock_store.set.side_effect = Exception("Store write failed")

    with pytest.raises(Exception, match="Store write failed"):
        await session_backend.set(session_id, session_data_bytes, mock_store)


@pytest.mark.asyncio
async def test_delete_store_exception(session_backend: SQLSpecSessionBackend, mock_store: MagicMock) -> None:
    """Test that store exceptions propagate correctly on delete."""
    session_id = "test_session_123"
    mock_store.delete.side_effect = Exception("Store delete failed")

    with pytest.raises(Exception, match="Store delete failed"):
        await session_backend.delete(session_id, mock_store)


@pytest.mark.asyncio
async def test_set_invalid_json_bytes(session_backend: SQLSpecSessionBackend, mock_store: MagicMock) -> None:
    """Test setting session data with invalid JSON bytes."""
    session_id = "test_session_123"
    invalid_json_bytes = b'{"invalid": json, data}'

    with pytest.raises(Exception):  # JSON decode error should propagate
        await session_backend.set(session_id, invalid_json_bytes, mock_store)


def test_config_backend_class_assignment() -> None:
    """Test that SQLSpecSessionConfig correctly sets the backend class."""
    config = SQLSpecSessionConfig()

    # After __post_init__, _backend_class should be set
    assert config.backend_class is SQLSpecSessionBackend


def test_inheritance() -> None:
    """Test that classes inherit from correct Litestar base classes."""
    config = SQLSpecSessionConfig()
    backend = SQLSpecSessionBackend(config=config)

    from litestar.middleware.session.server_side import ServerSideSessionBackend, ServerSideSessionConfig

    assert isinstance(config, ServerSideSessionConfig)
    assert isinstance(backend, ServerSideSessionBackend)


@pytest.mark.asyncio
async def test_serialization_roundtrip(session_backend: SQLSpecSessionBackend, mock_store: MagicMock) -> None:
    """Test that data can roundtrip through set/get operations."""
    session_id = "roundtrip_test"
    original_data = {"user_id": 999, "preferences": {"theme": "light", "lang": "en"}}

    # Mock store to return the data that was set
    stored_data = None

    async def mock_set(_sid: str, data, expires_in=None) -> None:
        nonlocal stored_data
        stored_data = data

    async def mock_get(_sid: str, renew_for=None):
        return stored_data

    mock_store.set.side_effect = mock_set
    mock_store.get.side_effect = mock_get

    # Simulate Litestar sending JSON bytes to set()
    json_bytes = b'{"user_id": 999, "preferences": {"theme": "light", "lang": "en"}}'

    # Set the data
    await session_backend.set(session_id, json_bytes, mock_store)

    # Get the data back
    result_bytes = await session_backend.get(session_id, mock_store)

    # Should get back equivalent JSON bytes
    assert result_bytes is not None

    # Deserialize to verify content matches
    import json

    result_data = json.loads(result_bytes.decode("utf-8"))
    assert result_data == original_data
