"""Unit tests for SQLSpec session backend."""

import datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

from sqlspec.extensions.litestar.session import SQLSpecSessionBackend, SQLSpecSessionConfig


class MockConnection:
    """Mock ASGI connection for testing."""

    def __init__(self, cookies: dict[str, str], session_id: str = None) -> None:
        self.cookies = cookies
        self._session_id = session_id

    def get_session_id(self) -> str:
        return self._session_id


@pytest.fixture()
def mock_config() -> MagicMock:
    """Create a mock database config."""
    config = MagicMock()
    config.provide_session.return_value.__aenter__ = AsyncMock()
    config.provide_session.return_value.__aexit__ = AsyncMock()
    return config


@pytest.fixture()
def mock_store() -> MagicMock:
    """Create a mock session store."""
    store = MagicMock()
    store.get = AsyncMock()
    store.set = AsyncMock()
    store.delete = AsyncMock()
    store.delete_expired = AsyncMock()
    store.get_all = AsyncMock()
    return store


@pytest.fixture()
def session_backend(mock_config: MagicMock) -> SQLSpecSessionBackend:
    """Create a session backend instance."""
    return SQLSpecSessionBackend(mock_config)


def test_sqlspec_session_config_defaults() -> None:
    """Test default configuration values."""
    config = SQLSpecSessionConfig()
    
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


def test_sqlspec_session_config_custom() -> None:
    """Test custom configuration values."""
    config = SQLSpecSessionConfig(
        key="custom_session",
        max_age=3600,
        path="/custom",
        domain="example.com",
        secure=True,
        httponly=False,
        samesite="strict",
        exclude=["/health", "/metrics"],
        exclude_opt_key="skip_custom_session",
        scopes=frozenset({"http"}),
    )
    
    assert config.key == "custom_session"
    assert config.max_age == 3600
    assert config.path == "/custom"
    assert config.domain == "example.com"
    assert config.secure is True
    assert config.httponly is False
    assert config.samesite == "strict"
    assert config.exclude == ["/health", "/metrics"]
    assert config.exclude_opt_key == "skip_custom_session"
    assert config.scopes == frozenset({"http"})


def test_session_backend_init_defaults(mock_config: MagicMock) -> None:
    """Test session backend initialization with defaults."""
    backend = SQLSpecSessionBackend(mock_config)
    
    assert backend._session_lifetime == 24 * 60 * 60  # 24 hours
    assert isinstance(backend.config, SQLSpecSessionConfig)
    assert backend.config.key == "session"
    assert backend._store is not None


def test_session_backend_init_custom(mock_config: MagicMock) -> None:
    """Test session backend initialization with custom values."""
    session_config = SQLSpecSessionConfig(key="custom", max_age=7200)
    
    backend = SQLSpecSessionBackend(
        mock_config,
        table_name="custom_sessions",
        session_id_column="id",
        data_column="payload",
        expires_at_column="expires",
        created_at_column="created",
        session_lifetime=3600,
        session_config=session_config,
    )
    
    assert backend._session_lifetime == 3600
    assert backend.config.key == "custom"
    assert backend.config.max_age == 7200


@pytest.mark.asyncio()
async def test_load_from_connection_no_session_id(session_backend: SQLSpecSessionBackend) -> None:
    """Test loading session data when no session ID is found."""
    connection = MockConnection(cookies={})
    
    result = await session_backend.load_from_connection(connection)
    
    assert result == {}


@pytest.mark.asyncio()
async def test_load_from_connection_with_session_id(session_backend: SQLSpecSessionBackend) -> None:
    """Test loading session data with valid session ID."""
    connection = MockConnection(cookies={"session": "test_session_id"})
    session_data = {"user_id": 123, "username": "test_user"}
    
    with patch.object(session_backend._store, "get", return_value=session_data) as mock_get:
        result = await session_backend.load_from_connection(connection)
        
        assert result == session_data
        mock_get.assert_called_once_with("test_session_id")


@pytest.mark.asyncio()
async def test_load_from_connection_invalid_data_type(session_backend: SQLSpecSessionBackend) -> None:
    """Test loading session data when store returns non-dict data."""
    connection = MockConnection(cookies={"session": "test_session_id"})
    
    with patch.object(session_backend._store, "get", return_value="invalid_data"):
        result = await session_backend.load_from_connection(connection)
        
        assert result == {}


@pytest.mark.asyncio()
async def test_load_from_connection_store_exception(session_backend: SQLSpecSessionBackend) -> None:
    """Test loading session data when store raises exception."""
    connection = MockConnection(cookies={"session": "test_session_id"})
    
    with patch.object(session_backend._store, "get", side_effect=Exception("Database error")):
        result = await session_backend.load_from_connection(connection)
        
        assert result == {}


@pytest.mark.asyncio()
async def test_dump_to_connection_new_session(session_backend: SQLSpecSessionBackend) -> None:
    """Test storing new session data."""
    connection = MockConnection(cookies={})
    session_data = {"user_id": 123}
    
    with patch.object(session_backend, "_session_id_generator", return_value="new_session_id"):
        with patch.object(session_backend._store, "set") as mock_set:
            result = await session_backend.dump_to_connection(session_data, connection)
            
            assert result == "new_session_id"
            mock_set.assert_called_once_with("new_session_id", session_data, expires_in=24 * 60 * 60)


@pytest.mark.asyncio()
async def test_dump_to_connection_existing_session(session_backend: SQLSpecSessionBackend) -> None:
    """Test updating existing session data."""
    connection = MockConnection(cookies={"session": "existing_session_id"})
    session_data = {"user_id": 123}
    
    with patch.object(session_backend._store, "set") as mock_set:
        result = await session_backend.dump_to_connection(session_data, connection)
        
        assert result == "existing_session_id"
        mock_set.assert_called_once_with("existing_session_id", session_data, expires_in=24 * 60 * 60)


@pytest.mark.asyncio()
async def test_dump_to_connection_store_exception(session_backend: SQLSpecSessionBackend) -> None:
    """Test storing session data when store raises exception."""
    connection = MockConnection(cookies={"session": "test_session_id"})
    session_data = {"user_id": 123}
    
    with patch.object(session_backend._store, "set", side_effect=Exception("Database error")):
        with pytest.raises(Exception, match="Database error"):
            await session_backend.dump_to_connection(session_data, connection)


def test_get_session_id_from_cookie(session_backend: SQLSpecSessionBackend) -> None:
    """Test getting session ID from cookie."""
    connection = MockConnection(cookies={"session": "cookie_session_id"})
    
    result = session_backend.get_session_id(connection)
    
    assert result == "cookie_session_id"


def test_get_session_id_null_cookie(session_backend: SQLSpecSessionBackend) -> None:
    """Test getting session ID when cookie is 'null'."""
    connection = MockConnection(cookies={"session": "null"})
    
    result = session_backend.get_session_id(connection)
    
    assert result is None


def test_get_session_id_from_connection_state(session_backend: SQLSpecSessionBackend) -> None:
    """Test getting session ID from connection state when no cookie."""
    connection = MockConnection(cookies={}, session_id="state_session_id")
    
    result = session_backend.get_session_id(connection)
    
    assert result == "state_session_id"


def test_get_session_id_no_session(session_backend: SQLSpecSessionBackend) -> None:
    """Test getting session ID when none exists."""
    connection = MockConnection(cookies={})
    
    result = session_backend.get_session_id(connection)
    
    assert result is None


def test_get_session_id_custom_key(mock_config: MagicMock) -> None:
    """Test getting session ID with custom cookie key."""
    session_config = SQLSpecSessionConfig(key="custom_session")
    backend = SQLSpecSessionBackend(mock_config, session_config=session_config)
    connection = MockConnection(cookies={"custom_session": "custom_session_id"})
    
    result = backend.get_session_id(connection)
    
    assert result == "custom_session_id"


@pytest.mark.asyncio()
async def test_store_in_message_empty_session(session_backend: SQLSpecSessionBackend) -> None:
    """Test storing empty session in message."""
    connection = MockConnection(cookies={})
    message = {"type": "http.response.start", "headers": []}
    scope_session = {}
    
    await session_backend.store_in_message(scope_session, message, connection)
    
    # Check that a null cookie was set
    headers = dict(message["headers"])
    assert b"set-cookie" in headers
    cookie_value = headers[b"set-cookie"].decode()
    assert "session=null" in cookie_value
    assert "Max-Age=0" in cookie_value


@pytest.mark.asyncio()
async def test_store_in_message_with_data(session_backend: SQLSpecSessionBackend) -> None:
    """Test storing session data in message."""
    connection = MockConnection(cookies={})
    message = {"type": "http.response.start", "headers": []}
    scope_session = {"user_id": 123}
    
    with patch.object(session_backend, "_session_id_generator", return_value="new_session_id"):
        with patch.object(session_backend._store, "set") as mock_set:
            await session_backend.store_in_message(scope_session, message, connection)
            
            mock_set.assert_called_once_with("new_session_id", scope_session, expires_in=24 * 60 * 60)
            
            # Check that session cookie was set
            headers = dict(message["headers"])
            assert b"set-cookie" in headers
            cookie_value = headers[b"set-cookie"].decode()
            assert "session=new_session_id" in cookie_value


@pytest.mark.asyncio()
async def test_store_in_message_store_failure(session_backend: SQLSpecSessionBackend) -> None:
    """Test storing session data when store fails."""
    connection = MockConnection(cookies={})
    message = {"type": "http.response.start", "headers": []}
    scope_session = {"user_id": 123}
    
    with patch.object(session_backend, "_session_id_generator", return_value="new_session_id"):
        with patch.object(session_backend._store, "set", side_effect=Exception("Store error")):
            await session_backend.store_in_message(scope_session, message, connection)
            
            # Should not set cookie if store fails
            headers = dict(message.get("headers", []))
            assert b"set-cookie" not in headers


@pytest.mark.asyncio()
async def test_store_in_message_wrong_message_type(session_backend: SQLSpecSessionBackend) -> None:
    """Test storing session data with wrong message type."""
    connection = MockConnection(cookies={})
    message = {"type": "http.request", "headers": []}
    scope_session = {"user_id": 123}
    
    await session_backend.store_in_message(scope_session, message, connection)
    
    # Should not modify message for non-response.start types
    assert message["headers"] == []


def test_build_cookie_value_minimal(session_backend: SQLSpecSessionBackend) -> None:
    """Test building cookie value with minimal parameters."""
    result = session_backend._build_cookie_value("test_key", "test_value")
    
    assert result == "test_key=test_value"


def test_build_cookie_value_full(session_backend: SQLSpecSessionBackend) -> None:
    """Test building cookie value with all parameters."""
    result = session_backend._build_cookie_value(
        key="session",
        value="session_id",
        max_age=3600,
        path="/app",
        domain="example.com",
        secure=True,
        httponly=True,
        samesite="strict",
    )
    
    expected_parts = [
        "session=session_id",
        "Path=/app",
        "Domain=example.com",
        "Max-Age=3600",
        "Secure",
        "HttpOnly",
        "SameSite=strict",
    ]
    
    for part in expected_parts:
        assert part in result


def test_add_cookie_to_message(session_backend: SQLSpecSessionBackend) -> None:
    """Test adding cookie to ASGI message."""
    message = {"type": "http.response.start", "headers": [[b"content-type", b"text/html"]]}
    cookie_value = "session=test_session; Path=/"
    
    session_backend._add_cookie_to_message(message, cookie_value)
    
    assert len(message["headers"]) == 2
    assert [b"set-cookie", b"session=test_session; Path=/"] in message["headers"]


def test_add_cookie_to_message_no_existing_headers(session_backend: SQLSpecSessionBackend) -> None:
    """Test adding cookie to message with no existing headers."""
    message = {"type": "http.response.start"}
    cookie_value = "session=test_session"
    
    session_backend._add_cookie_to_message(message, cookie_value)
    
    assert message["headers"] == [[b"set-cookie", b"session=test_session"]]


def test_add_cookie_to_message_wrong_type(session_backend: SQLSpecSessionBackend) -> None:
    """Test adding cookie to non-response message."""
    message = {"type": "http.request", "headers": []}
    cookie_value = "session=test_session"
    
    session_backend._add_cookie_to_message(message, cookie_value)
    
    # Should not modify headers for non-response messages
    assert message["headers"] == []


@pytest.mark.asyncio()
async def test_delete_session(session_backend: SQLSpecSessionBackend) -> None:
    """Test deleting a session."""
    with patch.object(session_backend._store, "delete") as mock_delete:
        await session_backend.delete_session("test_session_id")
        
        mock_delete.assert_called_once_with("test_session_id")


@pytest.mark.asyncio()
async def test_delete_session_store_exception(session_backend: SQLSpecSessionBackend) -> None:
    """Test deleting session when store raises exception."""
    with patch.object(session_backend._store, "delete", side_effect=Exception("Delete error")):
        with pytest.raises(Exception, match="Delete error"):
            await session_backend.delete_session("test_session_id")


@pytest.mark.asyncio()
async def test_delete_expired_sessions(session_backend: SQLSpecSessionBackend) -> None:
    """Test deleting expired sessions."""
    with patch.object(session_backend._store, "delete_expired") as mock_delete_expired:
        await session_backend.delete_expired_sessions()
        
        mock_delete_expired.assert_called_once()


@pytest.mark.asyncio()
async def test_delete_expired_sessions_store_exception(session_backend: SQLSpecSessionBackend) -> None:
    """Test deleting expired sessions when store raises exception."""
    with patch.object(session_backend._store, "delete_expired", side_effect=Exception("Delete error")):
        # Should not raise exception, just log it
        await session_backend.delete_expired_sessions()


@pytest.mark.asyncio()
async def test_get_all_session_ids(session_backend: SQLSpecSessionBackend) -> None:
    """Test getting all session IDs."""
    async def mock_get_all():
        yield "session_1", {"data": "1"}
        yield "session_2", {"data": "2"}
    
    with patch.object(session_backend._store, "get_all", return_value=mock_get_all()):
        result = await session_backend.get_all_session_ids()
        
        assert result == ["session_1", "session_2"]


@pytest.mark.asyncio()
async def test_get_all_session_ids_store_exception(session_backend: SQLSpecSessionBackend) -> None:
    """Test getting all session IDs when store raises exception."""
    async def mock_get_all():
        yield "session_1", {"data": "1"}
        raise Exception("Store error")
        yield "session_2", {"data": "2"}  # This won't be reached
    
    with patch.object(session_backend._store, "get_all", return_value=mock_get_all()):
        result = await session_backend.get_all_session_ids()
        
        # Should return partial results and not raise exception
        assert result == []


def test_store_property(session_backend: SQLSpecSessionBackend) -> None:
    """Test accessing the store property."""
    store = session_backend.store
    
    assert store is session_backend._store


def test_session_id_generator() -> None:
    """Test session ID generation."""
    from sqlspec.extensions.litestar.store import SQLSpecSessionStore
    
    session_id = SQLSpecSessionStore.generate_session_id()
    
    assert isinstance(session_id, str)
    assert len(session_id) > 0
    
    # Generate another to ensure they're unique
    another_id = SQLSpecSessionStore.generate_session_id()
    assert session_id != another_id


@pytest.mark.parametrize("cookie_key", ["session", "user_session", "app_session"])
def test_get_session_id_custom_cookie_keys(mock_config: MagicMock, cookie_key: str) -> None:
    """Test getting session ID with various custom cookie keys."""
    session_config = SQLSpecSessionConfig(key=cookie_key)
    backend = SQLSpecSessionBackend(mock_config, session_config=session_config)
    connection = MockConnection(cookies={cookie_key: "test_session_id"})
    
    result = backend.get_session_id(connection)
    
    assert result == "test_session_id"


def test_session_backend_attributes(session_backend: SQLSpecSessionBackend) -> None:
    """Test session backend has expected attributes."""
    assert hasattr(session_backend, "_store")
    assert hasattr(session_backend, "_session_id_generator")
    assert hasattr(session_backend, "_session_lifetime")
    assert hasattr(session_backend, "config")
    
    assert callable(session_backend._session_id_generator)
    assert isinstance(session_backend._session_lifetime, int)
    assert isinstance(session_backend.config, SQLSpecSessionConfig)


@pytest.mark.asyncio()
async def test_load_from_connection_integration(mock_config: MagicMock) -> None:
    """Test load_from_connection with store integration."""
    backend = SQLSpecSessionBackend(mock_config, session_lifetime=3600)
    connection = MockConnection(cookies={"session": "integration_session"})
    expected_data = {"user_id": 456, "permissions": ["read", "write"]}
    
    with patch.object(backend._store, "get", return_value=expected_data) as mock_get:
        result = await backend.load_from_connection(connection)
        
        assert result == expected_data
        mock_get.assert_called_once_with("integration_session")


@pytest.mark.asyncio()
async def test_dump_to_connection_integration(mock_config: MagicMock) -> None:
    """Test dump_to_connection with store integration."""
    backend = SQLSpecSessionBackend(mock_config, session_lifetime=7200)
    connection = MockConnection(cookies={})
    session_data = {"user_id": 789, "last_login": "2023-01-01T00:00:00Z"}
    
    with patch.object(backend, "_session_id_generator", return_value="integration_session"):
        with patch.object(backend._store, "set") as mock_set:
            result = await backend.dump_to_connection(session_data, connection)
            
            assert result == "integration_session"
            mock_set.assert_called_once_with("integration_session", session_data, expires_in=7200)