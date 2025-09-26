# pyright: reportPrivateUsage=false
"""Unit tests for SQLSpec session store."""

import datetime
from datetime import timedelta, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from sqlspec.core.statement import StatementConfig
from sqlspec.exceptions import SQLSpecError
from sqlspec.extensions.litestar.store import SQLSpecAsyncSessionStore, SQLSpecSessionStoreError


class MockDriver:
    """Mock database driver for testing."""

    def __init__(self, dialect: str = "sqlite") -> None:
        self.statement_config = StatementConfig(dialect=dialect)
        self.execute = AsyncMock()
        self.commit = AsyncMock()

        # Fix: Make execute return proper result structure with count column
        mock_result = MagicMock()
        mock_result.data = [{"count": 0}]  # Proper dict structure for handle_column_casing
        self.execute.return_value = mock_result


class MockConfig:
    """Mock database config for testing."""

    def __init__(self, driver: MockDriver = MockDriver()) -> None:
        self._driver = driver

    def provide_session(self) -> "MockConfig":
        return self

    async def __aenter__(self) -> MockDriver:
        return self._driver

    async def __aexit__(self, exc_type: "Any", exc_val: "Any", exc_tb: "Any") -> None:
        pass


@pytest.fixture()
def mock_config() -> MockConfig:
    """Create a mock database config."""
    return MockConfig()


@pytest.fixture()
def session_store(mock_config: MockConfig) -> SQLSpecAsyncSessionStore:
    """Create a session store instance."""
    return SQLSpecAsyncSessionStore(mock_config)  # type: ignore[arg-type,type-var]


@pytest.fixture()
def postgres_store() -> SQLSpecAsyncSessionStore:
    """Create a session store for PostgreSQL."""
    return SQLSpecAsyncSessionStore(MockConfig(MockDriver("postgres")))  # type: ignore[arg-type,type-var]


@pytest.fixture()
def mysql_store() -> SQLSpecAsyncSessionStore:
    """Create a session store for MySQL."""
    return SQLSpecAsyncSessionStore(MockConfig(MockDriver("mysql")))  # type: ignore[arg-type,type-var]


@pytest.fixture()
def oracle_store() -> SQLSpecAsyncSessionStore:
    """Create a session store for Oracle."""
    return SQLSpecAsyncSessionStore(MockConfig(MockDriver("oracle")))  # type: ignore[arg-type,type-var]


def test_session_store_init_defaults(mock_config: MockConfig) -> None:
    """Test session store initialization with defaults."""
    store = SQLSpecAsyncSessionStore(mock_config)  # type: ignore[arg-type,type-var]

    assert store.table_name == "litestar_sessions"
    assert store.session_id_column == "session_id"
    assert store.data_column == "data"
    assert store.expires_at_column == "expires_at"
    assert store.created_at_column == "created_at"


def test_session_store_init_custom(mock_config: MockConfig) -> None:
    """Test session store initialization with custom values."""
    store = SQLSpecAsyncSessionStore(
        mock_config,  # type: ignore[arg-type,type-var]
        table_name="custom_sessions",
        session_id_column="id",
        data_column="payload",
        expires_at_column="expires",
        created_at_column="created",
    )

    assert store.table_name == "custom_sessions"
    assert store.session_id_column == "id"
    assert store.data_column == "payload"
    assert store.expires_at_column == "expires"
    assert store.created_at_column == "created"


def test_build_upsert_sql_postgres(postgres_store: SQLSpecAsyncSessionStore) -> None:
    """Test PostgreSQL upsert SQL generation using new handler API."""
    expires_at = datetime.datetime.now(timezone.utc) + timedelta(hours=1)
    data_value = postgres_store._handler.serialize_data('{"key": "value"}')
    expires_at_value = postgres_store._handler.format_datetime(expires_at)
    current_time_value = postgres_store._handler.get_current_time()

    sql_list = postgres_store._handler.build_upsert_sql(
        postgres_store._table_name,
        postgres_store._session_id_column,
        postgres_store._data_column,
        postgres_store._expires_at_column,
        postgres_store._created_at_column,
        "test_id",
        data_value,
        expires_at_value,
        current_time_value,
    )

    assert isinstance(sql_list, list)
    assert len(sql_list) == 3  # Default check-update-insert pattern


def test_build_upsert_sql_mysql(mysql_store: SQLSpecAsyncSessionStore) -> None:
    """Test MySQL upsert SQL generation using new handler API."""
    expires_at = datetime.datetime.now(timezone.utc) + timedelta(hours=1)
    data_value = mysql_store._handler.serialize_data('{"key": "value"}')
    expires_at_value = mysql_store._handler.format_datetime(expires_at)
    current_time_value = mysql_store._handler.get_current_time()

    sql_list = mysql_store._handler.build_upsert_sql(
        mysql_store._table_name,
        mysql_store._session_id_column,
        mysql_store._data_column,
        mysql_store._expires_at_column,
        mysql_store._created_at_column,
        "test_id",
        data_value,
        expires_at_value,
        current_time_value,
    )

    assert isinstance(sql_list, list)
    assert len(sql_list) == 3  # Default check-update-insert pattern


def test_build_upsert_sql_sqlite(session_store: SQLSpecAsyncSessionStore) -> None:
    """Test SQLite upsert SQL generation using new handler API."""
    expires_at = datetime.datetime.now(timezone.utc) + timedelta(hours=1)
    data_value = session_store._handler.serialize_data('{"key": "value"}')
    expires_at_value = session_store._handler.format_datetime(expires_at)
    current_time_value = session_store._handler.get_current_time()

    sql_list = session_store._handler.build_upsert_sql(
        session_store._table_name,
        session_store._session_id_column,
        session_store._data_column,
        session_store._expires_at_column,
        session_store._created_at_column,
        "test_id",
        data_value,
        expires_at_value,
        current_time_value,
    )

    assert isinstance(sql_list, list)
    assert len(sql_list) == 3  # Default check-update-insert pattern


def test_build_upsert_sql_oracle(oracle_store: SQLSpecAsyncSessionStore) -> None:
    """Test Oracle upsert SQL generation using new handler API."""
    expires_at = datetime.datetime.now(timezone.utc) + timedelta(hours=1)
    data_value = oracle_store._handler.serialize_data('{"key": "value"}')
    expires_at_value = oracle_store._handler.format_datetime(expires_at)
    current_time_value = oracle_store._handler.get_current_time()

    sql_list = oracle_store._handler.build_upsert_sql(
        oracle_store._table_name,
        oracle_store._session_id_column,
        oracle_store._data_column,
        oracle_store._expires_at_column,
        oracle_store._created_at_column,
        "test_id",
        data_value,
        expires_at_value,
        current_time_value,
    )

    assert isinstance(sql_list, list)
    assert len(sql_list) == 3  # Oracle uses check-update-insert pattern due to MERGE syntax issues


def test_build_upsert_sql_fallback(session_store: SQLSpecAsyncSessionStore) -> None:
    """Test fallback upsert SQL generation using new handler API."""
    expires_at = datetime.datetime.now(timezone.utc) + timedelta(hours=1)
    data_value = session_store._handler.serialize_data('{"key": "value"}')
    expires_at_value = session_store._handler.format_datetime(expires_at)
    current_time_value = session_store._handler.get_current_time()

    sql_list = session_store._handler.build_upsert_sql(
        session_store._table_name,
        session_store._session_id_column,
        session_store._data_column,
        session_store._expires_at_column,
        session_store._created_at_column,
        "test_id",
        data_value,
        expires_at_value,
        current_time_value,
    )

    assert isinstance(sql_list, list)
    assert len(sql_list) == 3  # Fallback uses check-update-insert pattern


async def test_get_session_found(session_store: SQLSpecAsyncSessionStore) -> None:
    """Test getting existing session data."""
    mock_result = MagicMock()
    mock_result.data = [{"data": '{"user_id": 123}'}]

    with patch.object(session_store._config, "provide_session") as mock_session:
        driver = MockDriver()
        driver.execute = AsyncMock(return_value=mock_result)
        mock_session.return_value.__aenter__ = AsyncMock(return_value=driver)
        mock_session.return_value.__aexit__ = AsyncMock()

        with patch("sqlspec.extensions.litestar.store.from_json", return_value={"user_id": 123}) as mock_from_json:
            result = await session_store.get("test_session_id")

            assert result == {"user_id": 123}
            mock_from_json.assert_called_once_with('{"user_id": 123}')


async def test_get_session_not_found(session_store: SQLSpecAsyncSessionStore) -> None:
    """Test getting non-existent session data."""
    mock_result = MagicMock()
    mock_result.data = []

    with patch.object(session_store._config, "provide_session") as mock_session:
        driver = MockDriver()
        driver.execute = AsyncMock(return_value=mock_result)
        mock_session.return_value.__aenter__ = AsyncMock(return_value=driver)
        mock_session.return_value.__aexit__ = AsyncMock()

        result = await session_store.get("non_existent_session")

        assert result is None


async def test_get_session_with_renewal(session_store: SQLSpecAsyncSessionStore) -> None:
    """Test getting session data with renewal."""
    mock_result = MagicMock()
    mock_result.data = [{"data": '{"user_id": 123}'}]

    with patch.object(session_store._config, "provide_session") as mock_session:
        driver = MockDriver()
        driver.execute.return_value = mock_result  # Set the return value on the driver
        mock_session.return_value.__aenter__ = AsyncMock(return_value=driver)
        mock_session.return_value.__aexit__ = AsyncMock()

        with patch("sqlspec.extensions.litestar.store.from_json", return_value={"user_id": 123}):
            result = await session_store.get("test_session_id", renew_for=3600)

            assert result == {"user_id": 123}
            assert driver.execute.call_count >= 2  # SELECT + UPDATE


async def test_get_session_exception(session_store: SQLSpecAsyncSessionStore) -> None:
    """Test getting session data when database error occurs."""
    with patch.object(session_store._config, "provide_session") as mock_session:
        driver = MockDriver()
        driver.execute.side_effect = Exception("Database error")
        mock_session.return_value.__aenter__ = AsyncMock(return_value=driver)
        mock_session.return_value.__aexit__ = AsyncMock()

        result = await session_store.get("test_session_id")

        assert result is None


async def test_set_session_new(session_store: SQLSpecAsyncSessionStore) -> None:
    """Test setting new session data."""
    with patch.object(session_store._config, "provide_session") as mock_session:
        driver = MockDriver()
        mock_session.return_value.__aenter__ = AsyncMock(return_value=driver)
        mock_session.return_value.__aexit__ = AsyncMock()

        with patch("sqlspec.extensions.litestar.store.to_json", return_value='{"user_id": 123}') as mock_to_json:
            await session_store.set("test_session_id", {"user_id": 123})

            mock_to_json.assert_called_once_with({"user_id": 123})
            driver.execute.assert_called()


async def test_set_session_with_timedelta_expires(session_store: SQLSpecAsyncSessionStore) -> None:
    """Test setting session data with timedelta expiration."""
    with patch.object(session_store._config, "provide_session") as mock_session:
        driver = MockDriver()
        mock_session.return_value.__aenter__ = AsyncMock(return_value=driver)
        mock_session.return_value.__aexit__ = AsyncMock()

        with patch("sqlspec.extensions.litestar.store.to_json", return_value='{"user_id": 123}'):
            await session_store.set("test_session_id", {"user_id": 123}, expires_in=timedelta(hours=2))

            driver.execute.assert_called()


async def test_set_session_default_expiration(session_store: SQLSpecAsyncSessionStore) -> None:
    """Test setting session data with default expiration."""
    with patch.object(session_store._config, "provide_session") as mock_session:
        driver = MockDriver()
        mock_session.return_value.__aenter__ = AsyncMock(return_value=driver)
        mock_session.return_value.__aexit__ = AsyncMock()

        with patch("sqlspec.extensions.litestar.store.to_json", return_value='{"user_id": 123}'):
            await session_store.set("test_session_id", {"user_id": 123})

            driver.execute.assert_called()


async def test_set_session_fallback_dialect(session_store: SQLSpecAsyncSessionStore) -> None:
    """Test setting session data with fallback dialect (multiple statements)."""
    with patch.object(session_store._config, "provide_session") as mock_session:
        driver = MockDriver("unsupported")
        # Set up mock to return count=0 for the SELECT COUNT query (session doesn't exist)
        mock_count_result = MagicMock()
        mock_count_result.data = [{"count": 0}]
        driver.execute.return_value = mock_count_result

        mock_session.return_value.__aenter__ = AsyncMock(return_value=driver)
        mock_session.return_value.__aexit__ = AsyncMock()

        with patch("sqlspec.extensions.litestar.store.to_json", return_value='{"user_id": 123}'):
            await session_store.set("test_session_id", {"user_id": 123})

            assert driver.execute.call_count == 2  # Check exists (returns 0), then insert (not update)


async def test_set_session_exception(session_store: SQLSpecAsyncSessionStore) -> None:
    """Test setting session data when database error occurs."""
    with patch.object(session_store._config, "provide_session") as mock_session:
        driver = MockDriver()
        mock_session.return_value.__aenter__ = AsyncMock(return_value=driver)
        # Make sure __aexit__ doesn't suppress exceptions by returning False/None
        mock_session.return_value.__aexit__ = AsyncMock(return_value=False)
        driver.execute.side_effect = Exception("Database error")

        with patch("sqlspec.extensions.litestar.store.to_json", return_value='{"user_id": 123}'):
            with pytest.raises(SQLSpecSessionStoreError, match="Failed to store session"):
                await session_store.set("test_session_id", {"user_id": 123})


async def test_delete_session(session_store: SQLSpecAsyncSessionStore) -> None:
    """Test deleting session data."""
    with patch.object(session_store._config, "provide_session") as mock_session:
        driver = MockDriver()
        mock_session.return_value.__aenter__ = AsyncMock(return_value=driver)
        mock_session.return_value.__aexit__ = AsyncMock()

        await session_store.delete("test_session_id")

        driver.execute.assert_called()


async def test_delete_session_exception(session_store: SQLSpecAsyncSessionStore) -> None:
    """Test deleting session data when database error occurs."""
    with patch.object(session_store._config, "provide_session") as mock_session:
        driver = MockDriver()
        mock_session.return_value.__aenter__ = AsyncMock(return_value=driver)
        # Make sure __aexit__ doesn't suppress exceptions by returning False/None
        mock_session.return_value.__aexit__ = AsyncMock(return_value=False)
        driver.execute.side_effect = Exception("Database error")

        with pytest.raises(SQLSpecSessionStoreError, match="Failed to delete session"):
            await session_store.delete("test_session_id")


async def test_exists_session_true(session_store: SQLSpecAsyncSessionStore) -> None:
    """Test checking if session exists (returns True)."""
    mock_result = MagicMock()
    mock_result.data = [{"count": 1}]

    with patch.object(session_store._config, "provide_session") as mock_session:
        driver = MockDriver()
        mock_session.return_value.__aenter__ = AsyncMock(return_value=driver)
        mock_session.return_value.__aexit__ = AsyncMock()
        driver.execute.return_value = mock_result

        result = await session_store.exists("test_session_id")

        assert result is True


async def test_exists_session_false(session_store: SQLSpecAsyncSessionStore) -> None:
    """Test checking if session exists (returns False)."""
    mock_result = MagicMock()
    mock_result.data = [{"count": 0}]

    with patch.object(session_store._config, "provide_session") as mock_session:
        driver = MockDriver()
        mock_session.return_value.__aenter__ = AsyncMock(return_value=driver)
        mock_session.return_value.__aexit__ = AsyncMock()
        driver.execute.return_value = mock_result

        result = await session_store.exists("non_existent_session")

        assert result is False


async def test_exists_session_exception(session_store: SQLSpecAsyncSessionStore) -> None:
    """Test checking if session exists when database error occurs."""
    with patch.object(session_store._config, "provide_session") as mock_session:
        mock_session.return_value.__aenter__ = AsyncMock(side_effect=Exception("Database error"))
        mock_session.return_value.__aexit__ = AsyncMock()

        result = await session_store.exists("test_session_id")

        assert result is False


async def test_expires_in_valid_session(session_store: SQLSpecAsyncSessionStore) -> None:
    """Test getting expiration time for valid session."""
    now = datetime.datetime.now(timezone.utc)
    expires_at = now + timedelta(hours=1)
    mock_result = MagicMock()
    mock_result.data = [{"expires_at": expires_at}]

    with patch.object(session_store._config, "provide_session") as mock_session:
        driver = MockDriver()
        mock_session.return_value.__aenter__ = AsyncMock(return_value=driver)
        mock_session.return_value.__aexit__ = AsyncMock()
        driver.execute.return_value = mock_result

        result = await session_store.expires_in("test_session_id")

        assert 3590 <= result <= 3600  # Should be close to 1 hour


async def test_expires_in_expired_session(session_store: SQLSpecAsyncSessionStore) -> None:
    """Test getting expiration time for expired session."""
    now = datetime.datetime.now(timezone.utc)
    expires_at = now - timedelta(hours=1)  # Expired
    mock_result = MagicMock()
    mock_result.data = [{"expires_at": expires_at}]

    with patch.object(session_store._config, "provide_session") as mock_session:
        driver = MockDriver()
        mock_session.return_value.__aenter__ = AsyncMock(return_value=driver)
        mock_session.return_value.__aexit__ = AsyncMock()
        driver.execute.return_value = mock_result

        result = await session_store.expires_in("test_session_id")

        assert result == 0


async def test_expires_in_string_datetime(session_store: SQLSpecAsyncSessionStore) -> None:
    """Test getting expiration time when database returns string datetime."""
    now = datetime.datetime.now(timezone.utc)
    expires_at_str = (now + timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
    mock_result = MagicMock()
    mock_result.data = [{"expires_at": expires_at_str}]

    with patch.object(session_store._config, "provide_session") as mock_session:
        driver = MockDriver()
        mock_session.return_value.__aenter__ = AsyncMock(return_value=driver)
        mock_session.return_value.__aexit__ = AsyncMock()
        driver.execute.return_value = mock_result

        result = await session_store.expires_in("test_session_id")

        assert 3590 <= result <= 3600  # Should be close to 1 hour


async def test_expires_in_no_session(session_store: SQLSpecAsyncSessionStore) -> None:
    """Test getting expiration time for non-existent session."""
    mock_result = MagicMock()
    mock_result.data = []

    with patch.object(session_store._config, "provide_session") as mock_session:
        driver = MockDriver()
        mock_session.return_value.__aenter__ = AsyncMock(return_value=driver)
        mock_session.return_value.__aexit__ = AsyncMock()
        driver.execute.return_value = mock_result

        result = await session_store.expires_in("non_existent_session")

        assert result == 0


async def test_expires_in_invalid_datetime_format(session_store: SQLSpecAsyncSessionStore) -> None:
    """Test getting expiration time with invalid datetime format."""
    mock_result = MagicMock()
    mock_result.data = [{"expires_at": "invalid_datetime"}]

    with patch.object(session_store._config, "provide_session") as mock_session:
        driver = MockDriver()
        mock_session.return_value.__aenter__ = AsyncMock(return_value=driver)
        mock_session.return_value.__aexit__ = AsyncMock()
        driver.execute.return_value = mock_result

        result = await session_store.expires_in("test_session_id")

        assert result == 0


async def test_expires_in_exception(session_store: SQLSpecAsyncSessionStore) -> None:
    """Test getting expiration time when database error occurs."""
    with patch.object(session_store._config, "provide_session") as mock_session:
        mock_session.return_value.__aenter__ = AsyncMock(side_effect=Exception("Database error"))
        mock_session.return_value.__aexit__ = AsyncMock()

        result = await session_store.expires_in("test_session_id")

        assert result == 0


async def test_delete_all_sessions(session_store: SQLSpecAsyncSessionStore) -> None:
    """Test deleting all sessions."""
    with patch.object(session_store._config, "provide_session") as mock_session:
        driver = MockDriver()
        mock_session.return_value.__aenter__ = AsyncMock(return_value=driver)
        mock_session.return_value.__aexit__ = AsyncMock()

        await session_store.delete_all()

        driver.execute.assert_called()


async def test_delete_all_sessions_exception(session_store: SQLSpecAsyncSessionStore) -> None:
    """Test deleting all sessions when database error occurs."""
    with patch.object(session_store._config, "provide_session") as mock_session:
        driver = MockDriver()
        mock_session.return_value.__aenter__ = AsyncMock(return_value=driver)
        # Make sure __aexit__ doesn't suppress exceptions by returning False/None
        mock_session.return_value.__aexit__ = AsyncMock(return_value=False)
        driver.execute.side_effect = Exception("Database error")

        with pytest.raises(SQLSpecSessionStoreError, match="Failed to delete all sessions"):
            await session_store.delete_all()


async def test_delete_expired_sessions(session_store: SQLSpecAsyncSessionStore) -> None:
    """Test deleting expired sessions."""
    with patch.object(session_store._config, "provide_session") as mock_session:
        driver = MockDriver()
        mock_session.return_value.__aenter__ = AsyncMock(return_value=driver)
        mock_session.return_value.__aexit__ = AsyncMock()

        await session_store.delete_expired()

        driver.execute.assert_called()


async def test_delete_expired_sessions_exception(session_store: SQLSpecAsyncSessionStore) -> None:
    """Test deleting expired sessions when database error occurs."""
    with patch.object(session_store._config, "provide_session") as mock_session:
        driver = MockDriver()
        driver.execute.side_effect = Exception("Database error")
        mock_session.return_value.__aenter__ = AsyncMock(return_value=driver)
        mock_session.return_value.__aexit__ = AsyncMock()

        # Should not raise exception, just log it
        await session_store.delete_expired()


async def test_get_all_sessions(session_store: SQLSpecAsyncSessionStore) -> None:
    """Test getting all sessions."""
    mock_result = MagicMock()
    mock_result.data = [
        {"session_id": "session_1", "data": '{"user_id": 1}'},
        {"session_id": "session_2", "data": '{"user_id": 2}'},
    ]

    with patch.object(session_store._config, "provide_session") as mock_session:
        driver = MockDriver()
        mock_session.return_value.__aenter__ = AsyncMock(return_value=driver)
        mock_session.return_value.__aexit__ = AsyncMock()
        driver.execute.return_value = mock_result

        with patch("sqlspec.extensions.litestar.store.from_json", side_effect=[{"user_id": 1}, {"user_id": 2}]):
            sessions = []
            async for session_id, session_data in session_store.get_all():
                sessions.append((session_id, session_data))

            assert len(sessions) == 2
            assert sessions[0] == ("session_1", {"user_id": 1})
            assert sessions[1] == ("session_2", {"user_id": 2})


async def test_get_all_sessions_invalid_json(session_store: SQLSpecAsyncSessionStore) -> None:
    """Test getting all sessions with invalid JSON data."""
    mock_result = MagicMock()
    mock_result.data = [
        {"session_id": "session_1", "data": '{"user_id": 1}'},
        {"session_id": "session_2", "data": "invalid_json"},
        {"session_id": "session_3", "data": '{"user_id": 3}'},
    ]

    with patch.object(session_store._config, "provide_session") as mock_session:
        driver = MockDriver()
        mock_session.return_value.__aenter__ = AsyncMock(return_value=driver)
        mock_session.return_value.__aexit__ = AsyncMock()
        driver.execute.return_value = mock_result

        def mock_from_json(data: str) -> "dict[str, Any]":
            if data == "invalid_json":
                raise ValueError("Invalid JSON")
            return {"user_id": 1} if "1" in data else {"user_id": 3}

        with patch("sqlspec.extensions.litestar.store.from_json", side_effect=mock_from_json):
            sessions = []
            async for session_id, session_data in session_store.get_all():
                # Filter out invalid JSON (None values)
                if session_data is not None:
                    sessions.append((session_id, session_data))

            # Should skip invalid JSON entry
            assert len(sessions) == 2
            assert sessions[0] == ("session_1", {"user_id": 1})
            assert sessions[1] == ("session_3", {"user_id": 3})


async def test_get_all_sessions_exception(session_store: SQLSpecAsyncSessionStore) -> None:
    """Test getting all sessions when database error occurs."""
    with patch.object(session_store._config, "provide_session") as mock_session:
        mock_session.return_value.__aenter__ = AsyncMock(side_effect=Exception("Database error"))
        mock_session.return_value.__aexit__ = AsyncMock()

        # Should raise exception when database connection fails
        with pytest.raises(Exception, match="Database error"):
            sessions = []
            async for session_id, session_data in session_store.get_all():
                sessions.append((session_id, session_data))


def test_generate_session_id() -> None:
    """Test session ID generation."""
    session_id = SQLSpecAsyncSessionStore.generate_session_id()

    assert isinstance(session_id, str)
    assert len(session_id) > 0

    # Generate another to ensure they're unique
    another_id = SQLSpecAsyncSessionStore.generate_session_id()
    assert session_id != another_id


def test_session_store_error_inheritance() -> None:
    """Test SessionStoreError inheritance."""
    error = SQLSpecSessionStoreError("Test error")

    assert isinstance(error, SQLSpecError)
    assert isinstance(error, Exception)
    assert str(error) == "Test error"


async def test_update_expiration(session_store: SQLSpecAsyncSessionStore) -> None:
    """Test updating session expiration time."""
    new_expires_at = datetime.datetime.now(timezone.utc) + timedelta(hours=2)
    driver = MockDriver()

    await session_store._update_expiration(driver, "test_session_id", new_expires_at)  # type: ignore[arg-type]

    driver.execute.assert_called_once()


async def test_update_expiration_exception(session_store: SQLSpecAsyncSessionStore) -> None:
    """Test updating session expiration when database error occurs."""
    driver = MockDriver()
    driver.execute.side_effect = Exception("Database error")
    new_expires_at = datetime.datetime.now(timezone.utc) + timedelta(hours=2)

    # Should not raise exception, just log it
    await session_store._update_expiration(driver, "test_session_id", new_expires_at)  # type: ignore[arg-type]


async def test_get_session_data_internal(session_store: SQLSpecAsyncSessionStore) -> None:
    """Test internal get session data method."""
    driver = MockDriver()
    mock_result = MagicMock()
    mock_result.data = [{"data": '{"user_id": 123}'}]
    driver.execute.return_value = mock_result

    with patch("sqlspec.extensions.litestar.store.from_json", return_value={"user_id": 123}):
        result = await session_store._get_session_data(driver, "test_session_id", None)  # type: ignore[arg-type]

        assert result == {"user_id": 123}


async def test_set_session_data_internal(session_store: SQLSpecAsyncSessionStore) -> None:
    """Test internal set session data method."""
    driver = MockDriver()
    expires_at = datetime.datetime.now(timezone.utc) + timedelta(hours=1)

    await session_store._set_session_data(driver, "test_session_id", '{"user_id": 123}', expires_at)  # type: ignore[arg-type]

    driver.execute.assert_called()


async def test_delete_session_data_internal(session_store: SQLSpecAsyncSessionStore) -> None:
    """Test internal delete session data method."""
    driver = MockDriver()

    await session_store._delete_session_data(driver, "test_session_id")  # type: ignore[arg-type]

    driver.execute.assert_called()


async def test_delete_all_sessions_internal(session_store: SQLSpecAsyncSessionStore) -> None:
    """Test internal delete all sessions method."""
    driver = MockDriver()

    await session_store._delete_all_sessions(driver)  # type: ignore[arg-type]

    driver.execute.assert_called()


async def test_delete_expired_sessions_internal(session_store: SQLSpecAsyncSessionStore) -> None:
    """Test internal delete expired sessions method."""
    driver = MockDriver()
    current_time = datetime.datetime.now(timezone.utc)

    await session_store._delete_expired_sessions(driver, current_time)  # type: ignore[arg-type]

    driver.execute.assert_called()


async def test_get_all_sessions_internal(session_store: SQLSpecAsyncSessionStore) -> None:
    """Test internal get all sessions method."""
    driver = MockDriver()
    current_time = datetime.datetime.now(timezone.utc)
    mock_result = MagicMock()
    mock_result.data = [{"session_id": "session_1", "data": '{"user_id": 1}'}]
    driver.execute.return_value = mock_result

    with patch("sqlspec.extensions.litestar.store.from_json", return_value={"user_id": 1}):
        sessions = []
        async for session_id, session_data in session_store._get_all_sessions(driver, current_time):  # type: ignore[arg-type]
            sessions.append((session_id, session_data))

        assert len(sessions) == 1
        assert sessions[0] == ("session_1", {"user_id": 1})
