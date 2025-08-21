"""Unit tests for SQLSpec session store."""

import datetime
from datetime import timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from sqlspec.core.statement import StatementConfig
from sqlspec.exceptions import SQLSpecError
from sqlspec.extensions.litestar.store import SQLSpecSessionStore, SQLSpecSessionStoreError


class MockDriver:
    """Mock database driver for testing."""

    def __init__(self, dialect: str = "sqlite") -> None:
        self.statement_config = StatementConfig(dialect=dialect)
        self.execute = AsyncMock()


class MockConfig:
    """Mock database config for testing."""

    def __init__(self, driver: MockDriver = None) -> None:
        self._driver = driver or MockDriver()

    def provide_session(self):
        return self

    async def __aenter__(self):
        return self._driver

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


@pytest.fixture()
def mock_config() -> MockConfig:
    """Create a mock database config."""
    return MockConfig()


@pytest.fixture()
def session_store(mock_config: MockConfig) -> SQLSpecSessionStore:
    """Create a session store instance."""
    return SQLSpecSessionStore(mock_config)


@pytest.fixture()
def postgres_store() -> SQLSpecSessionStore:
    """Create a session store for PostgreSQL."""
    return SQLSpecSessionStore(MockConfig(MockDriver("postgres")))


@pytest.fixture()
def mysql_store() -> SQLSpecSessionStore:
    """Create a session store for MySQL."""
    return SQLSpecSessionStore(MockConfig(MockDriver("mysql")))


@pytest.fixture()
def oracle_store() -> SQLSpecSessionStore:
    """Create a session store for Oracle."""
    return SQLSpecSessionStore(MockConfig(MockDriver("oracle")))


def test_session_store_init_defaults(mock_config: MockConfig) -> None:
    """Test session store initialization with defaults."""
    store = SQLSpecSessionStore(mock_config)

    assert store._table_name == "litestar_sessions"
    assert store._session_id_column == "session_id"
    assert store._data_column == "data"
    assert store._expires_at_column == "expires_at"
    assert store._created_at_column == "created_at"
    assert store._table_created is False


def test_session_store_init_custom(mock_config: MockConfig) -> None:
    """Test session store initialization with custom values."""
    store = SQLSpecSessionStore(
        mock_config,
        table_name="custom_sessions",
        session_id_column="id",
        data_column="payload",
        expires_at_column="expires",
        created_at_column="created",
    )

    assert store._table_name == "custom_sessions"
    assert store._session_id_column == "id"
    assert store._data_column == "payload"
    assert store._expires_at_column == "expires"
    assert store._created_at_column == "created"


@pytest.mark.asyncio()
async def test_ensure_table_exists_sqlite(session_store: SQLSpecSessionStore) -> None:
    """Test table creation for SQLite."""
    driver = MockDriver("sqlite")

    await session_store._ensure_table_exists(driver)

    assert driver.execute.call_count == 2  # CREATE TABLE + CREATE INDEX
    assert session_store._table_created is True


@pytest.mark.asyncio()
async def test_ensure_table_exists_postgres(postgres_store: SQLSpecSessionStore) -> None:
    """Test table creation for PostgreSQL."""
    driver = MockDriver("postgres")

    await postgres_store._ensure_table_exists(driver)

    assert driver.execute.call_count == 2  # CREATE TABLE + CREATE INDEX
    assert postgres_store._table_created is True


@pytest.mark.asyncio()
async def test_ensure_table_exists_mysql(mysql_store: SQLSpecSessionStore) -> None:
    """Test table creation for MySQL."""
    driver = MockDriver("mysql")

    await mysql_store._ensure_table_exists(driver)

    assert driver.execute.call_count == 2  # CREATE TABLE + CREATE INDEX
    assert mysql_store._table_created is True


@pytest.mark.asyncio()
async def test_ensure_table_exists_oracle(oracle_store: SQLSpecSessionStore) -> None:
    """Test table creation for Oracle."""
    driver = MockDriver("oracle")

    await oracle_store._ensure_table_exists(driver)

    assert driver.execute.call_count == 2  # CREATE TABLE + CREATE INDEX
    assert oracle_store._table_created is True


@pytest.mark.asyncio()
async def test_ensure_table_exists_generic(mock_config: MockConfig) -> None:
    """Test table creation for generic dialect."""
    store = SQLSpecSessionStore(mock_config)
    driver = MockDriver("unknown")

    await store._ensure_table_exists(driver)

    assert driver.execute.call_count == 2  # CREATE TABLE + CREATE INDEX
    assert store._table_created is True


@pytest.mark.asyncio()
async def test_ensure_table_exists_already_created(session_store: SQLSpecSessionStore) -> None:
    """Test that table creation is skipped when already created."""
    driver = MockDriver()
    session_store._table_created = True

    await session_store._ensure_table_exists(driver)

    driver.execute.assert_not_called()


@pytest.mark.asyncio()
async def test_ensure_table_exists_failure(session_store: SQLSpecSessionStore) -> None:
    """Test table creation failure."""
    driver = MockDriver()
    driver.execute.side_effect = Exception("CREATE TABLE failed")

    with pytest.raises(SQLSpecSessionStoreError, match="Failed to create session table"):
        await session_store._ensure_table_exists(driver)


def test_get_dialect_upsert_sql_postgres(postgres_store: SQLSpecSessionStore) -> None:
    """Test PostgreSQL upsert SQL generation."""
    expires_at = datetime.datetime.now(timezone.utc) + timedelta(hours=1)

    sql = postgres_store._get_dialect_upsert_sql("postgres", "test_id", '{"key": "value"}', expires_at)

    assert sql is not None
    assert not isinstance(sql, list)  # Should be single statement for PostgreSQL


def test_get_dialect_upsert_sql_mysql(mysql_store: SQLSpecSessionStore) -> None:
    """Test MySQL upsert SQL generation."""
    expires_at = datetime.datetime.now(timezone.utc) + timedelta(hours=1)

    sql = mysql_store._get_dialect_upsert_sql("mysql", "test_id", '{"key": "value"}', expires_at)

    assert sql is not None
    assert not isinstance(sql, list)  # Should be single statement for MySQL


def test_get_dialect_upsert_sql_sqlite(session_store: SQLSpecSessionStore) -> None:
    """Test SQLite upsert SQL generation."""
    expires_at = datetime.datetime.now(timezone.utc) + timedelta(hours=1)

    sql = session_store._get_dialect_upsert_sql("sqlite", "test_id", '{"key": "value"}', expires_at)

    assert sql is not None
    assert not isinstance(sql, list)  # Should be single statement for SQLite


def test_get_dialect_upsert_sql_oracle(oracle_store: SQLSpecSessionStore) -> None:
    """Test Oracle upsert SQL generation."""
    expires_at = datetime.datetime.now(timezone.utc) + timedelta(hours=1)

    sql = oracle_store._get_dialect_upsert_sql("oracle", "test_id", '{"key": "value"}', expires_at)

    assert sql is not None
    assert not isinstance(sql, list)  # Should be single statement for Oracle


def test_get_dialect_upsert_sql_fallback(session_store: SQLSpecSessionStore) -> None:
    """Test fallback upsert SQL generation for unsupported dialects."""
    expires_at = datetime.datetime.now(timezone.utc) + timedelta(hours=1)

    sql = session_store._get_dialect_upsert_sql("unsupported", "test_id", '{"key": "value"}', expires_at)

    assert isinstance(sql, list)  # Should be list of DELETE + INSERT statements
    assert len(sql) == 2


@pytest.mark.asyncio()
async def test_get_session_found(session_store: SQLSpecSessionStore) -> None:
    """Test getting existing session data."""
    mock_result = MagicMock()
    mock_result.data = [{"data": '{"user_id": 123}'}]

    with patch("sqlspec.extensions.litestar.store.with_ensure_async_") as mock_context:
        mock_context.return_value.__aenter__ = AsyncMock(return_value=MockDriver())
        mock_context.return_value.__aexit__ = AsyncMock()

        with patch("sqlspec.extensions.litestar.store.ensure_async_") as mock_ensure_async:
            mock_ensure_async.return_value.return_value = mock_result

            with patch("sqlspec.extensions.litestar.store.from_json", return_value={"user_id": 123}) as mock_from_json:
                result = await session_store.get("test_session_id")

                assert result == {"user_id": 123}
                mock_from_json.assert_called_once_with('{"user_id": 123}')


@pytest.mark.asyncio()
async def test_get_session_not_found(session_store: SQLSpecSessionStore) -> None:
    """Test getting non-existent session data."""
    mock_result = MagicMock()
    mock_result.data = []

    with patch("sqlspec.extensions.litestar.store.with_ensure_async_") as mock_context:
        mock_context.return_value.__aenter__ = AsyncMock(return_value=MockDriver())
        mock_context.return_value.__aexit__ = AsyncMock()

        with patch("sqlspec.extensions.litestar.store.ensure_async_") as mock_ensure_async:
            mock_ensure_async.return_value.return_value = mock_result

            result = await session_store.get("non_existent_session")

            assert result is None


@pytest.mark.asyncio()
async def test_get_session_with_renewal(session_store: SQLSpecSessionStore) -> None:
    """Test getting session data with renewal."""
    mock_result = MagicMock()
    mock_result.data = [{"data": '{"user_id": 123}'}]

    with patch("sqlspec.extensions.litestar.store.with_ensure_async_") as mock_context:
        driver = MockDriver()
        mock_context.return_value.__aenter__ = AsyncMock(return_value=driver)
        mock_context.return_value.__aexit__ = AsyncMock()

        with patch("sqlspec.extensions.litestar.store.ensure_async_") as mock_ensure_async:
            mock_ensure_async.return_value.return_value = mock_result

            with patch("sqlspec.extensions.litestar.store.from_json", return_value={"user_id": 123}):
                result = await session_store.get("test_session_id", renew_for=3600)

                assert result == {"user_id": 123}
                assert driver.execute.call_count >= 2  # SELECT + UPDATE


@pytest.mark.asyncio()
async def test_get_session_exception(session_store: SQLSpecSessionStore) -> None:
    """Test getting session data when database error occurs."""
    with patch("sqlspec.extensions.litestar.store.with_ensure_async_") as mock_context:
        mock_context.return_value.__aenter__ = AsyncMock(side_effect=Exception("Database error"))
        mock_context.return_value.__aexit__ = AsyncMock()

        result = await session_store.get("test_session_id")

        assert result is None


@pytest.mark.asyncio()
async def test_set_session_new(session_store: SQLSpecSessionStore) -> None:
    """Test setting new session data."""
    with patch("sqlspec.extensions.litestar.store.with_ensure_async_") as mock_context:
        driver = MockDriver()
        mock_context.return_value.__aenter__ = AsyncMock(return_value=driver)
        mock_context.return_value.__aexit__ = AsyncMock()

        with patch("sqlspec.extensions.litestar.store.to_json", return_value='{"user_id": 123}') as mock_to_json:
            await session_store.set("test_session_id", {"user_id": 123})

            mock_to_json.assert_called_once_with({"user_id": 123})
            driver.execute.assert_called()


@pytest.mark.asyncio()
async def test_set_session_with_timedelta_expires(session_store: SQLSpecSessionStore) -> None:
    """Test setting session data with timedelta expiration."""
    with patch("sqlspec.extensions.litestar.store.with_ensure_async_") as mock_context:
        driver = MockDriver()
        mock_context.return_value.__aenter__ = AsyncMock(return_value=driver)
        mock_context.return_value.__aexit__ = AsyncMock()

        with patch("sqlspec.extensions.litestar.store.to_json", return_value='{"user_id": 123}'):
            await session_store.set("test_session_id", {"user_id": 123}, expires_in=timedelta(hours=2))

            driver.execute.assert_called()


@pytest.mark.asyncio()
async def test_set_session_default_expiration(session_store: SQLSpecSessionStore) -> None:
    """Test setting session data with default expiration."""
    with patch("sqlspec.extensions.litestar.store.with_ensure_async_") as mock_context:
        driver = MockDriver()
        mock_context.return_value.__aenter__ = AsyncMock(return_value=driver)
        mock_context.return_value.__aexit__ = AsyncMock()

        with patch("sqlspec.extensions.litestar.store.to_json", return_value='{"user_id": 123}'):
            await session_store.set("test_session_id", {"user_id": 123})

            driver.execute.assert_called()


@pytest.mark.asyncio()
async def test_set_session_fallback_dialect(session_store: SQLSpecSessionStore) -> None:
    """Test setting session data with fallback dialect (multiple statements)."""
    with patch("sqlspec.extensions.litestar.store.with_ensure_async_") as mock_context:
        driver = MockDriver("unsupported")
        mock_context.return_value.__aenter__ = AsyncMock(return_value=driver)
        mock_context.return_value.__aexit__ = AsyncMock()

        with patch("sqlspec.extensions.litestar.store.to_json", return_value='{"user_id": 123}'):
            await session_store.set("test_session_id", {"user_id": 123})

            assert driver.execute.call_count >= 2  # Multiple statements for fallback


@pytest.mark.asyncio()
async def test_set_session_exception(session_store: SQLSpecSessionStore) -> None:
    """Test setting session data when database error occurs."""
    with patch("sqlspec.extensions.litestar.store.with_ensure_async_") as mock_context:
        driver = MockDriver()
        driver.execute.side_effect = Exception("Database error")
        mock_context.return_value.__aenter__ = AsyncMock(return_value=driver)
        mock_context.return_value.__aexit__ = AsyncMock()

        with patch("sqlspec.extensions.litestar.store.to_json", return_value='{"user_id": 123}'):
            with pytest.raises(SQLSpecSessionStoreError, match="Failed to store session"):
                await session_store.set("test_session_id", {"user_id": 123})


@pytest.mark.asyncio()
async def test_delete_session(session_store: SQLSpecSessionStore) -> None:
    """Test deleting session data."""
    with patch("sqlspec.extensions.litestar.store.with_ensure_async_") as mock_context:
        driver = MockDriver()
        mock_context.return_value.__aenter__ = AsyncMock(return_value=driver)
        mock_context.return_value.__aexit__ = AsyncMock()

        await session_store.delete("test_session_id")

        driver.execute.assert_called()


@pytest.mark.asyncio()
async def test_delete_session_exception(session_store: SQLSpecSessionStore) -> None:
    """Test deleting session data when database error occurs."""
    with patch("sqlspec.extensions.litestar.store.with_ensure_async_") as mock_context:
        driver = MockDriver()
        driver.execute.side_effect = Exception("Database error")
        mock_context.return_value.__aenter__ = AsyncMock(return_value=driver)
        mock_context.return_value.__aexit__ = AsyncMock()

        with pytest.raises(SQLSpecSessionStoreError, match="Failed to delete session"):
            await session_store.delete("test_session_id")


@pytest.mark.asyncio()
async def test_exists_session_true(session_store: SQLSpecSessionStore) -> None:
    """Test checking if session exists (returns True)."""
    mock_result = MagicMock()
    mock_result.data = [{"count": 1}]

    with patch("sqlspec.extensions.litestar.store.with_ensure_async_") as mock_context:
        driver = MockDriver()
        mock_context.return_value.__aenter__ = AsyncMock(return_value=driver)
        mock_context.return_value.__aexit__ = AsyncMock()

        with patch("sqlspec.extensions.litestar.store.ensure_async_") as mock_ensure_async:
            mock_ensure_async.return_value.return_value = mock_result

            result = await session_store.exists("test_session_id")

            assert result is True


@pytest.mark.asyncio()
async def test_exists_session_false(session_store: SQLSpecSessionStore) -> None:
    """Test checking if session exists (returns False)."""
    mock_result = MagicMock()
    mock_result.data = [{"count": 0}]

    with patch("sqlspec.extensions.litestar.store.with_ensure_async_") as mock_context:
        driver = MockDriver()
        mock_context.return_value.__aenter__ = AsyncMock(return_value=driver)
        mock_context.return_value.__aexit__ = AsyncMock()

        with patch("sqlspec.extensions.litestar.store.ensure_async_") as mock_ensure_async:
            mock_ensure_async.return_value.return_value = mock_result

            result = await session_store.exists("non_existent_session")

            assert result is False


@pytest.mark.asyncio()
async def test_exists_session_exception(session_store: SQLSpecSessionStore) -> None:
    """Test checking if session exists when database error occurs."""
    with patch("sqlspec.extensions.litestar.store.with_ensure_async_") as mock_context:
        mock_context.return_value.__aenter__ = AsyncMock(side_effect=Exception("Database error"))
        mock_context.return_value.__aexit__ = AsyncMock()

        result = await session_store.exists("test_session_id")

        assert result is False


@pytest.mark.asyncio()
async def test_expires_in_valid_session(session_store: SQLSpecSessionStore) -> None:
    """Test getting expiration time for valid session."""
    now = datetime.datetime.now(timezone.utc)
    expires_at = now + timedelta(hours=1)
    mock_result = MagicMock()
    mock_result.data = [{"expires_at": expires_at}]

    with patch("sqlspec.extensions.litestar.store.with_ensure_async_") as mock_context:
        driver = MockDriver()
        mock_context.return_value.__aenter__ = AsyncMock(return_value=driver)
        mock_context.return_value.__aexit__ = AsyncMock()

        with patch("sqlspec.extensions.litestar.store.ensure_async_") as mock_ensure_async:
            mock_ensure_async.return_value.return_value = mock_result

            result = await session_store.expires_in("test_session_id")

            assert 3590 <= result <= 3600  # Should be close to 1 hour


@pytest.mark.asyncio()
async def test_expires_in_expired_session(session_store: SQLSpecSessionStore) -> None:
    """Test getting expiration time for expired session."""
    now = datetime.datetime.now(timezone.utc)
    expires_at = now - timedelta(hours=1)  # Expired
    mock_result = MagicMock()
    mock_result.data = [{"expires_at": expires_at}]

    with patch("sqlspec.extensions.litestar.store.with_ensure_async_") as mock_context:
        driver = MockDriver()
        mock_context.return_value.__aenter__ = AsyncMock(return_value=driver)
        mock_context.return_value.__aexit__ = AsyncMock()

        with patch("sqlspec.extensions.litestar.store.ensure_async_") as mock_ensure_async:
            mock_ensure_async.return_value.return_value = mock_result

            result = await session_store.expires_in("test_session_id")

            assert result == 0


@pytest.mark.asyncio()
async def test_expires_in_string_datetime(session_store: SQLSpecSessionStore) -> None:
    """Test getting expiration time when database returns string datetime."""
    now = datetime.datetime.now(timezone.utc)
    expires_at_str = (now + timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
    mock_result = MagicMock()
    mock_result.data = [{"expires_at": expires_at_str}]

    with patch("sqlspec.extensions.litestar.store.with_ensure_async_") as mock_context:
        driver = MockDriver()
        mock_context.return_value.__aenter__ = AsyncMock(return_value=driver)
        mock_context.return_value.__aexit__ = AsyncMock()

        with patch("sqlspec.extensions.litestar.store.ensure_async_") as mock_ensure_async:
            mock_ensure_async.return_value.return_value = mock_result

            result = await session_store.expires_in("test_session_id")

            assert 3590 <= result <= 3600  # Should be close to 1 hour


@pytest.mark.asyncio()
async def test_expires_in_no_session(session_store: SQLSpecSessionStore) -> None:
    """Test getting expiration time for non-existent session."""
    mock_result = MagicMock()
    mock_result.data = []

    with patch("sqlspec.extensions.litestar.store.with_ensure_async_") as mock_context:
        driver = MockDriver()
        mock_context.return_value.__aenter__ = AsyncMock(return_value=driver)
        mock_context.return_value.__aexit__ = AsyncMock()

        with patch("sqlspec.extensions.litestar.store.ensure_async_") as mock_ensure_async:
            mock_ensure_async.return_value.return_value = mock_result

            result = await session_store.expires_in("non_existent_session")

            assert result == 0


@pytest.mark.asyncio()
async def test_expires_in_invalid_datetime_format(session_store: SQLSpecSessionStore) -> None:
    """Test getting expiration time with invalid datetime format."""
    mock_result = MagicMock()
    mock_result.data = [{"expires_at": "invalid_datetime"}]

    with patch("sqlspec.extensions.litestar.store.with_ensure_async_") as mock_context:
        driver = MockDriver()
        mock_context.return_value.__aenter__ = AsyncMock(return_value=driver)
        mock_context.return_value.__aexit__ = AsyncMock()

        with patch("sqlspec.extensions.litestar.store.ensure_async_") as mock_ensure_async:
            mock_ensure_async.return_value.return_value = mock_result

            result = await session_store.expires_in("test_session_id")

            assert result == 0


@pytest.mark.asyncio()
async def test_expires_in_exception(session_store: SQLSpecSessionStore) -> None:
    """Test getting expiration time when database error occurs."""
    with patch("sqlspec.extensions.litestar.store.with_ensure_async_") as mock_context:
        mock_context.return_value.__aenter__ = AsyncMock(side_effect=Exception("Database error"))
        mock_context.return_value.__aexit__ = AsyncMock()

        result = await session_store.expires_in("test_session_id")

        assert result == 0


@pytest.mark.asyncio()
async def test_delete_all_sessions(session_store: SQLSpecSessionStore) -> None:
    """Test deleting all sessions."""
    with patch("sqlspec.extensions.litestar.store.with_ensure_async_") as mock_context:
        driver = MockDriver()
        mock_context.return_value.__aenter__ = AsyncMock(return_value=driver)
        mock_context.return_value.__aexit__ = AsyncMock()

        await session_store.delete_all()

        driver.execute.assert_called()


@pytest.mark.asyncio()
async def test_delete_all_sessions_exception(session_store: SQLSpecSessionStore) -> None:
    """Test deleting all sessions when database error occurs."""
    with patch("sqlspec.extensions.litestar.store.with_ensure_async_") as mock_context:
        driver = MockDriver()
        driver.execute.side_effect = Exception("Database error")
        mock_context.return_value.__aenter__ = AsyncMock(return_value=driver)
        mock_context.return_value.__aexit__ = AsyncMock()

        with pytest.raises(SQLSpecSessionStoreError, match="Failed to delete all sessions"):
            await session_store.delete_all()


@pytest.mark.asyncio()
async def test_delete_expired_sessions(session_store: SQLSpecSessionStore) -> None:
    """Test deleting expired sessions."""
    with patch("sqlspec.extensions.litestar.store.with_ensure_async_") as mock_context:
        driver = MockDriver()
        mock_context.return_value.__aenter__ = AsyncMock(return_value=driver)
        mock_context.return_value.__aexit__ = AsyncMock()

        await session_store.delete_expired()

        driver.execute.assert_called()


@pytest.mark.asyncio()
async def test_delete_expired_sessions_exception(session_store: SQLSpecSessionStore) -> None:
    """Test deleting expired sessions when database error occurs."""
    with patch("sqlspec.extensions.litestar.store.with_ensure_async_") as mock_context:
        driver = MockDriver()
        driver.execute.side_effect = Exception("Database error")
        mock_context.return_value.__aenter__ = AsyncMock(return_value=driver)
        mock_context.return_value.__aexit__ = AsyncMock()

        # Should not raise exception, just log it
        await session_store.delete_expired()


@pytest.mark.asyncio()
async def test_get_all_sessions(session_store: SQLSpecSessionStore) -> None:
    """Test getting all sessions."""
    mock_result = MagicMock()
    mock_result.data = [
        {"session_id": "session_1", "data": '{"user_id": 1}'},
        {"session_id": "session_2", "data": '{"user_id": 2}'},
    ]

    with patch("sqlspec.extensions.litestar.store.with_ensure_async_") as mock_context:
        driver = MockDriver()
        mock_context.return_value.__aenter__ = AsyncMock(return_value=driver)
        mock_context.return_value.__aexit__ = AsyncMock()

        with patch("sqlspec.extensions.litestar.store.ensure_async_") as mock_ensure_async:
            mock_ensure_async.return_value.return_value = mock_result

            with patch("sqlspec.extensions.litestar.store.from_json", side_effect=[{"user_id": 1}, {"user_id": 2}]):
                sessions = []
                async for session_id, session_data in session_store.get_all():
                    sessions.append((session_id, session_data))

                assert len(sessions) == 2
                assert sessions[0] == ("session_1", {"user_id": 1})
                assert sessions[1] == ("session_2", {"user_id": 2})


@pytest.mark.asyncio()
async def test_get_all_sessions_invalid_json(session_store: SQLSpecSessionStore) -> None:
    """Test getting all sessions with invalid JSON data."""
    mock_result = MagicMock()
    mock_result.data = [
        {"session_id": "session_1", "data": '{"user_id": 1}'},
        {"session_id": "session_2", "data": "invalid_json"},
        {"session_id": "session_3", "data": '{"user_id": 3}'},
    ]

    with patch("sqlspec.extensions.litestar.store.with_ensure_async_") as mock_context:
        driver = MockDriver()
        mock_context.return_value.__aenter__ = AsyncMock(return_value=driver)
        mock_context.return_value.__aexit__ = AsyncMock()

        with patch("sqlspec.extensions.litestar.store.ensure_async_") as mock_ensure_async:
            mock_ensure_async.return_value.return_value = mock_result

            def mock_from_json(data):
                if data == "invalid_json":
                    raise ValueError("Invalid JSON")
                return {"user_id": 1} if "1" in data else {"user_id": 3}

            with patch("sqlspec.extensions.litestar.store.from_json", side_effect=mock_from_json):
                sessions = []
                async for session_id, session_data in session_store.get_all():
                    sessions.append((session_id, session_data))

                # Should skip invalid JSON entry
                assert len(sessions) == 2
                assert sessions[0] == ("session_1", {"user_id": 1})
                assert sessions[1] == ("session_3", {"user_id": 3})


@pytest.mark.asyncio()
async def test_get_all_sessions_exception(session_store: SQLSpecSessionStore) -> None:
    """Test getting all sessions when database error occurs."""
    with patch("sqlspec.extensions.litestar.store.with_ensure_async_") as mock_context:
        mock_context.return_value.__aenter__ = AsyncMock(side_effect=Exception("Database error"))
        mock_context.return_value.__aexit__ = AsyncMock()

        sessions = []
        async for session_id, session_data in session_store.get_all():
            sessions.append((session_id, session_data))

        # Should handle exception gracefully and return empty
        assert len(sessions) == 0


def test_generate_session_id() -> None:
    """Test session ID generation."""
    session_id = SQLSpecSessionStore.generate_session_id()

    assert isinstance(session_id, str)
    assert len(session_id) > 0

    # Generate another to ensure they're unique
    another_id = SQLSpecSessionStore.generate_session_id()
    assert session_id != another_id


def test_session_store_error_inheritance() -> None:
    """Test SessionStoreError inheritance."""
    error = SQLSpecSessionStoreError("Test error")

    assert isinstance(error, SQLSpecError)
    assert isinstance(error, Exception)
    assert str(error) == "Test error"


@pytest.mark.asyncio()
async def test_update_expiration(session_store: SQLSpecSessionStore) -> None:
    """Test updating session expiration time."""
    new_expires_at = datetime.datetime.now(timezone.utc) + timedelta(hours=2)
    driver = MockDriver()

    await session_store._update_expiration(driver, "test_session_id", new_expires_at)

    driver.execute.assert_called_once()


@pytest.mark.asyncio()
async def test_update_expiration_exception(session_store: SQLSpecSessionStore) -> None:
    """Test updating session expiration when database error occurs."""
    driver = MockDriver()
    driver.execute.side_effect = Exception("Database error")
    new_expires_at = datetime.datetime.now(timezone.utc) + timedelta(hours=2)

    # Should not raise exception, just log it
    await session_store._update_expiration(driver, "test_session_id", new_expires_at)


@pytest.mark.parametrize(
    "dialect,expected_data_type,expected_timestamp_type",
    [
        ("postgres", "JSONB", "TIMESTAMP WITH TIME ZONE"),
        ("postgresql", "JSONB", "TIMESTAMP WITH TIME ZONE"),
        ("mysql", "JSON", "DATETIME"),
        ("mariadb", "JSON", "DATETIME"),
        ("sqlite", "TEXT", "DATETIME"),
        ("oracle", "JSON", "TIMESTAMP"),
        ("unknown", "TEXT", "TIMESTAMP"),
    ],
)
@pytest.mark.asyncio()
async def test_ensure_table_exists_dialect_types(
    mock_config: MockConfig, dialect: str, expected_data_type: str, expected_timestamp_type: str
) -> None:
    """Test table creation with different dialect-specific types."""
    store = SQLSpecSessionStore(mock_config)
    driver = MockDriver(dialect)

    await store._ensure_table_exists(driver)

    # Verify that execute was called (table creation)
    assert driver.execute.call_count == 2  # CREATE TABLE + CREATE INDEX


@pytest.mark.asyncio()
async def test_get_session_data_internal(session_store: SQLSpecSessionStore) -> None:
    """Test internal get session data method."""
    driver = MockDriver()
    mock_result = MagicMock()
    mock_result.data = [{"data": '{"user_id": 123}'}]

    with patch("sqlspec.extensions.litestar.store.ensure_async_") as mock_ensure_async:
        mock_ensure_async.return_value.return_value = mock_result

        with patch("sqlspec.extensions.litestar.store.from_json", return_value={"user_id": 123}):
            result = await session_store._get_session_data(driver, "test_session_id", None)

            assert result == {"user_id": 123}


@pytest.mark.asyncio()
async def test_set_session_data_internal(session_store: SQLSpecSessionStore) -> None:
    """Test internal set session data method."""
    driver = MockDriver()
    expires_at = datetime.datetime.now(timezone.utc) + timedelta(hours=1)

    await session_store._set_session_data(driver, "test_session_id", '{"user_id": 123}', expires_at)

    driver.execute.assert_called()


@pytest.mark.asyncio()
async def test_delete_session_data_internal(session_store: SQLSpecSessionStore) -> None:
    """Test internal delete session data method."""
    driver = MockDriver()

    await session_store._delete_session_data(driver, "test_session_id")

    driver.execute.assert_called()


@pytest.mark.asyncio()
async def test_delete_all_sessions_internal(session_store: SQLSpecSessionStore) -> None:
    """Test internal delete all sessions method."""
    driver = MockDriver()

    await session_store._delete_all_sessions(driver)

    driver.execute.assert_called()


@pytest.mark.asyncio()
async def test_delete_expired_sessions_internal(session_store: SQLSpecSessionStore) -> None:
    """Test internal delete expired sessions method."""
    driver = MockDriver()
    current_time = datetime.datetime.now(timezone.utc)

    await session_store._delete_expired_sessions(driver, current_time)

    driver.execute.assert_called()


@pytest.mark.asyncio()
async def test_get_all_sessions_internal(session_store: SQLSpecSessionStore) -> None:
    """Test internal get all sessions method."""
    driver = MockDriver()
    current_time = datetime.datetime.now(timezone.utc)
    mock_result = MagicMock()
    mock_result.data = [{"session_id": "session_1", "data": '{"user_id": 1}'}]

    with patch("sqlspec.extensions.litestar.store.ensure_async_") as mock_ensure_async:
        mock_ensure_async.return_value.return_value = mock_result

        with patch("sqlspec.extensions.litestar.store.from_json", return_value={"user_id": 1}):
            sessions = []
            async for session_id, session_data in session_store._get_all_sessions(driver, current_time):
                sessions.append((session_id, session_data))

            assert len(sessions) == 1
            assert sessions[0] == ("session_1", {"user_id": 1})
