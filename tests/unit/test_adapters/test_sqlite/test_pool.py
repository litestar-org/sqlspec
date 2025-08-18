"""Unit tests for SQLite thread-local connection pool."""

import sqlite3
import threading
import time
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any, cast
from unittest.mock import MagicMock, patch

import pytest

from sqlspec.adapters.sqlite.pool import SqliteConnectionParams, SqliteConnectionPool


@pytest.fixture
def mock_sqlite_connection() -> MagicMock:
    """Create a mock SQLite connection."""
    connection = MagicMock(spec=sqlite3.Connection)
    connection.execute.return_value = None
    connection.close.return_value = None
    return connection


@pytest.fixture
def basic_connection_params() -> "dict[str, Any]":
    """Basic connection parameters for testing."""
    return {"database": ":memory:", "timeout": 10.0}


@pytest.fixture
def file_connection_params() -> "dict[str, Any]":
    """File-based connection parameters for testing."""
    return {"database": "test.db", "timeout": 5.0, "check_same_thread": False}


@pytest.fixture
def pool_with_basic_params(basic_connection_params: "dict[str, Any]") -> SqliteConnectionPool:
    """Create a pool with basic connection parameters."""
    return SqliteConnectionPool(basic_connection_params)


class MockConnection:
    """Mock connection that tracks execute calls."""

    def __init__(self, database: str = ":memory:"):
        self.database = database
        self.execute_calls: list[str] = []
        self.closed = False

    def execute(self, sql: str) -> None:
        """Mock execute that tracks calls."""
        if self.closed:
            raise sqlite3.ProgrammingError("Cannot operate on a closed database")
        self.execute_calls.append(sql)

    def close(self) -> None:
        """Mock close method."""
        self.closed = True


def _cast_mock_connection(mock_conn: MockConnection) -> sqlite3.Connection:
    """Helper to cast mock connection to the proper type."""
    return cast(sqlite3.Connection, mock_conn)


def test_pool_initialization(basic_connection_params: "dict[str, Any]") -> None:
    """Test pool initialization with various parameters."""
    pool = SqliteConnectionPool(basic_connection_params, enable_optimizations=True)

    assert pool._connection_parameters == basic_connection_params
    assert pool._enable_optimizations is True
    assert hasattr(pool, "_thread_local")


def test_pool_initialization_with_disabled_optimizations(basic_connection_params: "dict[str, Any]") -> None:
    """Test pool initialization with optimizations disabled."""
    pool = SqliteConnectionPool(basic_connection_params, enable_optimizations=False)

    assert pool._enable_optimizations is False


def test_pool_initialization_with_pool_kwargs(basic_connection_params: "dict[str, Any]") -> None:
    """Test that pool initialization ignores pool parameters for compatibility."""
    pool = SqliteConnectionPool(
        basic_connection_params, enable_optimizations=True, pool_size=10, max_overflow=5, timeout=30
    )

    assert pool._connection_parameters == basic_connection_params


@patch("sqlspec.adapters.sqlite.pool.sqlite3.connect")
def test_create_connection_memory_database(mock_connect: MagicMock, basic_connection_params: "dict[str, Any]") -> None:
    """Test connection creation for memory database."""
    mock_connection = MockConnection(":memory:")
    mock_connect.return_value = mock_connection

    pool = SqliteConnectionPool(basic_connection_params, enable_optimizations=True)
    connection = pool._create_connection()

    mock_connect.assert_called_once_with(**basic_connection_params)
    assert connection == _cast_mock_connection(mock_connection)

    expected_pragmas = ["PRAGMA foreign_keys = ON", "PRAGMA synchronous = NORMAL"]

    for pragma in expected_pragmas:
        assert pragma in mock_connection.execute_calls


@patch("sqlspec.adapters.sqlite.pool.sqlite3.connect")
def test_create_connection_file_database(mock_connect: MagicMock, file_connection_params: "dict[str, Any]") -> None:
    """Test connection creation for file database."""
    mock_connection = MockConnection("test.db")
    mock_connect.return_value = mock_connection

    pool = SqliteConnectionPool(file_connection_params, enable_optimizations=True)
    connection = pool._create_connection()

    mock_connect.assert_called_once_with(**file_connection_params)
    assert connection == _cast_mock_connection(mock_connection)

    expected_pragmas = [
        "PRAGMA journal_mode = WAL",
        "PRAGMA busy_timeout = 5000",
        "PRAGMA optimize",
        "PRAGMA foreign_keys = ON",
        "PRAGMA synchronous = NORMAL",
    ]

    for pragma in expected_pragmas:
        assert pragma in mock_connection.execute_calls


@patch("sqlspec.adapters.sqlite.pool.sqlite3.connect")
def test_create_connection_shared_memory_database(mock_connect: MagicMock) -> None:
    """Test connection creation for shared memory database."""
    connection_params = {"database": "file::memory:?cache=shared", "uri": True}
    mock_connection = MockConnection("file::memory:?cache=shared")
    mock_connect.return_value = mock_connection

    pool = SqliteConnectionPool(connection_params, enable_optimizations=True)
    pool._create_connection()

    expected_pragmas = ["PRAGMA foreign_keys = ON", "PRAGMA synchronous = NORMAL"]

    for pragma in expected_pragmas:
        assert pragma in mock_connection.execute_calls


@patch("sqlspec.adapters.sqlite.pool.sqlite3.connect")
def test_create_connection_no_optimizations(mock_connect: MagicMock, basic_connection_params: "dict[str, Any]") -> None:
    """Test connection creation with optimizations disabled."""
    mock_connection = MockConnection()
    mock_connect.return_value = mock_connection

    pool = SqliteConnectionPool(basic_connection_params, enable_optimizations=False)
    pool._create_connection()

    assert len(mock_connection.execute_calls) == 0


def test_thread_local_connection_isolation() -> None:
    """Test that each thread gets its own connection."""
    connection_params = {"database": ":memory:"}
    pool = SqliteConnectionPool(connection_params)

    connections: dict[int, Any] = {}

    def get_connection_in_thread(thread_id: int) -> None:
        """Get connection in a specific thread."""
        with patch("sqlspec.adapters.sqlite.pool.sqlite3.connect") as mock_connect:
            mock_connect.return_value = MockConnection(f"thread_{thread_id}")
            connections[thread_id] = pool._get_thread_connection()

    threads = []
    for i in range(3):
        thread = threading.Thread(target=get_connection_in_thread, args=(i,))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    assert len(connections) == 3
    connection_ids = [id(conn) for conn in connections.values()]
    assert len(set(connection_ids)) == 3


def test_thread_local_connection_reuse() -> None:
    """Test that the same thread reuses the same connection."""
    connection_params = {"database": ":memory:"}
    pool = SqliteConnectionPool(connection_params)

    with patch("sqlspec.adapters.sqlite.pool.sqlite3.connect") as mock_connect:
        mock_connect.return_value = MockConnection()

        conn1 = pool._get_thread_connection()
        conn2 = pool._get_thread_connection()

        assert conn1 is conn2

        assert mock_connect.call_count == 1


@patch("sqlspec.adapters.sqlite.pool.sqlite3.connect")
def test_get_connection_context_manager(mock_connect: MagicMock, basic_connection_params: "dict[str, Any]") -> None:
    """Test the get_connection context manager."""
    mock_connection = MockConnection()
    mock_connect.return_value = mock_connection

    pool = SqliteConnectionPool(basic_connection_params)

    with pool.get_connection() as connection:
        assert connection == _cast_mock_connection(mock_connection)

    assert not mock_connection.closed


@patch("sqlspec.adapters.sqlite.pool.sqlite3.connect")
def test_acquire_and_release(mock_connect: MagicMock, basic_connection_params: "dict[str, Any]") -> None:
    """Test acquire and release methods."""
    mock_connection = MockConnection()
    mock_connect.return_value = mock_connection

    pool = SqliteConnectionPool(basic_connection_params)

    connection = pool.acquire()
    assert connection == _cast_mock_connection(mock_connection)

    pool.release(connection)

    connection2 = pool.acquire()
    assert connection2 is connection


@patch("sqlspec.adapters.sqlite.pool.sqlite3.connect")
def test_close_thread_connection(mock_connect: MagicMock, basic_connection_params: "dict[str, Any]") -> None:
    """Test closing thread-local connection."""
    mock_connection = MockConnection()
    mock_connect.return_value = mock_connection

    pool = SqliteConnectionPool(basic_connection_params)

    connection = pool._get_thread_connection()
    assert connection == _cast_mock_connection(mock_connection)

    pool._close_thread_connection()
    assert mock_connection.closed

    mock_connect.return_value = MockConnection()
    new_connection = pool._get_thread_connection()
    assert new_connection is not connection


def test_close_thread_connection_no_connection() -> None:
    """Test closing thread-local connection when none exists."""
    pool = SqliteConnectionPool({"database": ":memory:"})

    pool._close_thread_connection()


@patch("sqlspec.adapters.sqlite.pool.sqlite3.connect")
def test_pool_size_methods(mock_connect: MagicMock, basic_connection_params: "dict[str, Any]") -> None:
    """Test pool size and checked_out methods."""
    mock_connection = MockConnection()
    mock_connect.return_value = mock_connection

    pool = SqliteConnectionPool(basic_connection_params)

    assert pool.size() == 0
    assert pool.checked_out() == 0

    _ = pool._get_thread_connection()

    assert pool.size() == 1
    assert pool.checked_out() == 0


@patch("sqlspec.adapters.sqlite.pool.sqlite3.connect")
def test_pool_close(mock_connect: MagicMock, basic_connection_params: "dict[str, Any]") -> None:
    """Test pool close method."""
    mock_connection = MockConnection()
    mock_connect.return_value = mock_connection

    pool = SqliteConnectionPool(basic_connection_params)

    _ = pool._get_thread_connection()

    pool.close()

    assert mock_connection.closed


def test_connection_params_typing() -> None:
    """Test SqliteConnectionParams typing functionality."""

    params: SqliteConnectionParams = {
        "database": "test.db",
        "timeout": 30.0,
        "detect_types": sqlite3.PARSE_DECLTYPES,
        "isolation_level": "DEFERRED",
        "check_same_thread": False,
        "factory": None,
        "cached_statements": 100,
        "uri": True,
    }

    pool = SqliteConnectionPool(dict(params))
    assert pool._connection_parameters == params

    minimal_params: SqliteConnectionParams = {"database": ":memory:"}
    pool2 = SqliteConnectionPool(dict(minimal_params))
    assert pool2._connection_parameters == minimal_params


def test_thread_safety_concurrent_access() -> None:
    """Test thread safety with concurrent access."""
    connection_params = {"database": ":memory:"}
    pool = SqliteConnectionPool(connection_params)

    results: dict[str, Any] = {}
    errors: list[Exception] = []

    def thread_worker(thread_id: int) -> None:
        """Worker function for thread testing."""
        try:
            with patch("sqlspec.adapters.sqlite.pool.sqlite3.connect") as mock_connect:
                mock_connect.return_value = MockConnection(f"thread_{thread_id}")

                for _ in range(10):
                    conn = pool.acquire()
                    results[f"{thread_id}_{len(results)}"] = id(conn)
                    pool.release(conn)
                    time.sleep(0.001)

        except Exception as e:
            errors.append(e)

    threads = []
    for i in range(5):
        thread = threading.Thread(target=thread_worker, args=(i,))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    assert len(errors) == 0

    assert len(results) == 50


@patch("sqlspec.adapters.sqlite.pool.sqlite3.connect")
def test_connection_creation_failure(mock_connect: MagicMock, basic_connection_params: "dict[str, Any]") -> None:
    """Test handling of connection creation failures."""

    mock_connect.side_effect = sqlite3.Error("Database unavailable")

    pool = SqliteConnectionPool(basic_connection_params)

    with pytest.raises(sqlite3.Error, match="Database unavailable"):
        pool._create_connection()


@patch("sqlspec.adapters.sqlite.pool.sqlite3.connect")
def test_pragma_execution_failure(mock_connect: MagicMock, file_connection_params: "dict[str, Any]") -> None:
    """Test handling of PRAGMA execution failures."""
    mock_connection = MockConnection("test.db")

    original_execute = mock_connection.execute

    def failing_execute(sql: str) -> None:
        if "journal_mode" in sql:
            raise sqlite3.Error("PRAGMA failed")
        original_execute(sql)

    mock_connection.execute = failing_execute
    mock_connect.return_value = mock_connection

    pool = SqliteConnectionPool(file_connection_params, enable_optimizations=True)

    with pytest.raises(sqlite3.Error, match="PRAGMA failed"):
        pool._create_connection()


def test_memory_database_detection() -> None:
    """Test various memory database path detection."""
    test_cases = [
        (":memory:", True),
        ("file::memory:", True),
        ("file::memory:?cache=shared", True),
        ("test.db", False),
        ("/path/to/file.db", False),
        ("file:test.db", False),
        ("file:test.db?mode=memory", False),
    ]

    for database_path, is_memory in test_cases:
        params = {"database": database_path}

        with patch("sqlspec.adapters.sqlite.pool.sqlite3.connect") as mock_connect:
            mock_connection = MockConnection(database_path)
            mock_connect.return_value = mock_connection

            pool = SqliteConnectionPool(params, enable_optimizations=True)
            pool._create_connection()

            wal_pragma_called = any("journal_mode = WAL" in call for call in mock_connection.execute_calls)

            if is_memory:
                assert not wal_pragma_called, f"WAL pragma should not be called for memory database: {database_path}"
            else:
                assert wal_pragma_called, f"WAL pragma should be called for file database: {database_path}"


@contextmanager
def assert_no_thread_local_leak() -> "Generator[None, None, None]":
    """Context manager to assert no thread-local storage leaks."""
    initial_thread_count = threading.active_count()
    yield

    time.sleep(0.1)
    final_thread_count = threading.active_count()
    assert final_thread_count <= initial_thread_count, "Thread-local storage may have leaked"


def test_no_thread_local_memory_leak() -> None:
    """Test that thread-local connections don't cause memory leaks."""
    connection_params = {"database": ":memory:"}

    with assert_no_thread_local_leak():
        pool = SqliteConnectionPool(connection_params)

        def worker() -> None:
            with patch("sqlspec.adapters.sqlite.pool.sqlite3.connect") as mock_connect:
                mock_connect.return_value = MockConnection()

                conn = pool._get_thread_connection()
                assert conn is not None

                pool._close_thread_connection()

        threads = []
        for _ in range(10):
            thread = threading.Thread(target=worker)
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()
