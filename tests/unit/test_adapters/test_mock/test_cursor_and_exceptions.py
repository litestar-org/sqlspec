"""Unit tests for cursor management and exception handling."""

import sqlite3

import pytest

from sqlspec.adapters.mock.driver import MockAsyncCursor, MockAsyncExceptionHandler, MockCursor, MockExceptionHandler
from sqlspec.exceptions import UniqueViolationError


def test_mock_cursor_context_manager() -> None:
    """Test MockCursor context manager creates and cleans up cursor."""
    conn = sqlite3.connect(":memory:")

    with MockCursor(conn) as cursor:
        assert cursor is not None
        assert isinstance(cursor, sqlite3.Cursor)
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        assert result[0] == 1

    conn.close()


def test_mock_cursor_cleanup_on_exception() -> None:
    """Test MockCursor cleans up cursor even when exception occurs."""
    conn = sqlite3.connect(":memory:")

    try:
        with MockCursor(conn) as cursor:
            cursor.execute("SELECT 1")
            raise ValueError("Test exception")
    except ValueError:
        pass

    conn.close()


def test_mock_cursor_multiple_operations() -> None:
    """Test MockCursor with multiple operations."""
    conn = sqlite3.connect(":memory:")

    with MockCursor(conn) as cursor:
        cursor.execute("CREATE TABLE test (id INTEGER)")
        cursor.execute("INSERT INTO test VALUES (1)")
        cursor.execute("INSERT INTO test VALUES (2)")
        cursor.execute("SELECT * FROM test")
        results = cursor.fetchall()
        assert len(results) == 2

    conn.close()


def test_mock_exception_handler_no_exception() -> None:
    """Test MockExceptionHandler when no exception occurs."""
    with MockExceptionHandler() as handler:
        pass

    assert handler.pending_exception is None


def test_mock_exception_handler_non_sqlite_exception() -> None:
    """Test MockExceptionHandler passes through non-SQLite exceptions."""
    handler = MockExceptionHandler()
    try:
        with handler:
            raise ValueError("Not a SQLite error")
    except ValueError as e:
        assert str(e) == "Not a SQLite error"
        assert handler.pending_exception is None


def test_mock_exception_handler_sqlite_error() -> None:
    """Test MockExceptionHandler maps SQLite errors."""
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE test (id INTEGER UNIQUE)")
    conn.execute("INSERT INTO test VALUES (1)")

    try:
        conn.execute("INSERT INTO test VALUES (1)")
    except sqlite3.Error as e:
        with MockExceptionHandler() as handler:
            handler.__exit__(type(e), e, None)

        assert handler.pending_exception is not None
        assert isinstance(handler.pending_exception, UniqueViolationError)

    conn.close()


def test_mock_exception_handler_captures_and_suppresses() -> None:
    """Test MockExceptionHandler captures exception and returns True."""
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE test (id INTEGER UNIQUE)")
    conn.execute("INSERT INTO test VALUES (1)")

    with MockExceptionHandler() as handler:
        try:
            conn.execute("INSERT INTO test VALUES (1)")
        except sqlite3.Error as e:
            suppressed = handler.__exit__(type(e), e, None)
            assert suppressed is True
            assert handler.pending_exception is not None

    conn.close()


def test_mock_exception_handler_syntax_error() -> None:
    """Test MockExceptionHandler maps syntax errors."""
    conn = sqlite3.connect(":memory:")

    try:
        conn.execute("INVALID SQL")
    except sqlite3.Error as e:
        with MockExceptionHandler() as handler:
            handler.__exit__(type(e), e, None)

        assert handler.pending_exception is not None

    conn.close()


@pytest.mark.anyio
async def test_mock_async_cursor_context_manager() -> None:
    """Test MockAsyncCursor context manager."""
    conn = sqlite3.connect(":memory:")

    async with MockAsyncCursor(conn) as cursor:
        assert cursor is not None
        assert isinstance(cursor, sqlite3.Cursor)
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        assert result[0] == 1

    conn.close()


@pytest.mark.anyio
async def test_mock_async_cursor_cleanup_on_exception() -> None:
    """Test MockAsyncCursor cleans up even with exception."""
    conn = sqlite3.connect(":memory:")

    try:
        async with MockAsyncCursor(conn) as cursor:
            cursor.execute("SELECT 1")
            raise ValueError("Async test exception")
    except ValueError:
        pass

    conn.close()


@pytest.mark.anyio
async def test_mock_async_cursor_multiple_operations() -> None:
    """Test MockAsyncCursor with multiple operations."""
    conn = sqlite3.connect(":memory:")

    async with MockAsyncCursor(conn) as cursor:
        cursor.execute("CREATE TABLE async_test (id INTEGER)")
        cursor.execute("INSERT INTO async_test VALUES (1)")
        cursor.execute("SELECT * FROM async_test")
        results = cursor.fetchall()
        assert len(results) == 1

    conn.close()


@pytest.mark.anyio
async def test_mock_async_exception_handler_no_exception() -> None:
    """Test MockAsyncExceptionHandler when no exception occurs."""
    async with MockAsyncExceptionHandler() as handler:
        pass

    assert handler.pending_exception is None


@pytest.mark.anyio
async def test_mock_async_exception_handler_non_sqlite_exception() -> None:
    """Test MockAsyncExceptionHandler passes through non-SQLite exceptions."""
    handler = MockAsyncExceptionHandler()
    try:
        async with handler:
            raise ValueError("Not a SQLite error")
    except ValueError as e:
        assert str(e) == "Not a SQLite error"
        assert handler.pending_exception is None


@pytest.mark.anyio
async def test_mock_async_exception_handler_sqlite_error() -> None:
    """Test MockAsyncExceptionHandler maps SQLite errors."""
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE test (id INTEGER UNIQUE)")
    conn.execute("INSERT INTO test VALUES (1)")

    try:
        conn.execute("INSERT INTO test VALUES (1)")
    except sqlite3.Error as e:
        async with MockAsyncExceptionHandler() as handler:
            await handler.__aexit__(type(e), e, None)

        assert handler.pending_exception is not None
        assert isinstance(handler.pending_exception, UniqueViolationError)

    conn.close()


@pytest.mark.anyio
async def test_mock_async_exception_handler_captures_and_suppresses() -> None:
    """Test MockAsyncExceptionHandler captures and suppresses."""
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE test (id INTEGER UNIQUE)")
    conn.execute("INSERT INTO test VALUES (1)")

    async with MockAsyncExceptionHandler() as handler:
        try:
            conn.execute("INSERT INTO test VALUES (1)")
        except sqlite3.Error as e:
            suppressed = await handler.__aexit__(type(e), e, None)
            assert suppressed is True
            assert handler.pending_exception is not None

    conn.close()


def test_cursor_close_failure_suppressed() -> None:
    """Test that cursor close failures are suppressed."""

    class FailingCursor:
        def close(self) -> None:
            raise RuntimeError("Close failed")

    conn = sqlite3.connect(":memory:")
    cursor_manager = MockCursor(conn)
    cursor_manager.cursor = FailingCursor()  # type: ignore[assignment]

    cursor_manager.__exit__(None, None, None)

    conn.close()


@pytest.mark.anyio
async def test_async_cursor_close_failure_suppressed() -> None:
    """Test that async cursor close failures are suppressed."""

    class FailingCursor:
        def close(self) -> None:
            raise RuntimeError("Async close failed")

    conn = sqlite3.connect(":memory:")
    cursor_manager = MockAsyncCursor(conn)
    cursor_manager.cursor = FailingCursor()  # type: ignore[assignment]

    await cursor_manager.__aexit__(None, None, None)

    conn.close()
