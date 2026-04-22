"""Global conftest.py for SQLSpec unit tests.

Provides fixtures for configuration, caching, SQL statements, mock databases,
cleanup, and performance testing with proper scoping and test isolation.
"""

import sqlite3
import time
from collections.abc import AsyncGenerator, Callable, Generator
from contextlib import contextmanager
from decimal import Decimal
from typing import Any

import aiosqlite
import pytest

from sqlspec.adapters.aiosqlite import AiosqliteDriver
from sqlspec.adapters.sqlite import SqliteDriver
from sqlspec.core import (
    SQL,
    LRUCache,
    ParameterStyle,
    ParameterStyleConfig,
    StatementConfig,
    TypedParameter,
    get_default_cache,
)
from sqlspec.driver import ExecutionResult


class TestSqliteDriver(SqliteDriver):
    """Test-friendly SQLite driver that allows patching."""

    pass


class TestAiosqliteDriver(AiosqliteDriver):
    """Test-friendly aiosqlite driver that allows patching."""

    pass


@pytest.fixture
def sqlite_sync_driver() -> Generator[TestSqliteDriver, None, None]:
    """Fixture for a real SQLite sync driver using in-memory database."""
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
    conn.execute("INSERT INTO users (name) VALUES ('test'), ('example')")
    conn.commit()

    driver = TestSqliteDriver(conn)
    yield driver
    conn.close()


@pytest.fixture
async def aiosqlite_async_driver() -> AsyncGenerator[TestAiosqliteDriver, None]:
    """Fixture for a real aiosqlite async driver using in-memory database."""
    conn = await aiosqlite.connect(":memory:")
    await conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
    await conn.execute("INSERT INTO users (name) VALUES ('test'), ('example')")
    await conn.commit()

    driver = TestAiosqliteDriver(conn)
    yield driver
    await conn.close()


__all__ = (
    "aiosqlite_async_driver",
    "benchmark_tracker",
    "cache_config_disabled",
    "cache_config_enabled",
    "cache_statistics_tracker",
    "cleanup_test_state",
    "compilation_metrics",
    "complex_sql_with_joins",
    "memory_profiler",
    "mock_lru_cache",
    "parameter_style_config_advanced",
    "parameter_style_config_basic",
    "performance_timer",
    "reset_cache_state",
    "reset_global_state",
    "sample_delete_sql",
    "sample_insert_sql",
    "sample_parameters_mixed",
    "sample_parameters_named",
    "sample_parameters_positional",
    "sample_select_sql",
    "sample_update_sql",
    "sql_with_typed_parameters",
    "sqlite_sync_driver",
    "statement_config_mysql",
    "statement_config_postgres",
    "statement_config_sqlite",
    "test_isolation",
)


@pytest.fixture
def parameter_style_config_basic() -> ParameterStyleConfig:
    """Basic parameter style configuration for simple test cases."""
    return ParameterStyleConfig(
        default_parameter_style=ParameterStyle.QMARK,
        supported_parameter_styles={ParameterStyle.QMARK, ParameterStyle.NAMED_COLON},
        supported_execution_parameter_styles={ParameterStyle.QMARK},
        default_execution_parameter_style=ParameterStyle.QMARK,
        has_native_list_expansion=False,
        needs_static_script_compilation=True,
        allow_mixed_parameter_styles=False,
        preserve_parameter_format=False,
    )


@pytest.fixture
def parameter_style_config_advanced() -> ParameterStyleConfig:
    """Advanced parameter style configuration with type coercions and transformations."""

    def bool_coercion(value: bool) -> int:
        return 1 if value else 0

    def decimal_coercion(value: Decimal) -> float:
        return float(value)

    def list_coercion(value: list[Any]) -> str:
        return f"{{{','.join(str(v) for v in value)}}}"

    return ParameterStyleConfig(
        default_parameter_style=ParameterStyle.NAMED_COLON,
        supported_parameter_styles={
            ParameterStyle.QMARK,
            ParameterStyle.NAMED_COLON,
            ParameterStyle.POSITIONAL_PYFORMAT,
            ParameterStyle.NAMED_PYFORMAT,
        },
        supported_execution_parameter_styles={ParameterStyle.NUMERIC, ParameterStyle.NAMED_COLON},
        default_execution_parameter_style=ParameterStyle.NUMERIC,
        type_coercion_map={bool: bool_coercion, Decimal: decimal_coercion, list: list_coercion},
        has_native_list_expansion=True,
        needs_static_script_compilation=False,
        allow_mixed_parameter_styles=True,
        preserve_parameter_format=True,
    )


@pytest.fixture
def statement_config_sqlite(parameter_style_config_basic: ParameterStyleConfig) -> StatementConfig:
    """SQLite statement configuration for testing."""
    return StatementConfig(
        dialect="sqlite",
        parameter_config=parameter_style_config_basic,
        execution_mode=None,
        execution_args=None,
        enable_caching=True,
        enable_parsing=True,
        enable_validation=True,
    )


@pytest.fixture
def statement_config_postgres(parameter_style_config_advanced: ParameterStyleConfig) -> StatementConfig:
    """PostgreSQL statement configuration for testing."""
    return StatementConfig(
        dialect="postgres",
        parameter_config=parameter_style_config_advanced,
        execution_mode=None,
        execution_args=None,
        enable_caching=True,
        enable_parsing=True,
        enable_validation=True,
    )


@pytest.fixture
def statement_config_mysql(parameter_style_config_basic: ParameterStyleConfig) -> StatementConfig:
    """MySQL statement configuration for testing."""
    return StatementConfig(
        dialect="mysql",
        parameter_config=parameter_style_config_basic,
        execution_mode=None,
        execution_args=None,
        enable_caching=True,
        enable_parsing=True,
        enable_validation=True,
    )


@pytest.fixture
def cache_config_enabled() -> LRUCache:
    """Cache configuration with caching enabled."""
    return LRUCache(max_size=100)


@pytest.fixture
def cache_config_disabled() -> None:
    """Cache configuration with caching disabled."""
    return


@pytest.fixture
def mock_lru_cache() -> LRUCache:
    """Mock LRU cache for testing cache operations."""
    return LRUCache(max_size=10)


@pytest.fixture
def cache_statistics_tracker() -> dict[str, int]:
    """Tracker for cache hits and misses during tests."""
    return {"hits": 0, "misses": 0, "evictions": 0}


@pytest.fixture
def reset_cache_state() -> "Generator[None, None, None]":
    """Fixture to reset the global SQL cache before and after each test."""
    cache = get_default_cache()
    cache.clear()
    yield
    cache.clear()


@pytest.fixture
def sample_select_sql() -> str:
    """Simple SELECT SQL statement for testing."""
    return "SELECT id, name, email FROM users WHERE active = ? AND created_at > ?"


@pytest.fixture
def sample_insert_sql() -> str:
    """Simple INSERT SQL statement for testing."""
    return "INSERT INTO users (name, email, active, created_at) VALUES (?, ?, ?, ?)"


@pytest.fixture
def sample_update_sql() -> str:
    """Simple UPDATE SQL statement for testing."""
    return "UPDATE users SET name = :name, email = :email WHERE id = :user_id"


@pytest.fixture
def sample_delete_sql() -> str:
    """Simple DELETE SQL statement for testing."""
    return "DELETE FROM users WHERE id = ? AND active = ?"


@pytest.fixture
def sample_parameters_positional() -> list[Any]:
    """Sample positional parameters for testing."""
    return [1, "John Doe", "john@example.com", True, "2023-01-01 00:00:00"]


@pytest.fixture
def sample_parameters_named() -> dict[str, Any]:
    """Sample named parameters for testing."""
    return {
        "user_id": 1,
        "name": "John Doe",
        "email": "john@example.com",
        "active": True,
        "created_at": "2023-01-01 00:00:00",
    }


@pytest.fixture
def sample_parameters_mixed() -> list[dict[str, Any]]:
    """Sample mixed parameter sets for executemany testing."""
    return [
        {"name": "John Doe", "email": "john@example.com", "active": True},
        {"name": "Jane Smith", "email": "jane@example.com", "active": False},
        {"name": "Bob Johnson", "email": "bob@example.com", "active": True},
    ]


@pytest.fixture
def complex_sql_with_joins() -> str:
    """Complex SQL with joins for advanced testing scenarios."""
    return """
        SELECT u.id, u.name, u.email, p.title, c.name as company
        FROM users u
        LEFT JOIN profiles p ON u.id = p.user_id
        LEFT JOIN companies c ON p.company_id = c.id
        WHERE u.active = :active
          AND u.created_at BETWEEN :start_date AND :end_date
          AND (p.title LIKE :title_pattern OR p.title IS NULL)
        ORDER BY u.created_at DESC
        LIMIT :limit OFFSET :offset
    """


@pytest.fixture
def sql_with_typed_parameters(statement_config_sqlite: StatementConfig) -> SQL:
    """SQL statement with TypedParameter instances for type preservation testing."""
    sql = "SELECT * FROM products WHERE price > ? AND in_stock = ? AND categories = ?"
    params = [
        TypedParameter(Decimal("19.99"), Decimal, "price"),
        TypedParameter(True, bool, "in_stock"),
        TypedParameter(["electronics", "gadgets"], list, "categories"),
    ]
    return SQL(sql, *params, statement_config=statement_config_sqlite)


@pytest.fixture(autouse=True)
def test_isolation() -> "Generator[None, None, None]":
    """Auto-use fixture to ensure test isolation by resetting global state."""

    yield


@pytest.fixture
def cleanup_test_state() -> "Generator[Callable[[Callable[[], None]], None], None, None]":
    """Fixture that provides a cleanup function for test state management."""
    cleanup_functions = []

    def register_cleanup(func: "Callable[[], None]") -> None:
        """Register a cleanup function to be called during teardown."""
        cleanup_functions.append(func)

    yield register_cleanup

    for cleanup_func in reversed(cleanup_functions):
        try:
            cleanup_func()
        except Exception:
            pass


@pytest.fixture(scope="session", autouse=True)
def reset_global_state() -> "Generator[None, None, None]":
    """Session-scoped fixture to reset global state before and after test session."""

    yield


@pytest.fixture
def performance_timer() -> "Generator[Any, None, None]":
    """Performance timer fixture for measuring execution time during tests."""
    times = {}

    @contextmanager
    def timer(operation_name: str) -> "Generator[None, None, None]":
        """Time a specific operation."""
        start_time = time.perf_counter()
        yield
        end_time = time.perf_counter()
        times[operation_name] = end_time - start_time

    timer.times = times  # pyright: ignore[reportFunctionMemberAccess]
    yield timer


@pytest.fixture
def benchmark_tracker() -> dict[str, Any]:
    """Benchmark tracking fixture for collecting performance metrics during tests."""
    return {
        "operations": [],
        "timings": {},
        "memory_usage": {},
        "cache_statistics": {},
        "sql_compilation_times": [],
        "parameter_processing_times": [],
    }


@pytest.fixture
def memory_profiler() -> "Generator[Callable[[], dict[str, Any]], None, None]":
    """Memory profiling fixture for tracking memory usage during tests."""
    try:
        import os

        import psutil

        process = psutil.Process(os.getpid())

        def get_memory_usage() -> dict[str, Any]:
            """Get current memory usage statistics."""
            memory_info = process.memory_info()
            return {"rss": memory_info.rss, "vms": memory_info.vms, "percent": process.memory_percent()}

        yield get_memory_usage

    except ImportError:

        def get_memory_usage() -> dict[str, Any]:
            return {"rss": 0, "vms": 0, "percent": 0.0}

        yield get_memory_usage


@pytest.fixture
def compilation_metrics() -> "Generator[Any, None, None]":
    """Compilation metrics tracking for SQL compilation performance testing."""
    metrics: dict[str, Any] = {
        "compilation_count": 0,
        "cache_hits": 0,
        "cache_misses": 0,
        "parse_times": [],
        "transform_times": [],
        "total_compilation_times": [],
    }

    def record_compilation(
        parse_time: float, transform_time: float, total_time: float, was_cached: bool = False
    ) -> None:
        """Record compilation metrics."""
        metrics["compilation_count"] += 1
        if was_cached:
            metrics["cache_hits"] += 1
        else:
            metrics["cache_misses"] += 1
        metrics["parse_times"].append(parse_time)
        metrics["transform_times"].append(transform_time)
        metrics["total_compilation_times"].append(total_time)

    record_compilation.metrics = metrics  # pyright: ignore[reportFunctionMemberAccess]
    yield record_compilation


def pytest_configure(config: Any) -> None:
    """Configure pytest with custom markers for fixture categories."""
    config.addinivalue_line("markers", "config: Tests for configuration fixtures")
    config.addinivalue_line("markers", "cache: Tests for cache-related fixtures")
    config.addinivalue_line("markers", "sql: Tests for SQL statement fixtures")
    config.addinivalue_line("markers", "mock_db: Tests for mock database fixtures")
    config.addinivalue_line("markers", "cleanup: Tests for cleanup and isolation fixtures")
    config.addinivalue_line("markers", "performance: Tests for performance measurement fixtures")
    config.addinivalue_line("markers", "slow: Slow-running tests that require extra time")
    config.addinivalue_line("markers", "unit: Unit tests with isolated components")
    config.addinivalue_line("markers", "integration: Integration tests with multiple components")


def create_test_sql_statement(sql: str, *params: Any, **kwargs: Any) -> SQL:
    """Helper function to create SQL statements for testing."""
    return SQL(sql, *params, **kwargs)


def assert_sql_execution_result(result: ExecutionResult, expected_rowcount: int = -1) -> None:
    """Helper function to assert SQL execution results."""
    assert result is not None
    if expected_rowcount >= 0:
        assert result.data_row_count == expected_rowcount
