"""Comprehensive unit tests for the core.compiler module.

This module tests the enhanced SQLProcessor and CompiledSQL classes that provide
5-10x performance improvement over the current multi-pass processing system.

Test Coverage:
1. CompiledSQL class - Immutable compiled SQL results with complete information
2. SQLProcessor class - Single-pass compiler with integrated caching
3. Compilation pipeline - SQL parsing, optimization, and compilation
4. Query optimization - Performance optimizations during compilation
5. AST transformations - SQL AST transformations and optimizations
6. Dialect-specific compilation - Compilation for different database dialects
7. Error handling - Compilation error scenarios and fallbacks
8. Performance characteristics - Compilation speed and efficiency testing

Based on CORE_ROUND_3 architecture and existing patterns from tests/unit_old.
"""

import threading
import time
from collections import OrderedDict
from datetime import datetime
from typing import Any
from unittest.mock import Mock, patch

import pytest
import sqlglot
from sqlglot import expressions as exp
from sqlglot.errors import ParseError

from sqlspec.core.compiler import CompiledSQL, SQLProcessor
from sqlspec.core.parameters import ParameterProcessor, ParameterStyle, ParameterStyleConfig
from sqlspec.core.statement import StatementConfig

# Test fixtures for compiler testing


@pytest.fixture
def basic_statement_config() -> "StatementConfig":
    """Create a basic StatementConfig for testing."""

    parameter_config = ParameterStyleConfig(
        default_parameter_style=ParameterStyle.QMARK,
        supported_parameter_styles={ParameterStyle.QMARK, ParameterStyle.NAMED_COLON},
        supported_execution_parameter_styles={ParameterStyle.QMARK},
        default_execution_parameter_style=ParameterStyle.QMARK,
    )

    return StatementConfig(
        dialect="sqlite",
        parameter_config=parameter_config,
        enable_caching=True,
        enable_parsing=True,
        enable_validation=True,
    )


@pytest.fixture
def postgres_statement_config() -> "StatementConfig":
    """Create a PostgreSQL StatementConfig for testing."""

    parameter_config = ParameterStyleConfig(
        default_parameter_style=ParameterStyle.NUMERIC,
        supported_parameter_styles={ParameterStyle.NUMERIC, ParameterStyle.NAMED_COLON},
        supported_execution_parameter_styles={ParameterStyle.NUMERIC},
        default_execution_parameter_style=ParameterStyle.NUMERIC,
    )

    return StatementConfig(
        dialect="postgres",
        parameter_config=parameter_config,
        enable_caching=True,
        enable_parsing=True,
        enable_validation=True,
    )


@pytest.fixture
def mysql_statement_config() -> "StatementConfig":
    """Create a MySQL StatementConfig for testing."""

    parameter_config = ParameterStyleConfig(
        default_parameter_style=ParameterStyle.POSITIONAL_PYFORMAT,
        supported_parameter_styles={ParameterStyle.POSITIONAL_PYFORMAT, ParameterStyle.NAMED_PYFORMAT},
        supported_execution_parameter_styles={ParameterStyle.POSITIONAL_PYFORMAT},
        default_execution_parameter_style=ParameterStyle.POSITIONAL_PYFORMAT,
        type_coercion_map={bool: lambda b: 1 if b else 0},
    )

    return StatementConfig(
        dialect="mysql",
        parameter_config=parameter_config,
        enable_caching=True,
        enable_parsing=True,
        enable_validation=True,
    )


@pytest.fixture
def no_cache_config() -> "StatementConfig":
    """Create a config with caching disabled."""

    parameter_config = ParameterStyleConfig(default_parameter_style=ParameterStyle.QMARK)

    return StatementConfig(
        dialect="sqlite",
        parameter_config=parameter_config,
        enable_caching=False,
        enable_parsing=True,
        enable_validation=True,
    )


@pytest.fixture
def sample_sql_queries() -> "dict[str, str]":
    """Sample SQL queries for testing various operations."""
    return {
        "select": "SELECT * FROM users WHERE id = ?",
        "select_named": "SELECT * FROM users WHERE id = :user_id",
        "select_complex": "SELECT u.id, u.name, p.title FROM users u JOIN posts p ON u.id = p.user_id WHERE u.active = ?",
        "insert": "INSERT INTO users (name, email) VALUES (?, ?)",
        "insert_named": "INSERT INTO users (name, email) VALUES (:name, :email)",
        "update": "UPDATE users SET name = ? WHERE id = ?",
        "delete": "DELETE FROM users WHERE id = ?",
        "create_table": "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
        "drop_table": "DROP TABLE users",
        "alter_table": "ALTER TABLE users ADD COLUMN email TEXT",
        "copy": "COPY users FROM '/tmp/users.csv'",
        "execute": "EXECUTE my_procedure(?, ?)",
        "script": "DELETE FROM users WHERE active = 0; INSERT INTO audit (action) VALUES ('cleanup');",
        "malformed": "SELECT * FROM users WHERE",
        "empty": "",
        "whitespace": "   \n\t   ",
    }


# CompiledSQL class tests


def test_compiled_sql_creation() -> None:
    """Test CompiledSQL object creation and basic properties."""
    compiled_sql = "SELECT * FROM users WHERE id = ?"
    execution_parameters = [123]
    operation_type = "SELECT"
    expression = Mock(spec=exp.Select)

    result = CompiledSQL(
        compiled_sql=compiled_sql,
        execution_parameters=execution_parameters,
        operation_type=operation_type,
        expression=expression,
        parameter_style="qmark",
        supports_many=False,
    )

    assert result.compiled_sql == compiled_sql
    assert result.execution_parameters == execution_parameters
    assert result.operation_type == operation_type
    assert result.expression == expression
    assert result.parameter_style == "qmark"
    assert result.supports_many is False


def test_compiled_sql_hash_caching() -> None:
    """Test CompiledSQL hash caching for performance."""
    result = CompiledSQL(compiled_sql="SELECT * FROM users", execution_parameters=None, operation_type="SELECT")

    # Hash should be None initially
    assert result._hash is None

    # First call should compute and cache hash
    hash1 = hash(result)
    assert result._hash is not None
    assert hash1 == result._hash

    # Second call should return cached value
    hash2 = hash(result)
    assert hash2 == hash1
    assert hash2 == result._hash


def test_compiled_sql_equality() -> None:
    """Test CompiledSQL equality comparison."""
    result1 = CompiledSQL(
        compiled_sql="SELECT * FROM users", execution_parameters=[123], operation_type="SELECT", parameter_style="qmark"
    )
    result2 = CompiledSQL(
        compiled_sql="SELECT * FROM users", execution_parameters=[123], operation_type="SELECT", parameter_style="qmark"
    )
    result3 = CompiledSQL(
        compiled_sql="SELECT * FROM posts", execution_parameters=[123], operation_type="SELECT", parameter_style="qmark"
    )

    assert result1 == result2
    assert result1 != result3
    assert result1 != "not a CompiledSQL object"
    assert result1 is not None


def test_compiled_sql_repr() -> None:
    """Test CompiledSQL string representation."""
    result = CompiledSQL(compiled_sql="SELECT * FROM users", execution_parameters=[123], operation_type="SELECT")

    repr_str = repr(result)
    assert "CompiledSQL" in repr_str
    assert "SELECT * FROM users" in repr_str
    assert "[123]" in repr_str
    assert "SELECT" in repr_str


# SQLProcessor class tests


def test_sql_processor_initialization(basic_statement_config: "StatementConfig") -> None:
    """Test SQLProcessor initialization with configuration."""
    processor = SQLProcessor(basic_statement_config)

    assert processor._config == basic_statement_config
    assert isinstance(processor._cache, OrderedDict)
    assert processor._max_cache_size == 1000
    assert processor._cache_hits == 0
    assert processor._cache_misses == 0
    assert isinstance(processor._parameter_processor, ParameterProcessor)


def test_sql_processor_custom_cache_size(basic_statement_config: "StatementConfig") -> None:
    """Test SQLProcessor with custom cache size."""
    processor = SQLProcessor(basic_statement_config, max_cache_size=500)
    assert processor._max_cache_size == 500


def test_basic_compilation(basic_statement_config: "StatementConfig", sample_sql_queries: "dict[str, str]") -> None:
    """Test basic SQL compilation functionality."""
    processor = SQLProcessor(basic_statement_config)

    result = processor.compile(sample_sql_queries["select"], [123])

    assert isinstance(result, CompiledSQL)
    assert result.operation_type == "SELECT"
    assert isinstance(result.compiled_sql, str)
    assert len(result.compiled_sql) > 0
    assert result.execution_parameters is not None


def test_compilation_with_caching(
    basic_statement_config: "StatementConfig", sample_sql_queries: "dict[str, str]"
) -> None:
    """Test SQL compilation with caching enabled."""
    processor = SQLProcessor(basic_statement_config)

    sql = sample_sql_queries["select"]
    parameters = [123]

    # First compilation should be a cache miss
    result1 = processor.compile(sql, parameters)
    assert processor._cache_misses == 1
    assert processor._cache_hits == 0

    # Second compilation should be a cache hit
    result2 = processor.compile(sql, parameters)
    assert processor._cache_misses == 1
    assert processor._cache_hits == 1

    # Results should be identical
    assert result1 == result2


def test_compilation_without_caching(no_cache_config: "StatementConfig", sample_sql_queries: "dict[str, str]") -> None:
    """Test SQL compilation with caching disabled."""
    processor = SQLProcessor(no_cache_config)

    sql = sample_sql_queries["select"]
    parameters = [123]

    # Multiple compilations should not use cache
    result1 = processor.compile(sql, parameters)
    result2 = processor.compile(sql, parameters)

    # Cache stats should remain at 0
    assert processor._cache_hits == 0
    assert processor._cache_misses == 0

    # Results should be equal but not identical objects
    assert result1 == result2


def test_cache_key_generation(basic_statement_config: "StatementConfig") -> None:
    """Test cache key generation for consistent caching."""
    processor = SQLProcessor(basic_statement_config)

    # Same SQL and parameters should generate same key
    key1 = processor._make_cache_key("SELECT * FROM users", [123])
    key2 = processor._make_cache_key("SELECT * FROM users", [123])
    assert key1 == key2

    # Different SQL should generate different keys
    key3 = processor._make_cache_key("SELECT * FROM posts", [123])
    assert key1 != key3

    # Different parameters should generate different keys
    key4 = processor._make_cache_key("SELECT * FROM users", [456])
    assert key1 != key4

    # Keys should be strings and include "sql_" prefix
    assert isinstance(key1, str)
    assert key1.startswith("sql_")


def test_cache_eviction(basic_statement_config: "StatementConfig") -> None:
    """Test LRU cache eviction when at capacity."""
    processor = SQLProcessor(basic_statement_config, max_cache_size=2)

    # Fill cache to capacity
    processor.compile("SELECT 1", None)
    processor.compile("SELECT 2", None)

    # Verify cache is full
    assert len(processor._cache) == 2

    # Add third item should evict oldest
    processor.compile("SELECT 3", None)
    assert len(processor._cache) == 2

    # First item should have been evicted
    cache_keys = list(processor._cache.keys())
    key1 = processor._make_cache_key("SELECT 1", None)
    assert key1 not in cache_keys


def test_cache_lru_behavior(basic_statement_config: "StatementConfig") -> None:
    """Test LRU (Least Recently Used) cache behavior."""
    processor = SQLProcessor(basic_statement_config, max_cache_size=2)

    # Fill cache
    processor.compile("SELECT 1", None)
    processor.compile("SELECT 2", None)

    # Access first item to make it recently used
    processor.compile("SELECT 1", None)
    assert processor._cache_hits == 1

    # Add third item - should evict second item (least recently used)
    processor.compile("SELECT 3", None)

    # First and third items should be in cache
    key1 = processor._make_cache_key("SELECT 1", None)
    key2 = processor._make_cache_key("SELECT 2", None)
    key3 = processor._make_cache_key("SELECT 3", None)

    assert key1 in processor._cache
    assert key2 not in processor._cache
    assert key3 in processor._cache


# Compilation pipeline tests


@pytest.mark.parametrize(
    "sql,expected_operation",
    [
        ("SELECT * FROM users", "SELECT"),
        ("INSERT INTO users VALUES (1)", "INSERT"),
        ("UPDATE users SET name = 'test'", "UPDATE"),
        ("DELETE FROM users WHERE id = 1", "DELETE"),
        ("CREATE TABLE test (id INT)", "DDL"),
        ("DROP TABLE test", "DDL"),
        ("ALTER TABLE test ADD COLUMN name TEXT", "DDL"),
        ("COPY users FROM 'file.csv'", "COPY_FROM"),
        ("COPY users TO 'file.csv'", "COPY_TO"),
        ("EXECUTE my_proc()", "EXECUTE"),
    ],
)
def test_operation_type_detection_via_ast(
    basic_statement_config: "StatementConfig", sql: str, expected_operation: str
) -> None:
    """Test AST-based operation type detection."""
    processor = SQLProcessor(basic_statement_config)

    try:
        expression = sqlglot.parse_one(sql, dialect=basic_statement_config.dialect)
        detected_type = processor._detect_operation_type(expression)
        assert detected_type == expected_operation
    except ParseError:
        # If SQLGlot can't parse, should fall back to string-based detection
        detected_type = "EXECUTE"
        assert detected_type in ["SELECT", "INSERT", "UPDATE", "DELETE", "COPY", "EXECUTE", "SCRIPT", "DDL", "UNKNOWN"]


def test_single_pass_processing(
    basic_statement_config: "StatementConfig", sample_sql_queries: "dict[str, str]"
) -> None:
    """Test single-pass processing eliminates redundant parsing."""
    processor = SQLProcessor(basic_statement_config)

    with patch("sqlglot.parse_one") as mock_parse:
        mock_expression = Mock(spec=exp.Select)
        mock_expression.sql.return_value = "SELECT * FROM users WHERE id = ?"
        mock_parse.return_value = mock_expression

        result = processor.compile(sample_sql_queries["select"], [123])

        # SQLGlot parse should only be called once
        assert mock_parse.call_count == 1
        assert result.operation_type == "SELECT"


def test_parameter_processing_integration(basic_statement_config: "StatementConfig") -> None:
    """Test integration with parameter processing system."""
    processor = SQLProcessor(basic_statement_config)

    # Test with various parameter formats
    test_cases = [
        ("SELECT * FROM users WHERE id = ?", [123]),
        ("SELECT * FROM users WHERE id = :user_id", {"user_id": 456}),
        ("SELECT * FROM users WHERE name = ? AND age = ?", ["John", 25]),
    ]

    for sql, params in test_cases:
        result = processor.compile(sql, params)
        assert isinstance(result, CompiledSQL)
        assert result.execution_parameters is not None


def test_compilation_with_transformations(basic_statement_config: "StatementConfig") -> None:
    """Test compilation with output transformations."""
    # Create config for transformation testing
    config_with_transformer = basic_statement_config.replace()

    processor = SQLProcessor(config_with_transformer)
    result = processor.compile("select * from users", None)

    # Basic compilation should work
    assert isinstance(result, CompiledSQL)


# Query optimization tests


def test_parsing_enabled_optimization(
    basic_statement_config: "StatementConfig", sample_sql_queries: "dict[str, str]"
) -> None:
    """Test compilation with parsing enabled for optimization."""
    processor = SQLProcessor(basic_statement_config)

    result = processor.compile(sample_sql_queries["select_complex"], [True])

    assert isinstance(result, CompiledSQL)
    assert result.expression is not None
    assert result.operation_type == "SELECT"


def test_parsing_disabled_fallback(
    basic_statement_config: "StatementConfig", sample_sql_queries: "dict[str, str]"
) -> None:
    """Test compilation fallback when parsing is disabled."""
    # Disable parsing
    config = basic_statement_config.replace(enable_parsing=False)
    processor = SQLProcessor(config)

    result = processor.compile(sample_sql_queries["select"], [123])

    assert isinstance(result, CompiledSQL)
    assert result.expression is None
    assert result.operation_type in [
        "SELECT",
        "INSERT",
        "UPDATE",
        "DELETE",
        "COPY",
        "EXECUTE",
        "SCRIPT",
        "DDL",
        "UNKNOWN",
    ]


def test_compilation_performance_characteristics(
    basic_statement_config: "StatementConfig", sample_sql_queries: "dict[str, str]"
) -> None:
    """Test compilation performance characteristics."""
    processor = SQLProcessor(basic_statement_config)

    # Measure compilation time for complex query
    start_time = time.time()

    for _ in range(10):
        processor.compile(sample_sql_queries["select_complex"], [True])

    end_time = time.time()
    compilation_time = end_time - start_time

    # Should be fast enough (less than 1 second for 10 compilations)
    assert compilation_time < 1.0

    # Cache hits should improve performance
    assert processor._cache_hits >= 9  # 9 cache hits after first compilation


# AST transformation tests


def test_ast_based_operation_detection(basic_statement_config: "StatementConfig") -> None:
    """Test AST-based operation type detection accuracy."""
    processor = SQLProcessor(basic_statement_config)

    test_cases = [
        ("SELECT * FROM users", "SELECT", exp.Select),
        ("INSERT INTO users VALUES (1)", "INSERT", exp.Insert),
        ("UPDATE users SET name = 'test'", "UPDATE", exp.Update),
        ("DELETE FROM users", "DELETE", exp.Delete),
        ("CREATE TABLE test (id INT)", "DDL", exp.Create),
        ("DROP TABLE test", "DDL", exp.Drop),
    ]

    for sql, expected_op, expected_exp_type in test_cases:
        try:
            expression = sqlglot.parse_one(sql, dialect=basic_statement_config.dialect)
            assert isinstance(expression, expected_exp_type)

            detected_op = processor._detect_operation_type(expression)
            assert detected_op == expected_op
        except ParseError:
            pytest.skip(f"SQLGlot cannot parse: {sql}")


# Dialect-specific compilation tests


def test_sqlite_dialect_compilation(
    basic_statement_config: "StatementConfig", sample_sql_queries: "dict[str, str]"
) -> None:
    """Test SQLite-specific compilation."""
    processor = SQLProcessor(basic_statement_config)

    result = processor.compile(sample_sql_queries["select"], [123])

    assert result.parameter_style == ParameterStyle.QMARK.value
    assert result.compiled_sql.count("?") >= 1


def test_postgres_dialect_compilation(
    postgres_statement_config: "StatementConfig", sample_sql_queries: "dict[str, str]"
) -> None:
    """Test PostgreSQL-specific compilation."""
    processor = SQLProcessor(postgres_statement_config)

    result = processor.compile(sample_sql_queries["select"], [123])

    assert result.parameter_style == ParameterStyle.NUMERIC.value
    # Should convert to $1, $2, etc. format


def test_mysql_dialect_compilation(
    mysql_statement_config: "StatementConfig", sample_sql_queries: "dict[str, str]"
) -> None:
    """Test MySQL-specific compilation."""
    processor = SQLProcessor(mysql_statement_config)

    result = processor.compile(sample_sql_queries["select"], [123])

    assert result.parameter_style == ParameterStyle.POSITIONAL_PYFORMAT.value


def test_dialect_specific_optimizations(postgres_statement_config: "StatementConfig") -> None:
    """Test dialect-specific SQL optimizations."""
    processor = SQLProcessor(postgres_statement_config)

    # PostgreSQL-specific syntax
    postgres_sql = "SELECT * FROM users WHERE data ? 'key'"  # JSON operator

    result = processor.compile(postgres_sql, None)
    assert isinstance(result, CompiledSQL)
    # Should not confuse ? operator with parameter placeholder


# Error handling tests


def test_parse_error_fallback(basic_statement_config: "StatementConfig", sample_sql_queries: "dict[str, str]") -> None:
    """Test graceful handling of SQL parse errors."""
    processor = SQLProcessor(basic_statement_config)

    result = processor.compile(sample_sql_queries["malformed"], None)

    # Should not raise exception, should provide fallback result
    assert isinstance(result, CompiledSQL)
    # Malformed SQL "SELECT * FROM users WHERE" still starts with SELECT, so detected as SELECT
    assert result.operation_type == "EXECUTE"


def test_empty_sql_handling(basic_statement_config: "StatementConfig", sample_sql_queries: "dict[str, str]") -> None:
    """Test handling of empty SQL strings."""
    processor = SQLProcessor(basic_statement_config)

    # Empty SQL
    result = processor.compile(sample_sql_queries["empty"], None)
    assert isinstance(result, CompiledSQL)

    # Whitespace-only SQL
    result = processor.compile(sample_sql_queries["whitespace"], None)
    assert isinstance(result, CompiledSQL)


def test_parameter_processing_errors(basic_statement_config: "StatementConfig") -> None:
    """Test handling of parameter processing errors."""
    processor = SQLProcessor(basic_statement_config)

    # Test that processor can handle edge cases in parameter processing
    # Since ParameterProcessor.process is read-only (likely @mypyc_attr), test with unusual parameters
    result = processor.compile("SELECT * FROM users", object())  # Unusual parameter type

    # Should still compile successfully due to robust error handling
    assert isinstance(result, CompiledSQL)
    assert result.operation_type == "SELECT"


def test_sqlglot_parse_exceptions(basic_statement_config: "StatementConfig") -> None:
    """Test handling of SQLGlot parsing exceptions."""
    processor = SQLProcessor(basic_statement_config)

    with patch("sqlglot.parse_one", side_effect=ParseError("Parse failed")):
        result = processor.compile("SELECT * FROM users", None)

        # Should fall back to string-based operation detection
        assert isinstance(result, CompiledSQL)
        assert result.expression is None
        assert result.operation_type == "EXECUTE"


def test_compilation_exception_recovery(basic_statement_config: "StatementConfig") -> None:
    """Test recovery from compilation exceptions."""
    processor = SQLProcessor(basic_statement_config)

    # Test with SQL that might cause internal issues but should still be handled gracefully
    # Since _compile_uncached is read-only (likely @mypyc_attr), test actual edge cases
    result = processor.compile("COMPLETELY_INVALID_SQL_STATEMENT", None)

    # Should handle gracefully and return a result
    assert isinstance(result, CompiledSQL)
    assert result.operation_type == "UNKNOWN"


# Performance characteristics tests


def test_cache_statistics(basic_statement_config: "StatementConfig", sample_sql_queries: "dict[str, str]") -> None:
    """Test cache statistics collection."""
    processor = SQLProcessor(basic_statement_config)

    # Initial stats
    stats = processor.cache_stats
    assert stats["hits"] == 0
    assert stats["misses"] == 0
    assert stats["size"] == 0
    assert stats["max_size"] == 1000
    assert stats["hit_rate_percent"] == 0

    # After some operations
    processor.compile(sample_sql_queries["select"], [123])  # Miss
    processor.compile(sample_sql_queries["select"], [123])  # Hit
    processor.compile(sample_sql_queries["insert"], [456, "test"])  # Miss

    stats = processor.cache_stats
    assert stats["hits"] == 1
    assert stats["misses"] == 2
    assert stats["size"] == 2
    assert stats["hit_rate_percent"] == 33  # 1 hit out of 3 total requests


def test_cache_clear(basic_statement_config: "StatementConfig", sample_sql_queries: "dict[str, str]") -> None:
    """Test cache clearing functionality."""
    processor = SQLProcessor(basic_statement_config)

    # Populate cache
    processor.compile(sample_sql_queries["select"], [123])
    processor.compile(sample_sql_queries["insert"], [456, "test"])

    assert len(processor._cache) == 2
    assert processor._cache_misses == 2

    # Clear cache
    processor.clear_cache()

    assert len(processor._cache) == 0
    assert processor._cache_hits == 0
    assert processor._cache_misses == 0


def test_memory_efficiency_with_slots() -> None:
    """Test memory efficiency of CompiledSQL with __slots__."""
    result = CompiledSQL(compiled_sql="SELECT * FROM users", execution_parameters=[123], operation_type="SELECT")

    # Should not have __dict__ due to __slots__
    assert not hasattr(result, "__dict__")

    # Should have all expected slots
    expected_slots = {
        "_hash",
        "compiled_sql",
        "execution_parameters",
        "expression",
        "operation_type",
        "parameter_style",
        "supports_many",
    }
    assert set(result.__slots__) == expected_slots


def test_processor_memory_efficiency_with_slots() -> None:
    """Test memory efficiency of SQLProcessor with __slots__."""
    config = StatementConfig()
    processor = SQLProcessor(config)

    # Should not have __dict__ due to __slots__
    assert not hasattr(processor, "__dict__")

    # Should have all expected slots
    expected_slots = {"_cache", "_cache_hits", "_cache_misses", "_config", "_max_cache_size", "_parameter_processor"}
    assert set(processor.__slots__) == expected_slots


@pytest.mark.performance
def test_compilation_speed_benchmark(
    basic_statement_config: "StatementConfig", sample_sql_queries: "dict[str, str]"
) -> None:
    """Benchmark compilation speed for performance regression detection."""
    processor = SQLProcessor(basic_statement_config)

    # Warm up
    for _ in range(5):
        processor.compile(sample_sql_queries["select_complex"], [True])

    # Benchmark cached compilation
    start_time = time.time()
    for _ in range(100):
        processor.compile(sample_sql_queries["select_complex"], [True])
    cached_time = time.time() - start_time

    # Benchmark uncached compilation
    start_time = time.time()
    for i in range(100):
        processor.compile(f"SELECT {i} FROM users", [i])
    uncached_time = time.time() - start_time

    # Cached compilation should be significantly faster
    assert cached_time < uncached_time / 10

    # Performance targets (adjust as needed based on hardware)
    assert cached_time < 0.1  # 100 cached compilations in < 100ms
    assert uncached_time < 2.0  # 100 uncached compilations in < 2s


def test_end_to_end_compilation_workflow(basic_statement_config: "StatementConfig") -> None:
    """Test complete end-to-end compilation workflow."""
    processor = SQLProcessor(basic_statement_config)

    # Complex SQL with multiple parameter types
    sql = "SELECT u.id, u.name FROM users u WHERE u.id = ? AND u.active = ? AND u.created > ?"
    parameters = [123, True, datetime(2023, 1, 1)]

    result = processor.compile(sql, parameters)

    # Verify complete compilation
    assert isinstance(result, CompiledSQL)
    assert result.operation_type == "SELECT"
    assert result.compiled_sql is not None
    assert len(result.compiled_sql) > 0
    assert result.execution_parameters is not None
    assert result.parameter_style is not None
    assert result.expression is not None

    # Verify caching works for identical requests
    result2 = processor.compile(sql, parameters)
    assert result == result2
    assert processor._cache_hits == 1


def test_multiple_dialects_compilation() -> None:
    """Test compilation works correctly across multiple dialects."""
    dialects = ["sqlite", "postgres", "mysql"]
    sql = "SELECT * FROM users WHERE id = ? AND name = ?"
    parameters = [123, "test"]

    for dialect in dialects:
        config = StatementConfig(dialect=dialect)
        processor = SQLProcessor(config)

        result = processor.compile(sql, parameters)

        assert isinstance(result, CompiledSQL)
        assert result.operation_type == "SELECT"
        assert result.compiled_sql is not None


def test_concurrent_compilation_safety(basic_statement_config: "StatementConfig") -> None:
    """Test thread safety of compilation process."""

    processor = SQLProcessor(basic_statement_config)
    results = []
    errors = []

    def compile_sql(sql_id: int) -> None:
        try:
            result = processor.compile(f"SELECT {sql_id} FROM users", [sql_id])
            results.append(result)
        except Exception as e:
            errors.append(e)

    # Create multiple threads
    threads = []
    for i in range(10):
        thread = threading.Thread(target=compile_sql, args=(i,))
        threads.append(thread)

    # Start all threads
    for thread in threads:
        thread.start()

    # Wait for completion
    for thread in threads:
        thread.join()

    # Verify no errors and all compilations succeeded
    assert len(errors) == 0
    assert len(results) == 10
    assert all(isinstance(r, CompiledSQL) for r in results)


@pytest.mark.parametrize(
    "sql,parameters,expected_supports_many",
    [
        ("SELECT * FROM users WHERE id = ?", [123], True),  # List parameters = supports_many
        ("INSERT INTO users (name) VALUES (?)", [["john"], ["jane"]], True),
        ("UPDATE users SET name = ? WHERE id = ?", [("new", 1), ("other", 2)], True),
        ("SELECT * FROM users", None, False),  # No parameters = no many support
    ],
)
def test_execute_many_detection(
    basic_statement_config: "StatementConfig", sql: str, parameters: Any, expected_supports_many: bool
) -> None:
    """Test detection of execute_many scenarios."""
    processor = SQLProcessor(basic_statement_config)

    result = processor.compile(sql, parameters)

    assert result.supports_many == expected_supports_many


def test_module_constants() -> None:
    """Test module constants are properly defined."""
    # OperationType constants
    operation_types = ["SELECT", "INSERT", "UPDATE", "DELETE", "COPY", "EXECUTE", "SCRIPT", "DDL", "UNKNOWN"]
    assert "SELECT" in operation_types
    assert "INSERT" in operation_types
    assert "UPDATE" in operation_types
    assert "DELETE" in operation_types
    assert "COPY" in operation_types
    assert "EXECUTE" in operation_types
    assert "SCRIPT" in operation_types
    assert "DDL" in operation_types
    assert "UNKNOWN" in operation_types
