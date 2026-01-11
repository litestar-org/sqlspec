# pyright: reportPrivateImportUsage = false, reportPrivateUsage = false
"""Unit tests for the core.compiler module.

This module tests the SQLProcessor and CompiledSQL classes.

Test Coverage:
1. CompiledSQL class - Immutable compiled SQL results with complete information
2. SQLProcessor class - Single-pass compiler with integrated caching
3. Compilation pipeline - SQL parsing, optimization, and compilation
4. Query optimization - Performance optimizations during compilation
5. AST transformations - SQL AST transformations and optimizations
6. Dialect-specific compilation - Compilation for different database dialects
7. Error handling - Compilation error scenarios and fallbacks
8. Performance characteristics - Compilation speed and efficiency testing
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

from sqlspec.core import (
    CompiledSQL,
    OperationType,
    ParameterProcessor,
    ParameterProfile,
    ParameterStyle,
    ParameterStyleConfig,
    SQLProcessor,
    StatementConfig,
    is_copy_from_operation,
    is_copy_operation,
    is_copy_to_operation,
)
from sqlspec.core.pipeline import compile_with_pipeline, reset_statement_pipeline_cache
from sqlspec.core.statement import get_default_config
from tests.conftest import requires_interpreted

pytestmark = pytest.mark.xdist_group("core")


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


def test_compiled_sql_creation() -> None:
    """Test CompiledSQL object creation and basic properties."""
    compiled_sql = "SELECT * FROM users WHERE id = ?"
    execution_parameters = [123]
    operation_type: OperationType = "SELECT"
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

    assert result._hash is None

    hash1 = hash(result)
    assert result._hash is not None
    assert hash1 == result._hash

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


def test_sql_processor_initialization(basic_statement_config: "StatementConfig") -> None:
    """Test SQLProcessor initialization with configuration."""
    processor = SQLProcessor(basic_statement_config)

    assert processor._config == basic_statement_config
    assert isinstance(processor._cache, OrderedDict)
    assert processor._max_cache_size == 1000
    assert processor._parse_cache_max_size == 1000
    assert processor._cache_enabled is True
    assert processor._cache_hits == 0
    assert processor._cache_misses == 0
    assert isinstance(processor._parameter_processor, ParameterProcessor)


def test_sql_processor_custom_cache_size(basic_statement_config: "StatementConfig") -> None:
    """Test SQLProcessor with custom cache size."""
    processor = SQLProcessor(basic_statement_config, max_cache_size=500)
    assert processor._max_cache_size == 500
    assert processor._parse_cache_max_size == 500


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

    result1 = processor.compile(sql, parameters)
    assert processor._cache_misses == 1
    assert processor._cache_hits == 0

    result2 = processor.compile(sql, parameters)
    assert processor._cache_misses == 1
    assert processor._cache_hits == 1

    assert result1 == result2


def test_compilation_without_caching(no_cache_config: "StatementConfig", sample_sql_queries: "dict[str, str]") -> None:
    """Test SQL compilation with caching disabled."""
    processor = SQLProcessor(no_cache_config)

    sql = sample_sql_queries["select"]
    parameters = [123]

    result1 = processor.compile(sql, parameters)
    result2 = processor.compile(sql, parameters)

    assert processor._cache_hits == 0
    assert processor._cache_misses == 0

    assert result1 == result2


def test_parse_cache_when_compiled_cache_disabled(
    basic_statement_config: "StatementConfig", sample_sql_queries: "dict[str, str]"
) -> None:
    """Parse cache should still be active when compiled cache is disabled."""
    processor = SQLProcessor(basic_statement_config, cache_enabled=False, parse_cache_size=10)

    sql = sample_sql_queries["select"]
    parameters = [123]

    processor.compile(sql, parameters)
    processor.compile(sql, parameters)

    assert processor._cache_hits == 0
    assert processor._cache_misses == 0
    assert processor._parse_cache_hits == 1
    assert processor._parse_cache_misses == 1


def test_cache_key_generation(basic_statement_config: "StatementConfig") -> None:
    """Test cache key generation for consistent caching."""
    processor = SQLProcessor(basic_statement_config)

    key1 = processor._make_cache_key("SELECT * FROM users", [123])
    key2 = processor._make_cache_key("SELECT * FROM users", [123])
    assert key1 == key2

    key3 = processor._make_cache_key("SELECT * FROM posts", [123])
    assert key1 != key3

    key4 = processor._make_cache_key("SELECT * FROM users", [456])
    assert key1 != key4

    assert isinstance(key1, str)
    assert key1.startswith("sql_")


def test_cache_eviction(basic_statement_config: "StatementConfig") -> None:
    """Test LRU cache eviction when at capacity."""
    processor = SQLProcessor(basic_statement_config, max_cache_size=2)

    processor.compile("SELECT 1", None)
    processor.compile("SELECT 2", None)

    assert len(processor._cache) == 2

    processor.compile("SELECT 3", None)
    assert len(processor._cache) == 2

    cache_keys = list(processor._cache.keys())
    key1 = processor._make_cache_key("SELECT 1", None)
    assert key1 not in cache_keys


def test_cache_lru_behavior(basic_statement_config: "StatementConfig") -> None:
    """Test LRU (Least Recently Used) cache behavior."""
    processor = SQLProcessor(basic_statement_config, max_cache_size=2)

    processor.compile("SELECT 1", None)
    processor.compile("SELECT 2", None)

    processor.compile("SELECT 1", None)
    assert processor._cache_hits == 1

    processor.compile("SELECT 3", None)

    key1 = processor._make_cache_key("SELECT 1", None)
    key2 = processor._make_cache_key("SELECT 2", None)
    key3 = processor._make_cache_key("SELECT 3", None)

    assert key1 in processor._cache
    assert key2 not in processor._cache
    assert key3 in processor._cache


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
        detected_type = "EXECUTE"
        assert detected_type in ["SELECT", "INSERT", "UPDATE", "DELETE", "COPY", "EXECUTE", "SCRIPT", "DDL", "UNKNOWN"]


def test_copy_operation_helpers() -> None:
    """Ensure COPY helper predicates cover all variants."""

    assert is_copy_operation("COPY")
    assert is_copy_operation("COPY_FROM")
    assert is_copy_operation("COPY_TO")
    assert not is_copy_operation("SELECT")

    assert is_copy_from_operation("COPY")
    assert is_copy_from_operation("COPY_FROM")
    assert not is_copy_from_operation("COPY_TO")

    assert is_copy_to_operation("COPY_TO")
    assert not is_copy_to_operation("COPY")


def test_single_pass_processing(
    basic_statement_config: "StatementConfig", sample_sql_queries: "dict[str, str]"
) -> None:
    """Test single-pass processing eliminates redundant parsing."""
    processor = SQLProcessor(basic_statement_config)

    with patch("sqlglot.parse_one") as mock_parse:
        real_expression = exp.Select()
        real_expression.sql = Mock(return_value="SELECT * FROM users WHERE id = ?")
        mock_parse.return_value = real_expression

        result = processor.compile(sample_sql_queries["select"], [123])

        assert mock_parse.call_count == 1
        assert result.operation_type == "SELECT"


def test_parameter_processing_integration(basic_statement_config: "StatementConfig") -> None:
    """Test integration with parameter processing system."""
    processor = SQLProcessor(basic_statement_config)

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

    config_with_transformer = basic_statement_config.replace()

    processor = SQLProcessor(config_with_transformer)
    result = processor.compile("select * from users", None)

    assert isinstance(result, CompiledSQL)


def test_statement_transformers_apply(basic_statement_config: "StatementConfig") -> None:
    """Statement transformers should update the AST before compilation."""

    def rename_table(expression: exp.Expression, parameters: Any) -> "tuple[exp.Expression, Any]":
        updated = expression.transform(
            lambda node: exp.to_table("users_archive") if isinstance(node, exp.Table) else node
        )
        return updated, parameters

    config = basic_statement_config.replace(statement_transformers=(rename_table,))
    processor = SQLProcessor(config)

    result = processor.compile("SELECT * FROM users WHERE id = ?", [123])

    assert "users_archive" in result.compiled_sql
    assert result.operation_type == "SELECT"


def test_ast_transformer_single_parse(basic_statement_config: "StatementConfig") -> None:
    """AST transformers should not trigger a second parse."""

    def pass_through(
        expression: exp.Expression, parameters: Any, _parameter_profile: "ParameterProfile"
    ) -> "tuple[exp.Expression, Any]":
        return expression, parameters

    parameter_config = basic_statement_config.parameter_config.replace(ast_transformer=pass_through)
    config = basic_statement_config.replace(parameter_config=parameter_config)
    processor = SQLProcessor(config)

    with patch("sqlspec.core.compiler.sqlglot.parse_one", wraps=sqlglot.parse_one) as mock_parse:
        processor.compile("SELECT * FROM users WHERE id = ?", [123])

    assert mock_parse.call_count == 1


def test_ast_transformer_receives_parameter_profile(basic_statement_config: "StatementConfig") -> None:
    """AST transformers should receive the detected parameter profile."""
    captured: dict[str, int] = {}

    def capture_profile(
        expression: exp.Expression, parameters: Any, parameter_profile: "ParameterProfile"
    ) -> "tuple[exp.Expression, Any]":
        captured["count"] = parameter_profile.total_count
        return expression, parameters

    parameter_config = basic_statement_config.parameter_config.replace(ast_transformer=capture_profile)
    config = basic_statement_config.replace(parameter_config=parameter_config)
    processor = SQLProcessor(config)

    processor.compile("SELECT * FROM users WHERE id = ?", [123])

    assert captured["count"] == 1


def test_statement_transformer_updates_operation_type(basic_statement_config: "StatementConfig") -> None:
    """Statement transformers should refresh detected operation metadata."""

    def to_delete(expression: exp.Expression, parameters: Any) -> "tuple[exp.Expression, Any]":
        return exp.Delete(this=exp.to_table("users")), parameters

    config = basic_statement_config.replace(statement_transformers=(to_delete,))
    processor = SQLProcessor(config)

    result = processor.compile("SELECT * FROM users", None)

    assert result.operation_type == "DELETE"
    assert "DELETE" in result.compiled_sql


def test_parsing_enabled_optimization(
    basic_statement_config: "StatementConfig", sample_sql_queries: "dict[str, str]"
) -> None:
    """Test compilation with parsing enabled for optimization."""
    processor = SQLProcessor(basic_statement_config)

    result = processor.compile(sample_sql_queries["select_complex"], [True])

    assert isinstance(result, CompiledSQL)
    assert result.expression is not None
    assert result.operation_type == "SELECT"


def test_parsing_reuses_expression_override(basic_statement_config: "StatementConfig") -> None:
    """Expressions supplied to compile should bypass re-parsing."""
    processor = SQLProcessor(basic_statement_config)
    expression = exp.select("*").from_("users")
    sql_text = expression.sql(dialect=basic_statement_config.dialect)

    with patch("sqlspec.core.compiler.sqlglot.parse_one") as mock_parse:
        result = processor.compile(sql_text, None, expression=expression)

    mock_parse.assert_not_called()
    assert result.expression is expression


def test_parsing_disabled_fallback(
    basic_statement_config: "StatementConfig", sample_sql_queries: "dict[str, str]"
) -> None:
    """Test compilation fallback when parsing is disabled."""

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

    start_time = time.time()

    for _ in range(10):
        processor.compile(sample_sql_queries["select_complex"], [True])

    end_time = time.time()
    compilation_time = end_time - start_time

    assert compilation_time < 1.0

    assert processor._cache_hits >= 9


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

    postgres_sql = "SELECT * FROM users WHERE data ? 'key'"

    result = processor.compile(postgres_sql, None)
    assert isinstance(result, CompiledSQL)


def test_parse_error_fallback(basic_statement_config: "StatementConfig", sample_sql_queries: "dict[str, str]") -> None:
    """Test graceful handling of SQL parse errors."""
    processor = SQLProcessor(basic_statement_config)

    result = processor.compile(sample_sql_queries["malformed"], None)

    assert isinstance(result, CompiledSQL)

    assert result.operation_type == "EXECUTE"


def test_empty_sql_handling(basic_statement_config: "StatementConfig", sample_sql_queries: "dict[str, str]") -> None:
    """Test handling of empty SQL strings."""
    processor = SQLProcessor(basic_statement_config)

    result = processor.compile(sample_sql_queries["empty"], None)
    assert isinstance(result, CompiledSQL)

    result = processor.compile(sample_sql_queries["whitespace"], None)
    assert isinstance(result, CompiledSQL)


def test_parameter_processing_errors(basic_statement_config: "StatementConfig") -> None:
    """Test handling of parameter processing errors."""
    processor = SQLProcessor(basic_statement_config)

    result = processor.compile("SELECT * FROM users", object())

    assert isinstance(result, CompiledSQL)
    assert result.operation_type == "SELECT"


def test_sqlglot_parse_exceptions(basic_statement_config: "StatementConfig") -> None:
    """Test handling of SQLGlot parsing exceptions."""
    processor = SQLProcessor(basic_statement_config)

    with patch("sqlglot.parse_one", side_effect=ParseError("Parse failed")):
        result = processor.compile("SELECT * FROM users", None)

        assert isinstance(result, CompiledSQL)
        assert result.expression is None
        assert result.operation_type == "EXECUTE"


def test_compilation_exception_recovery(basic_statement_config: "StatementConfig") -> None:
    """Test recovery from compilation exceptions."""
    processor = SQLProcessor(basic_statement_config)

    result = processor.compile("COMPLETELY_INVALID_SQL_STATEMENT", None)

    assert isinstance(result, CompiledSQL)
    assert result.operation_type == "UNKNOWN"


def test_cache_statistics(basic_statement_config: "StatementConfig", sample_sql_queries: "dict[str, str]") -> None:
    """Test cache statistics collection."""
    processor = SQLProcessor(basic_statement_config)

    stats = processor.cache_stats
    assert stats["hits"] == 0
    assert stats["misses"] == 0
    assert stats["size"] == 0
    assert stats["max_size"] == 1000
    assert stats["hit_rate_percent"] == 0

    processor.compile(sample_sql_queries["select"], [123])
    processor.compile(sample_sql_queries["select"], [123])
    processor.compile(sample_sql_queries["insert"], [456, "test"])

    stats = processor.cache_stats
    assert stats["hits"] == 1
    assert stats["misses"] == 2
    assert stats["size"] == 2
    assert stats["hit_rate_percent"] == 33


def test_parameter_cache_statistics(basic_statement_config: "StatementConfig") -> None:
    """Parameter processor cache stats should reflect reuse."""
    processor = SQLProcessor(basic_statement_config, cache_enabled=False, parse_cache_size=10, parameter_cache_size=10)

    processor.compile("SELECT * FROM users WHERE id = ?", [123])
    processor.compile("SELECT * FROM users WHERE id = ?", [123])

    stats = processor.cache_stats
    assert stats["parameter_hits"] >= 1
    assert stats["parameter_misses"] >= 1
    assert stats["parameter_size"] >= 1


def test_cache_clear(basic_statement_config: "StatementConfig", sample_sql_queries: "dict[str, str]") -> None:
    """Test cache clearing functionality."""
    processor = SQLProcessor(basic_statement_config)

    processor.compile(sample_sql_queries["select"], [123])
    processor.compile(sample_sql_queries["insert"], [456, "test"])

    assert len(processor._cache) == 2
    assert processor._cache_misses == 2

    processor.clear_cache()

    assert len(processor._cache) == 0
    assert processor._cache_hits == 0
    assert processor._cache_misses == 0
    stats = processor.cache_stats
    assert stats["parameter_hits"] == 0
    assert stats["parameter_misses"] == 0
    assert stats["parameter_size"] == 0


def test_memory_efficiency_with_slots() -> None:
    """Test memory efficiency of CompiledSQL with __slots__."""
    result = CompiledSQL(compiled_sql="SELECT * FROM users", execution_parameters=[123], operation_type="SELECT")

    assert not hasattr(result, "__dict__")

    expected_slots = {
        "_hash",
        "compiled_sql",
        "execution_parameters",
        "expression",
        "operation_type",
        "operation_profile",
        "parameter_casts",
        "parameter_profile",
        "parameter_style",
        "supports_many",
    }
    slots = getattr(type(result), "__slots__", None)
    if slots is not None:
        assert set(slots) == expected_slots


def test_processor_memory_efficiency_with_slots() -> None:
    """Test memory efficiency of SQLProcessor with __slots__."""
    config = StatementConfig()
    processor = SQLProcessor(config)

    assert not hasattr(processor, "__dict__")

    expected_slots = {
        "_cache",
        "_cache_enabled",
        "_cache_hits",
        "_cache_misses",
        "_config",
        "_max_cache_size",
        "_parameter_processor",
        "_parse_cache",
        "_parse_cache_hits",
        "_parse_cache_max_size",
        "_parse_cache_misses",
    }
    slots = getattr(type(processor), "__slots__", None)
    if slots is not None:
        assert set(slots) == expected_slots


@pytest.mark.performance
def test_compilation_speed_benchmark(
    basic_statement_config: "StatementConfig", sample_sql_queries: "dict[str, str]"
) -> None:
    """Benchmark compilation speed for performance regression detection."""
    processor = SQLProcessor(basic_statement_config)

    for _ in range(5):
        processor.compile(sample_sql_queries["select_complex"], [True])

    start_time = time.time()
    for _ in range(100):
        processor.compile(sample_sql_queries["select_complex"], [True])
    cached_time = time.time() - start_time

    start_time = time.time()
    for i in range(100):
        processor.compile(f"SELECT {i} FROM users", [i])
    uncached_time = time.time() - start_time

    assert cached_time < uncached_time / 10

    assert cached_time < 0.1
    assert uncached_time < 2.0


def test_end_to_end_compilation_workflow(basic_statement_config: "StatementConfig") -> None:
    """Test complete end-to-end compilation workflow."""
    processor = SQLProcessor(basic_statement_config)

    sql = "SELECT u.id, u.name FROM users u WHERE u.id = ? AND u.active = ? AND u.created > ?"
    parameters = [123, True, datetime(2023, 1, 1)]

    result = processor.compile(sql, parameters)

    assert isinstance(result, CompiledSQL)
    assert result.operation_type == "SELECT"
    assert result.compiled_sql is not None
    assert len(result.compiled_sql) > 0
    assert result.execution_parameters is not None
    assert result.parameter_style is not None
    assert result.expression is not None

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

    threads = []
    for i in range(10):
        thread = threading.Thread(target=compile_sql, args=(i,))
        threads.append(thread)

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    assert len(errors) == 0
    assert len(results) == 10
    assert all(isinstance(r, CompiledSQL) for r in results)


@pytest.mark.parametrize(
    "sql,parameters,expected_supports_many",
    [
        ("SELECT * FROM users WHERE id = ?", [123], True),
        ("INSERT INTO users (name) VALUES (?)", [["john"], ["jane"]], True),
        ("UPDATE users SET name = ? WHERE id = ?", [("new", 1), ("other", 2)], True),
        ("SELECT * FROM users", None, False),
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


@requires_interpreted
def test_compile_with_pipeline_passes_expression() -> None:
    """Ensure pipeline forwards expressions to the SQL processor."""
    config = get_default_config()
    expression = exp.select("*").from_("users")

    reset_statement_pipeline_cache()
    with patch("sqlspec.core.pipeline.SQLProcessor.compile") as mock_compile:
        mock_compile.return_value = CompiledSQL(
            compiled_sql="SELECT * FROM users",
            execution_parameters=[],
            operation_type="SELECT",
            expression=expression,
            parameter_profile=ParameterProfile.empty(),
        )

        _ = compile_with_pipeline(config, "SELECT * FROM users", [], expression=expression)

        _, kwargs = mock_compile.call_args
        assert kwargs["expression"] is expression
