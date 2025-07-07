"""Tests for adapter-level caching functionality."""

from typing import Optional

from sqlspec.driver.mixins._cache import AsyncAdapterCacheMixin, SyncAdapterCacheMixin
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.sql import SQL


class MockConfig:
    """Mock config for testing."""

    def __init__(self, enable_cache: bool = True, cache_size: int = 100) -> None:
        self.enable_adapter_cache = enable_cache
        self.adapter_cache_size = cache_size
        self.enable_prepared_statements = False
        self.prepared_statement_cache_size = 50


class MockAdapter(SyncAdapterCacheMixin):
    """Mock adapter for testing cache mixin."""

    def __init__(self, config: Optional[MockConfig] = None) -> None:
        self.config = config
        super().__init__()


class MockAsyncAdapter(AsyncAdapterCacheMixin):
    """Mock async adapter for testing cache mixin."""

    def __init__(self, config: Optional[MockConfig] = None) -> None:
        self.config = config
        super().__init__()


class TestAdapterCacheMixin:
    """Test the adapter cache mixin functionality."""

    def test_cache_initialization_with_config(self) -> None:
        """Test cache is initialized with config values."""
        config = MockConfig(enable_cache=True, cache_size=200)
        adapter = MockAdapter(config=config)

        assert adapter._compiled_cache is not None
        assert adapter._compiled_cache.max_size == 200
        assert adapter._prepared_statements == {}
        assert adapter._prepared_counter == 0

    def test_cache_initialization_without_config(self) -> None:
        """Test cache is initialized with defaults when no config."""
        adapter = MockAdapter()

        assert adapter._compiled_cache is not None
        assert adapter._compiled_cache.max_size == 500  # default
        assert adapter._prepared_statements == {}
        assert adapter._prepared_counter == 0

    def test_cache_disabled(self) -> None:
        """Test cache is disabled when configured."""
        config = MockConfig(enable_cache=False)
        adapter = MockAdapter(config=config)

        assert adapter._compiled_cache is None

    def test_get_compiled_sql_caching(self) -> None:
        """Test that compiled SQL is cached and reused."""
        adapter = MockAdapter()
        statement = SQL("SELECT 1")
        target_style = ParameterStyle.QMARK

        # Cache should be empty initially
        assert adapter._compiled_cache is not None and adapter._compiled_cache.size == 0

        # First call should cache the result
        result1 = adapter._get_compiled_sql(statement, target_style)
        assert adapter._compiled_cache is not None and adapter._compiled_cache.size == 1

        # Second call should use cached result
        result2 = adapter._get_compiled_sql(statement, target_style)
        assert adapter._compiled_cache is not None and adapter._compiled_cache.size == 1  # Size shouldn't change

        # Results should be identical
        assert result1 == result2

    def test_get_compiled_sql_without_cache(self) -> None:
        """Test that compiled SQL works when cache is disabled."""
        config = MockConfig(enable_cache=False)
        adapter = MockAdapter(config=config)
        statement = SQL("SELECT 1")
        target_style = ParameterStyle.QMARK

        # Should still work without caching
        result = adapter._get_compiled_sql(statement, target_style)
        assert result is not None
        assert isinstance(result, tuple)
        assert len(result) == 2  # (sql, params)

    def test_adapter_cache_key_generation(self) -> None:
        """Test cache key generation includes adapter context."""
        adapter = MockAdapter()
        statement = SQL("SELECT 1")
        target_style = ParameterStyle.QMARK

        cache_key = adapter._adapter_cache_key(statement, target_style)

        # Should include adapter class name and parameter style
        assert "MockAdapter" in cache_key
        assert target_style.value in cache_key
        # Should include statement cache key
        assert len(cache_key) > 20  # Should be reasonably long

    def test_prepared_statement_name_generation(self) -> None:
        """Test prepared statement name generation and caching."""
        adapter = MockAdapter()
        sql_hash = "test_hash_123"

        # First call should create new name
        name1 = adapter._get_or_create_prepared_statement_name(sql_hash)
        assert name1.startswith("sqlspec_ps_")
        assert adapter._prepared_counter == 1

        # Second call with same hash should return same name
        name2 = adapter._get_or_create_prepared_statement_name(sql_hash)
        assert name1 == name2
        assert adapter._prepared_counter == 1  # Counter shouldn't increase

        # Different hash should create new name
        different_hash = "different_hash_456"
        name3 = adapter._get_or_create_prepared_statement_name(different_hash)
        assert name3 != name1
        assert adapter._prepared_counter == 2

    def test_cache_clearing(self) -> None:
        """Test cache clearing functionality."""
        adapter = MockAdapter()
        statement = SQL("SELECT 1")
        target_style = ParameterStyle.QMARK

        # Add some data to caches
        adapter._get_compiled_sql(statement, target_style)
        adapter._get_or_create_prepared_statement_name("test_hash")

        # Verify caches have data
        assert adapter._compiled_cache is not None and adapter._compiled_cache.size > 0
        assert len(adapter._prepared_statements) > 0
        assert adapter._prepared_counter > 0

        # Clear caches
        adapter._clear_adapter_cache()

        # Verify caches are cleared
        assert adapter._compiled_cache is not None and adapter._compiled_cache.size == 0
        assert len(adapter._prepared_statements) == 0
        assert adapter._prepared_counter == 0

    def test_different_parameter_styles_cached_separately(self) -> None:
        """Test that different parameter styles are cached separately."""
        adapter = MockAdapter()
        statement = SQL("SELECT ?", 1)  # Statement with parameter

        # Compile with different styles
        result_qmark = adapter._get_compiled_sql(statement, ParameterStyle.QMARK)
        result_named = adapter._get_compiled_sql(statement, ParameterStyle.NAMED_COLON)

        # Results should be different due to different parameter styles
        assert result_qmark != result_named

        # Cache should have two entries
        assert adapter._compiled_cache is not None and adapter._compiled_cache.size == 2


class TestAsyncAdapterCacheMixin:
    """Test the async adapter cache mixin (should be identical to sync)."""

    def test_async_cache_initialization(self) -> None:
        """Test async cache mixin initialization."""
        config = MockConfig(enable_cache=True, cache_size=150)
        adapter = MockAsyncAdapter(config=config)

        assert adapter._compiled_cache is not None
        assert adapter._compiled_cache.max_size == 150
        assert adapter._prepared_statements == {}
        assert adapter._prepared_counter == 0

    def test_async_get_compiled_sql_caching(self) -> None:
        """Test that async adapter caching works identically to sync."""
        adapter = MockAsyncAdapter()
        statement = SQL("SELECT 1")
        target_style = ParameterStyle.QMARK

        # Cache should be empty initially
        assert adapter._compiled_cache is not None and adapter._compiled_cache.size == 0

        # First call should cache the result
        result1 = adapter._get_compiled_sql(statement, target_style)
        assert adapter._compiled_cache is not None and adapter._compiled_cache.size == 1

        # Second call should use cached result
        result2 = adapter._get_compiled_sql(statement, target_style)
        assert adapter._compiled_cache is not None and adapter._compiled_cache.size == 1  # Size shouldn't change

        # Results should be identical
        assert result1 == result2
