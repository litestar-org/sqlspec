"""Unit tests for cache integration in SQL loader.

Tests cache integration with CORE_ROUND_3 architecture including:
- Cache configuration and lifecycle management
- File cache key generation and validation
- Cache hit/miss scenarios and performance
- Cache invalidation on file changes
- Memory management and cleanup
- Multi-loader cache sharing

Uses CORE_ROUND_3 cache system with UnifiedCache.
"""

import tempfile
from collections.abc import Generator
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from sqlspec.loader import CachedSQLFile, NamedStatement, SQLFile, SQLFileLoader


@patch("sqlspec.loader.get_cache_config")
def test_cache_disabled_loading(mock_get_cache_config: Mock) -> None:
    """Test loading when cache is disabled."""
    # Mock cache config to disable caching
    mock_config = Mock()
    mock_config.compiled_cache_enabled = False
    mock_get_cache_config.return_value = mock_config

    with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as tf:
        tf.write("""
-- name: test_query
SELECT 'no cache' as message;
""")
        tf.flush()

        loader = SQLFileLoader()

        with patch.object(loader, "_load_file_without_cache") as mock_load_without_cache:
            loader._load_single_file(tf.name, None)

            # Should use direct loading without cache
            mock_load_without_cache.assert_called_once_with(tf.name, None)

        Path(tf.name).unlink()


@patch("sqlspec.loader.get_cache_config")
@patch("sqlspec.loader.get_default_cache")
def test_cache_enabled_loading(mock_get_cache: Mock, mock_get_cache_config: Mock) -> None:
    """Test loading when cache is enabled."""
    # Mock cache config to enable caching
    mock_config = Mock()
    mock_config.compiled_cache_enabled = True
    mock_get_cache_config.return_value = mock_config

    mock_cache = Mock()
    mock_cache.get.return_value = None  # Cache miss
    mock_get_cache.return_value = mock_cache

    with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as tf:
        tf.write("""
-- name: test_query
SELECT 'with cache' as message;
""")
        tf.flush()

        loader = SQLFileLoader()
        loader._load_single_file(tf.name, None)

        # Should attempt to get from cache
        mock_cache.get.assert_called_once()
        # Should store in cache after loading
        mock_cache.put.assert_called_once()

        Path(tf.name).unlink()


def test_file_cache_key_generation() -> None:
    """Test file cache key generation is consistent."""
    loader = SQLFileLoader()

    path = "/test/path/file.sql"

    # Same path should generate same key
    key1 = loader._generate_file_cache_key(path)
    key2 = loader._generate_file_cache_key(path)

    assert key1 == key2
    assert isinstance(key1, str)
    assert key1.startswith("file:")

    # Different paths should generate different keys
    key3 = loader._generate_file_cache_key("/different/path.sql")
    assert key1 != key3


def test_cache_key_uniqueness() -> None:
    """Test that cache keys are unique for different paths."""
    loader = SQLFileLoader()

    test_paths = [
        "/path/to/file1.sql",
        "/path/to/file2.sql",
        "/different/path/file1.sql",
        "/very/long/path/to/deeply/nested/file.sql",
        "relative/path/file.sql",
    ]

    keys = [loader._generate_file_cache_key(path) for path in test_paths]

    # All keys should be unique
    assert len(set(keys)) == len(keys)

    # All keys should have correct format
    for key in keys:
        assert key.startswith("file:")
        assert len(key.split(":")[1]) == 16  # 16-character hash


def test_cache_key_with_path_object() -> None:
    """Test cache key generation with Path objects."""
    loader = SQLFileLoader()

    path_str = "/test/path/file.sql"
    path_obj = Path(path_str)

    key_from_str = loader._generate_file_cache_key(path_str)
    key_from_path = loader._generate_file_cache_key(path_obj)

    assert key_from_str == key_from_path


@pytest.fixture
def mock_cache_setup() -> Generator[tuple[Mock, Mock, SQLFileLoader], None, None]:
    """Set up mock cache infrastructure for testing."""
    with (
        patch("sqlspec.loader.get_cache_config") as mock_config,
        patch("sqlspec.loader.get_default_cache") as mock_cache_factory,
    ):
        # Configure cache as enabled
        mock_cache_config = Mock()
        mock_cache_config.compiled_cache_enabled = True
        mock_config.return_value = mock_cache_config

        # Create mock cache
        mock_cache = Mock()
        mock_cache_factory.return_value = mock_cache

        loader = SQLFileLoader()

        yield mock_cache_config, mock_cache, loader


def test_cache_hit_scenario(mock_cache_setup: tuple[Mock, Mock, SQLFileLoader]) -> None:
    """Test successful cache hit scenario."""
    _mock_config, mock_cache, loader = mock_cache_setup

    with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as tf:
        content = """
-- name: cached_query
SELECT 'from cache' as source;
"""
        tf.write(content)
        tf.flush()

        # Create cached file data
        sql_file = SQLFile(content.strip(), tf.name)
        statements = {"cached_query": NamedStatement("cached_query", "SELECT 'from cache' as source")}
        cached_file = CachedSQLFile(sql_file, statements)

        # Mock cache hit
        mock_cache.get.return_value = cached_file

        # Mock file unchanged check
        with patch.object(loader, "_is_file_unchanged", return_value=True):
            loader._load_single_file(tf.name, None)

        # Should get from cache
        mock_cache.get.assert_called_once()
        # Should not put to cache (already cached)
        mock_cache.put.assert_not_called()

        # Should have loaded the query
        assert "cached_query" in loader._queries
        assert loader._queries["cached_query"].sql.strip() == "SELECT 'from cache' as source"

        Path(tf.name).unlink()


def test_cache_miss_scenario(mock_cache_setup: tuple[Mock, Mock, SQLFileLoader]) -> None:
    """Test cache miss scenario."""
    _mock_config, mock_cache, loader = mock_cache_setup

    with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as tf:
        content = """
-- name: new_query
SELECT 'new content' as source;
"""
        tf.write(content)
        tf.flush()

        # Mock cache miss
        mock_cache.get.return_value = None

        loader._load_single_file(tf.name, None)

        # Should attempt to get from cache
        mock_cache.get.assert_called_once()
        # Should put new data to cache
        mock_cache.put.assert_called_once()

        # Should have loaded the query
        assert "new_query" in loader._queries

        Path(tf.name).unlink()


def test_cache_invalidation_on_file_change(mock_cache_setup: tuple[Mock, Mock, SQLFileLoader]) -> None:
    """Test cache invalidation when file changes."""
    _mock_config, mock_cache, loader = mock_cache_setup

    with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as tf:
        original_content = """
-- name: changing_query
SELECT 'original' as version;
"""
        tf.write(original_content)
        tf.flush()

        # Create cached file with original content
        sql_file = SQLFile(original_content.strip(), tf.name)
        statements = {"changing_query": NamedStatement("changing_query", "SELECT 'original' as version")}
        cached_file = CachedSQLFile(sql_file, statements)

        # Mock cache hit but file changed
        mock_cache.get.return_value = cached_file

        # Mock file changed check
        with patch.object(loader, "_is_file_unchanged", return_value=False):
            loader._load_single_file(tf.name, None)

        # Should get from cache (to check)
        mock_cache.get.assert_called_once()
        # Should put updated data to cache
        mock_cache.put.assert_called_once()

        Path(tf.name).unlink()


def test_file_content_change_detection() -> None:
    """Test detection of file content changes."""
    loader = SQLFileLoader()

    with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as tf:
        original_content = "SELECT 'original' as content;"
        tf.write(original_content)
        tf.flush()

        # Create cached file with original checksum
        sql_file = SQLFile(original_content, tf.name)
        cached_file = CachedSQLFile(sql_file, {})

        # File should be unchanged initially
        assert loader._is_file_unchanged(tf.name, cached_file)

        # Modify file content
        Path(tf.name).write_text("SELECT 'modified' as content;")

        # File should now be changed
        assert not loader._is_file_unchanged(tf.name, cached_file)

        Path(tf.name).unlink()


def test_file_deletion_handling() -> None:
    """Test handling when cached file is deleted."""
    loader = SQLFileLoader()

    with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as tf:
        content = "SELECT 'deleted' as status;"
        tf.write(content)
        tf.flush()

        # Create cached file
        sql_file = SQLFile(content, tf.name)
        cached_file = CachedSQLFile(sql_file, {})

        # Delete the file
        Path(tf.name).unlink()

        # Should detect file as changed (unable to read)
        assert not loader._is_file_unchanged(tf.name, cached_file)


def test_checksum_calculation_error_handling() -> None:
    """Test handling of checksum calculation errors."""
    loader = SQLFileLoader()

    with patch.object(loader, "_read_file_content", side_effect=Exception("Read error")):
        result = loader._is_file_unchanged("/nonexistent/file.sql", Mock())
        # Should return False when unable to calculate checksum
        assert not result


def test_cached_sqlfile_structure() -> None:
    """Test CachedSQLFile structure and data integrity."""
    content = """
-- name: test_query_1
SELECT 1;

-- name: test_query_2
SELECT 2;
"""

    sql_file = SQLFile(content, "test.sql")
    statements = {
        "test_query_1": NamedStatement("test_query_1", "SELECT 1"),
        "test_query_2": NamedStatement("test_query_2", "SELECT 2"),
    }

    cached_file = CachedSQLFile(sql_file, statements)

    assert cached_file.sql_file == sql_file
    assert cached_file.parsed_statements == statements
    assert set(cached_file.statement_names) == {"test_query_1", "test_query_2"}


def test_namespace_handling_in_cache() -> None:
    """Test proper namespace handling in cached data."""
    with tempfile.TemporaryDirectory() as temp_dir:
        base_path = Path(temp_dir)

        # Create nested directory structure
        (base_path / "analytics").mkdir()
        sql_file = base_path / "analytics" / "reports.sql"
        sql_file.write_text("""
-- name: user_report
SELECT COUNT(*) FROM users;
""")

        loader = SQLFileLoader()

        with (
            patch("sqlspec.loader.get_cache_config") as mock_config,
            patch("sqlspec.loader.get_default_cache") as mock_cache_factory,
        ):
            mock_cache_config = Mock()
            mock_cache_config.compiled_cache_enabled = True
            mock_config.return_value = mock_cache_config

            mock_cache = Mock()
            mock_cache.get.return_value = None  # Cache miss
            mock_cache_factory.return_value = mock_cache

            loader.load_sql(base_path)

            # Should have namespaced query
            assert "analytics.user_report" in loader._queries

            # Check what was cached
            mock_cache.put.assert_called()
            cache_call_args = mock_cache.put.call_args[0]
            cached_data = cache_call_args[1]

            assert isinstance(cached_data, CachedSQLFile)
            # Cached data should store query without namespace prefix
            assert "user_report" in cached_data.parsed_statements
            assert "analytics.user_report" not in cached_data.parsed_statements


def test_cache_restoration_with_namespace() -> None:
    """Test proper namespace restoration when loading from cache."""
    with tempfile.TemporaryDirectory() as temp_dir:
        base_path = Path(temp_dir)

        # Create nested structure
        (base_path / "reports").mkdir()
        sql_file = base_path / "reports" / "daily.sql"
        content = """
-- name: daily_users
SELECT COUNT(*) FROM users WHERE date = CURRENT_DATE;
"""
        sql_file.write_text(content)

        # Create cached data (without namespace in stored queries)
        cached_sql_file = SQLFile(content, str(sql_file))
        cached_statements = {
            "daily_users": NamedStatement("daily_users", "SELECT COUNT(*) FROM users WHERE date = CURRENT_DATE")
        }
        cached_file = CachedSQLFile(cached_sql_file, cached_statements)

        loader = SQLFileLoader()

        with (
            patch("sqlspec.loader.get_cache_config") as mock_config,
            patch("sqlspec.loader.get_default_cache") as mock_cache_factory,
            patch.object(loader, "_is_file_unchanged", return_value=True),
        ):
            mock_cache_config = Mock()
            mock_cache_config.compiled_cache_enabled = True
            mock_config.return_value = mock_cache_config

            mock_cache = Mock()
            mock_cache.get.return_value = cached_file  # Cache hit
            mock_cache_factory.return_value = mock_cache

            # Load with namespace
            loader._load_single_file(sql_file, "reports")

            # Should restore with proper namespace
            assert "reports.daily_users" in loader._queries
            assert "daily_users" not in loader._queries


def test_cache_clear_integration() -> None:
    """Test cache clearing integration."""
    loader = SQLFileLoader()

    with (
        patch("sqlspec.loader.get_cache_config") as mock_config,
        patch("sqlspec.loader.get_default_cache") as mock_cache_factory,
    ):
        mock_cache_config = Mock()
        mock_cache_config.compiled_cache_enabled = True
        mock_config.return_value = mock_cache_config

        mock_cache = Mock()
        mock_cache_factory.return_value = mock_cache

        # Add some data to loader
        loader.add_named_sql("test_query", "SELECT 1")

        # Clear cache
        loader.clear_cache()

        # Should clear internal state
        assert len(loader._queries) == 0
        assert len(loader._files) == 0
        assert len(loader._query_to_file) == 0

        # Should clear underlying cache
        mock_cache.clear.assert_called_once()


def test_file_cache_only_clear() -> None:
    """Test clearing only file cache while preserving loaded queries."""
    loader = SQLFileLoader()

    with (
        patch("sqlspec.loader.get_cache_config") as mock_config,
        patch("sqlspec.loader.get_default_cache") as mock_cache_factory,
    ):
        mock_cache_config = Mock()
        mock_cache_config.compiled_cache_enabled = True
        mock_config.return_value = mock_cache_config

        mock_cache = Mock()
        mock_cache_factory.return_value = mock_cache

        # Add some data
        loader.add_named_sql("test_query", "SELECT 1")

        # Clear only file cache
        loader.clear_file_cache()

        # Should preserve loaded queries
        assert len(loader._queries) == 1
        assert len(loader._files) == 0  # Files might be cleared
        assert len(loader._query_to_file) == 1

        # Should clear underlying cache
        mock_cache.clear.assert_called_once()


def test_cache_disabled_clear_behavior() -> None:
    """Test cache clear behavior when caching is disabled."""
    loader = SQLFileLoader()

    with patch("sqlspec.loader.get_cache_config") as mock_config:
        mock_cache_config = Mock()
        mock_cache_config.compiled_cache_enabled = False
        mock_config.return_value = mock_cache_config

        # Add some data
        loader.add_named_sql("test_query", "SELECT 1")

        # Clear cache should work even when disabled
        loader.clear_cache()

        assert len(loader._queries) == 0


def test_cache_sharing_between_loaders() -> None:
    """Test that multiple loaders can share cached data."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as tf:
        content = """
-- name: shared_query
SELECT 'shared between loaders' as message;
"""
        tf.write(content)
        tf.flush()

        with (
            patch("sqlspec.loader.get_cache_config") as mock_config,
            patch("sqlspec.loader.get_default_cache") as mock_cache_factory,
        ):
            mock_cache_config = Mock()
            mock_cache_config.compiled_cache_enabled = True
            mock_config.return_value = mock_cache_config

            # Create a shared cache instance
            shared_cache = Mock()
            mock_cache_factory.return_value = shared_cache

            # First loader - cache miss, then stores data
            loader1 = SQLFileLoader()
            shared_cache.get.return_value = None  # Cache miss

            with patch.object(loader1, "_is_file_unchanged", return_value=True):
                loader1._load_single_file(tf.name, None)

            shared_cache.put.assert_called_once()

            # Second loader - should get cached data
            loader2 = SQLFileLoader()

            # Create the cached data that would be stored
            sql_file = SQLFile(content.strip(), tf.name)
            statements = {"shared_query": NamedStatement("shared_query", "SELECT 'shared between loaders' as message")}
            cached_file = CachedSQLFile(sql_file, statements)

            shared_cache.get.return_value = cached_file  # Cache hit
            shared_cache.reset_mock()  # Reset call counts

            with patch.object(loader2, "_is_file_unchanged", return_value=True):
                loader2._load_single_file(tf.name, None)

            # Should get from cache, not store again
            shared_cache.get.assert_called_once()
            shared_cache.put.assert_not_called()

            # Both loaders should have the same query
            assert "shared_query" in loader1._queries
            assert "shared_query" in loader2._queries

        Path(tf.name).unlink()


def test_cache_isolation_between_loaders() -> None:
    """Test that loader internal state remains isolated despite shared cache."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as tf:
        content = """
-- name: isolated_query
SELECT 'isolated' as status;
"""
        tf.write(content)
        tf.flush()

        loader1 = SQLFileLoader()
        loader2 = SQLFileLoader()

        # Load in first loader only
        loader1.add_named_sql("loader1_query", "SELECT 'loader1' as source")

        # Second loader should not see first loader's queries
        assert "loader1_query" in loader1._queries
        assert "loader1_query" not in loader2._queries

        # But they can both load the same file
        loader1.load_sql(tf.name)
        loader2.load_sql(tf.name)

        assert "isolated_query" in loader1._queries
        assert "isolated_query" in loader2._queries

        Path(tf.name).unlink()


def test_cache_key_performance() -> None:
    """Test cache key generation performance."""
    loader = SQLFileLoader()

    # Generate many cache keys to test performance
    paths = [f"/test/path/file_{i:04d}.sql" for i in range(1000)]

    # Should complete quickly without issues
    keys = [loader._generate_file_cache_key(path) for path in paths]

    # All keys should be unique
    assert len(set(keys)) == len(keys)

    # All keys should have correct format
    for key in keys:
        assert key.startswith("file:")
        assert len(key.split(":")[1]) == 16


def test_checksum_calculation_performance() -> None:
    """Test checksum calculation performance with large content."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as tf:
        # Create large content
        large_content = "SELECT 'performance_test' as test;" * 10000
        tf.write(large_content)
        tf.flush()

        loader = SQLFileLoader()

        # Should calculate checksum efficiently
        checksum1 = loader._calculate_file_checksum(tf.name)
        checksum2 = loader._calculate_file_checksum(tf.name)

        # Results should be consistent
        assert checksum1 == checksum2
        assert isinstance(checksum1, str)
        assert len(checksum1) == 32  # MD5 hex length

        Path(tf.name).unlink()


def test_cache_hit_performance_benefit() -> None:
    """Test performance benefit of cache hits vs. parsing."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as tf:
        # Create file with many queries to make parsing expensive
        queries = [
            f"""
-- name: perf_query_{i:03d}
SELECT {i} as query_id, 'performance test {i}' as description
FROM performance_table
WHERE id > {i * 10}
LIMIT 100;
"""
            for i in range(100)
        ]
        tf.write("\n".join(queries))
        tf.flush()

        loader = SQLFileLoader()

        with (
            patch("sqlspec.loader.get_cache_config") as mock_config,
            patch("sqlspec.loader.get_default_cache") as mock_cache_factory,
        ):
            mock_cache_config = Mock()
            mock_cache_config.compiled_cache_enabled = True
            mock_config.return_value = mock_cache_config

            mock_cache = Mock()
            mock_cache_factory.return_value = mock_cache

            # First load - cache miss (should parse)
            mock_cache.get.return_value = None

            loader._load_single_file(tf.name, None)

            # Should have parsed and cached
            assert len(loader._queries) == 100
            mock_cache.put.assert_called_once()

            # Create cached data for second load
            sql_file = SQLFile("dummy content", tf.name)
            cached_statements = {
                f"perf_query_{i:03d}": NamedStatement(f"perf_query_{i:03d}", f"SELECT {i}") for i in range(100)
            }
            cached_file = CachedSQLFile(sql_file, cached_statements)

            # Second load - cache hit (should skip parsing)
            loader2 = SQLFileLoader()
            mock_cache.get.return_value = cached_file
            mock_cache.reset_mock()

            with patch.object(loader2, "_is_file_unchanged", return_value=True):
                loader2._load_single_file(tf.name, None)

            # Should have loaded from cache without parsing
            assert len(loader2._queries) == 100
            mock_cache.get.assert_called_once()
            mock_cache.put.assert_not_called()  # No new cache storage

        Path(tf.name).unlink()
