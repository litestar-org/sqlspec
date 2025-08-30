"""Integration tests for file system loading.

Tests real file system operations including:
- Loading from local file system paths
- File system watching and cache invalidation
- Permission handling and error scenarios
- Large file handling and performance
- Concurrent file access patterns
"""

import os
import tempfile
import time
from collections.abc import Generator
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from sqlspec.core.statement import SQL
from sqlspec.exceptions import SQLFileNotFoundError, SQLFileParseError
from sqlspec.loader import SQLFileLoader


@pytest.fixture
def temp_workspace() -> Generator[Path, None, None]:
    """Create a temporary workspace for file system tests."""
    with tempfile.TemporaryDirectory() as temp_dir:
        workspace = Path(temp_dir)
        yield workspace


def test_load_single_file_from_filesystem(temp_workspace: Path) -> None:
    """Test loading a single SQL file from the file system.

    Args:
        temp_workspace: Temporary directory for test files.
    """
    sql_file = temp_workspace / "test_queries.sql"
    sql_file.write_text("""
-- name: get_user_count
SELECT COUNT(*) as total_users FROM users;

-- name: get_active_users
SELECT id, name FROM users WHERE active = true;
""")

    loader = SQLFileLoader()
    loader.load_sql(sql_file)

    queries = loader.list_queries()
    assert "get_user_count" in queries
    assert "get_active_users" in queries

    user_count_sql = loader.get_sql("get_user_count")
    assert isinstance(user_count_sql, SQL)
    assert "COUNT(*)" in user_count_sql.sql


def test_load_multiple_files_from_filesystem(temp_workspace: Path) -> None:
    """Test loading multiple SQL files from the file system.

    Args:
        temp_workspace: Temporary directory for test files.
    """
    users_file = temp_workspace / "users.sql"
    users_file.write_text("""
-- name: create_user
INSERT INTO users (name, email) VALUES (:name, :email);

-- name: update_user_email
UPDATE users SET email = :email WHERE id = :user_id;
""")

    products_file = temp_workspace / "products.sql"
    products_file.write_text("""
-- name: list_products
SELECT id, name, price FROM products ORDER BY name;

-- name: get_product_by_id
SELECT * FROM products WHERE id = :product_id;
""")

    loader = SQLFileLoader()
    loader.load_sql(users_file, products_file)

    queries = loader.list_queries()
    assert "create_user" in queries
    assert "update_user_email" in queries
    assert "list_products" in queries
    assert "get_product_by_id" in queries

    files = loader.list_files()
    assert str(users_file) in files
    assert str(products_file) in files


def test_load_directory_structure_from_filesystem(temp_workspace: Path) -> None:
    """Test loading entire directory structures from file system.

    Args:
        temp_workspace: Temporary directory for test files.
    """
    queries_dir = temp_workspace / "queries"
    queries_dir.mkdir()

    analytics_dir = queries_dir / "analytics"
    analytics_dir.mkdir()

    admin_dir = queries_dir / "admin"
    admin_dir.mkdir()

    (temp_workspace / "root.sql").write_text("""
-- name: health_check
SELECT 'OK' as status;
""")

    (queries_dir / "common.sql").write_text("""
-- name: get_system_info
SELECT version() as db_version;
""")

    (analytics_dir / "reports.sql").write_text("""
-- name: user_analytics
SELECT COUNT(*) as users, AVG(age) as avg_age FROM users;

-- name: sales_analytics
SELECT SUM(amount) as total_sales FROM orders;
""")

    (admin_dir / "management.sql").write_text("""
-- name: cleanup_old_logs
DELETE FROM logs WHERE created_at < :cutoff_date;
""")

    loader = SQLFileLoader()
    loader.load_sql(temp_workspace)

    queries = loader.list_queries()

    assert "health_check" in queries

    assert "queries.get_system_info" in queries
    assert "queries.analytics.user_analytics" in queries
    assert "queries.analytics.sales_analytics" in queries
    assert "queries.admin.cleanup_old_logs" in queries


def test_file_content_encoding_handling(temp_workspace: Path) -> None:
    """Test handling of different file encodings.

    Args:
        temp_workspace: Temporary directory for test files.
    """
    utf8_file = temp_workspace / "utf8_queries.sql"
    utf8_content = """
-- name: unicode_query
-- Test with Unicode: 测试 файл עברית
SELECT 'Unicode test: 测试' as message;
"""
    utf8_file.write_text(utf8_content, encoding="utf-8")

    loader = SQLFileLoader(encoding="utf-8")
    loader.load_sql(utf8_file)

    queries = loader.list_queries()
    assert "unicode_query" in queries

    sql = loader.get_sql("unicode_query")
    assert isinstance(sql, SQL)


def test_file_modification_detection(temp_workspace: Path) -> None:
    """Test detection of file modifications.

    Args:
        temp_workspace: Temporary directory for test files.
    """
    sql_file = temp_workspace / "modifiable.sql"
    original_content = """
-- name: original_query
SELECT 'original' as version;
"""
    sql_file.write_text(original_content)

    loader = SQLFileLoader()
    loader.load_sql(sql_file)

    sql = loader.get_sql("original_query")
    assert "original" in sql.sql

    modified_content = """
-- name: modified_query
SELECT 'modified' as version;

-- name: additional_query
SELECT 'new' as status;
"""
    time.sleep(0.1)
    sql_file.write_text(modified_content)

    loader.clear_cache()
    loader.load_sql(sql_file)

    queries = loader.list_queries()
    assert "modified_query" in queries
    assert "additional_query" in queries
    assert "original_query" not in queries


def test_symlink_resolution(temp_workspace: Path) -> None:
    """Test resolution of symbolic links.

    Args:
        temp_workspace: Temporary directory for test files.
    """
    original_file = temp_workspace / "original.sql"
    original_file.write_text("""
-- name: symlinked_query
SELECT 'from symlink' as source;
""")

    symlink_file = temp_workspace / "linked.sql"
    try:
        symlink_file.symlink_to(original_file)
    except OSError:
        pytest.skip("Symbolic links not supported on this system")

    loader = SQLFileLoader()
    loader.load_sql(symlink_file)

    queries = loader.list_queries()
    assert "symlinked_query" in queries


def test_nonexistent_file_error(temp_workspace: Path) -> None:
    """Test error handling for nonexistent files.

    Args:
        temp_workspace: Temporary directory for test files.

    Raises:
        SQLFileNotFoundError: When attempting to load nonexistent file.
    """
    loader = SQLFileLoader()
    nonexistent_file = temp_workspace / "does_not_exist.sql"

    with pytest.raises(SQLFileNotFoundError):
        loader.load_sql(nonexistent_file)


def test_nonexistent_directory_handling(temp_workspace: Path) -> None:
    """Test handling of nonexistent directories.

    Args:
        temp_workspace: Temporary directory for test files.
    """
    loader = SQLFileLoader()
    nonexistent_dir = temp_workspace / "does_not_exist"

    loader.load_sql(nonexistent_dir)

    assert loader.list_queries() == []
    assert loader.list_files() == []


def test_permission_denied_error(temp_workspace: Path) -> None:
    """Test handling of permission denied errors.

    Args:
        temp_workspace: Temporary directory for test files.

    Raises:
        SQLFileParseError: When file permissions prevent reading.
    """
    if os.name == "nt":
        pytest.skip("Permission testing not reliable on Windows")

    restricted_file = temp_workspace / "restricted.sql"
    restricted_file.write_text("""
-- name: restricted_query
SELECT 'restricted' as access;
""")

    restricted_file.chmod(0o000)

    try:
        loader = SQLFileLoader()

        with pytest.raises(SQLFileParseError):
            loader.load_sql(restricted_file)
    finally:
        restricted_file.chmod(0o644)


def test_corrupted_file_handling(temp_workspace: Path) -> None:
    """Test handling of corrupted or invalid SQL files.

    Args:
        temp_workspace: Temporary directory for test files.

    Raises:
        SQLFileParseError: When file contains invalid SQL format.
    """
    corrupted_file = temp_workspace / "corrupted.sql"

    corrupted_file.write_text("""
This is not a valid SQL file with named queries.
It has no proper -- name: declarations.
Just random text that should cause parsing to fail.
""")

    loader = SQLFileLoader()

    with pytest.raises(SQLFileParseError) as exc_info:
        loader.load_sql(corrupted_file)

    assert "No named SQL statements found" in str(exc_info.value)


def test_empty_file_handling(temp_workspace: Path) -> None:
    """Test handling of empty files.

    Args:
        temp_workspace: Temporary directory for test files.

    Raises:
        SQLFileParseError: When file is empty or contains no SQL statements.
    """
    empty_file = temp_workspace / "empty.sql"
    empty_file.write_text("")

    loader = SQLFileLoader()

    with pytest.raises(SQLFileParseError) as exc_info:
        loader.load_sql(empty_file)

    assert "No named SQL statements found" in str(exc_info.value)


def test_binary_file_handling(temp_workspace: Path) -> None:
    """Test handling of binary files with .sql extension.

    Args:
        temp_workspace: Temporary directory for test files.

    Raises:
        SQLFileParseError: When file contains binary data instead of text.
    """
    binary_file = temp_workspace / "binary.sql"

    with open(binary_file, "wb") as f:
        f.write(b"\x00\x01\x02\x03\x04\x05")

    loader = SQLFileLoader()

    with pytest.raises(SQLFileParseError):
        loader.load_sql(binary_file)


def test_large_file_loading_performance(temp_workspace: Path) -> None:
    """Test performance with large SQL files.

    Args:
        temp_workspace: Temporary directory for test files.
    """
    large_file = temp_workspace / "large_queries.sql"

    large_content = "\n".join(
        f"""
-- name: large_query_{i:04d}
SELECT {i} as query_id,
       'This is query number {i}' as description,
       CURRENT_TIMESTAMP as generated_at
FROM large_table
WHERE id > {i * 100}
  AND status = 'active'
  AND created_at > '2024-01-01'
ORDER BY id
LIMIT 1000;
"""
        for i in range(500)
    )
    large_file.write_text(large_content)

    loader = SQLFileLoader()

    start_time = time.time()
    loader.load_sql(large_file)
    end_time = time.time()

    load_time = end_time - start_time

    queries = loader.list_queries()
    assert len(queries) == 500

    assert load_time < 5.0, f"Loading took too long: {load_time:.2f}s"


def test_many_small_files_performance(temp_workspace: Path) -> None:
    """Test performance with many small SQL files.

    Args:
        temp_workspace: Temporary directory for test files.
    """
    files_dir = temp_workspace / "many_files"
    files_dir.mkdir()

    for i in range(100):
        small_file = files_dir / f"query_{i:03d}.sql"
        small_file.write_text(f"""
-- name: small_query_{i:03d}
SELECT {i} as file_number, 'small file {i}' as description;
""")

    loader = SQLFileLoader()

    start_time = time.time()
    loader.load_sql(files_dir)
    end_time = time.time()

    load_time = end_time - start_time

    queries = loader.list_queries()
    assert len(queries) == 100

    assert load_time < 10.0, f"Loading took too long: {load_time:.2f}s"


def test_deep_directory_structure_performance(temp_workspace: Path) -> None:
    """Test performance with deep directory structures.

    Args:
        temp_workspace: Temporary directory for test files.
    """
    current_path = temp_workspace
    for level in range(10):
        current_path = current_path / f"level_{level}"
        current_path.mkdir()

        sql_file = current_path / f"queries_level_{level}.sql"
        sql_file.write_text(f"""
-- name: deep_query_level_{level}
SELECT {level} as depth_level, 'level {level}' as description;
""")

    loader = SQLFileLoader()

    start_time = time.time()
    loader.load_sql(temp_workspace)
    end_time = time.time()

    load_time = end_time - start_time

    queries = loader.list_queries()
    assert len(queries) == 10

    deepest_namespace = ".".join([f"level_{i}" for i in range(10)])
    deepest_query = f"{deepest_namespace}.deep_query_level_9"
    assert deepest_query in queries

    assert load_time < 5.0, f"Loading took too long: {load_time:.2f}s"


def test_concurrent_file_modification(temp_workspace: Path) -> None:
    """Test handling of concurrent file modifications.

    Args:
        temp_workspace: Temporary directory for test files.
    """
    shared_file = temp_workspace / "shared.sql"

    shared_file.write_text("""
-- name: shared_query_v1
SELECT 'version 1' as version;
""")

    loader1 = SQLFileLoader()
    loader2 = SQLFileLoader()

    loader1.load_sql(shared_file)
    loader2.load_sql(shared_file)

    assert "shared_query_v1" in loader1.list_queries()
    assert "shared_query_v1" in loader2.list_queries()

    shared_file.write_text("""
-- name: shared_query_v2
SELECT 'version 2' as version;
""")

    loader1.clear_cache()
    loader1.load_sql(shared_file)

    assert "shared_query_v2" in loader1.list_queries()
    assert "shared_query_v1" not in loader1.list_queries()

    assert "shared_query_v1" in loader2.list_queries()
    assert "shared_query_v2" not in loader2.list_queries()


def test_multiple_loaders_same_file(temp_workspace: Path) -> None:
    """Test multiple loaders accessing the same file.

    Args:
        temp_workspace: Temporary directory for test files.
    """
    sql_file = temp_workspace / "multi_access.sql"
    sql_file.write_text("""
-- name: multi_access_query
SELECT 'accessed by multiple loaders' as message;
""")

    loaders = [SQLFileLoader() for _ in range(5)]

    for loader in loaders:
        loader.load_sql(sql_file)

    for i, loader in enumerate(loaders):
        queries = loader.list_queries()
        assert "multi_access_query" in queries, f"Loader {i} missing query"

        sql = loader.get_sql("multi_access_query")
        assert isinstance(sql, SQL)


def test_loader_isolation(temp_workspace: Path) -> None:
    """Test that loaders are properly isolated from each other.

    Args:
        temp_workspace: Temporary directory for test files.
    """
    file1 = temp_workspace / "loader1.sql"
    file2 = temp_workspace / "loader2.sql"

    file1.write_text("""
-- name: loader1_query
SELECT 'from loader 1' as source;
""")

    file2.write_text("""
-- name: loader2_query
SELECT 'from loader 2' as source;
""")

    loader1 = SQLFileLoader()
    loader2 = SQLFileLoader()

    loader1.load_sql(file1)
    loader2.load_sql(file2)

    queries1 = loader1.list_queries()
    queries2 = loader2.list_queries()

    assert "loader1_query" in queries1
    assert "loader1_query" not in queries2

    assert "loader2_query" in queries2
    assert "loader2_query" not in queries1


def test_file_cache_persistence_across_loaders(temp_workspace: Path) -> None:
    """Test that file cache persists across different loader instances.

    Args:
        temp_workspace: Temporary directory for test files.
    """

    sql_file = temp_workspace / "cached.sql"
    sql_file.write_text("""
-- name: cached_query
SELECT 'cached content' as status;
""")

    loader1 = SQLFileLoader()
    loader1.load_sql(sql_file)

    loader2 = SQLFileLoader()

    with patch("sqlspec.loader.get_cache_config") as mock_config:
        mock_cache_config = Mock()
        mock_cache_config.compiled_cache_enabled = True
        mock_config.return_value = mock_cache_config

        start_time = time.time()
        loader2.load_sql(sql_file)
        end_time = time.time()

        cache_load_time = end_time - start_time

        assert "cached_query" in loader2.list_queries()

        assert cache_load_time < 1.0


def test_cache_invalidation_on_file_change(temp_workspace: Path) -> None:
    """Test cache invalidation when files change.

    Args:
        temp_workspace: Temporary directory for test files.
    """

    sql_file = temp_workspace / "changing.sql"

    original_content = """
-- name: changing_query_v1
SELECT 'version 1' as version;
"""
    sql_file.write_text(original_content)

    with patch("sqlspec.loader.get_cache_config") as mock_config:
        mock_cache_config = Mock()
        mock_cache_config.compiled_cache_enabled = True
        mock_config.return_value = mock_cache_config

        loader = SQLFileLoader()
        loader.load_sql(sql_file)

        assert "changing_query_v1" in loader.list_queries()

        modified_content = """
-- name: changing_query_v2
SELECT 'version 2' as version;
"""
        time.sleep(0.1)
        sql_file.write_text(modified_content)

        loader.clear_cache()
        loader.load_sql(sql_file)

        queries = loader.list_queries()
        assert "changing_query_v2" in queries
        assert "changing_query_v1" not in queries


def test_cache_behavior_with_file_deletion(temp_workspace: Path) -> None:
    """Test cache behavior when cached files are deleted.

    Args:
        temp_workspace: Temporary directory for test files.

    Raises:
        SQLFileNotFoundError: When attempting to load deleted file.
    """
    sql_file = temp_workspace / "deletable.sql"
    sql_file.write_text("""
-- name: deletable_query
SELECT 'will be deleted' as status;
""")

    loader = SQLFileLoader()
    loader.load_sql(sql_file)

    assert "deletable_query" in loader.list_queries()

    sql_file.unlink()

    loader2 = SQLFileLoader()

    with pytest.raises(SQLFileNotFoundError):
        loader2.load_sql(sql_file)

    assert "deletable_query" in loader.list_queries()


def test_unicode_file_names(temp_workspace: Path) -> None:
    """Test handling of Unicode file names.

    Args:
        temp_workspace: Temporary directory for test files.
    """
    try:
        unicode_file = temp_workspace / "测试_файл_test.sql"
        unicode_file.write_text(
            """
-- name: unicode_filename_query
SELECT 'Unicode filename works' as message;
""",
            encoding="utf-8",
        )
    except OSError:
        pytest.skip("Unicode filenames not supported on this system")

    loader = SQLFileLoader()
    loader.load_sql(unicode_file)

    queries = loader.list_queries()
    assert "unicode_filename_query" in queries


def test_unicode_file_content(temp_workspace: Path) -> None:
    """Test handling of Unicode content in files.

    Args:
        temp_workspace: Temporary directory for test files.
    """
    unicode_file = temp_workspace / "unicode_content.sql"

    unicode_content = """
-- name: unicode_content_query
-- Unicode comment: 这是一个测试 файл на русском עברית
SELECT 'Unicode: 测试 тест עברית' as multilingual_message,
       'Symbols: ★ ♥ ⚡ ✓' as symbols,
       'Math: ∑ ∆ π ∞' as math_symbols;
"""
    unicode_file.write_text(unicode_content, encoding="utf-8")

    loader = SQLFileLoader(encoding="utf-8")
    loader.load_sql(unicode_file)

    queries = loader.list_queries()
    assert "unicode_content_query" in queries

    sql = loader.get_sql("unicode_content_query")
    assert "Unicode: 测试 тест עברית" in sql.sql


def test_mixed_encoding_handling(temp_workspace: Path) -> None:
    """Test handling of different encodings.

    Args:
        temp_workspace: Temporary directory for test files.
    """
    utf8_file = temp_workspace / "utf8.sql"
    utf8_file.write_text(
        """
-- name: utf8_query
SELECT 'UTF-8: 测试' as message;
""",
        encoding="utf-8",
    )

    latin1_file = temp_workspace / "latin1.sql"
    latin1_content = """
-- name: latin1_query
SELECT 'Latin-1: café' as message;
"""
    latin1_file.write_text(latin1_content, encoding="latin-1")

    utf8_loader = SQLFileLoader(encoding="utf-8")
    utf8_loader.load_sql(utf8_file)

    assert "utf8_query" in utf8_loader.list_queries()

    latin1_loader = SQLFileLoader(encoding="latin-1")
    latin1_loader.load_sql(latin1_file)

    assert "latin1_query" in latin1_loader.list_queries()


def test_special_characters_in_paths(temp_workspace: Path) -> None:
    """Test handling of special characters in file paths.

    Args:
        temp_workspace: Temporary directory for test files.
    """
    try:
        special_dir = temp_workspace / "special-chars_&_symbols!@#$"
        special_dir.mkdir()

        special_file = special_dir / "query-file_with&symbols.sql"
        special_file.write_text("""
-- name: special_path_query
SELECT 'Special path works' as result;
""")
    except OSError:
        pytest.skip("Special characters in paths not supported on this system")

    loader = SQLFileLoader()
    loader.load_sql(special_file)

    queries = loader.list_queries()
    assert "special_path_query" in queries
