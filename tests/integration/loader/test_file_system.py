"""Integration tests for file system loading.

Tests real file system operations including:
- Loading from local file system paths
- File system watching and cache invalidation
- Permission handling and error scenarios
- Large file handling and performance
- Concurrent file access patterns
"""

import os
import time
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from sqlspec.core import SQL
from sqlspec.exceptions import SQLFileNotFoundError, SQLFileParseError
from sqlspec.loader import SQLFileLoader


def test_load_single_file_from_filesystem(tmp_path: Path) -> None:
    """Test loading a single SQL file from the file system.

    Args:
        tmp_path: Temporary directory for test files.
    """
    sql_file = tmp_path / "test_queries.sql"
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


def test_load_multiple_files_from_filesystem(tmp_path: Path) -> None:
    """Test loading multiple SQL files from the file system.

    Args:
        tmp_path: Temporary directory for test files.
    """
    users_file = tmp_path / "users.sql"
    users_file.write_text("""
-- name: create_user
INSERT INTO users (name, email) VALUES (:name, :email);

-- name: update_user_email
UPDATE users SET email = :email WHERE id = :user_id;
""")

    products_file = tmp_path / "products.sql"
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


def test_load_directory_structure_from_filesystem(tmp_path: Path) -> None:
    """Test loading entire directory structures from file system.

    Args:
        tmp_path: Temporary directory for test files.
    """
    queries_dir = tmp_path / "queries"
    queries_dir.mkdir()

    analytics_dir = queries_dir / "analytics"
    analytics_dir.mkdir()

    admin_dir = queries_dir / "admin"
    admin_dir.mkdir()

    (tmp_path / "root.sql").write_text("""
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
    loader.load_sql(tmp_path)

    queries = loader.list_queries()

    assert "health_check" in queries

    assert "queries.get_system_info" in queries
    assert "queries.analytics.user_analytics" in queries
    assert "queries.analytics.sales_analytics" in queries
    assert "queries.admin.cleanup_old_logs" in queries


def test_file_content_encoding_handling(tmp_path: Path) -> None:
    """Test handling of different file encodings.

    Args:
        tmp_path: Temporary directory for test files.
    """
    utf8_file = tmp_path / "utf8_queries.sql"
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


def test_file_modification_detection(tmp_path: Path) -> None:
    """Test detection of file modifications.

    Args:
        tmp_path: Temporary directory for test files.
    """
    sql_file = tmp_path / "modifiable.sql"
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


def test_symlink_resolution(tmp_path: Path) -> None:
    """Test resolution of symbolic links.

    Args:
        tmp_path: Temporary directory for test files.
    """
    original_file = tmp_path / "original.sql"
    original_file.write_text("""
-- name: symlinked_query
SELECT 'from symlink' as source;
""")

    symlink_file = tmp_path / "linked.sql"
    try:
        symlink_file.symlink_to(original_file)
    except OSError:
        pytest.skip("Symbolic links unsupported")

    loader = SQLFileLoader()
    loader.load_sql(symlink_file)

    queries = loader.list_queries()
    assert "symlinked_query" in queries


def test_nonexistent_file_error(tmp_path: Path) -> None:
    """Test error handling for nonexistent files.

    Args:
        tmp_path: Temporary directory for test files.

    Raises:
        SQLFileNotFoundError: When attempting to load nonexistent file.
    """
    loader = SQLFileLoader()
    nonexistent_file = tmp_path / "does_not_exist.sql"

    with pytest.raises(SQLFileNotFoundError):
        loader.load_sql(nonexistent_file)


def test_nonexistent_directory_handling(tmp_path: Path) -> None:
    """Test handling of nonexistent directories.

    Args:
        tmp_path: Temporary directory for test files.
    """
    loader = SQLFileLoader()
    nonexistent_dir = tmp_path / "does_not_exist"

    loader.load_sql(nonexistent_dir)

    assert loader.list_queries() == []
    assert loader.list_files() == []


def test_permission_denied_error(tmp_path: Path) -> None:
    """Test handling of permission denied errors.

    Args:
        tmp_path: Temporary directory for test files.

    Raises:
        SQLFileParseError: When file permissions prevent reading.
    """
    if os.name == "nt":
        pytest.skip("Windows permissions unreliable")

    restricted_file = tmp_path / "restricted.sql"
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


def test_corrupted_file_handling(tmp_path: Path) -> None:
    """Test handling of SQL files without named statements.

    Args:
        tmp_path: Temporary directory for test files.
    """
    corrupted_file = tmp_path / "corrupted.sql"

    corrupted_file.write_text("""
This is not a valid SQL file with named queries.
It has no proper -- name: declarations.
Just random text that should be gracefully skipped.
""")

    loader = SQLFileLoader()

    loader.load_sql(corrupted_file)

    assert len(loader.list_queries()) == 0
    assert str(corrupted_file) not in loader._files  # pyright: ignore


def test_empty_file_handling(tmp_path: Path) -> None:
    """Test handling of empty files.

    Args:
        tmp_path: Temporary directory for test files.
    """
    empty_file = tmp_path / "empty.sql"
    empty_file.write_text("")

    loader = SQLFileLoader()

    loader.load_sql(empty_file)

    assert len(loader.list_queries()) == 0
    assert str(empty_file) not in loader._files  # pyright: ignore


def test_binary_file_handling(tmp_path: Path) -> None:
    """Test handling of binary files with .sql extension.

    Args:
        tmp_path: Temporary directory for test files.

    Raises:
        SQLFileParseError: When file contains binary data that can't be decoded.
    """
    binary_file = tmp_path / "binary.sql"

    Path(binary_file).write_bytes(b"\xff\xfe\xfd\xfc")

    loader = SQLFileLoader(encoding="utf-8")

    with pytest.raises(SQLFileParseError):
        loader.load_sql(binary_file)


def test_large_file_loading_performance(tmp_path: Path) -> None:
    """Test performance with large SQL files.

    Args:
        tmp_path: Temporary directory for test files.
    """
    large_file = tmp_path / "large_queries.sql"

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


def test_many_small_files_performance(tmp_path: Path) -> None:
    """Test performance with many small SQL files.

    Args:
        tmp_path: Temporary directory for test files.
    """
    files_dir = tmp_path / "many_files"
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


def test_deep_directory_structure_performance(tmp_path: Path) -> None:
    """Test performance with deep directory structures.

    Args:
        tmp_path: Temporary directory for test files.
    """
    current_path = tmp_path
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
    loader.load_sql(tmp_path)
    end_time = time.time()

    load_time = end_time - start_time

    queries = loader.list_queries()
    assert len(queries) == 10

    deepest_namespace = ".".join([f"level_{i}" for i in range(10)])
    deepest_query = f"{deepest_namespace}.deep_query_level_9"
    assert deepest_query in queries

    assert load_time < 5.0, f"Loading took too long: {load_time:.2f}s"


def test_concurrent_file_modification(tmp_path: Path) -> None:
    """Test handling of concurrent file modifications.

    Args:
        tmp_path: Temporary directory for test files.
    """
    shared_file = tmp_path / "shared.sql"

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


def test_multiple_loaders_same_file(tmp_path: Path) -> None:
    """Test multiple loaders accessing the same file.

    Args:
        tmp_path: Temporary directory for test files.
    """
    sql_file = tmp_path / "multi_access.sql"
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


def test_loader_isolation(tmp_path: Path) -> None:
    """Test that loaders are properly isolated from each other.

    Args:
        tmp_path: Temporary directory for test files.
    """
    file1 = tmp_path / "loader1.sql"
    file2 = tmp_path / "loader2.sql"

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


def test_file_cache_persistence_across_loaders(tmp_path: Path) -> None:
    """Test that file cache persists across different loader instances.

    Args:
        tmp_path: Temporary directory for test files.
    """

    sql_file = tmp_path / "cached.sql"
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


def test_cache_invalidation_on_file_change(tmp_path: Path) -> None:
    """Test cache invalidation when files change.

    Args:
        tmp_path: Temporary directory for test files.
    """

    sql_file = tmp_path / "changing.sql"

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


def test_cache_behavior_with_file_deletion(tmp_path: Path) -> None:
    """Test cache behavior when cached files are deleted.

    Args:
        tmp_path: Temporary directory for test files.

    Raises:
        SQLFileNotFoundError: When attempting to load deleted file.
    """
    sql_file = tmp_path / "deletable.sql"
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


def test_unicode_file_names(tmp_path: Path) -> None:
    """Test handling of Unicode file names.

    Args:
        tmp_path: Temporary directory for test files.
    """
    try:
        unicode_file = tmp_path / "测试_файл_test.sql"
        unicode_file.write_text(
            """
-- name: unicode_filename_query
SELECT 'Unicode filename works' as message;
""",
            encoding="utf-8",
        )
    except OSError:
        pytest.skip("Unicode filenames unsupported")

    loader = SQLFileLoader()
    loader.load_sql(unicode_file)

    queries = loader.list_queries()
    assert "unicode_filename_query" in queries


def test_unicode_file_content(tmp_path: Path) -> None:
    """Test handling of Unicode content in files.

    Args:
        tmp_path: Temporary directory for test files.
    """
    unicode_file = tmp_path / "unicode_content.sql"

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


def test_mixed_encoding_handling(tmp_path: Path) -> None:
    """Test handling of different encodings.

    Args:
        tmp_path: Temporary directory for test files.
    """
    utf8_file = tmp_path / "utf8.sql"
    utf8_file.write_text(
        """
-- name: utf8_query
SELECT 'UTF-8: 测试' as message;
""",
        encoding="utf-8",
    )

    latin1_file = tmp_path / "latin1.sql"
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


def test_special_characters_in_paths(tmp_path: Path) -> None:
    """Test handling of special characters in file paths.

    Args:
        tmp_path: Temporary directory for test files.
    """
    try:
        special_dir = tmp_path / "special-chars_&_symbols!@#$"
        special_dir.mkdir()

        special_file = special_dir / "query-file_with&symbols.sql"
        special_file.write_text("""
-- name: special_path_query
SELECT 'Special path works' as result;
""")
    except OSError:
        pytest.skip("Special characters unsupported")

    loader = SQLFileLoader()
    loader.load_sql(special_file)

    queries = loader.list_queries()
    assert "special_path_query" in queries
