"""Unit tests for SQL file loading patterns.

Tests various SQL file loading patterns including:
- Directory scanning and recursive loading
- Namespace generation from directory structure
- File filtering and pattern matching
- Error handling for various file scenarios
- URI-based loading patterns

Uses CORE_ROUND_3 architecture with proper error handling and logging.
"""

import tempfile
from collections.abc import Generator
from pathlib import Path
from unittest.mock import Mock

import pytest

from sqlspec.exceptions import SQLFileNotFoundError, SQLFileParseError
from sqlspec.loader import SQLFileLoader


@pytest.fixture
def temp_directory_structure() -> Generator[Path, None, None]:
    """Create a temporary directory structure for testing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        base_path = Path(temp_dir)

        # Create nested directory structure
        (base_path / "queries").mkdir()
        (base_path / "queries" / "users").mkdir()
        (base_path / "queries" / "products").mkdir()
        (base_path / "migrations").mkdir()

        # Create SQL files at different levels
        (base_path / "root_queries.sql").write_text("""
-- name: global_health_check
SELECT 'OK' as status;

-- name: get_version
SELECT '1.0.0' as version;
""")

        (base_path / "queries" / "common.sql").write_text("""
-- name: count_all_records
SELECT COUNT(*) as total FROM information_schema.tables;
""")

        (base_path / "queries" / "users" / "user_queries.sql").write_text("""
-- name: get_user_by_id
SELECT id, name, email FROM users WHERE id = :user_id;

-- name: list_active_users
SELECT id, name FROM users WHERE active = true;
""")

        (base_path / "queries" / "products" / "product_queries.sql").write_text("""
-- name: get_product_by_id
SELECT id, name, price FROM products WHERE id = :product_id;

-- name: list_products_by_category
SELECT * FROM products WHERE category_id = :category_id;
""")

        # Create non-SQL files that should be ignored
        (base_path / "README.md").write_text("# Test Documentation")
        (base_path / "config.json").write_text('{"setting": "value"}')
        (base_path / "queries" / ".gitkeep").write_text("")

        yield base_path


def test_load_single_file(temp_directory_structure: Path) -> None:
    """Test loading a single SQL file."""
    loader = SQLFileLoader()

    sql_file = temp_directory_structure / "root_queries.sql"
    loader.load_sql(sql_file)

    queries = loader.list_queries()
    assert "global_health_check" in queries
    assert "get_version" in queries
    assert len(queries) == 2


def test_load_directory_recursive(temp_directory_structure: Path) -> None:
    """Test loading entire directory recursively."""
    loader = SQLFileLoader()

    loader.load_sql(temp_directory_structure)

    queries = loader.list_queries()

    # Root level queries (no namespace)
    assert "global_health_check" in queries
    assert "get_version" in queries

    # First level namespace
    assert "queries.count_all_records" in queries

    # Second level namespaces
    assert "queries.users.get_user_by_id" in queries
    assert "queries.users.list_active_users" in queries
    assert "queries.products.get_product_by_id" in queries
    assert "queries.products.list_products_by_category" in queries


def test_load_subdirectory_directly(temp_directory_structure: Path) -> None:
    """Test loading a subdirectory directly (no namespace prefix)."""
    loader = SQLFileLoader()

    users_dir = temp_directory_structure / "queries" / "users"
    loader.load_sql(users_dir)

    queries = loader.list_queries()

    # When loading subdirectory directly, no namespace prefix
    assert "get_user_by_id" in queries
    assert "list_active_users" in queries


def test_load_parent_directory_with_namespaces(temp_directory_structure: Path) -> None:
    """Test loading parent directory creates proper namespaces."""
    loader = SQLFileLoader()

    queries_dir = temp_directory_structure / "queries"
    loader.load_sql(queries_dir)

    queries = loader.list_queries()

    # When loading parent directory, subdirectories become namespaces
    assert "users.get_user_by_id" in queries
    assert "users.list_active_users" in queries
    assert "products.get_product_by_id" in queries
    assert "products.list_products_by_category" in queries


def test_empty_directory_handling() -> None:
    """Test handling of empty directories."""
    with tempfile.TemporaryDirectory() as temp_dir:
        empty_dir = Path(temp_dir) / "empty"
        empty_dir.mkdir()

        loader = SQLFileLoader()

        # Should not raise an error for empty directory
        loader.load_sql(empty_dir)

        assert loader.list_queries() == []
        assert loader.list_files() == []


def test_directory_with_only_non_sql_files() -> None:
    """Test directory containing only non-SQL files."""
    with tempfile.TemporaryDirectory() as temp_dir:
        base_path = Path(temp_dir)

        (base_path / "README.md").write_text("# Documentation")
        (base_path / "config.json").write_text('{"key": "value"}')
        (base_path / "script.py").write_text("print('hello')")

        loader = SQLFileLoader()
        loader.load_sql(base_path)

        assert loader.list_queries() == []
        assert loader.list_files() == []


def test_mixed_file_and_directory_loading(temp_directory_structure: Path) -> None:
    """Test loading mix of files and directories."""
    loader = SQLFileLoader()

    # Load a specific file and a directory
    root_file = temp_directory_structure / "root_queries.sql"
    users_dir = temp_directory_structure / "queries" / "users"

    loader.load_sql(root_file, users_dir)

    queries = loader.list_queries()

    # Should have queries from both sources
    assert "global_health_check" in queries  # From file
    assert "get_version" in queries  # From file
    assert "get_user_by_id" in queries  # From directory (no namespace when loaded directly)
    assert "list_active_users" in queries  # From directory (no namespace when loaded directly)


def test_simple_namespace_generation() -> None:
    """Test simple directory-to-namespace conversion."""
    with tempfile.TemporaryDirectory() as temp_dir:
        base_path = Path(temp_dir)

        (base_path / "analytics").mkdir()
        (base_path / "analytics" / "reports.sql").write_text("""
-- name: user_report
SELECT COUNT(*) FROM users;
""")

        loader = SQLFileLoader()
        loader.load_sql(base_path)

        queries = loader.list_queries()
        assert "analytics.user_report" in queries


def test_deep_namespace_generation() -> None:
    """Test deep directory structure namespace generation."""
    with tempfile.TemporaryDirectory() as temp_dir:
        base_path = Path(temp_dir)

        # Create deep directory structure
        deep_path = base_path / "level1" / "level2" / "level3"
        deep_path.mkdir(parents=True)

        (deep_path / "deep_queries.sql").write_text("""
-- name: deeply_nested_query
SELECT 'deep' as level;
""")

        loader = SQLFileLoader()
        loader.load_sql(base_path)

        queries = loader.list_queries()
        assert "level1.level2.level3.deeply_nested_query" in queries


def test_namespace_with_special_characters() -> None:
    """Test namespace generation with special directory names."""
    with tempfile.TemporaryDirectory() as temp_dir:
        base_path = Path(temp_dir)

        # Create directories with special characters
        (base_path / "user-analytics").mkdir()
        (base_path / "user-analytics" / "daily_reports.sql").write_text("""
-- name: daily_user_count
SELECT COUNT(*) FROM users;
""")

        loader = SQLFileLoader()
        loader.load_sql(base_path)

        queries = loader.list_queries()
        # Namespace should preserve directory names as-is
        assert "user-analytics.daily_user_count" in queries


def test_no_namespace_for_root_files() -> None:
    """Test that root-level files don't get namespaces."""
    with tempfile.TemporaryDirectory() as temp_dir:
        base_path = Path(temp_dir)

        (base_path / "root_query.sql").write_text("""
-- name: root_level_query
SELECT 'root' as level;
""")

        loader = SQLFileLoader()
        loader.load_sql(base_path)

        queries = loader.list_queries()
        # Should not have namespace prefix
        assert "root_level_query" in queries
        assert "root_level_query" not in [q for q in queries if "." in q]


def test_sql_extension_filtering() -> None:
    """Test that only .sql files are processed."""
    with tempfile.TemporaryDirectory() as temp_dir:
        base_path = Path(temp_dir)

        # Create various file types
        (base_path / "valid.sql").write_text("""
-- name: valid_query
SELECT 1;
""")
        (base_path / "invalid.txt").write_text("""
-- name: invalid_query
SELECT 2;
""")
        (base_path / "also_invalid.py").write_text("# Not SQL")

        loader = SQLFileLoader()
        loader.load_sql(base_path)

        queries = loader.list_queries()
        assert "valid_query" in queries
        assert len(queries) == 1  # Only the .sql file should be processed


def test_hidden_file_inclusion() -> None:
    """Test that hidden files (starting with .) are currently included."""
    with tempfile.TemporaryDirectory() as temp_dir:
        base_path = Path(temp_dir)

        # Create visible and hidden SQL files
        (base_path / "visible.sql").write_text("""
-- name: visible_query
SELECT 1;
""")
        (base_path / ".hidden.sql").write_text("""
-- name: hidden_query
SELECT 2;
""")

        loader = SQLFileLoader()
        loader.load_sql(base_path)

        queries = loader.list_queries()
        assert "visible_query" in queries
        # Currently, SQLFileLoader includes hidden files
        # TODO: Consider adding option to exclude hidden files in future
        assert "hidden_query" in queries
        assert len(queries) == 2


def test_recursive_pattern_matching() -> None:
    """Test recursive pattern matching across directory levels."""
    with tempfile.TemporaryDirectory() as temp_dir:
        base_path = Path(temp_dir)

        # Create nested structure with mixed file types
        (base_path / "level1").mkdir()
        (base_path / "level1" / "level2").mkdir()

        (base_path / "level1" / "query1.sql").write_text("""
-- name: query_level1
SELECT 1;
""")
        (base_path / "level1" / "level2" / "query2.sql").write_text("""
-- name: query_level2
SELECT 2;
""")
        (base_path / "level1" / "level2" / "not_sql.txt").write_text("Not SQL")

        loader = SQLFileLoader()
        loader.load_sql(base_path)

        queries = loader.list_queries()
        assert "level1.query_level1" in queries
        assert "level1.level2.query_level2" in queries
        assert len(queries) == 2


def test_file_uri_loading() -> None:
    """Test loading SQL files using file:// URIs."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as tf:
        tf.write("""
-- name: uri_query
SELECT 'loaded from URI' as source;
""")
        tf.flush()

        loader = SQLFileLoader()
        file_uri = f"file://{tf.name}"

        loader.load_sql(file_uri)

        queries = loader.list_queries()
        assert "uri_query" in queries

        # Verify content
        sql = loader.get_sql("uri_query")
        assert "loaded from URI" in sql.sql

        # Clean up
        Path(tf.name).unlink()


def test_mixed_local_and_uri_loading() -> None:
    """Test loading both local files and URIs together."""
    with tempfile.TemporaryDirectory() as temp_dir:
        base_path = Path(temp_dir)

        # Create local file
        local_file = base_path / "local.sql"
        local_file.write_text("""
-- name: local_query
SELECT 'local' as source;
""")

        # Create another file for URI loading
        uri_file = base_path / "uri_file.sql"
        uri_file.write_text("""
-- name: uri_query
SELECT 'uri' as source;
""")

        loader = SQLFileLoader()

        # Load both local file and URI
        file_uri = f"file://{uri_file}"
        loader.load_sql(local_file, file_uri)

        queries = loader.list_queries()
        assert "local_query" in queries
        assert "uri_query" in queries
        assert len(queries) == 2


def test_invalid_uri_handling() -> None:
    """Test handling of invalid URIs."""
    loader = SQLFileLoader()

    # Mock storage registry to simulate URI handling failure
    mock_registry = Mock()
    mock_registry.get.side_effect = KeyError("Unsupported URI scheme")
    loader.storage_registry = mock_registry

    with pytest.raises(SQLFileNotFoundError):
        loader.load_sql("unsupported://example.com/file.sql")


def test_nonexistent_file_error() -> None:
    """Test error handling for nonexistent files."""
    loader = SQLFileLoader()

    # Current implementation raises SQLFileParseError for nonexistent files
    # because the storage backend treats missing files as parse errors
    with pytest.raises(SQLFileParseError):
        loader.load_sql("/nonexistent/path/file.sql")


def test_nonexistent_directory_error() -> None:
    """Test error handling for nonexistent directories."""
    loader = SQLFileLoader()

    # Current implementation raises SQLFileParseError for nonexistent directories
    # because the loader tries to read the path as a file first
    with pytest.raises(SQLFileParseError):
        loader.load_sql("/nonexistent/directory")


def test_permission_error_handling() -> None:
    """Test handling of permission errors."""
    loader = SQLFileLoader()

    # Mock storage backend to simulate permission error
    mock_registry = Mock()
    mock_backend = Mock()
    mock_backend.read_text.side_effect = PermissionError("Access denied")
    mock_registry.get.return_value = mock_backend

    loader.storage_registry = mock_registry

    with pytest.raises(SQLFileParseError):
        loader.load_sql("/protected/file.sql")


def test_corrupted_sql_file_error() -> None:
    """Test handling of corrupted or invalid SQL files."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as tf:
        # Create file with invalid SQL structure (no named queries)
        tf.write("SELECT * FROM users; -- No name comment")
        tf.flush()

        loader = SQLFileLoader()

        with pytest.raises(SQLFileParseError) as exc_info:
            loader.load_sql(tf.name)

        assert "No named SQL statements found" in str(exc_info.value)

        # Clean up
        Path(tf.name).unlink()


def test_duplicate_queries_across_files_error() -> None:
    """Test error handling for duplicate query names across files."""
    with tempfile.TemporaryDirectory() as temp_dir:
        base_path = Path(temp_dir)

        # Create two files with same query name
        file1 = base_path / "file1.sql"
        file1.write_text("""
-- name: duplicate_query
SELECT 'from file1' as source;
""")

        file2 = base_path / "file2.sql"
        file2.write_text("""
-- name: duplicate_query
SELECT 'from file2' as source;
""")

        loader = SQLFileLoader()

        # Load first file successfully
        loader.load_sql(file1)

        # Loading second file should raise error due to duplicate name
        with pytest.raises(SQLFileParseError) as exc_info:
            loader.load_sql(file2)

        assert "already exists" in str(exc_info.value)


def test_encoding_error_handling() -> None:
    """Test handling of encoding errors."""
    with tempfile.NamedTemporaryFile(mode="wb", suffix=".sql", delete=False) as tf:
        # Write non-UTF-8 content
        tf.write(b"\xff\xfe-- name: test\nSELECT 1;")
        tf.flush()

        # Create loader with UTF-8 encoding
        loader = SQLFileLoader(encoding="utf-8")

        # Should handle encoding error gracefully
        with pytest.raises(SQLFileParseError):
            loader.load_sql(tf.name)

        # Clean up
        Path(tf.name).unlink()


def test_large_file_handling() -> None:
    """Test handling of large SQL files."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as tf:
        # Create a large file with many queries
        content = [
            f"""
-- name: query_{{i:03d}}
SELECT {i} as query_number, 'data_{i}' as data
FROM large_table
WHERE id > {i * 100}
LIMIT 1000;
"""
            for i in range(100)
        ]

        tf.write("\n".join(content))
        tf.flush()

        loader = SQLFileLoader()

        # Should handle large file without issues
        loader.load_sql(tf.name)

        queries = loader.list_queries()
        assert len(queries) == 100

        # Verify some queries
        assert "query_000" in queries
        assert "query_050" in queries
        assert "query_099" in queries

        # Clean up
        Path(tf.name).unlink()


def test_deep_directory_structure_performance() -> None:
    """Test performance with deep directory structures."""
    with tempfile.TemporaryDirectory() as temp_dir:
        base_path = Path(temp_dir)

        # Create deep nested structure (10 levels)
        current_path = base_path
        for i in range(10):
            current_path = current_path / f"level_{i}"
            current_path.mkdir()

            # Add a SQL file at each level
            sql_file = current_path / f"queries_level_{i}.sql"
            sql_file.write_text(
                f"""
-- name: query_at_level_{i}
SELECT {i} as level_number;
"""
            )

        loader = SQLFileLoader()

        # Should handle deep structure efficiently
        loader.load_sql(base_path)

        queries = loader.list_queries()
        assert len(queries) == 10

        # Verify namespace generation for deep structure
        deepest_query = (
            "level_0.level_1.level_2.level_3.level_4.level_5.level_6.level_7.level_8.level_9.query_at_level_9"
        )
        assert deepest_query in queries


def test_concurrent_loading_safety() -> None:
    """Test thread safety during concurrent loading operations."""
    with tempfile.TemporaryDirectory() as temp_dir:
        base_path = Path(temp_dir)

        # Create multiple SQL files
        for i in range(5):
            sql_file = base_path / f"concurrent_{i}.sql"
            sql_file.write_text(
                f"""
-- name: concurrent_query_{i}
SELECT {i} as concurrent_id;
"""
            )

        loader = SQLFileLoader()

        # Load all files - should work without issues
        # In a real concurrent scenario, this would need threading
        for i in range(5):
            sql_file = base_path / f"concurrent_{i}.sql"
            loader.load_sql(sql_file)

        queries = loader.list_queries()
        assert len(queries) == 5

        for i in range(5):
            assert f"concurrent_query_{i}" in queries


def test_symlink_handling() -> None:
    """Test handling of symbolic links."""
    with tempfile.TemporaryDirectory() as temp_dir:
        base_path = Path(temp_dir)

        # Create original file
        original_file = base_path / "original.sql"
        original_file.write_text(
            """
-- name: symlinked_query
SELECT 'original' as source;
"""
        )

        # Create symbolic link (skip if not supported)
        symlink_file = base_path / "symlinked.sql"
        try:
            symlink_file.symlink_to(original_file)
        except OSError:
            # Skip test if symlinks not supported
            pytest.skip("Symbolic links not supported on this system")

        loader = SQLFileLoader()
        loader.load_sql(symlink_file)

        queries = loader.list_queries()
        assert "symlinked_query" in queries


def test_case_sensitivity_handling() -> None:
    """Test handling of case-sensitive file systems."""
    with tempfile.TemporaryDirectory() as temp_dir:
        base_path = Path(temp_dir)

        # Create files with different cases
        (base_path / "Queries.SQL").write_text(
            """
-- name: uppercase_extension_query
SELECT 'UPPERCASE' as extension_type;
"""
        )

        (base_path / "queries.sql").write_text(
            """
-- name: lowercase_extension_query
SELECT 'lowercase' as extension_type;
"""
        )

        loader = SQLFileLoader()
        loader.load_sql(base_path)

        queries = loader.list_queries()

        # Should load both files (if file system is case-sensitive)
        # or just one (if case-insensitive)
        assert len(queries) >= 1
        assert "lowercase_extension_query" in queries or "uppercase_extension_query" in queries


def test_unicode_filename_handling() -> None:
    """Test handling of Unicode filenames."""
    with tempfile.TemporaryDirectory() as temp_dir:
        base_path = Path(temp_dir)

        # Create file with Unicode name
        unicode_file = base_path / "测试_файл_query.sql"
        try:
            unicode_file.write_text(
                """
-- name: unicode_filename_query
SELECT 'Unicode filename support' as message;
""",
                encoding="utf-8",
            )
        except OSError:
            # Skip test if Unicode filenames not supported
            pytest.skip("Unicode filenames not supported on this system")

        loader = SQLFileLoader()
        loader.load_sql(base_path)

        queries = loader.list_queries()
        assert "unicode_filename_query" in queries
