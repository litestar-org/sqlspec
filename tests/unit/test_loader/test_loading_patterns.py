"""Unit tests for SQL file loading patterns.

Tests various SQL file loading patterns including:
- Directory scanning and recursive loading
- Namespace generation from directory structure
- File filtering and pattern matching
- Error handling for various file scenarios
- URI-based loading patterns
"""

from pathlib import Path
from typing import Any

import pytest

from sqlspec.core import SQL
from sqlspec.exceptions import SQLFileNotFoundError, SQLFileParseError
from sqlspec.loader import SQLFileLoader
from sqlspec.storage.registry import StorageRegistry

pytestmark = pytest.mark.xdist_group("loader")


@pytest.fixture
def temp_directory_structure(tmp_path: Path) -> Path:
    """Create a temporary directory structure for testing."""
    base_path = tmp_path

    (base_path / "queries").mkdir()
    (base_path / "queries" / "users").mkdir()
    (base_path / "queries" / "products").mkdir()
    (base_path / "migrations").mkdir()

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

    (base_path / "README.md").write_text("# Test Documentation")
    (base_path / "config.json").write_text('{"setting": "value"}')
    (base_path / "queries" / ".gitkeep").write_text("")

    return base_path


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

    assert "global_health_check" in queries
    assert "get_version" in queries

    assert "queries.count_all_records" in queries

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

    assert "get_user_by_id" in queries
    assert "list_active_users" in queries


def test_load_parent_directory_with_namespaces(temp_directory_structure: Path) -> None:
    """Test loading parent directory creates proper namespaces."""
    loader = SQLFileLoader()

    queries_dir = temp_directory_structure / "queries"
    loader.load_sql(queries_dir)

    queries = loader.list_queries()

    assert "users.get_user_by_id" in queries
    assert "users.list_active_users" in queries
    assert "products.get_product_by_id" in queries
    assert "products.list_products_by_category" in queries


def test_empty_directory_handling(tmp_path: Path) -> None:
    """Test handling of empty directories."""
    empty_dir = tmp_path / "empty"
    empty_dir.mkdir()

    loader = SQLFileLoader()

    loader.load_sql(empty_dir)

    assert loader.list_queries() == []
    assert loader.list_files() == []


def test_directory_with_only_non_sql_files(tmp_path: Path) -> None:
    """Test directory containing only non-SQL files."""
    base_path = tmp_path

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

    root_file = temp_directory_structure / "root_queries.sql"
    users_dir = temp_directory_structure / "queries" / "users"

    loader.load_sql(root_file, users_dir)

    queries = loader.list_queries()

    assert "global_health_check" in queries
    assert "get_version" in queries
    assert "get_user_by_id" in queries
    assert "list_active_users" in queries


def test_simple_namespace_generation(tmp_path: Path) -> None:
    """Test simple directory-to-namespace conversion."""
    base_path = tmp_path

    (base_path / "analytics").mkdir()
    (base_path / "analytics" / "reports.sql").write_text("""
-- name: user_report
SELECT COUNT(*) FROM users;
""")

    loader = SQLFileLoader()
    loader.load_sql(base_path)

    queries = loader.list_queries()
    assert "analytics.user_report" in queries


def test_deep_namespace_generation(tmp_path: Path) -> None:
    """Test deep directory structure namespace generation."""
    base_path = tmp_path

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


def test_namespace_with_special_characters(tmp_path: Path) -> None:
    """Test namespace generation with special directory names."""
    base_path = tmp_path

    (base_path / "user-analytics").mkdir()
    (base_path / "user-analytics" / "daily_reports.sql").write_text("""
-- name: daily_user_count
SELECT COUNT(*) FROM users;
""")

    loader = SQLFileLoader()
    loader.load_sql(base_path)

    queries = loader.list_queries()

    assert "user-analytics.daily_user_count" in queries


def test_no_namespace_for_root_files(tmp_path: Path) -> None:
    """Test that root-level files don't get namespaces."""
    base_path = tmp_path

    (base_path / "root_query.sql").write_text("""
-- name: root_level_query
SELECT 'root' as level;
""")

    loader = SQLFileLoader()
    loader.load_sql(base_path)

    queries = loader.list_queries()

    assert "root_level_query" in queries
    assert "root_level_query" not in [q for q in queries if "." in q]


def test_sql_extension_filtering(tmp_path: Path) -> None:
    """Test that only .sql files are processed."""
    base_path = tmp_path

    (base_path / "valid.sql").write_text("""
-- name: valid_query
SELECT 1;
""")
    (base_path / "invalid.txt").write_text("""
-- name: invalid_query
SELECT 2;
""")
    (base_path / "also_invalid.py").write_text("# Not a SQL file")

    loader = SQLFileLoader()
    loader.load_sql(base_path)

    queries = loader.list_queries()
    assert "valid_query" in queries
    assert len(queries) == 1


def test_hidden_file_inclusion(tmp_path: Path) -> None:
    """Test that hidden files (starting with .) are currently included."""
    base_path = tmp_path

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

    assert "hidden_query" in queries
    assert len(queries) == 2


def test_recursive_pattern_matching(tmp_path: Path) -> None:
    """Test recursive pattern matching across directory levels."""
    base_path = tmp_path

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


def test_file_uri_loading(tmp_path: Path) -> None:
    """Test loading SQL files using file:// URIs."""
    sql_file = tmp_path / "uri_test.sql"
    sql_file.write_text("""
-- name: uri_query
SELECT 'loaded from URI' as source;
""")

    loader = SQLFileLoader()
    file_uri = f"file://{sql_file}"

    loader.load_sql(file_uri)

    queries = loader.list_queries()
    assert "uri_query" in queries

    sql = loader.get_sql("uri_query")
    assert "loaded from URI" in sql.sql


def test_mixed_local_and_uri_loading(tmp_path: Path) -> None:
    """Test loading both local files and URIs together."""
    base_path = tmp_path

    local_file = base_path / "local.sql"
    local_file.write_text("""
-- name: local_query
SELECT 'local' as source;
""")

    uri_file = base_path / "uri_file.sql"
    uri_file.write_text("""
-- name: uri_query
SELECT 'uri' as source;
""")

    loader = SQLFileLoader()

    file_uri = f"file://{uri_file}"
    loader.load_sql(local_file, file_uri)

    queries = loader.list_queries()
    assert "local_query" in queries
    assert "uri_query" in queries
    assert len(queries) == 2


def test_invalid_uri_handling() -> None:
    """Test handling of invalid URIs."""
    loader = SQLFileLoader()

    class UnsupportedRegistry(StorageRegistry):
        def get(self, uri_or_alias: str | Path, *, backend: str | None = None, **kwargs: Any) -> Any:
            raise KeyError("Unsupported URI scheme")

    loader.storage_registry = UnsupportedRegistry()

    with pytest.raises(SQLFileNotFoundError):
        loader.load_sql("unsupported://example.com/file.sql")


def test_nonexistent_directory_error() -> None:
    """Test error handling for nonexistent directories."""
    loader = SQLFileLoader()

    loader.load_sql("/nonexistent/directory")

    assert loader.list_queries() == []
    assert loader.list_files() == []


def test_sql_file_without_named_statements_skipped(tmp_path: Path) -> None:
    """Test that SQL files without named statements are gracefully skipped."""
    sql_file = tmp_path / "no_names.sql"
    sql_file.write_text("SELECT * FROM users; -- No name comment")

    loader = SQLFileLoader()
    loader.load_sql(str(sql_file))

    assert len(loader.list_queries()) == 0
    assert str(sql_file) not in loader._files  # pyright: ignore


def test_duplicate_queries_across_files_error(tmp_path: Path) -> None:
    """Test error handling for duplicate query names across files."""
    base_path = tmp_path

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

    loader.load_sql(file1)

    with pytest.raises(SQLFileParseError) as exc_info:
        loader.load_sql(file2)

    assert "already exists" in str(exc_info.value)


def test_encoding_error_handling(tmp_path: Path) -> None:
    """Test handling of encoding errors."""
    sql_file = tmp_path / "bad_encoding.sql"
    sql_file.write_bytes(b"\xff\xfe-- name: test\nSELECT 1;")

    loader = SQLFileLoader(encoding="utf-8")

    with pytest.raises(SQLFileParseError):
        loader.load_sql(str(sql_file))


def test_large_file_handling(tmp_path: Path) -> None:
    """Test handling of large SQL files."""
    sql_file = tmp_path / "large.sql"
    content = [
        f"""
-- name: query_{i:03d}
SELECT {i} as query_number, 'data_{i}' as data
FROM large_table
WHERE id > {i * 100}
LIMIT 1000;
"""
        for i in range(100)
    ]

    sql_file.write_text("\n".join(content))

    loader = SQLFileLoader()

    loader.load_sql(str(sql_file))

    queries = loader.list_queries()
    assert len(queries) == 100

    assert "query_000" in queries
    assert "query_050" in queries
    assert "query_099" in queries


def test_deep_directory_structure_performance(tmp_path: Path) -> None:
    """Test performance with deep directory structures."""
    base_path = tmp_path

    current_path = base_path
    for i in range(10):
        current_path = current_path / f"level_{i}"
        current_path.mkdir()

        sql_file = current_path / f"queries_level_{i}.sql"
        sql_file.write_text(
            f"""
-- name: query_at_level_{i}
SELECT {i} as level_number;
"""
        )

    loader = SQLFileLoader()

    loader.load_sql(base_path)

    queries = loader.list_queries()
    assert len(queries) == 10

    deepest_query = "level_0.level_1.level_2.level_3.level_4.level_5.level_6.level_7.level_8.level_9.query_at_level_9"
    assert deepest_query in queries


def test_concurrent_loading_safety(tmp_path: Path) -> None:
    """Test thread safety during concurrent loading operations."""
    base_path = tmp_path

    for i in range(5):
        sql_file = base_path / f"concurrent_{i}.sql"
        sql_file.write_text(
            f"""
-- name: concurrent_query_{i}
SELECT {i} as concurrent_id;
"""
        )

    loader = SQLFileLoader()

    for i in range(5):
        sql_file = base_path / f"concurrent_{i}.sql"
        loader.load_sql(sql_file)

    queries = loader.list_queries()
    assert len(queries) == 5

    for i in range(5):
        assert f"concurrent_query_{i}" in queries


def test_symlink_handling(tmp_path: Path) -> None:
    """Test handling of symbolic links."""
    base_path = tmp_path

    original_file = base_path / "original.sql"
    original_file.write_text(
        """
-- name: symlinked_query
SELECT 'original' as source;
"""
    )

    symlink_file = base_path / "symlinked.sql"
    try:
        symlink_file.symlink_to(original_file)
    except OSError:
        pytest.skip("Symbolic links unsupported")

    loader = SQLFileLoader()
    loader.load_sql(symlink_file)

    queries = loader.list_queries()
    assert "symlinked_query" in queries


def test_case_sensitivity_handling(tmp_path: Path) -> None:
    """Test handling of case-sensitive file systems."""
    base_path = tmp_path

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

    assert len(queries) >= 1
    assert "lowercase_extension_query" in queries or "uppercase_extension_query" in queries


def test_unicode_filename_handling(tmp_path: Path) -> None:
    """Test handling of Unicode filenames."""
    base_path = tmp_path

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
        pytest.skip("Unicode filenames unsupported")

    loader = SQLFileLoader()
    loader.load_sql(base_path)

    queries = loader.list_queries()
    assert "unicode_filename_query" in queries


@pytest.fixture
def fixtures_path() -> Path:
    """Get path to test fixtures directory."""
    return Path(__file__).parent.parent.parent / "fixtures"


def test_large_fixture_loading_performance(fixtures_path: Path) -> None:
    """Test performance loading large fixture files."""
    import time

    large_fixtures = [
        "postgres/collection-database_details.sql",
        "postgres/collection-table_details.sql",
        "postgres/collection-schema_details.sql",
        "mysql/collection-database_details.sql",
        "mysql/collection-table_details.sql",
    ]

    performance_results = {}

    for fixture_path in large_fixtures:
        fixture_file = fixtures_path / fixture_path
        if not fixture_file.exists():
            continue

        loader = SQLFileLoader()

        start_time = time.time()
        loader.load_sql(fixture_file)
        load_time = time.time() - start_time

        queries = loader.list_queries()
        performance_results[fixture_path] = {
            "load_time": load_time,
            "query_count": len(queries),
            "file_size": fixture_file.stat().st_size,
        }

        assert load_time < 2.0, f"Loading {fixture_path} took too long: {load_time:.3f}s"
        assert len(queries) > 0, f"No queries loaded from {fixture_path}"

        if queries:
            test_query = queries[0]
            sql_start = time.time()
            sql_obj = loader.get_sql(test_query)
            sql_time = time.time() - sql_start

            assert sql_time < 0.1, f"SQL object creation too slow: {sql_time:.3f}s"
            assert isinstance(sql_obj, SQL)


def test_multiple_fixture_batch_loading(fixtures_path: Path) -> None:
    """Test performance when loading multiple fixture files at once."""
    import time

    fixture_files = [
        fixtures_path / "init.sql",
        fixtures_path / "postgres" / "collection-extensions.sql",
        fixtures_path / "mysql" / "collection-engines.sql",
        fixtures_path / "postgres" / "collection-privileges.sql",
    ]

    existing_files = [f for f in fixture_files if f.exists()]
    if len(existing_files) < 2:
        pytest.skip("Need 2+ fixtures for batch test")

    loader = SQLFileLoader()

    start_time = time.time()
    loader.load_sql(*existing_files)
    total_load_time = time.time() - start_time

    all_queries = loader.list_queries()
    assert len(all_queries) > 0

    assert total_load_time < 3.0, f"Batch loading took too long: {total_load_time:.3f}s"

    loaded_files = loader.list_files()
    for fixture_file in existing_files:
        assert str(fixture_file) in loaded_files


def test_fixture_directory_scanning_performance(fixtures_path: Path) -> None:
    """Test performance when scanning fixture directories."""
    import time

    test_dirs = [fixtures_path / "postgres", fixtures_path / "mysql"]

    for test_dir in test_dirs:
        if not test_dir.exists():
            continue

        loader = SQLFileLoader()

        start_time = time.time()
        loader.load_sql(test_dir)
        scan_time = time.time() - start_time

        queries = loader.list_queries()
        files = loader.list_files()

        assert scan_time < 5.0, f"Directory scanning took too long: {scan_time:.3f}s"
        assert len(queries) > 0, f"No queries found in {test_dir}"
        assert len(files) > 0, f"No files loaded from {test_dir}"

        if test_dir.name in ["postgres", "mysql"]:
            assert len(queries) > 0, f"No queries found in {test_dir}"


def test_fixture_cache_performance(fixtures_path: Path) -> None:
    """Test performance benefits of caching with fixture files."""
    import time

    fixture_file = fixtures_path / "postgres" / "collection-database_details.sql"
    if not fixture_file.exists():
        pytest.skip("Large fixture missing")

    loader1 = SQLFileLoader()
    start_time = time.time()
    loader1.load_sql(fixture_file)
    first_load_time = time.time() - start_time

    start_time = time.time()
    loader1.load_sql(fixture_file)
    cached_load_time = time.time() - start_time

    assert cached_load_time <= first_load_time, "Cached load should not be slower than first load"

    queries1 = loader1.list_queries()
    assert len(queries1) > 0


def test_concurrent_fixture_access_simulation(fixtures_path: Path) -> None:
    """Test simulated concurrent access to fixture files."""
    import time

    fixture_file = fixtures_path / "init.sql"

    loaders = []
    load_times = []

    for i in range(5):
        loader = SQLFileLoader()

        start_time = time.time()
        loader.load_sql(fixture_file)
        load_time = time.time() - start_time

        loaders.append(loader)
        load_times.append(load_time)

        queries = loader.list_queries()
        assert len(queries) > 0

        assert load_time < 1.0, f"Load {i + 1} took too long: {load_time:.3f}s"

    base_queries = set(loaders[0].list_queries())
    for loader in loaders[1:]:
        assert set(loader.list_queries()) == base_queries


def test_memory_usage_with_large_fixtures(fixtures_path: Path) -> None:
    """Test memory usage patterns with large fixture files."""
    large_fixtures = ["postgres/collection-database_details.sql", "postgres/collection-table_details.sql"]

    loader = SQLFileLoader()
    initial_query_count = len(loader.list_queries())

    for fixture_path in large_fixtures:
        fixture_file = fixtures_path / fixture_path
        if not fixture_file.exists():
            continue

        loader.load_sql(fixture_file)

        queries = loader.list_queries()

        assert len(queries) > initial_query_count

        for query_name in queries[:5]:
            sql_obj = loader.get_sql(query_name)
            assert isinstance(sql_obj, SQL)

            assert len(str(sql_obj)) < 50000

        initial_query_count = len(queries)
