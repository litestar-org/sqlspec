# pyright: reportPrivateImportUsage = false, reportPrivateUsage = false
"""Unit tests for SQLFileLoader class.

Tests for SQLFileLoader core functionality including:
- SQL file parsing and statement extraction
- Query name normalization and validation
- Cache integration and file content checksums
- Error handling and validation
- Parameter style detection and preservation
"""

import time
from pathlib import Path
from typing import Any

import pytest

from sqlspec.core import SQL
from sqlspec.exceptions import SQLFileNotFoundError, SQLFileParseError
from sqlspec.loader import (
    NamedStatement,
    SQLFile,
    SQLFileCacheEntry,
    SQLFileLoader,
    _normalize_dialect,
    _normalize_query_name,
)
from sqlspec.storage.registry import StorageRegistry
from tests.conftest import requires_interpreted

pytestmark = pytest.mark.xdist_group("loader")


def test_named_statement_creation() -> None:
    """Test basic NamedStatement creation."""
    stmt = NamedStatement("test_query", "SELECT 1", "postgres", 10)

    assert stmt.name == "test_query"
    assert stmt.sql == "SELECT 1"
    assert stmt.dialect == "postgres"
    assert stmt.start_line == 10


def test_named_statement_no_dialect() -> None:
    """Test NamedStatement creation without dialect."""
    stmt = NamedStatement("test_query", "SELECT 1")

    assert stmt.name == "test_query"
    assert stmt.sql == "SELECT 1"
    assert stmt.dialect is None
    assert stmt.start_line == 0


@requires_interpreted
def test_named_statement_slots() -> None:
    """Test that NamedStatement uses __slots__."""
    stmt = NamedStatement("test", "SELECT 1")

    assert hasattr(stmt.__class__, "__slots__")
    assert stmt.__class__.__slots__ == ("dialect", "name", "sql", "start_line")

    with pytest.raises(AttributeError):
        stmt.arbitrary_attr = "value"  # pyright: ignore[reportAttributeAccessIssue]


def test_sql_file_creation() -> None:
    """Test SQLFile creation with content and path."""
    content = "SELECT * FROM users WHERE id = ?"
    path = "/tmp/test.sql"

    sql_file = SQLFile(content=content, path=path)

    assert sql_file.content == content
    assert sql_file.path == path
    assert sql_file.metadata == {}
    assert sql_file.checksum
    assert sql_file.loaded_at


def test_sql_file_checksum_calculation() -> None:
    """Test that SQLFile calculates consistent checksums."""
    content = "SELECT * FROM users WHERE id = ?"

    file1 = SQLFile(content=content, path="path1")
    file2 = SQLFile(content=content, path="path2")
    file3 = SQLFile(content="Different content", path="path1")

    assert file1.checksum == file2.checksum

    assert file1.checksum != file3.checksum


def test_sql_file_with_metadata() -> None:
    """Test SQLFile creation with metadata."""
    metadata = {"author": "test", "version": "1.0"}
    sql_file = SQLFile("SELECT 1", "test.sql", metadata=metadata)

    assert sql_file.metadata == metadata


def test_cached_sqlfile_creation() -> None:
    """Test SQLFileCacheEntry creation."""
    sql_file = SQLFile("SELECT 1", "test.sql")
    statements = {"query1": NamedStatement("query1", "SELECT 1"), "query2": NamedStatement("query2", "SELECT 2")}

    cached_file = SQLFileCacheEntry(sql_file, statements)

    assert cached_file.sql_file == sql_file
    assert cached_file.parsed_statements == statements
    assert cached_file.statement_names == ("query1", "query2")


@requires_interpreted
def test_cached_sqlfile_slots() -> None:
    """Test that SQLFileCacheEntry uses __slots__."""
    sql_file = SQLFile("SELECT 1", "test.sql")
    cached_file = SQLFileCacheEntry(sql_file, {})

    assert hasattr(cached_file.__class__, "__slots__")
    assert cached_file.__class__.__slots__ == ("parsed_statements", "sql_file", "statement_names")


def test_default_initialization() -> None:
    """Test SQLFileLoader with default settings."""
    loader = SQLFileLoader()

    assert loader.encoding == "utf-8"
    assert loader.storage_registry is not None
    assert loader._queries == {}
    assert loader._files == {}
    assert loader._query_to_file == {}


def test_custom_encoding() -> None:
    """Test SQLFileLoader with custom encoding."""
    loader = SQLFileLoader(encoding="latin-1")
    assert loader.encoding == "latin-1"


def test_custom_storage_registry() -> None:
    """Test SQLFileLoader with custom storage registry."""
    registry = StorageRegistry()
    loader = SQLFileLoader(storage_registry=registry)
    assert loader.storage_registry == registry


def test_parse_simple_named_statements() -> None:
    """Test parsing basic named statements."""
    content = """
-- name: get_user
SELECT id, name FROM users WHERE id = :user_id;

-- name: create_user
INSERT INTO users (name, email) VALUES (:name, :email);
"""

    statements = SQLFileLoader._parse_sql_content(content, "test.sql")

    assert len(statements) == 2
    assert "get_user" in statements
    assert "create_user" in statements

    get_user = statements["get_user"]
    assert get_user.name == "get_user"
    assert "SELECT id, name FROM users" in get_user.sql
    assert get_user.dialect is None


def test_parse_statements_with_dialects() -> None:
    """Test parsing statements with dialect specifications."""
    content = """
-- name: postgres_query
-- dialect: postgresql
SELECT ARRAY_AGG(name) FROM users;

-- name: mysql_query
-- dialect: mysql
SELECT GROUP_CONCAT(name) FROM users;

-- name: generic_query
SELECT name FROM users;
"""

    statements = SQLFileLoader._parse_sql_content(content, "test.sql")

    assert len(statements) == 3

    postgres_query = statements["postgres_query"]
    assert postgres_query.dialect == "postgres"

    mysql_query = statements["mysql_query"]
    assert mysql_query.dialect == "mysql"

    generic_query = statements["generic_query"]
    assert generic_query.dialect is None


def test_parse_normalize_query_names() -> None:
    """Test query name normalization."""
    content = """
-- name: get-user-by-id
SELECT * FROM users WHERE id = ?;

-- name: list_active_users
SELECT * FROM users WHERE active = true;

-- name: update-user-email!
UPDATE users SET email = ? WHERE id = ?;
"""

    statements = SQLFileLoader._parse_sql_content(content, "test.sql")

    assert "get_user_by_id" in statements
    assert "list_active_users" in statements

    assert "update_user_email" in statements


def test_get_sql_eagerly_compiles_expression() -> None:
    """SQL objects from get_sql should have expressions eagerly compiled.

    This ensures that SQL objects from get_sql() can be used with pagination
    and count queries without additional compile() calls (fixes issue #283).
    """

    loader = SQLFileLoader()
    content = """
-- name: list_users
SELECT id, email FROM user_account WHERE active = true;
"""

    statements = SQLFileLoader._parse_sql_content(content, "test.sql")
    loader._queries = statements

    sql_obj = loader.get_sql("list_users")

    # get_sql() now eagerly compiles the SQL, so expression is populated
    assert sql_obj.expression is not None
    assert sql_obj.expression.key == "select"


def test_parse_skips_files_without_named_statements() -> None:
    """Test that files without named statements return empty dict."""
    content = "SELECT * FROM users;"

    statements = SQLFileLoader._parse_sql_content(content, "test.sql")

    assert statements == {}
    assert len(statements) == 0


def test_parse_error_duplicate_names() -> None:
    """Test error for duplicate statement names."""
    content = """
-- name: get_user
SELECT * FROM users WHERE id = 1;

-- name: get_user
SELECT * FROM users WHERE id = 2;
"""

    with pytest.raises(SQLFileParseError) as exc_info:
        SQLFileLoader._parse_sql_content(content, "test.sql")

    assert "Duplicate statement name: get_user" in str(exc_info.value)


def test_parse_invalid_dialect_storage() -> None:
    """Test that invalid dialect names are stored as-is without warnings."""
    content = """
-- name: test_query
-- dialect: invalid_dialect
SELECT * FROM users;
"""

    statements = SQLFileLoader._parse_sql_content(content, "test.sql")

    assert len(statements) == 1
    assert statements["test_query"].dialect == "invalid_dialect"


def test_parse_empty_file() -> None:
    """Test parsing empty file returns empty dict."""
    statements = SQLFileLoader._parse_sql_content("", "empty.sql")
    assert statements == {}


def test_parse_comments_only_file() -> None:
    """Test parsing file with only comments returns empty dict."""
    content = "-- This is a comment\n-- Another comment"
    statements = SQLFileLoader._parse_sql_content(content, "comments.sql")
    assert statements == {}


def test_load_directory_with_mixed_files(tmp_path: Path) -> None:
    """Test loading directory with named queries and raw DDL."""
    named_file = tmp_path / "queries.sql"
    named_file.write_text("""
-- name: get_user
SELECT * FROM users WHERE id = ?;
""")

    ddl_file = tmp_path / "schema.sql"
    ddl_file.write_text("""
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);
""")

    loader = SQLFileLoader()
    loader.load_sql(tmp_path)

    assert loader.has_query("get_user")
    assert str(ddl_file) not in loader._files
    assert len(loader.list_queries()) == 1


def test_skipped_file_logging(tmp_path: Path, caplog) -> None:
    """Test that skipped files are logged at DEBUG level."""
    import logging

    ddl_file = tmp_path / "schema.sql"
    ddl_file.write_text("CREATE TABLE users (id INTEGER);")

    loader = SQLFileLoader()

    with caplog.at_level(logging.DEBUG):
        loader.load_sql(ddl_file)

    assert "sql.load" in caplog.text


def test_strip_leading_comments() -> None:
    """Test stripping leading comments from SQL."""
    sql_text = """
-- This is a comment
-- Another comment
SELECT * FROM users;
"""

    result = SQLFileLoader._strip_leading_comments(sql_text)
    assert result == "SELECT * FROM users;"


def test_strip_leading_comments_all_comments() -> None:
    """Test stripping when all lines are comments."""
    sql_text = """
-- This is a comment
-- Another comment
"""

    result = SQLFileLoader._strip_leading_comments(sql_text)
    assert result == ""


def test_generate_file_cache_key() -> None:
    """Test file cache key generation."""
    loader = SQLFileLoader()

    path1 = "/path/to/file.sql"
    path2 = "/path/to/file.sql"
    path3 = "/different/path.sql"

    key1 = loader._generate_file_cache_key(path1)
    key2 = loader._generate_file_cache_key(path2)
    key3 = loader._generate_file_cache_key(path3)

    assert key1 == key2

    assert key1 != key3

    assert key1.startswith("file:")
    assert len(key1.split(":")[1]) == 16


def test_calculate_file_checksum(tmp_path: Path) -> None:
    """Test file checksum calculation."""
    sql_file = tmp_path / "test.sql"
    sql_file.write_text("SELECT * FROM users;")

    loader = SQLFileLoader()
    checksum = loader._calculate_file_checksum(str(sql_file))

    assert isinstance(checksum, str)
    assert len(checksum) == 32


def test_is_file_unchanged(tmp_path: Path) -> None:
    """Test file change detection."""
    sql_file = tmp_path / "test.sql"
    original_content = "SELECT * FROM users;"
    sql_file.write_text(original_content)

    loader = SQLFileLoader()

    sql_file_obj = SQLFile(original_content, str(sql_file))
    cached_file = SQLFileCacheEntry(sql_file_obj, {})

    assert loader._is_file_unchanged(str(sql_file), cached_file)

    sql_file.write_text("SELECT * FROM products;")

    assert not loader._is_file_unchanged(str(sql_file), cached_file)


def test_add_named_sql() -> None:
    """Test adding named SQL directly."""
    loader = SQLFileLoader()

    loader.add_named_sql("test_query", "SELECT 1", "postgres")

    assert "test_query" in loader._queries
    statement = loader._queries["test_query"]
    assert statement.name == "test_query"
    assert statement.sql == "SELECT 1"
    assert statement.dialect == "postgres"

    assert loader._query_to_file["test_query"] == "<directly added>"


def test_add_named_sql_duplicate_error() -> None:
    """Test error when adding duplicate query names."""
    loader = SQLFileLoader()

    loader.add_named_sql("test_query", "SELECT 1")

    with pytest.raises(ValueError) as exc_info:
        loader.add_named_sql("test_query", "SELECT 2")

    assert "Query name 'test_query' already exists" in str(exc_info.value)


def test_has_query() -> None:
    """Test query existence checking."""
    loader = SQLFileLoader()

    assert not loader.has_query("nonexistent")

    loader.add_named_sql("test_query", "SELECT 1")
    assert loader.has_query("test_query")
    assert loader.has_query("test-query")


def test_list_queries() -> None:
    """Test listing all queries."""
    loader = SQLFileLoader()

    assert loader.list_queries() == []

    loader.add_named_sql("query_a", "SELECT 1")
    loader.add_named_sql("query_b", "SELECT 2")

    queries = loader.list_queries()
    assert sorted(queries) == ["query_a", "query_b"]


def test_list_files() -> None:
    """Test listing loaded files."""
    loader = SQLFileLoader()

    assert loader.list_files() == []

    sql_file = SQLFile("SELECT 1", "/test/file.sql")
    loader._files["/test/file.sql"] = sql_file

    files = loader.list_files()
    assert files == ["/test/file.sql"]


def test_get_query_text() -> None:
    """Test getting raw query text."""
    loader = SQLFileLoader()

    loader.add_named_sql("test_query", "SELECT * FROM users")

    text = loader.get_query_text("test_query")
    assert text == "SELECT * FROM users"

    text = loader.get_query_text("test-query")
    assert text == "SELECT * FROM users"


def test_get_query_text_not_found() -> None:
    """Test error when getting text for nonexistent query."""
    loader = SQLFileLoader()

    with pytest.raises(SQLFileNotFoundError):
        loader.get_query_text("nonexistent")


def test_clear_cache() -> None:
    """Test clearing loader cache."""
    loader = SQLFileLoader()

    loader.add_named_sql("test_query", "SELECT 1")
    loader._files["test.sql"] = SQLFile("SELECT 1", "test.sql")

    assert len(loader._queries) > 0
    assert len(loader._files) > 0
    assert len(loader._query_to_file) > 0

    loader.clear_cache()

    assert len(loader._queries) == 0
    assert len(loader._files) == 0
    assert len(loader._query_to_file) == 0


def test_get_sql_basic() -> None:
    """Test getting basic SQL object."""
    loader = SQLFileLoader()
    loader.add_named_sql("test_query", "SELECT * FROM users WHERE id = ?")

    sql = loader.get_sql("test_query")

    assert isinstance(sql, SQL)
    assert "SELECT * FROM users WHERE id = ?" in sql.sql


def test_get_sql_simplified() -> None:
    """Test getting SQL without parameters (simplified interface)."""
    loader = SQLFileLoader()
    loader.add_named_sql("test_query", "SELECT * FROM users WHERE id = :user_id")

    sql = loader.get_sql("test_query")

    assert isinstance(sql, SQL)
    assert "SELECT * FROM users WHERE id = :user_id" in sql.sql

    assert sql.parameters == []


def test_get_sql_with_dialect() -> None:
    """Test getting SQL with stored dialect."""
    loader = SQLFileLoader()
    loader.add_named_sql("test_query", "SELECT * FROM users", dialect="postgres")

    sql = loader.get_sql("test_query")

    assert isinstance(sql, SQL)


def test_get_sql_parameter_style_detection() -> None:
    """Test parameter style detection and preservation."""
    loader = SQLFileLoader()
    loader.add_named_sql("qmark_query", "SELECT * FROM users WHERE id = ? AND active = ?")
    loader.add_named_sql("named_query", "SELECT * FROM users WHERE id = :user_id AND name = :name")

    qmark_sql = loader.get_sql("qmark_query")
    assert isinstance(qmark_sql, SQL)

    named_sql = loader.get_sql("named_query")
    assert isinstance(named_sql, SQL)


def test_get_sql_not_found() -> None:
    """Test error when SQL not found."""
    loader = SQLFileLoader()

    with pytest.raises(SQLFileNotFoundError) as exc_info:
        loader.get_sql("nonexistent")

    assert "Statement 'nonexistent' not found" in str(exc_info.value)


def test_get_sql_name_normalization() -> None:
    """Test query name normalization in get_sql."""
    loader = SQLFileLoader()
    loader.add_named_sql("test_query", "SELECT 1")

    sql1 = loader.get_sql("test_query")
    sql2 = loader.get_sql("test-query")

    assert isinstance(sql1, SQL)
    assert isinstance(sql2, SQL)


def test_get_sql_usage_pattern() -> None:
    """Test the simplified usage pattern for get_sql method."""
    loader = SQLFileLoader()

    asset_maintenance_query = """
with inserted_data as (
insert into alert_users (user_id, asset_maintenance_id, alert_definition_id)
select responsible_id, id, (select id from alert_definition where name = 'maintenances_today') from asset_maintenance
where planned_date_start is not null
and planned_date_start between :date_start and :date_end
and cancelled = False ON CONFLICT ON CONSTRAINT unique_alert DO NOTHING
returning *)
select inserted_data.*, to_jsonb(users.*) as user
from inserted_data
left join users on users.id = inserted_data.user_id;
"""

    loader.add_named_sql("asset_maintenance_alert", asset_maintenance_query.strip())

    sql = loader.get_sql("asset_maintenance_alert")

    assert isinstance(sql, SQL)
    assert "inserted_data" in sql.sql
    assert ":date_start" in sql.sql
    assert ":date_end" in sql.sql
    assert "alert_users" in sql.sql

    assert sql.parameters == []


def test_get_file_methods() -> None:
    """Test file retrieval methods."""
    loader = SQLFileLoader()

    sql_file = SQLFile("SELECT 1", "/test/file.sql")
    loader._files["/test/file.sql"] = sql_file
    loader.add_named_sql("test_query", "SELECT 1")
    loader._query_to_file["test_query"] = "/test/file.sql"

    retrieved_file = loader.get_file("/test/file.sql")
    assert retrieved_file == sql_file

    query_file = loader.get_file_for_query("test_query")
    assert query_file == sql_file

    assert loader.get_file("/nonexistent.sql") is None
    assert loader.get_file_for_query("nonexistent") is None


def test_parameter_style_detection_simplified() -> None:
    """Test that SQL objects are created without parameter style detection."""
    loader = SQLFileLoader()
    loader.add_named_sql("test_query", "SELECT * FROM users WHERE id = ? AND active = ?")

    sql = loader.get_sql("test_query")

    assert isinstance(sql, SQL)

    assert "SELECT * FROM users WHERE id = ? AND active = ?" in sql.sql


def test_dialect_normalization() -> None:
    """Test dialect normalization for various aliases."""
    test_cases = [
        ("postgresql", "postgres"),
        ("pg", "postgres"),
        ("pgplsql", "postgres"),
        ("plsql", "oracle"),
        ("oracledb", "oracle"),
        ("tsql", "mssql"),
        ("mysql", "mysql"),
        ("sqlite", "sqlite"),
    ]

    for input_dialect, expected in test_cases:
        result = _normalize_dialect(input_dialect)
        assert result == expected, f"Failed for {input_dialect}: got {result}, expected {expected}"


def test_query_name_normalization_edge_cases() -> None:
    """Test edge cases in query name normalization."""

    test_cases = [
        ("simple", "simple"),
        ("with-hyphens", "with_hyphens"),
        ("with_underscores", "with_underscores"),
        ("trailing-special!", "trailing_special"),
        ("multiple-hyphens-here", "multiple_hyphens_here"),
        ("mixed-_styles", "mixed_styles"),
        ("ending$", "ending"),
        ("complex-name$!", "complex_name"),
    ]

    for input_name, expected in test_cases:
        result = _normalize_query_name(input_name)
        assert result == expected, f"Failed for {input_name}: got {result}, expected {expected}"


def test_parse_empty_name_marker() -> None:
    """Test that empty name markers are skipped gracefully."""
    content = """
-- name:
SELECT * FROM users;
"""

    statements = SQLFileLoader._parse_sql_content(content, "test.sql")
    assert statements == {}


def test_file_read_error_handling() -> None:
    """Test handling of file read errors."""
    loader = SQLFileLoader()

    class MissingBackendRegistry(StorageRegistry):
        def get(self, uri_or_alias: str | Path, *, backend: str | None = None, **kwargs: Any) -> Any:
            raise KeyError("Backend not found")

    loader.storage_registry = MissingBackendRegistry()

    with pytest.raises(SQLFileNotFoundError):
        loader._read_file_content("/nonexistent/file.sql")


def test_checksum_calculation_error() -> None:
    """Test handling of checksum calculation errors."""
    loader = SQLFileLoader()

    with pytest.raises(SQLFileParseError):
        loader._calculate_file_checksum("/test/file.sql")


@pytest.mark.parametrize(
    "dialect,expected",
    [
        ("postgres", "postgres"),
        ("postgresql", "postgres"),
        ("pg", "postgres"),
        ("mysql", "mysql"),
        ("sqlite", "sqlite"),
        ("oracle", "oracle"),
        ("plsql", "oracle"),
        ("bigquery", "bigquery"),
        ("snowflake", "snowflake"),
    ],
)
def test_dialect_aliases_parametrized(dialect: str, expected: str) -> None:
    """Parameterized test for dialect aliases."""

    result = _normalize_dialect(dialect)
    assert result == expected


@pytest.mark.parametrize(
    "name,expected",
    [
        ("simple_name", "simple_name"),
        ("name-with-hyphens", "name_with_hyphens"),
        ("name$", "name"),
        ("name!", "name"),
        ("name$!", "name"),
        ("complex-name-with$special!", "complex_name_with_special"),
    ],
)
def test_query_name_normalization_parametrized(name: str, expected: str) -> None:
    """Parameterized test for query name normalization."""

    result = _normalize_query_name(name)
    assert result == expected


@pytest.fixture
def fixture_parsing_path() -> Path:
    """Get path to test fixtures directory for parsing tests."""
    return Path(__file__).parent.parent.parent / "fixtures"


def test_parse_postgres_database_details_fixture(fixture_parsing_path: Path) -> None:
    """Test parsing complex PostgreSQL database details fixture."""

    fixture_file = fixture_parsing_path / "postgres" / "collection-database_details.sql"

    content = fixture_file.read_text(encoding="utf-8")

    statements = SQLFileLoader._parse_sql_content(content, str(fixture_file))

    expected_queries = [
        "collection_postgres_base_database_details",
        "collection_postgres_13_database_details",
        "collection_postgres_12_database_details",
    ]

    assert len(statements) == len(expected_queries)
    for query_name in expected_queries:
        assert query_name in statements
        stmt = statements[query_name]
        assert isinstance(stmt, NamedStatement)
        assert stmt.name == query_name
        assert "database_oid" in stmt.sql
        assert ":PKEY" in stmt.sql or ":DMA_SOURCE_ID" in stmt.sql


def test_parse_mysql_data_types_fixture(fixture_parsing_path: Path) -> None:
    """Test parsing MySQL data types fixture."""

    fixture_file = fixture_parsing_path / "mysql" / "collection-data_types.sql"

    with open(fixture_file, encoding="utf-8") as f:
        content = f.read()

    statements = SQLFileLoader._parse_sql_content(content, str(fixture_file))

    assert len(statements) == 1
    assert "collection_mysql_data_types" in statements

    stmt = statements["collection_mysql_data_types"]
    assert "information_schema.columns" in stmt.sql
    assert "@PKEY" in stmt.sql or "@DMA_SOURCE_ID" in stmt.sql


def test_parse_init_fixture(fixture_parsing_path: Path) -> None:
    """Test parsing the init.sql fixture with multiple small queries."""

    fixture_file = fixture_parsing_path / "init.sql"

    with open(fixture_file, encoding="utf-8") as f:
        content = f.read()

    statements = SQLFileLoader._parse_sql_content(content, str(fixture_file))

    expected_queries = [
        "readiness_check_init_get_db_count",
        "readiness_check_init_get_execution_id",
        "readiness_check_init_get_source_id",
    ]

    assert len(statements) == len(expected_queries)
    for query_name in expected_queries:
        assert query_name in statements


def test_parse_oracle_ddl_fixture(fixture_parsing_path: Path) -> None:
    """Test parsing Oracle DDL fixture for complex SQL structures."""

    fixture_file = fixture_parsing_path / "oracle.ddl.sql"

    if not fixture_file.exists():
        pytest.skip("Oracle DDL fixture not found")

    with open(fixture_file, encoding="utf-8") as f:
        content = f.read()

    try:
        statements = SQLFileLoader._parse_sql_content(content, str(fixture_file))

        for stmt_name, stmt in statements.items():
            assert isinstance(stmt, NamedStatement)
            assert stmt.name == stmt_name
            assert len(stmt.sql.strip()) > 0
    except SQLFileParseError as e:
        assert "No named SQL statements found" in str(e)


def test_large_fixture_parsing_performance(fixture_parsing_path: Path) -> None:
    """Test parsing performance with large fixture files."""

    large_fixtures = [
        "postgres/collection-database_details.sql",
        "postgres/collection-table_details.sql",
        "mysql/collection-database_details.sql",
    ]

    SQLFileLoader()

    for fixture_path in large_fixtures:
        fixture_file = fixture_parsing_path / fixture_path
        if not fixture_file.exists():
            continue

        with open(fixture_file, encoding="utf-8") as f:
            content = f.read()

        start_time = time.time()
        statements = SQLFileLoader._parse_sql_content(content, str(fixture_file))
        parse_time = time.time() - start_time

        assert parse_time < 0.5, f"Parsing {fixture_path} took too long: {parse_time:.3f}s"
        assert len(statements) > 0, f"No statements found in {fixture_path}"


def test_fixture_parameter_style_detection(fixture_parsing_path: Path) -> None:
    """Test parameter style detection in fixture files."""

    test_cases = [
        ("postgres/collection-database_details.sql", ":PKEY"),
        ("mysql/collection-data_types.sql", "@PKEY"),
        ("init.sql", "pg_control_system"),
    ]

    for fixture_path, expected_pattern in test_cases:
        fixture_file = fixture_parsing_path / fixture_path
        if not fixture_file.exists():
            continue

        with open(fixture_file, encoding="utf-8") as f:
            content = f.read()

        statements = SQLFileLoader._parse_sql_content(content, str(fixture_file))

        found_pattern = False
        for stmt in statements.values():
            if expected_pattern in stmt.sql:
                found_pattern = True
                break

        assert found_pattern, f"Pattern '{expected_pattern}' not found in {fixture_path}"


def test_complex_cte_parsing_from_fixtures(fixture_parsing_path: Path) -> None:
    """Test parsing complex CTE queries from fixtures."""

    fixture_file = fixture_parsing_path / "postgres" / "collection-database_details.sql"

    with open(fixture_file, encoding="utf-8") as f:
        content = f.read()

    statements = SQLFileLoader._parse_sql_content(content, str(fixture_file))

    for stmt in statements.values():
        sql = stmt.sql.upper()
        if "WITH" in sql:
            assert "SELECT" in sql

            assert "JOIN" in sql or "WHERE" in sql or "FROM" in sql


def test_multi_dialect_fixture_parsing(fixture_parsing_path: Path) -> None:
    """Test parsing fixtures from multiple database dialects."""

    dialect_fixtures = [
        ("postgres", "collection-extensions.sql"),
        ("mysql", "collection-engines.sql"),
        ("oracle.ddl.sql", None),
    ]

    SQLFileLoader()

    for dialect_info in dialect_fixtures:
        if len(dialect_info) == 2 and dialect_info[1] is not None:
            dialect_dir, filename = dialect_info
            assert filename is not None
            fixture_file = fixture_parsing_path / dialect_dir / filename
        else:
            fixture_file = fixture_parsing_path / dialect_info[0]

        if not fixture_file.exists():
            continue

        with open(fixture_file, encoding="utf-8") as f:
            content = f.read()

        try:
            statements = SQLFileLoader._parse_sql_content(content, str(fixture_file))

            for stmt_name, stmt in statements.items():
                assert isinstance(stmt, NamedStatement)
                assert len(stmt.sql.strip()) > 0

                assert stmt.name == stmt_name

        except SQLFileParseError:
            pass


@pytest.fixture
def fixture_integration_path() -> Path:
    """Get path to test fixtures directory for integration tests."""
    return Path(__file__).parent.parent.parent / "fixtures"


def test_load_and_execute_fixture_queries(fixture_integration_path: Path) -> None:
    """Test loading and creating SQL objects from fixture queries."""

    fixture_file = fixture_integration_path / "init.sql"

    loader = SQLFileLoader()
    loader.load_sql(fixture_file)

    queries = loader.list_queries()
    assert len(queries) >= 3

    for query_name in queries:
        sql = loader.get_sql(query_name)
        assert isinstance(sql, SQL)
        assert len(sql.sql.strip()) > 0


def test_fixture_query_metadata_preservation(fixture_integration_path: Path) -> None:
    """Test that fixture query metadata is preserved."""

    fixture_file = fixture_integration_path / "postgres" / "collection-database_details.sql"

    loader = SQLFileLoader()
    loader.load_sql(fixture_file)

    files = loader.list_files()
    assert str(fixture_file) in files

    queries = loader.list_queries()
    for query_name in queries:
        file_info = loader.get_file_for_query(query_name)
        assert file_info is not None
        assert fixture_file.name in file_info.path


def test_fixture_parameter_extraction(fixture_integration_path: Path) -> None:
    """Test parameter extraction from fixture queries."""

    fixture_file = fixture_integration_path / "postgres" / "collection-database_details.sql"

    loader = SQLFileLoader()
    loader.load_sql(fixture_file)

    queries = loader.list_queries()
    test_query = queries[0]

    sql = loader.get_sql(test_query)
    assert isinstance(sql, SQL)

    assert sql.parameters == []
