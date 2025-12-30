"""Unit tests for canonicalized checksum computation."""

from pathlib import Path

import pytest

from sqlspec.migrations.runner import SyncMigrationRunner

# pyright: reportPrivateUsage=false


@pytest.fixture
def temp_migrations_dir(tmp_path: Path) -> Path:
    """Create temporary migrations directory."""
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    return migrations_dir


def test_checksum_excludes_timestamp_version_up_header(temp_migrations_dir: Path) -> None:
    """Test checksum excludes timestamp version up query name header."""
    runner = SyncMigrationRunner(temp_migrations_dir)

    content = """-- name: migrate-20251011120000-up
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);
"""

    checksum = runner._calculate_checksum(content)

    content_without_header = """
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);
"""

    checksum_without_header = runner._calculate_checksum(content_without_header)

    assert checksum == checksum_without_header


def test_checksum_excludes_timestamp_version_down_header(temp_migrations_dir: Path) -> None:
    """Test checksum excludes timestamp version down query name header."""
    runner = SyncMigrationRunner(temp_migrations_dir)

    content = """-- name: migrate-20251011120000-down
DROP TABLE users;
"""

    checksum = runner._calculate_checksum(content)

    content_without_header = """
DROP TABLE users;
"""

    checksum_without_header = runner._calculate_checksum(content_without_header)

    assert checksum == checksum_without_header


def test_checksum_excludes_sequential_version_up_header(temp_migrations_dir: Path) -> None:
    """Test checksum excludes sequential version up query name header."""
    runner = SyncMigrationRunner(temp_migrations_dir)

    content = """-- name: migrate-0001-up
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);
"""

    checksum = runner._calculate_checksum(content)

    content_without_header = """
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);
"""

    checksum_without_header = runner._calculate_checksum(content_without_header)

    assert checksum == checksum_without_header


def test_checksum_excludes_sequential_version_down_header(temp_migrations_dir: Path) -> None:
    """Test checksum excludes sequential version down query name header."""
    runner = SyncMigrationRunner(temp_migrations_dir)

    content = """-- name: migrate-0001-down
DROP TABLE users;
"""

    checksum = runner._calculate_checksum(content)

    content_without_header = """
DROP TABLE users;
"""

    checksum_without_header = runner._calculate_checksum(content_without_header)

    assert checksum == checksum_without_header


def test_checksum_includes_actual_sql_content(temp_migrations_dir: Path) -> None:
    """Test checksum includes actual SQL statements."""
    runner = SyncMigrationRunner(temp_migrations_dir)

    content1 = """-- name: migrate-20251011120000-up
CREATE TABLE users (id INTEGER);
"""

    content2 = """-- name: migrate-20251011120000-up
CREATE TABLE products (id INTEGER);
"""

    checksum1 = runner._calculate_checksum(content1)
    checksum2 = runner._calculate_checksum(content2)

    assert checksum1 != checksum2


def test_checksum_stable_after_version_conversion(temp_migrations_dir: Path) -> None:
    """Test checksum remains stable when converting timestamp to sequential."""
    runner = SyncMigrationRunner(temp_migrations_dir)

    timestamp_content = """-- name: migrate-20251011120000-up
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);

-- name: migrate-20251011120000-down
DROP TABLE users;
"""

    sequential_content = """-- name: migrate-0001-up
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);

-- name: migrate-0001-down
DROP TABLE users;
"""

    timestamp_checksum = runner._calculate_checksum(timestamp_content)
    sequential_checksum = runner._calculate_checksum(sequential_content)

    assert timestamp_checksum == sequential_checksum


def test_checksum_handles_multiple_query_headers(temp_migrations_dir: Path) -> None:
    """Test checksum excludes all migrate-* query headers in file."""
    runner = SyncMigrationRunner(temp_migrations_dir)

    content = """-- name: migrate-20251011120000-up
CREATE TABLE users (id INTEGER);

-- name: migrate-20251011120000-down
DROP TABLE users;
"""

    expected_content = """
CREATE TABLE users (id INTEGER);


DROP TABLE users;
"""

    checksum = runner._calculate_checksum(content)
    expected_checksum = runner._calculate_checksum(expected_content)

    assert checksum == expected_checksum


def test_checksum_preserves_non_migration_name_headers(temp_migrations_dir: Path) -> None:
    """Test checksum preserves non-migrate query name headers."""
    runner = SyncMigrationRunner(temp_migrations_dir)

    content1 = """-- name: get-users
SELECT * FROM users;
"""

    content2 = """
SELECT * FROM users;
"""

    checksum1 = runner._calculate_checksum(content1)
    checksum2 = runner._calculate_checksum(content2)

    assert checksum1 != checksum2


def test_checksum_handles_whitespace_variations(temp_migrations_dir: Path) -> None:
    """Test checksum handles variations in whitespace around name header."""
    runner = SyncMigrationRunner(temp_migrations_dir)

    content1 = """-- name: migrate-20251011120000-up
CREATE TABLE users (id INTEGER);
"""

    content2 = """--    name:    migrate-20251011120000-up
CREATE TABLE users (id INTEGER);
"""

    content3 = """--name:migrate-20251011120000-up
CREATE TABLE users (id INTEGER);
"""

    checksum1 = runner._calculate_checksum(content1)
    checksum2 = runner._calculate_checksum(content2)
    checksum3 = runner._calculate_checksum(content3)

    assert checksum1 == checksum2 == checksum3


def test_checksum_handles_extension_versions(temp_migrations_dir: Path) -> None:
    """Test checksum excludes extension version headers."""
    runner = SyncMigrationRunner(temp_migrations_dir)

    timestamp_content = """-- name: migrate-ext_litestar_20251011120000-up
CREATE TABLE sessions (id INTEGER);
"""

    sequential_content = """-- name: migrate-ext_litestar_0001-up
CREATE TABLE sessions (id INTEGER);
"""

    timestamp_checksum = runner._calculate_checksum(timestamp_content)
    sequential_checksum = runner._calculate_checksum(sequential_content)

    assert timestamp_checksum == sequential_checksum


def test_checksum_empty_file(temp_migrations_dir: Path) -> None:
    """Test checksum computation for empty file."""
    runner = SyncMigrationRunner(temp_migrations_dir)

    checksum = runner._calculate_checksum("")

    assert isinstance(checksum, str)
    assert len(checksum) == 32


def test_checksum_only_headers(temp_migrations_dir: Path) -> None:
    """Test checksum when file contains only query name headers."""
    runner = SyncMigrationRunner(temp_migrations_dir)

    content = """-- name: migrate-20251011120000-up
-- name: migrate-20251011120000-down
"""

    checksum = runner._calculate_checksum(content)

    empty_checksum = runner._calculate_checksum("\n")

    assert checksum == empty_checksum


def test_checksum_preserves_sql_comments(temp_migrations_dir: Path) -> None:
    """Test checksum includes regular SQL comments."""
    runner = SyncMigrationRunner(temp_migrations_dir)

    content1 = """-- name: migrate-20251011120000-up
-- This is a regular comment
CREATE TABLE users (id INTEGER);
"""

    content2 = """-- name: migrate-20251011120000-up
CREATE TABLE users (id INTEGER);
"""

    checksum1 = runner._calculate_checksum(content1)
    checksum2 = runner._calculate_checksum(content2)

    assert checksum1 != checksum2


def test_checksum_case_sensitive(temp_migrations_dir: Path) -> None:
    """Test checksum is case-sensitive for SQL content."""
    runner = SyncMigrationRunner(temp_migrations_dir)

    content1 = """-- name: migrate-20251011120000-up
CREATE TABLE users (id INTEGER);
"""

    content2 = """-- name: migrate-20251011120000-up
create table users (id integer);
"""

    checksum1 = runner._calculate_checksum(content1)
    checksum2 = runner._calculate_checksum(content2)

    assert checksum1 != checksum2


def test_checksum_detects_content_changes(temp_migrations_dir: Path) -> None:
    """Test checksum changes when SQL content is modified."""
    runner = SyncMigrationRunner(temp_migrations_dir)

    original = """-- name: migrate-20251011120000-up
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);
"""

    modified = """-- name: migrate-20251011120000-up
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT
);
"""

    original_checksum = runner._calculate_checksum(original)
    modified_checksum = runner._calculate_checksum(modified)

    assert original_checksum != modified_checksum


def test_checksum_regex_pattern_matches_correctly(temp_migrations_dir: Path) -> None:
    """Test regex pattern only matches actual query name headers."""
    runner = SyncMigrationRunner(temp_migrations_dir)

    content_with_similar_text = """-- name: migrate-20251011120000-up
-- This comment mentions migrate-20251011120000-up but shouldn't be removed
SELECT 'migrate-20251011120000-up' as text;
CREATE TABLE users (id INTEGER);
"""

    content_header_only_removed = """
-- This comment mentions migrate-20251011120000-up but shouldn't be removed
SELECT 'migrate-20251011120000-up' as text;
CREATE TABLE users (id INTEGER);
"""

    checksum = runner._calculate_checksum(content_with_similar_text)
    expected_checksum = runner._calculate_checksum(content_header_only_removed)

    assert checksum == expected_checksum
