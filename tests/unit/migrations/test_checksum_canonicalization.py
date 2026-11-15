"""Unit tests for canonicalized checksum computation."""

import hashlib
import re
from pathlib import Path

import pytest

from sqlspec.migrations.runner import BaseMigrationRunner, SyncMigrationRunner


@pytest.fixture
def temp_migrations_dir(tmp_path: Path) -> Path:
    """Create temporary migrations directory."""
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    return migrations_dir


def test_checksum_ignores_timestamp_version_in_up_header(temp_migrations_dir: Path) -> None:
    """Test the up header digest ignores a timestamp version number."""
    runner = SyncMigrationRunner(temp_migrations_dir)

    first = """-- name: migrate-20251011120000-up
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);
"""
    second = """-- name: migrate-20260101000000-up
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);
"""

    assert runner.calculate_checksum(first) == runner.calculate_checksum(second)


def test_checksum_ignores_timestamp_version_in_down_header(temp_migrations_dir: Path) -> None:
    """Test the down header digest ignores a timestamp version number."""
    runner = SyncMigrationRunner(temp_migrations_dir)

    first = """-- name: migrate-20251011120000-down
DROP TABLE users;
"""
    second = """-- name: migrate-20260101000000-down
DROP TABLE users;
"""

    assert runner.calculate_checksum(first) == runner.calculate_checksum(second)


def test_checksum_ignores_sequential_version_in_up_header(temp_migrations_dir: Path) -> None:
    """Test the up header digest ignores a sequential version number."""
    runner = SyncMigrationRunner(temp_migrations_dir)

    first = """-- name: migrate-0001-up
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);
"""
    second = """-- name: migrate-0002-up
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);
"""

    assert runner.calculate_checksum(first) == runner.calculate_checksum(second)


def test_checksum_ignores_sequential_version_in_down_header(temp_migrations_dir: Path) -> None:
    """Test the down header digest ignores a sequential version number."""
    runner = SyncMigrationRunner(temp_migrations_dir)

    first = """-- name: migrate-0001-down
DROP TABLE users;
"""
    second = """-- name: migrate-0002-down
DROP TABLE users;
"""

    assert runner.calculate_checksum(first) == runner.calculate_checksum(second)


def test_checksum_includes_actual_sql_content(temp_migrations_dir: Path) -> None:
    """Test checksum includes actual SQL statements."""
    runner = SyncMigrationRunner(temp_migrations_dir)

    content1 = """-- name: migrate-20251011120000-up
CREATE TABLE users (id INTEGER);
"""

    content2 = """-- name: migrate-20251011120000-up
CREATE TABLE products (id INTEGER);
"""

    checksum1 = runner.calculate_checksum(content1)
    checksum2 = runner.calculate_checksum(content2)

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

    timestamp_checksum = runner.calculate_checksum(timestamp_content)
    sequential_checksum = runner.calculate_checksum(sequential_content)

    assert timestamp_checksum == sequential_checksum


def test_checksum_handles_multiple_query_headers(temp_migrations_dir: Path) -> None:
    """Test every migrate-* header in a file is normalized independently."""
    runner = SyncMigrationRunner(temp_migrations_dir)

    first = """-- name: migrate-20251011120000-up
CREATE TABLE users (id INTEGER);

-- name: migrate-20251011120000-down
DROP TABLE users;
"""
    second = """-- name: migrate-0007-up
CREATE TABLE users (id INTEGER);

-- name: migrate-0007-down
DROP TABLE users;
"""

    assert runner.calculate_checksum(first) == runner.calculate_checksum(second)


def test_checksum_preserves_non_migration_name_headers(temp_migrations_dir: Path) -> None:
    """Test checksum preserves non-migrate query name headers."""
    runner = SyncMigrationRunner(temp_migrations_dir)

    content1 = """-- name: get-users
SELECT * FROM users;
"""

    content2 = """
SELECT * FROM users;
"""

    checksum1 = runner.calculate_checksum(content1)
    checksum2 = runner.calculate_checksum(content2)

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

    checksum1 = runner.calculate_checksum(content1)
    checksum2 = runner.calculate_checksum(content2)
    checksum3 = runner.calculate_checksum(content3)

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

    timestamp_checksum = runner.calculate_checksum(timestamp_content)
    sequential_checksum = runner.calculate_checksum(sequential_content)

    assert timestamp_checksum == sequential_checksum


def test_checksum_empty_file(temp_migrations_dir: Path) -> None:
    """Test checksum computation for empty file."""
    runner = SyncMigrationRunner(temp_migrations_dir)

    checksum = runner.calculate_checksum("")

    assert isinstance(checksum, str)
    assert len(checksum) == 64


def test_checksum_only_headers(temp_migrations_dir: Path) -> None:
    """Test a file with only headers digests the same across version numbers."""
    runner = SyncMigrationRunner(temp_migrations_dir)

    first = """-- name: migrate-20251011120000-up
-- name: migrate-20251011120000-down
"""
    second = """-- name: migrate-0001-up
-- name: migrate-0001-down
"""

    assert runner.calculate_checksum(first) == runner.calculate_checksum(second)


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

    checksum1 = runner.calculate_checksum(content1)
    checksum2 = runner.calculate_checksum(content2)

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

    checksum1 = runner.calculate_checksum(content1)
    checksum2 = runner.calculate_checksum(content2)

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

    original_checksum = runner.calculate_checksum(original)
    modified_checksum = runner.calculate_checksum(modified)

    assert original_checksum != modified_checksum


def test_checksum_regex_pattern_matches_correctly(temp_migrations_dir: Path) -> None:
    """Test only the real header is normalized, not text that mentions one."""
    runner = SyncMigrationRunner(temp_migrations_dir)

    first = """-- name: migrate-20251011120000-up
-- mentions migrate-20251011120000-up here
SELECT 'migrate-20251011120000-up' as text;
"""
    second = """-- name: migrate-0003-up
-- mentions migrate-20251011120000-up here
SELECT 'migrate-20251011120000-up' as text;
"""

    assert runner.calculate_checksum(first) == runner.calculate_checksum(second)


@pytest.mark.parametrize(
    ("digest_mode", "content_variant", "uppercase", "expected"),
    [
        ("sha256", "same", False, True),
        ("sha256", "same", True, True),
        ("sha256", "renamed", False, True),
        ("sha256", "modified", False, False),
        ("md5", "same", False, True),
        ("md5", "same", True, True),
        ("md5", "renamed", False, True),
        ("md5", "modified", False, False),
        ("empty", "same", False, False),
        ("manual-stamp", "same", False, False),
        ("non-ascii", "same", False, False),
    ],
)
def test_verify_migration_checksum_supports_sha256_and_legacy_md5(
    tmp_path: Path, digest_mode: str, content_variant: str, uppercase: bool, expected: bool
) -> None:
    """Verify BaseMigrationRunner.verify_migration_checksum handles SHA-256, legacy MD5, and mismatches."""
    original_content = (
        "-- Version: 20251011120000\n-- name: migrate-20251011120000-up\nCREATE TABLE items (id INTEGER PRIMARY KEY);\n"
    )
    renamed_content = "-- Version: 0001\n-- name: migrate-0001-up\nCREATE TABLE items (id INTEGER PRIMARY KEY);\n"
    modified_content = (
        "-- Version: 0001\n-- name: migrate-0001-up\nCREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT);\n"
    )

    runner = SyncMigrationRunner(tmp_path)
    canonical_original = BaseMigrationRunner._canonicalize_migration_content(original_content)  # pyright: ignore[reportPrivateUsage]
    if digest_mode == "sha256":
        recorded = runner.calculate_checksum(original_content)
        assert len(recorded) == 64
    elif digest_mode == "md5":
        recorded = hashlib.md5(canonical_original.encode("utf-8"), usedforsecurity=False).hexdigest()
        assert len(recorded) == 32
    elif digest_mode == "non-ascii":
        recorded = "foo\xe9"
    else:
        recorded = "" if digest_mode == "empty" else digest_mode

    if uppercase:
        recorded = recorded.upper()

    candidate_content = {"same": original_content, "renamed": renamed_content, "modified": modified_content}[
        content_variant
    ]

    assert BaseMigrationRunner.verify_migration_checksum(candidate_content, recorded) is expected


def test_canonicalization_keeps_injected_down_block(temp_migrations_dir: Path) -> None:
    runner = SyncMigrationRunner(temp_migrations_dir)
    original = "-- name: migrate-0002-up\nCREATE TABLE orders(id int);\n"
    tampered = (
        original
        + "-- name: migrate-0002_down\nDROP TABLE orders;\nDROP TABLE users;\n"
        + "CREATE TABLE tampered AS SELECT 1 AS up;\nSELECT 1 FROM tampered WHERE 0\n-up\n"
    )

    assert runner.calculate_checksum(original) != runner.calculate_checksum(tampered)


def test_canonicalization_keeps_python_version_assignment(temp_migrations_dir: Path) -> None:
    runner = SyncMigrationRunner(temp_migrations_dir)
    original = '"""Users."""\nimport os\n\ndef up(context=None):\n    return ["CREATE TABLE users(id int)"]\n'
    tampered = original.replace("\n\ndef", '\nversion: __import__("os").getcwd()\ndef', 1)

    assert runner.calculate_checksum(original) != runner.calculate_checksum(tampered)


def test_canonicalization_header_never_crosses_lines(temp_migrations_dir: Path) -> None:
    runner = SyncMigrationRunner(temp_migrations_dir)

    canonical = runner._canonicalize_migration_content("-- Version:\nDROP TABLE x;\n")  # pyright: ignore[reportPrivateUsage]

    assert "DROP TABLE x;" in canonical


def test_moving_down_header_changes_digest(temp_migrations_dir: Path) -> None:
    runner = SyncMigrationRunner(temp_migrations_dir)
    original = (
        "-- name: migrate-0001-up\nCREATE TABLE a (id INT);\nCREATE TABLE b (id INT);\n"
        "-- name: migrate-0001-down\nDROP TABLE a;\n"
    )
    moved = (
        "-- name: migrate-0001-up\nCREATE TABLE a (id INT);\n"
        "-- name: migrate-0001-down\nCREATE TABLE b (id INT);\nDROP TABLE a;\n"
    )

    assert runner.calculate_checksum(original) != runner.calculate_checksum(moved)


@pytest.mark.parametrize(
    "edited_header",
    ["-- name: migrate-0001 x-down", "-- name: migrate-0009-down", "-- name: migrate-00-01-down"],
    ids=["space", "other-version", "extra-dash"],
)
def test_editing_down_header_changes_digest(temp_migrations_dir: Path, edited_header: str) -> None:
    runner = SyncMigrationRunner(temp_migrations_dir)
    original = "-- name: migrate-0001-up\nCREATE TABLE a (id INT);\n-- name: migrate-0001-down\nDROP TABLE a;\n"
    edited = original.replace("-- name: migrate-0001-down", edited_header)

    assert runner.calculate_checksum(original) != runner.calculate_checksum(edited)


def test_header_with_files_own_version_still_canonicalized(temp_migrations_dir: Path) -> None:
    runner = SyncMigrationRunner(temp_migrations_dir)
    first = "-- name: migrate-20251011120000-up\nSELECT 1;\n-- name: migrate-20251011120000-down\nSELECT 2;\n"
    second = "-- name: migrate-0001-up\nSELECT 1;\n-- name: migrate-0001-down\nSELECT 2;\n"

    assert runner.calculate_checksum(first) == runner.calculate_checksum(second)


def test_marker_digest_ignores_version_numbers(temp_migrations_dir: Path) -> None:
    runner = SyncMigrationRunner(temp_migrations_dir)

    assert runner.calculate_checksum("-- name: migrate-20251011120000-up\nSELECT 1;\n") == runner.calculate_checksum(
        "-- name: migrate-0001-up\nSELECT 1;\n"
    )
    assert runner.calculate_checksum("-- Version: 20251011120000\nSELECT 1;\n") == runner.calculate_checksum(
        "-- Version: 0001\nSELECT 1;\n"
    )


LEGACY_SQL = "-- Version: 0001\n-- name: migrate-0001-up\n\nCREATE TABLE a (id INT);\n-- name: migrate-0001-down\nDROP TABLE a;\n"
LEGACY_PY = '"""Migration.\n\nVersion: 0001\n"""\n\n\ndef up(context=None):\n    return ["CREATE TABLE a (id INT)"]\n'


@pytest.mark.parametrize(("content", "form"), [(LEGACY_SQL, "legacy"), (LEGACY_PY, "marker")], ids=["sql", "py"])
@pytest.mark.parametrize("digest", ["sha256", "md5"])
def test_legacy_sha256_and_md5_rows_still_verify(
    temp_migrations_dir: Path, content: str, form: str, digest: str
) -> None:
    old_pattern = r"^(?:--\s*name:\s*migrate-[^-]+-(?:up|down)|--\s*Version:\s*.*|Version:\s*.*)\s*$"
    legacy_text = re.sub(old_pattern, "", content, flags=re.MULTILINE | re.IGNORECASE)
    if digest == "sha256":
        recorded = hashlib.sha256(legacy_text.encode("utf-8")).hexdigest()
    else:
        recorded = hashlib.md5(legacy_text.encode("utf-8"), usedforsecurity=False).hexdigest()

    assert BaseMigrationRunner.verify_migration_checksum(content, recorded) is True
    assert BaseMigrationRunner.match_migration_checksum(content, recorded) == form
