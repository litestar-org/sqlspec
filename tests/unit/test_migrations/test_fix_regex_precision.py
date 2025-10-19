"""Unit tests for version-specific regex patterns in fix operations."""

import re
from pathlib import Path

import pytest

from sqlspec.migrations.fix import MigrationFixer


@pytest.fixture
def temp_migrations_dir(tmp_path: Path) -> Path:
    """Create temporary migrations directory."""
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    return migrations_dir


def test_update_file_content_only_replaces_specific_version(temp_migrations_dir: Path) -> None:
    """Test only specific old_version is replaced, not other versions."""
    fixer = MigrationFixer(temp_migrations_dir)

    file_path = temp_migrations_dir / "20251011120000_test.sql"
    content = """-- name: migrate-20251011120000-up
CREATE TABLE users (id INTEGER);

-- name: migrate-20251011120000-down
DROP TABLE users;

-- This comment mentions migrate-20251012130000-up
-- And also migrate-0001-up
"""

    file_path.write_text(content)

    fixer.update_file_content(file_path, "20251011120000", "0001")

    updated = file_path.read_text()

    assert "-- name: migrate-0001-up" in updated
    assert "-- name: migrate-0001-down" in updated
    assert "migrate-20251012130000-up" in updated
    assert "And also migrate-0001-up" in updated


def test_update_file_content_handles_special_regex_characters(temp_migrations_dir: Path) -> None:
    """Test version strings with special regex characters are escaped."""
    fixer = MigrationFixer(temp_migrations_dir)

    file_path = temp_migrations_dir / "test.sql"

    version_with_dots = "1.2.3"
    content = f"""-- name: migrate-{version_with_dots}-up
CREATE TABLE test (id INTEGER);
"""

    file_path.write_text(content)

    fixer.update_file_content(file_path, version_with_dots, "0001")

    updated = file_path.read_text()
    assert "-- name: migrate-0001-up" in updated


def test_update_file_content_does_not_replace_unrelated_migrate_patterns(temp_migrations_dir: Path) -> None:
    """Test unrelated migrate-* patterns are not replaced."""
    fixer = MigrationFixer(temp_migrations_dir)

    file_path = temp_migrations_dir / "20251011120000_test.sql"
    content = """-- name: migrate-20251011120000-up
CREATE TABLE users (id INTEGER);

-- name: migrate-other-pattern-up
-- This should not be touched

-- name: migrate-20251011120000-down
DROP TABLE users;
"""

    file_path.write_text(content)

    fixer.update_file_content(file_path, "20251011120000", "0001")

    updated = file_path.read_text()

    assert "-- name: migrate-0001-up" in updated
    assert "-- name: migrate-0001-down" in updated
    assert "-- name: migrate-other-pattern-up" in updated


def test_update_file_content_extension_version_pattern(temp_migrations_dir: Path) -> None:
    """Test extension version patterns are handled correctly."""
    fixer = MigrationFixer(temp_migrations_dir)

    file_path = temp_migrations_dir / "ext_litestar_20251011120000_test.py"
    content = '''"""Migration file."""
# This references migrate-ext_litestar_20251011120000-up in a comment
'''

    file_path.write_text(content)

    fixer.update_file_content(file_path, "ext_litestar_20251011120000", "ext_litestar_0001")

    updated = file_path.read_text()

    assert content == updated


def test_regex_pattern_matches_exact_version_only(temp_migrations_dir: Path) -> None:
    """Test regex pattern construction matches exact version only."""
    MigrationFixer(temp_migrations_dir)

    old_version = "20251011120000"
    up_pattern = re.compile(rf"(-- name:\s+migrate-){re.escape(old_version)}(-up)")

    test_cases = [
        ("-- name: migrate-20251011120000-up", True),
        ("-- name:  migrate-20251011120000-up", True),
        ("-- name:migrate-20251011120000-up", False),
        ("-- name: migrate-20251011120000-down", False),
        ("-- name: migrate-2025101112000-up", False),
        ("-- name: migrate-202510111200000-up", False),
        ("-- name: migrate-20251011120001-up", False),
        ("-- name: migrate-0001-up", False),
        ("migrate-20251011120000-up", False),
    ]

    for text, should_match in test_cases:
        match = up_pattern.search(text)
        if should_match:
            assert match is not None, f"Expected match for: {text}"
        else:
            assert match is None, f"Should not match: {text}"


def test_regex_pattern_handles_down_direction(temp_migrations_dir: Path) -> None:
    """Test regex pattern correctly handles down direction."""
    old_version = "20251011120000"
    down_pattern = re.compile(rf"(-- name:\s+migrate-){re.escape(old_version)}(-down)")

    test_cases = [
        ("-- name: migrate-20251011120000-down", True),
        ("-- name:  migrate-20251011120000-down", True),
        ("-- name:migrate-20251011120000-down", False),
        ("-- name: migrate-20251011120000-up", False),
        ("-- name: migrate-20251011120001-down", False),
    ]

    for text, should_match in test_cases:
        match = down_pattern.search(text)
        if should_match:
            assert match is not None, f"Expected match for: {text}"
        else:
            assert match is None, f"Should not match: {text}"


def test_update_file_content_multiple_versions_in_file(temp_migrations_dir: Path) -> None:
    """Test file with multiple different version references only updates target."""
    fixer = MigrationFixer(temp_migrations_dir)

    file_path = temp_migrations_dir / "20251011120000_test.sql"
    content = """-- name: migrate-20251011120000-up
-- Migration depends on migrate-20251010110000-up being applied first
CREATE TABLE users (id INTEGER);

-- name: migrate-20251011120000-down
-- Reverses the changes
DROP TABLE users;
"""

    file_path.write_text(content)

    fixer.update_file_content(file_path, "20251011120000", "0002")

    updated = file_path.read_text()

    assert "-- name: migrate-0002-up" in updated
    assert "-- name: migrate-0002-down" in updated
    assert "migrate-20251010110000-up" in updated
    assert "Reverses the changes" in updated


def test_regex_escape_prevents_version_injection(temp_migrations_dir: Path) -> None:
    """Test re.escape prevents regex injection in version strings."""
    malicious_version = "20251011.*"

    escaped = re.escape(malicious_version)

    pattern = re.compile(rf"(-- name:\s+migrate-){escaped}(-up)")

    should_not_match = [
        "-- name: migrate-20251011120000-up",
        "-- name: migrate-20251011999999-up",
        "-- name: migrate-20251011-up",
    ]

    for text in should_not_match:
        assert pattern.search(text) is None

    should_match = "-- name: migrate-20251011.*-up"
    assert pattern.search(should_match) is not None


def test_update_file_content_preserves_line_endings(temp_migrations_dir: Path) -> None:
    """Test file content update preserves line endings."""
    fixer = MigrationFixer(temp_migrations_dir)

    file_path = temp_migrations_dir / "20251011120000_test.sql"
    content = "-- name: migrate-20251011120000-up\nCREATE TABLE users (id INTEGER);\n"

    file_path.write_text(content)

    fixer.update_file_content(file_path, "20251011120000", "0001")

    updated = file_path.read_text()

    assert "\n" in updated
    assert updated.endswith("\n")


def test_update_file_content_handles_no_matches(temp_migrations_dir: Path) -> None:
    """Test update when version does not appear in file."""
    fixer = MigrationFixer(temp_migrations_dir)

    file_path = temp_migrations_dir / "20251011120000_test.sql"
    original_content = """-- name: migrate-20251012130000-up
CREATE TABLE products (id INTEGER);
"""

    file_path.write_text(original_content)

    fixer.update_file_content(file_path, "20251011120000", "0001")

    updated = file_path.read_text()

    assert updated == original_content


def test_update_file_content_complex_sql_not_affected(temp_migrations_dir: Path) -> None:
    """Test complex SQL content is not affected by version replacement."""
    fixer = MigrationFixer(temp_migrations_dir)

    file_path = temp_migrations_dir / "20251011120000_test.sql"
    content = """-- name: migrate-20251011120000-up
CREATE TABLE logs (
    id INTEGER PRIMARY KEY,
    message TEXT CHECK (message != 'migrate-20251011120000-up'),
    pattern VARCHAR(100) DEFAULT '-- name: migrate-pattern-up'
);

INSERT INTO logs (message) VALUES ('Testing migrate-20251011120000-up reference');

-- name: migrate-20251011120000-down
DROP TABLE logs;
"""

    file_path.write_text(content)

    fixer.update_file_content(file_path, "20251011120000", "0001")

    updated = file_path.read_text()

    assert "-- name: migrate-0001-up" in updated
    assert "-- name: migrate-0001-down" in updated
    assert "message != 'migrate-20251011120000-up'" in updated
    assert "pattern VARCHAR(100) DEFAULT '-- name: migrate-pattern-up'" in updated
    assert "VALUES ('Testing migrate-20251011120000-up reference')" in updated


def test_update_file_content_timestamp_vs_sequential_collision(temp_migrations_dir: Path) -> None:
    """Test version replacement doesn't confuse timestamp and sequential formats."""
    fixer = MigrationFixer(temp_migrations_dir)

    file_path = temp_migrations_dir / "20251011120000_test.sql"
    content = """-- name: migrate-20251011120000-up
-- This migration comes after migrate-0001-up
CREATE TABLE users (id INTEGER);

-- name: migrate-20251011120000-down
DROP TABLE users;
"""

    file_path.write_text(content)

    fixer.update_file_content(file_path, "20251011120000", "0002")

    updated = file_path.read_text()

    assert "-- name: migrate-0002-up" in updated
    assert "-- name: migrate-0002-down" in updated
    assert "after migrate-0001-up" in updated


def test_regex_pattern_boundary_conditions(temp_migrations_dir: Path) -> None:
    """Test regex pattern handles boundary conditions correctly."""
    test_cases = [
        ("0001", "0002"),
        ("9999", "10000"),
        ("20251011120000", "0001"),
        ("ext_litestar_0001", "ext_litestar_0002"),
        ("ext_adk_20251011120000", "ext_adk_0001"),
    ]

    for old_version, new_version in test_cases:
        up_pattern = re.compile(rf"(-- name:\s+migrate-){re.escape(old_version)}(-up)")
        down_pattern = re.compile(rf"(-- name:\s+migrate-){re.escape(old_version)}(-down)")

        test_line_up = f"-- name: migrate-{old_version}-up"
        test_line_down = f"-- name: migrate-{old_version}-down"

        assert up_pattern.search(test_line_up) is not None
        assert down_pattern.search(test_line_down) is not None

        replaced_up = up_pattern.sub(rf"\g<1>{new_version}\g<2>", test_line_up)
        replaced_down = down_pattern.sub(rf"\g<1>{new_version}\g<2>", test_line_down)

        assert replaced_up == f"-- name: migrate-{new_version}-up"
        assert replaced_down == f"-- name: migrate-{new_version}-down"


def test_update_file_content_updates_version_comment(temp_migrations_dir: Path) -> None:
    """Test version comment in migration header is updated."""
    fixer = MigrationFixer(temp_migrations_dir)

    file_path = temp_migrations_dir / "20251019204218_create_products.sql"
    content = """-- SQLSpec Migration
-- Version: 20251019204218
-- Description: create products table
-- Created: 2025-10-19T20:42:18.123456

-- name: migrate-20251019204218-up
CREATE TABLE products (id INTEGER PRIMARY KEY);

-- name: migrate-20251019204218-down
DROP TABLE products;
"""

    file_path.write_text(content)

    fixer.update_file_content(file_path, "20251019204218", "0001")

    updated = file_path.read_text()

    assert "-- Version: 0001" in updated
    assert "-- Version: 20251019204218" not in updated
    assert "-- name: migrate-0001-up" in updated
    assert "-- name: migrate-0001-down" in updated
    assert "-- Description: create products table" in updated


def test_update_file_content_preserves_version_comment_format(temp_migrations_dir: Path) -> None:
    """Test version comment format is preserved during update."""
    fixer = MigrationFixer(temp_migrations_dir)

    file_path = temp_migrations_dir / "0001_initial.sql"
    content = """-- SQLSpec Migration
-- Version:  0001
-- Another comment
"""

    file_path.write_text(content)

    fixer.update_file_content(file_path, "0001", "0002")

    updated = file_path.read_text()

    assert "-- Version:  0002" in updated
    assert "-- Another comment" in updated
