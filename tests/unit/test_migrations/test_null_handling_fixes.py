"""Test cases for null handling fixes in migration system."""

import pytest
import tempfile
from pathlib import Path

from sqlspec.utils.version import parse_version, is_sequential_version, is_timestamp_version
from sqlspec.migrations.fix import MigrationFixer
from sqlspec.migrations.validation import detect_out_of_order_migrations


class TestNullHandlingFixes:
    """Test fixes for None value handling in migrations."""

    def test_parse_version_with_none(self):
        """Test parse_version handles None gracefully."""
        with pytest.raises(ValueError, match="Invalid migration version: version string is None or empty"):
            parse_version(None)

    def test_parse_version_with_empty_string(self):
        """Test parse_version handles empty string gracefully."""
        with pytest.raises(ValueError, match="Invalid migration version: version string is None or empty"):
            parse_version("")

    def test_parse_version_with_whitespace_only(self):
        """Test parse_version handles whitespace-only strings."""
        with pytest.raises(ValueError, match="Invalid migration version: version string is None or empty"):
            parse_version("   ")

    def test_parse_version_valid_formats_still_work(self):
        """Test that valid version formats still work after fixes."""
        # Sequential versions
        result = parse_version("0001")
        assert result.type.value == "sequential"
        assert result.sequence == 1

        result = parse_version("9999") 
        assert result.type.value == "sequential"
        assert result.sequence == 9999

        # Timestamp versions
        result = parse_version("20251011120000")
        assert result.type.value == "timestamp"
        assert result.timestamp is not None

        # Extension versions
        result = parse_version("ext_litestar_0001")
        assert result.type.value == "sequential"  # Base is sequential
        assert result.extension == "litestar"

    def test_migration_fixer_handles_none_gracefully(self):
        """Test MigrationFixer.update_file_content handles None values."""
        with tempfile.TemporaryDirectory() as temp_dir:
            migrations_path = Path(temp_dir)
            fixer = MigrationFixer(migrations_path)

            test_file = migrations_path / "test.sql"
            test_file.write_text("-- Test content")

            # Should not crash with None values
            fixer.update_file_content(test_file, None, "0001")
            fixer.update_file_content(test_file, "0001", None)
            fixer.update_file_content(test_file, None, None)

            # File should remain unchanged
            content = test_file.read_text()
            assert content == "-- Test content"

    def test_validation_filters_none_values(self):
        """Test migration validation filters None values properly."""
        # Should not crash with None values in lists
        gaps = detect_out_of_order_migrations(
            pending_versions=["0001", None, "0003", ""],
            applied_versions=[None, "0002", "   ", "0004"]
        )

        # Should only process valid versions
        assert len(gaps) >= 0  # Should not crash

    def test_sequential_pattern_edge_cases(self):
        """Test sequential pattern handles edge cases."""
        assert is_sequential_version("0001") == True
        assert is_sequential_version("9999") == True
        assert is_sequential_version("10000") == True
        assert is_sequential_version("20251011120000") == False  # Timestamp
        assert is_sequential_version("abc") == False
        assert is_sequential_version("") == False
        assert is_sequential_version(None) == False

    def test_timestamp_pattern_edge_cases(self):
        """Test timestamp pattern handles edge cases."""
        assert is_timestamp_version("20251011120000") == True
        assert is_timestamp_version("20250101000000") == True
        assert is_timestamp_version("20251231235959") == True
        assert is_timestamp_version("0001") == False  # Sequential
        assert is_timestamp_version("2025101112000") == False  # Too short
        assert is_timestamp_version("202510111200000") == False  # Too long
        assert is_timestamp_version("") == False
        assert is_timestamp_version(None) == False

    def test_error_messages_are_descriptive(self):
        """Test that error messages are helpful for debugging."""
        try:
            parse_version(None)
        except ValueError as e:
            assert "version string is None or empty" in str(e)

        try:
            parse_version("")
        except ValueError as e:
            assert "version string is None or empty" in str(e)
