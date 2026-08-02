"""Regression tests for ``migration_config`` key validation.

``migration_config`` is a ``TypedDict``, so a misspelled key was accepted at
runtime and silently ignored, leaving the corresponding setting at its default.
These tests pin the validate-and-raise behavior and the suggestion text.
"""

import pytest

from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.config import MIGRATION_CONFIG_KEYS, MigrationConfig, validate_migration_config_keys
from sqlspec.exceptions import ImproperConfigurationError


def test_known_keys_cover_every_typed_dict_field() -> None:
    """Every declared MigrationConfig field is accepted by the validator."""
    assert MIGRATION_CONFIG_KEYS == MigrationConfig.__required_keys__ | MigrationConfig.__optional_keys__
    validate_migration_config_keys(dict.fromkeys(MIGRATION_CONFIG_KEYS, None))


def test_author_key_is_declared() -> None:
    """The author key is read when generating migrations, so it must be declared."""
    assert "author" in MIGRATION_CONFIG_KEYS


def test_unknown_key_raises_with_suggestion() -> None:
    """A near-miss key reports the intended field name."""
    with pytest.raises(ImproperConfigurationError) as exc_info:
        validate_migration_config_keys({"version_table": "_schema_versions"})

    message = str(exc_info.value)
    assert "Unknown migration_config key 'version_table'" in message
    assert "Did you mean 'version_table_name'?" in message


def test_unrecognizable_key_lists_valid_keys_without_a_suggestion() -> None:
    """A key with no close match still reports the accepted set."""
    with pytest.raises(ImproperConfigurationError) as exc_info:
        validate_migration_config_keys({"totally_unrelated": 1})

    message = str(exc_info.value)
    assert "Did you mean" not in message
    assert "script_location" in message


def test_every_unknown_key_is_reported() -> None:
    """Reporting covers all offending keys, not just the first."""
    with pytest.raises(ImproperConfigurationError) as exc_info:
        validate_migration_config_keys({"version_table": "a", "scriptlocation": "b"})

    message = str(exc_info.value)
    assert "'version_table'" in message
    assert "'scriptlocation'" in message


def test_config_construction_rejects_unknown_key() -> None:
    """The reporter's original config fails at construction instead of silently ignoring the key."""
    with pytest.raises(ImproperConfigurationError, match="version_table_name"):
        SqliteConfig(
            connection_config={"database": ":memory:"},
            migration_config={"script_location": "migrations", "version_table": "_schema_versions"},
        )


def test_post_construction_assignment_is_validated() -> None:
    """Assigning migration_config after construction runs the same check."""
    config = SqliteConfig(connection_config={"database": ":memory:"})

    with pytest.raises(ImproperConfigurationError, match="version_table_name"):
        config.set_migration_config({"version_table": "_schema_versions"})


def test_valid_config_is_unchanged() -> None:
    """A correctly spelled configuration is stored as provided."""
    config = SqliteConfig(
        connection_config={"database": ":memory:"},
        migration_config={"script_location": "migrations", "version_table_name": "_schema_versions"},
    )

    assert config.migration_config["version_table_name"] == "_schema_versions"
