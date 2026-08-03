"""Regression tests for ``migration_config`` key validation.

``migration_config`` is a ``TypedDict``, so a misspelled key was accepted at
runtime and silently ignored, leaving the corresponding setting at its default.
These tests pin the validate-and-raise behavior and the suggestion text.
"""

from typing import TYPE_CHECKING

import pytest

from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.config import MIGRATION_CONFIG_KEYS, MigrationConfig, validate_migration_config_keys
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.migrations.utils import create_migration_file

if TYPE_CHECKING:
    from pathlib import Path


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


@pytest.mark.parametrize(
    ("key", "value"),
    [("templates", {"sql": {"header": "-- {title} [ACME]"}}), ("title", "Acme Migration"), ("default_format", "py")],
)
def test_template_keys_are_accepted(key: str, value: object) -> None:
    """Keys read by build_template_settings must survive validation."""
    config = SqliteConfig(connection_config={"database": ":memory:"}, migration_config={key: value})

    assert config.migration_config[key] == value


def test_template_keys_are_declared() -> None:
    """The template settings reader consumes these keys, so they must be declared."""
    assert {"templates", "title", "default_format"} <= MIGRATION_CONFIG_KEYS


def test_unknown_template_section_key_is_reported() -> None:
    """A typo directly under templates names the dotted path and the intended key."""
    with pytest.raises(ImproperConfigurationError) as exc_info:
        validate_migration_config_keys({"templates": {"sqll": {}}})

    message = str(exc_info.value)
    assert "Unknown migration_config key 'templates.sqll'" in message
    assert "Did you mean 'sql'?" in message


@pytest.mark.parametrize(
    ("section", "bad_key", "suggestion"),
    [("sql", "headers", "header"), ("sql", "bodyy", "body"), ("py", "docstrings", "docstring")],
)
def test_unknown_template_fragment_key_is_reported(section: str, bad_key: str, suggestion: str) -> None:
    """A typo inside a template fragment is caught at construction, not at render time."""
    with pytest.raises(ImproperConfigurationError) as exc_info:
        validate_migration_config_keys({"templates": {section: {bad_key: "x"}}})

    message = str(exc_info.value)
    assert f"Unknown migration_config key 'templates.{section}.{bad_key}'" in message
    assert f"Did you mean {suggestion!r}?" in message


def test_description_key_is_the_accepted_fragment_spelling() -> None:
    """The singular description_key is declared; the resolved plural form is not a config key."""
    validate_migration_config_keys({"templates": {"sql": {"description_key": "Summary"}}})

    with pytest.raises(ImproperConfigurationError, match="Did you mean 'description_key'"):
        validate_migration_config_keys({"templates": {"sql": {"description_keys": "Summary"}}})


@pytest.mark.parametrize("path", ["templates", "templates.sql"])
def test_non_mapping_template_value_reports_the_type(path: str) -> None:
    """A non-mapping override raises a clear message instead of a TypeError at render time."""
    payload: dict[str, object] = (
        {"templates": ["not", "a", "mapping"]} if path == "templates" else {"templates": {"sql": "not a mapping"}}
    )

    with pytest.raises(ImproperConfigurationError) as exc_info:
        validate_migration_config_keys(payload)

    assert f"'{path}' must be a mapping" in str(exc_info.value)


def test_nested_typo_fails_at_config_construction() -> None:
    """Nested validation runs through the real config setter."""
    with pytest.raises(ImproperConfigurationError, match=r"templates\.sql\.headerr"):
        SqliteConfig(
            connection_config={"database": ":memory:"}, migration_config={"templates": {"sql": {"headerr": "-- x"}}}
        )


def test_valid_nested_template_config_is_accepted() -> None:
    """A fully populated template override passes validation."""
    validate_migration_config_keys({
        "title": "Acme",
        "default_format": "py",
        "templates": {
            "title": "Acme Fallback",
            "sql": {"header": "-- {title}", "metadata": ["-- {author}"], "body": "", "description_key": "Desc"},
            "py": {"docstring": "{title}", "body": "", "imports": [], "description_key": ["Desc"]},
        },
    })


def test_template_overrides_reach_the_rendered_migration(tmp_path: "Path") -> None:
    """A customized template configured on a real config renders through to disk."""
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    config = SqliteConfig(
        connection_config={"database": ":memory:"},
        migration_config={
            "author": "Acme Ops",
            "title": "Acme Migration",
            "default_format": "py",
            "templates": {"sql": {"header": "-- {title} [ACME]", "metadata": ["-- Owner: {author}"]}},
        },
    )

    sql_path = create_migration_file(migrations_dir, "0001", "custom", "sql", config=config)
    default_path = create_migration_file(migrations_dir, "0002", "defaulted", None, config=config)

    content = sql_path.read_text()
    assert "-- Acme Migration [ACME]" in content
    assert "-- Owner: Acme Ops" in content
    assert default_path.suffix == ".py"
