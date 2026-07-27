"""Tests for resolving extension ``migrations_path`` settings."""

from pathlib import Path

import pytest

from sqlspec.exceptions import MigrationError
from sqlspec.migrations.utils import resolve_extension_migrations_path


def test_resolves_path_object(tmp_path: Path) -> None:
    """A Path pointing at an existing directory resolves to itself."""
    assert resolve_extension_migrations_path("vendor", tmp_path) == tmp_path


def test_resolves_string_path(tmp_path: Path) -> None:
    """A string filesystem path resolves to the matching directory."""
    assert resolve_extension_migrations_path("vendor", str(tmp_path)) == tmp_path


def test_resolves_module_specification() -> None:
    """A '<dotted.module>:<subdir>' value resolves against the installed package."""
    resolved = resolve_extension_migrations_path("vendor", "sqlspec.extensions.adk:migrations")

    assert resolved is not None
    assert resolved.is_dir()
    assert resolved.name == "migrations"


def test_resolves_module_specification_without_subdir() -> None:
    """A module specification with an empty subdir resolves to the module directory."""
    resolved = resolve_extension_migrations_path("vendor", "sqlspec.extensions.adk:")

    assert resolved is not None
    assert resolved.name == "adk"


def test_missing_directory_returns_none(tmp_path: Path) -> None:
    """A resolvable value naming a directory that does not exist returns None."""
    assert resolve_extension_migrations_path("vendor", tmp_path / "absent") is None


def test_file_target_returns_none(tmp_path: Path) -> None:
    """A value naming a file rather than a directory returns None."""
    target = tmp_path / "migrations"
    target.write_text("not a directory")

    assert resolve_extension_migrations_path("vendor", target) is None


@pytest.mark.parametrize(
    "spec",
    [
        r"C:\srv\migrations",
        r"\\?\C:\srv\migrations",
        "/opt/my:app/migrations",
    ],
)
def test_path_like_values_are_not_treated_as_modules(spec: str) -> None:
    """Drive letters and colons inside filesystem paths do not trigger module resolution."""
    assert resolve_extension_migrations_path("vendor", spec) is None


def test_bare_dotted_value_is_a_filesystem_path(tmp_path: Path) -> None:
    """Without a ':' separator a dotted value is a filesystem path, not a module."""
    target = tmp_path / "sqlspec.extensions.adk"
    target.mkdir()

    assert resolve_extension_migrations_path("vendor", str(target)) == target


def test_unimportable_module_raises() -> None:
    """A module specification naming a missing package is a configuration error."""
    with pytest.raises(MigrationError, match="could not be imported"):
        resolve_extension_migrations_path("vendor", "definitely_not_installed:migrations")


@pytest.mark.parametrize("spec", [42, None, 1.5, ["migrations"]])
def test_non_string_value_raises(spec: object) -> None:
    """A migrations_path that is not a string or Path is a configuration error."""
    with pytest.raises(MigrationError, match="invalid migrations_path of type"):
        resolve_extension_migrations_path("vendor", spec)  # type: ignore[arg-type]


@pytest.mark.parametrize("spec", ["", "   "])
def test_empty_value_raises(spec: str) -> None:
    """An empty migrations_path is a configuration error."""
    with pytest.raises(MigrationError, match="empty migrations_path"):
        resolve_extension_migrations_path("vendor", spec)
