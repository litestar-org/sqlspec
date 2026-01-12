# pyright: reportPrivateUsage=false
"""Unit tests for migration utility functions.

Tests for migration utilities including:
- Author detection from git config
- Fallback to system username
- Error handling for git unavailability
"""

import os
import subprocess
from pathlib import Path
from typing import Any, cast
from unittest.mock import Mock, patch

import pytest

from sqlspec.config import DatabaseConfigProtocol
from sqlspec.migrations.templates import TemplateValidationError
from sqlspec.migrations.utils import _get_git_config, _get_system_username, create_migration_file, get_author


def _callable_author(_: Any | None = None) -> str:
    return "callable-user"


def test_get_git_config_success() -> None:
    """Test successful git config retrieval."""
    mock_result = Mock()
    mock_result.returncode = 0
    mock_result.stdout = "John Doe\n"

    with patch("subprocess.run", return_value=mock_result) as mock_run:
        result = _get_git_config("user.name")

    assert result == "John Doe"
    mock_run.assert_called_once_with(
        ["git", "config", "user.name"], capture_output=True, text=True, timeout=2, check=False
    )


def test_get_git_config_empty_value() -> None:
    """Test git config with empty value."""
    mock_result = Mock()
    mock_result.returncode = 0
    mock_result.stdout = "   \n"

    with patch("subprocess.run", return_value=mock_result):
        result = _get_git_config("user.name")

    assert result is None


def test_get_git_config_non_zero_return() -> None:
    """Test git config with non-zero return code."""
    mock_result = Mock()
    mock_result.returncode = 1
    mock_result.stdout = ""

    with patch("subprocess.run", return_value=mock_result):
        result = _get_git_config("user.name")

    assert result is None


def test_get_git_config_git_not_installed() -> None:
    """Test git config when git command not found."""
    with patch("subprocess.run", side_effect=FileNotFoundError("git not found")):
        result = _get_git_config("user.name")

    assert result is None


def test_get_git_config_subprocess_error() -> None:
    """Test git config with subprocess error."""
    with patch("subprocess.run", side_effect=subprocess.SubprocessError("subprocess error")):
        result = _get_git_config("user.name")

    assert result is None


def test_get_git_config_timeout() -> None:
    """Test git config with timeout."""
    with patch("subprocess.run", side_effect=subprocess.TimeoutExpired("git", 2)):
        result = _get_git_config("user.name")

    assert result is None


def test_get_git_config_os_error() -> None:
    """Test git config with OS error."""
    with patch("subprocess.run", side_effect=OSError("os error")):
        result = _get_git_config("user.name")

    assert result is None


def test_get_system_username_with_user_env() -> None:
    """Test system username retrieval with USER env var."""
    with patch.dict(os.environ, {"USER": "testuser"}):
        result = _get_system_username()

    assert result == "testuser"


def test_get_system_username_without_user_env() -> None:
    """Test system username retrieval without USER env var."""
    with patch.dict(os.environ, {}, clear=True):
        result = _get_system_username()

    assert result == "unknown"


def test_get_author_with_git_config() -> None:
    """Test author retrieval with full git config."""
    with (
        patch("sqlspec.migrations.utils._get_git_config") as mock_git_config,
        patch("sqlspec.migrations.utils._get_system_username") as mock_system,
    ):
        mock_git_config.side_effect = lambda key: {"user.name": "Jane Developer", "user.email": "jane@example.com"}.get(
            key
        )

        result = get_author()

    assert result == "Jane Developer <jane@example.com>"
    mock_system.assert_not_called()


def test_get_author_with_only_git_name() -> None:
    """Test author retrieval with only git name (no email)."""
    with (
        patch("sqlspec.migrations.utils._get_git_config") as mock_git_config,
        patch("sqlspec.migrations.utils._get_system_username", return_value="systemuser") as mock_system,
    ):
        mock_git_config.side_effect = lambda key: {"user.name": "Jane Developer", "user.email": None}.get(key)

        result = get_author()

    assert result == "systemuser"
    mock_system.assert_called_once()


def test_get_author_with_only_git_email() -> None:
    """Test author retrieval with only git email (no name)."""
    with (
        patch("sqlspec.migrations.utils._get_git_config") as mock_git_config,
        patch("sqlspec.migrations.utils._get_system_username", return_value="systemuser") as mock_system,
    ):
        mock_git_config.side_effect = lambda key: {"user.name": None, "user.email": "jane@example.com"}.get(key)

        result = get_author()

    assert result == "systemuser"
    mock_system.assert_called_once()


def test_get_author_without_git_config() -> None:
    """Test author retrieval without git config."""
    with (
        patch("sqlspec.migrations.utils._get_git_config", return_value=None),
        patch("sqlspec.migrations.utils._get_system_username", return_value="fallbackuser") as mock_system,
    ):
        result = get_author()

    assert result == "fallbackuser"
    mock_system.assert_called_once()


def test_get_author_git_command_fails() -> None:
    """Test author retrieval when git command fails."""
    with (
        patch("sqlspec.migrations.utils._get_git_config", return_value=None),
        patch.dict(os.environ, {"USER": "envuser"}),
    ):
        result = get_author()

    assert result == "envuser"


def test_get_author_integration_with_real_git() -> None:
    """Test author retrieval integration with real git (if available)."""
    result = get_author()

    assert isinstance(result, str)
    assert len(result) > 0

    if "<" in result and ">" in result:
        assert result.count("<") == 1
        assert result.count(">") == 1
        assert result.index("<") < result.index(">")
        name_part = result.split("<")[0].strip()
        email_part = result.split("<")[1].split(">")[0].strip()
        assert len(name_part) > 0
        assert len(email_part) > 0
        assert "@" in email_part or "." in email_part


def test_create_migration_file_includes_git_author(tmp_path: Path) -> None:
    """Test that created migration file includes git-based author."""
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()

    with patch("sqlspec.migrations.utils.get_author", return_value="Test Author <test@example.com>"):
        file_path = create_migration_file(migrations_dir, "0001", "test migration", "sql")

    content = file_path.read_text()
    assert "Author: Test Author <test@example.com>" in content


def test_create_migration_file_includes_fallback_author(tmp_path: Path) -> None:
    """Test that created migration file includes fallback author."""
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()

    with (
        patch("sqlspec.migrations.utils._get_git_config", return_value=None),
        patch.dict(os.environ, {"USER": "testuser"}),
    ):
        file_path = create_migration_file(migrations_dir, "0001", "test migration", "sql")

    content = file_path.read_text()
    assert "Author: testuser" in content


def test_create_migration_file_python_includes_git_author(tmp_path: Path) -> None:
    """Test that created Python migration file includes git-based author."""
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()

    with patch("sqlspec.migrations.utils.get_author", return_value="Python Dev <dev@example.com>"):
        file_path = create_migration_file(migrations_dir, "0001", "test migration", "py")

    content = file_path.read_text()
    assert "Author: Python Dev <dev@example.com>" in content


def test_create_migration_file_slugifies_message(tmp_path: Path) -> None:
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()

    with patch("sqlspec.migrations.utils.get_author", return_value="Author <author@example.com>"):
        file_path = create_migration_file(migrations_dir, "0001", "Test Migration!!!", "sql")

    assert file_path.name.startswith("0001_test_migration")


def test_create_migration_file_respects_default_format(tmp_path: Path) -> None:
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()

    class DummyConfig:
        migration_config = {"default_format": "py", "author": "Static"}
        bind_key: str | None = None
        driver_type: type | None = None

    file_path = create_migration_file(
        migrations_dir, "0001", "custom", None, config=cast(DatabaseConfigProtocol[Any, Any, Any], DummyConfig())
    )

    assert file_path.suffix == ".py"


def test_create_migration_file_uses_custom_sql_template(tmp_path: Path) -> None:
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()

    class DummyConfig:
        migration_config = {
            "author": "Acme Ops",
            "title": "Acme Migration",
            "templates": {
                "sql": {"header": "-- {title} [ACME]", "metadata": ["-- Owner: {author}"], "body": "-- custom body"}
            },
        }
        bind_key: str | None = None
        driver_type: type | None = None

    file_path = create_migration_file(
        migrations_dir, "0001", "custom", "sql", config=cast(DatabaseConfigProtocol[Any, Any, Any], DummyConfig())
    )
    content = file_path.read_text()

    assert "-- Acme Migration [ACME]" in content
    assert "-- Owner: Acme Ops" in content


def test_python_template_includes_down_and_context(tmp_path: Path) -> None:
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()

    file_path = create_migration_file(migrations_dir, "0001", "ctx", "py")
    content = file_path.read_text()

    assert "def up(context: object | None = None)" in content
    assert "def down(context: object | None = None)" in content


def test_git_config_with_special_characters() -> None:
    """Test git config with special characters in name."""
    mock_result = Mock()
    mock_result.returncode = 0
    mock_result.stdout = "José Müller\n"

    with patch("subprocess.run", return_value=mock_result):
        result = _get_git_config("user.name")

    assert result == "José Müller"


def test_get_author_with_unicode_git_config() -> None:
    """Test author retrieval with Unicode characters."""
    with patch("sqlspec.migrations.utils._get_git_config") as mock_git_config:
        mock_git_config.side_effect = lambda key: {
            "user.name": "André François",
            "user.email": "andré@example.com",
        }.get(key)

        result = get_author()

    assert result == "André François <andré@example.com>"


def test_get_author_with_whitespace_in_git_config() -> None:
    """Test author retrieval with extra whitespace in git config."""
    mock_name_result = Mock()
    mock_name_result.returncode = 0
    mock_name_result.stdout = "  John Doe  \n\n"

    mock_email_result = Mock()
    mock_email_result.returncode = 0
    mock_email_result.stdout = "  john@example.com  \n"

    def mock_run(cmd: list[str], **kwargs: object) -> Mock:
        if "user.name" in cmd:
            return mock_name_result
        if "user.email" in cmd:
            return mock_email_result
        return Mock(returncode=1, stdout="")

    with patch("subprocess.run", side_effect=mock_run):
        result = get_author()

    assert result == "John Doe <john@example.com>"


def test_get_author_env_string_mode() -> None:
    with patch.dict(os.environ, {"CI_AUTHOR": "CI User"}):
        result = get_author("env:CI_AUTHOR")

    assert result == "CI User"


def test_get_author_env_mode_dict() -> None:
    with patch.dict(os.environ, {"CI_AUTHOR": "CI User"}):
        result = get_author({"mode": "env", "value": "CI_AUTHOR"})

    assert result == "CI User"


def test_get_author_env_missing_raises() -> None:
    with pytest.raises(TemplateValidationError):
        get_author({"mode": "env", "value": "MISSING_VAR"})


def test_get_author_callable_string() -> None:
    result = get_author(f"{__name__}:_callable_author")

    assert result == "callable-user"


def test_get_author_callable_dict_with_config() -> None:
    result = get_author({"mode": "callable", "value": f"{__name__}:_callable_author"})

    assert result == "callable-user"


def test_get_author_invalid_callable_path() -> None:
    with pytest.raises(TemplateValidationError):
        get_author("callable:badpath")


def test_get_author_invalid_mode() -> None:
    with pytest.raises(TemplateValidationError):
        get_author({"mode": "unknown"})
