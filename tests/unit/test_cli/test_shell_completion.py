"""Tests for CLI shell completion functionality."""

import shutil
import sys

from click.testing import CliRunner

from sqlspec.cli import get_sqlspec_group


def _sqlspec_command() -> "list[str]":
    executable = shutil.which("sqlspec")
    if executable:
        return [executable]
    return [sys.executable, "-m", "sqlspec"]


def _invoke_completion(mode: str):
    runner = CliRunner()
    env = {"_SQLSPEC_COMPLETE": mode}
    return runner.invoke(get_sqlspec_group(), env=env, prog_name="sqlspec")


def test_bash_completion_generates_script() -> None:
    """Test that bash completion script can be generated."""
    result = _invoke_completion("bash_source")

    assert result.exit_code == 0, f"Failed with stderr: {result.output}"
    assert "_sqlspec_completion" in result.output
    assert "complete -o nosort -F _sqlspec_completion sqlspec" in result.output


def test_zsh_completion_generates_script() -> None:
    """Test that zsh completion script can be generated."""
    result = _invoke_completion("zsh_source")

    assert result.exit_code == 0, f"Failed with stderr: {result.output}"
    assert "#compdef sqlspec" in result.output
    assert "_sqlspec_completion" in result.output


def test_fish_completion_generates_script() -> None:
    """Test that fish completion script can be generated."""
    result = _invoke_completion("fish_source")

    assert result.exit_code == 0, f"Failed with stderr: {result.output}"
    assert "function _sqlspec_completion" in result.output
    assert "complete --no-files --command sqlspec" in result.output


def test_completion_scripts_are_valid_shell_syntax() -> None:
    """Test that generated completion scripts have valid shell syntax."""
    shells = {"bash": "bash_source", "zsh": "zsh_source", "fish": "fish_source"}

    for shell_name, complete_var in shells.items():
        result = _invoke_completion(complete_var)

        assert result.exit_code == 0, f"{shell_name} completion failed: {result.output}"
        assert len(result.output) > 0, f"{shell_name} completion script is empty"
