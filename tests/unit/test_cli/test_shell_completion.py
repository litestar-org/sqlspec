"""Tests for CLI shell completion functionality."""

import os
import shutil
import subprocess


def test_bash_completion_generates_script() -> None:
    """Test that bash completion script can be generated."""
    sqlspec_cmd = shutil.which("sqlspec")
    if not sqlspec_cmd:
        sqlspec_cmd = "sqlspec"

    env = os.environ.copy()
    env["_SQLSPEC_COMPLETE"] = "bash_source"

    result = subprocess.run([sqlspec_cmd], env=env, capture_output=True, text=True, timeout=10)

    assert result.returncode == 0, f"Failed with stderr: {result.stderr}"
    assert "_sqlspec_completion" in result.stdout
    assert "complete -o nosort -F _sqlspec_completion sqlspec" in result.stdout


def test_zsh_completion_generates_script() -> None:
    """Test that zsh completion script can be generated."""
    sqlspec_cmd = shutil.which("sqlspec")
    if not sqlspec_cmd:
        sqlspec_cmd = "sqlspec"

    env = os.environ.copy()
    env["_SQLSPEC_COMPLETE"] = "zsh_source"

    result = subprocess.run([sqlspec_cmd], env=env, capture_output=True, text=True, timeout=10)

    assert result.returncode == 0, f"Failed with stderr: {result.stderr}"
    assert "#compdef sqlspec" in result.stdout
    assert "_sqlspec_completion" in result.stdout


def test_fish_completion_generates_script() -> None:
    """Test that fish completion script can be generated."""
    sqlspec_cmd = shutil.which("sqlspec")
    if not sqlspec_cmd:
        sqlspec_cmd = "sqlspec"

    env = os.environ.copy()
    env["_SQLSPEC_COMPLETE"] = "fish_source"

    result = subprocess.run([sqlspec_cmd], env=env, capture_output=True, text=True, timeout=10)

    assert result.returncode == 0, f"Failed with stderr: {result.stderr}"
    assert "function _sqlspec_completion" in result.stdout
    assert "complete --no-files --command sqlspec" in result.stdout


def test_completion_scripts_are_valid_shell_syntax() -> None:
    """Test that generated completion scripts have valid shell syntax."""
    sqlspec_cmd = shutil.which("sqlspec")
    if not sqlspec_cmd:
        sqlspec_cmd = "sqlspec"

    shells = {"bash": "bash_source", "zsh": "zsh_source", "fish": "fish_source"}

    for shell_name, complete_var in shells.items():
        env = os.environ.copy()
        env["_SQLSPEC_COMPLETE"] = complete_var

        result = subprocess.run([sqlspec_cmd], env=env, capture_output=True, text=True, timeout=10)

        assert result.returncode == 0, f"{shell_name} completion failed: {result.stderr}"
        assert len(result.stdout) > 0, f"{shell_name} completion script is empty"
        assert result.stderr == "", f"{shell_name} completion has stderr output: {result.stderr}"
