"""Tests for config discovery functionality."""

import os
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from sqlspec.utils.config_tools import discover_config_from_pyproject, find_pyproject_toml, parse_pyproject_config


def test_discover_from_pyproject_single(tmp_path: "Path", monkeypatch: "pytest.MonkeyPatch") -> None:
    """Test pyproject.toml with single config string."""
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text("""
[tool.sqlspec]
config = "myapp.config:get_configs"
""")

    monkeypatch.chdir(tmp_path)
    result = discover_config_from_pyproject()

    assert result == "myapp.config:get_configs"


def test_discover_from_pyproject_multiple(tmp_path: "Path", monkeypatch: "pytest.MonkeyPatch") -> None:
    """Test pyproject.toml with config list."""
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text("""
[tool.sqlspec]
config = [
    "myapp.config:primary",
    "myapp.config:secondary"
]
""")

    monkeypatch.chdir(tmp_path)
    result = discover_config_from_pyproject()

    assert result == "myapp.config:primary,myapp.config:secondary"


def test_discover_from_pyproject_not_found(tmp_path: "Path", monkeypatch: "pytest.MonkeyPatch") -> None:
    """Test returns None when no pyproject.toml exists."""
    monkeypatch.chdir(tmp_path)
    result = discover_config_from_pyproject()

    assert result is None


def test_discover_from_pyproject_no_tool_section(tmp_path: "Path", monkeypatch: "pytest.MonkeyPatch") -> None:
    """Test returns None when pyproject.toml has no [tool.sqlspec]."""
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text("""
[project]
name = "myapp"
version = "1.0.0"
""")

    monkeypatch.chdir(tmp_path)
    result = discover_config_from_pyproject()

    assert result is None


def test_malformed_pyproject_error(tmp_path: "Path", monkeypatch: "pytest.MonkeyPatch") -> None:
    """Test malformed pyproject.toml gives clear error."""
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text("invalid toml syntax {{{")

    monkeypatch.chdir(tmp_path)

    with pytest.raises(ValueError, match="Failed to parse"):
        discover_config_from_pyproject()


def test_find_pyproject_walks_upward(tmp_path: "Path", monkeypatch: "pytest.MonkeyPatch") -> None:
    """Test walks filesystem upward to find pyproject.toml."""
    # Create nested directory structure
    root = tmp_path / "project"
    subdir = root / "src" / "myapp"
    subdir.mkdir(parents=True)

    # Place pyproject.toml in root
    pyproject = root / "pyproject.toml"
    pyproject.write_text("""
[tool.sqlspec]
config = "myapp.config:get_configs"
""")

    # Start from subdir
    monkeypatch.chdir(subdir)
    result = find_pyproject_toml()

    assert result is not None
    assert result == pyproject


def test_find_pyproject_stops_at_git(tmp_path: "Path", monkeypatch: "pytest.MonkeyPatch") -> None:
    """Test stops at .git directory boundary."""
    # Create structure with .git boundary
    git_root = tmp_path / "repo"
    git_root.mkdir()
    (git_root / ".git").mkdir()

    subdir = git_root / "src"
    subdir.mkdir()

    # Start from subdir - should stop at git_root
    monkeypatch.chdir(subdir)
    result = find_pyproject_toml()

    assert result is None


def test_find_pyproject_current_dir(tmp_path: "Path", monkeypatch: "pytest.MonkeyPatch") -> None:
    """Test finds pyproject.toml in current directory."""
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text("""
[tool.sqlspec]
config = "myapp.config:get_configs"
""")

    monkeypatch.chdir(tmp_path)
    result = find_pyproject_toml()

    assert result is not None
    assert result == pyproject


def test_parse_config_string(tmp_path: "Path") -> None:
    """Test parses config as string."""
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text("""
[tool.sqlspec]
config = "myapp.config:get_configs"
""")

    result = parse_pyproject_config(pyproject)

    assert result == "myapp.config:get_configs"


def test_parse_config_list(tmp_path: "Path") -> None:
    """Test parses config as list, joins with comma."""
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text("""
[tool.sqlspec]
config = ["path1:func1", "path2:func2"]
""")

    result = parse_pyproject_config(pyproject)

    assert result == "path1:func1,path2:func2"


def test_parse_config_invalid_type(tmp_path: "Path") -> None:
    """Test raises error for non-string, non-list config."""
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text("""
[tool.sqlspec]
config = 123
""")

    with pytest.raises(ValueError, match="must be string or list of strings, got int"):
        parse_pyproject_config(pyproject)


def test_parse_config_invalid_list_items(tmp_path: "Path") -> None:
    """Test raises error for list with non-string items."""
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text("""
[tool.sqlspec]
config = ["valid", 123, "another"]
""")

    with pytest.raises(ValueError, match="list items must be strings"):
        parse_pyproject_config(pyproject)


def test_parse_config_no_config_key(tmp_path: "Path") -> None:
    """Test returns None when [tool.sqlspec] has no config key."""
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text("""
[tool.sqlspec]
other_setting = "value"
""")

    result = parse_pyproject_config(pyproject)

    assert result is None


def test_parse_config_tool_not_dict(tmp_path: "Path") -> None:
    """Test returns None when [tool] is not a dict."""
    pyproject = tmp_path / "pyproject.toml"
    # This is actually not possible with TOML, but test the code path
    pyproject.write_text("""
[project]
name = "test"
""")

    result = parse_pyproject_config(pyproject)

    assert result is None


def test_discover_from_nested_subdirectory(tmp_path: "Path", monkeypatch: "pytest.MonkeyPatch") -> None:
    """Test discovery from deeply nested subdirectory."""
    root = tmp_path / "project"
    deep = root / "a" / "b" / "c" / "d"
    deep.mkdir(parents=True)

    pyproject = root / "pyproject.toml"
    pyproject.write_text("""
[tool.sqlspec]
config = "deep.config:get_configs"
""")

    monkeypatch.chdir(deep)
    result = discover_config_from_pyproject()

    assert result == "deep.config:get_configs"


def test_parse_config_empty_list(tmp_path: "Path") -> None:
    """Test handles empty config list."""
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text("""
[tool.sqlspec]
config = []
""")

    result = parse_pyproject_config(pyproject)

    # Empty list joins to empty string
    assert result == ""


def test_parse_config_with_whitespace(tmp_path: "Path") -> None:
    """Test config values are preserved with whitespace."""
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text("""
[tool.sqlspec]
config = "  myapp.config:get_configs  "
""")

    result = parse_pyproject_config(pyproject)

    # Whitespace should be preserved (caller can strip if needed)
    assert result == "  myapp.config:get_configs  "


def test_find_pyproject_filesystem_root(tmp_path: "Path", monkeypatch: "pytest.MonkeyPatch") -> None:
    """Test stops at filesystem root without infinite loop."""
    # Start from tmp_path (no pyproject.toml anywhere in parents)
    monkeypatch.chdir(tmp_path)
    result = find_pyproject_toml()

    # Should return None, not hang or error
    assert result is None


def test_find_pyproject_stops_at_directory_owned_by_other_uid(
    tmp_path: "Path", monkeypatch: "pytest.MonkeyPatch"
) -> None:
    """Discovery stops before traversing directories owned by another non-root UID."""
    parent_pyproject = tmp_path / "pyproject.toml"
    parent_pyproject.write_text('[tool.sqlspec]\nconfig = "myapp.config:get_configs"\n')

    foreign_dir = tmp_path / "shared"
    work_dir = foreign_dir / "project"
    work_dir.mkdir(parents=True)
    monkeypatch.chdir(work_dir)

    real_stat = os.stat
    uid = 1000
    if hasattr(os, "getuid"):
        monkeypatch.setattr(os, "getuid", lambda: uid)

    def _fake_stat(path: "str | os.PathLike[str]", *args: Any, **kwargs: Any) -> Any:
        if Path(path) == foreign_dir:
            return SimpleNamespace(st_uid=2000)
        if Path(path) in {work_dir, tmp_path, parent_pyproject}:
            return SimpleNamespace(st_uid=uid)
        return real_stat(path, *args, **kwargs)

    monkeypatch.setattr(os, "stat", _fake_stat)
    assert find_pyproject_toml() is None


def test_find_pyproject_refuses_file_owned_by_other_uid(tmp_path: "Path", monkeypatch: "pytest.MonkeyPatch") -> None:
    """Discovery refuses pyproject.toml owned by another non-root UID."""
    work_dir = tmp_path / "workspace"
    work_dir.mkdir()
    foreign_pyproject = work_dir / "pyproject.toml"
    foreign_pyproject.write_text('[tool.sqlspec]\nconfig = "evil.config:load"\n')
    monkeypatch.chdir(work_dir)

    real_stat = os.stat
    uid = 1000
    if hasattr(os, "getuid"):
        monkeypatch.setattr(os, "getuid", lambda: uid)

    def _fake_stat(path: "str | os.PathLike[str]", *args: Any, **kwargs: Any) -> Any:
        resolved = Path(path)
        if resolved == foreign_pyproject:
            return SimpleNamespace(st_uid=2000)
        if resolved == work_dir:
            return SimpleNamespace(st_uid=uid)
        return real_stat(path, *args, **kwargs)

    monkeypatch.setattr(os, "stat", _fake_stat)
    assert find_pyproject_toml() is None


def test_find_pyproject_allows_root_owned_ancestor_directory(
    tmp_path: "Path", monkeypatch: "pytest.MonkeyPatch"
) -> None:
    """Discovery continues across root-owned parent directories to user config."""
    root_pyproject = tmp_path / "pyproject.toml"
    root_pyproject.write_text('[tool.sqlspec]\nconfig = "myapp.config:get_configs"\n')
    shared_parent = tmp_path / "container"
    work_dir = shared_parent / "app"
    work_dir.mkdir(parents=True)
    monkeypatch.chdir(work_dir)

    real_stat = os.stat
    uid = 1000
    if hasattr(os, "getuid"):
        monkeypatch.setattr(os, "getuid", lambda: uid)

    def _fake_stat(path: "str | os.PathLike[str]", *args: Any, **kwargs: Any) -> Any:
        resolved = Path(path)
        if resolved == shared_parent:
            return SimpleNamespace(st_uid=0)
        if resolved in {work_dir, tmp_path, root_pyproject}:
            return SimpleNamespace(st_uid=uid)
        return real_stat(path, *args, **kwargs)

    monkeypatch.setattr(os, "stat", _fake_stat)
    assert find_pyproject_toml() == root_pyproject
