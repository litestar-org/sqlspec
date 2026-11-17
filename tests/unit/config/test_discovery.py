"""Tests for config discovery functionality."""

import os
import stat
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from sqlspec.utils.config_tools import discover_config_from_pyproject, find_pyproject_toml, parse_pyproject_config

requires_posix_ownership = pytest.mark.skipif(not hasattr(os, "getuid"), reason="POSIX ownership model required")


def _override_stat(monkeypatch: "pytest.MonkeyPatch", overrides: "dict[tuple[Path, bool], dict[str, int]]") -> None:
    """Replace selected ``os.stat`` fields, keyed by ``(path, follow_symlinks)``."""
    real_stat = os.stat

    def _fake_stat(path: Any, *args: Any, **kwargs: Any) -> Any:
        result = real_stat(path, *args, **kwargs)
        if not isinstance(path, (str, os.PathLike)):
            return result
        fields = overrides.get((Path(path), kwargs.get("follow_symlinks", True)))
        if fields is None:
            return result
        base = {"st_uid": result.st_uid, "st_mode": result.st_mode, "st_nlink": result.st_nlink}
        return SimpleNamespace(**{**base, **fields})

    monkeypatch.setattr(os, "stat", _fake_stat)


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


def test_find_pyproject_treats_symlinked_git_as_repository_boundary(
    tmp_path: "Path", monkeypatch: "pytest.MonkeyPatch"
) -> None:
    """A symlinked .git entry ends discovery before the parent configuration is reached."""
    (tmp_path / "pyproject.toml").write_text('[tool.sqlspec]\nconfig = "outer:load"\n', encoding="utf-8")
    work_dir = tmp_path / "checkout"
    work_dir.mkdir()
    (work_dir / ".git").symlink_to(tmp_path)
    monkeypatch.chdir(work_dir)

    assert find_pyproject_toml() is None


def test_find_pyproject_accepts_symlinked_config_owned_by_user(
    tmp_path: "Path", monkeypatch: "pytest.MonkeyPatch"
) -> None:
    """A symlinked pyproject.toml is returned when the link and its target belong to the current user."""
    shared_config = tmp_path / "shared" / "pyproject.toml"
    shared_config.parent.mkdir()
    shared_config.write_text('[tool.sqlspec]\nconfig = "myapp.config:get_configs"\n', encoding="utf-8")
    work_dir = tmp_path / "project"
    work_dir.mkdir()
    (work_dir / "pyproject.toml").symlink_to(shared_config)
    monkeypatch.chdir(work_dir)

    assert find_pyproject_toml() == work_dir / "pyproject.toml"
    assert discover_config_from_pyproject() == "myapp.config:get_configs"


@requires_posix_ownership
@pytest.mark.parametrize("follow_symlinks", [False, True], ids=["foreign-link", "foreign-target"])
def test_find_pyproject_refuses_symlinked_config_owned_by_other_uid(
    tmp_path: "Path", monkeypatch: "pytest.MonkeyPatch", caplog: "pytest.LogCaptureFixture", follow_symlinks: bool
) -> None:
    """A symlinked pyproject.toml is refused when another user owns the link or its target."""
    target = tmp_path / "real_pyproject.toml"
    target.write_text('[tool.sqlspec]\nconfig = "evil:load"\n', encoding="utf-8")
    work_dir = tmp_path / "project"
    work_dir.mkdir()
    link = work_dir / "pyproject.toml"
    link.symlink_to(target)
    monkeypatch.chdir(work_dir)
    foreign_uid = os.getuid() + 1000
    _override_stat(monkeypatch, {(link, follow_symlinks): {"st_uid": foreign_uid}})

    assert find_pyproject_toml() is None
    assert f"Refusing {link}: it is owned by another user (uid {foreign_uid})" in caplog.text


@pytest.mark.parametrize("config_mode", [0o644, 0o666], ids=["private-file", "world-writable-file"])
def test_find_pyproject_discovers_through_user_owned_world_writable_ancestors(
    tmp_path: "Path", monkeypatch: "pytest.MonkeyPatch", config_mode: int
) -> None:
    """Discovery from a subdirectory reaches pyproject.toml when the user's own tree is mode 0777."""
    project = tmp_path / "project"
    work_dir = project / "src" / "app"
    work_dir.mkdir(parents=True)
    pyproject = project / "pyproject.toml"
    pyproject.write_text('[tool.sqlspec]\nconfig = "myapp.config:get_configs"\n', encoding="utf-8")
    world_writable = [project, project / "src", work_dir]
    for directory in world_writable:
        os.chmod(directory, 0o777)
    os.chmod(pyproject, config_mode)
    monkeypatch.chdir(work_dir)

    try:
        assert find_pyproject_toml() == pyproject
    finally:
        for directory in world_writable:
            os.chmod(directory, 0o755)


def test_find_pyproject_discovers_through_ancestors_without_posix_ownership(
    tmp_path: "Path", monkeypatch: "pytest.MonkeyPatch"
) -> None:
    """Without os.getuid, mode bits are ignored and discovery walks through 0777 ancestors."""
    project = tmp_path / "project"
    work_dir = project / "src"
    work_dir.mkdir(parents=True)
    pyproject = project / "pyproject.toml"
    pyproject.write_text('[tool.sqlspec]\nconfig = "myapp.config:get_configs"\n', encoding="utf-8")
    monkeypatch.chdir(work_dir)
    monkeypatch.delattr(os, "getuid", raising=False)
    world_writable_dir = {"st_uid": 0, "st_mode": stat.S_IFDIR | 0o777}
    _override_stat(
        monkeypatch,
        {
            (work_dir, False): world_writable_dir,
            (project, False): world_writable_dir,
            (pyproject, False): {"st_uid": 0, "st_mode": stat.S_IFREG | 0o666},
        },
    )

    assert find_pyproject_toml() == pyproject


@requires_posix_ownership
def test_find_pyproject_refuses_foreign_config_in_world_writable_ancestor(
    tmp_path: "Path", monkeypatch: "pytest.MonkeyPatch", caplog: "pytest.LogCaptureFixture"
) -> None:
    """A pyproject.toml planted by another user in a world-writable ancestor is refused and logged."""
    shared = tmp_path / "shared"
    work_dir = shared / "app"
    work_dir.mkdir(parents=True)
    planted = shared / "pyproject.toml"
    planted.write_text('[tool.sqlspec]\nconfig = "evil:load"\n', encoding="utf-8")
    monkeypatch.chdir(work_dir)
    foreign_uid = os.getuid() + 1000
    _override_stat(
        monkeypatch, {(shared, False): {"st_mode": stat.S_IFDIR | 0o777}, (planted, False): {"st_uid": foreign_uid}}
    )

    assert find_pyproject_toml() is None
    assert f"Refusing {planted}: it is owned by another user (uid {foreign_uid})" in caplog.text


@requires_posix_ownership
@pytest.mark.parametrize(
    ("config_fields", "reason"),
    [
        ({}, None),
        ({"st_mode": stat.S_IFREG | 0o666}, "world-writable inside a shared world-writable directory"),
        ({"st_nlink": 2}, "several hard links inside a shared world-writable directory"),
    ],
    ids=["private-file", "world-writable-file", "hard-linked-file"],
)
def test_find_pyproject_in_sticky_world_writable_ancestor_requires_private_config(
    tmp_path: "Path",
    monkeypatch: "pytest.MonkeyPatch",
    caplog: "pytest.LogCaptureFixture",
    config_fields: "dict[str, int]",
    reason: "str | None",
) -> None:
    """In a root-owned sticky directory, only a singly linked, non-world-writable user config is used."""
    sticky = tmp_path / "sticky"
    work_dir = sticky / "app"
    work_dir.mkdir(parents=True)
    pyproject = sticky / "pyproject.toml"
    pyproject.write_text('[tool.sqlspec]\nconfig = "myapp.config:get_configs"\n', encoding="utf-8")
    monkeypatch.chdir(work_dir)
    _override_stat(
        monkeypatch,
        {(sticky, False): {"st_uid": 0, "st_mode": stat.S_IFDIR | 0o1777}, (pyproject, False): config_fields},
    )

    if reason is None:
        assert find_pyproject_toml() == pyproject
        assert caplog.text == ""
    else:
        assert find_pyproject_toml() is None
        assert str(pyproject) in caplog.text
        assert reason in caplog.text


@pytest.mark.skipif(not hasattr(os, "getuid"), reason="POSIX ownership model required")
def test_discovery_refuses_config_swapped_for_a_foreign_file_before_reading(
    tmp_path: "Path", monkeypatch: "pytest.MonkeyPatch", caplog: "pytest.LogCaptureFixture"
) -> None:
    """The ownership test applies to the file that is opened, not only to the path that was found."""
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text('[tool.sqlspec]\nconfig = "myapp.config:get_configs"\n')
    monkeypatch.chdir(tmp_path)
    foreign_uid = os.getuid() + 1000
    real_fstat = os.fstat

    def _foreign_fstat(descriptor: int) -> Any:
        result = real_fstat(descriptor)
        return SimpleNamespace(st_uid=foreign_uid, st_mode=result.st_mode, st_nlink=result.st_nlink)

    assert discover_config_from_pyproject() == "myapp.config:get_configs"

    monkeypatch.setattr(os, "fstat", _foreign_fstat)

    assert discover_config_from_pyproject() is None
    assert f"Refusing {pyproject}: it is owned by another user (uid {foreign_uid})" in caplog.text
