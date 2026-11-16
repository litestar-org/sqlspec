"""Unit tests for hash-pinned requirement subset generation."""

import shutil
import subprocess
import sys
import tomllib
from pathlib import Path
from typing import Any

import pytest
from packaging.markers import Marker
from tools.scripts.export_hashed_requirements import (
    REQUIREMENT_PROFILES,
    build_dependency_graph,
    export_uv_requirements,
    main,
    normalize_package_name,
    parse_exported_requirements,
    render_profile_requirements,
    resolve_package_closure,
)

__all__ = (
    "test_export_all_profiles_preserve_hashes_and_satisfy_uv_dry_run",
    "test_export_profiles_have_complete_closure_across_target_environments",
    "test_main_writes_output_file_and_errors_without_selection",
    "test_parse_and_closure_error_cases",
    "test_render_follows_extras_and_preserves_markers",
)

_REPO_ROOT = Path(__file__).resolve().parents[3]
_UV_LOCK_DATA: dict[str, Any] = tomllib.loads((_REPO_ROOT / "uv.lock").read_text(encoding="utf-8"))
_UV_EXPORTED_TEXT: str = export_uv_requirements(_REPO_ROOT)

_TARGET_ENVIRONMENTS: tuple[dict[str, str], ...] = (
    {"os_name": "posix", "sys_platform": "linux", "python_version": "3.11", "python_full_version": "3.11.9"},
    {"os_name": "posix", "sys_platform": "linux", "python_version": "3.12", "python_full_version": "3.12.6"},
    {"os_name": "posix", "sys_platform": "darwin", "python_version": "3.12", "python_full_version": "3.12.6"},
    {"os_name": "nt", "sys_platform": "win32", "python_version": "3.13", "python_full_version": "3.13.1"},
)


@pytest.mark.parametrize("profile_name", sorted(REQUIREMENT_PROFILES))
def test_export_all_profiles_preserve_hashes_and_satisfy_uv_dry_run(tmp_path: Path, profile_name: str) -> None:
    """Every profile exports sha256-hashed entries and validates under uv --require-hashes."""
    exported_cache = tmp_path / "uv-export.txt"
    exported_cache.write_text(_UV_EXPORTED_TEXT, encoding="utf-8")

    out_dir = tmp_path / "reqs"
    rc = main([
        "--lockfile",
        str(_REPO_ROOT / "uv.lock"),
        "--exported",
        str(exported_cache),
        "--profile",
        profile_name,
        "--output-dir",
        str(out_dir),
    ])
    assert rc == 0

    req_file = out_dir / f"{profile_name}.txt"
    rendered = req_file.read_text(encoding="utf-8")
    parsed = parse_exported_requirements(rendered)

    assert "sqlspec" not in parsed
    assert all(root in parsed for root in REQUIREMENT_PROFILES[profile_name])
    assert all(entry.hashes for entries in parsed.values() for entry in entries)

    uv_bin = shutil.which("uv")
    assert uv_bin is not None
    dry_run = subprocess.run(
        [
            uv_bin,
            "pip",
            "install",
            "--no-config",
            "--python",
            sys.executable,
            "--require-hashes",
            "--dry-run",
            "--no-index",
            "-r",
            str(req_file),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert dry_run.returncode == 0, dry_run.stderr


@pytest.mark.parametrize("profile_name", sorted(REQUIREMENT_PROFILES))
@pytest.mark.parametrize("env", _TARGET_ENVIRONMENTS, ids=["linux-3.11", "linux-3.12", "macos-3.12", "win-3.13"])
def test_export_profiles_have_complete_closure_across_target_environments(
    profile_name: str, env: dict[str, str]
) -> None:
    """Every emitted package's active dependencies for a target environment exist in the rendered output."""
    rendered = render_profile_requirements(_UV_LOCK_DATA, _UV_EXPORTED_TEXT, roots=REQUIREMENT_PROFILES[profile_name])
    parsed = parse_exported_requirements(rendered)
    active_names = {
        pkg_name
        for pkg_name, entries in parsed.items()
        if any(entry.marker is None or Marker(entry.marker).evaluate(env) for entry in entries)
    }

    lock_by_name = {
        normalize_package_name(str(pkg["name"])): pkg
        for pkg in _UV_LOCK_DATA["package"]
        if normalize_package_name(str(pkg["name"])) in active_names
    }
    missing = {
        (pkg_name, normalize_package_name(str(dep["name"])))
        for pkg_name, pkg in lock_by_name.items()
        for dep in pkg.get("dependencies", ())
        if (dep.get("marker") is None or Marker(str(dep["marker"])).evaluate(env))
        and normalize_package_name(str(dep["name"])) not in active_names
    }
    assert missing == set()


def test_render_follows_extras_and_preserves_markers() -> None:
    """Extra dependencies and conditional markers are retained across transitive edges."""
    synthetic_lock: dict[str, Any] = {
        "package": [
            {"name": "root_pkg", "version": "1.0.0", "dependencies": [{"name": "core-lib", "extra": ["grpc"]}]},
            {
                "name": "core-lib",
                "version": "2.0.0",
                "dependencies": [],
                "optional-dependencies": {"grpc": [{"name": "grpc_helper"}]},
            },
            {"name": "grpc-helper", "version": "3.1.0", "dependencies": []},
            {"name": "unused-pkg", "version": "9.9.9", "dependencies": []},
        ]
    }
    synthetic_export = (
        "core-lib==2.0.0 \\\n"
        "    --hash=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n"
        "grpc-helper==3.1.0 ; sys_platform == 'win32' \\\n"
        "    --hash=sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\n"
        "root-pkg==1.0.0 \\\n"
        "    --hash=sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc\n"
        "unused-pkg==9.9.9 \\\n"
        "    --hash=sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd\n"
    )
    rendered = render_profile_requirements(synthetic_lock, synthetic_export, roots=["Root_Pkg"])
    parsed = parse_exported_requirements(rendered)

    assert sorted(parsed) == ["core-lib", "grpc-helper", "root-pkg"]
    assert parsed["grpc-helper"][0].marker == "sys_platform == 'win32'"


@pytest.mark.parametrize(
    ("lock_data", "exported_text", "roots", "match"),
    [
        ({"package": []}, "", ["missing-pkg"], "not found in uv.lock"),
        ({"package": [{"name": "pkg-a", "dependencies": []}]}, "", ["pkg-a"], "absent from uv export"),
        (
            {"package": [{"name": "pkg-a", "dependencies": []}]},
            "pkg-a>=1.0.0 \\\n    --hash=sha256:abcd\n",
            ["pkg-a"],
            "Expected exact 'name==version'",
        ),
        (
            {"package": [{"name": "pkg-a", "dependencies": []}]},
            "pkg-a==1.0.0\n",
            ["pkg-a"],
            "missing required --hash=sha256:",
        ),
    ],
    ids=["missing-lock-pkg", "missing-export-pkg", "unpinned-spec", "missing-hash"],
)
def test_parse_and_closure_error_cases(
    lock_data: dict[str, Any], exported_text: str, roots: list[str], match: str
) -> None:
    """Missing lock entries, unpinned lines, and missing sha256 hashes fail loudly."""
    with pytest.raises(ValueError, match=match):
        render_profile_requirements(lock_data, exported_text, roots=roots)


def test_main_writes_output_file_and_errors_without_selection(tmp_path: Path) -> None:
    """CLI supports --output for custom subsets and exits non-zero when no selection is given."""
    exported_cache = tmp_path / "uv-export.txt"
    exported_cache.write_text(_UV_EXPORTED_TEXT, encoding="utf-8")
    out_file = tmp_path / "out" / "cli.txt"

    rc = main([
        "--lockfile",
        str(_REPO_ROOT / "uv.lock"),
        "--exported",
        str(exported_cache),
        "--profile",
        "cli",
        "--output",
        str(out_file),
    ])
    assert rc == 0
    assert "cibuildwheel==" in out_file.read_text(encoding="utf-8")

    graph = build_dependency_graph(_UV_LOCK_DATA)
    assert "cibuildwheel" in resolve_package_closure(graph, ["cibuildwheel"])

    with pytest.raises(SystemExit):
        main([])
