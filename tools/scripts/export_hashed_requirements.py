"""Export hash-pinned, marker-preserving requirement subsets from ``uv.lock``."""

import argparse
import re
import shutil
import subprocess
import sys
import tomllib
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any

__all__ = (
    "REQUIREMENT_PROFILES",
    "ExportedRequirement",
    "build_dependency_graph",
    "export_uv_requirements",
    "format_requirements_txt",
    "main",
    "normalize_package_name",
    "parse_exported_requirements",
    "parse_requirement_spec",
    "render_profile_requirements",
    "resolve_package_closure",
)

_NORMALIZE_RE = re.compile(r"[-_.]+")
_SPEC_RE = re.compile(r"^\s*([A-Za-z0-9][A-Za-z0-9._-]*)(?:\[([^\]]+)\])?\s*$")
_HEADER_RE = re.compile(r"^\s*([A-Za-z0-9][A-Za-z0-9._-]*)==([^;\s]+?)(?:\s*;\s*(.+?))?\s*$")

REQUIREMENT_PROFILES: dict[str, tuple[str, ...]] = {
    "cli": ("cibuildwheel",),
    "runtime": ("typing-extensions", "sqlglot", "mypy-extensions", "rich-click"),
    "build": ("hatch-mypyc", "hatchling", "typing-extensions", "sqlglot", "mypy-extensions", "rich-click"),
    "pgo": ("anyio", "pyarrow", "aiosqlite", "duckdb", "adbc-driver-sqlite", "adbc-driver-manager"),
    "test": ("aiosqlite", "pymssql", "cloud-sql-python-connector", "google-cloud-alloydb-connector"),
    "smoke": ("obstore", "pyarrow"),
    "drivers": (
        "adbc-driver-manager",
        "adbc-driver-postgresql",
        "adbc-driver-sqlite",
        "aiomysql",
        "aiosqlite",
        "asyncmy",
        "asyncpg",
        "duckdb",
        "google-cloud-bigquery",
        "google-cloud-spanner",
        "mssql-python",
        "mysql-connector-python",
        "oracledb",
        "psqlpy",
        "psycopg",
        "psycopg-binary",
        "psycopg-pool",
        "pyarrow",
        "pymssql",
        "pymysql",
    ),
    "drivers-db2": ("ibm-db",),
    "drivers-sqlanywhere": ("sqlanydb",),
    "drivers-sqlanywhere-build": ("setuptools",),
}
"""Root distributions exported per requirement profile.

``drivers`` holds SDKs with a binary wheel for every Linux wheel-test target. ``ibm-db`` ships Linux
wheels for x86_64 only, so it has its own profile. ``sqlanydb`` ships as a source distribution, so it
has its own profile and ``drivers-sqlanywhere-build`` pins the backend that builds it.
"""


@dataclass(frozen=True, slots=True)
class ExportedRequirement:
    """Single hash-pinned requirement entry parsed from ``uv export``.

    Attributes:
        name: Normalized PEP 503 package name.
        version: Exact locked version string.
        marker: Optional PEP 508 environment marker without leading semicolon.
        hashes: Tuple of ``--hash=sha256:...`` directives for the package version.
    """

    name: str
    version: str
    marker: str | None
    hashes: tuple[str, ...]


def normalize_package_name(name: str) -> str:
    """Normalize a Python distribution name per PEP 503."""
    return _NORMALIZE_RE.sub("-", name.strip()).lower()


def parse_requirement_spec(spec: str) -> tuple[str, tuple[str, ...]]:
    """Parse ``package`` or ``package[extra1,extra2]`` into normalized components."""
    match = _SPEC_RE.match(spec)
    if match is None:
        msg = f"Invalid requirement specifier: {spec!r}"
        raise ValueError(msg)
    name = normalize_package_name(match.group(1))
    extras_raw = match.group(2)
    if not extras_raw:
        return name, ()
    extras = tuple(normalize_package_name(part) for part in extras_raw.split(",") if part.strip())
    return name, extras


def build_dependency_graph(lock_data: dict[str, Any]) -> "dict[str, list[_LockPackageNode]]":
    """Build a normalized dependency adjacency table from parsed ``uv.lock`` TOML."""
    graph: dict[str, list[_LockPackageNode]] = {}
    for raw_pkg in lock_data.get("package", ()):
        name = normalize_package_name(str(raw_pkg["name"]))
        deps = tuple(_parse_edge(dep) for dep in raw_pkg.get("dependencies", ()))
        raw_opt = raw_pkg.get("optional-dependencies") or {}
        opt_deps = {
            normalize_package_name(str(extra_name)): tuple(_parse_edge(dep) for dep in extra_edges)
            for extra_name, extra_edges in raw_opt.items()
        }
        graph.setdefault(name, []).append(
            _LockPackageNode(name=name, dependencies=deps, optional_dependencies=opt_deps)
        )
    return graph


def parse_exported_requirements(exported_text: str) -> dict[str, list[ExportedRequirement]]:
    """Parse ``uv export --format requirements.txt`` output into hashed entries.

    Args:
        exported_text: Full stdout produced by ``uv export``.

    Returns:
        Mapping from normalized package name to exported version/marker entries.

    Raises:
        ValueError: If any requirement entry lacks an exact pin or ``sha256`` hash.
    """
    entries: dict[str, list[ExportedRequirement]] = {}
    for logical in _join_logical_lines(exported_text):
        parts = logical.split(" --hash=")
        header = parts[0].strip()
        match = _HEADER_RE.match(header)
        if match is None:
            msg = f"Expected exact 'name==version' requirement in uv export output, got: {header!r}"
            raise ValueError(msg)
        hashes = tuple(f"--hash={h.strip()}" for h in parts[1:] if h.strip())
        if not hashes or not all(h.startswith("--hash=sha256:") for h in hashes):
            msg = f"Requirement {header!r} is missing required --hash=sha256: entries"
            raise ValueError(msg)
        name = normalize_package_name(match.group(1))
        version = match.group(2).strip()
        marker = match.group(3).strip() if match.group(3) else None
        entries.setdefault(name, []).append(
            ExportedRequirement(name=name, version=version, marker=marker, hashes=hashes)
        )
    return entries


def resolve_package_closure(graph: "dict[str, list[_LockPackageNode]]", roots: tuple[str, ...] | list[str]) -> set[str]:
    """Compute the full transitive package closure from ``uv.lock`` for root specs.

    Args:
        graph: Dependency adjacency table from :func:`build_dependency_graph`.
        roots: Root distribution specifiers such as ``cibuildwheel`` or ``psycopg[binary]``.

    Returns:
        Set of normalized package names covering every transitive dependency.

    Raises:
        ValueError: If any root or reachable dependency does not exist in ``uv.lock``.
    """
    visited_packages: set[str] = set()
    visited_extras: set[tuple[str, str]] = set()
    queue: deque[tuple[str, tuple[str, ...]]] = deque(parse_requirement_spec(root) for root in roots)

    while queue:
        pkg_name, extras = queue.popleft()
        nodes = graph.get(pkg_name)
        if not nodes:
            msg = f"Package {pkg_name!r} was not found in uv.lock"
            raise ValueError(msg)
        if pkg_name not in visited_packages:
            visited_packages.add(pkg_name)
            for node in nodes:
                for dep in node.dependencies:
                    queue.append((dep.name, dep.extras))
        for extra_name in extras:
            key = (pkg_name, extra_name)
            if key in visited_extras:
                continue
            visited_extras.add(key)
            for node in nodes:
                for dep in node.optional_dependencies.get(extra_name, ()):
                    queue.append((dep.name, dep.extras))

    return visited_packages


def format_requirements_txt(requirements: list[ExportedRequirement], *, header_comment: str | None = None) -> str:
    """Render hashed requirement entries into deterministic ``pip -r`` text."""
    lines: list[str] = []
    if header_comment:
        lines.append(f"# {header_comment}")
    for entry in requirements:
        head = f"{entry.name}=={entry.version}"
        if entry.marker:
            head = f"{head} ; {entry.marker}"
        lines.append(f"{head} \\")
        for idx, hash_line in enumerate(entry.hashes):
            suffix = " \\" if idx < len(entry.hashes) - 1 else ""
            lines.append(f"    {hash_line}{suffix}")
    return "\n".join(lines) + ("\n" if lines else "")


def render_profile_requirements(
    lock_data: dict[str, Any],
    exported_text: str,
    *,
    roots: tuple[str, ...] | list[str],
    header_comment: str | None = None,
) -> str:
    """Resolve and render hashed requirements for the requested root specs."""
    graph = build_dependency_graph(lock_data)
    exported = parse_exported_requirements(exported_text)
    closure = resolve_package_closure(graph, roots)

    selected: list[ExportedRequirement] = []
    for pkg_name in sorted(closure):
        pkg_entries = exported.get(pkg_name)
        if not pkg_entries:
            msg = f"Package {pkg_name!r} is in closure from uv.lock but absent from uv export output"
            raise ValueError(msg)
        selected.extend(sorted(pkg_entries, key=lambda item: (item.version, item.marker or "")))
    return format_requirements_txt(selected, header_comment=header_comment)


def export_uv_requirements(project_root: Path) -> str:
    """Invoke ``uv export`` with frozen lockfile and hashes for all extras and groups."""
    uv_bin = shutil.which("uv")
    if uv_bin is None:
        msg = "uv executable was not found in PATH"
        raise RuntimeError(msg)
    result = subprocess.run(
        [
            uv_bin,
            "export",
            "--all-extras",
            "--all-groups",
            "--frozen",
            "--no-emit-project",
            "--format",
            "requirements.txt",
        ],
        cwd=project_root,
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    return result.stdout


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint for exporting hash-pinned requirement subsets."""
    parser = _build_parser()
    args = parser.parse_args(argv)

    profiles: list[str] = list(args.profile or ())
    if args.all:
        profiles = list(REQUIREMENT_PROFILES)
    if not profiles and not args.package:
        parser.error("Specify --all, at least one --profile, or at least one --package")

    lock_path = args.lockfile.resolve()
    lock_data = tomllib.loads(lock_path.read_text(encoding="utf-8"))
    exported_text = (
        args.exported.read_text(encoding="utf-8")
        if args.exported is not None
        else export_uv_requirements(lock_path.parent)
    )

    if args.output_dir is not None:
        if not profiles:
            parser.error("--output-dir requires --all or --profile")
        args.output_dir.mkdir(parents=True, exist_ok=True)
        for profile_name in profiles:
            roots = [*REQUIREMENT_PROFILES[profile_name], *args.package]
            rendered = render_profile_requirements(
                lock_data,
                exported_text,
                roots=roots,
                header_comment=f"Hash-pinned {profile_name} requirements generated from uv.lock",
            )
            (args.output_dir / f"{profile_name}.txt").write_text(rendered, encoding="utf-8")
        return 0

    roots = [pkg for name in profiles for pkg in REQUIREMENT_PROFILES[name]]
    roots.extend(args.package)
    label = "+".join(profiles) if profiles else "custom"
    rendered = render_profile_requirements(
        lock_data, exported_text, roots=roots, header_comment=f"Hash-pinned {label} requirements generated from uv.lock"
    )
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    else:
        sys.stdout.write(rendered)
    return 0


@dataclass(frozen=True, slots=True)
class _DependencyEdge:
    """Directed dependency edge parsed from a ``uv.lock`` package entry."""

    name: str
    extras: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class _LockPackageNode:
    """Dependency metadata for a locked package entry in ``uv.lock``."""

    name: str
    dependencies: tuple[_DependencyEdge, ...]
    optional_dependencies: dict[str, tuple[_DependencyEdge, ...]]


def _parse_edge(raw_dep: dict[str, Any]) -> _DependencyEdge:
    dep_name = normalize_package_name(str(raw_dep["name"]))
    raw_extras = raw_dep.get("extra", ())
    extras = tuple(normalize_package_name(str(item)) for item in raw_extras)
    return _DependencyEdge(name=dep_name, extras=extras)


def _join_logical_lines(exported_text: str) -> list[str]:
    logical_lines: list[str] = []
    buffer: list[str] = []
    for raw_line in exported_text.splitlines():
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#"):
            if buffer:
                logical_lines.append(" ".join(buffer))
                buffer = []
            continue
        if stripped.endswith("\\"):
            part = stripped[:-1].strip()
            if part:
                buffer.append(part)
            continue
        buffer.append(stripped)
        logical_lines.append(" ".join(buffer))
        buffer = []
    if buffer:
        logical_lines.append(" ".join(buffer))
    return logical_lines


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Export hash-pinned requirement subsets from uv.lock.")
    parser.add_argument(
        "--profile",
        action="append",
        choices=sorted(REQUIREMENT_PROFILES),
        help="Named requirement profile to export (repeatable).",
    )
    parser.add_argument("--all", action="store_true", help="Export all predefined CI requirement profiles.")
    parser.add_argument("--package", action="append", default=[], help="Extra root package specifier to include.")
    parser.add_argument("--lockfile", type=Path, default=Path("uv.lock"), help="Path to uv.lock.")
    parser.add_argument("--exported", type=Path, default=None, help="Pre-exported uv requirements file (optional).")
    parser.add_argument("--output", type=Path, default=None, help="Output file path for single subset output.")
    parser.add_argument("--output-dir", type=Path, default=None, help="Output directory for profile .txt files.")
    return parser


if __name__ == "__main__":
    raise SystemExit(main())
