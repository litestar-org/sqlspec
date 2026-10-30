"""Verification suite for standalone zero-comment documentation examples."""

import ast
import asyncio
import importlib.util
import inspect
import io
import sys
import tokenize
from pathlib import Path
from types import ModuleType

import pytest

from sqlspec.exceptions import MissingDependencyError

__all__ = ()

REPO_ROOT = Path(__file__).resolve().parents[3]
EXAMPLES_DIR = REPO_ROOT / "docs" / "examples"
DIRECTIVE_PREFIXES: tuple[str, ...] = (
    "#!",
    "# type:",
    "# pyright:",
    "# mypy:",
    "# noqa",
    "# ruff:",
    "# pragma:",
    "# fmt:",
    "# isort:",
    "# nosec",
    "# cov:",
)
CONFIG_ONLY_MODULES: frozenset[str] = frozenset({"migration_quickstart_config.py"})
EXTERNAL_DAEMON_EXAMPLES: frozenset[str] = frozenset({
    "drivers/asyncpg_connection.py",
    "drivers/cockroach_psycopg_connection.py",
    "drivers/mysqlconnector_connection.py",
})

ALL_PYTHON_FILES: list[Path] = sorted(EXAMPLES_DIR.rglob("*.py"))
INIT_FILES: list[Path] = [path for path in ALL_PYTHON_FILES if path.name == "__init__.py"]
EXAMPLE_MODULES: list[Path] = [path for path in ALL_PYTHON_FILES if path.name != "__init__.py"]


def test_examples_directory_inventory() -> None:
    """Ensure the documentation examples tree is discovered and non-empty."""
    assert len(INIT_FILES) >= 20
    assert len(EXAMPLE_MODULES) >= 63


def test_example_packages_define_docstring_and_dunder_all() -> None:
    """Verify every package __init__.py under docs/examples defines a docstring and __all__."""
    failures: list[str] = []
    for path in INIT_FILES:
        rel = path.relative_to(EXAMPLES_DIR).as_posix()
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        if not ast.get_docstring(tree):
            failures.append(f"{rel}: missing module docstring")
        has_all = any(
            isinstance(node, ast.Assign)
            and any(isinstance(target, ast.Name) and target.id == "__all__" for target in node.targets)
            for node in tree.body
        )
        if not has_all:
            failures.append(f"{rel}: missing __all__")
    assert not failures, "\n".join(failures)


def test_examples_contain_zero_inline_comments() -> None:
    """Verify zero non-directive inline # comments exist across docs/examples."""
    violations: list[str] = []
    for path in ALL_PYTHON_FILES:
        rel = path.relative_to(EXAMPLES_DIR).as_posix()
        source = path.read_text(encoding="utf-8")
        violations.extend(
            f"{rel}:{token.start[0]}: {token.string.strip()}"
            for token in tokenize.generate_tokens(io.StringIO(source).readline)
            if token.type == tokenize.COMMENT and not _is_directive_comment(token.string)
        )
    assert not violations, f"Found {len(violations)} inline comment(s):\n" + "\n".join(violations[:30])


def test_examples_do_not_reference_pytest_tmp_path() -> None:
    """Verify no documentation snippet references pytest's tmp_path fixture."""
    violations: list[str] = []
    for path in EXAMPLE_MODULES:
        rel = path.relative_to(EXAMPLES_DIR).as_posix()
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Name) and node.id == "tmp_path":
                violations.append(f"{rel}:{node.lineno}: ast.Name('tmp_path')")
            elif isinstance(node, ast.arg) and node.arg == "tmp_path":
                violations.append(f"{rel}:{node.lineno}: ast.arg('tmp_path')")
    assert not violations, f"Found tmp_path leaks in {len(violations)} location(s):\n" + "\n".join(violations)


def test_examples_define_standalone_structure() -> None:
    """Verify every example defines a docstring, top-level imports, __all__, and main()."""
    violations: list[str] = []
    for path in EXAMPLE_MODULES:
        rel = path.relative_to(EXAMPLES_DIR).as_posix()
        source = path.read_text(encoding="utf-8")
        compile(source, str(path), "exec")
        tree = ast.parse(source, filename=str(path))
        if not ast.get_docstring(tree):
            violations.append(f"{rel}: missing module docstring")

        has_all = any(
            isinstance(node, ast.Assign)
            and any(isinstance(target, ast.Name) and target.id == "__all__" for target in node.targets)
            for node in tree.body
        )
        if not has_all:
            violations.append(f"{rel}: missing __all__")

        if rel in CONFIG_ONLY_MODULES:
            continue

        main_defs = [
            node
            for node in tree.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == "main"
        ]
        if not main_defs:
            violations.append(f"{rel}: missing top-level def main() entrypoint")
            continue

        for func_node in main_defs:
            violations.extend(
                f"{rel}:{child.lineno}: nested import inside main()"
                for child in ast.walk(func_node)
                if isinstance(child, (ast.Import, ast.ImportFrom))
            )

    assert not violations, f"Found {len(violations)} structural issue(s):\n" + "\n".join(violations[:40])


@pytest.mark.parametrize(
    "example_path", EXAMPLE_MODULES, ids=[path.relative_to(EXAMPLES_DIR).as_posix() for path in EXAMPLE_MODULES]
)
def test_standalone_example_executes(example_path: Path) -> None:
    """Import each documentation example and execute main() when standalone."""
    rel = example_path.relative_to(EXAMPLES_DIR).as_posix()
    try:
        module = _load_example_module(example_path)
    except (ModuleNotFoundError, MissingDependencyError) as exc:
        pytest.skip(f"Optional dependency not installed for {rel}: {exc}")

    if rel in CONFIG_ONLY_MODULES:
        assert hasattr(module, "database_config")
        return

    assert hasattr(module, "main"), f"{rel} must expose main()"
    entrypoint = module.main
    assert callable(entrypoint)

    if rel in EXTERNAL_DAEMON_EXAMPLES:
        result = entrypoint()
        if inspect.iscoroutine(result):
            result.close()
        return

    try:
        outcome = entrypoint()
        if inspect.iscoroutine(outcome):
            asyncio.run(outcome)
    except (ModuleNotFoundError, MissingDependencyError) as exc:
        pytest.skip(f"Optional dependency not installed for {rel}: {exc}")


def _is_directive_comment(comment_text: str) -> bool:
    """Return whether comment_text is a recognized linter, type-checker, or coverage directive."""
    lowered = comment_text.strip().lower()
    return any(lowered.startswith(prefix) for prefix in DIRECTIVE_PREFIXES)


def _load_example_module(path: Path) -> ModuleType:
    """Load an example module by file path under its canonical docs.examples dotted name."""
    rel_parts = path.relative_to(REPO_ROOT).with_suffix("").parts
    module_name = ".".join(rel_parts)
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        msg = f"Unable to create module spec for {path}"
        raise RuntimeError(msg)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module
