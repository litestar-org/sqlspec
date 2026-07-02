"""Unit tests for arrow-odbc extension package boundaries."""

import ast
from pathlib import Path


def test_arrow_odbc_extensions_do_not_import_other_adapters() -> None:
    """Extension modules should stay inside the arrow-odbc adapter boundary."""
    adapter_root = Path(__file__).parents[4] / "sqlspec" / "adapters" / "arrow_odbc"
    extension_files = [
        *adapter_root.joinpath("adk").glob("*.py"),
        *adapter_root.joinpath("events").glob("*.py"),
        *adapter_root.joinpath("litestar").glob("*.py"),
    ]

    violations: list[tuple[str, str]] = []
    for path in extension_files:
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module:
                module = node.module
                if module.startswith("sqlspec.adapters.") and not module.startswith("sqlspec.adapters.arrow_odbc"):
                    violations.append((str(path.relative_to(adapter_root.parents[2])), module))
            if isinstance(node, ast.Import):
                for alias in node.names:
                    module = alias.name
                    if module.startswith("sqlspec.adapters.") and not module.startswith("sqlspec.adapters.arrow_odbc"):
                        violations.append((str(path.relative_to(adapter_root.parents[2])), module))

    assert violations == []
