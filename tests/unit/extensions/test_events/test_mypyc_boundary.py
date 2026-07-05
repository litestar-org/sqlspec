"""Tests for events mypyc boundary decisions."""

import ast
from pathlib import Path

try:
    import tomllib  # type: ignore[import-not-found]
except ModuleNotFoundError:  # pragma: no cover
    import tomli as tomllib

PROJECT_ROOT = Path(__file__).resolve().parents[4]
EVENTS_PACKAGE = "sqlspec.extensions.events"
EVENTS_ROOT = PROJECT_ROOT / "sqlspec" / "extensions" / "events"


def _events_mypyc_config() -> tuple[set[str], set[str]]:
    pyproject = tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text())
    config = pyproject["tool"]["hatch"]["build"]["targets"]["wheel"]["hooks"]["mypyc"]
    includes = {path for path in config["include"] if path.startswith("sqlspec/extensions/events/")}
    excludes = {path for path in config["exclude"] if path.startswith("sqlspec/extensions/events/")}
    return includes, excludes


def _imported_events_modules(path: Path) -> set[str]:
    tree = ast.parse(path.read_text())
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module and node.module.startswith(EVENTS_PACKAGE):
            modules.add(node.module)
        elif isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names if alias.name.startswith(EVENTS_PACKAGE))
    return modules


def test_compiled_events_modules_do_not_import_interpreted_events_modules() -> None:
    """Compiled events helpers should only depend on compiled events siblings."""
    includes, excludes = _events_mypyc_config()

    assert "sqlspec/extensions/events/_channel.py" in includes
    assert "sqlspec/extensions/events/_models.py" in includes
    assert "sqlspec/extensions/events/_queue.py" in includes

    excluded_modules = {f"{EVENTS_PACKAGE}.{Path(path).stem}" for path in excludes if Path(path).name != "__init__.py"}
    allowed_interpreted_imports: set[str] = set()
    for include in includes:
        imported_modules = _imported_events_modules(PROJECT_ROOT / include)
        assert imported_modules.isdisjoint(excluded_modules - allowed_interpreted_imports)
