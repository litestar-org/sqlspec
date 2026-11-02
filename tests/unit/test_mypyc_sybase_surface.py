"""Unit tests for Sybase and SQL Anywhere packaging, MyPyC inventory, and import boundaries."""

import subprocess
import sys
import tomllib
from pathlib import Path

from tools.scripts.mypyc_inventory import build_inventory, validate_inventory
from tools.scripts.mypyc_smoke import SMOKE_IMPORTS

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def test_pyproject_registers_sybase_and_sqlanywhere_extras_entrypoints_and_markers() -> None:
    """pyproject.toml registers optional dependencies, sqlglot dialect entry points, and pytest markers."""
    config = tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text())

    optional_deps = config["project"]["optional-dependencies"]
    assert optional_deps.get("sybase") == ["pymssql"]
    assert optional_deps.get("sqlanywhere") == ["sqlanydb>=1.0.14"]

    dialect_entrypoints = config["project"]["entry-points"]["sqlglot.dialects"]
    assert dialect_entrypoints.get("sybase") == "sqlspec.dialects.sybase:Sybase"
    assert dialect_entrypoints.get("sqlanywhere") == "sqlspec.dialects.sybase:SQLAnywhere"

    markers = config["tool"]["pytest"]["ini_options"]["markers"]
    assert any(marker.startswith("sybase:") for marker in markers)
    assert any(marker.startswith("sqlanywhere:") for marker in markers)

    mypy_overrides = config["tool"]["mypy"]["overrides"]
    ignored_modules: set[str] = set()
    for override in mypy_overrides:
        if override.get("ignore_missing_imports"):
            modules = override.get("module", [])
            if isinstance(modules, list):
                ignored_modules.update(modules)
    assert "sqlanydb" in ignored_modules
    assert "sqlanydb.*" in ignored_modules


def test_mypyc_inventory_includes_sybase_and_sqlanywhere_hot_surfaces() -> None:
    """mypyc_inventory validates cleanly and classifies Sybase and SQL Anywhere modules accurately."""
    errors = validate_inventory(PROJECT_ROOT)
    assert errors == []

    inventory = build_inventory(PROJECT_ROOT)
    compiled = set(inventory["compiled_modules"])
    interpreted = set(inventory["interpreted_modules"])
    hot_surfaces = inventory["hot_surfaces"]

    expected_compiled = {
        "sqlspec/dialects/sybase/_generators.py",
        "sqlspec/dialects/sybase/_parsers.py",
        "sqlspec/dialects/sybase/_transforms.py",
        "sqlspec/data_dictionary/dialects/sybase/config.py",
        "sqlspec/data_dictionary/dialects/sqlanywhere/config.py",
        "sqlspec/adapters/sybase/core.py",
        "sqlspec/adapters/sybase/pool.py",
        "sqlspec/adapters/sqlanywhere/core.py",
        "sqlspec/adapters/sqlanywhere/pool.py",
    }
    for module_path in expected_compiled:
        assert module_path in compiled, f"Expected compiled module: {module_path}"
        assert module_path in hot_surfaces, f"Expected hot_surfaces entry: {module_path}"
        assert hot_surfaces[module_path]["status"] == "compiled"

    expected_interpreted = {
        "sqlspec/dialects/sybase/__init__.py",
        "sqlspec/adapters/sybase/_typing.py",
        "sqlspec/adapters/sybase/config.py",
        "sqlspec/adapters/sybase/data_dictionary.py",
        "sqlspec/adapters/sybase/driver.py",
        "sqlspec/adapters/sqlanywhere/_typing.py",
        "sqlspec/adapters/sqlanywhere/config.py",
        "sqlspec/adapters/sqlanywhere/data_dictionary.py",
        "sqlspec/adapters/sqlanywhere/driver.py",
    }
    for module_path in expected_interpreted:
        assert module_path in interpreted, f"Expected interpreted module: {module_path}"


def test_mypyc_smoke_imports_include_sybase_and_sqlanywhere() -> None:
    """SMOKE_IMPORTS includes dialect and compiled helper/pool entries for Sybase and SQL Anywhere."""
    smoke_by_name = {entry.name: entry for entry in SMOKE_IMPORTS}
    assert "sybase_dialect" in smoke_by_name
    assert "sqlanywhere_dialect" in smoke_by_name
    assert "sybase_generators" in smoke_by_name
    assert smoke_by_name["sybase_generators"].require_compiled is True
    assert "sybase_pool" in smoke_by_name
    assert smoke_by_name["sybase_pool"].require_compiled is True
    assert "sqlanywhere_pool" in smoke_by_name
    assert smoke_by_name["sqlanywhere_pool"].require_compiled is True


def test_importing_sybase_and_sqlanywhere_adapters_does_not_import_rich() -> None:
    """Importing sqlspec.adapters.sybase and sqlspec.adapters.sqlanywhere does not load rich."""
    script = (
        "import sys\n"
        "import sqlspec.adapters.sybase\n"
        "import sqlspec.adapters.sqlanywhere\n"
        "leaked = [name for name in ('rich',) if name in sys.modules]\n"
        "assert not leaked, f'Eagerly imported: {leaked}'\n"
    )
    subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, check=True)
