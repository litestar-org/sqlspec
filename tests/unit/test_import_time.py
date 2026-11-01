"""Regression tests for import-time optional dependency leakage."""

import ast
import importlib
import subprocess
import sys
from pathlib import Path

import pytest

FORBIDDEN = (
    "pandas",
    "polars",
    "pyarrow",
    "litestar",
    "pydantic",
    "opentelemetry",
    "prometheus_client",
    "asyncpg",
    "rich",
    "sqlglot",
)


@pytest.mark.parametrize(
    ("statement", "unused"),
    [
        ("from sqlspec.builder import QueryBuilder", ("sqlspec.builder._factory",)),
        ("from sqlspec.migrations import SyncMigrationTracker", ("sqlspec.migrations.commands", "rich")),
        (
            "from sqlspec.extensions.events import EventRuntimeHints",
            ("sqlspec.extensions.events._channel", "sqlspec.migrations"),
        ),
        (
            "from sqlspec import SQLSpec; from sqlspec.adapters.sqlite import SqliteConfig; manager = SQLSpec(); config = manager.add_config(SqliteConfig());\nwith manager.provide_session(config) as session: assert session.select_value('SELECT 1') == 1\nconfig.close_pool()",
            ("sqlspec.migrations.commands", "sqlspec.builder._factory", "rich", "asyncpg"),
        ),
    ],
)
def test_first_use_avoids_unrelated_imports(statement: str, unused: tuple[str, ...]) -> None:
    """First-use imports of focused submodules avoid loading unrelated subsystems."""
    script = f"import sys\n{statement}\nassert not set({unused!r}).intersection(sys.modules)"
    subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, check=True)


@pytest.mark.parametrize("block_asyncpg", [False, True])
def test_primitive_json_does_not_load_asyncpg(block_asyncpg: bool) -> None:
    """Primitive encoding works without loading an optional database driver."""
    script = f"""
import importlib.abc
import json
import sys

class BlockAsyncpg(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path, target=None):
        if fullname == 'asyncpg' or fullname.startswith('asyncpg.'):
            raise ModuleNotFoundError('asyncpg blocked', name=fullname)

if {block_asyncpg!r}:
    sys.meta_path.insert(0, BlockAsyncpg())
from sqlspec.utils.serializers import to_json
assert json.loads(to_json({{'value': 1}})) == {{'value': 1}}
assert 'asyncpg' not in sys.modules
"""
    subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, check=True)


def test_asyncpg_uuid_encoding_and_encoder_override() -> None:
    """Direct UUID encoding and caller overrides work without adapter setup."""
    pytest.importorskip("asyncpg")
    script = """
import json
from asyncpg.pgproto.pgproto import UUID
from sqlspec.utils.serializers import DEFAULT_TYPE_ENCODERS, to_json
from sqlspec.utils.serializers._json import StandardLibSerializer

value = UUID('12345678-1234-5678-1234-567812345678')
assert json.loads(to_json(value)) == str(value)
assert DEFAULT_TYPE_ENCODERS.copy()[UUID](value) == str(value)
serializer = StandardLibSerializer(type_encoders={UUID: lambda value: 'override'})
assert json.loads(serializer.encode(value)) == 'override'
"""
    subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, check=True)


def test_import_sqlspec_does_not_import_heavy_optional_deps() -> None:
    """Importing ``sqlspec`` in a fresh subprocess should not pull heavy optional deps."""
    script = (
        f"import sys, sqlspec; leaked=[name for name in {FORBIDDEN!r} if name in sys.modules]; print(','.join(leaked))"
    )
    result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, check=True)
    leaked = [name for name in result.stdout.strip().split(",") if name]
    assert not leaked, f"import sqlspec eagerly imported: {leaked}"


@pytest.mark.skipif(any(name in sys.modules for name in FORBIDDEN), reason="forbidden deps already preloaded")
def test_import_sqlspec_does_not_import_heavy_optional_deps_in_process() -> None:
    """Same-process import should also stay clean when the environment is pristine."""
    importlib.import_module("sqlspec")

    leaked = [name for name in FORBIDDEN if name in sys.modules]
    assert not leaked, f"import sqlspec eagerly imported: {leaked}"


def test_unit_pytest_session_does_not_load_docker_or_integration_plugins() -> None:
    """Collecting unit tests does not load Docker service providers or integration fixtures."""
    script = """
import sys
import pytest

exit_code = pytest.main(["--collect-only", "-q", "-p", "no:cacheprovider", "tests/unit/test_import_time.py"])
assert exit_code == 0, f"pytest collection failed with exit code {exit_code}"
leaked = [
    name
    for name in sorted(sys.modules)
    if name.startswith(("pytest_databases.docker", "tests.integration", "tests.plugins"))
]
assert leaked == [], f"unit collection loaded integration plugins: {leaked}"
"""
    subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, check=True)


def test_adbc_import_does_not_require_or_load_duckdb() -> None:
    """Importing ADBC modules must not require or import DuckDB."""
    script = """
import importlib.abc
import sys

class BlockDuckDB(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path, target=None):
        if fullname == "duckdb" or fullname.startswith("duckdb."):
            raise ModuleNotFoundError("duckdb blocked", name=fullname)

sys.meta_path.insert(0, BlockDuckDB())

import sqlspec.adapters.adbc
import sqlspec.adapters.adbc.config
import sqlspec.adapters.adbc.driver
import sqlspec.adapters.adbc.data_dictionary
from sqlspec.adapters.adbc import AdbcConfig

config = AdbcConfig()
assert config is not None
assert "duckdb" not in sys.modules
assert "sqlspec.adapters.duckdb" not in sys.modules
assert "sqlspec.adapters.sqlite" not in sys.modules
"""
    subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, check=True)


def test_no_unintended_cross_adapter_imports() -> None:
    """Ensure adapter implementations do not import from other adapter packages."""
    adapters_root = Path("sqlspec/adapters")
    assert adapters_root.is_dir()

    allowed_cross_imports: dict[str, set[str]] = {"cockroach_asyncpg": {"asyncpg"}, "cockroach_psycopg": {"psycopg"}}

    violations: list[str] = []
    for adapter_dir in sorted(adapters_root.iterdir()):
        if not adapter_dir.is_dir() or adapter_dir.name.startswith((".", "_")):
            continue
        adapter_name = adapter_dir.name
        allowed = allowed_cross_imports.get(adapter_name, set())

        for py_file in sorted(adapter_dir.rglob("*.py")):
            tree = ast.parse(py_file.read_text(encoding="utf-8"), filename=str(py_file))
            pkg_parts = list(py_file.parent.parts)
            for node in ast.walk(tree):
                imported_modules: list[str] = []
                if isinstance(node, ast.ImportFrom):
                    if node.level > 0:
                        base_parts = pkg_parts[: -(node.level - 1)] if node.level > 1 else pkg_parts
                        target_parts = [*base_parts, *node.module.split(".")] if node.module else base_parts
                        if target_parts[:2] == ["sqlspec", "adapters"]:
                            if len(target_parts) >= 3:
                                imported_modules.append(target_parts[2])
                            else:
                                imported_modules.extend(alias.name.split(".")[0] for alias in node.names)
                    elif node.module == "sqlspec.adapters":
                        imported_modules.extend(alias.name.split(".")[0] for alias in node.names)
                    elif node.module and node.module.startswith("sqlspec.adapters."):
                        imported_modules.append(node.module.split(".")[2])
                elif isinstance(node, ast.Import):
                    imported_modules.extend(
                        alias.name.split(".")[2] for alias in node.names if alias.name.startswith("sqlspec.adapters.")
                    )

                violations.extend(
                    f"{py_file}:{getattr(node, 'lineno', '?')}: {adapter_name} imports sqlspec.adapters.{imported_module}"
                    for imported_module in imported_modules
                    if imported_module != adapter_name and imported_module not in allowed
                )

    assert not violations, "Found cross-adapter imports:\n" + "\n".join(violations)


def test_pytest_collect_only_succeeds_without_plugin_registration_errors() -> None:
    """Verifies that root pytest collection succeeds without non-top-level plugin errors."""
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "--collect-only", "-q", "tests"], capture_output=True, text=True, check=False
    )
    assert result.returncode == 0, f"pytest --collect-only failed:\n{result.stderr}\n{result.stdout}"


ADAPTER_DRIVERS: dict[str, tuple[str, ...]] = {
    "adbc": ("adbc_driver_manager",),
    "aiomysql": ("aiomysql",),
    "aiosqlite": ("aiosqlite",),
    "arrow_odbc": ("arrow_odbc",),
    "asyncmy": ("asyncmy",),
    "asyncpg": ("asyncpg",),
    "bigquery": ("google.cloud.bigquery",),
    "cockroach_asyncpg": ("asyncpg",),
    "cockroach_psycopg": ("psycopg",),
    "db2": ("ibm_db", "ibm_db_dbi"),
    "duckdb": ("duckdb",),
    "mssql_python": ("mssql_python",),
    "mysqlconnector": ("mysql.connector",),
    "oracledb": ("oracledb",),
    "psqlpy": ("psqlpy",),
    "psycopg": ("psycopg",),
    "pymssql": ("pymssql",),
    "pymysql": ("pymysql",),
    "spanner": ("google.cloud.spanner_v1",),
    "sqlanywhere": ("sqlanydb",),
    "sqlite": (),
    "sybase": ("pymssql",),
}
"""Driver module(s) each adapter imports; every package under ``sqlspec/adapters/`` must be registered."""

DRIVER_PACKAGE_DEPENDENCIES: dict[str, tuple[str, ...]] = {"aiomysql": ("pymysql",)}
"""Other registered driver modules that a driver package itself depends on (aiomysql requires PyMySQL)."""

SECONDARY_OPTIONAL_MODULES = (
    "pyarrow",
    "pandas",
    "polars",
    "numpy",
    "pgvector",
    "rich",
    "opentelemetry",
    "litestar",
    "pydantic",
    "fsspec",
    "obstore",
    "google.cloud.storage",
    "google.cloud.sql.connector",
    "google.cloud.alloydb.connector",
)

DRIVER_OWNED_OPTIONAL_IMPORTS: dict[str, frozenset[str]] = {
    "adbc": frozenset({"pyarrow", "pandas", "polars", "numpy"}),
    "arrow_odbc": frozenset({"pyarrow", "numpy"}),
    "bigquery": frozenset({"pyarrow", "pandas", "numpy", "opentelemetry"}),
    "mysqlconnector": frozenset({"opentelemetry"}),
    "spanner": frozenset({"opentelemetry"}),
}
"""Optional modules the driver SDK imports on its own when installed; sqlspec code does not import them."""


def test_every_adapter_package_is_registered() -> None:
    """Every adapter package declares its driver so the import-contract gates cover it."""
    packages = {
        path.name
        for path in Path("sqlspec/adapters").iterdir()
        if path.is_dir() and (path / "__init__.py").exists() and not path.name.startswith(("_", "."))
    }
    assert packages == set(ADAPTER_DRIVERS)


@pytest.mark.parametrize("adapter", sorted(name for name, drivers in ADAPTER_DRIVERS.items() if drivers))
def test_adapter_import_raises_driver_module_not_found(adapter: str) -> None:
    """Importing an adapter without its driver raises the driver's own ModuleNotFoundError."""
    drivers = ADAPTER_DRIVERS[adapter]
    script = f"""
import importlib.abc
import sys

blocked = {drivers!r}

class BlockDriver(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path, target=None):
        if any(fullname == name or fullname.startswith(name + ".") for name in blocked):
            raise ModuleNotFoundError(f"No module named {{fullname!r}}", name=fullname)
        return None

sys.meta_path.insert(0, BlockDriver())
try:
    import sqlspec.adapters.{adapter}
except ModuleNotFoundError as exc:
    assert exc.name and any(exc.name == name or exc.name.startswith(name + ".") or name.startswith(exc.name + ".") for name in blocked), exc.name
else:
    raise AssertionError("adapter imported without its driver")
"""
    result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, check=False)
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("adapter", sorted(ADAPTER_DRIVERS))
def test_adapter_imports_with_every_other_driver_blocked(adapter: str) -> None:
    """Each adapter works without any other adapter's driver installed."""
    own = {*ADAPTER_DRIVERS[adapter], *DRIVER_PACKAGE_DEPENDENCIES.get(adapter, ())}
    blocked = sorted({name for drivers in ADAPTER_DRIVERS.values() for name in drivers} - own)
    script = f"""
import importlib
import importlib.abc
import sys

blocked = {blocked!r}

class BlockOtherDrivers(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path, target=None):
        if any(fullname == name or fullname.startswith(name + ".") for name in blocked):
            raise ModuleNotFoundError(f"No module named {{fullname!r}}", name=fullname)
        return None

sys.meta_path.insert(0, BlockOtherDrivers())
package = importlib.import_module("sqlspec.adapters.{adapter}")
for name in getattr(package, "__all__", ()):
    getattr(package, name)
"""
    result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, check=False)
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("adapter", sorted(ADAPTER_DRIVERS))
def test_adapter_import_does_not_load_secondary_optional_modules(adapter: str) -> None:
    """Importing an adapter loads only its driver, plus optional modules the driver SDK imports itself."""
    allowed = sorted(DRIVER_OWNED_OPTIONAL_IMPORTS.get(adapter, frozenset()))
    script = (
        f"import sys, importlib; importlib.import_module('sqlspec.adapters.{adapter}'); "
        f"print(','.join(m for m in {SECONDARY_OPTIONAL_MODULES!r} if m in sys.modules and m not in {allowed!r}))"
    )
    result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, check=True)
    leaked = [name for name in result.stdout.strip().split(",") if name]
    assert not leaked, f"import sqlspec.adapters.{adapter} eagerly imported: {leaked}"
