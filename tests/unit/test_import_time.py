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
    if name.startswith(("pytest_databases.docker.", "tests.integration", "tests.plugins"))
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
            rel_path = py_file.relative_to(adapter_dir)
            is_core_module = rel_path == Path("core.py")
            is_cockroach_boundary = adapter_name in {"cockroach_asyncpg", "cockroach_psycopg"} and rel_path in {
                Path("config.py"),
                Path("driver.py"),
            }

            for lineno, imported_module, full_module, in_type_checking in _iter_adapter_imports(tree, pkg_parts):
                if imported_module == adapter_name:
                    continue
                if is_core_module:
                    violations.append(f"{py_file}:{lineno}: {adapter_name} core.py imports {full_module}")
                    continue
                if imported_module not in allowed:
                    violations.append(f"{py_file}:{lineno}: {adapter_name} imports sqlspec.adapters.{imported_module}")
                    continue
                if is_cockroach_boundary:
                    forbidden_core = f"sqlspec.adapters.{imported_module}.core"
                    if full_module == forbidden_core or full_module.startswith(f"{forbidden_core}."):
                        if adapter_name == "cockroach_asyncpg" or not in_type_checking:
                            violations.append(f"{py_file}:{lineno}: {adapter_name} imports parent core {full_module}")

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
    "snowflake": ("snowflake.connector",),
    "spanner": ("google.cloud.spanner_v1",),
    "sqlanywhere": ("sqlanydb",),
    "sqlite": (),
    "sybase": ("pymssql",),
    "teradata": (),
}
"""Driver module(s) each adapter imports; every package under ``sqlspec/adapters/`` must be registered."""

LAZY_ADAPTER_CONFIGS: dict[str, tuple[str, str]] = {"snowflake": ("SnowflakeConfig", "snowflake-connector-python")}
"""Adapters that report optional SDK availability when opening a physical connection."""

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

DIALECT_DICTIONARY_LAZY_EXPORTS: dict[str, tuple[str, ...]] = {
    "bigquery": ("BigQueryAsyncDataDictionary", "BigQueryMetadataScope", "BigQuerySyncDataDictionary"),
    "cockroachdb": ("CockroachDBAsyncDataDictionary", "CockroachDBSyncDataDictionary"),
    "db2": (
        "Db2AsyncDataDictionary",
        "Db2SyncDataDictionary",
        "build_column_metadata",
        "build_foreign_key_metadata",
        "build_index_metadata",
        "parse_db2_version",
    ),
    "duckdb": ("DuckDBAsyncDataDictionary", "DuckDBSyncDataDictionary"),
    "mariadb": ("MariaDBAsyncDataDictionary", "MariaDBSyncDataDictionary"),
    "mssql": ("MssqlAsyncDataDictionary", "MssqlSyncDataDictionary"),
    "mysql": ("MySQLAsyncDataDictionary", "MySQLSyncDataDictionary"),
    "oracle": (
        "JSONStorageType",
        "OracleAsyncDataDictionary",
        "OracleSyncDataDictionary",
        "OracleVersionInfo",
        "storage_type_from_version",
    ),
    "postgres": ("PostgresAsyncDataDictionary", "PostgresSyncDataDictionary"),
    "snowflake": ("SnowflakeSyncDataDictionary",),
    "spanner": ("SpannerAsyncDataDictionary", "SpannerSyncDataDictionary"),
    "sqlanywhere": (
        "SqlanywhereSyncDataDictionary",
        "build_column_metadata",
        "build_foreign_key_metadata",
        "build_index_metadata",
        "parse_sqlanywhere_version",
    ),
    "sqlite": ("SqliteAsyncDataDictionary", "SqliteSyncDataDictionary"),
    "sybase": (
        "SybaseSyncDataDictionary",
        "build_column_metadata",
        "build_foreign_key_metadata",
        "build_index_metadata",
        "parse_sybase_version",
    ),
    "teradata": (
        "TeradataSyncDataDictionary",
        "build_column_metadata",
        "build_foreign_key_metadata",
        "build_index_metadata",
        "parse_teradata_version",
    ),
}
"""Lazy PEP 562 ``dictionary.py`` exports declared by each shared dialect package."""

DIALECT_DICTIONARY_PACKAGES: tuple[str, ...] = tuple(DIALECT_DICTIONARY_LAZY_EXPORTS)
"""Shared data-dictionary dialect packages exposing PEP 562 lazy dictionary exports."""


def test_every_adapter_package_is_registered() -> None:
    """Every adapter package declares its driver so the import-contract gates cover it."""
    packages = {
        path.name
        for path in Path("sqlspec/adapters").iterdir()
        if path.is_dir() and (path / "__init__.py").exists() and not path.name.startswith(("_", "."))
    }
    assert packages == set(ADAPTER_DRIVERS)


@pytest.mark.parametrize(
    "adapter", sorted(name for name, drivers in ADAPTER_DRIVERS.items() if drivers and name not in LAZY_ADAPTER_CONFIGS)
)
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


@pytest.mark.parametrize("adapter", sorted(LAZY_ADAPTER_CONFIGS))
def test_lazy_adapter_reports_missing_driver_on_connection(adapter: str) -> None:
    """Lazy adapters expose configuration without an SDK and fail before connection I/O when it is missing."""
    drivers = ADAPTER_DRIVERS[adapter]
    config_name, install_package = LAZY_ADAPTER_CONFIGS[adapter]
    script = f"""
import importlib
import importlib.abc
import sys

blocked = {drivers!r}

class BlockDriver(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path, target=None):
        if any(fullname == name or fullname.startswith(name + ".") for name in blocked):
            raise ModuleNotFoundError(f"No module named {{fullname!r}}", name=fullname)
        return None

sys.meta_path.insert(0, BlockDriver())
package = importlib.import_module("sqlspec.adapters.{adapter}")
config = getattr(package, {config_name!r})()
from sqlspec.exceptions import MissingDependencyError
try:
    config.create_connection()
except MissingDependencyError as exc:
    assert {install_package!r} in str(exc)
else:
    raise AssertionError("physical connection opened without its driver SDK")
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


def test_dialects_package_import_stays_lazy() -> None:
    """Importing sqlspec.data_dictionary.dialects does not eagerly load dictionary or driver modules."""
    blocked = _blocked_dialect_drivers()
    script = f"""
import importlib.abc
import sys

blocked = {blocked!r}

class BlockAllDrivers(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path, target=None):
        if any(fullname == name or fullname.startswith(name + ".") for name in blocked):
            raise ModuleNotFoundError(f"No module named {{fullname!r}}", name=fullname)
        return None

sys.meta_path.insert(0, BlockAllDrivers())

import sqlspec.data_dictionary.dialects

loaded_dicts = [
    name
    for name in sys.modules
    if name.startswith("sqlspec.data_dictionary.dialects.") and name.endswith(".dictionary")
]
assert not loaded_dicts, f"eagerly loaded dialect dictionaries: {{loaded_dicts}}"
loaded_drivers = [name for name in sys.modules if name == "sqlspec.driver" or name.startswith("sqlspec.driver.")]
assert not loaded_drivers, f"eagerly loaded sqlspec.driver modules: {{loaded_drivers}}"
loaded_adapters = [
    name for name in sys.modules if name == "sqlspec.adapters" or name.startswith("sqlspec.adapters.")
]
assert not loaded_adapters, f"eagerly loaded adapter modules: {{loaded_adapters}}"
loaded_blocked = [
    name
    for name in sys.modules
    if any(name == driver or name.startswith(driver + ".") for driver in blocked)
]
assert not loaded_blocked, f"eagerly loaded blocked drivers: {{loaded_blocked}}"
"""
    result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, check=False)
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("dialect", DIALECT_DICTIONARY_PACKAGES)
def test_dialect_dictionary_imports_with_all_drivers_blocked(dialect: str) -> None:
    """Dialect package lazy exports load cleanly without any optional adapter driver installed."""
    blocked = _blocked_dialect_drivers()
    lazy_exports = DIALECT_DICTIONARY_LAZY_EXPORTS[dialect]
    script = f"""
import concurrent.futures
import importlib
import importlib.abc
import sys
import threading

blocked = {blocked!r}
dialect = {dialect!r}
lazy_exports = {lazy_exports!r}

class BlockAllDrivers(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path, target=None):
        if any(fullname == name or fullname.startswith(name + ".") for name in blocked):
            raise ModuleNotFoundError(f"No module named {{fullname!r}}", name=fullname)
        return None

sys.meta_path.insert(0, BlockAllDrivers())

pkg_name = f"sqlspec.data_dictionary.dialects.{{dialect}}"
dict_mod_name = f"{{pkg_name}}.dictionary"
pkg = importlib.import_module(pkg_name)

loaded_dicts = [
    name
    for name in sys.modules
    if name.startswith("sqlspec.data_dictionary.dialects.") and name.endswith(".dictionary")
]
loaded_driver = [name for name in sys.modules if name == "sqlspec.driver" or name.startswith("sqlspec.driver.")]
assert not loaded_dicts, f"bare import eagerly loaded dictionaries: {{loaded_dicts}}"
assert not loaded_driver, f"bare import eagerly loaded sqlspec.driver: {{loaded_driver}}"

names = dir(pkg)
assert len(names) == len(set(names)), f"duplicate dir() entries before load: {{names}}"
assert set(pkg.__all__).issubset(set(names)), (
    f"missing __all__ export in dir({{pkg_name}}): {{sorted(set(pkg.__all__) - set(names))}}"
)
for export_name in lazy_exports:
    assert export_name in names, f"missing lazy export {{export_name!r}} in dir({{pkg_name}})"
eager_exports = sorted(set(pkg.__all__) - set(lazy_exports))
for eager_name in eager_exports:
    assert eager_name in vars(pkg), f"eager export {{eager_name!r}} not already loaded in {{pkg_name}}"
    getattr(pkg, eager_name)
assert not [
    name
    for name in sys.modules
    if name.startswith("sqlspec.data_dictionary.dialects.") and name.endswith(".dictionary")
]
assert not [name for name in sys.modules if name == "sqlspec.driver" or name.startswith("sqlspec.driver.")]

try:
    getattr(pkg, "__missing_lazy_attr__")
except AttributeError:
    pass
else:
    raise AssertionError("unknown lazy attribute did not raise AttributeError")
assert not [
    name
    for name in sys.modules
    if name.startswith("sqlspec.data_dictionary.dialects.") and name.endswith(".dictionary")
]
assert not [name for name in sys.modules if name == "sqlspec.driver" or name.startswith("sqlspec.driver.")]

worker_count = 4
for export_name in lazy_exports:
    barrier = threading.Barrier(worker_count)

    def _load_attr(target: str = export_name):
        barrier.wait(timeout=5.0)
        return getattr(pkg, target)

    with concurrent.futures.ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = [executor.submit(_load_attr) for _ in range(worker_count)]
        resolved = [future.result() for future in futures]

    first = resolved[0]
    assert all(item is first for item in resolved), f"concurrent getattr mismatch for {{export_name}}"
    assert dict_mod_name in sys.modules
    assert export_name in vars(pkg)
    dict_mod = sys.modules[dict_mod_name]
    assert getattr(pkg, export_name) is getattr(dict_mod, export_name)
    assert first is getattr(dict_mod, export_name)
    after_names = dir(pkg)
    assert export_name in after_names
    assert set(pkg.__all__).issubset(set(after_names))
    assert len(after_names) == len(set(after_names)), f"duplicate dir() entries after load: {{after_names}}"
    from sqlspec.driver import AsyncDataDictionaryBase, SyncDataDictionaryBase
    if isinstance(first, type) and issubclass(first, (SyncDataDictionaryBase, AsyncDataDictionaryBase)):
        instance = first()
        assert instance.dialect == dialect

loaded_adapters = [
    name for name in sys.modules if name == "sqlspec.adapters" or name.startswith("sqlspec.adapters.")
]
assert not loaded_adapters, f"loaded adapter modules: {{loaded_adapters}}"
loaded_blocked = [
    name
    for name in sys.modules
    if any(name == driver or name.startswith(driver + ".") for driver in blocked)
]
assert not loaded_blocked, f"loaded blocked drivers: {{loaded_blocked}}"
"""
    result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, check=False)
    assert result.returncode == 0, result.stderr


def _blocked_dialect_drivers() -> list[str]:
    return sorted(
        {name for drivers in ADAPTER_DRIVERS.values() for name in drivers}
        | {"google.cloud.spanner", "google.cloud.spanner_v1", "google.cloud.spanner_admin_database_v1", "teradatasql"}
    )


def _is_type_checking_guard(test: ast.expr) -> bool:
    return (isinstance(test, ast.Name) and test.id == "TYPE_CHECKING") or (
        isinstance(test, ast.Attribute) and test.attr == "TYPE_CHECKING"
    )


def _iter_adapter_imports(
    node: ast.AST, pkg_parts: list[str], *, in_type_checking: bool = False
) -> list[tuple[int, str, str, bool]]:
    """Collect ``(lineno, adapter_name, full_module_path, in_type_checking)`` cross-adapter imports."""
    results: list[tuple[int, str, str, bool]] = []
    if isinstance(node, ast.If):
        if _is_type_checking_guard(node.test):
            for child in node.body:
                results.extend(_iter_adapter_imports(child, pkg_parts, in_type_checking=True))
            for child in node.orelse:
                results.extend(_iter_adapter_imports(child, pkg_parts, in_type_checking=in_type_checking))
            return results
        for child in ast.iter_child_nodes(node):
            results.extend(_iter_adapter_imports(child, pkg_parts, in_type_checking=in_type_checking))
        return results

    if isinstance(node, ast.ImportFrom):
        lineno = getattr(node, "lineno", 0)
        if node.level > 0:
            base_parts = pkg_parts[: -(node.level - 1)] if node.level > 1 else pkg_parts
            target_parts = [*base_parts, *node.module.split(".")] if node.module else base_parts
            if target_parts[:2] == ["sqlspec", "adapters"]:
                if len(target_parts) >= 3:
                    results.append((lineno, target_parts[2], ".".join(target_parts), in_type_checking))
                else:
                    for alias in node.names:
                        child_parts = [*target_parts, *alias.name.split(".")]
                        results.append((lineno, child_parts[2], ".".join(child_parts), in_type_checking))
        elif node.module == "sqlspec.adapters":
            results.extend(
                (lineno, alias.name.split(".")[0], f"sqlspec.adapters.{alias.name}", in_type_checking)
                for alias in node.names
            )
        elif node.module and node.module.startswith("sqlspec.adapters."):
            results.append((lineno, node.module.split(".")[2], node.module, in_type_checking))
            results.extend(
                (lineno, node.module.split(".")[2], f"{node.module}.core", in_type_checking)
                for alias in node.names
                if alias.name == "core" and len(node.module.split(".")) == 3
            )
        return results

    if isinstance(node, ast.Import):
        lineno = getattr(node, "lineno", 0)
        results.extend(
            (lineno, alias.name.split(".")[2], alias.name, in_type_checking)
            for alias in node.names
            if alias.name.startswith("sqlspec.adapters.")
        )
        return results

    for child in ast.iter_child_nodes(node):
        results.extend(_iter_adapter_imports(child, pkg_parts, in_type_checking=in_type_checking))
    return results
