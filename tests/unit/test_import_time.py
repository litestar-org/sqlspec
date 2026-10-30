"""Regression tests for import-time optional dependency leakage."""

import ast
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
    import sqlspec  # noqa: F401

    leaked = [name for name in FORBIDDEN if name in sys.modules]
    assert not leaked, f"import sqlspec eagerly imported: {leaked}"


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

    allowed_cross_imports: dict[str, set[str]] = {
        "aiomysql": {"mysql_common"},
        "asyncmy": {"mysql_common"},
        "cockroach_asyncpg": {"asyncpg"},
        "cockroach_psycopg": {"psycopg"},
        "mysqlconnector": {"mysql_common"},
        "pymysql": {"mysql_common"},
    }

    violations: list[str] = []
    for adapter_dir in sorted(adapters_root.iterdir()):
        if not adapter_dir.is_dir() or adapter_dir.name.startswith((".", "_")):
            continue
        adapter_name = adapter_dir.name
        allowed = allowed_cross_imports.get(adapter_name, set())

        for py_file in sorted(adapter_dir.rglob("*.py")):
            tree = ast.parse(py_file.read_text(encoding="utf-8"), filename=str(py_file))
            for node in ast.walk(tree):
                imported_module: str | None = None
                if isinstance(node, ast.ImportFrom) and node.module:
                    if node.module.startswith("sqlspec.adapters."):
                        imported_module = node.module.split(".")[2]
                elif isinstance(node, ast.Import):
                    for alias in node.names:
                        if alias.name.startswith("sqlspec.adapters."):
                            imported_module = alias.name.split(".")[2]

                if imported_module and imported_module != adapter_name and imported_module not in allowed:
                    violations.append(
                        f"{py_file}:{getattr(node, 'lineno', '?')}: {adapter_name} imports sqlspec.adapters.{imported_module}"
                    )

    assert not violations, "Found cross-adapter imports:\n" + "\n".join(violations)
