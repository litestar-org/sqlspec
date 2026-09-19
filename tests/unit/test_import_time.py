"""Regression tests for import-time optional dependency leakage."""

import subprocess
import sys

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
