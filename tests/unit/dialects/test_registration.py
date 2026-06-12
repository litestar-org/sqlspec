"""Tests for sqlglot dialect registration via entry points and the metaclass."""

import subprocess
import sys
from importlib.metadata import entry_points

DIALECT_ENTRY_POINTS = {
    "paradedb": "sqlspec.dialects.postgres",
    "pgvector": "sqlspec.dialects.postgres",
    "spangres": "sqlspec.dialects.spanner",
    "spanner": "sqlspec.dialects.spanner",
}


def test_entry_points_declare_all_dialects() -> None:
    eps = {ep.name: ep.value for ep in entry_points().select(group="sqlglot.dialects")}
    for name, module in DIALECT_ENTRY_POINTS.items():
        assert name in eps
        assert eps[name].startswith(module)


def test_sqlglot_resolves_dialects_without_sqlspec_dialect_import() -> None:
    """sqlglot must resolve sqlspec dialect names lazily through entry points."""
    code = (
        "import sys\n"
        "import sqlglot\n"
        "assert not [m for m in sys.modules if m.startswith('sqlspec')]\n"
        "print(sqlglot.parse_one('SELECT 1', dialect='spanner').sql(dialect='spanner'))\n"
        "print(sqlglot.parse_one(\"SELECT a <=> b FROM t\", dialect='pgvector').sql(dialect='pgvector'))\n"
    )
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=False)
    assert result.returncode == 0, result.stderr
    assert "SELECT 1" in result.stdout
    assert "<=>" in result.stdout


def test_importing_sqlspec_does_not_eagerly_load_dialect_machinery() -> None:
    """``import sqlspec`` must not pay the sqlglot dialect registration cost."""
    code = (
        "import sys\n"
        "import sqlspec\n"
        "loaded = [m for m in sys.modules if m.startswith('sqlspec.dialects')]\n"
        "assert not loaded, loaded\n"
        "upstream = [m for m in sys.modules if m.startswith('sqlglot.dialects.')]\n"
        "assert len(upstream) < 15, upstream\n"
        "print('ok')\n"
    )
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=False)
    assert result.returncode == 0, result.stderr


def test_lazy_dialects_attribute_still_works() -> None:
    code = (
        "import sqlspec\n"
        "from sqlspec.dialects import Spanner, Spangres, PGVector, ParadeDB\n"
        "assert sqlspec.dialects.Spanner is Spanner\n"
        "from sqlglot.dialects.dialect import Dialect\n"
        "assert Dialect.get('spanner') is Spanner\n"
        "assert Dialect.get('spangres') is Spangres\n"
        "assert Dialect.get('pgvector') is PGVector\n"
        "assert Dialect.get('paradedb') is ParadeDB\n"
        "print('ok')\n"
    )
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=False)
    assert result.returncode == 0, result.stderr
