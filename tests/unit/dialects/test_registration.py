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


def test_postgres_extension_operator_registration_is_idempotent() -> None:
    """Repeated registration preserves the installed operator table."""
    from sqlglot.parsers.postgres import PostgresParser

    from sqlspec.dialects.postgres._operators import PGVECTOR_OPERATOR_TOKENS, register_postgres_extension_operators

    register_postgres_extension_operators()
    factor_before = PostgresParser.FACTOR
    register_postgres_extension_operators()
    assert PostgresParser.FACTOR is factor_before
    assert all(token in factor_before for token in PGVECTOR_OPERATOR_TOKENS.values())


def test_spanner_property_parser_registration_is_idempotent() -> None:
    """Repeated registration preserves the installed property parser tables."""
    from sqlglot.parsers.bigquery import BigQueryParser
    from sqlglot.parsers.postgres import PostgresParser

    from sqlspec.dialects.spanner._parsers import register_spanner_property_parsers

    register_spanner_property_parsers()
    before = (BigQueryParser.PROPERTY_PARSERS, PostgresParser.PROPERTY_PARSERS)
    register_spanner_property_parsers()
    assert BigQueryParser.PROPERTY_PARSERS is before[0]
    assert PostgresParser.PROPERTY_PARSERS is before[1]
    assert "INTERLEAVE" in before[0]


def test_concurrent_first_use_registers_all_dialects() -> None:
    """Parallel first parses resolve all custom dialects in a fresh interpreter."""
    code = (
        "import concurrent.futures, sys\n"
        "import sqlglot\n"
        "assert not [m for m in sys.modules if m.startswith('sqlspec')]\n"
        "QUERIES = {'pgvector': 'SELECT a <=> b FROM t', 'paradedb': \"SELECT * FROM t WHERE body @@@ 'shoes'\",\n"
        "           'spanner': 'SELECT 1', 'spangres': 'SELECT 1'}\n"
        "def run(name):\n"
        "    return sqlglot.parse_one(QUERIES[name], dialect=name).sql(dialect=name)\n"
        "with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:\n"
        "    results = list(pool.map(run, list(QUERIES) * 8))\n"
        "assert len(results) == 32 and all(results)\n"
        "print('ok')\n"
    )
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=False)
    assert result.returncode == 0, result.stderr
    assert "ok" in result.stdout
