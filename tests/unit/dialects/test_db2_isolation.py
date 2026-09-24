"""Isolation tests proving the Db2 dialect leaves shared sqlglot state untouched."""

import json
import subprocess
import sys

import sqlglot

TARGET_DIALECTS = ("", "postgres", "tsql", "oracle", "mysql", "duckdb", "sqlite", "snowflake", "bigquery")

CORPUS_CODE = f"""
import json
import sqlglot

TARGETS = {TARGET_DIALECTS!r}
CORPUS = (
    ("postgres", "SELECT STRPOS(a, 'b') FROM t"),
    ("postgres", "SELECT a + INTERVAL '1 day' FROM t"),
    ("mysql", "SELECT DATE_ADD(a, INTERVAL 1 DAY) FROM t"),
    ("postgres", "SELECT * FROM t LIMIT 5 OFFSET 2"),
    ("postgres", "SELECT CONCAT(a, b), a % b, a ILIKE 'x' FROM t"),
    ("postgres", "SELECT CAST(x AS TEXT), CAST(y AS BYTEA), CAST(z AS TIMESTAMPTZ) FROM t"),
    ("postgres", "SELECT 1"),
    ("postgres", "SELECT * FROM t FOR UPDATE SKIP LOCKED"),
    ("postgres", "SELECT CURRENT_TIMESTAMP"),
    ("postgres", "SELECT TO_CHAR(ts, 'YYYY') FROM t"),
    ("postgres", "SELECT $1, :name FROM t"),
)


def render_corpus():
    return {{
        f"{{target}}|{{source}}": sqlglot.transpile(source, read=read, write=target)
        for target in TARGETS
        for read, source in CORPUS
    }}


def render_db2():
    return [sqlglot.transpile(source, read=read, write="db2") for read, source in CORPUS]
"""


def _run_python(code: str) -> str:
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=False)
    assert result.returncode == 0, result.stderr
    return result.stdout.strip()


def test_importing_db2_leaves_other_dialects_unchanged() -> None:
    """Rendering every dialect is byte-identical before and after Db2 is imported and used."""
    code = (
        "import sqlspec\n" + CORPUS_CODE + "before = render_corpus()\n"
        "import sqlspec.dialects.db2\n"
        "render_db2()\n"
        "after = render_corpus()\n"
        "print(json.dumps(before == after))\n"
    )
    assert _run_python(code) == "true"


def test_dialects_loaded_after_db2_render_like_a_clean_interpreter() -> None:
    """Dialect generators created after the Db2 import render exactly as without it."""
    baseline_code = "import sqlspec\n" + CORPUS_CODE + "print(json.dumps(render_corpus(), sort_keys=True))\n"
    db2_first_code = (
        "import sqlspec\n"
        "import sqlspec.dialects.db2\n" + CORPUS_CODE + "render_db2()\n"
        "print(json.dumps(render_corpus(), sort_keys=True))\n"
    )
    assert json.loads(_run_python(db2_first_code)) == json.loads(_run_python(baseline_code))


def test_db2_usage_leaves_shared_generator_and_parser_tables_unchanged() -> None:
    """Parsing and rendering Db2 SQL leaves the root generator and parser tables untouched."""
    code = (
        "import json\n"
        "import sqlspec\n"
        "import sqlglot\n"
        "from sqlglot import generator, parser\n"
        "before = (dict(generator.Generator.TRANSFORMS), dict(parser.Parser.FUNCTION_PARSERS))\n"
        "import sqlspec.dialects.db2\n"
        "sqlglot.parse_one(\"SELECT POSSTR(a, 'b') FROM t\", read='db2').sql(dialect='db2')\n"
        "sqlglot.transpile('SELECT a FROM t LIMIT 1', read='postgres', write='db2')\n"
        "after = (dict(generator.Generator.TRANSFORMS), dict(parser.Parser.FUNCTION_PARSERS))\n"
        "print(json.dumps(before == after))\n"
    )
    assert _run_python(code) == "true"


def test_db2_rendering_after_other_dialect_generators_are_created() -> None:
    """Postgres and Db2 render their own string position spelling regardless of order."""
    source = "SELECT STRPOS(a, 'b') FROM t"
    first_postgres = sqlglot.transpile(source, read="postgres", write="postgres")[0]
    db2 = sqlglot.transpile(source, read="postgres", write="db2")[0]
    second_postgres = sqlglot.transpile(source, read="postgres", write="postgres")[0]

    assert first_postgres == "SELECT POSITION('b' IN a) FROM t"
    assert second_postgres == first_postgres
    assert db2 == "SELECT POSSTR(a, 'b') FROM t"
