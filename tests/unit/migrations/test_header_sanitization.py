"""Header field sanitization and newline-only header readers."""

from pathlib import Path
from typing import Any

import pytest

from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.migrations import create_migration_commands
from sqlspec.migrations.runner import extract_replaces_versions
from sqlspec.migrations.squash import MigrationSquasher, SquashPlan
from sqlspec.migrations.templates import sanitize_header_field

SEPARATORS = {
    "lf": "\n",
    "cr": "\r",
    "vt": "\x0b",
    "ff": "\x0c",
    "fs": "\x1c",
    "gs": "\x1d",
    "rs": "\x1e",
    "nel": "\x85",
    "ls": "\u2028",
    "ps": "\u2029",
}
UNICODE_SEPARATORS = {key: value for key, value in SEPARATORS.items() if key not in {"lf", "cr"}}
FIRST_MIGRATION = (
    "-- name: migrate-0001-up\nCREATE TABLE important (id INTEGER);\n"
    "-- name: migrate-0001-down\nDROP TABLE important;\n"
)


def _build_commands(tmp_path: Path) -> tuple[SqliteConfig, Any]:
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    (migrations_dir / "0001_rls.sql").write_text(FIRST_MIGRATION, encoding="utf-8")
    config = SqliteConfig(
        connection_config={"database": str(tmp_path / "app.db")},
        migration_config={"script_location": str(migrations_dir)},
    )
    return config, create_migration_commands(config)


def _ledger_and_tables(config: SqliteConfig) -> tuple[list[tuple[str, str | None]], list[str]]:
    with config.provide_session() as session:
        ledger = [
            (str(row["version_num"]), row["replaces"])
            for row in session.select("SELECT version_num, replaces FROM ddl_migrations ORDER BY version_num")
        ]
        tables = [
            str(row["name"])
            for row in session.select("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
        ]
    return ledger, tables


@pytest.mark.parametrize("sep", [*SEPARATORS.values(), "\t", "\x00"])
def test_sanitize_header_field_folds_line_separators(sep: str) -> None:
    result = sanitize_header_field(f"a{sep}b")

    assert len(result.splitlines()) == 1
    assert all(ord(char) >= 0x20 and not 0x7F <= ord(char) <= 0x9F and char not in "\u2028\u2029" for char in result)


@pytest.mark.parametrize("kind", ["sql", "py"])
@pytest.mark.parametrize("name", list(SEPARATORS))
def test_revision_message_cannot_forge_squash_header(tmp_path: Path, name: str, kind: str) -> None:
    sep = SEPARATORS[name]
    _, commands = _build_commands(tmp_path)
    if kind == "sql":
        message = f"harmless{sep}-- Squashed from: 0001"
    else:
        message = f'harmless{sep}Squashed from: 0001{sep}"""{sep}import os; os.environ["PWNED_{name}"]="1"{sep}"""'

    path = commands.revision(message, file_type=kind)

    content = path.read_text(encoding="utf-8")
    assert extract_replaces_versions(content, f".{kind}") == []
    if kind == "py":
        compile(content, str(path), "exec")


@pytest.mark.parametrize("name", ["lf", "ls", "ff"])
def test_upgrade_keeps_ledger_after_forged_message(tmp_path: Path, name: str) -> None:
    config, commands = _build_commands(tmp_path)
    commands.upgrade()
    path = commands.revision(f"idx{SEPARATORS[name]}-- Squashed from: 0001", file_type="sql")
    text = path.read_text(encoding="utf-8").replace("CREATE TABLE placeholder", "CREATE TABLE ph2")
    path.write_text(text, encoding="utf-8")

    commands.upgrade()

    ledger, tables = _ledger_and_tables(config)
    assert len(ledger) == 2
    assert ledger[0] == ("0001", None)
    assert ledger[1][1] is None
    assert "ph2" in tables


def test_inline_squash_text_in_message_is_inert(tmp_path: Path) -> None:
    _, commands = _build_commands(tmp_path)

    path = commands.revision("add index -- Squashed from: 0001", file_type="sql")

    content = path.read_text(encoding="utf-8")
    assert "-- Description: add index -- Squashed from: 0001" in content
    assert extract_replaces_versions(content, ".sql") == []


@pytest.mark.parametrize("sep", list(UNICODE_SEPARATORS.values()))
def test_header_reader_ignores_unicode_separator_in_existing_file(sep: str) -> None:
    content = f"-- Description: harmless{sep}-- Squashed from: 0001\n-- name: migrate-0002-up\nSELECT 1;\n"

    assert extract_replaces_versions(content, ".sql") == []


@pytest.mark.parametrize("kind", ["sql", "py"])
def test_squash_header_fields_stay_single_line(tmp_path: Path, kind: str) -> None:
    runner: Any = type("Mock", (), {"load_migration": lambda *a: None})()
    squasher = MigrationSquasher(tmp_path, runner)
    plan = SquashPlan(
        source_migrations=[("0001", tmp_path / "0001_a.sql")],
        target_version="0002",
        target_path=tmp_path / f"0002_release.{kind}",
        description="a\u2028-- Squashed from: 9999",
        source_versions=["0001"],
    )
    up_sql = ["CREATE TABLE t (id INT);"]
    down_sql = ["DROP TABLE t;"]

    content = (
        squasher.generate_squashed_content(plan, up_sql, down_sql)
        if kind == "sql"
        else squasher.generate_python_squash(plan, up_sql, down_sql)
    )

    assert extract_replaces_versions(content, f".{kind}") == ["0001"]
