"""Checksum gate on upgrade and downgrade, legacy checksum upgrade, and the verify_checksums switch."""

import hashlib
import inspect
import re
import sqlite3
from collections.abc import AsyncIterator, Callable
from pathlib import Path
from typing import Any

import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.exceptions import MigrationError
from sqlspec.migrations.commands import AsyncMigrationCommands, SyncMigrationCommands
from sqlspec.migrations.runner import SyncMigrationRunner

pytestmark = pytest.mark.anyio

USERS_PY = '"""Users."""\nimport os\n\ndef up(context=None):\n    return ["CREATE TABLE users(id int)"]\n'
USERS_PY_TAMPERED = USERS_PY.replace("import os\n\n", 'import os\nversion: str = __import__("os").getcwd()\n\n', 1)
ORDERS_SQL = "-- name: migrate-0002-up\nCREATE TABLE orders(id int);\n"
ORDERS_SQL_TAMPERED = (
    ORDERS_SQL
    + "-- name: migrate-0002_down\nDROP TABLE orders;\nDROP TABLE users;\n"
    + "CREATE TABLE tampered AS SELECT 1 AS up;\nSELECT 1 FROM tampered WHERE 0\n-up\n"
)
DOWN_SQL = (
    "-- name: migrate-0003-up\nCREATE TABLE a (id INT);\nCREATE TABLE b (id INT);\n"
    "-- name: migrate-0003-down\nDROP TABLE a;\n"
)
DOWN_SQL_MOVED = (
    "-- name: migrate-0003-up\nCREATE TABLE a (id INT);\n"
    "-- name: migrate-0003-down\nCREATE TABLE b (id INT);\nDROP TABLE a;\n"
)
LEGACY_PATTERN = r"^(?:--\s*name:\s*migrate-[^-]+-(?:up|down)|--\s*Version:\s*.*|Version:\s*.*)\s*$"

CommandsFactory = Callable[..., Any]


async def _call(commands: Any, name: str, *args: Any, **kwargs: Any) -> Any:
    result = getattr(commands, name)(*args, **kwargs)
    if inspect.isawaitable(result):
        return await result
    return result


@pytest.fixture
async def make_commands(tmp_path: Path) -> "AsyncIterator[CommandsFactory]":
    configs: list[AiosqliteConfig] = []

    def factory(mode: str, **migration_config: Any) -> Any:
        config_kwargs: dict[str, Any] = {
            "connection_config": {"database": str(tmp_path / f"{mode}.db")},
            "migration_config": {"script_location": str(tmp_path / "migrations"), "enabled": True, **migration_config},
        }
        if mode == "sync":
            return SyncMigrationCommands(SqliteConfig(**config_kwargs))
        config = AiosqliteConfig(**config_kwargs)
        configs.append(config)
        return AsyncMigrationCommands(config)

    yield factory
    for config in configs:
        await config.close_pool()


def _migrations_dir(tmp_path: Path) -> Path:
    path = tmp_path / "migrations"
    path.mkdir(exist_ok=True)
    return path


def _db(tmp_path: Path, mode: str) -> Path:
    return tmp_path / f"{mode}.db"


def _tables(db_path: Path) -> list[str]:
    with sqlite3.connect(db_path) as connection:
        rows = connection.execute("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name").fetchall()
    return [row[0] for row in rows]


def _ledger(db_path: Path) -> list[tuple[str, str]]:
    with sqlite3.connect(db_path) as connection:
        return connection.execute("SELECT version_num, checksum FROM ddl_migrations ORDER BY version_num").fetchall()


def _set_checksum(db_path: Path, version: str, checksum: str) -> None:
    with sqlite3.connect(db_path) as connection:
        connection.execute("UPDATE ddl_migrations SET checksum = ? WHERE version_num = ?", (checksum, version))


def _legacy_text(content: str) -> str:
    return re.sub(LEGACY_PATTERN, "", content, flags=re.MULTILINE | re.IGNORECASE)


def _marker_digest(tmp_path: Path, content: str) -> str:
    return SyncMigrationRunner(tmp_path).calculate_checksum(content)


async def _applied_pair(tmp_path: Path, make_commands: CommandsFactory, mode: str, **migration_config: Any) -> Any:
    directory = _migrations_dir(tmp_path)
    (directory / "0001_users.py").write_text(USERS_PY, encoding="utf-8")
    (directory / "0002_orders.sql").write_text(ORDERS_SQL, encoding="utf-8")
    commands = make_commands(mode, **migration_config)
    await _call(commands, "upgrade", echo=False)
    return commands


@pytest.mark.parametrize("mode", ["sync", "async"])
async def test_upgrade_refuses_tampered_applied_file(tmp_path: Path, make_commands: CommandsFactory, mode: str) -> None:
    commands = await _applied_pair(tmp_path, make_commands, mode)
    db_path = _db(tmp_path, mode)
    ledger_before = _ledger(db_path)
    directory = tmp_path / "migrations"
    (directory / "0001_users.py").write_text(USERS_PY_TAMPERED, encoding="utf-8")
    (directory / "0003_extra.sql").write_text(
        "-- name: migrate-0003-up\nCREATE TABLE extra(id int);\n", encoding="utf-8"
    )

    with pytest.raises(MigrationError, match="0001"):
        await _call(commands, "upgrade", echo=False)

    assert "extra" not in _tables(db_path)
    assert _ledger(db_path) == ledger_before


async def test_upgrade_dry_run_reports_tampered_file(tmp_path: Path, make_commands: CommandsFactory) -> None:
    commands = await _applied_pair(tmp_path, make_commands, "sync")
    (tmp_path / "migrations" / "0001_users.py").write_text(USERS_PY_TAMPERED, encoding="utf-8")

    with pytest.raises(MigrationError, match="0001"):
        await _call(commands, "upgrade", dry_run=True, echo=False)


@pytest.mark.parametrize("mode", ["sync", "async"])
async def test_downgrade_refuses_tampered_down_block(tmp_path: Path, make_commands: CommandsFactory, mode: str) -> None:
    commands = await _applied_pair(tmp_path, make_commands, mode)
    db_path = _db(tmp_path, mode)
    ledger_before = _ledger(db_path)
    (tmp_path / "migrations" / "0002_orders.sql").write_text(ORDERS_SQL_TAMPERED, encoding="utf-8")

    with pytest.raises(MigrationError, match="0002"):
        await _call(commands, "downgrade", "base", echo=False)

    assert _tables(db_path) == ["ddl_migrations", "orders", "users"]
    assert _ledger(db_path) == ledger_before


@pytest.mark.parametrize("mode", ["sync", "async"])
async def test_downgrade_refuses_moved_down_header(tmp_path: Path, make_commands: CommandsFactory, mode: str) -> None:
    directory = _migrations_dir(tmp_path)
    (directory / "0003_split.sql").write_text(DOWN_SQL, encoding="utf-8")
    commands = make_commands(mode)
    await _call(commands, "upgrade", echo=False)
    (directory / "0003_split.sql").write_text(DOWN_SQL_MOVED, encoding="utf-8")

    with pytest.raises(MigrationError, match="0003"):
        await _call(commands, "downgrade", echo=False)

    assert _tables(_db(tmp_path, mode)) == ["a", "b", "ddl_migrations"]


@pytest.mark.parametrize("tamper", ["python", "sql"])
async def test_verify_strict_fails_on_tampered_files(
    tmp_path: Path, make_commands: CommandsFactory, tamper: str
) -> None:
    commands = await _applied_pair(tmp_path, make_commands, "sync")
    directory = tmp_path / "migrations"
    if tamper == "python":
        (directory / "0001_users.py").write_text(USERS_PY_TAMPERED, encoding="utf-8")
    else:
        (directory / "0002_orders.sql").write_text(ORDERS_SQL_TAMPERED, encoding="utf-8")

    with pytest.raises(MigrationError, match="checksum mismatch"):
        await _call(commands, "verify", strict=True, echo=False)


@pytest.mark.parametrize("command", ["upgrade", "verify"])
@pytest.mark.parametrize("digest", ["sha256", "md5"])
@pytest.mark.parametrize("mode", ["sync", "async"])
async def test_verified_legacy_checksum_upgraded_to_marker_digest(
    tmp_path: Path, make_commands: CommandsFactory, mode: str, digest: str, command: str
) -> None:
    directory = _migrations_dir(tmp_path)
    original = (
        "-- name: migrate-0001-up\nCREATE TABLE a (id INT);\nCREATE TABLE b (id INT);\n"
        "-- name: migrate-0001-down\nDROP TABLE a;\n"
    )
    path = directory / "0001_split.sql"
    path.write_text(original, encoding="utf-8")
    commands = make_commands(mode)
    await _call(commands, "upgrade", echo=False)
    db_path = _db(tmp_path, mode)
    legacy = _legacy_text(original).encode("utf-8")
    legacy_digest = (
        hashlib.sha256(legacy).hexdigest()
        if digest == "sha256"
        else hashlib.md5(legacy, usedforsecurity=False).hexdigest()
    )
    _set_checksum(db_path, "0001", legacy_digest)

    await _call(commands, command, echo=False)

    assert _ledger(db_path) == [("0001", _marker_digest(tmp_path, original))]
    path.write_text(
        "-- name: migrate-0001-up\nCREATE TABLE a (id INT);\n"
        "-- name: migrate-0001-down\nCREATE TABLE b (id INT);\nDROP TABLE a;\n",
        encoding="utf-8",
    )
    with pytest.raises(MigrationError, match="0001"):
        await _call(commands, "downgrade", echo=False)


async def test_legacy_checksum_rerecord_dry_run_reports_without_writing(
    tmp_path: Path, make_commands: CommandsFactory, capsys: pytest.CaptureFixture[str]
) -> None:
    directory = _migrations_dir(tmp_path)
    content = "-- name: migrate-0001-up\nCREATE TABLE a (id INT);\n-- name: migrate-0001-down\nDROP TABLE a;\n"
    (directory / "0001_a.sql").write_text(content, encoding="utf-8")
    commands = make_commands("sync")
    await _call(commands, "upgrade", echo=False)
    db_path = _db(tmp_path, "sync")
    legacy_digest = hashlib.sha256(_legacy_text(content).encode("utf-8")).hexdigest()
    _set_checksum(db_path, "0001", legacy_digest)

    await _call(commands, "upgrade", dry_run=True)

    assert "Would re-record legacy checksums for" in capsys.readouterr().out
    assert _ledger(db_path) == [("0001", legacy_digest)]


@pytest.mark.parametrize("mode", ["sync", "async"])
async def test_verify_checksums_false_allows_upgrade(tmp_path: Path, make_commands: CommandsFactory, mode: str) -> None:
    commands = await _applied_pair(tmp_path, make_commands, mode, verify_checksums=False)
    directory = tmp_path / "migrations"
    (directory / "0001_users.py").write_text(USERS_PY_TAMPERED, encoding="utf-8")
    (directory / "0003_extra.sql").write_text(
        "-- name: migrate-0003-up\nCREATE TABLE extra(id int);\n", encoding="utf-8"
    )

    await _call(commands, "upgrade", echo=False)

    assert "extra" in _tables(_db(tmp_path, mode))


async def test_upgrade_accepts_fix_renamed_files(tmp_path: Path, make_commands: CommandsFactory) -> None:
    directory = _migrations_dir(tmp_path)
    (directory / "20251011120000_users.sql").write_text(
        "-- name: migrate-20251011120000-up\nCREATE TABLE users (id INT);\n"
        "-- name: migrate-20251011120000-down\nDROP TABLE users;\n",
        encoding="utf-8",
    )
    commands = make_commands("sync")
    await _call(commands, "upgrade", echo=False)

    await _call(commands, "fix", yes=True)
    await _call(commands, "upgrade", echo=False)

    assert [version for version, _ in _ledger(_db(tmp_path, "sync"))] == ["0001"]


@pytest.mark.parametrize("command", ["upgrade", "verify"])
@pytest.mark.parametrize("md5_form", ["pre_067", "raw"])
@pytest.mark.parametrize("file_kind", ["sql", "py"])
@pytest.mark.parametrize("mode", ["sync", "async"])
async def test_md5_checksum_with_version_header_passes_and_upgrades_to_sha256(
    tmp_path: Path, make_commands: CommandsFactory, mode: str, file_kind: str, md5_form: str, command: str
) -> None:
    """Pre-v0.67.0 and raw MD5 rows with Version headers pass upgrade/verify and re-record to SHA-256."""
    directory = _migrations_dir(tmp_path)
    if file_kind == "sql":
        content = (
            "-- Version: 0001\n-- name: migrate-0001-up\nCREATE TABLE items (id INT);\n"
            "-- name: migrate-0001-down\nDROP TABLE items;\n"
        )
        file_path = directory / "0001_items.sql"
    else:
        content = (
            '"""Items.\n\nVersion: 0001\n"""\n\n\ndef up(context=None):\n    return ["CREATE TABLE items (id INT)"]\n'
        )
        file_path = directory / "0001_items.py"
    file_path.write_text(content, encoding="utf-8")
    commands = make_commands(mode)
    await _call(commands, "upgrade", echo=False)

    db_path = _db(tmp_path, mode)
    digest_input = (
        re.sub(r"^--\s*name:\s*migrate-[^-]+-(?:up|down)\s*$", "", content, flags=re.MULTILINE)
        if md5_form == "pre_067"
        else content
    )
    md5_checksum = hashlib.md5(digest_input.encode("utf-8"), usedforsecurity=False).hexdigest()
    _set_checksum(db_path, "0001", md5_checksum)

    await _call(commands, command, echo=False)

    assert _ledger(db_path) == [("0001", _marker_digest(tmp_path, content))]


@pytest.mark.parametrize("md5_form", ["pre_067", "raw"])
@pytest.mark.parametrize("mode", ["sync", "async"])
async def test_auto_sync_reconciles_renamed_timestamp_with_md5_checksum_and_upgrades_to_sha256(
    tmp_path: Path, make_commands: CommandsFactory, mode: str, md5_form: str
) -> None:
    """Reconciling a renamed timestamp migration against an MD5 checksum restores Version: and upgrades to SHA-256."""
    directory = _migrations_dir(tmp_path)
    old_content = (
        "-- Version: 20251011120000\n-- name: migrate-20251011120000-up\nCREATE TABLE items (id INT);\n"
        "-- name: migrate-20251011120000-down\nDROP TABLE items;\n"
    )
    old_path = directory / "20251011120000_items.sql"
    old_path.write_text(old_content, encoding="utf-8")
    commands = make_commands(mode)
    await _call(commands, "upgrade", echo=False)

    db_path = _db(tmp_path, mode)
    digest_input = (
        re.sub(r"^--\s*name:\s*migrate-[^-]+-(?:up|down)\s*$", "", old_content, flags=re.MULTILINE)
        if md5_form == "pre_067"
        else old_content
    )
    md5_checksum = hashlib.md5(digest_input.encode("utf-8"), usedforsecurity=False).hexdigest()
    _set_checksum(db_path, "20251011120000", md5_checksum)

    new_content = (
        "-- Version: 0001\n-- name: migrate-0001-up\nCREATE TABLE items (id INT);\n"
        "-- name: migrate-0001-down\nDROP TABLE items;\n"
    )
    old_path.unlink()
    (directory / "0001_items.sql").write_text(new_content, encoding="utf-8")

    await _call(commands, "upgrade", echo=False)

    assert _ledger(db_path) == [("0001", _marker_digest(tmp_path, new_content))]


@pytest.mark.parametrize("mode", ["sync", "async"])
async def test_fix_updates_md5_checksum_to_sha256_on_timestamp_rename(
    tmp_path: Path, make_commands: CommandsFactory, mode: str
) -> None:
    """Running fix(update_database=True) on a pre-v0.67.0 MD5 row upgrades its checksum to SHA-256."""
    directory = _migrations_dir(tmp_path)
    old_content = (
        "-- Version: 20251011120000\n-- name: migrate-20251011120000-up\nCREATE TABLE items (id INT);\n"
        "-- name: migrate-20251011120000-down\nDROP TABLE items;\n"
    )
    (directory / "20251011120000_items.sql").write_text(old_content, encoding="utf-8")
    commands = make_commands(mode)
    await _call(commands, "upgrade", echo=False)

    db_path = _db(tmp_path, mode)
    digest_input = re.sub(r"^--\s*name:\s*migrate-[^-]+-(?:up|down)\s*$", "", old_content, flags=re.MULTILINE)
    md5_checksum = hashlib.md5(digest_input.encode("utf-8"), usedforsecurity=False).hexdigest()
    _set_checksum(db_path, "20251011120000", md5_checksum)

    await _call(commands, "fix", yes=True)
    await _call(commands, "upgrade", echo=False)

    renamed_content = (directory / "0001_items.sql").read_text(encoding="utf-8")
    assert _ledger(db_path) == [("0001", _marker_digest(tmp_path, renamed_content))]
