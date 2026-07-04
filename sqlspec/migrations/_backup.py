"""Private migration backup helpers."""

import shutil
from datetime import datetime, timezone
from pathlib import Path

__all__ = ("create_migration_backup", "remove_migration_backup", "restore_migration_backup")


def create_migration_backup(migrations_path: Path) -> Path:
    timestamp = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
    backup_dir = migrations_path / f".backup_{timestamp}"
    backup_dir.mkdir(parents=True, exist_ok=False)

    for file_path in migrations_path.iterdir():
        if file_path.is_file() and not file_path.name.startswith("."):
            shutil.copy2(file_path, backup_dir / file_path.name)

    return backup_dir


def remove_migration_backup(backup_path: Path) -> None:
    shutil.rmtree(backup_path)


def restore_migration_backup(migrations_path: Path, backup_path: Path, *, remove_backup: bool = False) -> None:
    for file_path in migrations_path.iterdir():
        if file_path.is_file() and not file_path.name.startswith("."):
            file_path.unlink()

    for backup_file in backup_path.iterdir():
        if backup_file.is_file():
            shutil.copy2(backup_file, migrations_path / backup_file.name)

    if remove_backup:
        remove_migration_backup(backup_path)
