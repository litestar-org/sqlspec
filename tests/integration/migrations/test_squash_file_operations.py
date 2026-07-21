"""Integration tests for migration squash file operations.

Tests for:
- Gap detection with --allow-gaps
- Extension migration squash
- Python output format
- Mixed SQL/Python squash
- Idempotency checks
"""

from pathlib import Path

import pytest

from sqlspec.exceptions import SquashValidationError
from sqlspec.migrations.context import MigrationContext
from sqlspec.migrations.runner import SyncMigrationRunner
from sqlspec.migrations.squash import MigrationSquasher


@pytest.fixture
def temp_migrations_dir(tmp_path: Path) -> Path:
    """Create temporary migrations directory."""
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    return migrations_dir


@pytest.fixture
def migration_runner(temp_migrations_dir: Path) -> SyncMigrationRunner:
    """Create a real migration runner over the temporary migration directory."""
    return SyncMigrationRunner(temp_migrations_dir, context=MigrationContext(dialect="sqlite"))


def test_gap_handling_squash_detects_gap_in_sequence(
    temp_migrations_dir: Path, migration_runner: SyncMigrationRunner
) -> None:
    """Test that squash raises error when gap detected."""
    (temp_migrations_dir / "0001_initial.sql").write_text("-- name: migrate-0001-up\nCREATE TABLE t1 (id INT);")
    (temp_migrations_dir / "0002_users.sql").write_text("-- name: migrate-0002-up\nCREATE TABLE t2 (id INT);")
    (temp_migrations_dir / "0004_comments.sql").write_text("-- name: migrate-0004-up\nCREATE TABLE t4 (id INT);")
    squasher = MigrationSquasher(temp_migrations_dir, migration_runner)
    with pytest.raises(SquashValidationError, match=r"Gap detected.*0002.*0004"):
        squasher.plan_squash("0001", "0004", "release")


def test_gap_handling_squash_allows_gap_with_flag(
    temp_migrations_dir: Path, migration_runner: SyncMigrationRunner
) -> None:
    """Test that squash succeeds with --allow-gaps flag."""
    (temp_migrations_dir / "0001_initial.sql").write_text("-- name: migrate-0001-up\nCREATE TABLE t1 (id INT);")
    (temp_migrations_dir / "0002_users.sql").write_text("-- name: migrate-0002-up\nCREATE TABLE t2 (id INT);")
    (temp_migrations_dir / "0004_comments.sql").write_text("-- name: migrate-0004-up\nCREATE TABLE t4 (id INT);")
    squasher = MigrationSquasher(temp_migrations_dir, migration_runner)
    plans = squasher.plan_squash("0001", "0004", "release", allow_gaps=True)
    assert len(plans) == 1
    assert len(plans[0].source_migrations) == 3


def test_gap_handling_squash_succeeds_without_gap(
    temp_migrations_dir: Path, migration_runner: SyncMigrationRunner
) -> None:
    """Test squash succeeds with continuous sequence."""
    (temp_migrations_dir / "0001_initial.sql").write_text("-- name: migrate-0001-up\nCREATE TABLE t1 (id INT);")
    (temp_migrations_dir / "0002_users.sql").write_text("-- name: migrate-0002-up\nCREATE TABLE t2 (id INT);")
    squasher = MigrationSquasher(temp_migrations_dir, migration_runner)
    plans = squasher.plan_squash("0001", "0002", "release")
    assert len(plans) == 1
    assert len(plans[0].source_migrations) == 2


def test_extension_migrations_squash_extension_migrations_preserves_prefix(
    temp_migrations_dir: Path,
) -> None:
    """Test that extension migrations are handled correctly."""
    from sqlspec.migrations.validation import validate_extension_consistency

    migrations: list[tuple[str, Path]] = [
        ("ext_litestar_0001", temp_migrations_dir / "ext_litestar_0001_init.sql"),
        ("ext_litestar_0002", temp_migrations_dir / "ext_litestar_0002_tables.sql"),
    ]
    for version, path in migrations:
        path.write_text(f"-- {version}\nCREATE TABLE ext_{version} (id INT);")
    validate_extension_consistency(migrations)


def test_extension_migrations_squash_rejects_mixed_extensions(temp_migrations_dir: Path) -> None:
    """Test that mixing different extensions is rejected."""
    from sqlspec.migrations.validation import validate_extension_consistency

    migrations: list[tuple[str, Path]] = [
        ("ext_litestar_0001", temp_migrations_dir / "ext_litestar_0001_init.sql"),
        ("ext_adk_0001", temp_migrations_dir / "ext_adk_0001_init.sql"),
    ]
    with pytest.raises(SquashValidationError, match="different extensions"):
        validate_extension_consistency(migrations)


def test_extension_migrations_squash_rejects_mixed_core_and_extension(temp_migrations_dir: Path) -> None:
    """Test that mixing core and extension migrations is rejected."""
    from sqlspec.migrations.validation import validate_extension_consistency

    migrations: list[tuple[str, Path]] = [
        ("0001", temp_migrations_dir / "0001_initial.sql"),
        ("ext_litestar_0001", temp_migrations_dir / "ext_litestar_0001_init.sql"),
    ]
    with pytest.raises(SquashValidationError, match=r"core.*extension"):
        validate_extension_consistency(migrations)


def test_python_output_format_squash_with_python_output_format(
    temp_migrations_dir: Path, migration_runner: SyncMigrationRunner
) -> None:
    """Test squashing SQL migrations into Python format."""
    (temp_migrations_dir / "0001_initial.sql").write_text(
        "-- name: migrate-0001-up\nCREATE TABLE users (id INT);\n\n-- name: migrate-0001-down\nDROP TABLE users;"
    )
    (temp_migrations_dir / "0002_posts.sql").write_text(
        "-- name: migrate-0002-up\nCREATE TABLE posts (id INT);\n\n-- name: migrate-0002-down\nDROP TABLE posts;"
    )
    squasher = MigrationSquasher(temp_migrations_dir, migration_runner)
    plans = squasher.plan_squash("0001", "0002", "release", output_format="py")
    assert len(plans) == 1
    assert plans[0].target_path.suffix == ".py"
    assert plans[0].target_path.name == "0001_release.py"


def test_python_output_format_squash_apply_python_output(
    temp_migrations_dir: Path, migration_runner: SyncMigrationRunner
) -> None:
    """Test that applying squash with Python format creates valid Python file."""
    (temp_migrations_dir / "0001_initial.sql").write_text(
        "-- name: migrate-0001-up\nCREATE TABLE t1 (id INT);\n\n-- name: migrate-0001-down\nDROP TABLE t1;"
    )
    squasher = MigrationSquasher(temp_migrations_dir, migration_runner)
    plans = squasher.plan_squash("0001", "0001", "release", output_format="py")
    squasher.apply_squash(plans)
    target_path = temp_migrations_dir / "0001_release.py"
    assert target_path.exists()
    content = target_path.read_text()
    compile(content, str(target_path), "exec")
    assert "def up() -> list[str]:" in content
    assert "def down() -> list[str] | None:" in content
    assert not (temp_migrations_dir / "0001_initial.sql").exists()


def test_mixed_sql_python_squash_mixed_creates_multiple_files(
    temp_migrations_dir: Path, migration_runner: SyncMigrationRunner
) -> None:
    """Test that mixed migrations create separate output files."""
    (temp_migrations_dir / "0001_initial.sql").write_text("-- name: migrate-0001-up\nCREATE TABLE t1 (id INT);")
    (temp_migrations_dir / "0002_users.sql").write_text("-- name: migrate-0002-up\nCREATE TABLE t2 (id INT);")
    (temp_migrations_dir / "0003_auth.py").write_text(
        'def up(): return ["CREATE TABLE t3 (id INT);"]\ndef down(): return None'
    )
    squasher = MigrationSquasher(temp_migrations_dir, migration_runner)
    plans = squasher.plan_squash("0001", "0003", "release")
    assert len(plans) == 2
    assert plans[0].target_path.suffix == ".sql"
    assert plans[1].target_path.suffix == ".py"
    assert len(plans[0].source_migrations) == 2
    assert len(plans[1].source_migrations) == 1


def test_idempotency_idempotency_ready_state(temp_migrations_dir: Path) -> None:
    """Test idempotency check returns 'ready' when sources exist and target doesn't."""
    from sqlspec.migrations.validation import validate_squash_idempotency

    (temp_migrations_dir / "0001_initial.sql").write_text("-- migration")
    (temp_migrations_dir / "0002_users.sql").write_text("-- migration")
    source_files = [temp_migrations_dir / "0001_initial.sql", temp_migrations_dir / "0002_users.sql"]
    target_file = temp_migrations_dir / "0001_release.sql"
    status = validate_squash_idempotency(source_files, target_file)
    assert status == "ready"


def test_idempotency_idempotency_already_squashed_state(temp_migrations_dir: Path) -> None:
    """Test idempotency check returns 'already_squashed' when only target exists."""
    from sqlspec.migrations.validation import validate_squash_idempotency

    (temp_migrations_dir / "0001_release.sql").write_text("-- squashed migration")
    source_files = [temp_migrations_dir / "0001_initial.sql", temp_migrations_dir / "0002_users.sql"]
    target_file = temp_migrations_dir / "0001_release.sql"
    status = validate_squash_idempotency(source_files, target_file)
    assert status == "already_squashed"


def test_idempotency_idempotency_partial_state(temp_migrations_dir: Path) -> None:
    """Test idempotency check returns 'partial' when both target and some sources exist."""
    from sqlspec.migrations.validation import validate_squash_idempotency

    (temp_migrations_dir / "0001_initial.sql").write_text("-- migration")
    (temp_migrations_dir / "0001_release.sql").write_text("-- squashed migration")
    source_files = [temp_migrations_dir / "0001_initial.sql", temp_migrations_dir / "0002_users.sql"]
    target_file = temp_migrations_dir / "0001_release.sql"
    status = validate_squash_idempotency(source_files, target_file)
    assert status == "partial"
