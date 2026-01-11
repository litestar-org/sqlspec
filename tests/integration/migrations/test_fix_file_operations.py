"""Integration tests for migration fix file operations."""

from pathlib import Path

import pytest

from sqlspec.migrations.fix import MigrationFixer, MigrationRename


@pytest.fixture
def temp_migrations_dir(tmp_path: Path) -> Path:
    """Create temporary migrations directory."""
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    return migrations_dir


@pytest.fixture
def sample_sql_migration(temp_migrations_dir: Path) -> Path:
    """Create sample SQL migration file."""
    content = """-- name: migrate-20251011120000-up
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);

-- name: migrate-20251011120000-down
DROP TABLE users;
"""
    file_path = temp_migrations_dir / "20251011120000_create_users.sql"
    file_path.write_text(content)
    return file_path


@pytest.fixture
def sample_py_migration(temp_migrations_dir: Path) -> Path:
    """Create sample Python migration file."""
    content = '''"""Create products table migration."""

async def up(driver):
    """Apply migration."""
    await driver.execute("CREATE TABLE products (id INTEGER PRIMARY KEY)")

async def down(driver):
    """Revert migration."""
    await driver.execute("DROP TABLE products")
'''
    file_path = temp_migrations_dir / "20251012130000_create_products.py"
    file_path.write_text(content)
    return file_path


def test_plan_renames_empty_map(temp_migrations_dir: Path) -> None:
    """Test planning renames with empty conversion map."""
    fixer = MigrationFixer(temp_migrations_dir)
    renames = fixer.plan_renames({})
    assert renames == []


def test_plan_renames_no_matching_files(temp_migrations_dir: Path) -> None:
    """Test planning renames when no files match."""
    fixer = MigrationFixer(temp_migrations_dir)
    conversion_map = {"20251011120000": "0001"}
    renames = fixer.plan_renames(conversion_map)
    assert renames == []


def test_plan_renames_single_sql_file(temp_migrations_dir: Path, sample_sql_migration: Path) -> None:
    """Test planning rename for single SQL migration."""
    fixer = MigrationFixer(temp_migrations_dir)
    conversion_map = {"20251011120000": "0001"}

    renames = fixer.plan_renames(conversion_map)

    assert len(renames) == 1
    rename = renames[0]
    assert rename.old_version == "20251011120000"
    assert rename.new_version == "0001"
    assert rename.old_path == sample_sql_migration
    assert rename.new_path == temp_migrations_dir / "0001_create_users.sql"
    assert rename.needs_content_update is True


def test_plan_renames_single_py_file(temp_migrations_dir: Path, sample_py_migration: Path) -> None:
    """Test planning rename for single Python migration."""
    fixer = MigrationFixer(temp_migrations_dir)
    conversion_map = {"20251012130000": "0001"}

    renames = fixer.plan_renames(conversion_map)

    assert len(renames) == 1
    rename = renames[0]
    assert rename.old_version == "20251012130000"
    assert rename.new_version == "0001"
    assert rename.old_path == sample_py_migration
    assert rename.new_path == temp_migrations_dir / "0001_create_products.py"
    assert rename.needs_content_update is False


def test_plan_renames_multiple_files(
    temp_migrations_dir: Path, sample_sql_migration: Path, sample_py_migration: Path
) -> None:
    """Test planning renames for multiple migrations."""
    fixer = MigrationFixer(temp_migrations_dir)
    conversion_map = {"20251011120000": "0001", "20251012130000": "0002"}

    renames = fixer.plan_renames(conversion_map)

    assert len(renames) == 2

    sql_rename = next(r for r in renames if r.old_path == sample_sql_migration)
    assert sql_rename.new_version == "0001"
    assert sql_rename.needs_content_update is True

    py_rename = next(r for r in renames if r.old_path == sample_py_migration)
    assert py_rename.new_version == "0002"
    assert py_rename.needs_content_update is False


def test_plan_renames_detects_collision(temp_migrations_dir: Path, sample_sql_migration: Path) -> None:
    """Test planning renames detects target file collision."""
    existing_target = temp_migrations_dir / "0001_create_users.sql"
    existing_target.write_text("EXISTING FILE")

    fixer = MigrationFixer(temp_migrations_dir)
    conversion_map = {"20251011120000": "0001"}

    with pytest.raises(ValueError, match="Target file already exists"):
        fixer.plan_renames(conversion_map)


def test_create_backup(temp_migrations_dir: Path, sample_sql_migration: Path, sample_py_migration: Path) -> None:
    """Test backup creation."""
    fixer = MigrationFixer(temp_migrations_dir)

    backup_path = fixer.create_backup()

    assert backup_path.exists()
    assert backup_path.is_dir()
    assert backup_path.name.startswith(".backup_")

    backed_up_files = list(backup_path.iterdir())
    assert len(backed_up_files) == 2

    backup_names = {f.name for f in backed_up_files}
    assert "20251011120000_create_users.sql" in backup_names
    assert "20251012130000_create_products.py" in backup_names


def test_create_backup_only_copies_files(temp_migrations_dir: Path, sample_sql_migration: Path) -> None:
    """Test backup only copies files, not subdirectories."""
    subdir = temp_migrations_dir / "subdir"
    subdir.mkdir()
    (subdir / "file.txt").write_text("test")

    fixer = MigrationFixer(temp_migrations_dir)
    backup_path = fixer.create_backup()

    backed_up_files = list(backup_path.iterdir())
    assert len(backed_up_files) == 1
    assert backed_up_files[0].name == "20251011120000_create_users.sql"


def test_create_backup_ignores_hidden_files(temp_migrations_dir: Path) -> None:
    """Test backup ignores hidden files."""
    hidden_file = temp_migrations_dir / ".hidden"
    hidden_file.write_text("hidden content")

    visible_file = temp_migrations_dir / "20251011120000_create_users.sql"
    visible_file.write_text("visible content")

    fixer = MigrationFixer(temp_migrations_dir)
    backup_path = fixer.create_backup()

    backed_up_files = list(backup_path.iterdir())
    assert len(backed_up_files) == 1
    assert backed_up_files[0].name == "20251011120000_create_users.sql"


def test_apply_renames_dry_run(temp_migrations_dir: Path, sample_sql_migration: Path) -> None:
    """Test dry-run mode doesn't modify files."""
    original_content = sample_sql_migration.read_text()

    fixer = MigrationFixer(temp_migrations_dir)
    renames = [
        MigrationRename(
            old_path=sample_sql_migration,
            new_path=temp_migrations_dir / "0001_create_users.sql",
            old_version="20251011120000",
            new_version="0001",
            needs_content_update=True,
        )
    ]

    fixer.apply_renames(renames, dry_run=True)

    assert sample_sql_migration.exists()
    assert not (temp_migrations_dir / "0001_create_users.sql").exists()
    assert sample_sql_migration.read_text() == original_content


def test_apply_renames_actual(temp_migrations_dir: Path, sample_sql_migration: Path) -> None:
    """Test actual rename execution."""
    fixer = MigrationFixer(temp_migrations_dir)
    new_path = temp_migrations_dir / "0001_create_users.sql"

    renames = [
        MigrationRename(
            old_path=sample_sql_migration,
            new_path=new_path,
            old_version="20251011120000",
            new_version="0001",
            needs_content_update=True,
        )
    ]

    fixer.apply_renames(renames)

    assert not sample_sql_migration.exists()
    assert new_path.exists()


def test_apply_renames_empty_list(temp_migrations_dir: Path) -> None:
    """Test applying empty renames list."""
    fixer = MigrationFixer(temp_migrations_dir)
    fixer.apply_renames([])


def test_update_file_content_sql_up_and_down(temp_migrations_dir: Path, sample_sql_migration: Path) -> None:
    """Test updating SQL file content updates query names."""
    fixer = MigrationFixer(temp_migrations_dir)

    fixer.update_file_content(sample_sql_migration, "20251011120000", "0001")

    updated_content = sample_sql_migration.read_text()
    assert "-- name: migrate-0001-up" in updated_content
    assert "-- name: migrate-0001-down" in updated_content
    assert "migrate-20251011120000-up" not in updated_content
    assert "migrate-20251011120000-down" not in updated_content


def test_update_file_content_preserves_sql_statements(temp_migrations_dir: Path, sample_sql_migration: Path) -> None:
    """Test updating content preserves actual SQL statements."""
    fixer = MigrationFixer(temp_migrations_dir)

    fixer.update_file_content(sample_sql_migration, "20251011120000", "0001")

    updated_content = sample_sql_migration.read_text()
    assert "CREATE TABLE users" in updated_content
    assert "DROP TABLE users" in updated_content


def test_update_file_content_no_query_names(temp_migrations_dir: Path) -> None:
    """Test updating file without query names is a no-op."""
    file_path = temp_migrations_dir / "20251011120000_simple.sql"
    original_content = "CREATE TABLE test (id INTEGER);"
    file_path.write_text(original_content)

    fixer = MigrationFixer(temp_migrations_dir)
    fixer.update_file_content(file_path, "20251011120000", "0001")

    assert file_path.read_text() == original_content


def test_rollback_restores_files(temp_migrations_dir: Path, sample_sql_migration: Path) -> None:
    """Test rollback restores files from backup."""
    original_content = sample_sql_migration.read_text()

    fixer = MigrationFixer(temp_migrations_dir)
    fixer.create_backup()

    sample_sql_migration.unlink()
    modified_file = temp_migrations_dir / "0001_create_users.sql"
    modified_file.write_text("MODIFIED CONTENT")

    fixer.rollback()

    assert sample_sql_migration.exists()
    assert sample_sql_migration.read_text() == original_content
    assert not modified_file.exists()


def test_rollback_without_backup(temp_migrations_dir: Path) -> None:
    """Test rollback without backup is a no-op."""
    fixer = MigrationFixer(temp_migrations_dir)
    fixer.rollback()


def test_cleanup_removes_backup(temp_migrations_dir: Path, sample_sql_migration: Path) -> None:
    """Test cleanup removes backup directory."""
    fixer = MigrationFixer(temp_migrations_dir)
    backup_path = fixer.create_backup()

    assert backup_path.exists()

    fixer.cleanup()

    assert not backup_path.exists()
    assert fixer.backup_path is None


def test_cleanup_without_backup(temp_migrations_dir: Path) -> None:
    """Test cleanup without backup is a no-op."""
    fixer = MigrationFixer(temp_migrations_dir)
    fixer.cleanup()


def test_full_conversion_workflow(temp_migrations_dir: Path) -> None:
    """Test complete conversion workflow with rollback on error."""
    sql_file = temp_migrations_dir / "20251011120000_create_users.sql"
    sql_file.write_text("""-- name: migrate-20251011120000-up
CREATE TABLE users (id INTEGER);

-- name: migrate-20251011120000-down
DROP TABLE users;
""")

    py_file = temp_migrations_dir / "20251012130000_create_products.py"
    py_file.write_text('"""Migration."""\n\nasync def up(driver):\n    pass\n')

    fixer = MigrationFixer(temp_migrations_dir)
    conversion_map = {"20251011120000": "0001", "20251012130000": "0002"}

    backup_path = fixer.create_backup()
    renames = fixer.plan_renames(conversion_map)

    assert len(renames) == 2

    try:
        fixer.apply_renames(renames)
        fixer.cleanup()

        assert (temp_migrations_dir / "0001_create_users.sql").exists()
        assert (temp_migrations_dir / "0002_create_products.py").exists()
        assert not sql_file.exists()
        assert not py_file.exists()
        assert not backup_path.exists()

        converted_content = (temp_migrations_dir / "0001_create_users.sql").read_text()
        assert "migrate-0001-up" in converted_content
        assert "migrate-0001-down" in converted_content

    except Exception:
        fixer.rollback()
        raise


def test_extension_migration_rename(temp_migrations_dir: Path) -> None:
    """Test renaming extension migrations preserves prefix."""
    ext_file = temp_migrations_dir / "ext_litestar_20251011215440_create_sessions.py"
    ext_file.write_text('"""Extension migration."""\n')

    fixer = MigrationFixer(temp_migrations_dir)
    conversion_map = {"ext_litestar_20251011215440": "ext_litestar_0001"}

    renames = fixer.plan_renames(conversion_map)

    assert len(renames) == 1
    assert renames[0].new_path.name == "ext_litestar_0001_create_sessions.py"


def test_multiple_sql_files_same_version(temp_migrations_dir: Path) -> None:
    """Test handling multiple files with same version prefix."""
    file1 = temp_migrations_dir / "20251011120000_users.sql"
    file2 = temp_migrations_dir / "20251011120000_products.sql"
    file1.write_text("CREATE TABLE users (id INTEGER);")
    file2.write_text("CREATE TABLE products (id INTEGER);")

    fixer = MigrationFixer(temp_migrations_dir)
    conversion_map = {"20251011120000": "0001"}

    renames = fixer.plan_renames(conversion_map)

    assert len(renames) == 2
    new_names = {r.new_path.name for r in renames}
    assert new_names == {"0001_users.sql", "0001_products.sql"}
