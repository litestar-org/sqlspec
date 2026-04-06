from pathlib import Path

from sqlspec.loader import SQLFileLoader
from sqlspec.storage.registry import StorageRegistry


def test_load_file_with_alias_is_not_double_dir_stripped(tmp_path: Path) -> None:
    """Test loading a file through an alias to verify path handling."""
    sql_file = tmp_path / "my_query.sql"
    sql_file.write_text("-- name: query1\nSELECT 1;")

    registry = StorageRegistry()
    # Register an alias to the parent directory
    registry.register_alias("my_store", f"file://{tmp_path}")

    loader = SQLFileLoader(storage_registry=registry)
    # The storage backend for 'my_store/my_query.sql' will be scoped to 'file://tmp_path'
    loader.load_sql("my_store/my_query.sql")

    assert loader.has_query("query1")


def test_load_file_from_alias_nested(tmp_path: Path) -> None:
    """Test loading a nested file through an alias."""
    nested_dir = tmp_path / "nested"
    nested_dir.mkdir()
    sql_file = nested_dir / "my_query.sql"
    sql_file.write_text("-- name: query2\nSELECT 2;")

    registry = StorageRegistry()
    registry.register_alias("my_store", f"file://{tmp_path}")

    loader = SQLFileLoader(storage_registry=registry)
    # 'my_store/nested/my_query.sql'
    loader.load_sql("my_store/nested/my_query.sql")

    assert loader.has_query("query2")
