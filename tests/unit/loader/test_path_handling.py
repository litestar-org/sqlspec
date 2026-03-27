from pathlib import Path

from sqlspec.loader import SQLFileLoader


def test_load_specific_file_in_nested_dir(tmp_path: Path) -> None:
    """Test loading a specific file in a nested directory.

    It should only load the specified file, not everything in the directory.
    """
    nested_dir = tmp_path / "nested" / "dir"
    nested_dir.mkdir(parents=True)

    file1 = nested_dir / "file1.sql"
    file1.write_text("-- name: query1\nSELECT 1;")

    file2 = nested_dir / "file2.sql"
    file2.write_text("-- name: query2\nSELECT 2;")

    loader = SQLFileLoader()
    loader.load_sql(file1)

    assert loader.has_query("query1")
    assert not loader.has_query("query2")
    assert loader.list_files() == [str(file1)]


def test_load_specific_file_is_not_namespaced(tmp_path: Path) -> None:
    """Test that a specific file loaded directly is NOT namespaced by its directory.

    This matches current behavior where _load_single_file(path, None) is called.
    """
    nested_dir = tmp_path / "nested" / "dir"
    nested_dir.mkdir(parents=True)

    file1 = nested_dir / "file1.sql"
    file1.write_text("-- name: query1\nSELECT 1;")

    loader = SQLFileLoader()
    loader.load_sql(file1)

    # It should be "query1", not "nested.dir.query1"
    assert loader.has_query("query1")
    assert not loader.has_query("nested.dir.query1")


def test_load_directory_is_namespaced(tmp_path: Path) -> None:
    """Test that loading a directory IS namespaced."""
    nested_dir = tmp_path / "nested" / "dir"
    nested_dir.mkdir(parents=True)

    file1 = nested_dir / "file1.sql"
    file1.write_text("-- name: query1\nSELECT 1;")

    loader = SQLFileLoader()
    # Loading the base tmp_path should result in namespacing
    loader.load_sql(tmp_path)

    assert loader.has_query("nested.dir.query1")
