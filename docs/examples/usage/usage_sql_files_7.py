from pathlib import Path

from docs.examples.usage.usage_sql_files_1 import create_loader


def test_local_files_loading(tmp_path: Path) -> None:
    loader, _queries = create_loader(tmp_path)

    # start-example
    from pathlib import Path

    # Load from Path object
    loader.load_sql(Path(tmp_path / "sql/queries/users.sql"))

    # Load from string path
    loader.load_sql(tmp_path / "sql/queries/users.sql")

    # Load directory
    loader.load_sql(tmp_path / "sql/")
    # end-example
    # Dummy asserts for doc example
    assert hasattr(loader, "load_sql")
