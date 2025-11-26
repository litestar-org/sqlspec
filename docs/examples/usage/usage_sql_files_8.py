from pathlib import Path

from docs.examples.usage.usage_sql_files_1 import create_loader


def test_file_uris_loading(tmp_path: Path) -> None:
    loader, _ = create_loader(tmp_path)
    # copy the sql directory to an absolute path for testing
    absolute_sql_path = tmp_path / "absolute_sql"
    absolute_sql_path.mkdir()
    absolute_sql_file = absolute_sql_path / "queries.sql"
    absolute_sql_file.write_text("""
    -- name: another_query
    SELECT 1;
    """)
    # start-example
    # Load from file:// URI
    # change the path below to an absolute path on your system
    loader.load_sql(f"file://{absolute_sql_file.resolve()}")

    # Load from relative file URI
    loader.load_sql(f"file://{absolute_sql_path}/queries.sql")
    # end-example
    # Dummy asserts for doc example
    assert hasattr(loader, "load_sql")
