from pathlib import Path

from docs.examples.usage.usage_sql_files_1 import create_loader

__all__ = ("test_debugging_loaded_queries", )


def test_debugging_loaded_queries(tmp_path: Path) -> None:
    loader, _queries = create_loader(tmp_path)
    # start-example
    # Print query SQL
    query = loader.get_sql("create_user")
    print(f"SQL: {query}")
    print(f"Parameters: {query.parameters}")

    # Inspect file metadata
    file_info = loader.get_file_for_query("create_user")
    print(f"Loaded from: {file_info.path}")
    # end-example
    # Dummy asserts for doc example
    assert hasattr(loader, "get_sql")
    assert query is not None
