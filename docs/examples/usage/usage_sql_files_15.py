from pathlib import Path

from docs.examples.usage.usage_sql_files_1 import create_loader
from sqlspec.exceptions import SQLFileNotFoundError

__all__ = ("test_query_not_found", )


def test_query_not_found(tmp_path: Path) -> None:
    loader, _queries = create_loader(tmp_path)
    # start-example
    try:
        loader.get_sql("nonexistent_query")
    except SQLFileNotFoundError:
        print("Query not found. Available queries:")
        print(loader.list_queries())
    # end-example
    # Dummy asserts for doc example
    assert hasattr(loader, "get_sql")
    assert hasattr(loader, "list_queries")
