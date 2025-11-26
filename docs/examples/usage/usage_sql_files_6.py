from sqlspec.loader import SQLFileLoader

__all__ = ("test_loading_directories_with_mixed_files",)


def test_loading_directories_with_mixed_files() -> None:
    # start-example
    loader = SQLFileLoader()
    loader.load_sql("migrations/")  # Only loads queries.sql

    # Check what was loaded
    queries = loader.list_queries()  # Only returns named queries
    # end-example
    # Dummy asserts for doc example
    assert hasattr(loader, "load_sql")
    assert isinstance(queries, list)
