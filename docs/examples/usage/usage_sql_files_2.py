from pathlib import Path

from docs.examples.usage.usage_sql_files_1 import create_loader

__all__ = ("test_using_loaded_queries",)


def test_using_loaded_queries(tmp_path: Path) -> None:
    loader, _queries = create_loader(tmp_path)
    # start-example
    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig

    # Set up database
    spec = SQLSpec()
    config = SqliteConfig()
    spec.add_config(config)

    # Get SQL with parameters
    user_query = loader.get_sql("get_user_by_id")
    # end-example
    # Dummy asserts for doc example
    assert user_query is not None  # In real usage, loader must be defined
