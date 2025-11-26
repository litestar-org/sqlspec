from pathlib import Path

from docs.examples.usage.usage_sql_files_1 import create_loader

__all__ = ("test_caching_behavior",)


def test_caching_behavior(tmp_path: Path) -> None:
    loader, _queries = create_loader(tmp_path)
    # start-example
    # First load - reads from disk
    loader.load_sql(tmp_path / "sql/queries/users.sql")

    # Second load - uses cache (file already loaded)
    loader.load_sql(tmp_path / "sql/queries/users.sql")

    # Clear cache
    loader.clear_cache()

    # Force reload from disk
    loader.load_sql(tmp_path / "sql/queries/users.sql")
    # end-example
    # Dummy asserts for doc example
    assert hasattr(loader, "load_sql")
    assert hasattr(loader, "clear_cache")
