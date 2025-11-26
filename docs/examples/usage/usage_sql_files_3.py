from pathlib import Path

from docs.examples.usage.usage_sql_files_1 import create_loader


def test_add_queries_programmatically(tmp_path: Path) -> None:
    loader, _ = create_loader(tmp_path)
    # start-example
    # Add a query at runtime
    loader.add_named_sql("health_check", "SELECT 'OK' as status, CURRENT_TIMESTAMP as timestamp")

    # Add with dialect
    loader.add_named_sql("postgres_version", "SELECT version()", dialect="postgres")

    # Use the added query
    health_sql = loader.get_sql("health_check")
    # end-example
    # Dummy asserts for doc example
    assert health_sql is not None
    assert "SELECT 'OK' as status" in health_sql.sql
    postgres_sql = loader.get_sql("postgres_version")
    assert postgres_sql is not None
    assert "SELECT version()" in postgres_sql.sql
