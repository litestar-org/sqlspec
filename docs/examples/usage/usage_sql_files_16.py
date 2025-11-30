from pathlib import Path

from docs.examples.usage.usage_sql_files_1 import create_loader

__all__ = ("test_file_load_errors",)


def test_file_load_errors(tmp_path: Path) -> None:
    loader, _queries = create_loader(tmp_path)
    # start-example
    from sqlspec.exceptions import SQLFileNotFoundError, SQLFileParseError

    try:
        loader.load_sql("sql/queries.sql")
    except SQLFileNotFoundError as e:
        print(f"File not found: {e}")
    except SQLFileParseError as e:
        print(f"Failed to parse SQL file: {e}")
    # end-example
    # Dummy asserts for doc example
    assert hasattr(loader, "load_sql")
