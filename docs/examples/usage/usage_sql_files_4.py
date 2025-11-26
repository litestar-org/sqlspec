from pathlib import Path

from docs.examples.usage.usage_sql_files_1 import create_loader


def test_query_metadata(tmp_path: Path) -> None:
    loader, _ = create_loader(tmp_path)
    # start-example
    # Get file info for a query
    file_info = loader.get_file_for_query("get_user_by_id")
    if file_info:
        print(f"Query from: {file_info.path}")
        print(f"Checksum: {file_info.checksum}")
        print(f"Loaded at: {file_info.loaded_at}")

    # Get all queries from a specific file
    loader.get_file(tmp_path / "sql/queries/users.sql")
    # if file_obj:
    # print(f"Contains {len(file_obj.queries)} queries")
    # for query in file_obj.queries:
    # print(f"  - {query.name}")
    # end-example
    # Dummy asserts for doc example
    assert hasattr(file_info, "path")
