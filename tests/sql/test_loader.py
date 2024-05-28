from pathlib import Path

from sqlspec.sql.loader import QueryLoader

sql_path = Path(__file__).parent.parent / "sql"


def test_loader() -> None:
    ql = QueryLoader()
    sql = ql.load_query_data_from_dir_path(sql_path)
    assert len(sql) > 1
