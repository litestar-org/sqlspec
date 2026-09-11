"""Native COPY SQL keeps query values and object names bound."""

from pathlib import Path

import duckdb
import pytest

from sqlspec.adapters.duckdb.core import _build_storage_copy_sql, _build_storage_read_sql


@pytest.mark.parametrize("file_format", ["parquet", "csv"])
def test_copy_filename_precedes_query_bindings(tmp_path: Path, file_format: str) -> None:
    path = str(tmp_path / f"quote'and?question.{file_format}")
    sql = _build_storage_copy_sql("SELECT ? AS first, ? AS second, ? AS repeated, '?' AS literal", file_format)
    assert sql is not None
    with duckdb.connect() as connection:
        assert connection.execute(sql, [path, "first", 42, "first"]).fetchone() == (1,)
        read_sql = "SELECT * FROM read_parquet(?)" if file_format == "parquet" else "SELECT * FROM read_csv(?)"
        assert connection.execute(read_sql, [path]).fetchall() == [("first", 42, "first", "?")]


@pytest.mark.parametrize("query", ["DELETE FROM t", "INSERT INTO t VALUES (1)", "SELECT 1; SELECT 2"])
def test_non_query_or_multiple_statements_keep_original_path(query: str) -> None:
    assert _build_storage_copy_sql(query, "parquet") is None


def test_native_append_quotes_target_and_disables_hive_columns(tmp_path: Path) -> None:
    parent = tmp_path / "partition=value"
    parent.mkdir()
    path = str(parent / "data.parquet")
    sql = _build_storage_read_sql('"target space"', path, "parquet")
    assert sql is not None
    with duckdb.connect() as connection:
        connection.execute("COPY (SELECT 1 AS id, NULL::VARCHAR AS name) TO ? (FORMAT PARQUET)", [path])
        connection.execute('CREATE TABLE "target space" (id INTEGER, name VARCHAR)')
        assert connection.execute(sql, [path]).fetchone() == (1,)
        assert connection.execute('SELECT * FROM "target space"').fetchall() == [(1, None)]


@pytest.mark.parametrize("table", ["target; DELETE FROM other", "target t", "target WHERE true", "target()"])
def test_native_append_rejects_target_fragments(table: str) -> None:
    with pytest.raises(ValueError, match="table"):
        _build_storage_read_sql(table, "s3://bucket/data.parquet", "parquet")


@pytest.mark.parametrize("uri", ["s3://bucket/a*.parquet", "s3://bucket/a?.parquet", "s3://bucket/[ab].parquet"])
def test_native_append_globs_keep_single_object_fallback(uri: str) -> None:
    assert _build_storage_read_sql("target", uri, "parquet") is None
