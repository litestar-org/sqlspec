from sqlglot import parse_one

from sqlspec.builder import build_copy_from_statement, build_copy_to_statement, sql
from sqlspec.core import SQL


def test_build_copy_from_statement_generates_expected_sql() -> None:
    statement = build_copy_from_statement(
        "public.users", "s3://bucket/data.parquet", columns=("id", "name"), options={"format": "parquet"}
    )

    assert isinstance(statement, SQL)
    rendered = statement.sql
    assert rendered == "COPY \"public.users\" (id, name) FROM 's3://bucket/data.parquet' WITH (FORMAT 'parquet')"

    expression = parse_one(rendered, read="postgres")
    assert expression.args["kind"] is True


def test_build_copy_to_statement_generates_expected_sql() -> None:
    statement = build_copy_to_statement(
        "public.users", "s3://bucket/output.parquet", options={"format": "parquet", "compression": "gzip"}
    )

    assert isinstance(statement, SQL)
    rendered = statement.sql
    assert rendered == (
        "COPY \"public.users\" TO 's3://bucket/output.parquet' WITH (FORMAT 'parquet', COMPRESSION 'gzip')"
    )

    expression = parse_one(rendered, read="postgres")
    assert expression.args["kind"] is False


def test_sql_factory_copy_helpers() -> None:
    statement = sql.copy_from("users", "s3://bucket/in.csv", columns=("id", "name"), options={"format": "csv"})
    assert isinstance(statement, SQL)
    assert statement.sql.startswith("COPY users")

    to_statement = sql.copy("users", target="s3://bucket/out.csv", options={"format": "csv", "header": True})
    assert isinstance(to_statement, SQL)
    parsed = parse_one(to_statement.sql, read="postgres")
    assert parsed.args["kind"] is False
