"""Tests for AlterTable public API completeness."""

import pytest

from sqlspec import sql
from sqlspec.builder._ddl import AlterTable
from sqlspec.exceptions import SQLBuilderError


def test_alter_table_in_schema_qualifies_table_name() -> None:
    sql = AlterTable("users").in_schema("myschema").add_column("x", "INT").build().sql

    assert "myschema" in sql
    assert "users" in sql


def test_alter_table_in_schema_is_chainable() -> None:
    builder = AlterTable("t")

    assert builder.in_schema("s") is builder


def test_alter_table_set_column_default_renders_set_default() -> None:
    sql = AlterTable("t").set_column_default("x", 0).build().sql

    assert "SET DEFAULT" in sql
    assert "x" in sql


def test_alter_table_drop_column_default_renders_drop_default() -> None:
    sql = AlterTable("t").drop_column_default("x").build().sql

    assert "DROP DEFAULT" in sql
    assert "x" in sql


def test_alter_table_default_methods_are_chainable() -> None:
    builder = AlterTable("t")

    assert builder.set_column_default("x", 0) is builder
    assert builder.drop_column_default("x") is builder


def test_alter_table_set_column_default_empty_column_raises() -> None:
    with pytest.raises(SQLBuilderError):
        AlterTable("t").set_column_default("", 0)


def test_alter_table_drop_column_default_empty_column_raises() -> None:
    with pytest.raises(SQLBuilderError):
        AlterTable("t").drop_column_default("")


@pytest.mark.parametrize(
    "using",
    ["a::int; DROP TABLE t", "(SELECT secret FROM s)", "1; DROP TABLE t"],
    ids=["stacked", "subquery", "pg-stacked"],
)
def test_alter_column_type_using_rejects_bad_text(using: str) -> None:
    with pytest.raises(SQLBuilderError):
        sql.alter_table("t").alter_column_type("a", "INT", using=using)


def test_alter_column_type_using_rejected_at_build() -> None:
    builder = sql.alter_table("t").alter_column_type("a", "INT", using="a::int")
    builder._operations[-1].using_expression = "1; DROP TABLE t"  # pyright: ignore[reportPrivateUsage]

    with pytest.raises(SQLBuilderError):
        builder.build(dialect="postgres")


def test_alter_column_type_using_accepts_cast() -> None:
    rendered = sql.alter_table("t").alter_column_type("a", "INT", using="a::integer").build(dialect="postgres").sql

    assert rendered.count("USING") == 1
    assert ";" not in rendered
    assert "CAST" in rendered.upper()


def test_add_constraint_check_rejects_bad_text() -> None:
    with pytest.raises(SQLBuilderError):
        sql.alter_table("t").add_constraint("check", name="c", condition="a > 0); DROP TABLE t; --")
