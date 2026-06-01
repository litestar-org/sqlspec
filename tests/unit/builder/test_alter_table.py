"""Tests for AlterTable public API completeness."""

import pytest

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
