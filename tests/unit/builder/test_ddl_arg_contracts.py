"""Render regressions for DDL builders that silently dropped clauses.

Each construct here previously passed an argument key absent from the target
sqlglot expression's ``arg_types``; the generator ignores unknown keys, so the
clause vanished from rendered SQL without an error.
"""

from sqlspec import sql
from sqlspec.builder._ddl import CreateTable


def test_named_primary_key_constraint_renders_body() -> None:
    result = CreateTable("t").column("id", "INT").primary_key_constraint("id", name="pk_t").build()
    assert "CONSTRAINT" in result.sql
    assert "pk_t" in result.sql
    assert "PRIMARY KEY" in result.sql


def test_named_foreign_key_constraint_renders_body() -> None:
    result = (
        CreateTable("orders")
        .column("user_id", "INT")
        .foreign_key_constraint("user_id", "users", "id", name="fk_orders_user")
        .build()
    )
    assert "fk_orders_user" in result.sql
    assert "FOREIGN KEY" in result.sql
    assert "REFERENCES" in result.sql


def test_named_unique_constraint_renders_body() -> None:
    result = CreateTable("t").column("email", "TEXT").unique_constraint("email", name="uq_email").build()
    assert "uq_email" in result.sql
    assert "UNIQUE" in result.sql
    assert "email" in result.sql


def test_named_check_constraint_renders_body() -> None:
    result = CreateTable("t").column("age", "INT").check_constraint("age > 0", name="ck_age").build()
    assert "ck_age" in result.sql
    assert "CHECK" in result.sql
    assert '"age" > 0' in result.sql


def test_foreign_key_actions_render() -> None:
    result = (
        CreateTable("orders")
        .column("user_id", "INT")
        .foreign_key_constraint("user_id", "users", "id", on_delete="CASCADE", on_update="SET NULL")
        .build()
    )
    assert "ON DELETE CASCADE" in result.sql
    assert "ON UPDATE SET NULL" in result.sql


def test_alter_table_add_constraint_renders_constraint() -> None:
    result = sql.alter_table("t").add_constraint("UNIQUE", columns="email", name="uq_email").build()
    assert "ADD" in result.sql
    assert "uq_email" in result.sql
    assert "UNIQUE" in result.sql


def test_create_temporary_table_renders_temporary() -> None:
    result = CreateTable("t").temporary().column("id", "INT").build()
    assert "TEMPORARY" in result.sql


def test_create_table_like_renders_like() -> None:
    result = CreateTable("t").like("src").build(dialect="mysql")
    assert "LIKE" in result.sql
    assert "src" in result.sql


def test_ctas_renders_explicit_column_list() -> None:
    result = sql.create_table_as_select().name("t").columns("a", "b").as_select("SELECT 1, 2").build()
    assert "(" in result.sql.split("AS")[0]
    assert "a" in result.sql.split("AS")[0]
    assert "b" in result.sql.split("AS")[0]


def test_create_view_renders_explicit_column_list() -> None:
    result = sql.create_view("v").columns("a", "b").as_select("SELECT 1, 2").build()
    head = result.sql.split("AS")[0]
    assert "a" in head
    assert "b" in head


def test_create_materialized_view_renders_explicit_column_list() -> None:
    result = sql.create_materialized_view("mv").columns("a", "b").as_select("SELECT 1, 2").build(dialect="postgres")
    head = result.sql.split("AS")[0]
    assert "a" in head
    assert "b" in head


def test_truncate_renders_table_name() -> None:
    result = sql.truncate("t").build()
    assert "TRUNCATE TABLE" in result.sql
    assert "t" in result.sql.replace("TRUNCATE TABLE", "")


def test_truncate_cascade_renders_cascade() -> None:
    result = sql.truncate("t").cascade().build(dialect="postgres")
    assert "CASCADE" in result.sql


def test_drop_index_on_table_renders_table() -> None:
    result = sql.drop_index("idx").on_table("t").build(dialect="mysql")
    assert "ON" in result.sql
    assert "t" in result.sql.split("ON")[-1]
