"""Unit tests for build() and to_sql() dialect override parameter."""

from collections.abc import Callable
from typing import Any, cast

import pytest
import sqlglot
from sqlglot import exp

from sqlspec import sql
from sqlspec.builder import Column
from sqlspec.core import SQL, StatementConfig
from sqlspec.exceptions import SQLBuilderError


def test_build_with_default_dialect() -> None:
    """Test build() uses builder's default dialect."""
    query = sql.select("*").from_("products").where("id = :id")
    query.add_parameter(1, "id")

    result = query.build()
    assert ":id" in result.sql or "$1" in result.sql


def test_build_with_dialect_override() -> None:
    """Test build() can override dialect at build time."""
    query = sql.select("*").from_("products").where("id = :id")
    query.add_parameter(1, "id")

    postgres_result = query.build(dialect="postgres")
    assert "SELECT" in postgres_result.sql
    assert "products" in postgres_result.sql.lower()

    mysql_result = query.build(dialect="mysql")
    assert "SELECT" in mysql_result.sql
    assert "`products`" in mysql_result.sql or "products" in mysql_result.sql.lower()


def test_build_dialect_override_preserves_parameters() -> None:
    """Test dialect override doesn't affect parameters."""
    query = sql.select("*").from_("products").where("price > :min_price")
    query.add_parameter(100, "min_price")

    result1 = query.build(dialect="postgres")
    result2 = query.build(dialect="mysql")

    assert result1.parameters == {"min_price": 100}
    assert result2.parameters == {"min_price": 100}


def test_to_sql_with_default_dialect() -> None:
    """Test to_sql() uses builder's default dialect."""
    query = sql.select("name", "price").from_("products")

    sql_str = query.to_sql()
    assert "SELECT" in sql_str
    assert "products" in sql_str.lower()


def test_to_sql_with_dialect_override() -> None:
    """Test to_sql() can override dialect."""
    query = sql.select("*").from_("products").where("active = :active")
    query.add_parameter(True, "active")

    postgres_sql = query.to_sql(dialect="postgres")
    assert "SELECT" in postgres_sql
    assert "products" in postgres_sql.lower()

    mysql_sql = query.to_sql(dialect="mysql")
    assert "SELECT" in mysql_sql
    assert "products" in mysql_sql.lower()


def test_to_sql_with_show_parameters_and_dialect_override() -> None:
    """Test to_sql() with both show_parameters and dialect override.

    Note: Parameter substitution only works when using default dialect since
    SQLGlot changes parameter formats (e.g., :id → %(id)s for postgres).
    This test just verifies the dialect override works.
    """
    query = sql.select("*").from_("products").where("id = :id")
    query.add_parameter(123, "id")

    postgres_sql = query.to_sql(show_parameters=False, dialect="postgres")
    assert "SELECT" in postgres_sql
    assert "products" in postgres_sql.lower()

    mysql_sql = query.to_sql(show_parameters=False, dialect="mysql")
    assert "SELECT" in mysql_sql
    assert "products" in mysql_sql.lower()


def test_build_dialect_override_with_merge() -> None:
    """Test dialect override works with MERGE builder."""
    query = (
        sql.merge_
        .into("products", alias="t")
        .using({"id": 1, "name": "Widget"}, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name=sql.raw("src.name"))
        .when_not_matched_then_insert(id=sql.raw("src.id"), name=sql.raw("src.name"))
    )

    postgres_result = query.build(dialect="postgres")
    assert "MERGE INTO" in postgres_result.sql

    oracle_result = query.build(dialect="oracle")
    assert "MERGE INTO" in oracle_result.sql


def test_build_dialect_override_with_insert() -> None:
    """Test dialect override works with INSERT builder."""
    query = sql.insert("products").values(id=1, name="Widget", price=19.99)

    postgres_sql = query.build(dialect="postgres")
    assert "INSERT INTO" in postgres_sql.sql

    mysql_sql = query.build(dialect="mysql")
    assert "INSERT INTO" in mysql_sql.sql


def test_to_sql_dialect_override_preserves_builder_state() -> None:
    """Test dialect override doesn't modify builder's default dialect."""
    query = sql.select("*", dialect="postgres").from_("products")

    query.to_sql(dialect="mysql")
    query.to_sql()

    assert query.dialect_name == "postgres"


def test_build_multiple_dialect_overrides() -> None:
    """Test builder can generate SQL for multiple dialects."""
    query = sql.select("id", "name", "price").from_("products").where("category = :category").order_by("price DESC")
    query.add_parameter("electronics", "category")

    dialects = ["postgres", "mysql", "sqlite", "oracle", "bigquery"]
    results = [query.build(dialect=d) for d in dialects]

    for result in results:
        assert "SELECT" in result.sql
        assert "products" in result.sql.lower()
        assert result.parameters == {"category": "electronics"}


def test_oracle_to_statement_compile_uses_unquoted_identifiers() -> None:
    """Oracle expression-backed statements should not quote implicit lowercase identifiers."""
    statement_config = StatementConfig(dialect="oracle")
    statement = (
        sql
        .select("content", Column("embedding").vector_distance([0.1, 0.2, 0.3]).alias("distance"))
        .from_("vector_docs_oracledb_async")
        .order_by("distance")
        .to_statement(statement_config)
    )

    compiled_sql, _parameters = statement.compile()

    assert "FROM vector_docs_oracledb_async vector_docs_oracledb_async" in compiled_sql
    assert '"vector_docs_oracledb_async"' not in compiled_sql
    assert "VECTOR_DISTANCE(vector_docs_oracledb_async.embedding" in compiled_sql


def test_oracle_lock_target_rendering_does_not_use_builder_string_cleanup() -> None:
    query = sql.select("id", dialect="oracle").from_("job", alias="j").for_update(of="j")

    assert "FOR UPDATE OF j" in query.build().sql


def test_to_sql_dialect_override_with_complex_query() -> None:
    """Test dialect override with complex query (JOIN, WHERE, ORDER BY)."""
    query = (
        sql
        .select("p.id", "p.name", "c.name AS category_name")
        .from_("products AS p")
        .join("categories AS c", "p.category_id = c.id")
        .where("p.price > :min_price")
        .where("c.active = :active")
        .order_by("p.name")
    )
    query.add_parameter(100, "min_price")
    query.add_parameter(True, "active")

    postgres_sql = query.to_sql(dialect="postgres")
    mysql_sql = query.to_sql(dialect="mysql")

    assert "JOIN" in postgres_sql
    assert "JOIN" in mysql_sql
    assert "WHERE" in postgres_sql
    assert "WHERE" in mysql_sql


def test_mssql_alias_builds_tsql() -> None:
    """Test dialect override 'mssql' normalizes to 'tsql' and renders identical T-SQL."""
    query = sql.select("id", "name").from_("products").limit(10)
    mssql_sql = query.build(dialect="mssql").sql
    tsql_sql = query.build(dialect="tsql").sql
    assert "TOP" in mssql_sql
    assert mssql_sql == tsql_sql


def test_mariadb_and_cockroachdb_dialect_aliases() -> None:
    """Test dialect aliases for mariadb and cockroachdb render expected syntax."""
    query = sql.select("id", "name").from_("products")
    mariadb_sql = query.build(dialect="mariadb").sql
    mysql_sql = query.build(dialect="mysql").sql
    assert mariadb_sql == mysql_sql

    cockroach_query = sql.select("id", "name").from_("products").for_update(skip_locked=True)
    cockroach_sql = cockroach_query.build(dialect="cockroachdb").sql
    postgres_sql = cockroach_query.build(dialect="postgres").sql
    assert "FOR UPDATE SKIP LOCKED" in cockroach_sql
    assert cockroach_sql == postgres_sql


def test_dialect_class_override_and_alias_metadata() -> None:
    from sqlglot.dialects.mysql import MySQL

    query = sql.select("id").from_("products").limit(10)
    assert query.build(dialect=MySQL).sql == query.build(dialect="mysql").sql
    assert query.build(dialect=MySQL()).sql == query.build(dialect="mysql").sql
    assert query.build(dialect="mssql").dialect == "tsql"


_TSQL_OUTPUT_BUILDERS: "dict[str, tuple[Callable[[], Any], str, str]]" = {
    "insert": (
        lambda: sql.insert("t").columns("a").values(1),
        "INSERT INTO [t] (\n  [a]\n)\nOUTPUT %s\nVALUES\n  (:a)",
        "INSERTED",
    ),
    "update": (
        lambda: sql.update("t").set(a=1).where_eq("id", 2),
        "UPDATE [t] SET [a] = :a\nOUTPUT %s\nWHERE\n  [id] = :id",
        "INSERTED",
    ),
    "delete": (
        lambda: sql.delete("t").where_eq("id", 2),
        "DELETE   [t]\nOUTPUT %s FROM [t]\nWHERE\n  [id] = :id",
        "DELETED",
    ),
}


@pytest.mark.parametrize("kind", list(_TSQL_OUTPUT_BUILDERS))
@pytest.mark.parametrize(
    ("projection", "expected"),
    [
        pytest.param("a", "[{source}].[a]", id="column"),
        pytest.param("*", "[{source}].*", id="star"),
        pytest.param("t.a", "[{source}].[a]", id="target-column"),
        pytest.param("t.*", "[{source}].*", id="target-star"),
        pytest.param("a AS x", "[{source}].[a] AS [x]", id="alias"),
        pytest.param("a + 1 AS x", "[{source}].[a] + 1 AS [x]", id="arithmetic"),
        pytest.param("UPPER(a)", "UPPER([{source}].[a])", id="function"),
        pytest.param(
            "CASE WHEN a > 1 THEN b END AS c", "CASE WHEN [{source}].[a] > 1 THEN [{source}].[b] END AS [c]", id="case"
        ),
        pytest.param("inserted.a", "[inserted].[a]", id="inserted-qualifier"),
        pytest.param("DELETED.a", "[deleted].[a]", id="deleted-qualifier"),
        pytest.param("COUNT(*)", "COUNT_BIG(*)", id="aggregate"),
        pytest.param("other.a", "[other].[a]", id="unknown-qualifier"),
    ],
)
def test_tsql_returning_qualifies_projection(kind: str, projection: str, expected: str) -> None:
    factory, template, source = _TSQL_OUTPUT_BUILDERS[kind]
    built = factory().returning(projection).build(dialect="tsql")
    assert built.sql == template % expected.format(source=source)


@pytest.mark.parametrize(
    ("factory", "expected"),
    [
        pytest.param(
            lambda: sql.update("t").set(a=1).where_eq("id", 2).returning("DELETED.b AS old", "INSERTED.a AS new"),
            "UPDATE [t] SET [a] = :a\nOUTPUT [deleted].[b] AS [old], [inserted].[a] AS [new]\nWHERE\n  [id] = :id",
            id="old-and-new",
        ),
        pytest.param(
            lambda: sql.update("t").set(a=1).where_eq("id", 2).returning(Column("a"), exp.Star()),
            "UPDATE [t] SET [a] = :a\nOUTPUT [INSERTED].[a], [INSERTED].*\nWHERE\n  [id] = :id",
            id="nodes",
        ),
        pytest.param(
            lambda: sql.update("t").set(a=1).where_eq("id", 2).returning(cast("Any", sql.raw("a + 1"))),
            "UPDATE [t] SET [a] = :a\nOUTPUT [INSERTED].[a] + 1\nWHERE\n  [id] = :id",
            id="raw-wrapper",
        ),
        pytest.param(
            lambda: sql.update("dbo.t").set(a=1).returning("dbo.t.a", "t.b"),
            "UPDATE [dbo].[t] SET [a] = :a\nOUTPUT [INSERTED].[a], [INSERTED].[b]",
            id="schema-update",
        ),
        pytest.param(
            lambda: sql.update("t").set(a=1).returning("dbo.t.a"),
            "UPDATE [t] SET [a] = :a\nOUTPUT [dbo].[t].[a]",
            id="schema-mismatch-unqualified-target",
        ),
        pytest.param(
            lambda: sql.update("dbo.t").set(a=1).returning("other.t.a"),
            "UPDATE [dbo].[t] SET [a] = :a\nOUTPUT [other].[t].[a]",
            id="schema-mismatch-qualified-target",
        ),
        pytest.param(
            lambda: sql.delete("dbo.t").returning("a"),
            "DELETE   [dbo].[t]\nOUTPUT [DELETED].[a] FROM [dbo].[t]",
            id="schema-delete",
        ),
        pytest.param(
            lambda: sql.insert("dbo.t").columns("a").values(1).returning("dbo.t.a"),
            "INSERT INTO [dbo].[t] (\n  [a]\n)\nOUTPUT [INSERTED].[a]\nVALUES\n  (:a)",
            id="schema-insert",
        ),
        pytest.param(
            lambda: (
                sql.update("t").set(a=1).from_("src", alias="s").where("t.id = s.id").returning("a", "t.b", "s.c AS sc")
            ),
            "UPDATE [t] SET [a] = :a\nOUTPUT [INSERTED].[a], [INSERTED].[b], [s].[c] AS [sc]\nFROM [src] AS [s]\n"
            "WHERE\n  [s].[id] = [t].[id]",
            id="from-alias",
        ),
        pytest.param(
            lambda: sql.update("t").with_cte("c", sql.select("id").from_("x")).set(a=1).returning("id"),
            "WITH [c] AS (\n  SELECT\n    [x].[id] AS [id]\n  FROM [x] AS [x]\n)\nUPDATE [t] SET [a] = :a\n"
            "OUTPUT [INSERTED].[id]",
            id="cte",
        ),
        pytest.param(
            lambda: sql.insert("t").columns("a").from_select(sql.select("a").from_("s")).returning("a"),
            "INSERT INTO [t] (\n  [a]\n)\nOUTPUT [INSERTED].[a]\nSELECT\n  [s].[a] AS [a]\nFROM [s] AS [s]",
            id="insert-select",
        ),
        pytest.param(
            lambda: sql.delete("t").where_eq("id", 2), "DELETE FROM [t]\nWHERE\n  [id] = :id", id="no-returning"
        ),
    ],
)
def test_tsql_returning_target_and_source_forms(factory: "Callable[[], Any]", expected: str) -> None:
    assert factory().build(dialect="tsql").sql == expected


def test_tsql_returning_mssql_alias_and_bracket_string() -> None:
    built = sql.update("t", dialect="mssql").set(a=1).returning("[a b]").build()
    assert built.sql == "UPDATE [t] SET [a] = :a\nOUTPUT [INSERTED].[a b]"


@pytest.mark.parametrize("kind", list(_TSQL_OUTPUT_BUILDERS))
def test_tsql_returning_rejects_subquery(kind: str) -> None:
    factory, _, _ = _TSQL_OUTPUT_BUILDERS[kind]
    with pytest.raises(SQLBuilderError, match="do not allow subqueries"):
        factory().returning("(SELECT 1) AS m").build(dialect="tsql")


@pytest.mark.parametrize(
    ("dialect", "factory", "expected"),
    [
        pytest.param(
            "sqlanywhere",
            lambda: sql.update("t").set(a=1).returning("a + 1 AS x", "t.a"),
            "UPDATE [t] SET [a] = :a\nOUTPUT [a] + 1 AS [x], [t].[a]",
            id="sqlanywhere",
        ),
        pytest.param(
            "fabric",
            lambda: sql.delete("t").where_eq("id", 2).returning("*"),
            "DELETE\nOUTPUT * FROM [t]\nWHERE\n  [id] = :id",
            id="fabric",
        ),
        pytest.param(
            "postgres",
            lambda: sql.delete("t").where_eq("id", 2).returning("a"),
            'DELETE FROM "t"\nWHERE\n  "id" = %(id)s\nRETURNING "a"',
            id="postgres",
        ),
    ],
)
def test_tsql_output_rewrite_stays_on_tsql(dialect: str, factory: "Callable[[], Any]", expected: str) -> None:
    assert factory().build(dialect=dialect).sql == expected


def test_tsql_returning_statement_cache_isolated() -> None:
    builder = sql.update("t", dialect="tsql").set(a=1).returning("a")

    first = builder.to_statement().compile()
    assert first == ("UPDATE [t] SET [a] = ? OUTPUT INSERTED.[a]", (1,))
    assert builder.to_statement().compile() == first
    assert builder.to_statement(StatementConfig(dialect="postgres")).sql == 'UPDATE "t" SET "a" = %(a)s RETURNING "a"'
    assert builder.to_statement().sql == "UPDATE [t] SET [a] = :a OUTPUT INSERTED.[a]"
    assert builder.build(dialect="postgres").sql == 'UPDATE "t" SET "a" = %(a)s\nRETURNING "a"'
    assert builder.to_sql(show_parameters=True) == "UPDATE [t] SET [a] = 1\nOUTPUT [INSERTED].[a]"


@pytest.mark.parametrize(
    ("factory", "expected"),
    [
        pytest.param(
            lambda: sql.insert("t", dialect="tsql").columns("a").values(1).returning("*"),
            ("INSERT INTO [t] ([a]) OUTPUT INSERTED.* VALUES (?)", (1,)),
            id="insert-star",
        ),
        pytest.param(
            lambda: sql.delete("t", dialect="tsql").where_eq("id", 2).returning("a", "t.b"),
            ("DELETE [t] OUTPUT DELETED.[a], DELETED.[b] FROM [t] WHERE [id] = ?", (2,)),
            id="delete-columns",
        ),
    ],
)
def test_tsql_returning_statement_forms(factory: "Callable[[], Any]", expected: "tuple[str, tuple[int, ...]]") -> None:
    assert factory().to_statement().compile() == expected


def test_tsql_returning_static_build_keeps_caller_expression() -> None:
    source = sqlglot.parse_one("UPDATE t SET a = 1 RETURNING a")
    built = sql.update("t").build_static_expression(source, copy=False, dialect="tsql")
    assert built.sql == "UPDATE [t] SET [a] = 1\nOUTPUT [INSERTED].[a]"
    assert source.sql() == "UPDATE t SET a = 1 RETURNING a"


def test_raw_sql_compile_keeps_tsql_output_as_written() -> None:
    statement = SQL("UPDATE t SET a=1 OUTPUT a", statement_config=StatementConfig(dialect="tsql"))
    assert statement.compile() == ("UPDATE t SET a=1 OUTPUT a", [])


@pytest.mark.parametrize(
    ("raw", "source_dialect", "expected"),
    [
        pytest.param(
            "UPDATE t SET a=1 RETURNING a", "postgres", "UPDATE [t] SET [a] = 1\nOUTPUT [INSERTED].[a]", id="update"
        ),
        pytest.param(
            "DELETE FROM t AS u WHERE u.id = 1 RETURNING u.a",
            "postgres",
            "DELETE   [u]\nOUTPUT [DELETED].[a] FROM [t] AS [u]\nWHERE\n  [u].[id] = 1",
            id="delete-alias",
        ),
        pytest.param(
            "DELETE t OUTPUT deleted.a WHERE id=1",
            "tsql",
            "DELETE   [t]\nOUTPUT [deleted].[a]\nWHERE\n  [id] = 1",
            id="delete-existing-tables",
        ),
    ],
)
def test_raw_sql_builder_renders_tsql_output(raw: str, source_dialect: str, expected: str) -> None:
    builder = SQL(raw, statement_config=StatementConfig(dialect=source_dialect)).builder()
    assert builder.build(dialect="tsql").sql == expected
