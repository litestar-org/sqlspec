"""Unit tests for INSERT builder functionality including ON CONFLICT operations."""

from typing import Any, cast

import pytest
from sqlglot import exp

from sqlspec import sql
from sqlspec.exceptions import SQLBuilderError


def test_insert_basic_functionality() -> None:
    """Test basic INSERT builder functionality."""
    query = sql.insert("users").columns("name", "email").values("John", "john@test.com")
    stmt = query.build()

    assert "INSERT INTO" in stmt.sql
    assert '"users"' in stmt.sql or "users" in stmt.sql
    assert "name" in stmt.parameters
    assert "email" in stmt.parameters
    assert stmt.parameters["name"] == "John"
    assert stmt.parameters["email"] == "john@test.com"


def test_insert_with_table_in_constructor() -> None:
    """Test INSERT with table specified in constructor."""
    query = sql.insert("products").values(name="Widget", price=29.99)
    stmt = query.build()

    assert "INSERT INTO" in stmt.sql
    assert "products" in stmt.sql
    assert "name" in stmt.parameters
    assert "price" in stmt.parameters


def test_insert_preserves_schema_qualified_table() -> None:
    """INSERT column handling should keep schema-qualified table targets intact."""
    query = sql.insert("history.ddl_migrations").columns("version_num").values("0001")
    stmt = query.build()

    assert '"history"."ddl_migrations"' in stmt.sql
    assert stmt.parameters["version_num"] == "0001"


def test_insert_values_from_dict_removed() -> None:
    """The removed dict-only Insert API is absent."""
    builder = sql.insert("users")

    assert not hasattr(builder, "values_from_dict")
    assert not hasattr(builder, "values_from_dicts")


def test_insert_values_from() -> None:
    """Test INSERT using values_from."""
    data = {"id": 1, "name": "John", "status": "active"}
    query = sql.insert("users").values_from(data)
    stmt = query.build()

    assert "INSERT INTO" in stmt.sql
    assert len(stmt.parameters) == 3
    assert stmt.parameters["id"] == 1
    assert stmt.parameters["name"] == "John"
    assert stmt.parameters["status"] == "active"


def test_insert_values_from_many_multiple_rows() -> None:
    """Test INSERT using values_from_many for multiple rows."""
    data = [{"id": 1, "name": "John"}, {"id": 2, "name": "Jane"}, {"id": 3, "name": "Bob"}]
    query = sql.insert("users").values_from_many(data)
    stmt = query.build()

    assert "INSERT INTO" in stmt.sql

    assert "id" in stmt.parameters
    assert "name" in stmt.parameters
    assert "id_1" in stmt.parameters
    assert "name_1" in stmt.parameters
    assert "id_2" in stmt.parameters
    assert "name_2" in stmt.parameters


def test_insert_with_kwargs() -> None:
    """Test INSERT using kwargs in values method."""
    query = sql.insert("products").values(name="Widget", price=29.99, in_stock=True)
    stmt = query.build()

    assert "INSERT INTO" in stmt.sql
    assert "name" in stmt.parameters
    assert "price" in stmt.parameters
    assert "in_stock" in stmt.parameters
    assert stmt.parameters["name"] == "Widget"
    assert stmt.parameters["price"] == 29.99
    assert stmt.parameters["in_stock"] is True


def test_insert_mixed_args_kwargs_error() -> None:
    """Test that mixing positional and keyword arguments raises error."""
    with pytest.raises(SQLBuilderError, match="Cannot mix positional values with keyword values"):
        sql.insert("users").values("John", email="john@test.com")


def test_insert_multiple_values_calls() -> None:
    """Test multiple calls to values() method for multi-row insert."""
    query = sql.insert("users").columns("name", "email").values("John", "john@test.com").values("Jane", "jane@test.com")
    stmt = query.build()

    assert "INSERT INTO" in stmt.sql

    assert "name" in stmt.parameters
    assert "email" in stmt.parameters
    assert "name_1" in stmt.parameters
    assert "email_1" in stmt.parameters


def test_insert_with_returning() -> None:
    """Test INSERT with RETURNING clause."""
    query = sql.insert("users").values(name="John").returning("id", "created_at")
    stmt = query.build()

    assert "INSERT INTO" in stmt.sql
    assert "RETURNING" in stmt.sql
    assert "id" in stmt.sql
    assert "created_at" in stmt.sql


def test_insert_without_table_error() -> None:
    """Test that values() without table raises error."""
    with pytest.raises(SQLBuilderError, match="The target table must be set"):
        sql.insert().values(name="John")


def test_insert_values_columns_mismatch_error() -> None:
    """Test that mismatched columns and values raises error."""
    with pytest.raises(SQLBuilderError, match="Number of values"):
        sql.insert("users").columns("name", "email").values("John")


def test_insert_columns_and_values_consistency() -> None:
    """Test that columns and values are consistent."""
    query = sql.insert("users").columns("name", "email", "age").values("John", "john@test.com", 25)
    stmt = query.build()

    assert "INSERT INTO" in stmt.sql
    assert len(stmt.parameters) == 3
    assert stmt.parameters["name"] == "John"
    assert stmt.parameters["email"] == "john@test.com"
    assert stmt.parameters["age"] == 25


def test_insert_inconsistent_dict_keys_error() -> None:
    """Test that inconsistent dictionary keys in values_from_many raises error."""
    data = [{"id": 1, "name": "John"}, {"id": 2, "email": "jane@test.com"}]
    with pytest.raises(SQLBuilderError, match="do not match expected keys"):
        sql.insert("users").values_from_many(data).build()


def test_insert_with_sql_raw_expressions() -> None:
    """Test INSERT with sql.raw expressions."""
    query = sql.insert("logs").values(message="Test message", created_at=sql.raw("NOW()"), uuid=sql.raw("UUID()"))
    stmt = query.build()

    assert "INSERT INTO" in stmt.sql
    assert "NOW()" in stmt.sql
    assert "UUID()" in stmt.sql
    assert "message" in stmt.parameters
    assert stmt.parameters["message"] == "Test message"


def test_insert_with_sql_raw_parameters() -> None:
    """Test INSERT with sql.raw that has parameters."""
    query = sql.insert("users").values(
        name="John", computed_field=sql.raw("COALESCE(:fallback, 'default')", fallback="custom")
    )
    stmt = query.build()

    assert "INSERT INTO" in stmt.sql
    assert "COALESCE" in stmt.sql
    assert "name" in stmt.parameters
    assert "fallback" in stmt.parameters
    assert stmt.parameters["name"] == "John"
    assert stmt.parameters["fallback"] == "custom"


def test_on_conflict_do_nothing_basic() -> None:
    """Test basic ON CONFLICT DO NOTHING."""
    query = sql.insert("users").values(id=1, name="John").on_conflict("id").do_nothing()
    stmt = query.build()

    assert "INSERT INTO" in stmt.sql
    assert "ON CONFLICT" in stmt.sql
    assert "DO NOTHING" in stmt.sql
    assert "id" in stmt.parameters


def test_on_conflict_do_update_basic() -> None:
    """Test basic ON CONFLICT DO UPDATE."""
    query = sql.insert("users").values(id=1, name="John").on_conflict("id").do_update(name="Updated")
    stmt = query.build()

    assert "INSERT INTO" in stmt.sql
    assert "ON CONFLICT" in stmt.sql
    assert "DO UPDATE" in stmt.sql
    assert "SET" in stmt.sql
    assert "name_1" in stmt.parameters
    assert stmt.parameters["name_1"] == "Updated"


@pytest.mark.parametrize("dialect", ["postgres", "sqlite", "duckdb"])
def test_on_conflict_do_update_keeps_excluded_assignment_order(dialect: str) -> None:
    """Optimized builds keep the target column on the left of each conflict SET assignment."""
    query = (
        sql
        .insert("users", dialect=dialect)
        .values(id=1, name="John")
        .on_conflict("id")
        .do_update(name=exp.column("name", table="excluded"))
    )

    built_sql = " ".join(query.build().sql.split())
    statement_sql = " ".join(query.to_statement().sql.split())

    assert 'SET "name" = "excluded"."name"' in built_sql
    assert 'ON CONFLICT("id") DO UPDATE SET "name" = "excluded"."name"' in statement_sql


def test_on_conflict_statement_quotes_reserved_word_identifiers() -> None:
    """Statements built for execution quote conflict targets and SET columns that are reserved words."""
    from sqlspec.adapters.sqlite import SqliteConfig

    config = SqliteConfig(connection_config={"database": ":memory:"})
    with config.provide_session() as driver:
        driver.execute('CREATE TABLE "t" ("order" INTEGER PRIMARY KEY, "group" TEXT)')
        driver.execute("INSERT INTO t VALUES (1, 'a')")

        driver.execute(sql.insert("t").values(**{"order": 1, "group": "b"}).on_conflict("order").do_update(group="z"))
        driver.execute(sql.insert("t").values(**{"order": 1, "group": "c"}).on_conflict("order").do_nothing())

        assert driver.select('SELECT "order", "group" FROM t') == [{"order": 1, "group": "z"}]
    config.close_pool()


def test_on_duplicate_key_update_keeps_assignment_order() -> None:
    """Optimized builds keep the target column on the left of each duplicate-key assignment."""
    query = (
        sql
        .insert("users", dialect="mysql")
        .values(id=1, name="John")
        .on_duplicate_key_update(name=exp.column("name", table="excluded"))
    )

    built_sql = " ".join(query.build().sql.split())

    assert "UPDATE `name` = `excluded`.`name`" in built_sql


def test_on_conflict_multiple_columns() -> None:
    """Test ON CONFLICT with multiple columns."""
    query = (
        sql
        .insert("users")
        .values(email="john@test.com", username="john", name="John")
        .on_conflict("email", "username")
        .do_nothing()
    )
    stmt = query.build()

    assert "ON CONFLICT" in stmt.sql
    assert "email" in stmt.sql
    assert "username" in stmt.sql


def test_on_conflict_no_columns() -> None:
    """Test ON CONFLICT without specific columns."""
    query = sql.insert("users").values(id=1, name="John").on_conflict().do_nothing()
    stmt = query.build()

    assert "ON CONFLICT" in stmt.sql
    assert "DO NOTHING" in stmt.sql

    assert "ON CONFLICT(" not in stmt.sql


def test_on_conflict_do_update_with_sql_raw() -> None:
    """Test ON CONFLICT DO UPDATE with sql.raw expressions."""
    query = (
        sql
        .insert("users")
        .values(id=1, name="John")
        .on_conflict("id")
        .do_update(updated_at=sql.raw("NOW()"), name="Updated")
    )
    stmt = query.build()

    assert "ON CONFLICT" in stmt.sql
    assert "DO UPDATE" in stmt.sql
    assert "NOW()" in stmt.sql
    assert "name_1" in stmt.parameters


def test_on_conflict_convenience_method() -> None:
    """Test on_conflict_do_nothing convenience method."""
    query = sql.insert("users").values(id=1, name="John").on_conflict_do_nothing("id")
    stmt = query.build()

    assert "ON CONFLICT" in stmt.sql
    assert "DO NOTHING" in stmt.sql


def test_legacy_on_duplicate_key_update() -> None:
    """Test legacy on_duplicate_key_update method."""
    query = (
        sql
        .insert("users")
        .values(id=1, name="John")
        .on_duplicate_key_update(name="Updated", updated_at=sql.raw("NOW()"))
    )
    stmt = query.build()

    assert "ON DUPLICATE KEY UPDATE" in stmt.sql
    assert "NOW()" in stmt.sql


def test_on_conflict_chaining() -> None:
    """Test ON CONFLICT method chaining."""
    query = (
        sql
        .insert("users")
        .values(id=1, name="John")
        .on_conflict("id")
        .do_update(name="Updated")
        .returning("id", "name")
    )
    stmt = query.build()

    assert "ON CONFLICT" in stmt.sql
    assert "DO UPDATE" in stmt.sql
    assert "RETURNING" in stmt.sql


def test_on_conflict_type_safety() -> None:
    """Test ON CONFLICT method return types for chaining."""
    insert_builder = sql.insert("users").values(id=1, name="John")

    conflict_builder = insert_builder.on_conflict("id")
    assert hasattr(conflict_builder, "do_nothing")
    assert hasattr(conflict_builder, "do_update")

    final_builder = conflict_builder.do_nothing()
    assert hasattr(final_builder, "returning")
    assert hasattr(final_builder, "build")


def test_on_conflict_empty_do_update() -> None:
    """Test ON CONFLICT DO UPDATE with no arguments."""
    query = sql.insert("users").values(id=1, name="John").on_conflict("id").do_update()
    stmt = query.build()

    assert "ON CONFLICT" in stmt.sql
    assert "DO UPDATE" in stmt.sql


def test_on_conflict_parameter_merging() -> None:
    """Test that ON CONFLICT properly merges parameters from SQL objects."""
    query = (
        sql
        .insert("users")
        .values(id=1, name="John")
        .on_conflict("id")
        .do_update(name=sql.raw("COALESCE(:new_name, name)", new_name="Updated"), updated_at=sql.raw("NOW()"))
    )
    stmt = query.build()

    assert "new_name" in stmt.parameters
    assert stmt.parameters["new_name"] == "Updated"
    assert "NOW()" in stmt.sql


def test_on_conflict_with_values_from() -> None:
    """Test ON CONFLICT with values_from."""
    data = {"id": 1, "name": "John", "email": "john@test.com"}
    query = sql.insert("users").values_from(data).on_conflict("id").do_update(name="Updated")
    stmt = query.build()

    assert "ON CONFLICT" in stmt.sql
    assert "DO UPDATE" in stmt.sql
    assert "name_1" in stmt.parameters
    assert stmt.parameters["name_1"] == "Updated"


def test_on_conflict_mysql_transpiles() -> None:
    """Test ON CONFLICT transpiles to ON DUPLICATE KEY UPDATE for MySQL and MariaDB."""
    query = sql.insert("users").values(id=1, name="John").on_conflict("id").do_update(name="Updated")
    mysql_stmt = query.build(dialect="mysql")
    assert "ON DUPLICATE KEY UPDATE" in mysql_stmt.sql
    assert "ON CONFLICT" not in mysql_stmt.sql
    assert "name" in mysql_stmt.sql

    mariadb_stmt = query.build(dialect="mariadb")
    assert "ON DUPLICATE KEY UPDATE" in mariadb_stmt.sql
    assert "ON CONFLICT" not in mariadb_stmt.sql

    nothing_query = sql.insert("users").values(id=1, name="John").on_conflict("id").do_nothing()
    nothing_mysql = nothing_query.build(dialect="mysql")
    assert "ON DUPLICATE KEY UPDATE" in nothing_mysql.sql
    assert "id = id" in nothing_mysql.sql or "`id` = `id`" in nothing_mysql.sql


@pytest.mark.parametrize("dialect", ["oracle", "tsql", "mssql", "bigquery"])
def test_on_conflict_raises_oracle_tsql(dialect: str) -> None:
    """Test ON CONFLICT raises SQLBuilderError mentioning sql.merge() on unsupported dialects."""
    query = sql.insert("users").values(id=1, name="John").on_conflict("id").do_update(name="Updated")
    with pytest.raises(SQLBuilderError, match=r"sql\.merge\(\)"):
        query.build(dialect=dialect)


def test_on_conflict_postgres_unchanged() -> None:
    """Test ON CONFLICT on postgres retains standard ON CONFLICT clause."""
    query = sql.insert("users").values(id=1, name="John").on_conflict("id").do_update(name="Updated")
    stmt = query.build(dialect="postgres")
    assert "ON CONFLICT" in stmt.sql
    assert "DO UPDATE" in stmt.sql
    assert "ON DUPLICATE KEY UPDATE" not in stmt.sql


def test_to_statement_translates_conflict_without_mutating_builder() -> None:
    from sqlspec.core import StatementConfig

    query = (
        sql
        .insert("users")
        .values(id=1, name="John")
        .on_conflict("id")
        .do_update(name=exp.column("name", table="excluded"))
    )
    original = query.build(dialect="postgres").sql
    statement = query.to_statement(StatementConfig(dialect="mysql"))
    assert "ON DUPLICATE KEY UPDATE" in statement.sql
    assert "VALUES(" in statement.sql
    assert "excluded" not in statement.sql.lower()
    assert query.build(dialect="postgres").sql == original


def test_to_statement_rejects_unsupported_conflict() -> None:
    from sqlspec.core import StatementConfig

    query = sql.insert("users").values(id=1).on_conflict("id").do_nothing()
    with pytest.raises(SQLBuilderError, match=r"sql\.merge\(\)"):
        query.to_statement(StatementConfig(dialect="oracle"))


def test_mysql_do_nothing_requires_known_column() -> None:
    query = sql.insert("users").values(1).on_conflict().do_nothing()
    with pytest.raises(SQLBuilderError, match="requires a conflict column"):
        query.build(dialect="mysql")


@pytest.mark.parametrize("argument", ["where", "index_predicate", "constraint"])
def test_mysql_rejects_conflict_semantics_it_cannot_preserve(argument: str) -> None:
    query = sql.insert("users").values(id=1).on_conflict("id").do_update(id=2)
    conflict = query.get_insert_expression().args["conflict"]
    conflict.set(argument, exp.to_identifier("restricted"))
    with pytest.raises(SQLBuilderError, match="cannot preserve"):
        query.build(dialect="mysql")


@pytest.mark.parametrize("dialect", ["spanner", "spangres"])
def test_spanner_native_conflicts(dialect: str) -> None:
    from sqlspec.core import StatementConfig

    query = sql.insert("users").values(id=1).on_conflict("id").do_nothing()
    assert "ON CONFLICT" in query.to_statement(StatementConfig(dialect=dialect)).sql
    query = sql.insert("users").values(id=1).on_conflict("id").do_update(id=exp.column("id", table="excluded"))
    assert "DO UPDATE" in query.build(dialect=dialect).sql


def test_spangres_rejects_non_insert_value_conflict_update() -> None:
    query = sql.insert("users").values(id=1).on_conflict("id").do_update(id=2)
    with pytest.raises(SQLBuilderError, match="require excluded column values"):
        query.build(dialect="spangres")


def _normalized_sql(statement: Any) -> str:
    return " ".join(statement.sql.split())


CONFLICT_PREFIX_POSTGRES = 'INSERT INTO "t" ( "id" ) VALUES (%(id)s) ON CONFLICT("id") DO UPDATE SET '


@pytest.mark.parametrize(
    ("dialect", "value", "expected"),
    [
        pytest.param("postgres", sql.raw("a + 1"), CONFLICT_PREFIX_POSTGRES + '"a" = "a" + 1', id="raw-postgres"),
        pytest.param(
            "sqlite",
            sql.raw("a + 1"),
            'INSERT INTO "t" ( "id" ) VALUES (:id) ON CONFLICT("id") DO UPDATE SET "a" = "a" + 1',
            id="raw-sqlite",
        ),
        pytest.param(
            "duckdb",
            sql.raw("a + 1"),
            'INSERT INTO "t" ( "id" ) VALUES ($id) ON CONFLICT("id") DO UPDATE SET "a" = "a" + 1',
            id="raw-duckdb",
        ),
        pytest.param(
            "postgres",
            exp.Nullif(this=exp.column("a"), expression=exp.Literal.number(0)),
            CONFLICT_PREFIX_POSTGRES + '"a" = NULLIF("a", 0)',
            id="nullif",
        ),
        pytest.param(
            "postgres",
            exp.Left(this=exp.column("a"), expression=exp.Literal.number(2)),
            CONFLICT_PREFIX_POSTGRES + '"a" = LEFT("a", 2)',
            id="left",
        ),
        pytest.param(
            "postgres",
            exp.condition("a > 1 AND b < 2"),
            CONFLICT_PREFIX_POSTGRES + '"a" = "a" > 1 AND "b" < 2',
            id="and",
        ),
        pytest.param("postgres", exp.var("DEFAULT"), CONFLICT_PREFIX_POSTGRES + '"a" = DEFAULT', id="var"),
    ],
)
def test_conflict_update_renders_full_expression(dialect: str, value: Any, expected: str) -> None:
    """Conflict updates render both operands of a fragment or SQLGlot node."""
    statement = sql.insert("t", dialect=dialect).columns("id").values(1).on_conflict("id").do_update(a=value).build()

    assert _normalized_sql(statement) == expected
    assert statement.parameters == {"id": 1}


def test_conflict_update_keeps_fragment_parameters() -> None:
    """A parameterized fragment keeps its expression and merges its parameters."""
    statement = (
        sql
        .insert("t", dialect="postgres")
        .columns("id")
        .values(1)
        .on_conflict("id")
        .do_update(a=sql.raw("a + :p", p=3))
        .build()
    )

    assert _normalized_sql(statement) == CONFLICT_PREFIX_POSTGRES + '"a" = "a" + %(p)s'
    assert statement.parameters == {"id": 1, "p": 3}


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param(sql.raw("a + 1"), "`a` = `a` + 1", id="raw"),
        pytest.param(
            exp.Nullif(this=exp.column("a"), expression=exp.Literal.number(0)), "`a` = NULLIF(`a`, 0)", id="nullif"
        ),
    ],
)
def test_duplicate_key_update_renders_full_expression(value: Any, expected: str) -> None:
    """Duplicate-key updates render both operands of a fragment or SQLGlot node."""
    statement = sql.insert("t", dialect="mysql").columns("id").values(1).on_duplicate_key_update(a=value).build()

    assert _normalized_sql(statement) == f"INSERT INTO `t` ( `id` ) VALUES (:id) ON DUPLICATE KEY UPDATE {expected}"


EXPRESSION_VALUES = [
    pytest.param(sql.column("b"), '"b"', "`b`", id="column"),
    pytest.param(sql.column("b").cast("INT"), 'CAST("b" AS INT)', "CAST(`b` AS SIGNED)", id="function_column"),
    pytest.param(sql.column("b") > 1, '"b" > 1', "`b` > 1", id="column_expression"),
    pytest.param(
        sql.case().when("x > 1", 1).else_(0).end(),
        'CASE WHEN "x" > 1 THEN 1 ELSE 0 END',
        "CASE WHEN `x` > 1 THEN 1 ELSE 0 END",
        id="case",
    ),
    pytest.param(sql.count("b"), 'COUNT("b")', "COUNT(`b`)", id="aggregate"),
    pytest.param(sql.coalesce("b", "c"), 'COALESCE("b", "c")', "COALESCE(`b`, `c`)", id="conversion"),
]


def _query() -> Any:
    return sql.select("x").from_("y").where_eq("z", 5)


@pytest.mark.parametrize(("value", "postgres", "mysql"), EXPRESSION_VALUES)
def test_insert_values_renders_expression_values(value: Any, postgres: str, mysql: str) -> None:
    """Positional INSERT values render wrappers as SQL."""
    statement = sql.insert("t", dialect="postgres").columns("a").values(value).build()

    assert _normalized_sql(statement) == f'INSERT INTO "t" ( "a" ) VALUES ({postgres})'
    assert statement.parameters == {}


@pytest.mark.parametrize(("value", "postgres", "mysql"), EXPRESSION_VALUES)
def test_insert_keyword_values_renders_expression_values(value: Any, postgres: str, mysql: str) -> None:
    """Keyword INSERT values render wrappers as SQL."""
    statement = sql.insert("t", dialect="postgres").values(a=value).build()

    assert _normalized_sql(statement) == f'INSERT INTO "t" ( "a" ) VALUES ({postgres})'
    assert statement.parameters == {}


@pytest.mark.parametrize(("value", "postgres", "mysql"), EXPRESSION_VALUES)
def test_conflict_update_renders_expression_values(value: Any, postgres: str, mysql: str) -> None:
    """Conflict updates render wrappers as SQL."""
    statement = sql.insert("t", dialect="postgres").columns("id").values(1).on_conflict("id").do_update(a=value).build()

    assert _normalized_sql(statement) == f'{CONFLICT_PREFIX_POSTGRES}"a" = {postgres}'
    assert statement.parameters == {"id": 1}


@pytest.mark.parametrize(("value", "postgres", "mysql"), EXPRESSION_VALUES)
def test_duplicate_key_update_renders_expression_values(value: Any, postgres: str, mysql: str) -> None:
    """Duplicate-key updates render wrappers as SQL."""
    statement = sql.insert("t", dialect="mysql").columns("id").values(1).on_duplicate_key_update(a=value).build()

    assert _normalized_sql(statement) == f"INSERT INTO `t` ( `id` ) VALUES (:id) ON DUPLICATE KEY UPDATE `a` = {mysql}"
    assert statement.parameters == {"id": 1}


def test_insert_values_embeds_query_builder() -> None:
    """A query builder in VALUES embeds as a subquery with merged parameters."""
    statement = sql.insert("t", dialect="postgres").columns("a").values(_query()).build()

    assert _normalized_sql(statement) == 'INSERT INTO "t" ( "a" ) VALUES (( SELECT "x" FROM "y" WHERE "z" = %(z)s ))'
    assert statement.parameters == {"z": 5}


def test_conflict_update_embeds_query_builder() -> None:
    """A query builder in a conflict update embeds as a subquery with merged parameters."""
    statement = (
        sql.insert("t", dialect="postgres").columns("id").values(1).on_conflict("id").do_update(a=_query()).build()
    )

    assert _normalized_sql(statement) == f'{CONFLICT_PREFIX_POSTGRES}"a" = ( SELECT "x" FROM "y" WHERE "z" = %(z)s )'
    assert statement.parameters == {"id": 1, "z": 5}


def test_conflict_update_parenthesizes_select_node() -> None:
    """A Select node renders as a parenthesized scalar subquery."""
    statement = (
        sql
        .insert("t", dialect="postgres")
        .columns("id")
        .values(1)
        .on_conflict("id")
        .do_update(a=exp.select("x").from_("y"))
        .build()
    )

    assert _normalized_sql(statement) == f'{CONFLICT_PREFIX_POSTGRES}"a" = ( SELECT "x" FROM "y" )'
    assert statement.parameters == {"id": 1}


def test_conflict_update_leaves_caller_node_detached() -> None:
    """One node assigned to two columns renders twice and stays detached."""
    node = cast("exp.Expr", exp.maybe_parse("a + 1"))

    statement = (
        sql.insert("t", dialect="postgres").columns("id").values(1).on_conflict("id").do_update(a=node, c=node).build()
    )

    assert _normalized_sql(statement) == f'{CONFLICT_PREFIX_POSTGRES}"a" = "a" + 1, "c" = "a" + 1'
    assert node.parent is None
