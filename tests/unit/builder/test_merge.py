"""Unit tests for MERGE builder functionality."""

from typing import Any, cast

import pytest
from sqlglot import exp

import sqlspec.builder._merge as merge_module
from sqlspec import sql
from sqlspec.builder import Merge
from sqlspec.exceptions import DialectNotSupportedError, SQLBuilderError
from tests.conftest import requires_interpreted


def test_merge_basic_structure() -> None:
    """Test basic MERGE statement structure with all required clauses."""
    query = (
        sql
        .merge()
        .into("products")
        .using({"id": 1, "name": "Widget", "price": 29.99}, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name", price="src.price")
        .when_not_matched_then_insert(columns=["id", "name", "price"], values=[1, "Widget", 29.99])
    )
    stmt = query.build()

    assert "MERGE INTO" in stmt.sql.upper()
    assert "products" in stmt.sql
    assert "USING" in stmt.sql.upper()
    assert "WHEN MATCHED" in stmt.sql.upper()
    assert "WHEN NOT MATCHED" in stmt.sql.upper()


def test_merge_into_clause() -> None:
    """Test MERGE INTO clause with table name."""
    query = sql.merge().into("users")
    stmt = query.build()

    assert "MERGE INTO" in stmt.sql.upper()
    assert "users" in stmt.sql


def test_merge_into_with_alias() -> None:
    """Test MERGE INTO clause with table alias."""
    query = sql.merge().into("products", alias="t")
    stmt = query.build()

    assert "MERGE INTO" in stmt.sql.upper()
    assert "products" in stmt.sql


def test_merge_using_table_name() -> None:
    """Test MERGE USING clause with table name as source."""
    query = sql.merge().into("target").using("source", alias="src").on("target.id = src.id")
    stmt = query.build()

    assert "USING" in stmt.sql.upper()
    assert "source" in stmt.sql


def test_merge_using_dict_single_row() -> None:
    """Test MERGE USING clause with dict as source (single row)."""
    query = (
        sql.merge().into("products").using({"id": 1, "name": "Widget", "price": 29.99}, alias="src").on("t.id = src.id")
    )
    stmt = query.build()

    assert "USING" in stmt.sql.upper()
    assert "SELECT" in stmt.sql.upper()
    assert "id" in stmt.parameters
    assert "name" in stmt.parameters
    assert "price" in stmt.parameters
    assert stmt.parameters["id"] == 1
    assert stmt.parameters["name"] == "Widget"
    assert stmt.parameters["price"] == 29.99


def test_merge_using_dict_parameter_binding() -> None:
    """Test that dict values are properly parameterized in USING clause."""
    query = sql.merge().into("users").using({"user_id": 42, "username": "john_doe"}, alias="s").on("t.id = s.user_id")
    stmt = query.build()

    assert "user_id" in stmt.parameters
    assert "username" in stmt.parameters
    assert stmt.parameters["user_id"] == 42
    assert stmt.parameters["username"] == "john_doe"


def test_merge_on_clause_string() -> None:
    """Test MERGE ON clause with string condition."""
    query = sql.merge().into("products", alias="t").using("staging", alias="s").on("t.id = s.id")
    stmt = query.build()

    assert "ON" in stmt.sql.upper()
    assert "t.id" in stmt.sql or "t" in stmt.sql
    assert "s.id" in stmt.sql or "s" in stmt.sql


def test_merge_on_clause_complex_condition() -> None:
    """Test MERGE ON clause with complex condition."""
    query = (
        sql
        .merge()
        .into("orders", alias="t")
        .using("new_orders", alias="s")
        .on("t.order_id = s.order_id AND t.customer_id = s.customer_id")
    )
    stmt = query.build()

    assert "ON" in stmt.sql.upper()
    assert "AND" in stmt.sql.upper()


def test_merge_when_matched_update_with_dict() -> None:
    """Test WHEN MATCHED THEN UPDATE with dict."""
    query = (
        sql
        .merge()
        .into("products", alias="t")
        .using("staging", alias="s")
        .on("t.id = s.id")
        .when_matched_then_update({"name": "s.name", "price": "s.price"})
    )
    stmt = query.build()

    assert "WHEN MATCHED" in stmt.sql.upper()
    assert "UPDATE" in stmt.sql.upper()
    assert "SET" in stmt.sql.upper()


def test_merge_when_matched_update_with_kwargs() -> None:
    """Test WHEN MATCHED THEN UPDATE with kwargs."""
    query = (
        sql
        .merge()
        .into("products", alias="t")
        .using("staging", alias="s")
        .on("t.id = s.id")
        .when_matched_then_update(name="s.name", price="s.price", updated_at="CURRENT_TIMESTAMP")
    )
    stmt = query.build()

    assert "WHEN MATCHED" in stmt.sql.upper()
    assert "UPDATE" in stmt.sql.upper()
    assert "SET" in stmt.sql.upper()


def test_merge_when_matched_update_parameter_values() -> None:
    """Test WHEN MATCHED THEN UPDATE with literal parameter values."""
    query = (
        sql
        .merge()
        .into("products", alias="t")
        .using({"id": 1}, alias="s")
        .on("t.id = s.id")
        .when_matched_then_update(status="discontinued", stock=0)
    )
    stmt = query.build()

    assert "WHEN MATCHED" in stmt.sql.upper()
    assert "status" in stmt.parameters
    assert "stock" in stmt.parameters
    assert stmt.parameters["status"] == "discontinued"
    assert stmt.parameters["stock"] == 0


def test_merge_when_matched_update_with_condition() -> None:
    """Test WHEN MATCHED THEN UPDATE with WHERE condition."""
    query = (
        sql
        .merge()
        .into("products", alias="t")
        .using("staging", alias="s")
        .on("t.id = s.id")
        .when_matched_then_update({"price": "s.price"}, condition="s.price < t.price")
    )
    stmt = query.build()

    assert "WHEN MATCHED" in stmt.sql.upper()
    assert "UPDATE" in stmt.sql.upper()
    upper_sql = " ".join(stmt.sql.upper().split())
    assert "AND" in upper_sql or "WHERE" in upper_sql


def test_merge_when_matched_update_with_condition_oracle() -> None:
    """Oracle MERGE UPDATE condition renders as UPDATE SET ... WHERE predicate."""
    query = (
        sql
        .merge(dialect="oracle")
        .into("products", alias="t")
        .using({"id": 1, "new_price": 5.00}, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update({"price": "src.new_price"}, condition="src.new_price < t.price")
    )
    stmt = query.build()

    rendered = " ".join(stmt.sql.split())
    assert "UPDATE SET price = src.new_price WHERE src.new_price < t.price" in rendered


def test_merge_oracle_build_does_not_mutate_subsequent_postgres_build() -> None:
    """Building a MERGE query for Oracle must not mutate the AST for subsequent Postgres builds."""
    query = (
        sql
        .merge()
        .into("products", alias="t")
        .using("staging", alias="src")
        .on("t.id = src.id")
        .when_matched_then_update({"price": "src.new_price"}, condition="src.new_price < t.price")
    )

    oracle_stmt = query.build(dialect="oracle")
    postgres_stmt = query.build(dialect="postgres")

    oracle_sql = " ".join(oracle_stmt.sql.split())
    postgres_sql = " ".join(postgres_stmt.sql.split())
    assert "UPDATE SET price = src.new_price WHERE src.new_price < t.price" in oracle_sql
    assert (
        'WHEN MATCHED AND "src"."new_price" < "t"."price" THEN UPDATE SET "price" = "src"."new_price"' in postgres_sql
    )
    assert "WHERE" not in postgres_sql


def test_merge_when_matched_update_no_values_error() -> None:
    """Test that WHEN MATCHED UPDATE without values raises error."""
    query = sql.merge().into("products", alias="t").using("staging", alias="s").on("t.id = s.id")

    with pytest.raises(SQLBuilderError, match="No update values provided"):
        query.when_matched_then_update()


def test_merge_when_matched_delete() -> None:
    """Test WHEN MATCHED THEN DELETE clause."""
    query = (
        sql.merge().into("products", alias="t").using("staging", alias="s").on("t.id = s.id").when_matched_then_delete()
    )
    stmt = query.build()

    assert "WHEN MATCHED" in stmt.sql.upper()
    assert "DELETE" in stmt.sql.upper()


def test_merge_when_matched_delete_with_condition() -> None:
    """Test WHEN MATCHED THEN DELETE with WHERE condition."""
    query = (
        sql
        .merge()
        .into("products", alias="t")
        .using("staging", alias="s")
        .on("t.id = s.id")
        .when_matched_then_delete(condition="s.discontinued = 1")
    )
    stmt = query.build()

    assert "WHEN MATCHED" in stmt.sql.upper()
    assert "DELETE" in stmt.sql.upper()

    merge_expr = query.get_expression()
    assert merge_expr is not None
    whens = merge_expr.args.get("whens")
    assert whens is not None and whens.expressions
    last_when = whens.expressions[-1]
    assert "condition" in last_when.args
    assert "this" not in last_when.args


def test_merge_when_matched_update_stores_condition_field() -> None:
    """WHEN MATCHED UPDATE should place predicates in the condition arg."""
    query = (
        sql
        .merge()
        .into("products", alias="t")
        .using("staging", alias="s")
        .on("t.id = s.id")
        .when_matched_then_update({"price": "s.price"}, condition="s.price < t.price")
    )

    merge_expr = query.get_expression()
    assert merge_expr is not None
    whens = merge_expr.args.get("whens")
    assert whens is not None and whens.expressions
    last_when = whens.expressions[-1]
    assert "condition" in last_when.args
    assert "this" not in last_when.args


def test_merge_when_not_matched_insert_with_mapping() -> None:
    """Test WHEN NOT MATCHED THEN INSERT with dict mapping."""
    query = (
        sql
        .merge()
        .into("products", alias="t")
        .using("staging", alias="s")
        .on("t.id = s.id")
        .when_not_matched_then_insert({"id": 1, "name": "New Product", "price": 99.99})
    )
    stmt = query.build()

    assert "WHEN NOT MATCHED" in stmt.sql.upper()
    assert "INSERT" in stmt.sql.upper()
    assert "id" in stmt.parameters
    assert "name" in stmt.parameters
    assert "price" in stmt.parameters


def test_merge_when_not_matched_insert_with_kwargs() -> None:
    """Test WHEN NOT MATCHED THEN INSERT with kwargs."""
    query = (
        sql
        .merge()
        .into("products", alias="t")
        .using("staging", alias="s")
        .on("t.id = s.id")
        .when_not_matched_then_insert(id=1, name="New Product", price=99.99)
    )
    stmt = query.build()

    assert "WHEN NOT MATCHED" in stmt.sql.upper()
    assert "INSERT" in stmt.sql.upper()


def test_merge_when_not_matched_insert_with_columns_values() -> None:
    """Test WHEN NOT MATCHED THEN INSERT with explicit columns and values."""
    query = (
        sql
        .merge()
        .into("products", alias="t")
        .using("staging", alias="s")
        .on("t.id = s.id")
        .when_not_matched_then_insert(columns=["id", "name", "price"], values=[1, "Widget", 29.99])
    )
    stmt = query.build()

    assert "WHEN NOT MATCHED" in stmt.sql.upper()
    assert "INSERT" in stmt.sql.upper()


def test_merge_when_not_matched_insert_from_source_columns() -> None:
    """Test WHEN NOT MATCHED INSERT infers values from source table when only columns provided."""
    query = (
        sql
        .merge()
        .into("products", alias="t")
        .using("staging", alias="s")
        .on("t.id = s.id")
        .when_not_matched_then_insert(columns=["id", "name", "price"])
    )
    stmt = query.build()

    assert "WHEN NOT MATCHED" in stmt.sql.upper()
    assert "INSERT" in stmt.sql.upper()
    assert "s.id" in stmt.sql or "id" in stmt.sql
    assert "s.name" in stmt.sql or "name" in stmt.sql
    assert "s.price" in stmt.sql or "price" in stmt.sql


def test_merge_when_not_matched_insert_mismatched_columns_error() -> None:
    """Test that mismatched columns and values raises error."""
    query = sql.merge().into("products", alias="t").using("staging", alias="s").on("t.id = s.id")

    with pytest.raises(SQLBuilderError, match="Number of columns must match number of values"):
        query.when_not_matched_then_insert(columns=["id", "name"], values=[1])


def test_merge_when_not_matched_by_source_update() -> None:
    """Test WHEN NOT MATCHED BY SOURCE THEN UPDATE clause."""
    query = (
        sql
        .merge()
        .into("products", alias="t")
        .using("staging", alias="s")
        .on("t.id = s.id")
        .when_not_matched_by_source_then_update(status="archived")
    )
    stmt = query.build()

    assert "WHEN NOT MATCHED BY SOURCE" in stmt.sql.upper() or "WHEN NOT MATCHED" in stmt.sql.upper()
    assert "UPDATE" in stmt.sql.upper()
    assert "status" in stmt.parameters


def test_merge_when_not_matched_by_source_delete() -> None:
    """Test WHEN NOT MATCHED BY SOURCE THEN DELETE clause."""
    query = (
        sql
        .merge()
        .into("products", alias="t")
        .using("staging", alias="s")
        .on("t.id = s.id")
        .when_not_matched_by_source_then_delete()
    )
    stmt = query.build()

    assert "WHEN NOT MATCHED BY SOURCE" in stmt.sql.upper() or "WHEN NOT MATCHED" in stmt.sql.upper()
    assert "DELETE" in stmt.sql.upper()


def test_merge_multiple_when_clauses() -> None:
    """Test MERGE with multiple WHEN clauses."""
    query = (
        sql
        .merge()
        .into("products", alias="t")
        .using("staging", alias="s")
        .on("t.id = s.id")
        .when_matched_then_update(price="s.price", name="s.name")
        .when_not_matched_then_insert(id=1, name="New", price=10.00)
        .when_not_matched_by_source_then_update(status="inactive")
    )
    stmt = query.build()

    assert stmt.sql.upper().count("WHEN") >= 3


def test_merge_complex_scenario() -> None:
    """Test complex MERGE scenario with all clause types."""
    query = (
        sql
        .merge()
        .into("inventory", alias="inv")
        .using({"product_id": 100, "quantity": 50, "location": "Warehouse A"}, alias="src")
        .on("inv.product_id = src.product_id AND inv.location = src.location")
        .when_matched_then_update(quantity="inv.quantity + src.quantity", last_updated="CURRENT_TIMESTAMP")
        .when_not_matched_then_insert(product_id=100, quantity=50, location="Warehouse A")
    )
    stmt = query.build()

    assert "MERGE INTO" in stmt.sql.upper()
    assert "USING" in stmt.sql.upper()
    assert "ON" in stmt.sql.upper()
    assert "WHEN MATCHED" in stmt.sql.upper()
    assert "WHEN NOT MATCHED" in stmt.sql.upper()
    assert "product_id" in stmt.parameters
    assert "quantity" in stmt.parameters
    assert "location" in stmt.parameters


def test_merge_with_constructor_table() -> None:
    """Test MERGE with table specified in constructor."""
    query = (
        sql
        .merge("products")
        .using("staging", alias="s")
        .on("products.id = s.id")
        .when_matched_then_update(name="s.name")
    )
    stmt = query.build()

    assert "MERGE INTO" in stmt.sql.upper()
    assert "products" in stmt.sql


def test_merge_parameter_naming_uniqueness() -> None:
    """Test that parameters are named uniquely when columns collide."""
    query = (
        sql
        .merge()
        .into("products", alias="t")
        .using({"id": 1, "name": "First"}, alias="s")
        .on("t.id = s.id")
        .when_matched_then_update(name="Second")
        .when_not_matched_then_insert(id=2, name="Third")
    )
    stmt = query.build()

    params = stmt.parameters
    assert len(params) >= 5


def test_merge_null_value_handling() -> None:
    """Test MERGE with NULL values in parameters."""
    query = (
        sql
        .merge()
        .into("users", alias="t")
        .using({"id": 1, "email": None, "status": "active"}, alias="s")
        .on("t.id = s.id")
        .when_matched_then_update(email="s.email")
    )
    stmt = query.build()

    assert "email" in stmt.parameters
    assert stmt.parameters["email"] is None


def test_merge_column_reference_detection() -> None:
    """Test that column references are not parameterized."""
    query = (
        sql
        .merge()
        .into("products", alias="t")
        .using("staging", alias="s")
        .on("t.id = s.id")
        .when_matched_then_update(
            name="s.name", price="s.price", quantity="t.quantity + s.quantity", updated_at="CURRENT_TIMESTAMP"
        )
    )
    stmt = query.build()

    assert "CURRENT_TIMESTAMP" in stmt.sql.upper() or "TIMESTAMP" in stmt.sql.upper()


def test_merge_subquery_as_source() -> None:
    """Test MERGE with subquery as source."""
    subquery = sql.select("id", "name", "price").from_("staging").where("active = :active", active=True)
    query = (
        sql
        .merge()
        .into("products", alias="t")
        .using(subquery, alias="s")
        .on("t.id = s.id")
        .when_matched_then_update(name="s.name")
    )
    stmt = query.build()

    assert "USING" in stmt.sql.upper()
    assert "SELECT" in stmt.sql.upper()
    assert "active" in stmt.parameters


def test_merge_builds_valid_statement() -> None:
    """Test that MERGE builder returns a valid Statement object."""
    query = (
        sql
        .merge()
        .into("test_table")
        .using({"id": 1}, alias="s")
        .on("test_table.id = s.id")
        .when_matched_then_update(status="updated")
    )
    stmt = query.build()

    assert hasattr(stmt, "sql")
    assert hasattr(stmt, "parameters")
    assert isinstance(stmt.sql, str)
    assert isinstance(stmt.parameters, dict)
    assert len(stmt.sql) > 0


def test_merge_using_list_of_dicts() -> None:
    """Test MERGE with list of dicts as source for bulk upsert."""
    data = [
        {"id": 1, "name": "Widget A", "price": 19.99},
        {"id": 2, "name": "Widget B", "price": 29.99},
        {"id": 3, "name": "Widget C", "price": 39.99},
    ]
    query = (
        sql
        .merge()
        .into("products", alias="t")
        .using(data, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name", price="src.price")
        .when_not_matched_then_insert(columns=["id", "name", "price"])
    )
    stmt = query.build()

    assert "MERGE INTO" in stmt.sql.upper()
    assert "USING" in stmt.sql.upper()
    assert "WHEN MATCHED" in stmt.sql.upper()
    assert "WHEN NOT MATCHED" in stmt.sql.upper()
    assert len(stmt.parameters) > 0


def test_merge_using_list_of_dicts_postgres_dialect() -> None:
    """Test MERGE with list of dicts generates jsonb_to_recordset for PostgreSQL."""
    data = [{"id": 1, "name": "Widget A"}, {"id": 2, "name": "Widget B"}]
    query = (
        sql
        .merge(dialect="postgres")
        .into("products", alias="t")
        .using(data, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name")
    )
    stmt = query.build()

    assert "jsonb_to_recordset" in stmt.sql.lower()
    assert 'as "src"("id"' in stmt.sql.lower() or "as src(id" in stmt.sql.lower()
    assert "json_data" in stmt.parameters


def test_merge_using_single_dict_postgres_dialect() -> None:
    """Test MERGE with single dict generates jsonb_to_recordset for PostgreSQL."""
    data = {"id": 1, "name": "Widget A"}
    query = (
        sql
        .merge(dialect="postgres")
        .into("products", alias="t")
        .using(data, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name")
    )
    stmt = query.build()

    assert "jsonb_to_recordset" in stmt.sql.lower()
    assert 'as "src"("id"' in stmt.sql.lower() or "as src(id" in stmt.sql.lower()
    assert "json_data" in stmt.parameters


def test_merge_postgres_json_source_golden_sql_and_parameters() -> None:
    """PostgreSQL JSON source SQL and parameters should stay byte-identical."""
    data = [{"id": 1, "name": "Widget", "price": 12.5, "active": True, "meta": {"x": 1}, "missing": None}]
    query = (
        sql
        .merge(dialect="postgres")
        .into("products", alias="t")
        .using(data, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name", price="src.price")
        .when_not_matched_then_insert(columns=["id", "name", "price", "active", "meta", "missing"])
    )

    stmt = query.build()
    source_expr = query.get_expression().args["using"]  # type: ignore[index,union-attr]

    assert stmt.parameters == {"json_data": data}
    assert source_expr.sql(dialect="postgres") == (
        "(SELECT id, name, price, active, meta, missing FROM "
        "JSONB_TO_RECORDSET(CAST(%(json_data)s AS JSONB)) AS src_data(id INT, name TEXT, "
        "price DOUBLE PRECISION, active BOOLEAN, meta JSONB, missing DECIMAL)) AS "
        "src(id, name, price, active, meta, missing)"
    )
    assert (
        stmt.sql
        == """MERGE INTO products AS "t"
USING (
  SELECT
    "id",
    "name",
    "price",
    "active",
    "meta",
    "missing"
  FROM JSONB_TO_RECORDSET(CAST(%(json_data)s AS JSONB)) AS "src_data"("id" INT, "name" TEXT, "price" DOUBLE PRECISION, "active" BOOLEAN, "meta" JSONB, "missing" DECIMAL)
) AS "src"("id", "name", "price", "active", "meta", "missing")
ON (
  "t"."id" = "src"."id"
)
WHEN MATCHED THEN UPDATE SET
  "name" = "src"."name",
  "price" = "src"."price"
WHEN NOT MATCHED THEN INSERT ("id", "name", "price", "active", "meta", "missing") VALUES (
  "src"."id",
  "src"."name",
  "src"."price",
  "src"."active",
  "src"."meta",
  "src"."missing"
)"""
    )


def test_merge_oracle_json_source_golden_sql_and_parameters() -> None:
    """Oracle JSON source SQL and parameters should stay byte-identical."""
    data = [{"id": 1, "name": "Widget", "price": 12.5, "active": True, "meta": {"x": 1}, "missing": None}]
    query = (
        sql
        .merge(dialect="oracle")
        .into("products", alias="t")
        .using(data, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name", price="src.price")
        .when_not_matched_then_insert(columns=["id", "name", "price", "active", "meta", "missing"])
    )

    stmt = query.build()
    source_expr = query.get_expression().args["using"]  # type: ignore[index,union-attr]

    assert stmt.parameters == {
        "json_payload": '[{"id":1,"name":"Widget","price":12.5,"active":true,"meta":{"x":1},"missing":null}]'
    }
    assert source_expr.sql(dialect="oracle") == (
        "(SELECT id, name, price, active, meta, missing FROM JSON_TABLE(:json_payload, '$[*]' "
        "COLUMNS(id NUMBER PATH '$.id', name VARCHAR2(4000) PATH '$.name', price NUMBER PATH '$.price', "
        "active NUMBER(1) PATH '$.active', meta JSON PATH '$.meta', missing VARCHAR2(4000) PATH '$.missing'))) src"
    )
    assert (
        stmt.sql
        == """MERGE INTO products t
USING (
  SELECT
    id,
    name,
    price,
    active,
    meta,
    missing
  FROM JSON_TABLE(:json_payload, '$[*]' COLUMNS(
    id NUMBER PATH '$.id',
    name VARCHAR2(4000) PATH '$.name',
    price NUMBER PATH '$.price',
    active NUMBER(1) PATH '$.active',
    meta JSON PATH '$.meta',
    missing VARCHAR2(4000) PATH '$.missing'
  ))
) src
ON (
  t.id = src.id
)
WHEN MATCHED THEN UPDATE SET
  name = src.name,
  price = src.price
WHEN NOT MATCHED THEN INSERT (id, name, price, active, meta, missing) VALUES (src.id, src.name, src.price, src.active, src.meta, src.missing)"""
    )


@pytest.mark.parametrize("dialect", ["postgres", "oracle"])
def test_merge_json_source_avoids_parse_one(monkeypatch: pytest.MonkeyPatch, dialect: str) -> None:
    """JSON source construction should not round-trip through sqlglot.parse_one."""
    data = [{"id": 1, "name": "Widget"}]
    builder = sql.merge(dialect=dialect)

    def fail_parse_one(*args, **kwargs):  # type: ignore[no-untyped-def]
        raise AssertionError("parse_one should not be called for JSON source construction")

    merge_sqlglot = getattr(merge_module, "sg", None)
    if merge_sqlglot is not None:
        monkeypatch.setattr(merge_sqlglot, "parse_one", fail_parse_one)

    if dialect == "postgres":
        source_expr = builder._create_postgres_json_source(data, ["id", "name"], True, "src")  # pyright: ignore[reportPrivateUsage]
    else:
        source_expr = builder._create_oracle_json_source(data, ["id", "name"], "src")  # pyright: ignore[reportPrivateUsage]

    assert "SELECT" in source_expr.sql(dialect=dialect)


def test_merge_using_empty_list_raises_error() -> None:
    """Test MERGE with empty list raises appropriate error."""
    with pytest.raises(SQLBuilderError, match="Cannot create USING clause from empty list"):
        sql.merge().into("products").using([], alias="src")


def test_merge_mysql_dialect_raises_error() -> None:
    """Test MERGE with MySQL dialect raises DialectNotSupportedError."""

    query = (
        sql
        .merge(dialect="mysql")
        .into("products")
        .using({"id": 1, "name": "Widget"}, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name")
    )

    with pytest.raises(DialectNotSupportedError, match="MERGE statements are not supported in MYSQL"):
        query.build()


def test_merge_sqlite_dialect_raises_error() -> None:
    """Test MERGE with SQLite dialect raises DialectNotSupportedError."""

    query = (
        sql
        .merge(dialect="sqlite")
        .into("products")
        .using({"id": 1, "name": "Widget"}, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name")
    )

    with pytest.raises(DialectNotSupportedError, match="MERGE statements are not supported in SQLITE"):
        query.build()


def test_merge_duckdb_dialect_raises_error() -> None:
    """Test MERGE with DuckDB dialect raises DialectNotSupportedError."""

    query = (
        sql
        .merge(dialect="duckdb")
        .into("products")
        .using({"id": 1, "name": "Widget"}, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name")
    )

    with pytest.raises(DialectNotSupportedError, match="MERGE statements are not supported in DUCKDB"):
        query.build()


def test_merge_mysql_error_suggests_alternative() -> None:
    """Test MySQL error message includes INSERT ON DUPLICATE KEY suggestion."""

    query = sql.merge(dialect="mysql").into("products").using({"id": 1}, alias="src").on("t.id = src.id")

    with pytest.raises(DialectNotSupportedError, match=r"INSERT \.\.\. ON DUPLICATE KEY UPDATE"):
        query.build()


def test_merge_sqlite_error_suggests_alternative() -> None:
    """Test SQLite error message includes INSERT ON CONFLICT suggestion."""

    query = sql.merge(dialect="sqlite").into("products").using({"id": 1}, alias="src").on("t.id = src.id")

    with pytest.raises(DialectNotSupportedError, match=r"INSERT \.\.\. ON CONFLICT DO UPDATE"):
        query.build()


def test_merge_postgres_dialect_allowed() -> None:
    """Test MERGE with PostgreSQL dialect is allowed."""
    query = (
        sql
        .merge(dialect="postgres")
        .into("products")
        .using({"id": 1, "name": "Widget"}, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name")
    )

    stmt = query.build()
    assert "MERGE INTO" in stmt.sql.upper()


def test_merge_oracle_dialect_allowed() -> None:
    """Test MERGE with Oracle dialect is allowed."""
    query = (
        sql
        .merge(dialect="oracle")
        .into("products")
        .using({"id": 1, "name": "Widget"}, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name")
    )

    stmt = query.build()
    assert "MERGE INTO" in stmt.sql.upper()


def test_merge_type_inference_supports_sequence_subclasses() -> None:
    """MERGE type inference should treat builtin container subclasses as JSON payloads."""

    class JsonList(list):
        pass

    builder = sql.merge(dialect="postgres")

    assert builder._infer_postgres_type(JsonList([1, 2])) == "JSONB"  # pyright: ignore[reportPrivateUsage]


def test_merge_type_inference_preserves_bool_priority_for_oracle() -> None:
    """Boolean values should resolve before int-compatible handlers."""
    builder = sql.merge(dialect="oracle")

    assert builder._infer_oracle_type(True) == "NUMBER(1)"  # pyright: ignore[reportPrivateUsage]


def test_merge_no_dialect_allowed() -> None:
    """Test MERGE with no dialect specified is allowed."""
    query = (
        sql
        .merge()
        .into("products")
        .using({"id": 1, "name": "Widget"}, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name")
    )

    stmt = query.build()
    assert "MERGE INTO" in stmt.sql.upper()


def test_merge_property_shorthand() -> None:
    """Test sql.merge_ property returns new Merge builder."""

    query = (
        sql.merge_
        .into("products", alias="t")
        .using({"id": 1, "name": "Widget"}, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name")
        .when_not_matched_then_insert(id="src.id", name="src.name")
    )

    assert isinstance(query, Merge)

    built = query.build()
    assert "MERGE INTO" in built.sql
    assert "products" in built.sql.lower()


def test_merge_property_creates_new_instance() -> None:
    """Test sql.merge_ property returns new instance each time."""

    builder1 = sql.merge_
    builder2 = sql.merge_

    assert builder1 is not builder2
    assert isinstance(builder1, Merge)
    assert isinstance(builder2, Merge)


def test_merge_factory_sets_target_table_from_positional_arg() -> None:
    """sql.merge(table) should set INTO target without separate into()."""
    query = sql.merge("products").using("staging", alias="s").on("products.id = s.id").when_matched_then_delete()

    stmt = query.build()

    assert "products" in stmt.sql.lower()
    assert "merge" in stmt.sql.lower()


def test_merge_factory_rejects_non_merge_sql() -> None:
    """sql.merge() with non-MERGE SQL should raise helpful error."""
    bad_sql = "SELECT * FROM products"

    with pytest.raises(SQLBuilderError):
        sql.merge(bad_sql)


@requires_interpreted
def test_merge_corrupted_expression_state_raises_builder_error() -> None:
    """A merge builder whose expression state cannot be initialized raises SQLBuilderError."""

    class _BrokenMerge(Merge):
        def get_expression(self) -> "exp.Expr | None":
            return None

    with pytest.raises(SQLBuilderError, match="MERGE"):
        _BrokenMerge().into("target")

    with pytest.raises(SQLBuilderError, match="MERGE"):
        _BrokenMerge().when_matched_then_delete()


def test_merge_defaults_disable_optimization_unlike_other_builders() -> None:
    """Merge defaults enable_optimization=False while the other DML builders default to True."""
    assert sql.merge().enable_optimization is False
    assert sql.select("*").enable_optimization is True
    assert sql.insert("t").enable_optimization is True
    assert sql.update("t").enable_optimization is True
    assert sql.delete().enable_optimization is True
    assert Merge(enable_optimization=True).enable_optimization is True


MERGE_PREFIXES = {
    "tsql": "MERGE INTO [t] USING [s] AS s ON ( [t].[id] = [s].[id] ) WHEN ",
    "oracle": "MERGE INTO t USING s s ON ( t.id = s.id ) WHEN ",
    "postgres": 'MERGE INTO t USING "s" AS s ON ( "t"."id" = "s"."id" ) WHEN ',
}


def _merge_builder(dialect: str) -> Merge:
    return sql.merge(dialect=dialect).into("t").using("s", alias="s").on("t.id = s.id")


def _normalized_sql(statement: Any) -> str:
    return " ".join(statement.sql.split())


def _nullif() -> exp.Expr:
    return exp.Nullif(this=exp.column("a"), expression=exp.Literal.number(0))


def _left() -> exp.Expr:
    return exp.Left(this=exp.column("a"), expression=exp.Literal.number(2))


@pytest.mark.parametrize(
    ("dialect", "value", "suffix"),
    [
        pytest.param("tsql", sql.raw("a + 1"), "[a] = [a] + 1", id="tsql-raw"),
        pytest.param("tsql", _nullif(), "[a] = NULLIF([a], 0)", id="tsql-nullif"),
        pytest.param("oracle", sql.raw("a + 1"), "a = a + 1", id="oracle-raw"),
        pytest.param("oracle", _nullif(), "a = NULLIF(a, 0)", id="oracle-nullif"),
        pytest.param("postgres", sql.raw("a + 1"), '"a" = "a" + 1', id="postgres-raw"),
        pytest.param("postgres", _nullif(), '"a" = NULLIF("a", 0)', id="postgres-nullif"),
    ],
)
def test_merge_update_renders_full_expression(dialect: str, value: Any, suffix: str) -> None:
    """Matched updates render both operands of a fragment or SQLGlot node."""
    statement = _merge_builder(dialect).when_matched_then_update(a=value).build()

    assert _normalized_sql(statement) == f"{MERGE_PREFIXES[dialect]}MATCHED THEN UPDATE SET {suffix}"


@pytest.mark.parametrize(
    ("dialect", "value", "suffix"),
    [
        pytest.param("tsql", sql.raw("a + 1"), "([a]) VALUES ([a] + 1)", id="tsql-raw"),
        pytest.param("tsql", _left(), "([a]) VALUES (LEFT([a], 2))", id="tsql-left"),
        pytest.param("oracle", sql.raw("a + 1"), "(a) VALUES (a + 1)", id="oracle-raw"),
        pytest.param("oracle", _left(), "(a) VALUES (LEFT(a, 2))", id="oracle-left"),
        pytest.param("postgres", sql.raw("a + 1"), '("a") VALUES ("a" + 1)', id="postgres-raw"),
        pytest.param("postgres", _left(), '("a") VALUES (LEFT("a", 2))', id="postgres-left"),
    ],
)
def test_merge_insert_renders_full_expression(dialect: str, value: Any, suffix: str) -> None:
    """Not-matched inserts render both operands of a fragment or SQLGlot node."""
    statement = _merge_builder(dialect).when_not_matched_then_insert(columns=["a"], values=[value]).build()

    assert _normalized_sql(statement) == f"{MERGE_PREFIXES[dialect]}NOT MATCHED THEN INSERT {suffix}"


def test_merge_by_source_update_renders_full_expression() -> None:
    """By-source updates render both operands of a fragment."""
    statement = _merge_builder("tsql").when_not_matched_by_source_then_update(a=sql.raw("a + 1")).build()

    assert _normalized_sql(statement) == f"{MERGE_PREFIXES['tsql']}NOT MATCHED BY SOURCE THEN UPDATE SET [a] = [a] + 1"


MERGE_EXPRESSION_VALUES = [
    pytest.param(sql.column("b"), "[b]", id="column"),
    pytest.param(sql.column("b").cast("INT"), "CAST([b] AS INTEGER)", id="function-column"),
    pytest.param(sql.column("b") > 1, "[b] > 1", id="column-expression"),
    pytest.param(sql.case().when("x > 1", 1).else_(0).end(), "CASE WHEN [x] > 1 THEN 1 ELSE 0 END", id="case"),
    pytest.param(sql.count("b"), "COUNT([b])", id="aggregate"),
    pytest.param(sql.coalesce("b", "c"), "COALESCE([b], [c])", id="conversion"),
]


@pytest.mark.parametrize(("value", "fragment"), MERGE_EXPRESSION_VALUES)
@pytest.mark.parametrize(
    ("apply", "suffix"),
    [
        pytest.param(
            lambda merge, value: merge.when_matched_then_update(a=value),
            "MATCHED THEN UPDATE SET [a] = {fragment}",
            id="matched",
        ),
        pytest.param(
            lambda merge, value: merge.when_not_matched_then_insert(columns=["a"], values=[value]),
            "NOT MATCHED THEN INSERT ([a]) VALUES ({fragment})",
            id="not-matched",
        ),
        pytest.param(
            lambda merge, value: merge.when_not_matched_by_source_then_update(a=value),
            "NOT MATCHED BY SOURCE THEN UPDATE SET [a] = {fragment}",
            id="by-source",
        ),
    ],
)
def test_merge_clauses_render_expression_values(apply: Any, suffix: str, value: Any, fragment: str) -> None:
    """Every MERGE clause renders wrappers as SQL and binds nothing."""
    statement = apply(_merge_builder("tsql"), value).build()

    assert _normalized_sql(statement) == MERGE_PREFIXES["tsql"] + suffix.format(fragment=fragment)
    assert statement.parameters == {}


@pytest.mark.parametrize(
    ("dialect", "expected"),
    [
        pytest.param("oracle", "a = b, c = COUNT(e)", id="oracle"),
        pytest.param("postgres", '"a" = "b", "c" = COUNT("e")', id="postgres"),
    ],
)
def test_merge_update_renders_values_per_dialect(dialect: str, expected: str) -> None:
    """Wrappers render with the quoting of the target dialect."""
    statement = _merge_builder(dialect).when_matched_then_update(a=sql.column("b"), c=sql.count("e")).build()

    assert _normalized_sql(statement) == f"{MERGE_PREFIXES[dialect]}MATCHED THEN UPDATE SET {expected}"
    assert statement.parameters == {}


def test_merge_embeds_query_builder() -> None:
    """A query builder embeds as a subquery with merged parameters."""
    query = sql.select("x").from_("y").where_eq("z", 5)

    statement = _merge_builder("tsql").when_matched_then_update(a=query).build()

    assert _normalized_sql(statement) == (
        f"{MERGE_PREFIXES['tsql']}MATCHED THEN UPDATE SET [a] = ( SELECT [x] FROM [y] WHERE [z] = :z )"
    )
    assert statement.parameters == {"z": 5}


def test_merge_keeps_string_column_reference() -> None:
    """A qualified string keeps its column-reference shorthand."""
    statement = _merge_builder("tsql").when_matched_then_update(a="s.a").build()

    assert _normalized_sql(statement) == f"{MERGE_PREFIXES['tsql']}MATCHED THEN UPDATE SET [a] = [s].[a]"
    assert statement.parameters == {}


@pytest.mark.parametrize(
    ("value", "expected_parameters"),
    [pytest.param("text", {"a": "text"}, id="text"), pytest.param(1, {"a": 1}, id="number")],
)
def test_merge_keeps_plain_values_bound(value: Any, expected_parameters: dict[str, Any]) -> None:
    """Plain values bind under the assignment's column name."""
    statement = _merge_builder("tsql").when_matched_then_update(a=value).build()

    assert _normalized_sql(statement) == f"{MERGE_PREFIXES['tsql']}MATCHED THEN UPDATE SET [a] = :a"
    assert statement.parameters == expected_parameters


def test_merge_leaves_caller_node_detached() -> None:
    """One node assigned to two columns renders twice and stays detached."""
    node = cast("exp.Expr", exp.maybe_parse("a + 1"))

    statement = _merge_builder("tsql").when_matched_then_update(a=node, c=node).build()

    assert _normalized_sql(statement) == f"{MERGE_PREFIXES['tsql']}MATCHED THEN UPDATE SET [a] = [a] + 1, [c] = [a] + 1"
    assert node.parent is None


def test_merge_rejects_vector_distance_on_unsupported_dialect() -> None:
    """A vector distance on a dialect without vector support raises instead of losing its operand."""
    distance = sql.column("v").vector_distance([0.1, 0.2])

    with pytest.raises(SQLBuilderError, match="Vector distance is not supported"):
        _merge_builder("tsql").when_matched_then_update(a=distance).build()
