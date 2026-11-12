"""Teradata public builder contracts."""

from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from sqlglot import exp, parse_one

from sqlspec.builder import sql
from sqlspec.exceptions import SQLBuilderError


def test_teradata_alias_pagination() -> None:
    query = sql.select("id", dialect="vantage").from_("users").order_by("id").limit(10).offset(20)
    first = " ".join(query.build().sql.split())
    assert "QUALIFY ROW_NUMBER()" in first
    assert "BETWEEN 21 AND 30" in first
    assert " ".join(query.build().sql.split()) == first


def test_teradata_merge_column_types() -> None:
    query = (
        sql
        .merge(dialect="teradata")
        .into("t")
        .using(
            [
                {"id": 1, "name": "ab", "amount": Decimal("12.34"), "active": True},
                {"id": 2, "name": "abcdef", "amount": Decimal("1.2"), "active": False},
            ],
            alias="src",
        )
        .on("t.id = src.id")
        .when_matched_then_delete()
        .build()
    )
    assert query.sql.count("VARCHAR(6)") == 2
    assert query.sql.count("DECIMAL(18, 2)") == 2
    assert query.sql.count("BYTEINT") == 2
    assert "UNION ALL" in query.sql
    assert query.parameters["name"] == "ab"


@pytest.mark.parametrize("amount", [Decimal("NaN"), Decimal("Infinity"), Decimal("1e39")])
def test_teradata_merge_rejects_unrepresentable_decimal(amount: Decimal) -> None:
    with pytest.raises(SQLBuilderError):
        sql.merge(dialect="teradata").into("t").using({"amount": amount})


def test_teradata_merge_large_and_structured_values() -> None:
    query = (
        sql
        .merge(dialect="teradata")
        .into("t")
        .using(
            {
                "doc": {"a": 1},
                "note": None,
                "raw": b"x" * 64001,
                "ts": datetime(2026, 1, 1, tzinfo=UTC),
                "small": bytearray(b"xy"),
            },
            alias="src",
        )
        .on("t.id = src.id")
        .when_matched_then_delete()
        .build()
    )
    assert "CLOB" in query.sql
    assert "VARCHAR(32000)" in query.sql
    assert "BLOB" in query.sql
    assert "TIMESTAMP WITH TIME ZONE" in query.sql
    assert "VARBYTE(2)" in query.sql
    assert query.parameters["doc"] == '{"a":1}'
    assert query.parameters["small"] == b"xy"


def test_teradata_merge_mixed_date_and_datetime_columns() -> None:
    naive_query = (
        sql
        .merge(dialect="teradata")
        .into("t")
        .using(
            [
                {"id": 1, "event_at": date(2026, 1, 1), "only_date": date(2026, 1, 1)},
                {"id": 2, "event_at": datetime(2026, 1, 2, 12, 0), "only_date": date(2026, 1, 2)},
            ],
            alias="src",
        )
        .on("t.id = src.id")
        .when_matched_then_delete()
        .build()
    )
    assert naive_query.sql.count("AS TIMESTAMP)") == 2
    assert naive_query.sql.count("AS DATE)") == 2
    assert "TIMESTAMP WITH TIME ZONE" not in naive_query.sql

    aware_query = (
        sql
        .merge(dialect="teradata")
        .into("t")
        .using(
            [{"id": 1, "event_at": date(2026, 1, 1)}, {"id": 2, "event_at": datetime(2026, 1, 2, 12, 0, tzinfo=UTC)}],
            alias="src",
        )
        .on("t.id = src.id")
        .when_matched_then_delete()
        .build()
    )
    assert aware_query.sql.count("AS TIMESTAMP WITH TIME ZONE)") == 2
    assert "AS DATE)" not in aware_query.sql


def test_teradata_merge_using_subquery_defaults_correlation_alias() -> None:
    select_source = sql.select("id", "name").from_("staging")
    built = (
        sql
        .merge(dialect="teradata")
        .into("t")
        .using(select_source)
        .on("t.id = src.id")
        .when_matched_then_update({"name": sql.column("name", table="src")})
        .when_not_matched_then_insert(["id", "name"])
        .build()
    )
    normalized = " ".join(built.sql.split())
    assert 'USING ( SELECT "id", "name" FROM "staging" ) AS "src"' in normalized
    assert 'VALUES ("src"."id", "src"."name")' in normalized

    unaliased_subquery = exp.Subquery(this=parse_one("SELECT id, name FROM staging"))
    subquery_built = (
        sql
        .merge(dialect="teradata")
        .into("t")
        .using(unaliased_subquery)
        .on("t.id = src.id")
        .when_matched_then_delete()
        .build()
    )
    assert 'AS "src"' in subquery_built.sql
    assert unaliased_subquery.alias == ""

    pre_aliased_subquery = exp.Subquery(
        this=parse_one("SELECT id, name FROM staging"), alias=exp.TableAlias(this=exp.to_identifier("custom_src"))
    )
    preserved_built = (
        sql
        .merge(dialect="teradata")
        .into("t")
        .using(pre_aliased_subquery)
        .on("t.id = custom_src.id")
        .when_matched_then_delete()
        .build()
    )
    assert 'AS "custom_src"' in preserved_built.sql
    assert 'AS "src"' not in preserved_built.sql


@pytest.mark.parametrize("dialect", ["teradata", "vantage"])
def test_teradata_upsert_uses_merge(dialect: str) -> None:
    from sqlspec.builder import Merge

    assert isinstance(sql.upsert("users", dialect=dialect), Merge)


def test_teradata_merge_subquery_source_value_binds_parameters_once() -> None:
    subquery = sql.select("MAX(amount)").from_("orders").where_eq("customer_id", 7)
    built = (
        sql
        .merge(dialect="teradata")
        .into("totals", alias="t")
        .using([{"id": 1, "total": subquery}], alias="src")
        .on("t.id = src.id")
        .when_not_matched_then_insert(columns=["id", "total"], values=[1, 2])
        .build()
    )
    assert '"customer_id" = :customer_id' in built.sql
    assert [name for name in built.parameters if name.startswith("customer_id")] == ["customer_id"]
