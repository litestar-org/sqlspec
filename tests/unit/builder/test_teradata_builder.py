"""Teradata public builder contracts."""

from datetime import UTC
from decimal import Decimal

import pytest

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
    from datetime import datetime

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


@pytest.mark.parametrize("dialect", ["teradata", "vantage"])
def test_teradata_upsert_uses_merge(dialect: str) -> None:
    from sqlspec.builder import Merge

    assert isinstance(sql.upsert("users", dialect=dialect), Merge)
