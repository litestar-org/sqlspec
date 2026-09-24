"""Query builder output for the Db2 dialect."""

from datetime import date, datetime, time
from decimal import Decimal
from uuid import UUID

import pytest

from sqlspec import sql
from sqlspec.builder import Merge, QueryBuilder
from sqlspec.core import StatementConfig
from sqlspec.exceptions import SQLBuilderError


def test_for_update_skip_locked_renders_db2_isolation_lock() -> None:
    built = sql.select("a").from_("t").for_update(skip_locked=True).build(dialect="db2")
    assert built.sql.endswith("WITH RS USE AND KEEP UPDATE LOCKS SKIP LOCKED DATA")


def test_for_share_renders_share_locks() -> None:
    built = sql.select("a").from_("t").for_share().build(dialect="db2")
    assert built.sql.endswith("WITH RS USE AND KEEP SHARE LOCKS")


@pytest.mark.parametrize("lock_options", [{"nowait": True}, {"of": "t"}], ids=["nowait", "of-table"])
def test_lock_options_db2_cannot_express_are_refused(lock_options: "dict[str, object]") -> None:
    query = sql.select("a").from_("t").for_update(**lock_options)  # type: ignore[arg-type]
    with pytest.raises(SQLBuilderError):
        query.build(dialect="db2")


def test_statement_path_renders_db2_isolation_lock() -> None:
    statement = sql.select("a").from_("t").for_update(skip_locked=True).to_statement(StatementConfig(dialect="db2"))
    assert statement.sql.endswith("WITH RS USE AND KEEP UPDATE LOCKS SKIP LOCKED DATA")


def test_db2_select_renders_unquoted_identifiers() -> None:
    built = sql.select("a").from_("t").where_eq("a", 1).build(dialect="db2")
    assert '"' not in built.sql
    assert "FROM t" in built.sql


def test_db2_statement_path_renders_unquoted_identifiers() -> None:
    statement = sql.select("a").from_("t").where_eq("a", 1).to_statement(StatementConfig(dialect="db2"))
    assert '"' not in statement.sql
    assert "FROM t" in statement.sql


@pytest.mark.parametrize("dialect", ["oracle", "db2"])
def test_db2_explicitly_quoted_mixed_case_identifier_is_stripped_like_oracle(dialect: str) -> None:
    built = sql.select('"MixedCase"').from_("t").build(dialect=dialect)
    assert '"' not in built.sql
    assert "t.MixedCase AS MixedCase" in built.sql


@pytest.mark.parametrize(
    ("query", "expected_prefix"),
    [
        (sql.insert("t").columns("a").values(1), "INSERT INTO t"),
        (sql.update("t").set("a", 1).where_eq("b", 2), "UPDATE t"),
        (sql.delete().from_("t").where_eq("a", 1), "DELETE FROM t"),
    ],
    ids=["insert", "update", "delete"],
)
def test_db2_insert_update_delete_unquoted(query: "QueryBuilder", expected_prefix: str) -> None:
    built = query.build(dialect="db2")
    assert '"' not in built.sql
    assert built.sql.startswith(expected_prefix)


def test_upsert_returns_merge_for_db2() -> None:
    assert isinstance(sql.upsert("t", dialect="db2"), Merge)


def test_db2_merge_dict_source_casts_parameters() -> None:
    row = {"id": 1, "name": "x", "active": True, "amount": Decimal("1.5"), "at": datetime(2026, 1, 1)}
    built = (
        sql
        .merge(dialect="db2")
        .into("t")
        .using(row, alias="src")
        .on("t.id = src.id")
        .when_not_matched_then_insert(columns=["id", "name"], values=["src.id", "src.name"])
        .build(dialect="db2")
    )
    flat = " ".join(built.sql.split())
    assert "CAST(:id AS BIGINT) AS id" in flat
    assert "CAST(:name AS VARCHAR(32672)) AS name" in flat
    assert "CAST(:active AS BOOLEAN) AS active" in flat
    assert "CAST(:amount AS DECFLOAT(34)) AS amount" in flat
    assert "CAST(:at AS TIMESTAMP) AS at" in flat
    assert "FROM SYSIBM.SYSDUMMY1" in flat
    assert "SELECT :" not in flat
    assert built.parameters["active"] is True


def test_db2_merge_list_source_unions_rows() -> None:
    rows = [{"id": 1, "payload": None}, {"id": 2, "payload": {"k": "v"}}]
    built = (
        sql
        .merge(dialect="db2")
        .into("t")
        .using(rows, alias="src")
        .on("t.id = src.id")
        .when_not_matched_then_insert(columns=["id", "payload"], values=["src.id", "src.payload"])
        .build(dialect="db2")
    )
    flat = " ".join(built.sql.split())
    assert flat.count("UNION ALL") == 1
    assert flat.count("FROM SYSIBM.SYSDUMMY1") == 2
    assert flat.count("AS CLOB)") == 2
    assert flat.count("AS BIGINT)") == 2
    assert "SELECT :" not in flat
    assert '{"k":"v"}' in [value for value in built.parameters.values() if isinstance(value, str)]


def test_db2_merge_unaliased_source_casts_remaining_types() -> None:
    row = {
        "id": UUID("12345678-1234-5678-1234-567812345678"),
        "body": "x" * 32673,
        "day": date(2026, 1, 1),
        "at_time": time(12, 0),
        "raw": b"\x00",
        "ratio": 0.5,
    }
    built = (
        sql.merge(dialect="db2").into("t").using(row).on("t.id = id").when_matched_then_delete().build(dialect="db2")
    )
    flat = " ".join(built.sql.split())
    assert "CAST(:id AS VARCHAR(36)) AS id" in flat
    assert "CAST(:body AS CLOB) AS body" in flat
    assert "CAST(:day AS DATE) AS day" in flat
    assert "CAST(:at_time AS TIME) AS at_time" in flat
    assert "CAST(:raw AS VARBINARY(32672)) AS raw" in flat
    assert "CAST(:ratio AS DOUBLE) AS ratio" in flat
    assert "USING ( SELECT" in flat
