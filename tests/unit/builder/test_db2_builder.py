"""Query builder output for the Db2 dialect."""

import pytest

from sqlspec import sql
from sqlspec.builder import QueryBuilder
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
