"""Query builder output for the Db2 dialect."""

import pytest

from sqlspec import sql
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
