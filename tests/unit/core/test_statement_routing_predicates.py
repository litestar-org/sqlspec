"""Statement predicates retain fail-closed semantics before compilation."""

import pytest

from sqlspec.core import SQL, StatementConfig


@pytest.mark.parametrize("text", ["INSERT INTO t VALUES (1)", "UPDATE t SET a = 1", "DELETE FROM t"])
def test_statement_modifying_predicate_compiles_lazily(text: str) -> None:
    statement = SQL(text)
    assert statement.is_modifying_operation() is True
    assert statement.is_read_only() is False


def test_statement_read_only_guards_and_cached_reuse() -> None:
    statement = SQL("SELECT 1")
    assert statement.is_read_only() is True
    compiled = statement.compile()
    assert statement.is_read_only() is True
    assert statement.compile() == compiled
    assert SQL("SELECT 1").as_script().is_read_only() is False
    assert SQL("SELECT ?", [(1,), (2,)], is_many=True).is_read_only() is False
    config = StatementConfig(enable_parsing=False)
    assert SQL("SELECT 1", statement_config=config).is_read_only() is False
    assert SQL("INSERT INTO t VALUES (1)", statement_config=config).is_modifying_operation() is False
