"""Statement predicates retain fail-closed semantics before compilation."""

from pathlib import Path

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


@pytest.mark.parametrize(
    ("text", "dialect"),
    [
        ("SELECT seq.NEXTVAL FROM dual", "oracle"),
        ("SELECT owner.seq.CURRVAL FROM dual", "oracle"),
        ("SELECT * FROM x WITH (UPDLOCK)", "tsql"),
        ("SELECT * FROM x WITH (TABLOCKX)", "tsql"),
    ],
)
def test_stateful_reads_rejected_by_public_sql_and_strict_routing(tmp_path: Path, text: str, dialect: str) -> None:
    from sqlspec.adapters.sqlite import SqliteConfig
    from sqlspec.exceptions import RoutingContextError
    from sqlspec.utils.routing import RoutingContext

    config = StatementConfig(dialect=dialect)
    for _ in range(2):
        assert SQL(text, statement_config=config).is_read_only() is False
    primary = SqliteConfig(
        connection_config={"database": str(tmp_path / "writer.db")},
        replicas=[{"database": str(tmp_path / "reader.db")}],
    )
    try:
        with primary.provide_routing_session(statement_config=config) as session, RoutingContext.replica():
            with pytest.raises(RoutingContextError):
                session.select(text)
            assert session.primary_session is None
            assert session.replica_session is None
    finally:
        primary.close_pool()
