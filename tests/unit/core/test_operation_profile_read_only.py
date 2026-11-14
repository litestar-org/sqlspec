"""Fail-closed replica routing through the public SQL compiler seam."""

import pytest
from sqlglot import exp

from sqlspec.core import SQLProcessor, StatementConfig


@pytest.mark.parametrize(
    ("text", "dialect", "read_only", "modifies"),
    [
        ("SELECT id FROM users", "postgres", True, False),
        ("SELECT 1 UNION ALL SELECT 2", "postgres", True, False),
        ("WITH c AS (SELECT 1) SELECT * FROM c", "postgres", True, False),
        ("VALUES (1, 2)", "postgres", True, False),
        ("SELECT * FROM jobs FOR UPDATE", "postgres", False, False),
        ("SELECT * FROM jobs FOR SHARE", "postgres", False, False),
        ("SELECT * FROM jobs LOCK IN SHARE MODE", "mysql", False, False),
        ("SELECT * INTO archive FROM users", "postgres", False, True),
        ("WITH d AS (DELETE FROM users RETURNING id) SELECT * FROM d", "postgres", False, True),
        ("SELECT nextval('s')", "postgres", False, False),
        ("SELECT pg_advisory_lock(42)", "postgres", False, False),
        ("SELECT GET_LOCK('k', 5)", "mysql", False, False),
        ("SELECT NEXT VALUE FOR seq", "tsql", False, False),
        ("SELECT seq.NEXTVAL FROM dual", "oracle", False, False),
        ("SELECT owner.seq.NEXTVAL FROM dual", "oracle", False, False),
        ("SELECT seq.CURRVAL FROM dual", "oracle", False, False),
        ("SELECT owner.seq.CURRVAL FROM dual", "oracle", False, False),
        ('SELECT seq."NEXTVAL" FROM records seq', "oracle", True, False),
        ("SELECT NEXTVAL FROM records", "oracle", True, False),
        ("SELECT * FROM x WITH (UPDLOCK)", "tsql", False, False),
        ("SELECT * FROM x WITH (HOLDLOCK)", "tsql", False, False),
        ("SELECT * FROM x WITH (XLOCK)", "tsql", False, False),
        ("SELECT * FROM x WITH (TABLOCKX)", "tsql", False, False),
        ("SELECT * FROM x WITH (READPAST)", "tsql", False, False),
        ("SELECT * FROM x WITH (TABLOCK)", "tsql", False, False),
        ("SELECT * FROM x WITH (ROWLOCK)", "tsql", False, False),
        ("SELECT * FROM x WITH (NOLOCK)", "tsql", True, False),
        ("SELECT * FROM x WITH (READUNCOMMITTED)", "tsql", True, False),
        ("SELECT * FROM x WITH (NOLOCK, UPDLOCK)", "tsql", False, False),
        ("EXPLAIN SELECT 1", "postgres", True, False),
        ("EXPLAIN ANALYZE SELECT 1", "postgres", False, False),
        ("EXPLAIN ANALYZE SELECT 1", "mysql", False, False),
        ("EXPLAIN (BUFFERS, ANALYZE) SELECT 1", "postgres", False, False),
        ("EXPLAIN PLAN FOR SELECT 1", "oracle", False, False),
        ("COPY users TO STDOUT", "postgres", True, False),
        ("COPY (SELECT nextval('s')) TO STDOUT", "postgres", False, False),
        ("COPY users FROM STDIN", "postgres", False, True),
        ("SHOW TABLES", "mysql", True, False),
        ("DESCRIBE users", "mysql", True, False),
        ("PRAGMA foreign_keys = ON", "sqlite", False, False),
        ("CALL my_proc()", "postgres", False, False),
        ("SET TIME ZONE 'UTC'", "postgres", False, False),
        ("SELECT set_config('statement_timeout', '5000', false)", "postgres", False, False),
        ("SELECT txid_current()", "postgres", False, False),
        ("SELECT pg_current_xact_id()", "postgres", False, False),
        ("SELECT pg_notify('ch', 'payload')", "postgres", False, False),
        ("SELECT dblink_exec('conn', 'DELETE FROM t')", "postgres", False, False),
        ("SELECT pg_switch_wal()", "postgres", False, False),
        ("SELECT pg_create_restore_point('rp')", "postgres", False, False),
        ("SELECT pg_create_logical_replication_slot('slot', 'pgoutput')", "postgres", False, False),
        ("SELECT pg_logical_emit_message(true, 'prefix', 'msg')", "postgres", False, False),
        ("SELECT pg_export_snapshot()", "postgres", False, False),
        ("SELECT pg_stat_reset()", "postgres", False, False),
        ("SELECT dbms_session.set_identifier('id') FROM dual", "oracle", False, False),
        ("SELECT sys.dbms_session.set_identifier('id') FROM dual", "oracle", False, False),
        ("DESCRIBE analyze_log", "mysql", True, False),
        ("EXPLAIN SELECT 1 FROM analyze_tbl", "mysql", True, False),
        ("EXPLAIN SELECT 1::int", "postgres", True, False),
        ("EXPLAIN SELECT * FROM x WITH (NOLOCK)", "tsql", True, False),
        ("EXPLAIN SELECT * FROM x WITH (UPDLOCK)", "tsql", False, False),
    ],
)
def test_operation_profile_read_only_matrix(text: str, dialect: str, read_only: bool, modifies: bool) -> None:
    processor = SQLProcessor(StatementConfig(dialect=dialect))
    for _ in range(2):
        profile = processor.compile(text).operation_profile
        assert profile.is_read_only is read_only
        assert profile.modifies_rows is modifies


@pytest.mark.parametrize(
    "nested_node",
    [
        exp.TruncateTable(expressions=[exp.to_table("users")]),
        exp.Command(this="SET", expression=exp.Literal.string("x = 1")),
        exp.Create(this=exp.to_table("t"), kind="TABLE"),
        exp.Set(expressions=[]),
        exp.LoadData(),
    ],
)
def test_non_read_only_write_ast_nodes(nested_node: "exp.Expr") -> None:
    expr = exp.Describe(this=nested_node)
    assert SQLProcessor._expression_is_read_only(expr, False) is False
