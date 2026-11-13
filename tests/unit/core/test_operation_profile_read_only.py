"""Fail-closed replica routing through the public SQL compiler seam."""

import pytest

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
    ],
)
def test_operation_profile_read_only_matrix(text: str, dialect: str, read_only: bool, modifies: bool) -> None:
    processor = SQLProcessor(StatementConfig(dialect=dialect))
    for _ in range(2):
        profile = processor.compile(text).operation_profile
        assert profile.is_read_only is read_only
        assert profile.modifies_rows is modifies
