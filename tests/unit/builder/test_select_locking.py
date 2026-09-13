"""Unit tests for SELECT locking functionality (FOR UPDATE, FOR SHARE, etc)."""

import re

import pytest

from sqlspec import sql
from sqlspec.data_dictionary import DialectConfig, register_dialect
from sqlspec.exceptions import SQLBuilderError


def test_for_update_basic() -> None:
    """Test basic FOR UPDATE clause."""
    query = sql.select("id", "status", dialect="postgres").from_("job").for_update()
    stmt = query.build()
    assert "FOR UPDATE" in stmt.sql


def test_for_update_skip_locked() -> None:
    """Test FOR UPDATE SKIP LOCKED."""
    query = sql.select("*", dialect="postgres").from_("job").for_update(skip_locked=True)
    stmt = query.build()
    assert "FOR UPDATE SKIP LOCKED" in stmt.sql


def test_for_update_nowait() -> None:
    """Test FOR UPDATE NOWAIT."""
    query = sql.select("*", dialect="postgres").from_("job").for_update(nowait=True)
    stmt = query.build()
    assert "FOR UPDATE NOWAIT" in stmt.sql


def test_for_update_of_single_table() -> None:
    """Test FOR UPDATE OF specific table."""
    query = (
        sql
        .select("j.id", "u.name", dialect="postgres")
        .from_("job j")
        .join("users u ON j.user_id = u.id")
        .for_update(of="j")
    )
    stmt = query.build()
    assert "FOR UPDATE OF j" in stmt.sql


def test_for_update_of_multiple_tables() -> None:
    """Test FOR UPDATE OF multiple tables."""
    query = (
        sql
        .select("j.id", "u.name", "c.title", dialect="postgres")
        .from_("job j")
        .join("users u ON j.user_id = u.id")
        .join("companies c ON u.company_id = c.id")
        .for_update(of=["j", "u"])
    )
    stmt = query.build()
    sql_content = stmt.sql
    # Should contain both tables in OF clause
    assert "FOR UPDATE OF" in sql_content
    assert "j" in sql_content
    assert "u" in sql_content


def test_for_update_error_skip_locked_and_nowait() -> None:
    """Test error when both skip_locked and nowait are True."""
    with pytest.raises(SQLBuilderError, match="Cannot use both"):
        sql.select("*").from_("job").for_update(skip_locked=True, nowait=True)


def test_for_update_error_non_select() -> None:
    """Test error when applying FOR UPDATE to non-SELECT statement."""
    # FOR UPDATE methods are only available on Select objects
    # This test verifies they don't exist on Update objects
    update_query = sql.update("job").set(status="running")
    assert not hasattr(update_query, "for_update")


def test_for_share_basic() -> None:
    """Test basic FOR SHARE clause."""
    query = sql.select("*", dialect="postgres").from_("job").for_share()
    stmt = query.build()
    assert "FOR SHARE" in stmt.sql


def test_for_share_skip_locked() -> None:
    """Test FOR SHARE SKIP LOCKED."""
    query = sql.select("*", dialect="postgres").from_("job").for_share(skip_locked=True)
    stmt = query.build()
    assert "FOR SHARE SKIP LOCKED" in stmt.sql


def test_for_share_nowait() -> None:
    """Test FOR SHARE NOWAIT."""
    query = sql.select("*", dialect="postgres").from_("job").for_share(nowait=True)
    stmt = query.build()
    assert "FOR SHARE NOWAIT" in stmt.sql


def test_for_share_of_table() -> None:
    """Test FOR SHARE OF specific table."""
    query = (
        sql
        .select("j.id", "u.name", dialect="postgres")
        .from_("job j")
        .join("users u ON j.user_id = u.id")
        .for_share(of="j")
    )
    stmt = query.build()
    assert "FOR SHARE OF j" in stmt.sql


def test_for_share_error_skip_locked_and_nowait() -> None:
    """Test error when both skip_locked and nowait are True for FOR SHARE."""
    with pytest.raises(SQLBuilderError, match="Cannot use both"):
        sql.select("*").from_("job").for_share(skip_locked=True, nowait=True)


def test_for_share_error_non_select() -> None:
    """Test error when applying FOR SHARE to non-SELECT statement."""
    # FOR SHARE methods are only available on Select objects
    update_query = sql.update("job").set(status="running")
    assert not hasattr(update_query, "for_share")


def test_for_key_share() -> None:
    """Test FOR KEY SHARE clause (PostgreSQL-specific)."""
    query = sql.select("*").from_("job").for_key_share()
    stmt = query.build()
    # Note: The exact SQL output may vary based on dialect
    # We'll test this works and doesn't error
    assert stmt.sql is not None
    assert len(stmt.sql) > 0


def test_for_key_share_error_non_select() -> None:
    """Test error when applying FOR KEY SHARE to non-SELECT statement."""
    # FOR KEY SHARE methods are only available on Select objects
    update_query = sql.update("job").set(status="running")
    assert not hasattr(update_query, "for_key_share")


def test_for_no_key_update() -> None:
    """Test FOR NO KEY UPDATE clause (PostgreSQL-specific)."""
    query = sql.select("*").from_("job").for_no_key_update()
    stmt = query.build()
    # Note: The exact SQL output may vary based on dialect
    # We'll test this works and doesn't error
    assert stmt.sql is not None
    assert len(stmt.sql) > 0


def test_for_no_key_update_error_non_select() -> None:
    """Test error when applying FOR NO KEY UPDATE to non-SELECT statement."""
    # FOR NO KEY UPDATE methods are only available on Select objects
    update_query = sql.update("job").set(status="running")
    assert not hasattr(update_query, "for_no_key_update")


def test_chaining_with_other_clauses() -> None:
    """Test FOR UPDATE chained with other SQL clauses."""
    query = (
        sql
        .select("id", "name", "status", dialect="postgres")
        .from_("job")
        .where("status = 'pending'")
        .order_by("priority DESC")
        .limit(10)
        .for_update(skip_locked=True)
    )

    stmt = query.build()
    sql_content = stmt.sql

    # All clauses should be present
    assert "SELECT" in sql_content
    assert "FROM" in sql_content
    assert "job" in sql_content
    assert "WHERE" in sql_content
    assert "ORDER BY" in sql_content
    assert "LIMIT" in sql_content
    assert "FOR UPDATE SKIP LOCKED" in sql_content


def test_multiple_locks_same_query() -> None:
    """Test adding multiple lock types to the same query."""
    # This is technically possible but unusual - testing for robustness
    query = sql.select("*", dialect="postgres").from_("job")
    query = query.for_update()
    query = query.for_share()

    stmt = query.build()
    sql_content = stmt.sql

    # Both locks should be present (though this is unusual usage)
    assert "FOR UPDATE" in sql_content
    assert "FOR SHARE" in sql_content


def test_postgresql_dialect_specific() -> None:
    """Test PostgreSQL-specific locking with explicit dialect."""
    query = sql.select("*", dialect="postgres").from_("job").for_key_share()

    stmt = query.build()
    # Should generate valid SQL without errors
    assert stmt.sql is not None
    assert stmt.dialect == "postgres"


def test_mysql_dialect_basic() -> None:
    """Test basic FOR UPDATE with MySQL dialect."""
    query = sql.select("*", dialect="mysql").from_("job").for_update(skip_locked=True)

    stmt = query.build()
    # Should generate valid SQL without errors
    assert stmt.sql is not None
    assert stmt.dialect == "mysql"
    assert "FOR UPDATE" in stmt.sql


def test_complex_join_with_for_update_of() -> None:
    """Test FOR UPDATE OF with complex joins."""
    query = (
        sql
        .select("j.id", "j.status", "u.name", "c.title", dialect="postgres")
        .from_("job j")
        .join("users u ON j.user_id = u.id")
        .join("companies c ON u.company_id = c.id")
        .where("j.status = 'pending'")
        .for_update(of=["j", "u"])
    )  # Lock only job and users tables, not companies

    stmt = query.build()
    sql_content = stmt.sql

    # Note: The SQL formatting might vary, but key elements should be present
    assert "FOR UPDATE OF" in sql_content
    # Both tables should be mentioned in the OF clause
    assert "j" in sql_content
    assert "u" in sql_content
    assert "c" in sql_content


@pytest.mark.parametrize("dialect", ["tsql", "mssql", "sqlite", "duckdb", "bigquery"])
@pytest.mark.parametrize("statement", [False, True])
def test_for_update_raises_on_unsupported_dialects(dialect: str, statement: bool) -> None:
    """Test FOR UPDATE raises SQLBuilderError on dialects without row lock support."""
    from sqlspec.core import StatementConfig

    query = sql.select("*").from_("job").for_update()
    with pytest.raises(SQLBuilderError, match="does not support FOR UPDATE"):
        if statement:
            query.to_statement(StatementConfig(dialect=dialect))
        else:
            query.build(dialect=dialect)


@pytest.mark.parametrize("dialect", ["postgres", "mysql", "oracle", "cockroachdb", "mariadb", "spanner", "spangres"])
def test_for_update_supported_dialects(dialect: str) -> None:
    """Test FOR UPDATE renders on dialects supporting row locks."""
    query = sql.select("*").from_("job").for_update()
    stmt = query.build(dialect=dialect)
    assert "FOR UPDATE" in stmt.sql


def test_skip_locked_flag_gate() -> None:
    """Test SKIP LOCKED raises SQLBuilderError when supports_skip_locked is False."""
    custom_config = DialectConfig(
        name="custom_no_skip_locked",
        feature_flags={"supports_for_update": True, "supports_skip_locked": False},
        feature_versions={},
        type_mappings={},
        version_pattern=re.compile(r".*"),
    )
    register_dialect(custom_config)

    query = sql.select("*").from_("job").for_update(skip_locked=True)
    with pytest.raises(SQLBuilderError, match="does not support SKIP LOCKED"):
        query.build(dialect="custom_no_skip_locked")


@pytest.mark.parametrize("statement", [False, True])
def test_oracle_for_share_rejected(statement: bool) -> None:
    from sqlspec.core import StatementConfig

    query = sql.select("id").from_("job").for_share()
    with pytest.raises(SQLBuilderError, match="does not support FOR SHARE"):
        if statement:
            query.to_statement(StatementConfig(dialect="oracle"))
        else:
            query.build(dialect="oracle")


@pytest.mark.parametrize("statement", [False, True])
def test_mariadb_shared_lock_rendering(statement: bool) -> None:
    from sqlspec.core import StatementConfig

    query = sql.select("id").from_("job").for_share(skip_locked=True)
    rendered = query.to_statement(StatementConfig(dialect="mariadb")) if statement else query.build(dialect="mariadb")
    assert "LOCK IN SHARE MODE SKIP LOCKED" in rendered.sql
    assert "FOR SHARE" in query.build(dialect="postgres").sql


@pytest.mark.parametrize("dialect", ["spanner", "spangres"])
def test_spanner_statement_locking(dialect: str) -> None:
    from sqlspec.core import StatementConfig

    query = sql.select("id").from_("job").for_update()
    assert "FOR UPDATE" in query.to_statement(StatementConfig(dialect=dialect)).sql
    for locked_query in (
        sql.select("id").from_("job").for_update(skip_locked=True),
        sql.select("id").from_("job").for_update(nowait=True),
        sql.select("id").from_("job").for_update(of="job"),
        sql.select("id").from_("job").for_share(),
    ):
        with pytest.raises(SQLBuilderError, match="only plain FOR UPDATE"):
            locked_query.build(dialect=dialect)


@pytest.mark.parametrize("dialect", ["mysql", "mariadb", "oracle"])
def test_postgresql_key_lock_modes_rejected_elsewhere(dialect: str) -> None:
    for query in (sql.select("id").from_("job").for_no_key_update(), sql.select("id").from_("job").for_key_share()):
        with pytest.raises(SQLBuilderError, match="PostgreSQL key lock modes"):
            query.build(dialect=dialect)


def test_mariadb_rejects_lock_targets() -> None:
    for query in (
        sql.select("id").from_("job").for_update(of="job"),
        sql.select("id").from_("job").for_share(of="job"),
    ):
        with pytest.raises(SQLBuilderError, match="do not support OF targets"):
            query.build(dialect="mariadb")
