"""Unit tests for SQL splitter helpers."""

from sqlspec.core.splitter import _join_string_fragments, split_sql_script


def test_join_string_fragments_returns_joined_text() -> None:
    """The optional writer path should preserve plain string assembly behavior."""
    assert _join_string_fragments(["SELECT", " ", "1"]) == "SELECT 1"


def test_split_sql_script_preserves_statement_output() -> None:
    """Statement splitting should preserve existing semicolon handling."""
    assert split_sql_script("SELECT 1; SELECT 2;", strip_trailing_terminator=True) == ["SELECT 1", "SELECT 2"]


def test_tsql_go_separates_batches() -> None:
    """T-SQL GO batch separators should split scripts into executable batches."""
    script = "CREATE TABLE t1 (id INT);\nGO\nINSERT INTO t1 VALUES (1);\nGO\nSELECT * FROM t1;"

    statements = split_sql_script(script, dialect="tsql", strip_trailing_terminator=True)

    assert len(statements) == 3
    assert statements[0].startswith("CREATE TABLE")
    assert statements[1].startswith("INSERT")
    assert statements[2].startswith("SELECT")


def test_tsql_go_is_case_insensitive() -> None:
    """T-SQL GO batch matching should not depend on casing."""
    script = "SELECT 1;\ngo\nSELECT 2;\nGO\nSELECT 3;"

    statements = split_sql_script(script, dialect="tsql", strip_trailing_terminator=True)

    assert statements == ["SELECT 1", "SELECT 2", "SELECT 3"]


def test_tsql_begin_try_block_not_split_on_inner_semicolons() -> None:
    """T-SQL TRY/CATCH blocks should remain one statement despite inner semicolons."""
    script = (
        "BEGIN TRY\n"
        "  INSERT INTO t1 VALUES (1);\n"
        "  INSERT INTO t1 VALUES (2);\n"
        "END TRY\n"
        "BEGIN CATCH\n"
        "  THROW;\n"
        "END CATCH;"
    )

    statements = split_sql_script(script, dialect="tsql", strip_trailing_terminator=True)

    assert len(statements) == 1


def test_tsql_semicolon_terminator_within_batch() -> None:
    """T-SQL semicolons should split statements inside the current batch."""
    script = "SELECT 1;\nSELECT 2;\nGO\nSELECT 3;"

    statements = split_sql_script(script, dialect="tsql", strip_trailing_terminator=True)

    assert statements == ["SELECT 1", "SELECT 2", "SELECT 3"]


def test_mssql_alias_dispatches_to_tsql_splitter() -> None:
    """The mssql alias should use the same splitter behavior as tsql."""
    script = "SELECT 1;\nGO\nSELECT 2;"

    assert split_sql_script(script, dialect="mssql") == split_sql_script(script, dialect="tsql")


def test_sqlserver_alias_dispatches_to_tsql_splitter() -> None:
    """The sqlserver alias should use the same splitter behavior as tsql."""
    script = "SELECT 1;\nGO\nSELECT 2;"

    assert split_sql_script(script, dialect="sqlserver") == split_sql_script(script, dialect="tsql")
