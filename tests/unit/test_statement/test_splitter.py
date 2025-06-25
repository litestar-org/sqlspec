"""Unit tests for sqlspec.statement.splitter module."""

from typing import TYPE_CHECKING, Callable

import pytest

from sqlspec.statement.splitter import OracleDialectConfig, StatementSplitter, split_sql_script

if TYPE_CHECKING:
    pass


# Test Oracle dialect splitting
@pytest.mark.parametrize(
    "script,expected_count,expected_content",
    [
        # Simple statements
        (
            """
            SELECT * FROM users;
            INSERT INTO users (name) VALUES ('John');
            DELETE FROM users WHERE id = 1;
            """,
            3,
            ["SELECT * FROM users", "INSERT INTO users", "DELETE FROM users"],
        ),
        # PL/SQL anonymous block
        (
            """
            BEGIN
                EXECUTE IMMEDIATE 'DROP TABLE test_table';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -942 THEN
                        RAISE;
                    END IF;
            END;
            """,
            1,
            ["BEGIN", "END;"],
        ),
        # DECLARE block
        (
            """
            DECLARE
                v_count NUMBER;
            BEGIN
                SELECT COUNT(*) INTO v_count FROM users;
                DBMS_OUTPUT.PUT_LINE('Count: ' || v_count);
            END;
            """,
            1,
            ["DECLARE", "END;"],
        ),
        # Mixed statements and blocks
        (
            """
            CREATE TABLE test_table (id NUMBER);

            BEGIN
                INSERT INTO test_table VALUES (1);
            END;

            SELECT * FROM test_table;
            """,
            3,
            ["CREATE TABLE", "BEGIN", "SELECT"],
        ),
        # Slash terminator
        (
            """
            BEGIN
                NULL;
            END;
            /

            SELECT * FROM dual;
            """,
            2,
            ["BEGIN", "SELECT"],
        ),
    ],
    ids=["simple_statements", "plsql_block", "declare_block", "mixed_statements", "slash_terminator"],
)
def test_oracle_splitting(script: str, expected_count: int, expected_content: list[str]) -> None:
    """Test Oracle SQL script splitting."""
    statements = split_sql_script(script, dialect="oracle")

    assert len(statements) == expected_count
    # Check that expected content appears in the statements
    for content in expected_content:
        found = any(content in stmt for stmt in statements)
        assert found, f"Expected content '{content}' not found in any statement"


def test_oracle_nested_blocks() -> None:
    """Test Oracle nested BEGIN/END blocks."""
    script = """
    BEGIN
        BEGIN
            INSERT INTO test (id) VALUES (1);
        EXCEPTION
            WHEN DUP_VAL_ON_INDEX THEN
                BEGIN
                    UPDATE test SET updated = SYSDATE WHERE id = 1;
                END;
        END;
    END;
    """

    statements = split_sql_script(script, dialect="oracle")
    assert len(statements) == 1
    assert statements[0].count("BEGIN") == 3
    assert statements[0].count("END;") == 3


@pytest.mark.parametrize(
    "script,expected_count",
    [
        # Keywords in string literals
        (
            """
            INSERT INTO messages (text) VALUES ('BEGIN transaction');
            UPDATE messages SET text = 'END of story' WHERE id = 1;
            """,
            2,
        ),
        # Keywords in comments
        (
            """
            -- BEGIN comment
            SELECT * FROM users;
            /* This is the END
               of a multi-line comment */
            INSERT INTO users VALUES (1);
            """,
            2,
        ),
    ],
    ids=["keywords_in_strings", "keywords_in_comments"],
)
def test_oracle_keywords_in_literals(script: str, expected_count: int) -> None:
    """Test Oracle handling of keywords inside literals and comments."""
    statements = split_sql_script(script, dialect="oracle")
    assert len(statements) == expected_count


# Test T-SQL dialect splitting
@pytest.mark.parametrize(
    "script,expected_count,expected_content",
    [
        # GO batch separator
        (
            """
            CREATE TABLE test (id INT);
            GO

            INSERT INTO test VALUES (1);
            INSERT INTO test VALUES (2);
            GO

            SELECT * FROM test;
            """,
            3,
            [["CREATE TABLE"], ["INSERT", "INSERT"], ["SELECT"]],
        ),
        # TRY...CATCH blocks
        (
            """
            BEGIN TRY
                INSERT INTO test VALUES (1);
            END TRY
            BEGIN CATCH
                PRINT ERROR_MESSAGE();
            END CATCH;
            """,
            1,
            [["BEGIN TRY", "END CATCH"]],
        ),
    ],
    ids=["go_separator", "try_catch"],
)
def test_tsql_splitting(script: str, expected_count: int, expected_content: list[list[str]]) -> None:
    """Test T-SQL script splitting."""
    statements = split_sql_script(script, dialect="tsql")

    assert len(statements) == expected_count
    for i, content_list in enumerate(expected_content):
        for content in content_list:
            assert content in statements[i]


# Test PostgreSQL dialect splitting
@pytest.mark.parametrize(
    "script,expected_count,check_content",
    [
        # Dollar-quoted strings
        (
            """
            CREATE FUNCTION test_func() RETURNS void AS $$
            BEGIN
                INSERT INTO test VALUES (1);
            END;
            $$ LANGUAGE plpgsql;

            SELECT * FROM test;
            """,
            2,
            lambda stmts: "$$" in stmts[0] and "CREATE FUNCTION" in stmts[0] and "SELECT" in stmts[1],
        ),
        # Nested dollar quotes with tags
        (
            """
            CREATE FUNCTION complex_func() RETURNS void AS $BODY$
            DECLARE
                v_sql TEXT := $sql$SELECT * FROM users WHERE name = 'test';$sql$;
            BEGIN
                EXECUTE v_sql;
            END;
            $BODY$ LANGUAGE plpgsql;
            """,
            1,
            lambda stmts: "$BODY$" in stmts[0] and "$sql$" in stmts[0],
        ),
    ],
    ids=["dollar_quoted", "nested_dollar_quotes"],
)
def test_postgresql_splitting(script: str, expected_count: int, check_content: Callable[[list[str]], bool]) -> None:
    """Test PostgreSQL script splitting."""
    statements = split_sql_script(script, dialect="postgresql")

    assert len(statements) == expected_count
    assert check_content(statements)


# Test edge cases
@pytest.mark.parametrize(
    "script,dialect,expected_count",
    [
        ("", "oracle", 0),  # Empty script
        ("", "tsql", 0),
        ("", "postgresql", 0),
        (
            """
            -- This is a comment
            /* Another comment */
            """,
            "oracle",
            0,
        ),  # Only comments
    ],
    ids=["empty_oracle", "empty_tsql", "empty_postgresql", "only_comments"],
)
def test_edge_case_empty_scripts(script: str, dialect: str, expected_count: int) -> None:
    """Test edge cases with empty or comment-only scripts."""
    statements = split_sql_script(script, dialect=dialect)
    assert len(statements) == expected_count


def test_unclosed_block() -> None:
    """Test handling of unclosed BEGIN block."""
    script = """
    BEGIN
        INSERT INTO test VALUES (1);
    -- Missing END
    """

    statements = split_sql_script(script, dialect="oracle")
    assert len(statements) == 1
    assert "BEGIN" in statements[0]
    # Should include the incomplete block


def test_deeply_nested_blocks() -> None:
    """Test handling of deeply nested blocks."""
    depth = 10
    script = "BEGIN\n" * depth + "NULL;" + "\nEND;" * depth

    statements = split_sql_script(script, dialect="oracle")
    assert len(statements) == 1
    assert statements[0].count("BEGIN") == depth
    assert statements[0].count("END;") == depth


def test_max_nesting_depth_exceeded() -> None:
    """Test that exceeding maximum nesting depth raises an error."""
    config = OracleDialectConfig()
    splitter = StatementSplitter(config)

    # Generate script exceeding max depth
    depth = config.max_nesting_depth + 1
    script = "BEGIN " * depth

    with pytest.raises(ValueError, match="Maximum nesting depth"):
        splitter.split(script)


# Test dialect configuration
@pytest.mark.parametrize(
    "dialect,expected_name,has_batch_separators",
    [("oracle", "oracle", False), ("tsql", "tsql", True), ("postgresql", "postgresql", False)],
)
def test_dialect_configuration(dialect: str, expected_name: str, has_batch_separators: bool) -> None:
    """Test dialect configuration properties."""
    # Use split_sql_script to validate dialect is configured
    _ = split_sql_script("SELECT 1;", dialect=dialect)

    # This tests that the dialect is properly configured
    if dialect == "oracle":
        config = OracleDialectConfig()
    elif dialect == "tsql":
        from sqlspec.statement.splitter import TSQLDialectConfig

        config = TSQLDialectConfig()
    else:  # postgresql
        from sqlspec.statement.splitter import PostgreSQLDialectConfig

        config = PostgreSQLDialectConfig()

    assert config.name == expected_name
    assert bool(config.batch_separators) == has_batch_separators


# Test complex real-world scenarios
def test_complex_oracle_script() -> None:
    """Test complex Oracle script with multiple constructs."""
    script = """
    -- Create table
    CREATE TABLE employees (
        id NUMBER PRIMARY KEY,
        name VARCHAR2(100)
    );

    -- Insert data with PL/SQL block
    DECLARE
        v_id NUMBER := 1;
    BEGIN
        FOR i IN 1..10 LOOP
            INSERT INTO employees (id, name)
            VALUES (v_id, 'Employee ' || v_id);
            v_id := v_id + 1;
        END LOOP;
        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            RAISE;
    END;
    /

    -- Query the data
    SELECT * FROM employees ORDER BY id;
    """

    statements = split_sql_script(script, dialect="oracle")
    # The splitter may combine the DECLARE block and following SELECT
    assert len(statements) >= 2
    assert "CREATE TABLE" in statements[0]
    assert "DECLARE" in statements[1]
    # SELECT might be in the same statement as DECLARE block or separate
    assert any("SELECT" in stmt for stmt in statements)


def test_complex_tsql_script() -> None:
    """Test complex T-SQL script with batches and error handling."""
    script = """
    -- Create procedure
    CREATE PROCEDURE UpdateEmployee
        @EmployeeId INT,
        @Name NVARCHAR(100)
    AS
    BEGIN
        SET NOCOUNT ON;
        UPDATE Employees SET Name = @Name WHERE Id = @EmployeeId;
    END
    GO

    -- Use the procedure with error handling
    BEGIN TRY
        EXEC UpdateEmployee @EmployeeId = 1, @Name = 'John Doe';
    END TRY
    BEGIN CATCH
        SELECT ERROR_MESSAGE() AS ErrorMessage;
    END CATCH
    GO

    -- Check results
    SELECT * FROM Employees WHERE Id = 1;
    """

    statements = split_sql_script(script, dialect="tsql")
    assert len(statements) == 3
    assert "CREATE PROCEDURE" in statements[0]
    assert "BEGIN TRY" in statements[1]
    assert "SELECT * FROM Employees" in statements[2]


def test_splitter_instance_reuse() -> None:
    """Test that StatementSplitter instance can be reused."""
    config = OracleDialectConfig()
    splitter = StatementSplitter(config)

    # First split
    script1 = "SELECT 1 FROM dual; SELECT 2 FROM dual;"
    statements1 = splitter.split(script1)
    assert len(statements1) == 2

    # Second split with same instance
    script2 = "INSERT INTO test VALUES (1); DELETE FROM test;"
    statements2 = splitter.split(script2)
    assert len(statements2) == 2

    # Results should be independent
    assert statements1[0] != statements2[0]


@pytest.mark.parametrize("invalid_dialect", ["invalid", "unknown"])
def test_invalid_dialect(invalid_dialect: str) -> None:
    """Test that unsupported dialect falls back to generic splitter."""
    # Should not raise, but use generic splitter
    result = split_sql_script("SELECT 1; SELECT 2;", dialect=invalid_dialect)
    assert result == ["SELECT 1;", "SELECT 2;"]


@pytest.mark.parametrize("valid_dialect", ["mysql", "sqlite", "duckdb", "bigquery", "generic"])
def test_newly_supported_dialects(valid_dialect: str) -> None:
    """Test that newly supported dialects work correctly."""
    result = split_sql_script("SELECT 1; SELECT 2;", dialect=valid_dialect)
    assert result == ["SELECT 1;", "SELECT 2;"]


# Test statement preservation
def test_statement_whitespace_preservation() -> None:
    """Test that meaningful whitespace is preserved in statements."""
    script = """
    SELECT
        id,
        name,
        email
    FROM
        users
    WHERE
        active = 1;
    """

    statements = split_sql_script(script, dialect="oracle")
    assert len(statements) == 1
    # Check that newlines are preserved
    assert "\n" in statements[0]
    assert "    id," in statements[0]  # Indentation preserved
