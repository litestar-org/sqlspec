"""Tests for the SQL statement splitter."""

import pytest

from sqlspec.statement.splitter import OracleDialectConfig, StatementSplitter, split_sql_script


class TestOracleSplitter:
    """Test Oracle-specific SQL splitting."""

    def test_simple_statements(self) -> None:
        """Test splitting simple statements."""
        script = """
        SELECT * FROM users;
        INSERT INTO users (name) VALUES ('John');
        DELETE FROM users WHERE id = 1;
        """

        statements = split_sql_script(script, dialect="oracle")
        assert len(statements) == 3
        assert "SELECT * FROM users;" in statements[0]
        assert "INSERT INTO users" in statements[1]
        assert "DELETE FROM users" in statements[2]

    def test_plsql_block(self) -> None:
        """Test PL/SQL anonymous block."""
        script = """
        BEGIN
            EXECUTE IMMEDIATE 'DROP TABLE test_table';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -942 THEN
                    RAISE;
                END IF;
        END;
        """

        statements = split_sql_script(script, dialect="oracle")
        assert len(statements) == 1
        assert "BEGIN" in statements[0]
        assert "END;" in statements[0]

    def test_declare_block(self) -> None:
        """Test PL/SQL block with DECLARE."""
        script = """
        DECLARE
            v_count NUMBER;
        BEGIN
            SELECT COUNT(*) INTO v_count FROM users;
            DBMS_OUTPUT.PUT_LINE('Count: ' || v_count);
        END;
        """

        statements = split_sql_script(script, dialect="oracle")
        assert len(statements) == 1
        assert "DECLARE" in statements[0]
        assert "END;" in statements[0]

    def test_nested_blocks(self) -> None:
        """Test nested BEGIN/END blocks."""
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

    def test_mixed_statements_and_blocks(self) -> None:
        """Test mix of regular statements and PL/SQL blocks."""
        script = """
        CREATE TABLE test_table (id NUMBER);

        BEGIN
            INSERT INTO test_table VALUES (1);
        END;

        SELECT * FROM test_table;
        """

        statements = split_sql_script(script, dialect="oracle")
        assert len(statements) == 3
        assert "CREATE TABLE" in statements[0]
        assert "BEGIN" in statements[1]
        assert "SELECT" in statements[2]

    def test_slash_terminator(self) -> None:
        """Test Oracle / terminator."""
        script = """
        BEGIN
            NULL;
        END;
        /

        SELECT * FROM dual;
        """

        statements = split_sql_script(script, dialect="oracle")
        assert len(statements) == 2
        assert "BEGIN" in statements[0]
        assert "/" in statements[0]
        assert "SELECT" in statements[1]

    def test_keywords_in_strings(self) -> None:
        """Test keywords inside string literals."""
        script = """
        INSERT INTO messages (text) VALUES ('BEGIN transaction');
        UPDATE messages SET text = 'END of story' WHERE id = 1;
        """

        statements = split_sql_script(script, dialect="oracle")
        assert len(statements) == 2
        assert "BEGIN transaction" in statements[0]
        assert "END of story" in statements[1]

    def test_comments_with_keywords(self) -> None:
        """Test keywords in comments."""
        script = """
        -- BEGIN comment
        SELECT * FROM users;
        /* This is the END
           of a multi-line comment */
        INSERT INTO users VALUES (1);
        """

        statements = split_sql_script(script, dialect="oracle")
        assert len(statements) == 2
        assert "-- BEGIN comment" in statements[0]
        assert "/* This is the END" in statements[1]


class TestTSQLSplitter:
    """Test T-SQL (SQL Server) specific splitting."""

    def test_go_batch_separator(self) -> None:
        """Test GO batch separator."""
        script = """
        CREATE TABLE test (id INT);
        GO

        INSERT INTO test VALUES (1);
        INSERT INTO test VALUES (2);
        GO

        SELECT * FROM test;
        """

        statements = split_sql_script(script, dialect="tsql")
        assert len(statements) == 3
        assert "CREATE TABLE" in statements[0]
        assert "GO" in statements[0]
        assert statements[1].count("INSERT") == 2
        assert "SELECT" in statements[2]

    def test_try_catch_blocks(self) -> None:
        """Test TRY...CATCH blocks."""
        script = """
        BEGIN TRY
            INSERT INTO test VALUES (1);
        END TRY
        BEGIN CATCH
            PRINT ERROR_MESSAGE();
        END CATCH;
        """

        statements = split_sql_script(script, dialect="tsql")
        assert len(statements) == 1
        assert "BEGIN TRY" in statements[0]
        assert "END CATCH;" in statements[0]


class TestPostgreSQLSplitter:
    """Test PostgreSQL-specific splitting."""

    def test_dollar_quoted_strings(self) -> None:
        """Test PostgreSQL dollar-quoted strings."""
        script = """
        CREATE FUNCTION test_func() RETURNS void AS $$
        BEGIN
            INSERT INTO test VALUES (1);
        END;
        $$ LANGUAGE plpgsql;

        SELECT * FROM test;
        """

        statements = split_sql_script(script, dialect="postgresql")
        assert len(statements) == 2
        assert "$$" in statements[0]
        assert "CREATE FUNCTION" in statements[0]
        assert "SELECT" in statements[1]

    def test_nested_dollar_quotes(self) -> None:
        """Test nested dollar-quoted strings with tags."""
        script = """
        CREATE FUNCTION complex_func() RETURNS void AS $BODY$
        DECLARE
            v_sql TEXT := $sql$SELECT * FROM users WHERE name = 'test';$sql$;
        BEGIN
            EXECUTE v_sql;
        END;
        $BODY$ LANGUAGE plpgsql;
        """

        statements = split_sql_script(script, dialect="postgresql")
        assert len(statements) == 1
        assert "$BODY$" in statements[0]
        assert "$sql$" in statements[0]


class TestEdgeCases:
    """Test edge cases and error conditions."""

    def test_empty_script(self) -> None:
        """Test empty script."""
        statements = split_sql_script("", dialect="oracle")
        assert len(statements) == 0

    def test_only_comments(self) -> None:
        """Test script with only comments."""
        script = """
        -- This is a comment
        /* Another comment */
        """
        statements = split_sql_script(script, dialect="oracle")
        assert len(statements) == 0

    def test_unclosed_block(self) -> None:
        """Test unclosed BEGIN block."""
        script = """
        BEGIN
            INSERT INTO test VALUES (1);
        -- Missing END
        """

        statements = split_sql_script(script, dialect="oracle")
        assert len(statements) == 1
        assert "BEGIN" in statements[0]
        # Should include the incomplete block

    def test_deeply_nested_blocks(self) -> None:
        """Test deeply nested blocks."""
        # Generate deeply nested blocks
        depth = 10
        script = "BEGIN\n" * depth + "NULL;" + "\nEND;" * depth

        statements = split_sql_script(script, dialect="oracle")
        assert len(statements) == 1
        assert statements[0].count("BEGIN") == depth
        assert statements[0].count("END;") == depth

    def test_max_nesting_depth(self) -> None:
        """Test maximum nesting depth limit."""
        config = OracleDialectConfig()
        splitter = StatementSplitter(config)

        # Generate script exceeding max depth
        depth = config.max_nesting_depth + 1
        script = "BEGIN " * depth

        with pytest.raises(ValueError, match="Maximum nesting depth"):
            splitter.split(script)
