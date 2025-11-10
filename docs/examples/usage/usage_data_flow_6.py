"""Example 6: Compilation to target dialect."""

__all__ = ("test_compilation",)


def test_compilation() -> None:
    """Test compiling AST to target SQL dialect."""
    # start-example
    import sqlglot

    # Compile AST to target dialect
    compiled_sql = sqlglot.parse_one("SELECT * FROM users WHERE id = ?", dialect="sqlite").sql(dialect="postgres")
    # Result: "SELECT * FROM users WHERE id = $1"
    # end-example

    # Verify compilation produced a string
    assert isinstance(compiled_sql, str)
    assert "users" in compiled_sql
