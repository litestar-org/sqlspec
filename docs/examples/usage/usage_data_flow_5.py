"""Example 5: AST Generation with SQLGlot."""

__all__ = ("test_ast_generation",)


def test_ast_generation() -> None:
    """Test parsing SQL into Abstract Syntax Tree."""
    # start-example
    import sqlglot

    # Parse SQL into structured AST
    expression = sqlglot.parse_one("SELECT * FROM users WHERE id = ?", dialect="sqlite")
    # end-example

    # Verify expression was created
    assert expression is not None
