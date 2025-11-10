"""Example 1: Direct SQL Creation with positional and named parameters."""

__all__ = ("test_direct_sql_creation",)


def test_direct_sql_creation() -> None:
    """Test creating SQL statements with different parameter styles."""
    # start-example
    from sqlspec import SQL

    # Raw SQL string with positional parameters
    sql = SQL("SELECT * FROM users WHERE id = ?", 1)

    # Named parameters
    sql = SQL("SELECT * FROM users WHERE email = :email", email="user@example.com")
    # end-example

    # Verify SQL objects were created
    assert sql is not None
