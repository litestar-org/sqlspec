"""Example 2: Using the Query Builder."""


def test_query_builder() -> None:
    """Test building SQL programmatically."""
    # start-example
    from sqlspec import sql

    # Build SQL programmatically
    query = sql.select("id", "name", "email").from_("users").where("status = ?", "active")
    # end-example

    # Verify query object was created
    assert query is not None

