"""Example 4: Parameter Extraction."""

__all__ = ("test_parameter_extraction",)


def test_parameter_extraction() -> None:
    """Show how SQL captures positional and named parameters."""
    from sqlspec import SQL

    # start-example
    positional = SQL("SELECT * FROM users WHERE id = ? AND status = ?", 1, "active")
    positional_map = dict(enumerate(positional.positional_parameters))

    named = SQL("SELECT * FROM users WHERE email = :email", email="user@example.com")
    named_map = named.named_parameters
    # end-example

    assert positional_map == {0: 1, 1: "active"}
    assert named_map == {"email": "user@example.com"}
