"""Example 4: Parameter Extraction."""


def test_parameter_extraction() -> None:
    """Test parameter extraction and mapping."""
    # start-example
    # SQLSpec identifies parameter placeholders
    # Input:  "SELECT * FROM users WHERE id = ? AND status = ?"
    # Params: [1, 'active']
    #
    # Result: Positional parameter mapping created
    #         Position 0 → value: 1
    #         Position 1 → value: 'active'
    # end-example

    # This is a comment example showing the process
    pass

