"""Example 7: Parameter Processing."""

__all__ = ("test_parameter_processing",)


def test_parameter_processing() -> None:
    """Convert SQLite-style placeholders to PostgreSQL numeric parameters."""
    from sqlspec import SQL, ParameterStyle, ParameterStyleConfig, StatementConfig

    # start-example
    statement_config = StatementConfig(
        dialect="sqlite",
        parameter_config=ParameterStyleConfig(
            default_parameter_style=ParameterStyle.NUMERIC, has_native_list_expansion=False
        ),
    )

    sql = SQL("SELECT * FROM users WHERE id = ? AND status = ?", 1, "active", statement_config=statement_config)

    compiled_sql, execution_params = sql.compile()
    # end-example

    assert "$1" in compiled_sql
    assert "$2" in compiled_sql
    assert execution_params == [1, "active"]
