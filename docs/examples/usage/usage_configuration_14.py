def test_parameter_styles() -> None:
__all__ = ("test_parameter_styles", )


    from sqlspec import ParameterStyle

    # Question mark (SQLite, DuckDB)
    qmark = ParameterStyle.QMARK  # WHERE id = ?

    # Numeric (PostgreSQL, asyncpg)
    numeric = ParameterStyle.NUMERIC  # WHERE id = $1

    # Named colon (Oracle, SQLite)
    named_colon = ParameterStyle.NAMED_COLON  # WHERE id = :id

    # Named at (BigQuery)

    # Format/pyformat (psycopg, MySQL)

    assert qmark == ParameterStyle.QMARK
    assert numeric == ParameterStyle.NUMERIC
    assert named_colon == ParameterStyle.NAMED_COLON
