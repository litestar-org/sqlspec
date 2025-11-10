__all__ = ("test_parameter_styles",)


def test_parameter_styles() -> None:

    # start-example
    from sqlspec import ParameterStyle

    # Question mark (SQLite, DuckDB)
    qmark = ParameterStyle.QMARK  # WHERE id = ?

    # Numeric (PostgreSQL, asyncpg)
    numeric = ParameterStyle.NUMERIC  # WHERE id = $1

    # Named colon (Oracle, SQLite)
    named_colon = ParameterStyle.NAMED_COLON  # WHERE id = :id

    # Named at (BigQuery)

    # Format/pyformat (psycopg, MySQL)

    # end-example
    assert qmark == ParameterStyle.QMARK
    assert numeric == ParameterStyle.NUMERIC
    assert named_colon == ParameterStyle.NAMED_COLON
