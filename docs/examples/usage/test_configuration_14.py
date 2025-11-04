from sqlspec.core.parameters import ParameterStyle


def test_parameter_styles():
    # Question mark (SQLite, DuckDB)
    qmark = ParameterStyle.QMARK          # WHERE id = ?

    # Numeric (PostgreSQL, asyncpg)
    numeric = ParameterStyle.NUMERIC        # WHERE id = $1

    # Named colon (Oracle, SQLite)
    named_colon = ParameterStyle.NAMED_COLON    # WHERE id = :id

    # Named at (BigQuery)
    named_at = ParameterStyle.NAMED_AT       # WHERE id = @id

    # Format/pyformat (psycopg, MySQL)
    positional_pyformat = ParameterStyle.POSITIONAL_PYFORMAT         # WHERE id = %s
    named_pyformat = ParameterStyle.NAMED_PYFORMAT       # WHERE id = %(id)s

    assert qmark == ParameterStyle.QMARK
    assert numeric == ParameterStyle.NUMERIC
    assert named_colon == ParameterStyle.NAMED_COLON

