from sqlspec.core.statement import SQL


def test_auto_detect_many_optimization() -> None:
    # Homogeneous list of tuples - should detect
    params = [(1,), (2,), (3,)]
    sql = SQL("INSERT INTO table VALUES (?)", params)
    assert sql.is_many is True

    # Single item list - should NOT detect (len > 1 check)
    params_single = [(1,)]
    sql_single = SQL("INSERT INTO table VALUES (?)", params_single)
    assert sql_single.is_many is False

    # List of non-sequences - should NOT detect
    params_scalar = [1, 2, 3]
    # SQL constructor might treat this as positional params if not list of lists
    sql_scalar = SQL("SELECT * FROM table WHERE id IN (?)", params_scalar)
    assert sql_scalar.is_many is False

    # Large list - should be fast
    large_params = [(i,) for i in range(100_000)]
    sql_large = SQL("INSERT INTO table VALUES (?)", large_params)
    assert sql_large.is_many is True
