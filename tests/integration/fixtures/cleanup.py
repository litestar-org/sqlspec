"""Shared integration database cleanup definitions."""

MYSQL_CLEANUP_TABLE_PREFIXES = (
    "test_table",
    "data_types_test",
    "user_profiles",
    "test_parameter_conversion",
    "transaction_test",
    "concurrent_test",
    "arrow_users",
    "arrow_table_test",
    "arrow_batch_test",
    "arrow_params_test",
    "arrow_empty_test",
    "arrow_null_test",
    "arrow_polars_test",
    "arrow_large_test",
    "arrow_types_test",
    "arrow_json_test",
    "driver_feature_test",
)


def mysql_cleanup_statements(adapter_suffix: str, *, procedure_suffix: str | None = None) -> "tuple[str, ...]":
    """Build the cleanup statements for one MySQL adapter test namespace."""
    procedures = ("test_procedure", "simple_procedure")
    if procedure_suffix is not None:
        procedures = tuple(f"{procedure}_{procedure_suffix}" for procedure in procedures)
    return (
        "SET sql_notes = 0",
        *(f"DROP TABLE IF EXISTS {table}_{adapter_suffix}" for table in MYSQL_CLEANUP_TABLE_PREFIXES),
        *(f"DROP PROCEDURE IF EXISTS {procedure}" for procedure in procedures),
        "SET sql_notes = 1",
    )


__all__ = ("mysql_cleanup_statements",)
