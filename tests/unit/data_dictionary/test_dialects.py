"""Unit tests for built-in data dictionary dialect registration."""

from sqlspec.data_dictionary import get_dialect_config


def test_mssql_dialect_registered() -> None:
    """The MSSQL data-dictionary dialect should be available by canonical name."""
    cfg = get_dialect_config("mssql")

    assert cfg.name == "mssql"


def test_tsql_alias_resolves_to_mssql() -> None:
    """The tsql alias should resolve to the MSSQL dialect config."""
    assert get_dialect_config("tsql") is get_dialect_config("mssql")


def test_sqlserver_alias_resolves_to_mssql() -> None:
    """The sqlserver alias should resolve to the MSSQL dialect config."""
    assert get_dialect_config("sqlserver") is get_dialect_config("mssql")


def test_all_dialects_define_numeric_and_varchar_types() -> None:
    """All 10 registered dialect configs must define numeric and varchar mappings."""
    dialects = [
        "postgres",
        "mssql",
        "oracle",
        "mysql",
        "mariadb",
        "sqlite",
        "duckdb",
        "spanner",
        "bigquery",
        "cockroachdb",
    ]
    for dialect_name in dialects:
        cfg = get_dialect_config(dialect_name)
        assert "integer" in cfg.type_mappings
        assert "bigint" in cfg.type_mappings
        assert "float" in cfg.type_mappings
        assert "varchar" in cfg.type_mappings

    pg = get_dialect_config("postgres")
    assert pg.type_mappings["integer"] == "INTEGER"
    assert pg.type_mappings["bigint"] == "BIGINT"
    assert pg.type_mappings["float"] == "DOUBLE PRECISION"
    assert pg.type_mappings["varchar"] == "VARCHAR({length})"

    ms = get_dialect_config("mssql")
    assert ms.type_mappings["integer"] == "INT"
    assert ms.type_mappings["bigint"] == "BIGINT"
    assert ms.type_mappings["float"] == "FLOAT"
    assert ms.type_mappings["varchar"] == "NVARCHAR({length})"

    ora = get_dialect_config("oracle")
    assert ora.type_mappings["integer"] == "NUMBER(10)"
    assert ora.type_mappings["bigint"] == "NUMBER(19)"
    assert ora.type_mappings["float"] == "BINARY_DOUBLE"
    assert ora.type_mappings["varchar"] == "VARCHAR2({length})"

    sl = get_dialect_config("sqlite")
    assert sl.type_mappings["integer"] == "INTEGER"
    assert sl.type_mappings["bigint"] == "INTEGER"
    assert sl.type_mappings["float"] == "REAL"
    assert sl.type_mappings["varchar"] == "TEXT"
