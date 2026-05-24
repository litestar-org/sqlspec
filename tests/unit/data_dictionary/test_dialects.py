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
