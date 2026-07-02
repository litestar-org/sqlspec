"""pymssql contract and docs wiring tests."""

from pathlib import Path

from tests.integration.adapters.contracts._cases import get_driver_case


def test_deferred_driver_case_is_registered_with_sync_metadata() -> None:
    """pymssql should be visible to the contract registry even before a live fixture exists."""
    case = get_driver_case("pymssql-sync")

    assert case.adapter == "pymssql"
    assert case.dialect == "tsql"
    assert case.mode == "sync"
    assert case.integration_status == "deferred"
    assert case.supports_migrations is True
    assert case.supports_pooling is False
    assert case.supports_connection_hook is False
    assert case.supports_execute_many is True
    assert case.supports_data_dictionary is True
    assert case.supports_arrow is False


def test_reference_docs_include_pymssql_page_and_index_entry() -> None:
    """The adapter reference should include pymssql."""
    docs_root = Path("docs/reference/adapters")

    assert (docs_root / "pymssql.rst").is_file()
    index = (docs_root / "index.rst").read_text()
    page = (docs_root / "pymssql.rst").read_text()

    assert ":link: pymssql" in index
    assert "pymssql" in index.split(".. toctree::", 1)[1]
    assert "PymssqlConfig" in page
    assert "PymssqlDriver" in page
