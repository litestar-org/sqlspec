"""pymssql contract and docs wiring tests."""

from pathlib import Path

from tests.integration.adapters._shared._cases import get_driver_case


def test_active_driver_case_is_registered_with_sync_metadata() -> None:
    """pymssql should expose active synchronous contract metadata."""
    case = get_driver_case("pymssql-sync")

    assert case.adapter == "pymssql"
    assert case.dialect == "tsql"
    assert case.mode == "sync"
    assert case.integration_status == "active"
    assert case.supports_migrations is True
    assert case.supports_pooling is False
    assert case.supports_connection_hook is False
    assert case.supports_execute_many is True
    assert case.supports_data_dictionary is True
    assert case.supports_arrow is False


def test_reference_docs_include_pymssql_page_and_index_entry() -> None:
    """The adapter reference should include pymssql."""
    docs_root = Path("docs/reference/adapters")

    assert (docs_root / "pymssql.md").is_file()
    index = (docs_root / "index.md").read_text()
    page = (docs_root / "pymssql.md").read_text()

    assert "pymssql.md" in index
    assert "PymssqlConfig" in page
    assert "PymssqlDriver" in page
