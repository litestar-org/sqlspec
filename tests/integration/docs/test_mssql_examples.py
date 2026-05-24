"""Smoke tests for MSSQL documentation examples."""

from pathlib import Path

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.mssql]


def test_mssql_litestar_examples_import() -> None:
    """The documented Litestar examples should import and build app objects."""
    pytest.importorskip("mssql_python")
    import docs.examples.mssql_litestar.app as app_example
    import docs.examples.mssql_litestar.store_example as store_example

    assert app_example.app is not None
    assert store_example.app is not None


def test_mssql_docs_are_wired_into_extensions_index() -> None:
    """The MSSQL chapter should be present in the extensions toctree."""
    index = Path("docs/extensions/index.rst").read_text()
    assert "mssql/index" in index


def test_mssql_cookbook_contains_required_recipes() -> None:
    """The cookbook should keep the expected recipe coverage."""
    cookbook = Path("docs/extensions/mssql/cookbook.rst").read_text()
    for heading in (
        "MERGE Upsert",
        "JSON Columns",
        "UUID Round Trips",
        "DATETIMEOFFSET",
        "BulkCopy",
        "Arrow Fetch for Analytics",
        "Microsoft Entra ID",
        "Always Encrypted",
        "AlwaysOn Read-Only Replicas",
        "Litestar Session Store",
        "Sync vs Async Config",
        "arrow-odbc for Oracle",
        "arrow-odbc for MySQL",
    ):
        assert heading in cookbook


def test_mssql_knowledge_chapter_is_indexed() -> None:
    """The agent-facing knowledge chapter should be discoverable."""
    knowledge_index_path = Path(".agents/knowledge/index.md")
    if not knowledge_index_path.exists():
        pytest.skip(".agents knowledge index is local Flow state")
    assert "mssql_type_handling.md" in knowledge_index_path.read_text()
