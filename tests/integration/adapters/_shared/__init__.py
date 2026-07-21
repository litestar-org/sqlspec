"""Reusable integration-test templates for database-family suites."""

from importlib import import_module
from typing import Any

_SUITE_MODULES = (
    "adk_store_contract",
    "arrow_contract",
    "bulk_ingest_contract",
    "case_metadata_contract",
    "connect_time_settings_contract",
    "driver_contract",
    "driver_features_contract",
    "events_queue_contract",
    "exceptions_contract",
    "execute_many_contract",
    "explain_contract",
    "extra_assertions_proof_contract",
    "lifecycle_contract",
    "listen_notify_contract",
    "litestar_store_contract",
    "merge_contract",
    "metadata_contract",
    "migrations_contract",
    "oracle_lob_fetch_contract",
    "parameter_contract",
    "parameter_styles_contract",
    "postgres_extensions_contract",
    "query_contract",
    "result_contract",
    "script_error_contract",
    "statement_inputs_contract",
    "storage_bridge_contract",
    "storage_bridge_rustfs_contract",
    "streaming_contract",
    "vector_contract",
)
_SUITE_FAMILIES = {
    "case_metadata_contract": frozenset({"sqlite"}),
    "events_queue_contract": frozenset({"duckdb", "mssql", "mysql", "oracle", "postgres", "sqlite"}),
    "extra_assertions_proof_contract": frozenset({"sqlite"}),
    "listen_notify_contract": frozenset({"postgres"}),
    "oracle_lob_fetch_contract": frozenset({"oracle"}),
    "postgres_extensions_contract": frozenset({"postgres"}),
}


def install_shared_tests(namespace: dict[str, Any], family: str) -> None:
    """Install shared test templates into a database-family module.

    Args:
        namespace: The family test module globals receiving the test callables.
        family: Database family that owns the receiving module.
    """
    for module_name in _SUITE_MODULES:
        if family not in _SUITE_FAMILIES.get(module_name, {family}):
            continue
        module = import_module(f"tests.integration.adapters._shared.suite_{module_name}")
        for name, value in vars(module).items():
            if not name.startswith("test_") or not callable(value):
                continue
            if name in namespace:
                msg = f"Duplicate shared test template: {name}"
                raise RuntimeError(msg)
            namespace[name] = value


__all__ = ("install_shared_tests",)
