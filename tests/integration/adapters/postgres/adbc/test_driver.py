"""PostgreSQL-backed ADBC driver residuals."""

from tests.integration.adapters._shared.adbc_backends import postgresql_session, test_postgresql_specific_features
from tests.integration.adapters._shared.adbc_connection import (
    test_connection,
    test_connection_info_retrieval,
    test_connection_transaction_handling,
)
from tests.integration.adapters._shared.adbc_driver import test_adbc_postgresql_statement_stack_continue_on_error
from tests.integration.adapters._shared.adbc_edge_cases import (
    test_connection_resilience,
    test_execute_script_edge_cases,
)

__all__ = (
    "postgresql_session",
    "test_adbc_postgresql_statement_stack_continue_on_error",
    "test_connection",
    "test_connection_info_retrieval",
    "test_connection_resilience",
    "test_connection_transaction_handling",
    "test_execute_script_edge_cases",
    "test_postgresql_specific_features",
)
