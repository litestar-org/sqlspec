"""Unit tests for Oracle capability gates."""

import oracledb

from sqlspec.adapters.oracledb import _vector_handlers
from sqlspec.adapters.oracledb.core import (
    ORACLEDB_SUPPORTS_SPARSE_VECTORS,
    connection_is_thin,
    supports_df_batches,
    supports_direct_path_load,
)


class _Connection:
    def __init__(self, *, thin: bool | None = None) -> None:
        if thin is not None:
            self.thin = thin


def test_connection_is_thin_defaults_to_true_when_attribute_missing() -> None:
    assert connection_is_thin(object()) is True


def test_connection_is_thin_uses_connection_attribute() -> None:
    assert connection_is_thin(_Connection(thin=True)) is True
    assert connection_is_thin(_Connection(thin=False)) is False


def test_supports_direct_path_load_requires_only_thin_mode() -> None:
    """The declared oracledb floor guarantees the API, so only Thin mode gates it."""
    assert supports_direct_path_load(_Connection(thin=True)) is True
    assert supports_direct_path_load(_Connection(thin=False)) is False


def test_supports_df_batches_is_guaranteed_by_the_declared_floor() -> None:
    """fetch_df_batches landed well below the declared oracledb floor."""
    assert supports_df_batches(object()) is True


def test_sparse_vector_type_alias_matches_oracledb_export() -> None:
    assert ORACLEDB_SUPPORTS_SPARSE_VECTORS is True
    assert _vector_handlers.SPARSE_VECTOR_TYPE is oracledb.SparseVector
