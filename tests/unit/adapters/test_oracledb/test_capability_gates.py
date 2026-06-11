"""Unit tests for Oracle capability gates."""

import oracledb

from sqlspec.adapters.oracledb import _typing as oracle_typing
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


class _DirectPathConnection(_Connection):
    def direct_path_load(self, *_args: object, **_kwargs: object) -> None:
        return None


class _DataFrameBatchConnection:
    def fetch_df_batches(self, *_args: object, **_kwargs: object) -> None:
        return None


def test_connection_is_thin_defaults_to_true_when_attribute_missing() -> None:
    assert connection_is_thin(object()) is True


def test_connection_is_thin_uses_connection_attribute() -> None:
    assert connection_is_thin(_Connection(thin=True)) is True
    assert connection_is_thin(_Connection(thin=False)) is False


def test_supports_direct_path_load_requires_thin_connection_and_method() -> None:
    assert supports_direct_path_load(_DirectPathConnection(thin=True)) is True
    assert supports_direct_path_load(_DirectPathConnection(thin=False)) is False
    assert supports_direct_path_load(_Connection(thin=True)) is False


def test_supports_df_batches_checks_fetch_df_batches_method() -> None:
    assert supports_df_batches(_DataFrameBatchConnection()) is True
    assert supports_df_batches(object()) is False


def test_sparse_vector_type_alias_matches_oracledb_export() -> None:
    assert ORACLEDB_SUPPORTS_SPARSE_VECTORS is True
    assert oracle_typing.SPARSE_VECTOR_TYPE is oracledb.SparseVector
