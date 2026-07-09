"""Regression guard for the PGO training workload imports."""

from tools.scripts.pgo_training import _train_serialization


def test_train_serialization_runs() -> None:
    """The serialization workload must execute without import errors."""
    _train_serialization()
