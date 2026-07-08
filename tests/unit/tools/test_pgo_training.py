"""Regression guard for the PGO training workload imports."""

from tools.scripts.pgo_training import _train_serialization


def test_train_serialization_runs() -> None:
    """The serialization workload must execute without import errors.

    A prior revision lazily imported a non-existent sqlspec._serialization module,
    crashing the PGO training CI path with ModuleNotFoundError. This exercises the
    real code path (the broken import is inside the function body).
    """
    _train_serialization()
