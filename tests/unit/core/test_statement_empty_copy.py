# pyright: reportPrivateUsage=false
"""Regression tests for SQL empty-copy processed-state sharing."""

from sqlspec.core.statement import SQL
from sqlspec.typing import Empty


def test_empty_copy_flag_state_consistency_processed() -> None:
    source = SQL("SELECT 1")
    source.compile()

    copied = source._create_empty_copy()

    assert copied._compiled_from_cache is True
    assert copied._processed_state is not Empty
    assert copied._processed_state is source._processed_state


def test_empty_copy_flag_state_consistency_unprocessed() -> None:
    source = SQL("SELECT 1")

    copied = source._create_empty_copy()

    assert copied._compiled_from_cache is False
    assert copied._processed_state is Empty


def test_empty_copy_reset_does_not_release_shared_state() -> None:
    source = SQL("SELECT 1")
    source.compile()
    shared = source._processed_state
    copied = source._create_empty_copy()

    copied.reset()

    assert source._processed_state is shared
    assert source._processed_state is not Empty
