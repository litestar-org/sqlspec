"""Tests for the PGO training script."""

from __future__ import annotations

import subprocess
import sys


def test_pgo_training_runs_without_error() -> None:
    """Training script must exit cleanly and complete in reasonable time."""
    result = subprocess.run([sys.executable, "-m", "sqlspec._pgo_training"], capture_output=True, timeout=90)
    assert result.returncode == 0, f"Training failed: {result.stderr.decode()}"
    assert b"PGO training complete" in result.stdout
