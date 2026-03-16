"""Tests for the PGO training script."""

import subprocess
import sys
from pathlib import Path


def test_pgo_training_runs_without_error() -> None:
    """Training script must exit cleanly and complete in reasonable time."""
    script = Path(__file__).resolve().parents[2] / "tools" / "scripts" / "pgo_training.py"
    result = subprocess.run([sys.executable, str(script)], capture_output=True, timeout=90)
    assert result.returncode == 0, f"Training failed: {result.stderr.decode()}"
    assert b"PGO training complete" in result.stdout
