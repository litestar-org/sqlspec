"""Tests for Makefile quality-gate safety."""

import re
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]


def test_oneshell_recipes_fail_fast() -> None:
    """Makefile recipes should stop when an inner command fails."""
    makefile = (PROJECT_ROOT / "Makefile").read_text()

    assert ".ONESHELL:" in makefile
    shellflags_match = re.search(r"^\.SHELLFLAGS\s*:?=\s*(?P<flags>.+)$", makefile, flags=re.MULTILINE)
    assert shellflags_match is not None
    flags = shellflags_match.group("flags")
    assert "-e" in flags or "-eu" in flags or "-euo" in flags
    assert "-o pipefail" in flags


def test_default_mypy_target_uses_parallel_checking() -> None:
    """The default mypy gate should use the faster mypy 2 parallel checker."""
    makefile = (PROJECT_ROOT / "Makefile").read_text()

    mypy_target_match = re.search(r"^mypy:.*?(?=^\S)", makefile, flags=re.MULTILINE | re.DOTALL)
    assert mypy_target_match is not None
    mypy_target = mypy_target_match.group(0)
    assert "uv run mypy" in mypy_target
    assert "-n $(MYPY_WORKERS)" in mypy_target
    assert "uv run dmypy" not in mypy_target

    assert re.search(r"^dmypy:.*?## Run mypy daemon", makefile, flags=re.MULTILINE) is not None
