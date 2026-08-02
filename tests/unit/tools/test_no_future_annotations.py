from pathlib import Path

import pytest
from tools.hooks.no_future_annotations import main


@pytest.mark.parametrize("source", ["value: int = 1\n", "from __future__ import generator_stop\n"])
def test_clean_python_files_pass(tmp_path: Path, source: str) -> None:
    path = tmp_path / "clean.py"
    path.write_text(source, encoding="utf-8")

    assert main([str(path)]) == 0


@pytest.mark.parametrize(
    "source", ["from __future__ import annotations\n", "from __future__ import annotations, generator_stop\n"]
)
def test_future_annotations_imports_fail(tmp_path: Path, capsys: pytest.CaptureFixture[str], source: str) -> None:
    path = tmp_path / "future.py"
    path.write_text(source, encoding="utf-8")

    assert main([str(path)]) == 1
    assert str(path) in capsys.readouterr().err


def test_syntax_error_falls_back_to_text_detection(tmp_path: Path) -> None:
    path = tmp_path / "invalid.py"
    path.write_text("from __future__ import annotations\nthis is not valid python !!!\n", encoding="utf-8")

    assert main([str(path)]) == 1


def test_non_python_files_are_ignored(tmp_path: Path) -> None:
    path = tmp_path / "example.txt"
    path.write_text("from __future__ import annotations\n", encoding="utf-8")

    assert main([str(path)]) == 0


def test_all_offending_files_are_reported(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    first = tmp_path / "first.py"
    second = tmp_path / "second.py"
    clean = tmp_path / "clean.py"
    first.write_text("from __future__ import annotations\n", encoding="utf-8")
    second.write_text("from __future__ import annotations, generator_stop\n", encoding="utf-8")
    clean.write_text("value = 1\n", encoding="utf-8")

    assert main([str(first), str(clean), str(second)]) == 1
    error = capsys.readouterr().err
    assert str(first) in error
    assert str(second) in error
    assert str(clean) not in error
