"""Behavioral unit tests for the SQLSpec Click CLI reference Markdown generator."""

from pathlib import Path

import click
import pytest
from tools.docs.generate_cli_docs import main as cli_docs_main
from tools.docs.generate_cli_docs import render_cli_reference

import sqlspec.cli as cli_module

__all__ = (
    "test_cli_docs_main_writes_and_checks_staleness",
    "test_lazy_cli_attribute_resolves_click_group",
    "test_render_cli_reference_renders_all_commands_arguments_and_options_cleanly",
)


def test_lazy_cli_attribute_resolves_click_group() -> None:
    resolved = cli_module.cli
    assert isinstance(resolved, click.Group)
    assert resolved.name == "sqlspec"
    with pytest.raises(AttributeError, match="has no attribute 'nonexistent_attr'"):
        _ = cli_module.nonexistent_attr  # type: ignore[attr-defined]


def test_render_cli_reference_renders_all_commands_arguments_and_options_cleanly() -> None:
    markdown = render_cli_reference()

    assert markdown.startswith("# CLI Reference\n")
    assert "## Global Options" in markdown
    assert "`--config`" in markdown
    assert "`SQLSPEC_CONFIG`" in markdown
    assert "`--validate-config`" in markdown

    expected_commands = (
        "#### `sqlspec show-config`",
        "#### `sqlspec show-current-revision`",
        "#### `sqlspec verify`",
        "#### `sqlspec init`",
        "#### `sqlspec create-migration`",
        "#### `sqlspec upgrade`",
        "#### `sqlspec downgrade`",
        "#### `sqlspec stamp`",
        "#### `sqlspec squash`",
        "#### `sqlspec fix`",
        "#### `sqlspec dump-schema`",
        "#### `sqlspec adk memory cleanup`",
        "#### `sqlspec adk memory verify`",
    )
    for heading in expected_commands:
        assert heading in markdown

    adk_section_pos = markdown.index("## ADK Extension Commands")
    assert markdown.index("#### `sqlspec verify`") < adk_section_pos
    assert markdown.index("#### `sqlspec dump-schema`") < adk_section_pos

    assert "| `revision` |" in markdown
    assert "| `directory` |" in markdown
    assert "| `version_range` |" in markdown
    assert "| `-m`, `--message` |" in markdown
    assert "| `--bind-key` |" in markdown

    assert "Sentinel.UNSET" not in markdown
    assert "--help" not in markdown
    assert "[dim]" not in markdown
    assert "[/]" not in markdown

    h2_headings = [line for line in markdown.splitlines() if line.startswith("## ")]
    h3_headings = [line for line in markdown.splitlines() if line.startswith("### ")]
    assert 2 <= len(h2_headings) <= 5
    assert len(h2_headings) + len(h3_headings) <= 12


def test_cli_docs_main_writes_and_checks_staleness(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    output_file = tmp_path / "cli.md"

    missing_rc = cli_docs_main(["--check", "--output", str(output_file)])
    assert missing_rc == 1
    assert "missing" in capsys.readouterr().err.lower()

    write_rc = cli_docs_main(["--output", str(output_file)])
    assert write_rc == 0
    assert output_file.is_file()

    clean_rc = cli_docs_main(["--check", "--output", str(output_file)])
    assert clean_rc == 0

    output_file.write_text("# stale\n", encoding="utf-8")
    stale_rc = cli_docs_main(["--check", "--output", str(output_file)])
    assert stale_rc == 1
    assert "out of date" in capsys.readouterr().err.lower()
