"""Behavioral unit tests for the SQLSpec CLI, data dictionary, and config table Markdown generators."""

import inspect
from pathlib import Path

import click
import pytest
from tools.docs.generate_cli_docs import _format_param_type, render_cli_reference
from tools.docs.generate_cli_docs import main as cli_docs_main
from tools.docs.generate_config_tables import (
    check_adapter_reference_parity,
    render_filter_catalog_table,
    render_typeddict_table,
    sync_markdown_markers,
)
from tools.docs.generate_config_tables import main as config_tables_main
from tools.docs.generate_data_dictionary_docs import DIALECT_SPECS, render_all_dialect_docs
from tools.docs.generate_data_dictionary_docs import main as dialect_docs_main

import sqlspec.cli as cli_module
import sqlspec.core.filters as filters_module

__all__ = (
    "test_cli_docs_main_writes_and_checks_staleness",
    "test_cli_reference_renders_command_heading",
    "test_config_tables_main_exit_codes",
    "test_dialect_capability_matrix_rows",
    "test_dialect_docs_generator_renders_all_dialects_and_checks_staleness",
    "test_filter_catalog_covers_every_concrete_statement_filter",
    "test_lazy_cli_attribute_resolves_click_group",
    "test_render_cli_reference_renders_all_commands_arguments_and_options_cleanly",
    "test_render_typeddict_table_cross_module_inheritance_forward_refs_and_defaults",
    "test_render_typeddict_table_keeps_full_multiline_docstrings_with_lists",
    "test_render_typeddict_table_renders_inheritance_types_defaults_and_or_unions",
    "test_sync_markdown_markers_preserves_tab_indentation_and_is_idempotent",
    "test_sync_markdown_markers_rejects_malformed_blocks",
    "test_sync_markdown_markers_rewrites_stale_blocks",
)


_FILTER_BASE_CLASS_NAMES: frozenset[str] = frozenset({"InAnyFilter", "PaginationFilter", "StatementFilter"})


@pytest.fixture(scope="module")
def cli_markdown() -> str:
    return render_cli_reference()


@pytest.fixture(scope="module")
def filter_catalog() -> str:
    return render_filter_catalog_table()


@pytest.fixture(scope="module")
def dialect_docs() -> dict[str, str]:
    return render_all_dialect_docs()


def test_lazy_cli_attribute_resolves_click_group() -> None:
    resolved = cli_module.cli
    assert isinstance(resolved, click.Group)
    assert resolved.name == "sqlspec"
    with pytest.raises(AttributeError, match="has no attribute 'nonexistent_attr'"):
        _ = cli_module.nonexistent_attr  # type: ignore[attr-defined]


@pytest.mark.parametrize(
    "command",
    [
        "show-config",
        "show-current-revision",
        "verify",
        "init",
        "create-migration",
        "upgrade",
        "downgrade",
        "stamp",
        "squash",
        "fix",
        "dump-schema",
        "adk memory cleanup",
        "adk memory verify",
    ],
)
def test_cli_reference_renders_command_heading(cli_markdown: str, command: str) -> None:
    assert f"\n#### sqlspec {command}\n" in cli_markdown


def test_render_cli_reference_renders_all_commands_arguments_and_options_cleanly(cli_markdown: str) -> None:
    assert cli_markdown.startswith("# CLI Reference\n")
    assert "## Global Options" in cli_markdown
    assert "`--config`" in cli_markdown
    assert "`SQLSPEC_CONFIG`" in cli_markdown
    assert "`--validate-config`" in cli_markdown
    assert "#### `" not in cli_markdown

    adk_section_pos = cli_markdown.index("## ADK Extension Commands")
    assert cli_markdown.index("#### sqlspec verify") < adk_section_pos
    assert cli_markdown.index("#### sqlspec dump-schema") < adk_section_pos
    assert cli_markdown.index("#### sqlspec adk memory cleanup") > adk_section_pos

    assert "| `revision` |" in cli_markdown
    assert "| `directory` |" in cli_markdown
    assert "| `version_range` |" in cli_markdown
    assert "| `-m`, `--message` |" in cli_markdown
    assert "| `--bind-key` |" in cli_markdown
    assert "`choice(sql / py)`" in cli_markdown
    assert "`choice(ddl / schema / concat)`" in cli_markdown
    assert "choice(sql | py)" not in cli_markdown
    assert _format_param_type(click.Option(["--format"], type=click.Choice(["sql", "py"]))) == "choice(sql / py)"

    assert "Sentinel.UNSET" not in cli_markdown
    assert "--help" not in cli_markdown
    assert "[dim]" not in cli_markdown
    assert "[/]" not in cli_markdown

    h2_headings = [line for line in cli_markdown.splitlines() if line.startswith("## ")]
    h3_headings = [line for line in cli_markdown.splitlines() if line.startswith("### ")]
    assert 2 <= len(h2_headings) <= 5
    assert len(h2_headings) + len(h3_headings) <= 12


def test_cli_docs_main_writes_and_checks_staleness(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    output_file = tmp_path / "cli.md"

    missing_rc = cli_docs_main(["--check", "--output", str(output_file)])
    assert missing_rc == 1
    assert "missing" in capsys.readouterr().err.lower()

    write_rc = cli_docs_main(["--output", str(output_file)])
    assert write_rc == 0
    assert output_file.read_text(encoding="utf-8") == render_cli_reference()

    clean_rc = cli_docs_main(["--check", "--output", str(output_file)])
    assert clean_rc == 0

    output_file.write_text("# stale\n", encoding="utf-8")
    stale_rc = cli_docs_main(["--check", "--output", str(output_file)])
    assert stale_rc == 1
    assert "out of date" in capsys.readouterr().err.lower()


@pytest.mark.parametrize(
    ("dialect_row_prefix", "expected_cells"),
    [
        (
            "[Oracle Database](oracle.md)",
            "`Yes` | `Yes` | `Yes` | `Yes` | `Yes` | `>=12.1.0` | `No` | `>=23.4.0` | `Yes`",
        ),
        ("[SQL Server](mssql.md)", "`Yes` | `Yes` | `Yes` | `Yes` | `Yes` | `>=13.0.0` | `No` | `No` | `Yes`"),
        ("[Google Cloud Spanner](spanner.md)", "`Yes` | `Yes` | `Yes` | `Yes` | `Yes` | `Yes` | `Yes` | `No` | `Yes`"),
        (
            "[SQLite](sqlite.md)",
            "`No` | `>=3.35.0` | `>=3.24.0` | `>=3.25.0` | `>=3.8.3` | `>=3.38.0` | `No` | `No` | `Yes`",
        ),
    ],
)
def test_dialect_capability_matrix_rows(
    dialect_docs: dict[str, str], dialect_row_prefix: str, expected_cells: str
) -> None:
    assert f"| {dialect_row_prefix} | {expected_cells} |" in dialect_docs["index.md"]


def test_dialect_docs_generator_renders_all_dialects_and_checks_staleness(
    dialect_docs: dict[str, str], tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert set(dialect_docs) == {"index.md", *(f"{spec.dialect}.md" for spec in DIALECT_SPECS)}
    assert "[Data Dictionary Reference](../index.md)" in dialect_docs["index.md"]
    assert "::: sqlspec.adapters." not in dialect_docs["postgres.md"]
    assert "??? code" not in dialect_docs["postgres.md"]
    assert "| Domain Pack | Source File | Named Queries | Bind Parameters |" in dialect_docs["postgres.md"]
    assert "`sqlspec/data_dictionary/dialects/postgres/sql/columns.sql`" in dialect_docs["postgres.md"]
    assert "1 named query" in dialect_docs["postgres.md"]
    assert "1 named queries" not in dialect_docs["postgres.md"]
    assert "SYS.SYSTABCOL" in dialect_docs["sqlanywhere.md"]
    assert "sysobjects" in dialect_docs["sybase.md"]

    out_dir = tmp_path / "dialects"
    missing_rc = dialect_docs_main(["--check", "--output-dir", str(out_dir)])
    assert missing_rc == 1
    assert "missing" in capsys.readouterr().err.lower()

    write_rc = dialect_docs_main(["--output-dir", str(out_dir)])
    assert write_rc == 0
    assert (out_dir / "postgres.md").read_text(encoding="utf-8") == dialect_docs["postgres.md"]

    clean_rc = dialect_docs_main(["--check", "--output-dir", str(out_dir)])
    assert clean_rc == 0

    (out_dir / "postgres.md").write_text("# stale\n", encoding="utf-8")
    stale_rc = dialect_docs_main(["--check", "--output-dir", str(out_dir)])
    assert stale_rc == 1
    assert "out of date" in capsys.readouterr().err.lower()


def test_render_typeddict_table_renders_inheritance_types_defaults_and_or_unions() -> None:
    asyncpg_table = render_typeddict_table([
        "sqlspec.adapters.asyncpg.config.AsyncpgConnectionConfig",
        "sqlspec.adapters.asyncpg.config.AsyncpgPoolConfig",
    ])
    lines = asyncpg_table.splitlines()
    assert lines[:2] == ["| Key | Type | Default | Description |", "| --- | --- | --- | --- |"]
    keys = [line.split(" | ")[0].removeprefix("| ") for line in lines[2:]]
    assert len(keys) == len(set(keys))
    assert keys.index("`dsn`") < keys.index("`min_size`")
    assert "| `dsn` | `str` | - |" in asyncpg_table
    assert "| `port` | `int` | `5432` |" in asyncpg_table
    assert "| `connection_class` | `type[AsyncpgConnection]` |" in asyncpg_table
    assert "NotRequired" not in asyncpg_table
    assert all(" | " not in span for line in lines for span in line.split("`")[1::2])

    flask_table = render_typeddict_table(["sqlspec.config.FlaskConfig"])
    assert "| `session_key` | `str` | `'db_session'` |" in flask_table
    assert "| `connection_key` | `str` | - |" in flask_table
    assert "`'manual'`" in flask_table

    litestar_table = render_typeddict_table(["sqlspec.config.LitestarConfig"])
    assert "| `manage_lifespan` | `bool` | - |" in litestar_table

    filter_config_table = render_typeddict_table(["sqlspec.extensions.litestar.providers.FilterConfig"])
    assert '| `id_field` | `str` | `"id"` |' in filter_config_table
    assert "| `pagination_max_size` | `int` | `1000` |" in filter_config_table


def test_render_typeddict_table_cross_module_inheritance_forward_refs_and_defaults(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    package_dir = tmp_path / "doc_table_pkg"
    package_dir.mkdir()
    (package_dir / "__init__.py").write_text('"""Synthetic package."""\n', encoding="utf-8")
    (package_dir / "base.py").write_text(
        '"""Base params."""\n\n'
        "from typing import Literal, NotRequired, TypedDict\n\n"
        "class BaseParams(TypedDict):\n"
        '    """Base parameters."""\n\n'
        "    host: str\n"
        '    """Server hostname. Defaults to ``\'localhost\'``."""\n'
        "    mode: NotRequired[Literal['fast', 'safe']]\n"
        '    """Execution mode (defaults to the server setting)."""\n'
        "    timeout: NotRequired[float]\n"
        '    """Timeout in seconds. Default: 30.0."""\n',
        encoding="utf-8",
    )
    (package_dir / "derived.py").write_text(
        '"""Derived params."""\n\n'
        "from typing import NotRequired\n\n"
        "from doc_table_pkg import base\n\n"
        "class DerivedParams(base.BaseParams):\n"
        '    """Derived parameters."""\n\n'
        '    owner: NotRequired["type[Owner] | None"]\n'
        '    """Owner class. Default: the configured owner."""\n'
        "    timeout: NotRequired[int]\n"
        '    """Timeout in whole seconds. Default: 60."""\n\n'
        "class Owner:\n"
        '    """Owner."""\n',
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))

    table = render_typeddict_table(["doc_table_pkg.derived.DerivedParams"])

    assert table.splitlines()[2:] == [
        "| `host` | `str` | `'localhost'` | Server hostname. Defaults to `'localhost'`. |",
        "| `mode` | `Literal['fast', 'safe']` | - | Execution mode (defaults to the server setting). |",
        "| `timeout` | `int` | `60` | Timeout in whole seconds. Default: 60. |",
        "| `owner` | `type[Owner] or None` | - | Owner class. Default: the configured owner. |",
    ]


def test_render_typeddict_table_keeps_full_multiline_docstrings_with_lists(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    package_dir = tmp_path / "doc_list_pkg"
    package_dir.mkdir()
    (package_dir / "__init__.py").write_text('"""Synthetic package."""\n', encoding="utf-8")
    (package_dir / "params.py").write_text(
        '"""List params."""\n\n'
        "from typing import NotRequired, TypedDict\n\n"
        "class ListParams(TypedDict):\n"
        '    """List parameters."""\n\n'
        "    search: NotRequired[bool]\n"
        '    """Enable search. Default: False.\n\n'
        "    Backends used where available:\n"
        "    - PostgreSQL: GIN index\n"
        "    - SQLite: FTS5 table with a long\n"
        "      continuation line\n\n"
        "    When False, LIKE | ILIKE is used.\n"
        '    """\n'
        "    mode: NotRequired[str]\n"
        '    """Commit mode. Default: \'manual\'\n\n'
        "    Supports:\n"
        "        * manual: explicit commits\n"
        "        * autocommit: commit on success\n"
        '    """\n'
        "    note: NotRequired[int]\n"
        '    """Retry count.\n\n'
        '    Note: values above 10 are capped."""\n',
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))

    rows = render_typeddict_table(["doc_list_pkg.params.ListParams"]).splitlines()[2:]

    assert rows == [
        (
            "| `search` | `bool` | `False` | Enable search. Default: False. Backends used where available: "
            "PostgreSQL: GIN index; SQLite: FTS5 table with a long continuation line. "
            "When False, LIKE / ILIKE is used. |"
        ),
        (
            "| `mode` | `str` | `'manual'` | Commit mode. Default: 'manual'. Supports: "
            "manual: explicit commits; autocommit: commit on success |"
        ),
        "| `note` | `int` | - | Retry count. Note: values above 10 are capped. |",
    ]
    assert not any(row.removesuffix(" |").endswith(":") for row in rows)


def test_filter_catalog_covers_every_concrete_statement_filter(filter_catalog: str) -> None:
    lines = filter_catalog.splitlines()
    assert lines[:2] == [
        "| Filter Class | Constructor Signature | SQL Clause Generated | Description |",
        "| --- | --- | --- | --- |",
    ]
    rendered_names = sorted(line.split("`")[1] for line in lines[2:])
    assert rendered_names == _concrete_filter_names()
    assert all(" | " not in span for line in lines[2:] for span in line.split("`")[1::2])
    assert "| `LimitOffsetFilter` | `LimitOffsetFilter(limit, offset)` |" in filter_catalog


def test_sync_markdown_markers_rewrites_stale_blocks(tmp_path: Path) -> None:
    md_file = tmp_path / "sample.md"
    md_file.write_text(
        "# Sample\n\n"
        "<!-- GENERATED:START config-table:sqlspec.config.FlaskConfig -->\n"
        "stale\n"
        "<!-- GENERATED:END config-table:sqlspec.config.FlaskConfig -->\n\n"
        "<!-- GENERATED:START filter-catalog -->\n"
        "<!-- GENERATED:END filter-catalog -->\n",
        encoding="utf-8",
    )

    stale = sync_markdown_markers(tmp_path, check=True)
    assert len(stale) == 2
    assert md_file.read_text(encoding="utf-8").count("stale\n") == 1

    assert len(sync_markdown_markers(tmp_path, check=False)) == 2
    updated_text = md_file.read_text(encoding="utf-8")
    assert render_typeddict_table(["sqlspec.config.FlaskConfig"]) in updated_text
    assert render_filter_catalog_table() in updated_text
    assert sync_markdown_markers(tmp_path, check=True) == []


def test_sync_markdown_markers_preserves_tab_indentation_and_is_idempotent(tmp_path: Path) -> None:
    md_file = tmp_path / "tabbed.md"
    md_file.write_text(
        '=== "Flask"\n\n'
        "    <!-- GENERATED:START config-table:sqlspec.config.FlaskConfig -->\n"
        "    <!-- GENERATED:END config-table:sqlspec.config.FlaskConfig -->\n\n"
        "Trailing paragraph.\n",
        encoding="utf-8",
    )

    sync_markdown_markers(tmp_path, check=False)
    first_pass = md_file.read_text(encoding="utf-8")
    block_lines = first_pass.split("<!-- GENERATED:START", 1)[1].split("<!-- GENERATED:END", 1)[0].splitlines()[1:-1]
    assert block_lines
    assert all(line.startswith("    | ") for line in block_lines)
    assert first_pass.endswith(
        "    <!-- GENERATED:END config-table:sqlspec.config.FlaskConfig -->\n\nTrailing paragraph.\n"
    )

    assert sync_markdown_markers(tmp_path, check=False) == []
    assert md_file.read_text(encoding="utf-8") == first_pass

    md_file.write_text(first_pass.replace("    | `session_key`", "  | `session_key`"), encoding="utf-8")
    assert len(sync_markdown_markers(tmp_path, check=True)) == 1


@pytest.mark.parametrize(
    ("content", "expected_fragment"),
    [
        ("<!-- GENERATED:START filter-catalog -->\nbody\n", "mismatched GENERATED:START"),
        (
            "<!-- GENERATED:START filter_catalog -->\nbody\n<!-- GENERATED:END filter_catalog -->\n",
            "unsupported or misindented",
        ),
        (
            "    <!-- GENERATED:START filter-catalog -->\nbody\n<!-- GENERATED:END filter-catalog -->\n",
            "unsupported or misindented",
        ),
    ],
    ids=["missing-end", "unknown-spec", "misindented-end"],
)
def test_sync_markdown_markers_rejects_malformed_blocks(tmp_path: Path, content: str, expected_fragment: str) -> None:
    md_file = tmp_path / "broken.md"
    md_file.write_text(content, encoding="utf-8")

    issues = sync_markdown_markers(tmp_path, check=False)

    assert len(issues) == 1
    assert expected_fragment in issues[0]
    assert md_file.read_text(encoding="utf-8") == content
    assert config_tables_main(["--docs-dir", str(tmp_path), "--skip-adapter-parity"]) == 1


def test_config_tables_main_exit_codes(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    (tmp_path / "sample.md").write_text(
        "<!-- GENERATED:START filter-catalog -->\nstale\n<!-- GENERATED:END filter-catalog -->\n", encoding="utf-8"
    )

    assert config_tables_main(["--check", "--docs-dir", str(tmp_path), "--skip-adapter-parity"]) == 1
    assert "sample.md" in capsys.readouterr().err

    assert config_tables_main(["--docs-dir", str(tmp_path), "--skip-adapter-parity"]) == 0
    assert "updated: " in capsys.readouterr().out

    assert config_tables_main(["--check", "--docs-dir", str(tmp_path), "--skip-adapter-parity"]) == 0

    assert check_adapter_reference_parity() == []

    adapter_docs_dir = tmp_path / "reference" / "adapters"
    adapter_docs_dir.mkdir(parents=True)
    (adapter_docs_dir / "oracledb.md").write_text("# oracledb\n", encoding="utf-8")
    synthetic_errors = check_adapter_reference_parity(docs_dir=tmp_path)
    assert any("oracledb" in err and "OracleTopologyConfig" in err for err in synthetic_errors)
    assert config_tables_main(["--check", "--docs-dir", str(tmp_path)]) == 1


def _concrete_filter_names() -> list[str]:
    """Return exported StatementFilter classes excluding the shared abstract base classes."""
    return sorted(
        name
        for name in filters_module.__all__
        if name not in _FILTER_BASE_CLASS_NAMES
        and inspect.isclass(obj := getattr(filters_module, name))
        and issubclass(obj, filters_module.StatementFilter)
    )
