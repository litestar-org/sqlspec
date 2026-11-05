"""Behavioral unit tests for the SQLSpec Click CLI reference Markdown generator."""

from pathlib import Path

import click
import pytest
from tools.docs.generate_cli_docs import _format_param_type, render_cli_reference
from tools.docs.generate_cli_docs import main as cli_docs_main
from tools.docs.generate_config_tables import (
    UNMIGRATED_ADAPTER_REFS,
    check_adapter_reference_parity,
    render_filter_catalog_table,
    render_typeddict_table,
    sync_markdown_markers,
)
from tools.docs.generate_config_tables import main as config_tables_main
from tools.docs.generate_data_dictionary_docs import DIALECT_SPECS, render_all_dialect_docs
from tools.docs.generate_data_dictionary_docs import main as dialect_docs_main
from tools.docs.scan_public_docstrings import main as scan_docstrings_main
from tools.docs.scan_public_docstrings import scan_public_docstring_leaks, scan_undocumented_structs
from tools.docs.verify_docs_artifacts import BANNED_PHRASES, DocsVerificationError, verify_site_artifacts

import sqlspec.cli as cli_module

__all__ = (
    "test_cli_docs_main_writes_and_checks_staleness",
    "test_dialect_docs_generator_renders_all_dialects_and_checks_staleness",
    "test_lazy_cli_attribute_resolves_click_group",
    "test_render_cli_reference_renders_all_commands_arguments_and_options_cleanly",
    "test_render_filter_catalog_table_covers_all_15_filter_types",
    "test_render_typeddict_table_renders_inheritance_types_defaults_and_or_unions",
    "test_scan_public_docstrings_detects_struct_field_violations_and_leak_phrases",
    "test_sync_markdown_markers_and_adapter_parity_check",
    "test_verify_site_artifacts_rejects_heading_code_table_pipes_and_budget_breaches",
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
        "#### sqlspec show-config",
        "#### sqlspec show-current-revision",
        "#### sqlspec verify",
        "#### sqlspec init",
        "#### sqlspec create-migration",
        "#### sqlspec upgrade",
        "#### sqlspec downgrade",
        "#### sqlspec stamp",
        "#### sqlspec squash",
        "#### sqlspec fix",
        "#### sqlspec dump-schema",
        "#### sqlspec adk memory cleanup",
        "#### sqlspec adk memory verify",
    )
    for heading in expected_commands:
        assert heading in markdown
    assert "#### `" not in markdown

    adk_section_pos = markdown.index("## ADK Extension Commands")
    assert markdown.index("#### sqlspec verify") < adk_section_pos
    assert markdown.index("#### sqlspec dump-schema") < adk_section_pos

    assert "| `revision` |" in markdown
    assert "| `directory` |" in markdown
    assert "| `version_range` |" in markdown
    assert "| `-m`, `--message` |" in markdown
    assert "| `--bind-key` |" in markdown
    assert "`choice(sql / py)`" in markdown
    assert "`choice(ddl / schema / concat)`" in markdown
    assert "choice(sql | py)" not in markdown
    assert _format_param_type(click.Option(["--format"], type=click.Choice(["sql", "py"]))) == "choice(sql / py)"

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


def test_dialect_docs_generator_renders_all_dialects_and_checks_staleness(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    rendered = render_all_dialect_docs()
    assert "index.md" in rendered
    assert len(rendered) == len(DIALECT_SPECS) + 1
    assert "spanner" in rendered["index.md"]
    assert "[Data Dictionary Reference](../index.md)" in rendered["index.md"]
    assert (
        "| [Oracle Database](oracle.md) | `Yes` | `Yes` | `Yes` | `Yes` | `Yes` | `>=12.1.0` | `Yes` | `>=23.4.0` | `Yes` |"
        in rendered["index.md"]
    )
    assert (
        "| [SQL Server](mssql.md) | `Yes` | `Yes` | `Yes` | `Yes` | `Yes` | `>=13.0.0` | `No` | `No` | `Yes` |"
        in rendered["index.md"]
    )
    assert (
        "| [Google Cloud Spanner](spanner.md) | `Yes` | `Yes` | `Yes` | `Yes` | `Yes` | `Yes` | `Yes` | `No` | `Yes` |"
        in rendered["index.md"]
    )
    assert "columns.sql" in rendered["postgres.md"]
    assert "::: sqlspec.adapters." not in rendered["postgres.md"]
    assert "??? code" not in rendered["postgres.md"]
    assert "| Domain Pack | Source File | Named Queries | Bind Parameters |" in rendered["postgres.md"]
    assert "`sqlspec/data_dictionary/dialects/postgres/sql/columns.sql`" in rendered["postgres.md"]
    assert "1 named query" in rendered["postgres.md"]
    assert "1 named queries" not in rendered["postgres.md"]
    assert "SYS.SYSTABCOL" in rendered["sqlanywhere.md"]
    assert "sysobjects" in rendered["sybase.md"]

    out_dir = tmp_path / "dialects"
    missing_rc = dialect_docs_main(["--check", "--output-dir", str(out_dir)])
    assert missing_rc == 1
    assert "missing" in capsys.readouterr().err.lower()

    write_rc = dialect_docs_main(["--output-dir", str(out_dir)])
    assert write_rc == 0
    assert (out_dir / "index.md").is_file()
    assert (out_dir / "postgres.md").is_file()

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
    assert asyncpg_table.startswith("| Key | Type | Default | Description |\n| --- | --- | --- | --- |")
    assert "| `dsn` | `str` | - |" in asyncpg_table
    assert "| `port` | `int` | `5432` |" in asyncpg_table
    assert "| `min_size` | `int` |" in asyncpg_table
    assert "NotRequired" not in asyncpg_table
    assert " | " not in "".join(part for line in asyncpg_table.splitlines() for part in line.split("`")[1::2])
    assert asyncpg_table.count("| `dsn` |") == 1

    flask_table = render_typeddict_table(["sqlspec.config.FlaskConfig"])
    assert "| `session_key` | `str` | `'db_session'` |" in flask_table
    assert "| `commit_mode` |" in flask_table
    assert "`'manual'`" in flask_table

    filter_config_table = render_typeddict_table(["sqlspec.extensions.litestar.providers.FilterConfig"])
    assert '| `id_field` | `str` | `"id"` |' in filter_config_table
    assert "| `pagination_max_size` | `int` | `1000` |" in filter_config_table
    assert " or " in filter_config_table


def test_render_filter_catalog_table_covers_all_15_filter_types() -> None:
    table = render_filter_catalog_table()
    assert table.startswith(
        "| Filter Class | Constructor Signature | SQL Clause Generated | Description |\n| --- | --- | --- | --- |"
    )
    expected_filters = (
        "BeforeAfterFilter",
        "OnBeforeAfterFilter",
        "InCollectionFilter",
        "NotInCollectionFilter",
        "AnyCollectionFilter",
        "NotAnyCollectionFilter",
        "ChoicesFilter",
        "LimitOffsetFilter",
        "CursorFilter",
        "OrderByFilter",
        "SearchFilter",
        "NotInSearchFilter",
        "NullFilter",
        "NotNullFilter",
        "BooleanFilter",
    )
    for filter_name in expected_filters:
        assert f"`{filter_name}`" in table
    code_spans = [part for line in table.splitlines()[2:] for part in line.split("`")[1::2]]
    assert all("|" not in span for span in code_spans)


def test_sync_markdown_markers_and_adapter_parity_check(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    md_file = tmp_path / "sample.md"
    md_file.write_text(
        "# Sample\n\n"
        "<!-- GENERATED:START config-table:sqlspec.config.FlaskConfig -->\n"
        "stale\n"
        "<!-- GENERATED:END config-table:sqlspec.config.FlaskConfig -->\n\n"
        "<!-- GENERATED:START filter-catalog -->\n"
        "stale\n"
        "<!-- GENERATED:END filter-catalog -->\n",
        encoding="utf-8",
    )

    check_stale = sync_markdown_markers(tmp_path, check=True)
    assert len(check_stale) == 2

    rc_stale = config_tables_main(["--check", "--docs-dir", str(tmp_path), "--skip-adapter-parity"])
    assert rc_stale == 1
    assert "sample.md" in capsys.readouterr().err

    rc_write = config_tables_main(["--docs-dir", str(tmp_path), "--skip-adapter-parity"])
    assert rc_write == 0
    updated_text = md_file.read_text(encoding="utf-8")
    assert "| `session_key` |" in updated_text
    assert "`BeforeAfterFilter`" in updated_text

    rc_clean = config_tables_main(["--check", "--docs-dir", str(tmp_path), "--skip-adapter-parity"])
    assert rc_clean == 0

    default_parity_errors = check_adapter_reference_parity()
    assert default_parity_errors == []
    assert UNMIGRATED_ADAPTER_REFS == frozenset()

    adapter_docs_dir = tmp_path / "reference" / "adapters"
    adapter_docs_dir.mkdir(parents=True)
    (adapter_docs_dir / "oracledb.md").write_text("# oracledb\n", encoding="utf-8")
    synthetic_errors = check_adapter_reference_parity(docs_dir=tmp_path)
    assert any("oracledb" in err and "OracleTopologyConfig" in err for err in synthetic_errors)


def test_scan_public_docstrings_detects_struct_field_violations_and_leak_phrases(tmp_path: Path) -> None:
    sample_py = tmp_path / "sample_module.py"
    sample_py.write_text(
        '"""Sample module."""\n\n'
        "import msgspec\n\n"
        "class SampleStruct(msgspec.Struct, frozen=True, gc=False):\n"
        '    """Sample struct."""\n'
        "    documented_field: str\n"
        '    """Documented field."""\n'
        "    undocumented_field: int\n\n"
        "def leaky_function() -> None:\n"
        '    """Uses PyCapsule, Sentinel.UNSET, Cython, and Represent a PEP 604 union type."""\n'
        "    return None\n",
        encoding="utf-8",
    )

    struct_violations = scan_undocumented_structs([tmp_path])
    assert len(struct_violations) == 1
    assert struct_violations[0].symbol == "SampleStruct"
    assert struct_violations[0].missing_fields == ("undocumented_field",)

    leak_violations = scan_public_docstring_leaks([tmp_path])
    assert len(leak_violations) == 1
    assert leak_violations[0].symbol == "leaky_function"
    assert set(leak_violations[0].matched_patterns) == {"PEP 604 union leak", "PyCapsule", "Sentinel.UNSET", "Cython"}

    assert (
        scan_docstrings_main(["--check-leaks", "--check-overloads", "--check-typeddicts", "--check-structs", "sqlspec"])
        == 0
    )


def test_verify_site_artifacts_rejects_heading_code_table_pipes_and_budget_breaches(tmp_path: Path) -> None:
    for required in ("objects.inv", "llms.txt", "llms-full.txt"):
        (tmp_path / required).write_text("ok\n", encoding="utf-8")

    index_html = tmp_path / "index.html"
    index_html.write_text(
        "<html><body>"
        '<h2 id="clean">Clean Heading</h2>'
        '<h3 class="doc doc-heading" id="sym"><code>symbol</code></h3>'
        "<table><tr><td><code>str or None</code></td></tr></table>"
        "</body></html>\n",
        encoding="utf-8",
    )
    assert verify_site_artifacts(tmp_path) == []
    assert "PyCapsule" in BANNED_PHRASES
    assert "Sentinel.UNSET" in BANNED_PHRASES
    assert "Cython" in BANNED_PHRASES

    index_html.write_text(
        "<html><body>"
        '<h2 id="bad-heading">Heading with <code>inline code</code></h2>'
        '<h3 class="doc doc-heading" id="sym"><code>symbol</code></h3>'
        r"<table><tr><td><code>str \| None</code></td></tr></table>"
        "</body></html>\n",
        encoding="utf-8",
    )
    with pytest.raises(DocsVerificationError) as exc_info:
        verify_site_artifacts(tmp_path)

    joined = "\n".join(exc_info.value.violations)
    assert "bad-heading" in joined or "inline code" in joined
    assert r"str \| None" in joined
    assert "sym" not in joined

    ref_dir = tmp_path / "reference"
    ref_dir.mkdir()
    (ref_dir / "oversized.html").write_text("<html><body>" + ("x" * 121_000) + "</body></html>", encoding="utf-8")
    index_html.write_text("<html><body><h2>Clean</h2></body></html>\n", encoding="utf-8")
    with pytest.raises(DocsVerificationError, match="exceeds limit of 120000 bytes"):
        verify_site_artifacts(tmp_path)
