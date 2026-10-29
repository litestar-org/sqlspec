"""Behavioral unit tests for Zensical documentation post-build and verification tools."""

from pathlib import Path

import pytest
from tools.docs.postbuild import build_postbuild_artifacts, emit_legacy_html_redirects, generate_llms_files
from tools.docs.postbuild import main as postbuild_main
from tools.docs.verify_docs_artifacts import DocsVerificationError, verify_site_artifacts
from tools.docs.verify_docs_artifacts import main as verify_main

__all__ = (
    "test_build_postbuild_artifacts_and_cli_main",
    "test_emit_legacy_html_redirects_preserves_hashes",
    "test_generate_llms_files_expands_snippets_and_orders_nav",
    "test_verify_site_artifacts_accepts_clean_site_and_exemptions",
    "test_verify_site_artifacts_rejects_bloated_secondary_toc",
    "test_verify_site_artifacts_rejects_broken_same_page_anchors",
    "test_verify_site_artifacts_rejects_leaked_docstrings_and_tmp_path",
    "test_verify_site_artifacts_rejects_missing_inventories",
    "test_verify_site_artifacts_rejects_oversized_pages",
)


def test_generate_llms_files_expands_snippets_and_orders_nav(tmp_path: Path) -> None:
    config_path, docs_dir, site_dir = _write_sample_project(tmp_path)

    generate_llms_files(config_path=config_path, docs_dir=docs_dir, site_dir=site_dir)

    llms_txt = (site_dir / "llms.txt").read_text(encoding="utf-8")
    llms_full = (site_dir / "llms-full.txt").read_text(encoding="utf-8")

    assert llms_txt.startswith("# SQLSpec\n\n> Typed SQL query execution for Python.")
    assert "https://sqlspec.dev/" in llms_txt
    assert "https://sqlspec.dev/learn/" in llms_txt
    assert "https://sqlspec.dev/learn/quickstart/" in llms_txt
    assert llms_txt.index("/learn/") < llms_txt.index("/learn/quickstart/")

    assert "def main() -> int:" in llms_full
    assert "return 42" in llms_full
    assert '--8<-- "docs/examples/demo.py"' not in llms_full
    assert "<script>console.log(1);</script>" not in llms_full


def test_emit_legacy_html_redirects_preserves_hashes(tmp_path: Path) -> None:
    config_path, _, site_dir = _write_sample_project(tmp_path)

    emit_legacy_html_redirects(config_path=config_path, site_dir=site_dir)

    migrations_stub = (site_dir / "usage" / "migrations.html").read_text(encoding="utf-8")
    assert "url=/learn/quickstart/" in migrations_stub
    assert 'href="https://sqlspec.dev/learn/quickstart/"' in migrations_stub
    assert "window.location.hash" in migrations_stub

    adk_stub = (site_dir / "extensions" / "adk" / "api.html").read_text(encoding="utf-8")
    assert "/reference/extensions/adk/#stores" in adk_stub
    assert "window.location.hash || " in adk_stub

    root_stub = (site_dir / "index_legacy.html").read_text(encoding="utf-8")
    assert "url=/" in root_stub


def test_build_postbuild_artifacts_and_cli_main(tmp_path: Path) -> None:
    config_path, docs_dir, site_dir = _write_sample_project(tmp_path)

    build_postbuild_artifacts(config_path=config_path, docs_dir=docs_dir, site_dir=site_dir)
    assert (site_dir / "llms.txt").is_file()
    assert (site_dir / "llms-full.txt").is_file()
    assert (site_dir / "usage" / "migrations.html").is_file()

    rc = postbuild_main(["--config", str(config_path), "--docs-dir", str(docs_dir), "--site-dir", str(site_dir)])
    assert rc == 0


def test_verify_site_artifacts_accepts_clean_site_and_exemptions(tmp_path: Path) -> None:
    site_dir = tmp_path / "site"
    _write_clean_site(site_dir)

    playground_dir = site_dir / "playground"
    playground_dir.mkdir(parents=True)
    (playground_dir / "index.html").write_text(
        "<!doctype html><html><body>" + ("x" * 200_000) + "</body></html>", encoding="utf-8"
    )

    changelog_dir = site_dir / "changelog"
    changelog_dir.mkdir(parents=True)
    many_toc = "".join(f'<li><a class="md-nav__link" href="#v{i}">v{i}</a></li>' for i in range(25))
    many_h2 = "".join(f'<h2 id="v{i}">v{i}</h2>' for i in range(25))
    (changelog_dir / "index.html").write_text(
        f'<!doctype html><html><body><nav class="md-nav md-nav--secondary"><ul>{many_toc}</ul></nav>'
        f"{many_h2}" + ("y" * 200_000) + "</body></html>",
        encoding="utf-8",
    )

    assert verify_site_artifacts(site_dir) == []
    assert verify_main(["--site-dir", str(site_dir)]) == 0


def test_verify_site_artifacts_rejects_missing_inventories(tmp_path: Path) -> None:
    site_dir = tmp_path / "site"
    _write_clean_site(site_dir)
    (site_dir / "objects.inv").unlink()

    with pytest.raises(DocsVerificationError, match=r"objects\.inv"):
        verify_site_artifacts(site_dir)
    assert verify_main(["--site-dir", str(site_dir)]) == 1


@pytest.mark.parametrize(
    "leaked_phrase",
    [
        "Represent a PEP 604 union type",
        "avoid mypyc segmentation faults",
        "prevent ABI boundary violations",
        "NameError: name 'tmp_path' is not defined",
        'db_path = tmp_path / "demo.db"',
    ],
)
def test_verify_site_artifacts_rejects_leaked_docstrings_and_tmp_path(tmp_path: Path, leaked_phrase: str) -> None:
    site_dir = tmp_path / "site"
    _write_clean_site(site_dir)
    leak_page = site_dir / "reference" / "leak.html"
    leak_page.write_text(f"<!doctype html><html><body><p>{leaked_phrase}</p></body></html>", encoding="utf-8")

    with pytest.raises(DocsVerificationError) as exc_info:
        verify_site_artifacts(site_dir)
    assert any("leak.html" in item for item in exc_info.value.violations)


def test_verify_site_artifacts_rejects_oversized_pages(tmp_path: Path) -> None:
    site_dir = tmp_path / "site"
    _write_clean_site(site_dir)
    bloated_ref = site_dir / "reference" / "bloated.html"
    bloated_ref.write_text("<!doctype html><html><body>" + ("a" * 125_000) + "</body></html>", encoding="utf-8")

    with pytest.raises(DocsVerificationError, match=r"bloated\.html"):
        verify_site_artifacts(site_dir)


def test_verify_site_artifacts_rejects_bloated_secondary_toc(tmp_path: Path) -> None:
    site_dir = tmp_path / "site"
    _write_clean_site(site_dir)
    toc_links = "".join(f'<li><a class="md-nav__link" href="#m-{idx}">m_{idx}</a></li>' for idx in range(15))
    headings = "".join(f'<h3 id="m-{idx}">m_{idx}</h3>' for idx in range(15))
    bloated_toc = site_dir / "reference" / "methods.html"
    bloated_toc.write_text(
        f'<!doctype html><html><body><nav class="md-nav md-nav--secondary"><ul>{toc_links}</ul></nav>{headings}</body></html>',
        encoding="utf-8",
    )

    with pytest.raises(DocsVerificationError, match=r"methods\.html"):
        verify_site_artifacts(site_dir)


def test_verify_site_artifacts_rejects_broken_same_page_anchors(tmp_path: Path) -> None:
    site_dir = tmp_path / "site"
    _write_clean_site(site_dir)
    broken_page = site_dir / "learn" / "broken.html"
    broken_page.parent.mkdir(parents=True, exist_ok=True)
    broken_page.write_text(
        '<!doctype html><html><body><h1 id="present">Present</h1><a href="#missing-id">Broken</a></body></html>',
        encoding="utf-8",
    )

    with pytest.raises(DocsVerificationError, match="#missing-id"):
        verify_site_artifacts(site_dir)


def _write_sample_project(root: Path) -> tuple[Path, Path, Path]:
    """Create a synthetic Zensical workspace for post-build testing."""
    docs_dir = root / "docs"
    site_dir = root / "site"
    examples_dir = docs_dir / "examples"
    includes_dir = root / "tools" / "docs" / "includes"
    (docs_dir / "learn").mkdir(parents=True)
    examples_dir.mkdir(parents=True)
    includes_dir.mkdir(parents=True)
    site_dir.mkdir(parents=True)

    (examples_dir / "demo.py").write_text(
        '"""Standalone demo."""\n\n\ndef main() -> int:\n    """Return answer."""\n    return 42\n', encoding="utf-8"
    )
    (includes_dir / "widget.html").write_text(
        '<div id="sqlspec-playground-main"><script>console.log(1);</script></div>\n', encoding="utf-8"
    )
    (docs_dir / "index.md").write_text("# SQLSpec\n\nTyped SQL execution for Python.\n", encoding="utf-8")
    (docs_dir / "learn" / "index.md").write_text("# Learn Overview\n\nStart learning SQLSpec.\n", encoding="utf-8")
    (docs_dir / "learn" / "quickstart.md").write_text(
        '# Quickstart\n\nRun your first query:\n\n```python\n--8<-- "docs/examples/demo.py"\n```\n', encoding="utf-8"
    )
    (docs_dir / "playground.md").write_text(
        '# Interactive Playground\n\nTry SQLSpec in browser.\n\n--8<-- "tools/docs/includes/widget.html"\n',
        encoding="utf-8",
    )

    config_path = root / "zensical.toml"
    config_path.write_text(
        """
[project]
site_name = "SQLSpec"
site_description = "Typed SQL query execution for Python."
site_url = "https://sqlspec.dev/"
docs_dir = "docs"
site_dir = "site"
nav = [
  { "Home" = "index.md" },
  { "Learn" = [
    "learn/index.md",
    { "Quickstart" = "learn/quickstart.md" },
  ] },
  { "Playground" = "playground.md" },
]

[project.plugins.redirects.redirect_maps]
"usage/migrations.md" = "learn/quickstart.md"
"extensions/adk/api.md" = "reference/extensions/adk.md#stores"
"index_legacy.md" = "index.md"
""".strip()
        + "\n",
        encoding="utf-8",
    )
    return config_path, docs_dir, site_dir


def _write_clean_site(site_dir: Path) -> None:
    """Populate a valid built Zensical site directory."""
    site_dir.mkdir(parents=True, exist_ok=True)
    (site_dir / "objects.inv").write_bytes(b"# Sphinx inventory version 2\n")
    (site_dir / "llms.txt").write_text("# SQLSpec\n", encoding="utf-8")
    (site_dir / "llms-full.txt").write_text("# SQLSpec Full\n", encoding="utf-8")

    toc_links = "".join(f'<li><a class="md-nav__link" href="#sec-{idx}">Section {idx}</a></li>' for idx in range(6))
    headings = "".join(f'<h2 id="sec-{idx}">Section {idx}</h2>' for idx in range(6))
    (site_dir / "index.html").write_text(
        f"<!doctype html><html><body>"
        f'<nav class="md-nav md-nav--secondary"><ul>{toc_links}</ul></nav>'
        f'<main><h1 id="top">SQLSpec</h1>{headings}<a href="#sec-2">Jump</a><a href="#">Top</a></main>'
        f"</body></html>",
        encoding="utf-8",
    )

    ref_dir = site_dir / "reference" / "adapters" / "oracledb"
    ref_dir.mkdir(parents=True)
    (ref_dir / "index.html").write_text(
        "<!doctype html><html><body>"
        '<nav class="md-nav md-nav--secondary"><ul>'
        '<li><a class="md-nav__link" href="#configuration">Configuration</a></li>'
        '<li><a class="md-nav__link" href="#oraclesyncconfig">OracleSyncConfig</a></li>'
        "</ul></nav>"
        '<h1 id="oracle">Oracle</h1>'
        '<h2 id="configuration">Configuration</h2>'
        '<h3 id="oraclesyncconfig">OracleSyncConfig</h3>'
        '<h4 id="oraclesyncconfig-provide_session">provide_session</h4>'
        '<a href="#oraclesyncconfig-provide_session">provide_session</a>'
        "</body></html>",
        encoding="utf-8",
    )
