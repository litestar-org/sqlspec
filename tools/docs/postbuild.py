"""Post-build generator for llms.txt, llms-full.txt, and flat .html redirect stubs."""

import argparse
import html
import json
import re
import sys
import tomllib
from pathlib import Path
from typing import Any

__all__ = ("build_postbuild_artifacts", "emit_legacy_html_redirects", "generate_llms_files", "main")

_FRONTMATTER_PATTERN = re.compile(r"\A---\r?\n(.*?)\r?\n---(?:\r?\n|$)", re.DOTALL)
_FRONTMATTER_TITLE_PATTERN = re.compile(r"^[ \t]*title:[ \t]*(.+?)[ \t]*$", re.MULTILINE)
_SNIPPET_PATTERN = re.compile(r'^([ \t]*)--8<--[ \t]+"([^"\n]+)"[ \t]*$', re.MULTILINE)
_HEADING_PATTERN = re.compile(r"^#\s+(.+?)\s*$", re.MULTILINE)
_HTML_BLOCK_PATTERN = re.compile(r"<(script|style)\b[^>]*>.*?</\1>", re.IGNORECASE | re.DOTALL)
_MIN_SNIPPET_RANGE_PARTS = 2
_FULL_SNIPPET_RANGE_PARTS = 3
_MIN_QUOTED_SCALAR_LENGTH = 2


def build_postbuild_artifacts(config_path: Path, docs_dir: Path, site_dir: Path) -> None:
    """Generate llms.txt, llms-full.txt, and flat .html redirect files in site_dir."""
    generate_llms_files(config_path=config_path, docs_dir=docs_dir, site_dir=site_dir)
    emit_legacy_html_redirects(config_path=config_path, site_dir=site_dir)


def generate_llms_files(config_path: Path, docs_dir: Path, site_dir: Path) -> None:
    """Write site/llms.txt and site/llms-full.txt ordered by zensical.toml navigation."""
    config = _load_config(config_path)
    project = config.get("project", {})
    site_name = str(project.get("site_name", "Documentation")).strip()
    site_description = str(project.get("site_description", "")).strip()
    site_url = str(project.get("site_url", "")).rstrip("/")
    nav = project.get("nav", [])

    repo_root = config_path.resolve().parent
    sections = _collect_nav_sections(nav=nav, docs_dir=docs_dir)
    if not sections:
        fallback_items = [
            (_extract_markdown_title(md_file), md_file.relative_to(docs_dir).as_posix())
            for md_file in sorted(docs_dir.rglob("*.md"))
        ]
        sections = [("Pages", fallback_items)]

    site_dir.mkdir(parents=True, exist_ok=True)
    llms_lines: list[str] = [f"# {site_name}", ""]
    if site_description:
        llms_lines.extend([f"> {site_description}", ""])

    ordered_rel_paths: list[str] = []
    seen_rel_paths: set[str] = set()

    for section_title, entries in sections:
        if not entries:
            continue
        llms_lines.append(f"## {section_title}")
        llms_lines.append("")
        for label, rel_md in entries:
            clean_url = _doc_path_to_url(rel_md)
            full_url = f"{site_url}{clean_url}" if site_url else clean_url
            page_title = label or _resolve_title_for_rel_path(docs_dir, rel_md)
            llms_lines.append(f"- [{page_title}]({full_url})")
            if rel_md not in seen_rel_paths:
                seen_rel_paths.add(rel_md)
                ordered_rel_paths.append(rel_md)
        llms_lines.append("")

    (site_dir / "llms.txt").write_text("\n".join(llms_lines).rstrip() + "\n", encoding="utf-8")

    full_chunks: list[str] = [f"# {site_name}", ""]
    if site_description:
        full_chunks.extend([f"> {site_description}", ""])

    for rel_md in ordered_rel_paths:
        source_path = docs_dir / rel_md
        if not source_path.is_file():
            continue
        clean_url = _doc_path_to_url(rel_md)
        full_url = f"{site_url}{clean_url}" if site_url else clean_url
        rendered = _expand_markdown_for_llms(
            markdown_text=source_path.read_text(encoding="utf-8"), repo_root=repo_root, page_url=full_url
        )
        full_chunks.append("---")
        full_chunks.append(f"Source: {full_url}")
        full_chunks.append("---")
        full_chunks.append("")
        full_chunks.append(rendered.strip())
        full_chunks.append("")

    (site_dir / "llms-full.txt").write_text("\n".join(full_chunks).rstrip() + "\n", encoding="utf-8")


def emit_legacy_html_redirects(config_path: Path, site_dir: Path) -> int:
    """Emit flat site/<legacy>.html redirect stubs for every redirect_maps entry."""
    config = _load_config(config_path)
    project = config.get("project", {})
    site_url = str(project.get("site_url", "")).rstrip("/")
    plugins = project.get("plugins", {})
    redirects_cfg = plugins.get("redirects", {})
    redirect_maps = redirects_cfg.get("redirect_maps", {})
    if not isinstance(redirect_maps, dict):
        return 0

    written = 0
    for raw_old, raw_new in sorted(redirect_maps.items()):
        old_rel = str(raw_old).strip().lstrip("/")
        new_rel = str(raw_new).strip()
        if not old_rel or not new_rel:
            continue

        old_stem = old_rel.removesuffix(".md")
        if old_stem == "index" or old_stem.endswith("/index"):
            continue

        new_doc, sep, fragment = new_rel.partition("#")
        target_path = _doc_path_to_url(new_doc.lstrip("/"))
        default_hash = f"#{fragment}" if sep and fragment else ""
        refresh_url = f"{target_path}{default_hash}"
        canonical_base = f"{site_url}{target_path}" if site_url else target_path
        canonical_url = f"{canonical_base}{default_hash}"

        out_file = site_dir / f"{old_stem}.html"
        out_file.parent.mkdir(parents=True, exist_ok=True)
        out_file.write_text(
            _render_redirect_html(
                target_path=target_path, default_hash=default_hash, refresh_url=refresh_url, canonical_url=canonical_url
            ),
            encoding="utf-8",
        )
        written += 1

    return written


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint for post-build documentation artifact generation."""
    parser = argparse.ArgumentParser(description="Generate llms.txt, llms-full.txt, and legacy .html redirects.")
    parser.add_argument("--config", type=Path, default=Path("zensical.toml"), help="Path to zensical.toml.")
    parser.add_argument("--docs-dir", type=Path, default=Path("docs"), help="Documentation source directory.")
    parser.add_argument("--site-dir", type=Path, default=Path("site"), help="Built site output directory.")
    args = parser.parse_args(argv)

    generate_llms_files(config_path=args.config, docs_dir=args.docs_dir, site_dir=args.site_dir)
    redirects_count = emit_legacy_html_redirects(config_path=args.config, site_dir=args.site_dir)
    sys.stdout.write(
        f"Generated {args.site_dir / 'llms.txt'}, {args.site_dir / 'llms-full.txt'}, "
        f"and {redirects_count} legacy .html redirect stub(s).\n"
    )
    return 0


def _load_config(config_path: Path) -> dict[str, Any]:
    """Parse zensical.toml using stdlib tomllib."""
    return tomllib.loads(config_path.read_text(encoding="utf-8"))


def _collect_nav_sections(nav: Any, docs_dir: Path) -> list[tuple[str, list[tuple[str, str]]]]:
    """Flatten project.nav into top-level sections of (label, rel_md_path) pairs."""
    if not isinstance(nav, list):
        return []

    sections: list[tuple[str, list[tuple[str, str]]]] = []
    root_items: list[tuple[str, str]] = []

    for item in nav:
        if isinstance(item, str):
            root_items.append((_resolve_title_for_rel_path(docs_dir, item), item))
        elif isinstance(item, dict):
            for key, value in item.items():
                if isinstance(value, str):
                    sections.append((str(key), [(str(key), value)]))
                elif isinstance(value, list):
                    nested: list[tuple[str, str]] = []
                    _collect_nested_nav_items(items=value, docs_dir=docs_dir, prefix="", out=nested)
                    sections.append((str(key), nested))

    if root_items:
        sections.insert(0, ("Overview", root_items))
    return sections


def _collect_nested_nav_items(items: list[Any], docs_dir: Path, prefix: str, out: list[tuple[str, str]]) -> None:
    """Recursively collect leaf Markdown routes from a nested nav list."""
    for entry in items:
        if isinstance(entry, str):
            title = _resolve_title_for_rel_path(docs_dir, entry)
            full_label = f"{prefix}{title}" if prefix else title
            out.append((full_label, entry))
        elif isinstance(entry, dict):
            for label, target in entry.items():
                if isinstance(target, str):
                    full_label = f"{prefix}{label}" if prefix else str(label)
                    out.append((full_label, target))
                elif isinstance(target, list):
                    next_prefix = f"{prefix}{label} - " if prefix else f"{label} - "
                    _collect_nested_nav_items(items=target, docs_dir=docs_dir, prefix=next_prefix, out=out)


def _resolve_title_for_rel_path(docs_dir: Path, rel_md: str) -> str:
    """Extract the H1 title from docs_dir / rel_md or derive a readable fallback."""
    candidate = docs_dir / rel_md
    if candidate.is_file():
        return _extract_markdown_title(candidate)
    stem = Path(rel_md).stem
    if stem == "index":
        parent = Path(rel_md).parent.name
        return parent.replace("-", " ").replace("_", " ").title() or "Overview"
    return stem.replace("-", " ").replace("_", " ").title()


def _extract_markdown_title(md_file: Path) -> str:
    """Read the first Markdown H1 heading or YAML frontmatter title from md_file."""
    frontmatter_title, body = _strip_yaml_frontmatter(md_file.read_text(encoding="utf-8"))
    match = _HEADING_PATTERN.search(body)
    if match:
        return match.group(1).strip()
    if frontmatter_title:
        return frontmatter_title
    stem = md_file.stem
    return stem.replace("-", " ").replace("_", " ").title()


def _strip_yaml_frontmatter(markdown_text: str) -> tuple[str | None, str]:
    """Strip leading YAML frontmatter and return an optional title scalar plus Markdown body."""
    match = _FRONTMATTER_PATTERN.match(markdown_text)
    if match is None:
        return None, markdown_text
    raw_frontmatter = match.group(1)
    body = markdown_text[match.end() :]
    title_match = _FRONTMATTER_TITLE_PATTERN.search(raw_frontmatter)
    if title_match is None:
        return None, body
    raw_title = title_match.group(1).strip()
    if len(raw_title) >= _MIN_QUOTED_SCALAR_LENGTH and raw_title[0] == raw_title[-1] and raw_title[0] in {'"', "'"}:
        raw_title = raw_title[1:-1].strip()
    return (raw_title or None), body


def _doc_path_to_url(rel_md: str) -> str:
    """Convert a docs-relative Markdown path to a directory-style URL path."""
    cleaned = rel_md.strip().lstrip("/")
    if cleaned.endswith(".md"):
        cleaned = cleaned[:-3]
    elif cleaned.endswith(".html"):
        cleaned = cleaned[:-5]

    if cleaned in {"", "index"}:
        return "/"
    if cleaned.endswith("/index"):
        return f"/{cleaned[:-6]}/"
    return f"/{cleaned.rstrip('/')}/"


def _expand_markdown_for_llms(markdown_text: str, repo_root: Path, page_url: str) -> str:
    """Strip YAML frontmatter, expand --8<-- file snippets, and omit HTML widget blocks."""
    _, body = _strip_yaml_frontmatter(markdown_text)

    def _replace_snippet(match: re.Match[str]) -> str:
        indent = match.group(1)
        spec = match.group(2).strip()
        path_part, line_slice = _split_snippet_spec(spec)
        snippet_path = (repo_root / path_part).resolve()
        if not snippet_path.is_relative_to(repo_root) or not snippet_path.is_file():
            return match.group(0)
        if snippet_path.suffix.lower() in {".html", ".htm"}:
            return f"{indent}[Interactive HTML widget omitted; open {page_url} in a browser.]"
        content = snippet_path.read_text(encoding="utf-8")
        if line_slice is not None:
            start_line, end_line = line_slice
            lines = content.splitlines()
            start_idx = max(start_line - 1, 0)
            end_idx = end_line if end_line is not None else len(lines)
            content = "\n".join(lines[start_idx:end_idx])
        return _indent_block(content.rstrip("\n"), indent)

    expanded = _SNIPPET_PATTERN.sub(_replace_snippet, body)
    return _HTML_BLOCK_PATTERN.sub("", expanded)


def _indent_block(text: str, indent: str) -> str:
    """Prefix every non-empty line in text with indent."""
    if not indent:
        return text
    return "\n".join(f"{indent}{line}" if line else "" for line in text.splitlines())


def _split_snippet_spec(spec: str) -> tuple[str, tuple[int, int | None] | None]:
    """Parse optional pymdownx.snippets path:start:end line range syntax."""
    parts = spec.split(":")
    if len(parts) >= _MIN_SNIPPET_RANGE_PARTS and parts[-1].isdigit():
        if len(parts) >= _FULL_SNIPPET_RANGE_PARTS and (parts[-2].isdigit() or parts[-2] == ""):
            start = int(parts[-2]) if parts[-2] else 1
            end = int(parts[-1])
            return ":".join(parts[:-2]), (start, end)
        start = int(parts[-1])
        return ":".join(parts[:-1]), (start, None)
    return spec, None


def _render_redirect_html(*, target_path: str, default_hash: str, refresh_url: str, canonical_url: str) -> str:
    """Render a minimal fragment-preserving HTML redirect document."""
    escaped_refresh = html.escape(refresh_url, quote=True)
    escaped_canonical = html.escape(canonical_url, quote=True)
    js_target = json.dumps(target_path)
    if default_hash:
        js_default_hash = json.dumps(default_hash)
        script_body = (
            f"const hash = window.location.hash || {js_default_hash}; window.location.replace({js_target} + hash);"
        )
    else:
        script_body = f"window.location.replace({js_target} + window.location.hash);"

    return (
        "<!doctype html>\n"
        '<html lang="en">\n'
        "<head>\n"
        '  <meta charset="utf-8">\n'
        f'  <meta http-equiv="refresh" content="0; url={escaped_refresh}">\n'
        f'  <link rel="canonical" href="{escaped_canonical}">\n'
        "  <title>Redirecting...</title>\n"
        f"  <script>{script_body}</script>\n"
        "</head>\n"
        "<body>\n"
        f'  <p>Redirecting to <a href="{escaped_refresh}">{escaped_canonical}</a>.</p>\n'
        "</body>\n"
        "</html>\n"
    )


if __name__ == "__main__":
    raise SystemExit(main())
