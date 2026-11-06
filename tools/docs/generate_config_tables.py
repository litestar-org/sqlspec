"""Generate and verify deterministic TypedDict configuration and StatementFilter Markdown tables."""

import argparse
import ast
import inspect
import re
import sys
from collections.abc import Sequence
from functools import cache
from pathlib import Path
from typing import Final, cast

from sqlspec.core.filters import (
    AnyCollectionFilter,
    BeforeAfterFilter,
    BooleanFilter,
    ChoicesFilter,
    CursorFilter,
    InCollectionFilter,
    LimitOffsetFilter,
    NotAnyCollectionFilter,
    NotInCollectionFilter,
    NotInSearchFilter,
    NotNullFilter,
    NullFilter,
    OnBeforeAfterFilter,
    OrderByFilter,
    SearchFilter,
    StatementFilter,
)

__all__ = (
    "check_adapter_reference_parity",
    "main",
    "render_filter_catalog_table",
    "render_typeddict_table",
    "sync_markdown_markers",
)

_FILTER_CATALOG_SPECS: tuple[tuple[type[StatementFilter], str], ...] = (
    (BeforeAfterFilter, "WHERE col < :col_before AND col > :col_after"),
    (OnBeforeAfterFilter, "WHERE col <= :col_on_or_before AND col >= :col_on_or_after"),
    (InCollectionFilter, "WHERE col IN (:col_in_0, :col_in_1, ...)"),
    (NotInCollectionFilter, "WHERE col NOT IN (:col_notin_0, :col_notin_1, ...)"),
    (AnyCollectionFilter, "WHERE col = ANY ([:col_any_0, :col_any_1, ...])"),
    (NotAnyCollectionFilter, "WHERE NOT (col = ANY ([:col_not_any_0, ...]))"),
    (ChoicesFilter, "WHERE col IN (:col_choices_0, :col_choices_1, ...)"),
    (LimitOffsetFilter, "LIMIT :limit OFFSET :offset"),
    (CursorFilter, "WHERE (col, id) > (:cursor_k0, :cursor_k1) ORDER BY col ASC, id ASC LIMIT :cursor_limit"),
    (OrderByFilter, "ORDER BY col ASC | DESC [NULLS FIRST | LAST]"),
    (SearchFilter, "WHERE col LIKE :col_search (or ILIKE when ignore_case=True)"),
    (NotInSearchFilter, "WHERE col NOT LIKE :col_not_search (or NOT ILIKE when ignore_case=True)"),
    (NullFilter, "WHERE col IS NULL"),
    (NotNullFilter, "WHERE col IS NOT NULL"),
    (BooleanFilter, "WHERE col = :col_boolean"),
)

_MAX_DEFAULT_VAL_LEN: Final[int] = 48
_SENTENCE_ENDINGS: tuple[str, ...] = (".", ":", ";", "!", "?")
_MARKER_BLOCK_PATTERN = re.compile(
    r"^(?P<indent>[ \t]*)(?P<start><!-- GENERATED:START (?P<spec>config-table:[A-Za-z0-9_.,]+|filter-catalog) -->)\n"
    r"(?:(?P<body>.*?)\n)?"
    r"(?P=indent)(?P<end><!-- GENERATED:END (?P=spec) -->)",
    re.DOTALL | re.MULTILINE,
)
_MARKER_START_PATTERN = re.compile(r"<!-- GENERATED:START (?P<spec>[^>]+?) -->")
_MARKER_END_PATTERN = re.compile(r"<!-- GENERATED:END (?P<spec>[^>]+?) -->")
_CODE_SPAN_DEFAULT_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\(\s*defaults?\s+to\s+`([^`]+)`[^)]*\)", re.IGNORECASE),
    re.compile(r"(?:^|\.\s+|;\s+)Defaults?\s+to\s+`([^`]+)`", re.IGNORECASE),
    re.compile(r"(?:^|\.\s+|;\s+)Default:\s*`([^`]+)`", re.IGNORECASE),
)
_PROSE_DEFAULT_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\(\s*defaults?\s+to\s+([^);]+?)\s*\)", re.IGNORECASE),
    re.compile(r"(?:^|\.\s+|;\s+)Defaults?\s+to\s+(['\"][^'\"]+['\"]|True|False|None|\d+(?:\.\d+)?)\b", re.IGNORECASE),
    re.compile(r"(?:^|\.\s+|;\s+)Default:\s*(.+?)(?:\.(?:\s|$)|$)", re.IGNORECASE),
)
_LITERAL_DEFAULT_PATTERN = re.compile(
    r"^(?:'[^']*'|\"[^\"]*\"|-?\d+(?:\.\d+)?|True|False|None|[A-Za-z_][\w.]*(?:\(\))?)$"
)
_TYPE_WRAPPERS_TO_UNWRAP: frozenset[str] = frozenset({
    "NotRequired",
    "Required",
    "typing.NotRequired",
    "typing.Required",
    "typing_extensions.NotRequired",
    "typing_extensions.Required",
})
_LITERAL_TYPE_NAMES: frozenset[str] = frozenset({"Literal", "typing.Literal", "typing_extensions.Literal"})
_TYPEDDICT_BASE_NAMES: frozenset[str] = frozenset({"TypedDict", "typing.TypedDict", "typing_extensions.TypedDict"})
_EXCLUDED_ADAPTER_SYMBOL_SUFFIXES: tuple[str, ...] = (
    "Connection",
    "ConnectionContext",
    "Converter",
    "Cursor",
    "Error",
    "GSSLib",
    "Json",
    "Blob",
    "Clob",
    "MigrationTracker",
    "OutputConverter",
    "PoolConnection",
    "PreparedStatement",
    "SessionContext",
    "SslConfig",
    "TargetSessionAttrs",
    "Timeout",
    "TypeConverter",
    "VersionInfo",
)


def render_typeddict_table(symbol_paths: Sequence[str]) -> str:
    """Render a deterministic Markdown table from one or more TypedDict schemas.

    Fields are emitted base-first across each TypedDict inheritance chain. A key
    redeclared by a subclass or by a later symbol keeps its first position but takes
    the most derived type, default, and description.

    Args:
        symbol_paths: Dotted module and symbol paths (for example
            ``["sqlspec.adapters.asyncpg.config.AsyncpgConnectionConfig"]``).

    Returns:
        Markdown table string with header ``| Key | Type | Default | Description |``.
    """
    fields: dict[str, tuple[str, str, str]] = {}
    for raw_path in symbol_paths:
        symbol_path = raw_path.strip()
        if not symbol_path:
            continue
        for key, type_cell, default_cell, desc_cell in _extract_typeddict_chain_fields(symbol_path):
            fields[key] = (type_cell, default_cell, desc_cell)

    rows: list[str] = ["| Key | Type | Default | Description |", "| --- | --- | --- | --- |"]
    rows.extend(
        f"| `{key}` | {type_cell} | {default_cell} | {desc_cell} |"
        for key, (type_cell, default_cell, desc_cell) in fields.items()
    )
    return "\n".join(rows)


def render_filter_catalog_table() -> str:
    """Render a deterministic Markdown table covering every concrete StatementFilter class.

    Returns:
        Markdown table string with header
        ``| Filter Class | Constructor Signature | SQL Clause Generated | Description |``.
    """
    rows: list[str] = [
        "| Filter Class | Constructor Signature | SQL Clause Generated | Description |",
        "| --- | --- | --- | --- |",
    ]
    for filter_cls, sql_clause in _FILTER_CATALOG_SPECS:
        sig_text = _format_filter_signature(filter_cls)
        clean_clause = sql_clause.replace(" | ", " / ")
        summary = _extract_first_paragraph(inspect.getdoc(filter_cls) or "")
        rows.append(f"| `{filter_cls.__name__}` | `{sig_text}` | `{clean_clause}` | {summary} |")
    return "\n".join(rows)


def sync_markdown_markers(docs_dir: Path = Path("docs"), *, check: bool = False) -> list[str]:
    """Synchronize or verify GENERATED:START/END marker blocks across Markdown files.

    Args:
        docs_dir: Root documentation directory to scan.
        check: When True, report stale or malformed blocks without writing files.

    Returns:
        Diagnostic messages for stale, malformed, or unsupported marker blocks.
        In write mode stale blocks are rewritten and still reported.
    """
    issues: list[str] = []
    if not docs_dir.exists():
        return issues

    for md_path in sorted(docs_dir.rglob("*.md")):
        original = md_path.read_text(encoding="utf-8")
        starts = [m.group("spec").strip() for m in _MARKER_START_PATTERN.finditer(original)]
        ends = [m.group("spec").strip() for m in _MARKER_END_PATTERN.finditer(original)]
        if starts != ends:
            issues.append(f"{md_path}: mismatched GENERATED:START ({starts}) and GENERATED:END ({ends}) markers")
            continue
        if not starts:
            continue

        recognized = [m.group("spec") for m in _MARKER_BLOCK_PATTERN.finditer(original)]
        if len(recognized) != len(starts):
            unsupported = sorted(set(starts) - set(recognized))
            issues.append(f"{md_path}: unsupported or misindented GENERATED marker block(s): {unsupported or starts}")
            continue

        file_stale, updated = _sync_single_file_markers(md_path, original)
        if file_stale:
            issues.extend(file_stale)
            if not check:
                md_path.write_text(updated, encoding="utf-8")

    return issues


def check_adapter_reference_parity(
    docs_dir: Path = Path("docs"), adapters_dir: Path = Path("sqlspec/adapters")
) -> list[str]:
    """Verify that adapter Reference Markdown pages document all public classes exported by each adapter.

    Args:
        docs_dir: Documentation root directory containing ``reference/adapters/*.md``.
        adapters_dir: Source directory containing ``sqlspec/adapters/<adapter>/__init__.py``.

    Returns:
        List of parity violation strings.
    """
    errors: list[str] = []
    adapter_docs_dir = docs_dir / "reference" / "adapters"
    if not adapters_dir.is_dir() or not adapter_docs_dir.is_dir():
        return errors

    for init_file in sorted(adapters_dir.glob("*/__init__.py")):
        adapter = init_file.parent.name
        init_exports = _extract_module_all(init_file)
        config_exports = _extract_module_all(init_file.parent / "config.py")
        combined_exports = list(dict.fromkeys(init_exports + config_exports))

        doc_file = adapter_docs_dir / f"{adapter}.md"
        if not doc_file.is_file():
            doc_file = adapter_docs_dir / f"{adapter.replace('_', '-')}.md"
        if not doc_file.is_file():
            errors.append(f"{adapter}: missing reference page in {adapter_docs_dir}")
            continue

        doc_text = doc_file.read_text(encoding="utf-8")
        doc_imports = _extract_opening_imports(doc_text, adapter)
        directives = set(
            re.findall(rf"^:::\s+sqlspec\.adapters\.{re.escape(adapter)}\.([A-Za-z0-9_.]+)", doc_text, re.MULTILINE)
        )
        directive_leafs = {item.split(".")[-1] for item in directives}
        documented = doc_imports | directive_leafs

        required: list[str] = []
        for symbol in combined_exports:
            if not symbol or not symbol[0].isupper() or symbol.isupper():
                continue
            if symbol.endswith(_EXCLUDED_ADAPTER_SYMBOL_SUFFIXES):
                continue
            if symbol.endswith("DataDictionary") and any(d.endswith("DataDictionary") for d in documented):
                continue
            required.append(symbol)

        missing = sorted(set(required) - documented)
        if missing:
            errors.append(f"{doc_file}: missing exported adapter symbols: {', '.join(missing)}")

        extra_imports = sorted(doc_imports - set(init_exports))
        if extra_imports:
            errors.append(f"{doc_file}: opening import block lists symbols not in __all__: {', '.join(extra_imports)}")

        unrendered_imports = sorted(doc_imports - directive_leafs)
        if unrendered_imports:
            errors.append(
                f"{doc_file}: opening import block symbols missing ':::' directives: {', '.join(unrendered_imports)}"
            )

    return errors


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entry point for generating or checking deterministic config/filter tables and adapter parity.

    Write mode rewrites stale marker blocks and then re-verifies them, so malformed
    markers that cannot be repaired automatically still fail the run.

    Args:
        argv: Optional sequence of CLI arguments.

    Returns:
        Exit code ``0`` when clean or successfully updated, or ``1`` when stale blocks
        (under ``--check``), malformed markers, or adapter parity violations remain.
    """
    parser = argparse.ArgumentParser(
        description="Synchronize or verify generated TypedDict/filter Markdown tables and adapter reference parity."
    )
    parser.add_argument(
        "--docs-dir", type=Path, default=Path("docs"), help="Root documentation directory (default: docs)."
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Verify that all marker blocks and adapter reference pages are up to date without modifying files.",
    )
    parser.add_argument(
        "--skip-adapter-parity",
        action="store_true",
        help="Skip checking docs/reference/adapters/*.md against sqlspec/adapters/*/__init__.py.",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)

    if not args.check:
        updated = sync_markdown_markers(args.docs_dir, check=False)
        for item in updated:
            sys.stdout.write(f"updated: {item}\n")

    marker_issues = sync_markdown_markers(args.docs_dir, check=True)
    parity_issues = [] if args.skip_adapter_parity else check_adapter_reference_parity(docs_dir=args.docs_dir)
    all_issues = [*marker_issues, *parity_issues]
    if all_issues:
        for issue in all_issues:
            sys.stderr.write(f"{issue}\n")
        if marker_issues:
            sys.stderr.write("Run 'uv run python -m tools.docs.generate_config_tables' to regenerate.\n")
        return 1

    sys.stdout.write("Config and filter tables are up to date.\n")
    return 0


def _sync_single_file_markers(md_path: Path, original: str) -> tuple[list[str], str]:
    """Replace marker blocks in a single Markdown file text and report stale blocks.

    The rendered body is re-indented with the marker indentation so tables nested
    inside tabbed or admonition blocks stay inside their container. Staleness is an
    exact comparison against that rendered body, which keeps repeated runs idempotent.
    """
    file_stale: list[str] = []

    def _replace_block(match: re.Match[str]) -> str:
        indent = match.group("indent")
        spec = match.group("spec")
        current_body = match.group("body") or ""
        raw_expected = _render_marker_spec(spec).strip()
        expected_body = "\n".join(f"{indent}{line}" if line else "" for line in raw_expected.splitlines())
        if current_body != expected_body:
            file_stale.append(f"{md_path}: out-of-date marker block '{spec}'")
        return f"{indent}{match.group('start')}\n{expected_body}\n{indent}{match.group('end')}"

    updated = _MARKER_BLOCK_PATTERN.sub(_replace_block, original)
    return file_stale, updated


def _render_marker_spec(spec: str) -> str:
    """Dispatch a GENERATED marker specification to its table renderer."""
    if spec == "filter-catalog":
        return render_filter_catalog_table()
    raw_symbols = spec.removeprefix("config-table:").split(",")
    return render_typeddict_table([item.strip() for item in raw_symbols if item.strip()])


def _find_module_source_path(module_name: str) -> Path | None:
    """Resolve a dotted module name to its Python source file on sys.path without importing it."""
    rel_parts = module_name.split(".")
    rel_module = Path(*rel_parts).with_suffix(".py")
    rel_pkg_init = Path(*rel_parts, "__init__.py")
    search_roots = [*(Path(entry) for entry in sys.path if entry), Path.cwd()]
    for root in search_roots:
        module_candidate = root / rel_module
        if module_candidate.is_file():
            return module_candidate.resolve()
        pkg_candidate = root / rel_pkg_init
        if pkg_candidate.is_file():
            return pkg_candidate.resolve()
    return None


@cache
def _parse_source_file_ast(source_path: Path, module_name: str) -> tuple[dict[str, ast.ClassDef], dict[str, str]]:
    """Parse a Python source file once and index its top-level ClassDefs and import bindings."""
    tree = ast.parse(source_path.read_text(encoding="utf-8"), filename=str(source_path))
    classes = {node.name: node for node in tree.body if isinstance(node, ast.ClassDef)}
    package_parts = module_name.split(".") if source_path.name == "__init__.py" else module_name.split(".")[:-1]
    imports: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                local_name = alias.asname or alias.name
                imports[local_name] = alias.name
        elif isinstance(node, ast.ImportFrom):
            base_parts = list(package_parts)
            if node.level > 0:
                trim = node.level - 1
                base_parts = base_parts[: len(base_parts) - trim] if trim <= len(base_parts) else []
            if node.module:
                target_module = ".".join([*base_parts, node.module]) if node.level > 0 else node.module
            else:
                target_module = ".".join(base_parts)
            for alias in node.names:
                if alias.name == "*":
                    continue
                local_name = alias.asname or alias.name
                imports[local_name] = f"{target_module}.{alias.name}" if target_module else alias.name
    return classes, imports


def _parse_module_ast(module_name: str) -> tuple[dict[str, ast.ClassDef], dict[str, str]]:
    """Locate a module on disk without importing it and return its AST class and import maps."""
    source_path = _find_module_source_path(module_name)
    if source_path is None:
        return {}, {}
    return _parse_source_file_ast(source_path, module_name)


def _module_class_nodes(module_name: str) -> dict[str, ast.ClassDef]:
    """Parse a module's source once and index its top-level class definitions by name."""
    classes, _ = _parse_module_ast(module_name)
    return classes


def _resolve_symbol_class_node(
    symbol_path: str, seen_aliases: set[str] | None = None
) -> tuple[str, ast.ClassDef] | None:
    """Resolve a dotted class path to its defining module name and AST ClassDef without importing."""
    visited = seen_aliases if seen_aliases is not None else set()
    if symbol_path in visited:
        return None
    visited.add(symbol_path)

    module_name, _, class_name = symbol_path.rpartition(".")
    if not module_name or not class_name:
        return None
    classes, imports = _parse_module_ast(module_name)
    cls_node = classes.get(class_name)
    if cls_node is not None:
        return module_name, cls_node
    imported_target = imports.get(class_name)
    if imported_target is not None:
        return _resolve_symbol_class_node(imported_target, visited)
    return None


def _resolve_base_symbol_path(module_name: str, base_expr: ast.expr) -> str | None:
    """Resolve an AST base class expression against its defining module's AST scope."""
    target_expr = base_expr.value if isinstance(base_expr, ast.Subscript) else base_expr
    raw_base = ast.unparse(target_expr).strip()
    if raw_base in _TYPEDDICT_BASE_NAMES:
        return "TypedDict"
    classes, imports = _parse_module_ast(module_name)
    if raw_base in imports:
        target = imports[raw_base]
        if target in _TYPEDDICT_BASE_NAMES or target.endswith(".TypedDict"):
            return "TypedDict"
        return target
    if raw_base in classes:
        return f"{module_name}.{raw_base}"
    if "." in raw_base:
        prefix, _, attr = raw_base.rpartition(".")
        if prefix in imports:
            resolved = f"{imports[prefix]}.{attr}"
            if resolved in _TYPEDDICT_BASE_NAMES or resolved.endswith(".TypedDict"):
                return "TypedDict"
            return resolved
        return raw_base
    return None


def _collect_typeddict_class_nodes(symbol_path: str) -> list[ast.ClassDef]:
    """Return AST nodes for a TypedDict and all TypedDict ancestors in base-first order.

    Base expressions are resolved against each class's defining module via AST and
    filesystem lookup, so inheritance works within one module and across modules
    without importing optional third-party database driver dependencies.
    """
    ordered: list[ast.ClassDef] = []
    typeddict_status: dict[str, bool] = {}

    def _visit(current_symbol: str) -> bool:
        resolved = _resolve_symbol_class_node(current_symbol)
        if resolved is None:
            return False
        canonical_module, cls_node = resolved
        canonical_symbol = f"{canonical_module}.{cls_node.name}"
        if canonical_symbol in typeddict_status:
            return typeddict_status[canonical_symbol]
        typeddict_status[canonical_symbol] = False

        is_td = False
        for base_expr in cls_node.bases:
            base_target = _resolve_base_symbol_path(canonical_module, base_expr)
            if base_target == "TypedDict" or (base_target is not None and _visit(base_target)):
                is_td = True
        typeddict_status[canonical_symbol] = is_td
        if is_td:
            ordered.append(cls_node)
        return is_td

    if not _visit(symbol_path):
        msg = f"{symbol_path!r} is not a TypedDict class"
        raise TypeError(msg)
    return ordered


def _extract_typeddict_chain_fields(symbol_path: str) -> list[tuple[str, str, str, str]]:
    """Resolve a TypedDict class and its TypedDict bases in declaration order and extract field metadata."""
    module_name, _, class_name = symbol_path.rpartition(".")
    if not module_name or not class_name:
        msg = f"Invalid TypedDict symbol path: {symbol_path!r}"
        raise ValueError(msg)

    if _resolve_symbol_class_node(symbol_path) is None:
        msg = f"Unable to resolve TypedDict symbol: {symbol_path!r}"
        raise ValueError(msg)

    collected: dict[str, tuple[str, str, str]] = {}
    for cls_node in _collect_typeddict_class_nodes(symbol_path):
        for key, type_cell, default_cell, desc_cell in _extract_ast_class_fields(cls_node):
            collected[key] = (type_cell, default_cell, desc_cell)
    return [(key, *values) for key, values in collected.items()]


def _extract_ast_class_fields(cls_node: ast.ClassDef) -> list[tuple[str, str, str, str]]:
    """Extract (field_name, type_cell, default_cell, description_cell) tuples from an AST ClassDef."""
    results: list[tuple[str, str, str, str]] = []
    statements = cls_node.body
    for idx, stmt in enumerate(statements):
        if not isinstance(stmt, ast.AnnAssign) or not isinstance(stmt.target, ast.Name):
            continue
        raw_doc = ""
        if idx + 1 < len(statements):
            next_stmt = statements[idx + 1]
            if (
                isinstance(next_stmt, ast.Expr)
                and isinstance(next_stmt.value, ast.Constant)
                and isinstance(next_stmt.value.value, str)
            ):
                raw_doc = next_stmt.value.value
        default_cell, desc_cell = _extract_default_and_description(raw_doc)
        results.append((stmt.target.id, f"`{_format_type_annotation(stmt.annotation)}`", default_cell, desc_cell))
    return results


def _format_type_annotation(annotation: ast.expr) -> str:
    """Format an AST type annotation with NotRequired unwrapped, forward refs unquoted, and unions as ' or '."""
    resolved = _ForwardRefUnquoter().visit(_unwrap_not_required(annotation))
    text = " ".join(ast.unparse(resolved).split())
    return re.sub(r"\s*\|\s*", " or ", text)


def _unwrap_not_required(node: ast.expr) -> ast.expr:
    """Unwrap NotRequired[...] or Required[...] AST subscript nodes and string forward refs."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        try:
            parsed = ast.parse(node.value, mode="eval").body
        except SyntaxError:
            return node
        return _unwrap_not_required(parsed)
    if isinstance(node, ast.Subscript) and ast.unparse(node.value) in _TYPE_WRAPPERS_TO_UNWRAP:
        return _unwrap_not_required(node.slice)
    return node


class _ForwardRefUnquoter(ast.NodeTransformer):
    """Replace quoted forward references nested inside annotations with their parsed expressions.

    ``Literal[...]`` arguments are left untouched because their string constants are
    values, not type references.
    """

    def visit_Subscript(self, node: ast.Subscript) -> ast.AST:
        if ast.unparse(node.value) in _LITERAL_TYPE_NAMES:
            return node
        return self.generic_visit(node)

    def visit_Constant(self, node: ast.Constant) -> ast.AST:
        if not isinstance(node.value, str):
            return node
        try:
            parsed = ast.parse(node.value, mode="eval").body
        except SyntaxError:
            return node
        return cast("ast.AST", self.visit(parsed))


def _extract_default_and_description(raw_doc: str) -> tuple[str, str]:
    """Extract a backticked default value and a full single-line description from a PEP 257 field docstring.

    Defaults written as code spans are trusted verbatim. Defaults written as plain
    prose are only accepted when they look like a Python literal or dotted name, so
    explanations such as "Default: the inverse of ``disable_di``" render as ``-``.
    """
    description = _flatten_docstring(raw_doc.replace("``", "`"))
    return _extract_default_value(description), _sanitize_table_cell(description)


def _flatten_docstring(raw_doc: str) -> str:
    """Flatten a multi-paragraph field docstring, including bullet lists, into one table-cell line.

    Paragraphs are joined with spaces. Bullet items (``-`` or ``*``) are joined with
    ``"; "`` after their lead-in sentence, and indented continuation lines are folded
    into the preceding item, so no part of the docstring is dropped. A paragraph that
    ends without sentence punctuation (typically a bullet list or a bare default value)
    gets a period before the next one.
    """
    rendered_paragraphs: list[str] = []
    for paragraph in re.split(r"\n\s*\n", inspect.cleandoc(raw_doc)):
        prose: list[str] = []
        items: list[str] = []
        for raw_line in paragraph.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            if line.startswith(("- ", "* ")):
                items.append(line[2:].strip())
            elif items:
                items[-1] = f"{items[-1]} {line}"
            else:
                prose.append(line)
        parts = [" ".join(prose)] if prose else []
        if items:
            parts.append("; ".join(item.rstrip(";") for item in items))
        if parts:
            rendered_paragraphs.append(" ".join(parts))
    return " ".join(
        f"{paragraph}."
        if idx < len(rendered_paragraphs) - 1 and not paragraph.endswith(_SENTENCE_ENDINGS)
        else paragraph
        for idx, paragraph in enumerate(rendered_paragraphs)
    )


def _extract_default_value(text: str) -> str:
    """Return the backticked default value cell for a field description, or ``-`` when absent."""
    for pattern in _CODE_SPAN_DEFAULT_PATTERNS:
        match = pattern.search(text)
        if match:
            candidate = match.group(1).strip()
            if candidate and len(candidate) <= _MAX_DEFAULT_VAL_LEN:
                return f"`{candidate}`"
    for pattern in _PROSE_DEFAULT_PATTERNS:
        match = pattern.search(text)
        if match:
            candidate = match.group(1).strip().rstrip(".")
            if _LITERAL_DEFAULT_PATTERN.match(candidate) and len(candidate) <= _MAX_DEFAULT_VAL_LEN:
                return f"`{candidate}`"
    return "-"


def _sanitize_table_cell(text: str) -> str:
    """Ensure a Markdown table cell contains no raw or escaped pipes inside or outside code spans."""
    parts = text.split("`")
    for idx in range(len(parts)):
        if idx % 2 == 1:
            parts[idx] = parts[idx].replace("\\|", " or ").replace("|", " or ")
        else:
            parts[idx] = parts[idx].replace("\\|", "/").replace("|", "/")
    return "`".join(parts).strip()


def _format_filter_signature(filter_cls: type[StatementFilter]) -> str:
    """Format a compact constructor signature for a StatementFilter subclass."""
    for base in filter_cls.__mro__:
        if base is object:
            break
        cls_node = _module_class_nodes(base.__module__).get(base.__name__)
        if cls_node is None:
            continue
        init_node = next(
            (item for item in cls_node.body if isinstance(item, ast.FunctionDef) and item.name == "__init__"), None
        )
        if init_node is not None:
            return _format_ast_init_signature(filter_cls.__name__, init_node)
    return f"{filter_cls.__name__}(...)"


def _format_ast_init_signature(class_name: str, init_node: ast.FunctionDef) -> str:
    """Format an AST __init__ FunctionDef into a clean constructor signature without '|'."""
    pos_args = init_node.args.args[1:]
    defaults = init_node.args.defaults
    default_offset = len(pos_args) - len(defaults)
    rendered_params: list[str] = []
    for idx, arg in enumerate(pos_args):
        param_str = arg.arg
        if idx >= default_offset:
            default_text = ast.unparse(defaults[idx - default_offset]).replace("|", " or ")
            param_str = f"{param_str}={default_text}"
        rendered_params.append(param_str)
    for kw_arg, kw_default in zip(init_node.args.kwonlyargs, init_node.args.kw_defaults, strict=True):
        if kw_default is not None:
            default_text = ast.unparse(kw_default).replace("|", " or ")
            rendered_params.append(f"{kw_arg.arg}={default_text}")
        else:
            rendered_params.append(kw_arg.arg)
    return f"{class_name}({', '.join(rendered_params)})"


def _extract_first_paragraph(docstring: str) -> str:
    """Extract the opening summary paragraph from a class docstring as a single table cell."""
    if not docstring:
        return "-"
    first_para = docstring.strip().split("\n\n", 1)[0]
    joined = " ".join(line.strip() for line in first_para.splitlines() if line.strip())
    return _sanitize_table_cell(joined)


def _extract_module_all(py_file: Path) -> list[str]:
    """Parse __all__ string literals from a Python source file."""
    if not py_file.is_file():
        return []
    tree = ast.parse(py_file.read_text(encoding="utf-8"), filename=str(py_file))
    for node in tree.body:
        if not isinstance(node, ast.Assign) or not isinstance(node.value, (ast.Tuple, ast.List)):
            continue
        if any(isinstance(target, ast.Name) and target.id == "__all__" for target in node.targets):
            return [
                elt.value for elt in node.value.elts if isinstance(elt, ast.Constant) and isinstance(elt.value, str)
            ]
    return []


def _extract_opening_imports(doc_text: str, adapter: str) -> set[str]:
    """Extract imported symbol names from the opening 'from sqlspec.adapters.<adapter> import ...' block."""
    match = re.search(
        rf"from sqlspec\.adapters\.{re.escape(adapter)} import \((.*?)\)|"
        rf"from sqlspec\.adapters\.{re.escape(adapter)} import ([^\n]+)",
        doc_text,
        re.DOTALL,
    )
    if not match:
        return set()
    raw = match.group(1) or match.group(2) or ""
    return {item.strip() for item in raw.replace("\n", ",").split(",") if item.strip()}


if __name__ == "__main__":
    raise SystemExit(main())
