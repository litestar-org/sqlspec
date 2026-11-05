"""Generate and verify deterministic TypedDict configuration and StatementFilter Markdown tables."""

import argparse
import ast
import importlib
import inspect
import re
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Final

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
    "UNMIGRATED_ADAPTER_REFS",
    "check_adapter_reference_parity",
    "main",
    "render_filter_catalog_table",
    "render_typeddict_table",
    "sync_markdown_markers",
)

UNMIGRATED_ADAPTER_REFS: frozenset[str] = frozenset({
    "oracledb",
    "sqlite",
    "aiosqlite",
    "asyncmy",
    "pymysql",
    "mysqlconnector",
})

_TYPEDDICT_BASES: frozenset[str] = frozenset({"TypedDict", "typing.TypedDict", "typing_extensions.TypedDict"})
_MIN_QUOTED_LEN: Final[int] = 2
_MAX_DEFAULT_VAL_LEN: Final[int] = 48
_MARKER_BLOCK_PATTERN = re.compile(
    r"(?P<indent>[ \t]*)(<!-- GENERATED:START (?P<spec>config-table:[A-Za-z0-9_.,\s]+|filter-catalog) -->)\n"
    r"(?:(?P<body>.*?)\n)?"
    r"(?P=indent)(<!-- GENERATED:END (?P=spec) -->)",
    re.DOTALL,
)
_MARKER_START_PATTERN = re.compile(r"<!-- GENERATED:START (?P<spec>[^>]+?) -->")
_MARKER_END_PATTERN = re.compile(r"<!-- GENERATED:END (?P<spec>[^>]+?) -->")
_DEFAULT_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\(\s*defaults?\s+to\s+``([^`]+)``[^)]*\)", re.IGNORECASE),
    re.compile(r"\(\s*defaults?\s+to\s+`([^`]+)`[^)]*\)", re.IGNORECASE),
    re.compile(r"\(\s*defaults?\s+to\s+([^);]+?)\s*\)", re.IGNORECASE),
    re.compile(r"(?:^|\.\s+|\;\s+)Defaults?\s+to\s+``([^`]+)``\.?", re.IGNORECASE),
    re.compile(r"(?:^|\.\s+|\;\s+)Defaults?\s+to\s+`([^`]+)`\.?", re.IGNORECASE),
    re.compile(
        r"(?:^|\.\s+|\;\s+)Defaults?\s+to\s+(['\"][^'\"]+['\"]|True|False|None|\d+(?:\.\d+)?)\b\.?", re.IGNORECASE
    ),
    re.compile(r"(?:^|\.\s+|\;\s+)Default:\s*``([^`]+)``\.?", re.IGNORECASE),
    re.compile(r"(?:^|\.\s+|\;\s+)Default:\s*`([^`]+)`\.?", re.IGNORECASE),
    re.compile(r"(?:^|\.\s+|\;\s+)Default:\s*([^\n.]+)\.", re.IGNORECASE),
    re.compile(r"(?:^|\.\s+|\;\s+)Default:\s*([^\n.]+)$", re.IGNORECASE),
)
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


def render_typeddict_table(symbol_paths: Sequence[str]) -> str:
    """Render a deterministic 4-column Markdown table from one or more TypedDict schemas.

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
    for key, (type_cell, default_cell, desc_cell) in fields.items():
        rows.append(f"| `{key}` | {type_cell} | {default_cell} | {desc_cell} |")
    return "\n".join(rows)


def render_filter_catalog_table() -> str:
    """Render a deterministic 4-column Markdown table covering all 15 StatementFilter classes.

    Returns:
        Markdown table string with header
        ``| Filter Class | Constructor Signature | SQL Clause Generated | Description |``.
    """
    rows: list[str] = [
        "| Filter Class | Constructor Signature | SQL Clause Generated | Description |",
        "| --- | --- | --- | --- |",
    ]
    for filter_cls, sql_clause in _FILTER_CATALOG_SPECS:
        cls_name = filter_cls.__name__
        sig_text = _format_filter_signature(filter_cls)
        clean_clause = sql_clause.replace(" | ", " / ")
        summary = _extract_first_sentence(inspect.getdoc(filter_cls) or "")
        rows.append(f"| `{cls_name}` | `{sig_text}` | `{clean_clause}` | {summary} |")
    return "\n".join(rows)


def sync_markdown_markers(docs_dir: Path = Path("docs"), *, check: bool = False) -> list[str]:
    """Synchronize or verify GENERATED:START/END marker blocks across Markdown files.

    Args:
        docs_dir: Root documentation directory to scan.
        check: When True, report stale or malformed blocks without writing files.

    Returns:
        List of diagnostic messages for stale or malformed marker blocks.
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

        file_stale, updated = _sync_single_file_markers(md_path, original)
        if file_stale:
            issues.extend(file_stale)
            if not check:
                md_path.write_text(updated, encoding="utf-8")

    return issues


def check_adapter_reference_parity(
    docs_dir: Path = Path("docs"),
    adapters_dir: Path = Path("sqlspec/adapters"),
    unmigrated_allowlist: frozenset[str] = UNMIGRATED_ADAPTER_REFS,
) -> list[str]:
    """Verify that adapter Reference Markdown pages document all public classes exported by each adapter.

    Args:
        docs_dir: Documentation root directory containing ``reference/adapters/*.md``.
        adapters_dir: Source directory containing ``sqlspec/adapters/<adapter>/__init__.py``.
        unmigrated_allowlist: Set of adapter package names temporarily exempted until Chapter 5.

    Returns:
        List of parity violation strings.
    """
    errors: list[str] = []
    adapter_docs_dir = docs_dir / "reference" / "adapters"
    if not adapters_dir.is_dir() or not adapter_docs_dir.is_dir():
        return errors

    for init_file in sorted(adapters_dir.glob("*/__init__.py")):
        adapter = init_file.parent.name
        if adapter in unmigrated_allowlist:
            continue

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

    Args:
        argv: Optional sequence of CLI arguments.

    Returns:
        Exit code ``0`` when clean/updated, or ``1`` when stale or parity violations exist.
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

    marker_issues = sync_markdown_markers(args.docs_dir, check=args.check)
    parity_issues = [] if args.skip_adapter_parity else check_adapter_reference_parity(docs_dir=args.docs_dir)

    all_issues = [*marker_issues, *parity_issues]
    if all_issues:
        for issue in all_issues:
            sys.stderr.write(f"{issue}\n")
        if args.check:
            return 1
        if parity_issues:
            return 1

    sys.stdout.write("Config and filter tables check passed.\n")
    return 0


def _sync_single_file_markers(md_path: Path, original: str) -> tuple[list[str], str]:
    """Replace marker blocks in a single Markdown file text and report stale blocks."""
    file_stale: list[str] = []

    def _replace_block(match: re.Match[str]) -> str:
        indent = match.group("indent")
        spec = match.group("spec").strip()
        current_body = match.group("body") or ""
        raw_expected = _render_marker_spec(spec).strip()
        expected_body = "\n".join(f"{indent}{line}" if line else "" for line in raw_expected.splitlines())
        if current_body.strip() != expected_body.strip() or (indent and not current_body.startswith(indent)):
            file_stale.append(f"{md_path}: out-of-date marker block '{spec}'")
        return f"{indent}{match.group(2)}\n{expected_body}\n{indent}{match.group(5)}"

    updated = _MARKER_BLOCK_PATTERN.sub(_replace_block, original)
    return file_stale, updated


def _render_marker_spec(spec: str) -> str:
    """Dispatch a GENERATED marker specification to its table renderer."""
    if spec == "filter-catalog":
        return render_filter_catalog_table()
    if spec.startswith("config-table:"):
        raw_symbols = spec.removeprefix("config-table:").split(",")
        return render_typeddict_table([item.strip() for item in raw_symbols if item.strip()])
    msg = f"Unsupported GENERATED marker specification: {spec!r}"
    raise ValueError(msg)


def _collect_typeddict_ancestors(cls_obj: type) -> list[type]:
    """Return TypedDict ancestor classes in base-first declaration order."""
    visited: list[type] = []
    seen: set[type] = set()

    def _visit(current: object) -> None:
        if not isinstance(current, type) or current in seen:
            return
        if current is dict or current is object or current.__name__ == "TypedDict" or current.__module__ == "builtins":
            return
        for orig_base in getattr(current, "__orig_bases__", ()):
            _visit(orig_base)
        if current not in seen:
            seen.add(current)
            visited.append(current)

    _visit(cls_obj)
    return visited or [cls_obj]


def _extract_typeddict_chain_fields(symbol_path: str) -> list[tuple[str, str, str, str]]:
    """Resolve a TypedDict class and its TypedDict bases in declaration order and extract field metadata."""
    module_name, _, class_name = symbol_path.rpartition(".")
    if not module_name or not class_name:
        msg = f"Invalid TypedDict symbol path: {symbol_path!r}"
        raise ValueError(msg)

    module = importlib.import_module(module_name)
    cls_obj = getattr(module, class_name)
    mro_classes = _collect_typeddict_ancestors(cls_obj)

    collected: dict[str, tuple[str, str, str]] = {}
    for base_cls in mro_classes:
        base_mod = importlib.import_module(base_cls.__module__)
        source_file = getattr(base_mod, "__file__", None)
        if not source_file:
            continue
        tree = ast.parse(Path(source_file).read_text(encoding="utf-8"), filename=source_file)
        class_map = {node.name: node for node in tree.body if isinstance(node, ast.ClassDef)}
        cls_node = class_map.get(base_cls.__name__)
        if cls_node is None:
            continue
        for ancestor_node in _collect_ast_base_nodes(cls_node, class_map):
            for key, type_cell, default_cell, desc_cell in _extract_ast_class_fields(ancestor_node):
                collected[key] = (type_cell, default_cell, desc_cell)

    return [(k, v[0], v[1], v[2]) for k, v in collected.items()]


def _collect_ast_base_nodes(cls_node: ast.ClassDef, class_map: dict[str, ast.ClassDef]) -> list[ast.ClassDef]:
    """Collect intra-module AST base ClassDef nodes in base-first order."""
    ordered: list[ast.ClassDef] = []
    seen: set[str] = set()

    def _visit_node(node: ast.ClassDef) -> None:
        if node.name in seen:
            return
        for base_expr in node.bases:
            base_name = ast.unparse(base_expr)
            if base_name in class_map and base_name not in _TYPEDDICT_BASES:
                _visit_node(class_map[base_name])
        if node.name not in seen:
            seen.add(node.name)
            ordered.append(node)

    _visit_node(cls_node)
    return ordered


def _extract_ast_class_fields(cls_node: ast.ClassDef) -> list[tuple[str, str, str, str]]:
    """Extract (field_name, type_cell, default_cell, description_cell) tuples from an AST ClassDef."""
    results: list[tuple[str, str, str, str]] = []
    statements = cls_node.body
    for idx, stmt in enumerate(statements):
        if not isinstance(stmt, ast.AnnAssign) or not isinstance(stmt.target, ast.Name):
            continue
        field_name = stmt.target.id
        type_str = _format_type_annotation(stmt.annotation)
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
        results.append((field_name, f"`{type_str}`", default_cell, desc_cell))
    return results


def _format_type_annotation(annotation: ast.expr) -> str:
    """Format an AST type annotation with NotRequired unwrapped and union pipes replaced by ' or '."""
    unwrapped = _unwrap_not_required(annotation)
    text = ast.unparse(unwrapped)
    text = _strip_outer_quotes(text)
    if text.startswith(("NotRequired[", "Required[", "typing.NotRequired[", "typing_extensions.NotRequired[")):
        inner = text[text.find("[") + 1 : -1]
        text = _strip_outer_quotes(inner)
    text = re.sub(r'(?<![A-Za-z0-9_])"([^"]+)"(?![A-Za-z0-9_])', r"\1", text)
    text = " ".join(text.split())
    return re.sub(r"\s*\|\s*", " or ", text)


def _unwrap_not_required(node: ast.expr) -> ast.expr:
    """Unwrap NotRequired[...] or Required[...] AST subscript nodes and string forward refs."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        try:
            parsed = ast.parse(node.value, mode="eval").body
            return _unwrap_not_required(parsed)
        except SyntaxError:
            return node
    if isinstance(node, ast.Subscript):
        wrapper_name = ast.unparse(node.value)
        if wrapper_name in {
            "NotRequired",
            "Required",
            "typing.NotRequired",
            "typing.Required",
            "typing_extensions.NotRequired",
            "typing_extensions.Required",
        }:
            return _unwrap_not_required(node.slice)
    return node


def _strip_outer_quotes(text: str) -> str:
    """Remove matching outer single or double quotes when wrapping a whole type expression."""
    stripped = text.strip()
    if len(stripped) >= _MIN_QUOTED_LEN and stripped[0] == stripped[-1] and stripped[0] in {"'", '"'}:
        inner = stripped[1:-1]
        if stripped[0] not in inner:
            return inner
    return stripped


def _extract_default_and_description(raw_doc: str) -> tuple[str, str]:
    """Extract a backticked default value and single-line description from a PEP 257 field docstring."""
    cleaned = raw_doc.strip().replace("``", "`")
    lines = [line.strip() for line in cleaned.splitlines() if line.strip()]
    primary_lines: list[str] = []
    for line in lines:
        if line.startswith(("- ", "* ", "Note:")):
            break
        primary_lines.append(line)
    single_line = " ".join(primary_lines or lines)

    default_val = "-"
    for pattern in _DEFAULT_PATTERNS:
        match = pattern.search(single_line)
        if match:
            candidate = match.group(1).strip().rstrip(".")
            candidate = candidate.strip("`").strip()
            if candidate and len(candidate) <= _MAX_DEFAULT_VAL_LEN:
                default_val = f"`{candidate}`"
                break

    desc = _sanitize_table_cell(single_line)
    return default_val, desc


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
    source_file = inspect.getsourcefile(filter_cls)
    if source_file:
        tree = ast.parse(Path(source_file).read_text(encoding="utf-8"), filename=source_file)
        class_map = {node.name: node for node in tree.body if isinstance(node, ast.ClassDef)}
        for base in filter_cls.__mro__:
            cls_node = class_map.get(base.__name__)
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
            default_node = defaults[idx - default_offset]
            default_text = ast.unparse(default_node).replace("|", " or ")
            param_str = f"{param_str}={default_text}"
        rendered_params.append(param_str)
    for kw_arg, kw_default in zip(init_node.args.kwonlyargs, init_node.args.kw_defaults, strict=False):
        if kw_default is not None:
            default_text = ast.unparse(kw_default).replace("|", " or ")
            rendered_params.append(f"{kw_arg.arg}={default_text}")
        else:
            rendered_params.append(kw_arg.arg)
    return f"{class_name}({', '.join(rendered_params)})"


def _extract_first_sentence(docstring: str) -> str:
    """Extract the opening summary paragraph from a class docstring."""
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
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if (
                    isinstance(target, ast.Name)
                    and target.id == "__all__"
                    and isinstance(node.value, (ast.Tuple, ast.List))
                ):
                    return [
                        elt.value
                        for elt in node.value.elts
                        if isinstance(elt, ast.Constant) and isinstance(elt.value, str)
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
