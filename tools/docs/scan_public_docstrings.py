"""AST scanner for public docstring compiler leaks, @overload docstrings, and TypedDict field docstrings."""

import argparse
import ast
import re
import sys
from collections.abc import Sequence
from pathlib import Path

import msgspec

__all__ = (
    "DocstringLeakViolation",
    "OverloadDocstringViolation",
    "TypedDictDocstringViolation",
    "main",
    "scan_overload_docstrings",
    "scan_public_docstring_leaks",
    "scan_undocumented_typeddicts",
)

_LEAK_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("mypyc", re.compile(r"\bmypyc\b", re.IGNORECASE)),
    ("segfault", re.compile(r"\bsegfaults?\b", re.IGNORECASE)),
    ("segmentation fault", re.compile(r"\bsegmentation\s+faults?\b", re.IGNORECASE)),
    ("ABI boundary", re.compile(r"\babi\s+boundar(?:y|ies)\b", re.IGNORECASE)),
    ("Private/internal methods", re.compile(r"private/internal\s+methods", re.IGNORECASE)),
    ("cross-trait attribute", re.compile(r"cross-trait\s+attribute", re.IGNORECASE)),
)
_ATTRIBUTES_HEADER_PATTERN = re.compile(r"^\s*Attributes:\s*$", re.MULTILINE)
_OVERLOAD_DECORATORS: frozenset[str] = frozenset({"overload", "typing.overload", "typing_extensions.overload"})
_TYPEDDICT_BASES: frozenset[str] = frozenset({"TypedDict", "typing.TypedDict", "typing_extensions.TypedDict"})


class DocstringLeakViolation(msgspec.Struct, frozen=True, gc=False):
    """Public class, function, or method docstring containing internal compiler or layout notes."""

    path: Path
    line: int
    symbol: str
    matched_patterns: tuple[str, ...]

    def format(self) -> str:
        """Render a concise diagnostic line."""
        joined = ", ".join(repr(item) for item in self.matched_patterns)
        return f"{self.path}:{self.line}: {self.symbol} leaks internal pattern(s) {joined}"


class OverloadDocstringViolation(msgspec.Struct, frozen=True, gc=False):
    """Overload stub carrying a docstring instead of leaving documentation on the implementation."""

    path: Path
    line: int
    symbol: str

    def format(self) -> str:
        """Render a concise diagnostic line."""
        return f"{self.path}:{self.line}: @overload {self.symbol} must not define a docstring"


class TypedDictDocstringViolation(msgspec.Struct, frozen=True, gc=False):
    """Public or inherited TypedDict missing inline PEP 257 field docstrings or duplicating class Attributes."""

    path: Path
    line: int
    symbol: str
    missing_fields: tuple[str, ...]
    has_duplicate_class_attributes: bool

    def format(self) -> str:
        """Render a concise diagnostic line."""
        details: list[str] = []
        if self.missing_fields:
            joined = ", ".join(self.missing_fields)
            details.append(f"missing PEP 257 field docstrings ({len(self.missing_fields)}): {joined}")
        if self.has_duplicate_class_attributes:
            details.append("duplicate class-level Attributes/field list in class docstring")
        return f"{self.path}:{self.line}: {self.symbol}: {'; '.join(details)}"


def scan_public_docstring_leaks(paths: Sequence[Path]) -> list[DocstringLeakViolation]:
    """Scan Python files for public symbol docstrings containing compiler or internal-layout leaks."""
    violations: list[DocstringLeakViolation] = []
    for file_path in _iter_python_files(paths):
        if file_path.name == "_typing.py":
            continue
        violations.extend(_scan_module_leaks(file_path))
    return violations


def scan_overload_docstrings(paths: Sequence[Path]) -> list[OverloadDocstringViolation]:
    """Scan Python files for @overload-decorated functions or methods that carry docstrings."""
    violations: list[OverloadDocstringViolation] = []
    for file_path in _iter_python_files(paths):
        violations.extend(_scan_module_overloads(file_path))
    return violations


def scan_undocumented_typeddicts(paths: Sequence[Path]) -> list[TypedDictDocstringViolation]:
    """Scan Python files for public TypedDict classes missing PEP 257 field docstrings or duplicating Attributes."""
    violations: list[TypedDictDocstringViolation] = []
    for file_path in _iter_python_files(paths):
        violations.extend(_scan_module_typeddicts(file_path))
    return violations


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entrypoint for public docstring, overload, and TypedDict hygiene checks."""
    parser = argparse.ArgumentParser(
        description="Verify public docstrings for compiler leaks, @overload docstrings, and TypedDict field coverage."
    )
    parser.add_argument(
        "--check-leaks",
        action="store_true",
        help="Fail when public classes, functions, or methods leak compiler/ABI/layout notes.",
    )
    parser.add_argument("--check-overloads", action="store_true", help="Fail when @overload stubs define docstrings.")
    parser.add_argument(
        "--check-typeddicts",
        action="store_true",
        help="Fail when public TypedDict fields lack PEP 257 inline docstrings or duplicate class Attributes.",
    )
    parser.add_argument(
        "paths",
        nargs="*",
        type=Path,
        default=[Path("sqlspec")],
        help="Target files or directories to scan (defaults to sqlspec).",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)

    run_all = not (args.check_leaks or args.check_overloads or args.check_typeddicts)
    messages: list[str] = []

    if args.check_leaks or run_all:
        messages.extend(item.format() for item in scan_public_docstring_leaks(args.paths))
    if args.check_overloads or run_all:
        messages.extend(item.format() for item in scan_overload_docstrings(args.paths))
    if args.check_typeddicts or run_all:
        messages.extend(item.format() for item in scan_undocumented_typeddicts(args.paths))

    if messages:
        sys.stderr.write(f"Docstring hygiene check failed ({len(messages)} violation(s)):\n")
        for msg in messages:
            sys.stderr.write(f"  - {msg}\n")
        return 1

    sys.stdout.write("Docstring hygiene check passed.\n")
    return 0


def _iter_python_files(paths: Sequence[Path]) -> list[Path]:
    """Expand file and directory paths into a sorted deduplicated list of .py files."""
    collected: set[Path] = set()
    for target in paths:
        if target.is_file() and target.suffix == ".py":
            collected.add(target)
        elif target.is_dir():
            collected.update(candidate for candidate in target.rglob("*.py") if candidate.is_file())
    return sorted(collected)


def _match_leak_patterns(docstring: str | None) -> tuple[str, ...]:
    """Return all matched compiler/layout leak labels in docstring."""
    if not docstring:
        return ()
    return tuple(label for label, pattern in _LEAK_PATTERNS if pattern.search(docstring))


def _scan_module_leaks(file_path: Path) -> list[DocstringLeakViolation]:
    """Inspect public top-level classes, functions, and public methods in file_path for docstring leaks."""
    tree = ast.parse(file_path.read_text(encoding="utf-8"), filename=str(file_path))
    violations: list[DocstringLeakViolation] = []

    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if node.name.startswith("_"):
                continue
            matched = _match_leak_patterns(ast.get_docstring(node))
            if matched:
                violations.append(
                    DocstringLeakViolation(path=file_path, line=node.lineno, symbol=node.name, matched_patterns=matched)
                )
        elif isinstance(node, ast.ClassDef) and not node.name.startswith("_"):
            matched = _match_leak_patterns(ast.get_docstring(node))
            if matched:
                violations.append(
                    DocstringLeakViolation(path=file_path, line=node.lineno, symbol=node.name, matched_patterns=matched)
                )
            violations.extend(_scan_class_method_leaks(file_path, node))

    return violations


def _scan_class_method_leaks(file_path: Path, class_node: ast.ClassDef) -> list[DocstringLeakViolation]:
    """Inspect public methods of class_node for docstring leaks."""
    violations: list[DocstringLeakViolation] = []
    for item in class_node.body:
        if not isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)) or item.name.startswith("_"):
            continue
        matched = _match_leak_patterns(ast.get_docstring(item))
        if matched:
            violations.append(
                DocstringLeakViolation(
                    path=file_path, line=item.lineno, symbol=f"{class_node.name}.{item.name}", matched_patterns=matched
                )
            )
    return violations


def _is_overload_decorated(node: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
    """Return whether node carries an @overload decorator."""
    return any(ast.unparse(decorator) in _OVERLOAD_DECORATORS for decorator in node.decorator_list)


def _scan_module_overloads(file_path: Path) -> list[OverloadDocstringViolation]:
    """Inspect top-level functions and class methods in file_path for documented @overload stubs."""
    tree = ast.parse(file_path.read_text(encoding="utf-8"), filename=str(file_path))
    violations: list[OverloadDocstringViolation] = []

    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if _is_overload_decorated(node) and ast.get_docstring(node):
                violations.append(OverloadDocstringViolation(path=file_path, line=node.lineno, symbol=node.name))
        elif isinstance(node, ast.ClassDef):
            violations.extend(
                OverloadDocstringViolation(path=file_path, line=item.lineno, symbol=f"{node.name}.{item.name}")
                for item in node.body
                if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef))
                and _is_overload_decorated(item)
                and ast.get_docstring(item)
            )
    return violations


def _resolve_effective_typeddicts(classes: dict[str, ast.ClassDef]) -> list[ast.ClassDef]:
    """Resolve public TypedDict classes and any intra-module private TypedDict base classes they inherit."""
    all_typeddicts: set[str] = set()
    changed = True
    while changed:
        changed = False
        for name, cls_node in classes.items():
            if name in all_typeddicts:
                continue
            if any(
                ast.unparse(base) in _TYPEDDICT_BASES or ast.unparse(base) in all_typeddicts for base in cls_node.bases
            ):
                all_typeddicts.add(name)
                changed = True

    effective: set[str] = {name for name in all_typeddicts if not name.startswith("_")}
    changed = True
    while changed:
        changed = False
        for name in tuple(effective):
            cls_node = classes[name]
            for base in cls_node.bases:
                base_name = ast.unparse(base)
                if base_name in all_typeddicts and base_name not in effective:
                    effective.add(base_name)
                    changed = True

    return [classes[name] for name in sorted(effective, key=lambda key: classes[key].lineno)]


def _extract_typeddict_field_status(cls_node: ast.ClassDef) -> tuple[list[str], tuple[str, ...]]:
    """Return all annotated field names and the subset missing inline PEP 257 string docstrings."""
    statements = cls_node.body
    all_fields: list[str] = []
    missing_fields: list[str] = []

    for idx, stmt in enumerate(statements):
        if not isinstance(stmt, ast.AnnAssign) or not isinstance(stmt.target, ast.Name):
            continue
        field_name = stmt.target.id
        all_fields.append(field_name)
        next_stmt = statements[idx + 1] if idx + 1 < len(statements) else None
        has_inline_doc = (
            isinstance(next_stmt, ast.Expr)
            and isinstance(next_stmt.value, ast.Constant)
            and isinstance(next_stmt.value.value, str)
            and bool(next_stmt.value.value.strip())
        )
        if not has_inline_doc:
            missing_fields.append(field_name)

    return all_fields, tuple(missing_fields)


def _has_duplicate_class_attributes(docstring: str | None, field_names: Sequence[str]) -> bool:
    """Return whether docstring contains a redundant Attributes: header or manual field_name: list."""
    if not docstring:
        return False
    if _ATTRIBUTES_HEADER_PATTERN.search(docstring):
        return True
    return any(
        re.search(rf"^\s*{re.escape(field_name)}\s*(?:\([^)\n]*\))?\s*:", docstring, re.MULTILINE)
        for field_name in field_names
    )


def _scan_module_typeddicts(file_path: Path) -> list[TypedDictDocstringViolation]:
    """Inspect public and inherited TypedDict schemas in file_path for missing or duplicated field docs."""
    tree = ast.parse(file_path.read_text(encoding="utf-8"), filename=str(file_path))
    classes = {node.name: node for node in tree.body if isinstance(node, ast.ClassDef)}
    if not classes:
        return []

    violations: list[TypedDictDocstringViolation] = []
    for cls_node in _resolve_effective_typeddicts(classes):
        field_names, missing_fields = _extract_typeddict_field_status(cls_node)
        if not field_names:
            continue
        has_duplicate = _has_duplicate_class_attributes(ast.get_docstring(cls_node), field_names)
        if missing_fields or has_duplicate:
            violations.append(
                TypedDictDocstringViolation(
                    path=file_path,
                    line=cls_node.lineno,
                    symbol=cls_node.name,
                    missing_fields=missing_fields,
                    has_duplicate_class_attributes=has_duplicate,
                )
            )
    return violations


if __name__ == "__main__":
    raise SystemExit(main())
