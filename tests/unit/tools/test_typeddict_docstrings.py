"""Repository-wide regression test enforcing PEP 257 field docstrings on public TypedDict schemas."""

from pathlib import Path

from tools.docs.scan_public_docstrings import scan_undocumented_typeddicts

__all__ = ("test_sqlspec_public_typeddicts_have_complete_field_docstrings",)


def test_sqlspec_public_typeddicts_have_complete_field_docstrings() -> None:
    sqlspec_root = Path(__file__).resolve().parents[3] / "sqlspec"
    violations = scan_undocumented_typeddicts([sqlspec_root])
    assert violations == [], "\n".join(item.format() for item in violations)
