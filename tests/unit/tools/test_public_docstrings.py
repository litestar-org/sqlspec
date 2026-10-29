"""Behavioral unit tests for AST public docstring leak, overload, and TypedDict scanners."""

from pathlib import Path

import pytest
from tools.docs.scan_public_docstrings import main as scan_main
from tools.docs.scan_public_docstrings import (
    scan_overload_docstrings,
    scan_public_docstring_leaks,
    scan_undocumented_typeddicts,
)

__all__ = (
    "test_scan_overload_docstrings_flags_documented_overloads",
    "test_scan_public_docstring_leaks_detects_compiler_and_layout_leaks",
    "test_scan_public_docstrings_cli_main",
    "test_scan_undocumented_typeddicts_resolves_inheritance_and_duplicate_blocks",
    "test_sqlspec_public_docstrings_have_zero_leaks_and_zero_overload_docs",
)


def test_scan_public_docstring_leaks_detects_compiler_and_layout_leaks(tmp_path: Path) -> None:
    module_file = tmp_path / "sample.py"
    module_file.write_text(
        '"""Module docstring mentioning mypyc compilation and ABI boundary notes."""\n\n'
        "class CleanDriver:\n"
        '    """Execute SQL statements synchronously."""\n\n'
        "    def run(self) -> None:\n"
        '        """Run a statement."""\n\n'
        "    def _internal(self) -> None:\n"
        '        """Internal helper avoiding mypyc segfault."""\n\n'
        "class LeakyBase:\n"
        '    """Driver base avoiding cross-trait attribute access and Private/internal methods."""\n\n'
        "    async def handle_errors(self) -> None:\n"
        '        """Defer raising to avoid ABI boundary violations."""\n\n'
        "def leaky_helper() -> None:\n"
        '    """Keep signature introspectable under mypyc."""\n\n'
        "class CrashPool:\n"
        '    """Prevent segmentation fault crashes."""\n\n'
        "def segfault_fn() -> None:\n"
        '    """Avoid a segfault during lookup."""\n\n'
        "class _PrivateHelper:\n"
        '    """Private class mentioning mypyc ABI boundary."""\n',
        encoding="utf-8",
    )

    private_typing = tmp_path / "_typing.py"
    private_typing.write_text(
        'class SampleSessionContext:\n    """Excluded from mypyc compilation to avoid ABI boundary issues."""\n',
        encoding="utf-8",
    )

    violations = scan_public_docstring_leaks([tmp_path])
    messages = [item.format() for item in violations]

    assert len(violations) == 5
    assert any(
        "LeakyBase" in msg and "cross-trait attribute" in msg.lower() and "private/internal methods" in msg.lower()
        for msg in messages
    )
    assert any("LeakyBase.handle_errors" in msg and "abi boundary" in msg.lower() for msg in messages)
    assert any("leaky_helper" in msg and "mypyc" in msg.lower() for msg in messages)
    assert any("CrashPool" in msg and "segmentation fault" in msg.lower() for msg in messages)
    assert any("segfault_fn" in msg and "segfault" in msg.lower() for msg in messages)
    assert not any("SampleSessionContext" in msg for msg in messages)
    assert not any("_PrivateHelper" in msg for msg in messages)
    assert not any("_internal" in msg for msg in messages)


def test_scan_overload_docstrings_flags_documented_overloads(tmp_path: Path) -> None:
    module_file = tmp_path / "overloads.py"
    module_file.write_text(
        "import typing\n"
        "from typing import overload\n\n"
        "@overload\n"
        "def fetch(x: int) -> int: ...\n\n"
        "@overload\n"
        "def fetch(x: str) -> str:\n"
        '    """Documented overload stub."""\n'
        "    ...\n\n"
        "def fetch(x: int | str) -> int | str:\n"
        '    """Fetch value."""\n'
        "    return x\n\n"
        "class Client:\n"
        "    @typing.overload\n"
        "    async def query(self, sql: str) -> list[dict[str, object]]:\n"
        '        """Documented method overload."""\n'
        "        ...\n\n"
        "    async def query(self, sql: str) -> list[dict[str, object]]:\n"
        '        """Execute query."""\n'
        "        return []\n",
        encoding="utf-8",
    )

    violations = scan_overload_docstrings([module_file])
    symbols = {item.symbol for item in violations}
    assert symbols == {"fetch", "Client.query"}


def test_scan_undocumented_typeddicts_resolves_inheritance_and_duplicate_blocks(tmp_path: Path) -> None:
    module_file = tmp_path / "configs.py"
    module_file.write_text(
        "from typing import NotRequired, TypedDict\n\n"
        "class CleanConfig(TypedDict):\n"
        '    """Connection configuration."""\n\n'
        "    host: str\n"
        '    """Database hostname."""\n'
        "    port: NotRequired[int]\n"
        '    """Database TCP port."""\n\n'
        "class CleanPoolConfig(CleanConfig):\n"
        '    """Pool configuration."""\n\n'
        "    min_size: NotRequired[int]\n"
        '    """Minimum pool connections."""\n\n'
        "class _PrivateUnused(TypedDict):\n"
        "    secret: str\n\n"
        "class _SharedBaseParams(TypedDict):\n"
        '    """Shared connection base."""\n\n'
        "    dsn: str\n\n"
        "class PublicDerivedParams(_SharedBaseParams):\n"
        '    """Derived connection parameters."""\n\n'
        "    timeout: NotRequired[float]\n\n"
        "class DuplicateAttributesConfig(TypedDict):\n"
        '    """Driver features.\n\n'
        "    Attributes:\n"
        "        enable_cache: Enable statement caching.\n"
        '    """\n\n'
        "    enable_cache: NotRequired[bool]\n"
        '    """Enable statement caching."""\n\n'
        "class DuplicateFieldListConfig(TypedDict):\n"
        '    """Pool parameters.\n\n'
        "    max_idle: Maximum idle time.\n"
        '    """\n\n'
        "    max_idle: NotRequired[int]\n"
        '    """Maximum idle time in seconds."""\n',
        encoding="utf-8",
    )

    violations = scan_undocumented_typeddicts([module_file])
    by_symbol = {item.symbol: item for item in violations}

    assert "CleanConfig" not in by_symbol
    assert "CleanPoolConfig" not in by_symbol
    assert "_PrivateUnused" not in by_symbol
    assert "_SharedBaseParams" in by_symbol
    assert by_symbol["_SharedBaseParams"].missing_fields == ("dsn",)
    assert "PublicDerivedParams" in by_symbol
    assert by_symbol["PublicDerivedParams"].missing_fields == ("timeout",)
    assert "DuplicateAttributesConfig" in by_symbol
    assert by_symbol["DuplicateAttributesConfig"].has_duplicate_class_attributes is True
    assert "DuplicateFieldListConfig" in by_symbol
    assert by_symbol["DuplicateFieldListConfig"].has_duplicate_class_attributes is True


def test_scan_public_docstrings_cli_main(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    clean_file = tmp_path / "clean.py"
    clean_file.write_text(
        "from typing import TypedDict\n\n"
        "class ItemConfig(TypedDict):\n"
        '    """Item configuration."""\n\n'
        "    name: str\n"
        '    """Item identifier."""\n',
        encoding="utf-8",
    )

    exit_ok = scan_main(["--check-leaks", "--check-overloads", "--check-typeddicts", str(clean_file)])
    assert exit_ok == 0

    bad_file = tmp_path / "bad.py"
    bad_file.write_text('def helper() -> None:\n    """Avoid mypyc ABI boundary failure."""\n', encoding="utf-8")
    exit_fail = scan_main(["--check-leaks", str(bad_file)])
    captured = capsys.readouterr()
    assert exit_fail == 1
    assert "helper" in captured.err


def test_sqlspec_public_docstrings_have_zero_leaks_and_zero_overload_docs() -> None:
    sqlspec_root = Path(__file__).resolve().parents[3] / "sqlspec"
    assert scan_public_docstring_leaks([sqlspec_root]) == []
    assert scan_overload_docstrings([sqlspec_root]) == []
