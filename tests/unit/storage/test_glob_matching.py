"""Glob matching must agree across storage backends.

`local` and `fsspec` both delegate to an implementation with correct glob
semantics, so they are the reference. The obstore backend filters client-side
and must produce the same answers.
"""

from pathlib import Path

import pytest

from sqlspec.storage._paths import glob_to_regex
from sqlspec.storage.backends.local import LocalStore
from sqlspec.typing import FSSPEC_INSTALLED, OBSTORE_INSTALLED

LAYOUT = ("data/a.parquet", "data/sub/b.parquet", "data/x/y/z.parquet", "data_other/c.parquet", "top.parquet")

EXPECTED = {
    "data/*.parquet": ["data/a.parquet"],
    "data/**/*.parquet": ["data/a.parquet", "data/sub/b.parquet", "data/x/y/z.parquet"],
    "*.parquet": ["top.parquet"],
    "**/*.parquet": [
        "data/a.parquet",
        "data/sub/b.parquet",
        "data/x/y/z.parquet",
        "data_other/c.parquet",
        "top.parquet",
    ],
    "data/sub/b.parquet": ["data/sub/b.parquet"],
    "data/?.parquet": ["data/a.parquet"],
    "data/[ab].parquet": ["data/a.parquet"],
    "data_*/*.parquet": ["data_other/c.parquet"],
    "missing/*.parquet": [],
}


@pytest.fixture
def populated_root(tmp_path: Path) -> Path:
    for rel in LAYOUT:
        target = tmp_path / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(b"x")
    return tmp_path


def _normalize(root: Path, results: "list[str]") -> "list[str]":
    prefix = f"{root}/"
    return sorted(item.removeprefix(prefix) for item in results)


@pytest.mark.parametrize("pattern", sorted(EXPECTED))
def test_local_store_glob_is_the_reference(populated_root: Path, pattern: str) -> None:
    """Pin the reference semantics so a change to them is visible."""
    store = LocalStore(str(populated_root))

    assert _normalize(populated_root, store.glob_sync(pattern)) == EXPECTED[pattern]


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
@pytest.mark.parametrize("pattern", sorted(EXPECTED))
def test_obstore_glob_matches_the_reference(populated_root: Path, pattern: str) -> None:
    """The obstore backend must agree with the local backend."""
    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend(f"file://{populated_root}")

    assert _normalize(populated_root, store.glob_sync(pattern)) == EXPECTED[pattern]


@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
@pytest.mark.parametrize("pattern", sorted(EXPECTED))
def test_fsspec_glob_matches_the_reference(populated_root: Path, pattern: str) -> None:
    """The fsspec backend must agree with the local backend."""
    from sqlspec.storage.backends.fsspec import FSSpecBackend

    store = FSSpecBackend(f"file://{populated_root}")

    assert _normalize(populated_root, store.glob_sync(pattern)) == EXPECTED[pattern]


@pytest.mark.parametrize(
    ("pattern", "candidate", "matches"),
    [
        ("data/*.parquet", "data/a.parquet", True),
        ("data/*.parquet", "data/sub/b.parquet", False),
        ("*.parquet", "top.parquet", True),
        ("*.parquet", "data/a.parquet", False),
        ("data/**/*.parquet", "data/a.parquet", True),
        ("data/**/*.parquet", "data/sub/b.parquet", True),
        ("data/**/*.parquet", "data/x/y/z.parquet", True),
        ("data/**/*.parquet", "other/a.parquet", False),
        ("**/*.parquet", "top.parquet", True),
        ("**/*.parquet", "data/x/y/z.parquet", True),
        ("data/?.parquet", "data/a.parquet", True),
        ("data/?.parquet", "data/ab.parquet", False),
        ("data/[ab].parquet", "data/a.parquet", True),
        ("data/[ab].parquet", "data/c.parquet", False),
        ("data/[!ab].parquet", "data/c.parquet", True),
        ("data/[!ab].parquet", "data/a.parquet", False),
        ("data/sub/b.parquet", "data/sub/b.parquet", True),
        ("data/sub/b.parquet", "x/data/sub/b.parquet", False),
        ("data_*/*.parquet", "data_other/c.parquet", True),
        ("data_*/*.parquet", "data/a.parquet", False),
        ("a/**/b", "a/b", True),
        ("a/**/b", "a/x/b", True),
        ("a/**/b", "a/x/y/b", True),
        ("a/**/b", "a/b/c", False),
    ],
)
def test_glob_to_regex_semantics(pattern: str, candidate: str, matches: bool) -> None:
    """`*` and `?` stay within one segment; `**` spans zero or more segments; matching is anchored."""
    assert bool(glob_to_regex(pattern).match(candidate)) is matches


def test_glob_to_regex_escapes_regex_metacharacters() -> None:
    """A dot in a pattern is a literal dot, not a wildcard."""
    regex = glob_to_regex("data/a.parquet")

    assert regex.match("data/a.parquet")
    assert not regex.match("data/axparquet")


def test_glob_to_regex_empty_pattern_matches_nothing() -> None:
    """An empty pattern is not an error and selects nothing."""
    assert not glob_to_regex("").match("a.parquet")


MALFORMED_PATTERNS = [
    "[",
    "]",
    "[]",
    "[]]",
    "[!]",
    "[^]",
    "[a-",
    "[[]",
    "a[",
    "(",
    ")",
    "a(b)c",
    "a|b",
    "a+b",
    "^a",
    "a$",
    "\\",
    "a\\b",
    "{",
    "}",
    "{a,b}",
    "a{1,2}b",
    "***",
    "a/**",
    "**/",
    "/",
    "//",
    "a//b",
    ".",
    "..",
]


@pytest.mark.filterwarnings("error::FutureWarning")
@pytest.mark.parametrize("pattern", MALFORMED_PATTERNS)
def test_glob_to_regex_never_raises_on_malformed_input(pattern: str) -> None:
    """A caller-supplied pattern must not produce a regex error or warning.

    An empty or unterminated character class is treated as a literal bracket
    rather than emitted as ``[]``, which would swallow the trailing anchor.
    """
    regex = glob_to_regex(pattern)

    assert regex.pattern.endswith("\\Z")
    regex.match("data/a.parquet")


@pytest.mark.parametrize(
    ("pattern", "candidate", "matches"),
    [
        ("data/[a-z].parquet", "data/q.parquet", True),
        ("data/[a-z].parquet", "data/1.parquet", False),
        ("data/[!a-z].parquet", "data/1.parquet", True),
        ("data/[[].parquet", "data/[.parquet", True),
    ],
)
def test_glob_to_regex_character_classes(pattern: str, candidate: str, matches: bool) -> None:
    """Ranges, negation, and a literal bracket inside a class all behave."""
    assert bool(glob_to_regex(pattern).match(candidate)) is matches
