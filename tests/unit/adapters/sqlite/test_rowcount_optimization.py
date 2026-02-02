from unittest.mock import Mock
from sqlspec.adapters.sqlite.core import resolve_rowcount

def test_resolve_rowcount_fast_path() -> None:
    # Cursor with rowcount
    cursor = Mock()
    cursor.rowcount = 10
    
    # Should get 10
    assert resolve_rowcount(cursor) == 10

def test_resolve_rowcount_missing_attr() -> None:
    # Cursor without rowcount
    cursor = Mock(spec=[]) # No attributes
    
    # Should not crash, return 0
    assert resolve_rowcount(cursor) == 0

def test_resolve_rowcount_none_value() -> None:
    cursor = Mock()
    cursor.rowcount = None
    assert resolve_rowcount(cursor) == 0

def test_resolve_rowcount_negative() -> None:
    cursor = Mock()
    cursor.rowcount = -1
    assert resolve_rowcount(cursor) == 0
