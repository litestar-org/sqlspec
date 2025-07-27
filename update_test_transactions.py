#!/usr/bin/env python3
"""Script to update test fixtures to use explicit transaction management instead of autocommit."""

import re
from pathlib import Path


def update_test_file(file_path: Path) -> bool:
    """Update a test file to use explicit transactions."""
    content = file_path.read_text()
    original_content = content
    updated = False

    # Pattern 1: Remove autocommit from config
    autocommit_pattern = r'(["\']\s*autocommit["\']\s*:\s*True\s*,?)'
    if re.search(autocommit_pattern, content):
        content = re.sub(autocommit_pattern, "", content)
        # Clean up extra commas
        content = re.sub(r",\s*}", "}", content)
        content = re.sub(r",\s*\)", ")", content)
        updated = True

    # Pattern 2: Add transaction handling to session fixtures
    # Look for yield session patterns and wrap with transaction
    fixture_pattern = r"(yield\s+session)"
    if re.search(fixture_pattern, content) and "session.begin()" not in content:
        # Add begin before yield
        begin_replacement = """# Start transaction for test isolation
            session.begin()
            yield session"""
        content = re.sub(fixture_pattern, begin_replacement, content)

        # Add rollback after yield in finally block
        finally_pattern = r"(finally:\s*\n)"
        if re.search(finally_pattern, content):
            rollback_addition = """finally:
        # Rollback any uncommitted changes
        try:
            if hasattr(session, 'rollback'):
                session.rollback()
        except Exception:
            pass
        """
            content = re.sub(finally_pattern, rollback_addition, content, count=1)
        updated = True

    # Pattern 3: Update individual test operations that modify data
    # Look for INSERT/UPDATE/DELETE operations
    modify_patterns = [
        (r'(session\.execute\s*\(\s*["\']INSERT\s+INTO[^"\']*["\']\s*,)', "INSERT"),
        (r'(session\.execute\s*\(\s*["\']UPDATE\s+[^"\']*["\']\s*,)', "UPDATE"),
        (r'(session\.execute\s*\(\s*["\']DELETE\s+FROM[^"\']*["\']\s*,)', "DELETE"),
    ]

    for pattern, _op_type in modify_patterns:
        matches = list(re.finditer(pattern, content, re.IGNORECASE))
        if matches:
            pass
            # This would need more complex logic to add commits after operations
            # For now, we'll rely on the fixture-level transaction handling

    if updated and content != original_content:
        file_path.write_text(content)
        return True
    return False

def main() -> None:
    """Update all test files to use explicit transactions."""
    test_dir = Path("tests/integration/test_adapters")

    # Find all test driver files
    test_files = list(test_dir.rglob("test_driver*.py"))
    test_files.extend(test_dir.rglob("test_connection.py"))
    test_files.extend(test_dir.rglob("conftest.py"))


    updated_count = 0
    for test_file in sorted(test_files):
        if update_test_file(test_file):
            updated_count += 1


    # Create a sample transaction fixture pattern


if __name__ == "__main__":
    main()
