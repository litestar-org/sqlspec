#!/usr/bin/env python3
"""Simple SQL File Loader Usage Example.

This example shows the basic usage of the SQL file loader with a real SQL file.
"""

from pathlib import Path

from sqlspec.loader import SQLFileLoader

__all__ = ("main",)


def main() -> None:
    """Run the simple example."""
    # Initialize the loader
    loader = SQLFileLoader()

    # Load the SQL file containing user queries
    queries_dir = Path(__file__).parent / "queries"
    loader.load_sql(queries_dir / "users.sql")

    # List all available queries
    print("Available queries:")
    for query in loader.list_queries():
        print(f"  - {query}")

    print("\n" + "=" * 50 + "\n")

    # Get and display a specific query
    print("Getting 'get_user_by_id' query:")
    user_query = loader.get_sql("get_user_by_id", user_id=123)
    print(f"SQL: {user_query._sql}")
    print(f"Parameters: {user_query.parameters}")

    print("\n" + "=" * 50 + "\n")

    # Add a custom query at runtime
    loader.add_named_sql(
        "custom_search",
        """
        SELECT * FROM users
        WHERE username LIKE :search_pattern
        OR email LIKE :search_pattern
        ORDER BY username
    """,
    )

    # Use the custom query
    print("Using custom search query:")
    search_sql = loader.get_sql("custom_search", search_pattern="%john%")
    print(f"SQL: {search_sql._sql}")
    print(f"Parameters: {search_sql.parameters}")

    print("\n" + "=" * 50 + "\n")

    # Show file information
    print("File information:")
    for file_path in loader.list_files():
        file_info = loader.get_file(file_path)
        if file_info:
            print(f"  File: {file_info.path}")
            print(f"  Checksum: {file_info.checksum}")
            print(f"  Loaded at: {file_info.loaded_at}")


if __name__ == "__main__":
    main()
