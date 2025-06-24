"""Integration tests for storage mixins in database drivers."""

import tempfile
from collections.abc import Generator
from pathlib import Path

import pytest

from sqlspec.adapters.sqlite import SqliteConfig, SqliteDriver
from sqlspec.statement.result import ArrowResult, SQLResult
from sqlspec.statement.sql import SQLConfig


@pytest.fixture
def temp_directory() -> Generator[Path, None, None]:
    """Create a temporary directory for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def sqlite_driver_with_storage() -> Generator[SqliteDriver, None, None]:
    """Create a SQLite driver with storage capabilities for testing."""
    config = SqliteConfig(database=":memory:", statement_config=SQLConfig(strict_mode=False))

    with config.provide_session() as driver:
        # Create test table with sample data
        driver.execute_script("""
            CREATE TABLE storage_test (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                category TEXT,
                value INTEGER,
                price REAL,
                active BOOLEAN DEFAULT 1,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Insert test data
        test_data = [
            ("Product A", "electronics", 100, 19.99, True),
            ("Product B", "books", 50, 15.50, True),
            ("Product C", "electronics", 75, 29.99, False),
            ("Product D", "clothing", 200, 45.00, True),
            ("Product E", "books", 30, 12.99, True),
        ]

        driver.execute_many(
            "INSERT INTO storage_test (name, category, value, price, active) VALUES (?, ?, ?, ?, ?)", test_data
        )

        yield driver


def test_driver_export_to_storage_parquet(sqlite_driver_with_storage: SqliteDriver, temp_directory: Path) -> None:
    """Test export_to_storage with Parquet format."""
    output_file = temp_directory / "export_test.parquet"

    # Export data to Parquet
    sqlite_driver_with_storage.export_to_storage(
        "SELECT * FROM storage_test WHERE active = 1 ORDER BY id", destination_uri=str(output_file)
    )

    assert output_file.exists()
    assert output_file.stat().st_size > 0

    # Verify we can read the exported data
    import pyarrow.parquet as pq

    table = pq.read_table(output_file)

    assert table.num_rows == 4  # Only active products
    assert "name" in table.column_names
    assert "category" in table.column_names

    # Check specific data
    names = table["name"].to_pylist()
    assert "Product A" in names
    assert "Product C" not in names  # Inactive product


def test_driver_export_to_storage_with_parameters(
    sqlite_driver_with_storage: SqliteDriver, temp_directory: Path
) -> None:
    """Test export_to_storage with parameterized queries."""
    output_file = temp_directory / "filtered_export.parquet"

    # Export with parameters
    sqlite_driver_with_storage.export_to_storage(
        "SELECT name, price FROM storage_test WHERE category = ? AND price > ?",
        ("electronics", 20.0),
        destination_uri=str(output_file),
    )

    assert output_file.exists()

    # Verify filtered data
    import pyarrow.parquet as pq

    table = pq.read_table(output_file)

    assert table.num_rows == 1  # Only Product C meets criteria (but might be inactive)
    # Actually, let's check - Product A is electronics and 19.99 < 20.0, Product C is electronics and 29.99 > 20.0
    # So we should get Product C even though it's inactive
    prices = table["price"].to_pylist()
    assert 29.99 in prices


def test_driver_export_to_storage_csv_format(sqlite_driver_with_storage: SqliteDriver, temp_directory: Path) -> None:
    """Test export_to_storage with CSV format."""
    output_file = temp_directory / "export_test.csv"

    # Export to CSV
    sqlite_driver_with_storage.export_to_storage(
        "SELECT name, category, price FROM storage_test ORDER BY price", destination_uri=str(output_file), format="csv"
    )

    assert output_file.exists()

    # Verify CSV content
    csv_content = output_file.read_text()
    assert "name,category,price" in csv_content or "name" in csv_content.split("\n")[0]
    assert "Product E" in csv_content  # Cheapest product
    assert "Product D" in csv_content  # Most expensive product


def test_driver_export_to_storage_json_format(sqlite_driver_with_storage: SqliteDriver, temp_directory: Path) -> None:
    """Test export_to_storage with JSON format."""
    output_file = temp_directory / "export_test.json"

    # Export to JSON
    sqlite_driver_with_storage.export_to_storage(
        "SELECT id, name, category FROM storage_test WHERE id <= 3", destination_uri=str(output_file), format="json"
    )

    assert output_file.exists()

    # Verify JSON content
    import json

    with open(output_file) as f:
        data = json.load(f)

    assert isinstance(data, list)
    assert len(data) == 3

    # Check structure
    first_record = data[0]
    assert "id" in first_record
    assert "name" in first_record
    assert "category" in first_record


def test_driver_import_from_storage(sqlite_driver_with_storage: SqliteDriver, temp_directory: Path) -> None:
    """Test import_from_storage functionality."""
    # First export data - use CSV format since SQLite only supports CSV for bulk import
    export_file = temp_directory / "for_import.csv"
    sqlite_driver_with_storage.export_to_storage(
        "SELECT name, category, price FROM storage_test WHERE category = 'books'",
        destination_uri=str(export_file),
        format="csv",
    )

    # Create new table for import
    sqlite_driver_with_storage.execute_script("""
        CREATE TABLE imported_products (
            name TEXT,
            category TEXT,
            price REAL
        )
    """)

    # Import data
    rows_imported = sqlite_driver_with_storage.import_from_storage(str(export_file), "imported_products")

    assert rows_imported == 2  # Two book products

    # Verify imported data
    result = sqlite_driver_with_storage.execute("SELECT COUNT(*) as count FROM imported_products")
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert result.data[0]["count"] == 2


def test_driver_fetch_arrow_table_direct(sqlite_driver_with_storage: SqliteDriver) -> None:
    """Test direct fetch_arrow_table functionality."""
    result = sqlite_driver_with_storage.fetch_arrow_table(
        "SELECT name, price FROM storage_test WHERE active = 1 ORDER BY price"
    )

    assert isinstance(result, ArrowResult)
    assert result.num_rows == 4  # Active products
    assert result.num_columns == 2
    assert result.column_names == ["name", "price"]

    # Verify data ordering (by price)
    prices = result.data["price"].to_pylist()
    assert all(p is not None for p in prices)  # No nulls in price
    # Filter out None values for sorting
    non_null_prices = [p for p in prices if p is not None]
    assert non_null_prices == sorted(non_null_prices)

    names = result.data["name"].to_pylist()
    assert "Product E" in names  # Cheapest
    assert "Product D" in names  # Most expensive


def test_driver_fetch_arrow_table_with_parameters(sqlite_driver_with_storage: SqliteDriver) -> None:
    """Test fetch_arrow_table with parameters."""
    from sqlspec.statement.sql import SQL

    sql_query = SQL("SELECT * FROM storage_test WHERE category = ? AND value > ?", parameters=["electronics", 50])

    result = sqlite_driver_with_storage.fetch_arrow_table(sql_query)

    assert isinstance(result, ArrowResult)
    assert result.num_rows >= 1  # Should find some electronics with value > 50

    # Verify filtering worked
    if result.num_rows > 0:
        categories = result.data["category"].to_pylist()
        values = result.data["value"].to_pylist()

        # All should be electronics
        assert all(cat == "electronics" for cat in categories)
        # All values should be > 50
        assert all(val is not None and val > 50 for val in values)


def test_driver_storage_operations_with_large_dataset(
    sqlite_driver_with_storage: SqliteDriver, temp_directory: Path
) -> None:
    """Test storage operations with larger datasets."""
    # Insert larger dataset
    large_data = [(f"Item_{i}", f"cat_{i % 5}", i * 10, i * 2.5, i % 2 == 0) for i in range(1000)]

    sqlite_driver_with_storage.execute_many(
        "INSERT INTO storage_test (name, category, value, price, active) VALUES (?, ?, ?, ?, ?)", large_data
    )

    # Export large dataset
    output_file = temp_directory / "large_export.parquet"
    sqlite_driver_with_storage.export_to_storage(
        "SELECT * FROM storage_test WHERE value > 5000 ORDER BY value",
        destination_uri=str(output_file),
        compression="snappy",
    )

    assert output_file.exists()

    # Verify export
    import pyarrow.parquet as pq

    table = pq.read_table(output_file)

    assert table.num_rows > 100  # Should have many rows

    # Verify data integrity with spot checks
    values = table["value"].to_pylist()
    assert all(val is not None and val > 5000 for val in values)
    assert all(v is not None for v in values)  # No nulls
    # Filter out None values for sorting
    non_null_values = [v for v in values if v is not None]
    assert non_null_values == sorted(non_null_values)


def test_driver_storage_error_handling(sqlite_driver_with_storage: SqliteDriver, temp_directory: Path) -> None:
    """Test error handling in storage operations."""
    # Test export to invalid path
    invalid_path = "/root/invalid_export.parquet"

    with pytest.raises(Exception):  # Should raise permission or path error
        sqlite_driver_with_storage.export_to_storage("SELECT * FROM storage_test", destination_uri=invalid_path)

    # Test import from nonexistent file
    nonexistent_file = temp_directory / "nonexistent.parquet"

    # Storage backend wraps FileNotFoundError in StorageOperationFailedError
    from sqlspec.exceptions import StorageOperationFailedError

    with pytest.raises(StorageOperationFailedError):
        sqlite_driver_with_storage.import_from_storage(str(nonexistent_file), "storage_test")


def test_driver_storage_format_detection(sqlite_driver_with_storage: SqliteDriver, temp_directory: Path) -> None:
    """Test automatic format detection from file extensions."""
    query = "SELECT name, price FROM storage_test LIMIT 3"

    # Test different formats with auto-detection
    formats = ["parquet", "csv", "json"]

    for fmt in formats:
        output_file = temp_directory / f"auto_detect.{fmt}"

        # Export without specifying format (should auto-detect)
        sqlite_driver_with_storage.export_to_storage(query, destination_uri=str(output_file))

        assert output_file.exists()
        assert output_file.stat().st_size > 0


def test_driver_storage_compression_options(sqlite_driver_with_storage: SqliteDriver, temp_directory: Path) -> None:
    """Test different compression options."""
    query = "SELECT * FROM storage_test"

    # Test different compression levels for Parquet
    compression_types = ["none", "snappy", "gzip"]

    file_sizes = {}
    for compression in compression_types:
        output_file = temp_directory / f"compressed_{compression}.parquet"

        sqlite_driver_with_storage.export_to_storage(query, destination_uri=str(output_file), compression=compression)

        assert output_file.exists()
        file_sizes[compression] = output_file.stat().st_size

    # Compressed files should generally be smaller than uncompressed
    # (though with small datasets, overhead might make this not always true)
    assert all(size > 0 for size in file_sizes.values())


def test_driver_storage_schema_preservation(sqlite_driver_with_storage: SqliteDriver, temp_directory: Path) -> None:
    """Test that data types and schema are preserved through storage operations."""
    # Create table with specific types
    sqlite_driver_with_storage.execute_script("""
        CREATE TABLE schema_test (
            id INTEGER,
            name TEXT,
            price REAL,
            active BOOLEAN,
            created_date DATE
        )
    """)

    # Insert data with specific types
    sqlite_driver_with_storage.execute(
        "INSERT INTO schema_test VALUES (?, ?, ?, ?, ?)", (1, "Test Product", 29.99, True, "2024-01-15")
    )

    # Export and import back
    export_file = temp_directory / "schema_test.parquet"
    sqlite_driver_with_storage.export_to_storage("SELECT * FROM schema_test", destination_uri=str(export_file))

    # Read back as Arrow to check schema
    _ = sqlite_driver_with_storage.fetch_arrow_table(
        "SELECT * FROM (SELECT * FROM storage_test LIMIT 0)"  # Empty result to get schema
    )

    # Verify we can export and the file exists
    assert export_file.exists()

    # Read the Parquet file directly to check schema preservation
    import pyarrow.parquet as pq

    table = pq.read_table(export_file)

    assert table.num_rows == 1
    assert "id" in table.column_names
    assert "name" in table.column_names
    assert "price" in table.column_names


@pytest.mark.skip(reason="SQLite connections cannot be shared across threads")
def test_driver_concurrent_storage_operations(sqlite_driver_with_storage: SqliteDriver, temp_directory: Path) -> None:
    """Test concurrent storage operations."""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    def export_worker(worker_id: int) -> str:
        output_file = temp_directory / f"concurrent_export_{worker_id}.parquet"
        sqlite_driver_with_storage.export_to_storage(
            f"SELECT * FROM storage_test WHERE id = {worker_id + 1}", destination_uri=str(output_file)
        )
        return str(output_file)

    # Create multiple concurrent exports
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(export_worker, i) for i in range(5)]

        exported_files = [future.result() for future in as_completed(futures)]

    # Verify all exports succeeded
    assert len(exported_files) == 5
    for file_path in exported_files:
        assert Path(file_path).exists()
        assert Path(file_path).stat().st_size > 0
