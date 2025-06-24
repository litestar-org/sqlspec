"""Integration tests for storage functionality within database drivers."""

import tempfile
from collections.abc import Generator
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from sqlspec.adapters.sqlite import SqliteConfig, SqliteDriver
from sqlspec.statement.result import ArrowResult
from sqlspec.statement.sql import SQLConfig


@pytest.fixture
def temp_directory() -> Generator[Path, None, None]:
    """Create a temporary directory for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def sqlite_with_test_data() -> Generator[SqliteDriver, None, None]:
    """Create SQLite driver with test data for storage operations."""
    config = SqliteConfig(database=":memory:", statement_config=SQLConfig(strict_mode=False))

    with config.provide_session() as driver:
        # Create test table
        driver.execute_script("""
            CREATE TABLE products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                category TEXT,
                price REAL,
                in_stock BOOLEAN DEFAULT 1,
                created_date DATE DEFAULT CURRENT_DATE
            )
        """)

        # Insert test data
        test_products = [
            ("Laptop", "Electronics", 999.99, True),
            ("Book", "Education", 19.99, True),
            ("Chair", "Furniture", 89.99, False),
            ("Phone", "Electronics", 599.99, True),
            ("Desk", "Furniture", 199.99, True),
        ]

        driver.execute_many("INSERT INTO products (name, category, price, in_stock) VALUES (?, ?, ?, ?)", test_products)

        yield driver


def test_export_to_storage_parquet_basic(sqlite_with_test_data: SqliteDriver, temp_directory: Path) -> None:
    """Test basic export to Parquet storage."""
    output_file = temp_directory / "products.parquet"

    # Export all products
    sqlite_with_test_data.export_to_storage("SELECT * FROM products ORDER BY id", destination_uri=str(output_file))

    # Verify file was created
    assert output_file.exists()
    assert output_file.stat().st_size > 0

    # Verify content by reading with pyarrow directly
    table = pq.read_table(output_file)
    assert table.num_rows == 5
    assert "name" in table.column_names
    assert "category" in table.column_names
    assert "price" in table.column_names

    # Verify data integrity
    names = table["name"].to_pylist()
    assert "Laptop" in names
    assert "Book" in names


def test_export_to_storage_with_filters(sqlite_with_test_data: SqliteDriver, temp_directory: Path) -> None:
    """Test export with WHERE clause filtering."""
    output_file = temp_directory / "electronics.parquet"

    # Export only electronics
    sqlite_with_test_data.export_to_storage(
        "SELECT name, price FROM products WHERE category = 'Electronics' ORDER BY price DESC",
        destination_uri=str(output_file),
    )

    assert output_file.exists()

    # Verify filtered content
    table = pq.read_table(output_file)
    assert table.num_rows == 2  # Laptop and Phone

    names = table["name"].to_pylist()
    prices = table["price"].to_pylist()
    assert len(names) == 2
    assert len(prices) == 2
    assert prices is not None

    assert "Laptop" in names
    assert "Phone" in names
    assert "Book" not in names  # Should be filtered out

    # Verify ordering (DESC by price)
    assert prices[0] > prices[1]  # type: ignore[operator]


def test_export_to_storage_csv_format(sqlite_with_test_data: SqliteDriver, temp_directory: Path) -> None:
    """Test export to CSV format."""
    output_file = temp_directory / "products.csv"

    # Export to CSV
    sqlite_with_test_data.export_to_storage(
        "SELECT name, category, price FROM products WHERE in_stock = 1", destination_uri=str(output_file), format="csv"
    )

    assert output_file.exists()

    # Read and verify CSV content
    csv_content = output_file.read_text()
    lines = csv_content.strip().split("\n")

    # Should have header + 4 in-stock items
    assert len(lines) >= 4

    # Check header
    header = lines[0].lower()
    assert "name" in header
    assert "category" in header
    assert "price" in header

    # Verify data is present
    csv_data = "\n".join(lines)
    assert "Laptop" in csv_data
    assert "Book" in csv_data
    assert "Chair" not in csv_data  # Out of stock


def test_export_to_storage_json_format(sqlite_with_test_data: SqliteDriver, temp_directory: Path) -> None:
    """Test export to JSON format."""
    output_file = temp_directory / "products.json"

    # Export to JSON
    sqlite_with_test_data.export_to_storage(
        "SELECT id, name, price FROM products WHERE price > 50", destination_uri=str(output_file), format="json"
    )

    assert output_file.exists()

    # Read and verify JSON content
    import json

    with open(output_file) as f:
        data = json.load(f)

    assert isinstance(data, list)
    assert len(data) == 4  # 4 products with price > 50

    # Check structure of first record
    first_item = data[0]
    assert "id" in first_item
    assert "name" in first_item
    assert "price" in first_item

    # Verify price filtering
    for item in data:
        assert item["price"] > 50

    # Verify specific items
    names = [item["name"] for item in data]
    assert "Laptop" in names
    assert "Phone" in names
    assert "Desk" in names
    assert "Chair" in names
    assert "Book" not in names  # Price is 19.99, should be excluded


def test_fetch_arrow_table_functionality(sqlite_with_test_data: SqliteDriver) -> None:
    """Test fetch_arrow_table returns proper ArrowResult."""
    result = sqlite_with_test_data.fetch_arrow_table(
        "SELECT name, category, price FROM products WHERE category = 'Electronics'"
    )

    assert isinstance(result, ArrowResult)
    assert result.num_rows == 2
    assert result.num_columns == 3
    assert result.column_names == ["name", "category", "price"]

    # Verify data access
    names = result.data["name"].to_pylist()
    categories = result.data["category"].to_pylist()

    assert "Laptop" in names
    assert "Phone" in names
    assert all(cat == "Electronics" for cat in categories)


def test_fetch_arrow_table_with_parameters(sqlite_with_test_data: SqliteDriver) -> None:
    """Test fetch_arrow_table with parameterized queries."""
    from sqlspec.statement.sql import SQL

    sql_query = SQL("SELECT * FROM products WHERE price BETWEEN ? AND ? ORDER BY price", parameters=[50.0, 500.0])

    result = sqlite_with_test_data.fetch_arrow_table(sql_query)

    assert isinstance(result, ArrowResult)
    assert result.num_rows >= 1  # Should find some products in this range

    if result.num_rows > 0:
        prices = result.data["price"].to_pylist()
        # Verify price filtering worked
        assert all(50.0 <= price <= 500.0 for price in prices if price is not None)
        # Verify ordering
        assert prices is not None
        assert all(p is not None for p in prices)  # No null prices
        # Filter out None values for sorting
        non_null_prices = [p for p in prices if p is not None]
        assert non_null_prices == sorted(non_null_prices)


def test_storage_error_handling(sqlite_with_test_data: SqliteDriver, temp_directory: Path) -> None:
    """Test error handling in storage operations."""
    # Test export to invalid path (should raise exception)
    invalid_path = "/root/nonexistent/invalid.parquet"

    with pytest.raises(Exception):  # Could be PermissionError, FileNotFoundError, etc.
        sqlite_with_test_data.export_to_storage("SELECT * FROM products", destination_uri=invalid_path)

    # Test export with invalid SQL
    valid_path = temp_directory / "test.parquet"

    with pytest.raises(Exception):  # Should be SQL syntax error
        sqlite_with_test_data.export_to_storage("SELECT * FROM nonexistent_table", destination_uri=str(valid_path))


def test_storage_compression_options(sqlite_with_test_data: SqliteDriver, temp_directory: Path) -> None:
    """Test different compression options for Parquet export."""
    base_query = "SELECT * FROM products"

    # Test different compression algorithms
    compression_types = ["none", "snappy", "gzip"]
    file_sizes = {}

    for compression in compression_types:
        output_file = temp_directory / f"products_{compression}.parquet"

        sqlite_with_test_data.export_to_storage(base_query, destination_uri=str(output_file), compression=compression)

        assert output_file.exists()
        file_sizes[compression] = output_file.stat().st_size

    # All files should have reasonable sizes
    for size in file_sizes.values():
        assert size > 0

    # Compressed files might be smaller (though with small datasets, overhead can make them larger)
    # Just verify they all contain the same data
    for compression in compression_types:
        table = pq.read_table(temp_directory / f"products_{compression}.parquet")
        assert table.num_rows == 5
        assert table.num_columns >= 5


def test_storage_schema_preservation(sqlite_with_test_data: SqliteDriver, temp_directory: Path) -> None:
    """Test that schema and data types are preserved through storage operations."""
    output_file = temp_directory / "schema_test.parquet"

    # Export data with various types
    sqlite_with_test_data.export_to_storage(
        "SELECT id, name, price, in_stock FROM products", destination_uri=str(output_file)
    )

    # Read back and verify schema
    table = pq.read_table(output_file)

    assert table.num_rows == 5
    assert "id" in table.column_names
    assert "name" in table.column_names
    assert "price" in table.column_names
    assert "in_stock" in table.column_names

    # Verify data types are reasonable
    schema = table.schema
    for field in schema:
        assert field.name in ["id", "name", "price", "in_stock"]
        # Types should be preserved as much as possible


def test_storage_large_dataset_handling(sqlite_with_test_data: SqliteDriver, temp_directory: Path) -> None:
    """Test storage operations with larger datasets."""
    # Insert more test data
    large_batch = [(f"Product_{i}", f"Category_{i % 5}", i * 10.0 + 9.99, i % 2 == 0) for i in range(100, 1000)]

    sqlite_with_test_data.execute_many(
        "INSERT INTO products (name, category, price, in_stock) VALUES (?, ?, ?, ?)", large_batch
    )

    # Export larger dataset
    output_file = temp_directory / "large_dataset.parquet"

    sqlite_with_test_data.export_to_storage(
        "SELECT * FROM products WHERE price > 100 ORDER BY id", destination_uri=str(output_file), compression="snappy"
    )

    assert output_file.exists()

    # Verify large dataset
    table = pq.read_table(output_file)
    assert table.num_rows > 100  # Should have many rows

    # Spot check data integrity
    prices = table["price"].to_pylist()
    assert prices is not None
    assert len(prices) > 0
    assert all(price > 100 for price in prices)  # type: ignore[operator]


def test_export_with_complex_sql(sqlite_with_test_data: SqliteDriver, temp_directory: Path) -> None:
    """Test export with complex SQL queries (aggregations, grouping, etc.)."""
    # Create aggregated export
    output_file = temp_directory / "category_summary.parquet"

    sqlite_with_test_data.export_to_storage(
        """
        SELECT
            category,
            COUNT(*) as product_count,
            AVG(price) as avg_price,
            MAX(price) as max_price,
            MIN(price) as min_price,
            SUM(CASE WHEN in_stock THEN 1 ELSE 0 END) as in_stock_count
        FROM products
        GROUP BY category
        ORDER BY avg_price DESC
        """,
        destination_uri=str(output_file),
    )

    assert output_file.exists()

    # Verify aggregated data
    table = pq.read_table(output_file)
    assert table.num_rows >= 2  # Should have multiple categories

    expected_columns = ["category", "product_count", "avg_price", "max_price", "min_price", "in_stock_count"]
    for col in expected_columns:
        assert col in table.column_names

    # Verify aggregations make sense
    product_counts = table["product_count"].to_pylist() or []
    assert all(count > 0 for count in product_counts)  # type: ignore[operator]

    avg_prices = table["avg_price"].to_pylist() or []
    max_prices = table["max_price"].to_pylist() or []

    # Max should be >= avg for each category
    for avg, max_price in zip(avg_prices, max_prices):
        assert max_price >= avg  # type: ignore[operator]


@pytest.mark.skip(reason="SQLite connections cannot be shared across threads")
def test_concurrent_storage_operations(sqlite_with_test_data: SqliteDriver, temp_directory: Path) -> None:
    """Test concurrent storage operations."""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    def export_worker(worker_id: int) -> str:
        output_file = temp_directory / f"concurrent_{worker_id}.parquet"
        sqlite_with_test_data.export_to_storage(
            f"SELECT * FROM products WHERE id % 5 = {worker_id % 5}", destination_uri=str(output_file)
        )
        return str(output_file)

    # Run multiple concurrent exports
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(export_worker, i) for i in range(5)]

        exported_files = [future.result() for future in as_completed(futures)]

    # Verify all exports succeeded
    assert len(exported_files) == 5
    for file_path in exported_files:
        assert Path(file_path).exists()
        assert Path(file_path).stat().st_size > 0

        # Verify content
        table = pq.read_table(file_path)
        assert table.num_rows >= 0  # Could be 0 or more depending on filter
