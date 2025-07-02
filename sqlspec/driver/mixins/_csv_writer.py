"""Optimized CSV writing utilities."""

import csv
from typing import TYPE_CHECKING, Any

from sqlspec.typing import PYARROW_INSTALLED

if TYPE_CHECKING:
    from sqlspec.statement.result import SQLResult

__all__ = ("write_csv", "write_csv_default", "write_csv_optimized")


def _raise_no_column_names_error() -> None:
    """Raise error when no column names are available."""
    msg = "No column names available"
    raise ValueError(msg)


def write_csv(result: "SQLResult", file: Any, **options: Any) -> None:
    """Write result to CSV file.

    Args:
        result: SQL result to write
        file: File-like object to write to
        **options: CSV writer options
    """
    if PYARROW_INSTALLED:
        try:
            write_csv_optimized(result, file, **options)
        except Exception:
            write_csv_default(result, file, **options)
    else:
        write_csv_default(result, file, **options)


def write_csv_default(result: "SQLResult", file: Any, **options: Any) -> None:
    """Write result to CSV file using default method.

    Args:
        result: SQL result to write
        file: File-like object to write to
        **options: CSV writer options
    """
    csv_options = options.copy()
    csv_options.pop("compression", None)
    csv_options.pop("partition_by", None)

    writer = csv.writer(file, **csv_options)
    if result.column_names:
        writer.writerow(result.column_names)
    if result.data:
        if result.data and isinstance(result.data[0], dict):
            rows = []
            for row_dict in result.data:
                row_values = [row_dict.get(col) for col in result.column_names or []]
                rows.append(row_values)
            writer.writerows(rows)
        else:
            writer.writerows(result.data)


def write_csv_optimized(result: "SQLResult", file: Any, **options: Any) -> None:
    """Write result to CSV using PyArrow if available for better performance.

    Args:
        result: SQL result to write
        file: File-like object to write to
        **options: CSV writer options
    """
    _ = options
    import pyarrow as pa
    import pyarrow.csv as pa_csv

    if not result.data:
        return

    if not hasattr(file, "name"):
        msg = "PyArrow CSV writer requires a file with a 'name' attribute"
        raise ValueError(msg)

    table: Any
    if isinstance(result.data[0], dict):
        table = pa.Table.from_pylist(result.data)
    elif result.column_names:
        data_dicts = [dict(zip(result.column_names, row)) for row in result.data]
        table = pa.Table.from_pylist(data_dicts)
    else:
        _raise_no_column_names_error()

    pa_csv.write_csv(table, file.name)  # pyright: ignore
