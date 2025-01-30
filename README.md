# SQLSpec

## A Query Mapper for Python

SQLSpec is an experimental Python library designed to streamline and modernize your SQL interactions across a variety of database systems. While still in its early stages, SQLSpec aims to provide a flexible, typed, and extensible interface for working with SQL in Python.

**Note**: SQLSpec is currently under active development and the API is subject to change.  It is not yet ready for production use.  Contributions are welcome!

## Core Features (Planned but subject to change, removal or redesign)

- **Consistent Database Session Interface**: Provides a consistent connectivity interface for interacting with one or more database systems, including SQLite, Postgres, DuckDB, MySQL, Oracle, SQL Server, Spanner, BigQuery, and more.
- **Emphasis on RAW SQL and Minimal Abstractions and Performance**: SQLSpec is a library for working with SQL in Python.  It's goals are to offer minimal abstractions between the user and the database.  It does not aim to be an ORM library.
- **Type-Safe Queries**: Quickly map SQL queries to typed objects using libraries such as Pydantic, Msgspec, Attrs, etc.
- **Extensible Design**: Easily add support for new database dialects or extend existing functionality to meet your specific needs.  Easily add support for async and sync database drivers.
- **Minimal Dependencies**: SQLSpec is designed to be lightweight and can run on it's own or with other libraries such as `litestar`, `fastapi`, `flask` and more.  (Contributions welcome!)
- **Dynamic Query Manipulation**: Easily apply filters to pre-defined queries with a fluent, Pythonic API. Safely manipulate queries without the risk of SQL injection.
- **Dialect Validation and Conversion**: Use `sqlglot` to validate your SQL against specific dialects and seamlessly convert between them.
- **Support for Async and Sync Database Drivers**: SQLSpec supports both async and sync database drivers, allowing you to choose the style that best fits your application.
- **Basic Migration Management**: A mechanism to generate empty migration files where you can add your own SQL and intelligently track which migrations have been applied.

## What SQLSpec Is Not (Yet)

SQLSpec is a work in progress. While it offers a solid foundation for modern SQL interactions, it does not yet include every feature you might find in a mature ORM or database toolkit. The focus is on building a robust, flexible core that can be extended over time.

## Inspiration and Future Direction

SQLSpec originally drew inspiration from features found in the `aiosql` library.  This is a great library for working with and executed SQL stored in files.  It's unclear how much of an overlap there will be between the two libraries, but it's possible that some features will be contributed back to `aiosql` where appropriate.

## Current Focus: Universal Connectivity

The primary goal at this stage is to establish a **native connectivity interface** that works seamlessly across all supported database environments. This means you can connect to any of the supported databases using a consistent API, regardless of the underlying driver or dialect.

## Adapters: Completed, In Progress, and Planned

This list is not final. If you have a driver you'd like to see added, please open an issue or submit a PR!

| Driver                                                                                                       | Database   | Mode    | Status     |
| :----------------------------------------------------------------------------------------------------------- | :--------- | :------ | :--------- |
| [`adbc`](https://arrow.apache.org/adbc/)                                                                     | Postgres   | Sync    | ✅         |
| [`adbc`](https://arrow.apache.org/adbc/)                                                                     | SQLite     | Sync    | ✅         |
| [`adbc`](https://arrow.apache.org/adbc/)                                                                     | Snowflake  | Sync    | ✅         |
| [`adbc`](https://arrow.apache.org/adbc/)                                                                     | DuckDB     | Sync    | ✅         |
| [`asyncpg`](https://magicstack.github.io/asyncpg/current/)                                                    | PostgreSQL | Async   | ✅         |
| [`psycopg`](https://www.psycopg.org/)                                                                         | PostgreSQL | Sync    | ✅         |
| [`psycopg`](https://www.psycopg.org/)                                                                         | PostgreSQL | Async   | ✅         |
| [`aiosqlite`](https://github.com/omnilib/aiosqlite)                                                           | SQLite     | Async   | ✅         |
| `sqlite3`                                                                                                    | SQLite     | Sync    | ✅         |
| [`oracledb`](https://oracle.github.io/python-oracledb/)                                                      | Oracle     | Async   | ✅         |
| [`oracledb`](https://oracle.github.io/python-oracledb/)                                                      | Oracle     | Sync    | ✅         |
| [`duckdb`](https://duckdb.org/)                                                                               | DuckDB     | Sync    | ✅         |
| [`bigquery`](https://googleapis.dev/python/bigquery/latest/index.html)                                        | BigQuery   | Sync    | 🗓️ |
| [`spanner`](https://googleapis.dev/python/spanner/latest/index.html)                                         | Spanner    | Sync    | 🗓️  |
| [`sqlserver`](https://docs.microsoft.com/en-us/sql/connect/python/pyodbc/python-sql-driver-for-pyodbc?view=sql-server-ver16) | SQL Server | Sync    | 🗓️  |
| [`mysql`](https://dev.mysql.com/doc/connector-python/en/connector-python-api-mysql-connector-python.html)     | MySQL      | Sync    | 🗓️  |
| [`snowflake`](https://docs.snowflake.com)                                                                    | Snowflake  | Sync    | 🗓️  |

## Proposed Project Structure

- `sqlspec/`:
    - `adapters/`: Contains all database drivers and associated configuration.
    - `extensions/`:
        - `litestar/`: Future home of `litestar` integration.
        - `fastapi/`: Future home of `fastapi` integration.
        - `flask/`: Future home of `flask` integration.
        - `*/`: Future home of your favorite framework integration 🔌 ✨
    - `base.py`: Contains base protocols for database configurations.
    - `filters.py`: Contains the `Filter` class which is used to apply filters to pre-defined SQL queries.
    - `utils/`: Contains utility functions used throughout the project.
    - `exceptions.py`: Contains custom exceptions for SQLSpec.
    - `typing.py`: Contains type hints, type guards and several facades for optional libraries that are not required for the core functionality of SQLSpec.

## Get Involved

SQLSpec is an open-source project, and contributions are welcome! Whether you're interested in adding support for new databases, improving the query interface, or simply providing feedback, your input is valuable.

**Disclaimer**: SQLSpec is under active development. Expect changes and improvements as the project evolves.
