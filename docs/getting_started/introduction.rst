============
Introduction
============

What is SQLSpec?
----------------

SQLSpec is a modern, high-performance Python library for database interaction that prioritizes **raw SQL**, **type safety**, and a **unified data access layer**. It is designed for developers who want to work directly with SQL while benefiting from modern Python features like type hinting and asynchronous programming, without the overhead of a traditional Object-Relational Mapper (ORM).

At its core, SQLSpec provides a consistent and extensible interface for a wide array of database systems, including PostgreSQL, SQLite, DuckDB, MySQL, Oracle, BigQuery, and many others.

Why Not an ORM?
---------------

While both SQLSpec and ORMs like SQLAlchemy facilitate database interaction, they operate on fundamentally different philosophies. SQLAlchemy is best known for its powerful ORM, which abstracts away SQL in favor of a high-level, object-oriented domain model. It excels at managing complex object graphs and database sessions.

SQLSpec, by contrast, is **not an ORM** and is designed for developers who prefer to work directly with SQL. Its primary goal is to enhance the experience of writing raw SQL, not to hide it. The key differentiators are:

1.  **Level of Abstraction**: SQLSpec provides a minimal, lightweight abstraction layer. It keeps you close to the database, focusing on query validation, type-safe result mapping, and a unified I/O layer.
2.  **Core Use Case**: SQLSpec is a "query mapper" and data integration tool. It is ideal for data-centric applications, APIs, and data engineering workflows where you write optimized SQL and need to move data efficiently.
3.  **Performance & Data Engineering Focus**: SQLSpec's architecture is designed for high-performance data workflows. The ability to efficiently handle data formats like Apache Arrow is a core feature.

Core Features
-------------

-   **First-Class SQL with Validation**: Write the SQL you want. SQLSpec's processing pipeline parses, transforms, and validates your queries for security and performance before they ever hit the database.
-   **Type-Safe Results**: Automatically map query results to typed data objects like Pydantic models, ``msgspec`` Structs, or dataclasses. This eliminates guesswork and runtime errors.
-   **Unified Connectivity**: A single, consistent API for database operations across more than 10 supported database backends, both synchronous and asynchronous.
-   **Comprehensive Query Builder**: An optional, fluent API for programmatically constructing everything from simple ``SELECT`` statements to complex DDL and DML queries.
-   **Built-in Instrumentation**: Observability is a first-class citizen with configurable support for OpenTelemetry and Prometheus.

Who is SQLSpec for?
-------------------

SQLSpec is for you if:

-   You like writing SQL and want to maintain full control over your queries.
-   You value type safety and want your database interactions to be as predictable as the rest of your Python code.
-   You are building data-intensive applications, APIs, or data engineering pipelines.
-   You want to avoid the complexity and overhead of a full-featured ORM.
-   You need to work with multiple different database backends without changing your application logic.
