<!-- markdownlint-disable -->
<p align="center">
  <img src="https://raw.githubusercontent.com/litestar-org/branding/473f54621e55cde9acbb6fcab7fc03036173eb3d/assets/Branding%20-%20PNG%20-%20Transparent/Logo%20-%20Banner%20-%20Inline%20-%20Light.png" alt="Litestar Logo - Light" width="100%" height="auto" />

</p>
<div align="center">
<!-- markdownlint-restore -->

# SQLSpec

SQL Experiments in Python


## Minimal SQL Abstractions for Python.

- Modern: Typed and Extensible
- Multi-database: SQLite, Postgres, DuckDB, MySQL, Oracle, SQL Server, Spanner, Big Query, and more...
- Easy ability to manipulate and add filters to queries
- Validate and Convert between dialects with `sqlglot`
- and more...

## Can it do `X`?

- Probably not currently; but, if it makes sense we can add enhancements.

## Inspiration

`aiosql` is the primary influence for this library.  However, I wanted to be able to use the query interface from `aiosql` a bit more flexibly.

Why not add it to `aiosql`?  Where it makes sense, many of these changes will likely get submitted to aiosql as a PR (`spanner` and `bigquery` drivers are likely the starting point.)
