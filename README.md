<!-- markdownlint-disable -->
<p align="center">
  <!-- github-banner-start -->
  <img src="https://raw.githubusercontent.com/litestar-org/branding/main/assets/Branding%20-%20SVG%20-%20Transparent/Logo%20-%20Banner%20-%20Inline%20-%20Light.svg#gh-light-mode-only" alt="Litestar Logo - Light" width="100%" height="auto" />
  <img src="https://raw.githubusercontent.com/litestar-org/branding/main/assets/Branding%20-%20SVG%20-%20Transparent/Logo%20-%20Banner%20-%20Inline%20-%20Dark.svg#gh-dark-mode-only" alt="Litestar Logo - Dark" width="100%" height="auto" />
  <!-- github-banner-end -->

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
