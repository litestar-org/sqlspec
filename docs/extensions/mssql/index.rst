===============================
Microsoft SQL Server and ODBC
===============================

SQLSpec ships two adapters for SQL Server and ODBC-backed databases:

.. list-table::
   :header-rows: 1
   :widths: 25 30 45

   * - Adapter
     - Use it for
     - Notes
   * - :doc:`mssql_python`
     - Default SQL Server and Azure SQL application work.
     - Microsoft-backed driver with sync and async SQLSpec sessions, T-SQL
       migrations, ADK stores, Litestar store support, native Arrow reads, and
       BulkCopy.
   * - :doc:`arrow_odbc`
     - Arrow-native reads and bulk Arrow inserts through vendor ODBC drivers.
     - Generic bridge for SQL Server, Oracle, MySQL, and other ODBC targets.
       It is intentionally Arrow-first and does not replace dedicated adapters
       for migrations, ADK, or framework stores.

Choose a guide
==============

.. grid:: 1 1 2 3
   :gutter: 2
   :padding: 0

   .. grid-item-card:: mssql-python
      :link: mssql_python
      :link-type: doc

      Configure SQL Server sessions, pooling, migrations, ADK, Litestar, Arrow,
      and BulkCopy.

   .. grid-item-card:: arrow-odbc
      :link: arrow_odbc
      :link-type: doc

      Stream Arrow batches and bulk insert ``pyarrow.Table`` payloads through
      ODBC.

   .. grid-item-card:: Cookbook
      :link: cookbook
      :link-type: doc

      T-SQL recipes for MERGE, JSON, UUIDs, temporal values, BulkCopy, Arrow,
      Entra ID, and Litestar sessions.

.. toctree::
   :hidden:

   mssql_python
   arrow_odbc
   cookbook
