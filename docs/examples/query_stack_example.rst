====================
Query Stack Example
====================

This example builds an immutable ``StatementStack`` and executes it against both the synchronous SQLite adapter and the asynchronous AioSQLite adapter. Each stack:

1. Inserts an audit log row
2. Updates the user's last action
3. Fetches the user's roles

.. literalinclude:: query_stack_example.py
   :language: python
   :caption: ``docs/examples/query_stack_example.py``
   :linenos:

Run the script:

.. code-block:: console

   uv run python docs/examples/query_stack_example.py

Expected output shows inserted/updated row counts plus the projected role list for each adapter.
