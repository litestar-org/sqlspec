Oracle Async Connection
=======================

Use SQLSpec's async Oracle adapter to fetch the current timestamp. Override credentials with
``SQLSPEC_ORACLE_USER``, ``SQLSPEC_ORACLE_PASSWORD``, and ``SQLSPEC_ORACLE_DSN``.

.. code-block:: console

   SQLSPEC_ORACLE_USER=system SQLSPEC_ORACLE_PASSWORD=oracle SQLSPEC_ORACLE_DSN=localhost/FREE \
     uv run python docs/examples/adapters/oracledb/connect_async.py

Source
------

.. literalinclude:: connect_async.py
   :language: python
   :linenos:
