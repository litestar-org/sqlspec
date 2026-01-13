Data Flow
=========

SQLSpec processes SQL statements through a consistent pipeline: statement parsing,
parameter normalization, driver execution, and result transformation. This guide keeps
things short and points you to deeper examples when you need them.

Pipeline Overview
-----------------

.. mermaid::

   flowchart LR
     A[SQL / QueryBuilder] --> B[StatementConfig]
     B --> C[Driver Adapter]
     C --> D[Result Objects]

Minimal Execution Example
-------------------------

.. literalinclude:: /examples/querying/execute_select.py
   :language: python
   :caption: ``execute select``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Next Steps
----------

- :doc:`drivers_and_querying` for driver-specific behaviors.
- :doc:`query_builder` for the fluent SQL builder pipeline.
