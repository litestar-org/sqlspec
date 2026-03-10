==========
Exceptions
==========

Complete exception hierarchy for SQLSpec. All exceptions inherit from
:class:`SQLSpecError`.

.. currentmodule:: sqlspec.exceptions

Base
====

.. autoclass:: SQLSpecError
   :members:
   :show-inheritance:

Configuration
=============

.. autoclass:: ImproperConfigurationError
   :members:
   :show-inheritance:

.. autoclass:: ConfigResolverError
   :members:
   :show-inheritance:

.. autoclass:: BackendNotRegisteredError
   :members:
   :show-inheritance:

.. autoclass:: MissingDependencyError
   :members:
   :show-inheritance:

Connection
==========

.. autoclass:: DatabaseConnectionError
   :members:
   :show-inheritance:

.. autoclass:: ConnectionTimeoutError
   :members:
   :show-inheritance:

.. autoclass:: PermissionDeniedError
   :members:
   :show-inheritance:

Transaction
===========

.. autoclass:: TransactionError
   :members:
   :show-inheritance:

.. autoclass:: SerializationConflictError
   :members:
   :show-inheritance:

.. autoclass:: TransactionRetryError
   :members:
   :show-inheritance:

.. autoclass:: DeadlockError
   :members:
   :show-inheritance:

Repository
==========

.. autoclass:: RepositoryError
   :members:
   :show-inheritance:

.. autoclass:: NotFoundError
   :members:
   :show-inheritance:

.. autoclass:: MultipleResultsFoundError
   :members:
   :show-inheritance:

Integrity
=========

.. autoclass:: IntegrityError
   :members:
   :show-inheritance:

.. autoclass:: UniqueViolationError
   :members:
   :show-inheritance:

.. autoclass:: ForeignKeyViolationError
   :members:
   :show-inheritance:

.. autoclass:: CheckViolationError
   :members:
   :show-inheritance:

.. autoclass:: NotNullViolationError
   :members:
   :show-inheritance:

SQL Processing
==============

.. autoclass:: SQLParsingError
   :members:
   :show-inheritance:

.. autoclass:: SQLBuilderError
   :members:
   :show-inheritance:

.. autoclass:: SQLConversionError
   :members:
   :show-inheritance:

.. autoclass:: DialectNotSupportedError
   :members:
   :show-inheritance:

Execution
=========

.. autoclass:: OperationalError
   :members:
   :show-inheritance:

.. autoclass:: QueryTimeoutError
   :members:
   :show-inheritance:

.. autoclass:: DataError
   :members:
   :show-inheritance:

.. autoclass:: StackExecutionError
   :members:
   :show-inheritance:

Storage
=======

.. autoclass:: StorageOperationFailedError
   :members:
   :show-inheritance:

.. autoclass:: StorageCapabilityError
   :members:
   :show-inheritance:

.. autoclass:: FileNotFoundInStorageError
   :members:
   :show-inheritance:

SQL Files
=========

.. autoclass:: SQLFileNotFoundError
   :members:
   :show-inheritance:

.. autoclass:: SQLFileParseError
   :members:
   :show-inheritance:

Migration
=========

.. autoclass:: MigrationError
   :members:
   :show-inheritance:

.. autoclass:: InvalidVersionFormatError
   :members:
   :show-inheritance:

.. autoclass:: OutOfOrderMigrationError
   :members:
   :show-inheritance:

.. autoclass:: SquashValidationError
   :members:
   :show-inheritance:

Serialization
=============

.. autoclass:: SerializationError
   :members:
   :show-inheritance:

Events
======

.. autoclass:: EventChannelError
   :members:
   :show-inheritance:

Inheritance Tree
================

.. code-block:: text

   SQLSpecError
   +-- ImproperConfigurationError
   +-- ConfigResolverError
   +-- BackendNotRegisteredError
   +-- MissingDependencyError
   +-- EventChannelError
   +-- SQLParsingError
   +-- SQLBuilderError
   |   +-- DialectNotSupportedError
   +-- SQLConversionError
   +-- SerializationError
   +-- DatabaseConnectionError
   |   +-- PermissionDeniedError
   |   +-- ConnectionTimeoutError
   +-- TransactionError
   |   +-- SerializationConflictError
   |   +-- TransactionRetryError
   |   +-- DeadlockError
   +-- RepositoryError
   |   +-- NotFoundError
   |   +-- MultipleResultsFoundError
   |   +-- IntegrityError
   |       +-- UniqueViolationError
   |       +-- ForeignKeyViolationError
   |       +-- CheckViolationError
   |       +-- NotNullViolationError
   +-- DataError
   +-- OperationalError
   |   +-- QueryTimeoutError
   +-- StackExecutionError
   +-- StorageOperationFailedError
   |   +-- FileNotFoundInStorageError
   +-- StorageCapabilityError
   +-- SQLFileNotFoundError
   +-- SQLFileParseError
   +-- MigrationError
       +-- InvalidVersionFormatError
       +-- OutOfOrderMigrationError
       +-- SquashValidationError

SQLSTATE Mapping
================

.. autofunction:: map_sqlstate_to_exception
