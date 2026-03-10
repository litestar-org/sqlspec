==========
Parameters
==========

Type-safe parameter processing with automatic style detection and conversion.
Supports QMARK (``?``), NAMED (``:name``), NUMERIC (``$1``), and FORMAT (``%s``) styles.

.. currentmodule:: sqlspec.core.parameters

ParameterProcessor
==================

.. autoclass:: sqlspec.core.parameters._processor.ParameterProcessor
   :members:
   :show-inheritance:

ParameterConverter
==================

.. autoclass:: sqlspec.core.parameters._converter.ParameterConverter
   :members:
   :show-inheritance:

ParameterValidator
==================

.. autoclass:: sqlspec.core.parameters._validator.ParameterValidator
   :members:
   :show-inheritance:

Types
=====

.. autoclass:: sqlspec.core.parameters._types.ParameterStyle
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.core.parameters._types.ParameterStyleConfig
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.core.parameters._types.TypedParameter
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.core.parameters._types.ParameterInfo
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.core.parameters._types.DriverParameterProfile
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.core.parameters._types.ParameterProfile
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.core.parameters._types.ParameterProcessingResult
   :members:
   :show-inheritance:
