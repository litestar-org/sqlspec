==========
Parameters
==========

Type-safe parameter processing with automatic style detection and conversion.
Supports QMARK (``?``), NAMED (``:name``), NUMERIC (``$1``), and FORMAT (``%s``) styles.

.. currentmodule:: sqlspec.core.parameters

ParameterProcessor
==================

.. autoclass:: ParameterProcessor
   :members:
   :show-inheritance:

ParameterConverter
==================

.. autoclass:: ParameterConverter
   :members:
   :show-inheritance:

ParameterValidator
==================

.. autoclass:: ParameterValidator
   :members:
   :show-inheritance:

Types and Profiles
==================

.. autoclass:: ParameterStyle
   :members:
   :show-inheritance:

.. autoclass:: ParameterStyleConfig
   :members:
   :show-inheritance:

.. autoclass:: TypedParameter
   :members:
   :show-inheritance:

.. autoclass:: ParameterInfo
   :members:
   :show-inheritance:

.. autoclass:: DriverParameterProfile
   :members:
   :show-inheritance:

.. autoclass:: ParameterProfile
   :members:
   :show-inheritance:

.. autoclass:: ParameterProcessingResult
   :members:
   :show-inheritance:

.. autoclass:: ParameterDeclaration
   :members:
   :show-inheritance:

Profile Management
==================

.. autofunction:: get_driver_profile

.. autofunction:: register_driver_profile

.. autofunction:: build_statement_config_from_profile

Parameter Helpers
=================

.. autofunction:: validate_parameter_alignment

.. autofunction:: normalize_parameter_key

.. autofunction:: is_iterable_parameters

.. autofunction:: wrap_with_type

.. autofunction:: register_param_type

.. autofunction:: resolve_param_type

.. autofunction:: matches_param_type
