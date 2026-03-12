===============
Type Converters
===============

Output and input type converters for transforming database values. Used by
drivers to convert between Python types and database-native representations.

.. currentmodule:: sqlspec.core.type_converter

Base Classes
============

.. autoclass:: BaseTypeConverter
   :members:
   :show-inheritance:

.. autoclass:: CachedOutputConverter
   :members:
   :show-inheritance:

.. autoclass:: BaseInputConverter
   :members:
   :show-inheritance:

Built-in Converters
===================

.. autofunction:: convert_decimal

.. autofunction:: convert_iso_date

.. autofunction:: convert_iso_datetime

.. autofunction:: convert_iso_time

.. autofunction:: convert_json

.. autofunction:: convert_uuid

.. autofunction:: format_datetime_rfc3339

.. autofunction:: parse_datetime_rfc3339
