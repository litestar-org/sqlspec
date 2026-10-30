# Parameters & Coercion

```python
from sqlspec.core import (
    BaseInputConverter,
    DriverParameterProfile,
    ParameterConverter,
    ParameterDeclaration,
    ParameterInfo,
    ParameterProcessingResult,
    ParameterProcessor,
    ParameterProfile,
    ParameterStyle,
    ParameterStyleConfig,
    ParameterValidator,
    ParamTypeMatcher,
    TypeCoercionCapabilities,
    TypedParameter,
    get_driver_profile,
    register_driver_profile,
    register_param_type,
    wrap_with_type,
)
```

Parameter extraction, validation, placeholder style rewriting (`?`, `$1`, `:name`, `@name`, `%(name)s`), and runtime type coercion. See [Parameters & Bindings](../../learn/parameters.md) for usage examples.

## Parameter Styles & Profiles

::: sqlspec.core.parameters.ParameterStyle
    options:
      heading_level: 3
      members: false

::: sqlspec.core.parameters.ParameterStyleConfig
    options:
      heading_level: 3
      members:
        - default_parameter_style
        - supported_parameter_styles
        - default_execution_parameter_style
        - supported_execution_parameter_styles
        - type_coercion_map
        - has_native_list_expansion
        - replace

::: sqlspec.core.parameters.DriverParameterProfile
    options:
      heading_level: 3
      members: false

::: sqlspec.core.parameters.ParameterProfile
    options:
      heading_level: 3
      members: false

::: sqlspec.core.capabilities.TypeCoercionCapabilities
    options:
      heading_level: 3
      members: false

## Processing, Validation & Conversion

::: sqlspec.core.parameters.ParameterProcessor
    options:
      heading_level: 3
      members:
        - process
        - process_for_execution
        - clear_cache
        - cache_stats

::: sqlspec.core.parameters.ParameterConverter
    options:
      heading_level: 3
      members:
        - convert_placeholder_style

::: sqlspec.core.parameters.ParameterValidator
    options:
      heading_level: 3
      members:
        - extract_parameters

::: sqlspec.core.parameters.ParameterProcessingResult
    options:
      heading_level: 4
      members: false

::: sqlspec.core.parameters.ParameterInfo
    options:
      heading_level: 4
      members: false

::: sqlspec.core.parameters.ParameterDeclaration
    options:
      heading_level: 4
      members: false

## Typed Parameters & Input Converters

::: sqlspec.core.parameters.TypedParameter
    options:
      heading_level: 3
      members: false

::: sqlspec.core.parameters.ParamTypeMatcher
    options:
      heading_level: 4
      members: false

::: sqlspec.core.parameters.wrap_with_type
    options:
      heading_level: 4

::: sqlspec.core.parameters.register_param_type
    options:
      heading_level: 4

::: sqlspec.core.parameters.get_driver_profile
    options:
      heading_level: 4

::: sqlspec.core.parameters.register_driver_profile
    options:
      heading_level: 4

::: sqlspec.core.type_converter.BaseInputConverter
    options:
      heading_level: 3
      members: false
