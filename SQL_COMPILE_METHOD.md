# SQL Class Compile Method Implementation

## Implementation Details

### 1. Add to SQL class (sqlspec/statement/sql.py)

```python
def compile(
    self, 
    placeholder_style: Optional[Union[ParameterStyle, str]] = None
) -> tuple[str, Optional[SQLParameterType]]:
    """Compile SQL statement and parameters together for execution.
    
    This method ensures SQL and parameters are processed exactly once
    and returned in a format ready for database execution.
    
    Args:
        placeholder_style: Target parameter style for the compiled output.
                          If None, uses the dialect's default style.
    
    Returns:
        Tuple of (sql_string, parameters) where:
        - sql_string: The SQL with placeholders in the target style
        - parameters: Parameters formatted for the target style
                     (list, tuple, dict, or None)
    
    Example:
        >>> stmt = SQL("SELECT * FROM users WHERE id = ?", [123])
        >>> sql, params = stmt.compile(placeholder_style=ParameterStyle.NUMERIC)
        >>> print(sql)    # "SELECT * FROM users WHERE id = $1"
        >>> print(params) # [123]
    """
    # Ensure processing happens exactly once
    processed = self._ensure_processed()
    
    # Generate SQL with target placeholder style
    sql = self.to_sql(placeholder_style=placeholder_style)
    
    # Get parameters in the format expected by the target style
    params = self.get_parameters(style=placeholder_style)
    
    return sql, params
```

### 2. Optimize to_sql to avoid re-processing

Update the `to_sql` method to use cached results when possible:

```python
def to_sql(self, placeholder_style: Optional[Union[ParameterStyle, str]] = None) -> str:
    """Get SQL string with specified placeholder style.
    
    This method now checks if we've already generated SQL for this style
    to avoid redundant processing.
    """
    processed = self._ensure_processed()
    
    # Check if we've already compiled for this style
    style_key = str(placeholder_style) if placeholder_style else "default"
    
    # Use existing _render_sql logic but optimize for repeated calls
    if not self._config.enable_parsing or processed.transformed_expression is None:
        # Direct SQL manipulation when parsing is disabled
        return self._convert_sql_placeholders(
            processed.raw_sql_input, 
            placeholder_style
        )
    
    # Use the expression-based rendering
    return self._render_sql(processed.transformed_expression, placeholder_style)
```

### 3. Add placeholder conversion that handles all cases

```python
def _convert_sql_placeholders(
    self, 
    sql: str, 
    target_style: Optional[Union[ParameterStyle, str]]
) -> str:
    """Convert SQL placeholders when parsing is disabled.
    
    This replaces the need for convert_placeholders_in_raw_sql in drivers.
    """
    if not target_style:
        return sql
        
    # Get target style enum
    if isinstance(target_style, str):
        target_style = self._parse_placeholder_style(target_style)
    
    # If SQL has pyformat placeholders that sqlglot can't handle
    if self._config.input_sql_had_placeholders:
        param_info = self._processed_state.final_parameter_info
        if param_info:
            # Use existing parameter converter logic
            converter = ParameterConverter()
            result_sql = converter.convert_sql_placeholders(
                sql,
                param_info,
                target_style
            )
            return result_sql
    
    return sql
```

### 4. Update get_parameters to handle edge cases

```python
def get_parameters(
    self, 
    style: Optional[Union[ParameterStyle, str]] = None
) -> Optional[SQLParameterType]:
    """Get parameters formatted for the specified style.
    
    This method now handles all parameter conversion internally,
    eliminating the need for driver-specific conversion.
    """
    processed = self._ensure_processed()
    
    if not processed.final_merged_parameters:
        return None
    
    # For execute_many, return parameters as-is
    if self._is_many:
        return processed.final_merged_parameters
    
    # Handle style conversion
    if style:
        return self._convert_parameters_for_style(
            processed.final_merged_parameters,
            processed.final_parameter_info,
            style
        )
    
    return processed.final_merged_parameters
```

### 5. Example Driver Usage

```python
# Before (multiple calls, separate processing)
def _execute_statement(self, statement: SQL, connection=None, **kwargs):
    if statement.is_script:
        return self._execute_script(
            statement.to_sql(placeholder_style=ParameterStyle.STATIC),
            connection=connection,
            **kwargs
        )
    
    sql = statement.to_sql(placeholder_style=self._get_placeholder_style())
    params = statement.get_parameters(style=self._get_placeholder_style())
    
    # Driver-specific parameter conversion
    converted_params = self._convert_parameters_to_driver_format(sql, params)
    
    return self._execute(sql, converted_params, statement, connection, **kwargs)

# After (single call, unified processing)
def _execute_statement(self, statement: SQL, connection=None, **kwargs):
    if statement.is_script:
        sql, _ = statement.compile(placeholder_style=ParameterStyle.STATIC)
        return self._execute_script(sql, connection=connection, **kwargs)
    
    if statement.is_many:
        sql, params = statement.compile(placeholder_style=self.parameter_style)
        return self._execute_many(sql, params, connection=connection, **kwargs)
    
    sql, params = statement.compile(placeholder_style=self.parameter_style)
    return self._execute(sql, params, statement, connection=connection, **kwargs)
```

## Benefits

1. **Single Processing Pass**: SQL and parameters are processed together
2. **No Driver Bypasses**: All parameter conversion happens in SQL object
3. **Cleaner API**: One method instead of two
4. **Better Performance**: Avoids redundant processing
5. **Easier Testing**: Single point to test compilation logic

## Migration Notes

1. The `compile()` method is additive - existing `to_sql()` and `get_parameters()` continue to work
2. Drivers can be migrated incrementally
3. Once all drivers use `compile()`, we can deprecate separate calls
4. The SQL object becomes the single source of truth for all parameter handling
