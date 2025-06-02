## [REF-011] SQLStatement & StatementPipeline: Unified SQL Processing

**DECISION**: Implement a cohesive SQL processing system where `SQLStatement` (`sqlspec.statement.sql.SQL`) is the central immutable object representing a query and its state. Processing (transformation, validation, analysis) is delegated to a `StatementPipeline` (`sqlspec.statement.pipelines.StatementPipeline`) which operates on a shared `SQLProcessingContext`.

**ARCHITECTURE OVERVIEW**:

1. **`SQL` Object (The "What")**:
    * Represents a specific SQL statement, its parameters, dialect, and configuration (`SQLConfig`).
    * Immutable: Methods like `.where()`, `.limit()`, `.transform()`, `.copy()` return *new* `SQL` instances.
    * Upon instantiation (`__init__`), it prepares an `SQLProcessingContext`.
    * It then invokes the `StatementPipeline` to process this context.
    * Finally, it populates its internal state (parsed expression, validation results, analysis results, final parameters) from the `StatementPipelineResult`.

2. **`SQLConfig` (The "How-To Customize")**:
    * Controls all aspects of processing: parsing, transformation, validation, analysis enablement.
    * Defines which processor components (transformers, validators, analyzers) are part of the pipeline, allowing for distinct lists for each stage.
    * Includes a flag `input_sql_had_placeholders`, determined by `SQL.__init__`, to inform transformers like `ParameterizeLiterals`.

3. **`SQLProcessingContext` (The "Shared Workspace")**:
    * A dataclass (`sqlspec.statement.pipelines.context.SQLProcessingContext`) passed through the pipeline stages.
    * Holds mutable state during a single pipeline run:
        * `initial_sql_string`, `dialect`, `config` (from `SQL` object).
        * `initial_parameters`, `initial_kwargs`, `merged_parameters`, `parameter_info` (from `SQL` object's parameter processing).
        * `current_expression`: The `sqlglot.exp.Expression`, potentially modified by transformers.
        * `extracted_parameters_from_pipeline`: Parameters extracted by transformers.
        * `validation_result: Optional[ValidationResult]`: Populated by the validation stage.
        * `analysis_result: Optional[StatementAnalysis]`: Populated by the analysis stage.
        * `input_sql_had_placeholders`: Copied from `SQLConfig`.
        * `statement_type`: (Future) Could be populated by an early analysis step.

4. **`StatementPipeline` (The "Orchestrator")**:
    * Defined in `sqlspec.statement.pipelines.base.StatementPipeline`.
    * Its `execute_pipeline(context: SQLProcessingContext)` method orchestrates the stages:
        * **Parsing (Implicit/Initial)**: Ensures `context.current_expression` is populated from `context.initial_sql_string` if not already an expression (respecting `context.config.enable_parsing`).
        * **Transformation Stage**: Iterates through configured transformers. Each transformer receives the `context`, can modify `context.current_expression` and add to `context.extracted_parameters_from_pipeline`.
        * **Validation Stage**: Iterates through configured validators. Each receives `context`, performs checks on `context.current_expression`, and contributes to an aggregated `ValidationResult` which is then stored in `context.validation_result`.
        * **Analysis Stage**: Iterates through configured analyzers. Each receives `context` (including `context.validation_result`), performs analysis on `context.current_expression`, and the primary analyzer sets `context.analysis_result`.
    * Returns a `StatementPipelineResult` dataclass containing the final state from the context.

5. **`ProcessorProtocol` (The "Component Contract")**:
    * Base protocol (`sqlspec.statement.pipelines.base.ProcessorProtocol`) for all transformers, validators, and analyzers.
    * Defines `process(self, context: SQLProcessingContext) -> tuple[exp.Expression, Optional[ValidationResult]]`.
        * Concrete implementations adapt this: transformers usually update `context.current_expression` and return `(context.current_expression, None)`. Validators return `(context.current_expression, ValidationResult_part)`. Analyzers update `context.analysis_result` and return `(context.current_expression, None)`.

6. **`StatementPipelineResult` (The "Outcome")**:
    * A dataclass (`sqlspec.statement.pipelines.context.StatementPipelineResult`) bundling the final outputs of a pipeline run, which the `SQL` object uses to set its state.

**USER BENEFIT & KEY DESIGN PRINCIPLES**:

* **Parse Once, Process Many Ways**: The SQL string is parsed into a `sqlglot` expression once (if parsing is enabled). This expression (or its transformed versions) is then passed through validation and analysis stages. This is efficient.
* **Clear Data Flow**: `SQLProcessingContext` makes the data available to each processing stage explicit, reducing side effects and making the pipeline easier to reason about.
* **Extensibility**: New transformers, validators, or analyzers can be created by implementing `ProcessorProtocol` and added to `SQLConfig`.
* **Configurability**: Users can precisely control each stage (enable/disable, provide custom components) via `SQLConfig`.
* **Improved Testability**: Individual processors can be tested by mocking the `SQLProcessingContext`.
* **Separation of Concerns**:
    * `SQL` object: User-facing API and final state holder.
    * `SQLConfig`: Defines processing rules.
    * `SQLProcessingContext`: Transient state during a single processing run.
    * `StatementPipeline`: Orchestrates the run.
    * Processors: Implement specific logic for transformation, validation, or analysis.
* **Robust Parameter Handling**: The system distinguishes between parameters provided initially to the `SQL` object and those extracted by transformers (e.g., `ParameterizeLiterals`), merging them correctly.
* **Informed Analysis**: The analysis stage can leverage results from the validation stage (e.g., a cartesian product validator can provide data that an analyzer then reports), promoting synergy between stages.

**EXAMPLE PIPELINE EXECUTION FLOW (Conceptual)**:

```python
# 1. User creates SQL object
# config = SQLConfig(transformers=[T1, T2], validators=[V1], analyzers=[A1])
# sql_obj = SQL("SELECT * FROM data WHERE id = 1", config=my_config)

# 2. SQL.__init__ -> SQL._initialize_statement:
#    - Creates SQLProcessingContext (ctx)
#    - ctx.initial_sql_string = "SELECT * FROM data WHERE id = 1"
#    - Determines ctx.input_sql_had_placeholders = False
#    - Processes initial parameters (none here) -> ctx.merged_parameters = []
#    - Parses SQL -> ctx.current_expression = sqlglot.parse_one(...)
#    - Calls pipeline = self.config.get_statement_pipeline()
#    - pipeline_result = pipeline.execute_pipeline(ctx)

# 3. StatementPipeline.execute_pipeline(ctx):
#    - Stage 0: Parsing (already done by SQL._initialize_statement, or done here if ctx.current_expression is None)
#    - Stage 1: Transformers
#        - T1.process(ctx) -> updates ctx.current_expression, maybe ctx.extracted_parameters_from_pipeline
#        - T2.process(ctx) -> updates ctx.current_expression, maybe ctx.extracted_parameters_from_pipeline
#    - Stage 2: Validators
#        - V1.process(ctx) -> returns (ctx.current_expression, v1_result). Pipeline aggregates into ctx.validation_result.
#    - Stage 3: Analyzers
#        - A1.process(ctx) -> updates ctx.analysis_result.
#    - Returns StatementPipelineResult (with final ctx.current_expression, ctx.validation_result, etc.)

# 4. SQL._initialize_statement (continues):
#    - self._parsed_expression = pipeline_result.final_expression
#    - self._validation_result = pipeline_result.validation_result
#    - self._analysis_result = pipeline_result.analysis_result
#    - self._merge_extracted_parameters(ctx.extracted_parameters_from_pipeline)
#    - self._check_and_raise_for_strict_mode()

# 5. User can now access results:
#    print(sql_obj.sql) # Potentially transformed SQL
#    print(sql_obj.parameters) # Final merged parameters
#    print(sql_obj.validation_result)
#    print(sql_obj.analysis_result)
```

**KEY POINTS FOR DOCS**:

* Emphasize the "Parse Once, Process Many Ways" philosophy.
* Explain the roles of `SQL`, `SQLConfig`, `SQLProcessingContext`, `StatementPipeline`, and `ProcessorProtocol`.
* Highlight how `SQLConfig` allows fine-grained control over the pipeline.
* Detail how information (like `input_sql_had_placeholders` or `validation_result`) flows via the `SQLProcessingContext` to inform later stages.
* Show how to create and plug in custom processors.
* Explain the benefits for security (e.g., `ParameterizeLiterals` informed by context), performance (cached parsing, efficient data flow), and extensibility.

---
