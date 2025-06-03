---

## Section X: Optimizing `SQL` Class Internals for "Process Once" Workflow

**(Assumes breaking changes to *private* methods and internal state management of `sqlspec.statement.sql.SQL` are permissible. Public API and behavior must remain consistent.)**

**Current Challenge:**
The `SQL` class in `sqlspec.statement.sql.py` currently handles parsing, parameter processing, transformations, validation, and SQL generation. While it has caching mechanisms, the flow can be complex, especially with method calls like `copy()`, `append_filter()`, or when different configurations are applied. This can lead to implicit or explicit re-processing of the statement, potentially multiple times, and makes the internal state management intricate.

**Goal:**
Refactor the internal workings of the `SQL` class to implement a clear "process once" model. An SQL statement, upon its first relevant property access or method call that requires a processed form (e.g., `.expression`, `.to_sql()`), will undergo a single, well-defined pipeline. The results of this pipeline (final expression, parameters, validation status, etc.) will be robustly cached. Subsequent accesses will use this cached state unless a change invalidates it.

---

### X.1. Core Internal Design Changes

1. **Unified `ProcessedState` Cache:**
    * **Concept:** Introduce an internal (private) dataclass or similar structure, say `_ProcessedState`, to hold all artifacts resulting from the full processing pipeline.
    * **Contents:**
        * `raw_sql_input: str` (The initial SQL string used for this processing cycle)
        * `raw_parameters_input: SQLParameterType`
        * `initial_expression: Optional[exp.Expression]` (Result of `SQL.to_expression()` if parsing enabled)
        * `transformed_expression: Optional[exp.Expression]` (Expression after all transformers in the pipeline)
        * `final_parameter_info: list[ParameterInfo]`
        * `final_merged_parameters: SQLParameterType` (Parameters after all processing, including literal parameterization)
        * `validation_result: Optional[ValidationResult]`
        * `analysis_result: Optional[StatementAnalysis]`
        * `input_had_placeholders: bool` (Flag from initial parsing)
        * `config_snapshot: SQLConfig` (A copy of the config used for this processing, for validation)
    * The `SQL` instance will hold an optional instance of this: `self._processed_state: Optional[_ProcessedState] = None`.

2. **Lazy, Memoized Processing Method (`_ensure_processed()`):**
    * **Concept:** A central private method, e.g., `_ensure_processed()`, will be the gatekeeper for accessing processed data.
    * **Logic:**
        1. If `self._processed_state` exists and is still valid (e.g., config hasn't changed in a way that invalidates it), return it.
        2. If not, and not already processing (re-entrancy guard `self._is_processing`), execute the full pipeline:
            * **Stage 0: Input Preparation:**
                * Determine the SQL string to process from `self._sql` (which could be a string, `exp.Expression`, or another `SQL` object).
                * Store initial parameters/kwargs.
            * **Stage 1: Initial Parsing (if `config.enable_parsing`):**
                * Call static `SQL.to_expression(raw_sql_string, self._dialect, self._is_script)` to get `initial_expression`.
                * Determine `input_had_placeholders`.
            * **Stage 2: Parameter Pre-processing:**
                * Refactor `_process_parameters` to take the `initial_expression` (if available) or the raw SQL string. This stage resolves the initial set of `ParameterInfo` and `merged_parameters` based on the *original* SQL structure before transformations like `ParameterizeLiterals`.
            * **Stage 3: Pipeline Execution (if `config.enable_parsing` and `initial_expression` exists):**
                * Construct `SQLProcessingContext` using `initial_expression`, pre-processed parameters, and other relevant data.
                * Get the `StatementPipeline` from `self._config.get_statement_pipeline()`.
                * Execute `pipeline.execute_pipeline(context)`.
                * This yields `transformed_expression`, `validation_result`, `analysis_result`, and potentially updated `merged_parameters` and `parameter_info` (if `ParameterizeLiterals` ran).
            * **Stage 4: Fallback Validation (if parsing disabled but validation enabled):**
                * Perform basic string-based validation if applicable.
            * **Stage 5: Cache Results:** Populate `self._processed_state` with all artifacts.
            * **Stage 6: Strict Mode Check:** Apply strict mode based on `validation_result`.
        3. Return `self._processed_state`.

3. **Streamlined Initialization (`__init__`):**
    * `__init__` will primarily store the raw inputs (`statement`, `parameters`, `filters`, `dialect`, `config`, `kwargs`, etc.) as internal attributes (e.g., `self._raw_statement`, `self._raw_parameters`).
    * It will initialize `self._processed_state = None` (unless a valid state is passed internally by `copy()`).
    * It will *not* immediately trigger the full processing pipeline.

4. **Refined Parameter Handling:**
    * The logic from `_process_parameters` and `_merge_extracted_parameters` will be integrated into the `_ensure_processed()` flow.
    * Parameter extraction from the *original* SQL (to understand its placeholder style) should occur early.
    * Merging of parameters extracted by transformers (like `ParameterizeLiterals`) will happen within the pipeline execution stage of `_ensure_processed()`, resulting in `final_merged_parameters` and `final_parameter_info` in the `_ProcessedState`.

---

### X.2. Impact on Existing Methods and Properties

1. **Property Accessors (`.sql`, `.expression`, `.parameters`, `.validation_result`, etc.):**
    * These will all call `self._ensure_processed()` and then return the relevant attribute from the cached `_ProcessedState` object.
    * Example: `self.expression` -> `return self._ensure_processed().transformed_expression`

2. **`to_sql()` Method:**
    * Will call `self._ensure_processed()`.
    * It will then use the `_ProcessedState.transformed_expression` and `_ProcessedState.final_merged_parameters` as the definitive inputs for SQL string generation.
    * The logic for handling scripts (iterating sub-expressions for placeholder transformation) will operate on `transformed_expression` (if it's a `Command("SCRIPT")`).
    * The methods `_transform_sql_placeholders`, `_convert_placeholder_style`, and `_render_static_sql` will be simplified to work with a definitive `exp.Expression` and fully resolved parameters, without needing to re-parse or re-evaluate parameter styles from raw strings.

3. **`_apply_filters(filters_to_apply)`:**
    * Will perform its current logic to create a new combined `exp.Expression` from the current statement and the applied filters.
    * Crucially, after updating `self._sql` (or `self._raw_statement`) to this new expression, it *must* set `self._processed_state = None`.
    * This invalidation ensures that the next call to any property or method requiring processed data will trigger `_ensure_processed()` to re-run the entire pipeline on the new, filtered expression.

4. **`copy()` Method:**
    * When `copy()` is called, it needs to decide if the new instance can reuse the `_processed_state` or if reprocessing is needed.
    * **If `statement`, `parameters`, or `kwargs` (that affect parameter merging) are provided to `copy()`:** The new `SQL` instance should be created with `_processed_state = None` (or simply not pass any internal state), forcing it to reprocess.
    * **If only `config`, `dialect`, or `filters` are changed:**
        * If `config` changes in a way that alters the processing pipeline (e.g., `enable_parsing` toggled, different transformers/validators), the new instance must reprocess (`_processed_state = None`).
        * If only `dialect` changes, the `_processed_state.transformed_expression` might still be valid, but `to_sql()` rendering will differ. The `_ensure_processed()` logic might need a way to just re-render if only dialect changes for `to_sql()`, but for simplicity, full reprocessing on config/dialect change in copy might be safer initially.
        * If `filters` are applied via `copy(*filters)`, `_apply_filters` will handle the invalidation as above.
    * **Optimization:** If `copy()` is called with no arguments that would change the processed SQL or parameters, and the config is identical, it *could* pass the existing `self._processed_state` to the new instance for efficiency.

5. **`as_many()` and `as_script()`:**
    * `as_script()`: Should ensure `self._is_script = True` and then invalidate `self._processed_state` to ensure `SQL.to_expression` is called with `is_script=True` during the next `_ensure_processed()` call.
    * `as_many()`: This method has special behavior (disabling validation/parsing for the `as_many` call). It should create a new `SQL` instance with a config tailored for `executemany` and set `self._is_many = True`. The `parameters` for `as_many` are handled differently, often at the driver execution stage. The `SQL` object might simply hold these batch parameters without deeply processing them in its own pipeline.

---

### X.3. Benefits of This Refactor

* **Clarity:** A single, well-defined data flow for processing SQL statements.
* **Correctness:** Reduces the risk of inconsistent state or applying transformations/validations multiple times or out of order.
* **Performance:** Avoids redundant computations by robustly caching the full processing result.
* **Maintainability:** Simplifies debugging and future enhancements as the processing logic is centralized.
* **Adherence to "Process Once":** The core goal is achieved for the primary lifecycle of an `SQL` object.

---

### X.4. Detailed TODOs for `SQL` Class Internal Refactor

1. **Design `_ProcessedState`:** Define the internal dataclass/structure to hold all processing artifacts.
2. **Implement `_ensure_processed()`:**
    * Develop the core logic with stages (Input Prep, Parsing, Parameter Pre-processing, Pipeline Execution, Fallback Validation, Caching, Strict Mode).
    * Include re-entrancy guard (`self._is_processing`).
    * Implement robust cache validity checks (e.g., based on `id(self._config)` or a hash of relevant config parts, though `id` is simpler if config objects are treated as immutable after association with an SQL object).
3. **Refactor `__init__`:** Shift processing logic to `_ensure_processed()`. `__init__` stores raw inputs and initializes `self._processed_state = None`.
4. **Integrate Parameter Processing:**
    * Adapt `_process_parameters` (perhaps rename to `_internal_resolve_initial_parameters`) to work within `_ensure_processed()` using the `initial_expression` or raw SQL.
    * Ensure `ParameterizeLiterals` (and any other transformers modifying parameters) correctly updates parameter info and values, which are then stored in `_ProcessedState`. Remove `_merge_extracted_parameters` as its logic moves into the pipeline result handling.
5. **Update Property Accessors:** Modify all properties (`.sql`, `.expression`, `.parameters`, etc.) to use `self._ensure_processed().attribute_name`.
6. **Revamp `to_sql()`:**
    * Make it rely solely on `_ProcessedState.transformed_expression` and `_ProcessedState.final_merged_parameters`.
    * Clean up script handling logic to operate on the `Command("SCRIPT")` expression from the processed state.
    * Simplify `_transform_sql_placeholders`, `_convert_placeholder_style`, `_render_static_sql` to be pure rendering functions based on the definitive expression and parameters.
7. **Modify `_apply_filters()`:** Ensure it updates `self._raw_statement` (or equivalent) and sets `self._processed_state = None`.
8. **Refine `copy()`:** Implement logic to determine when `_processed_state` can be passed to the new instance versus when it must be cleared (forcing reprocessing). Consider making `SQLConfig` hashable or providing a method to get a "processing signature" for easier comparison.
9. **Adjust `as_many()` and `as_script()`:** Ensure they correctly set flags and manage `_processed_state` invalidation/repopulation.
10. **Testing:**
    * Write extensive unit tests for `_ensure_processed()` covering all stages and configurations.
    * Verify that existing functional tests for `SQL` behavior (public API) continue to pass.
    * Add tests specifically for caching behavior, filter application, and copy scenarios.

This internal refactor is a significant undertaking but promises a much more robust and efficient `SQL` class, which is central to `sqlspec`.

---

## Section XI: General Code Quality and Minor Cleanup TODOs (Across `sqlspec.statement`)

**(Assumes breaking changes to *private* methods and internal state management are permissible. Public API and behavior must remain consistent.)**

This section lists various smaller opportunities for code cleanup and quality improvements within the `sqlspec.statement` module and its submodules. These are generally independent of the larger `SQL` class or pipeline refactors but contribute to overall maintainability.

1. **Review and Consolidate Caching Logic in `SQL` Class:**
    * **Current:** `_cached_sql_string`, `_invalidate_sql_cache()`, `_invalidate_validation_cache()`.
    * **Action:** With the `_ProcessedState` model, these specific caches and invalidation methods become obsolete. Ensure all caching relies solely on `_ProcessedState` and its invalidation. Remove the old private attributes and methods.

2. **Simplify Parameter Conversion Utilities in `SQL` Class:**
    * **Current:** `_convert_to_dict_parameters()`, `_convert_to_list_parameters()`.
    * **Action:** These methods currently work with `self._merged_parameters` and `self._parameter_info`. Once `_ProcessedState` holds the definitive `final_merged_parameters` and `final_parameter_info`, review if these conversion utilities can be simplified or made static if they only operate on their inputs. Ensure they are used consistently after `_ensure_processed()` has been called.

3. **Standardize Expression Acquisition for Modifying Methods in `SQL` Class:**
    * **Current:** Methods like `where()`, `limit()`, `offset()`, `order_by()` call `_get_current_expression_for_modification()`.
    * **Action:** Ensure these methods consistently get their base expression from `self._ensure_processed().transformed_expression.copy()`. The logic in `_get_current_expression_for_modification()` that defaults to `exp.Select()` if `self.expression is None` should be effectively handled by `_ensure_processed()` which guarantees a valid `transformed_expression` (or raises an error if parsing fails). Remove `_get_current_expression_for_modification()` if redundant.

4. **Reduce Logging Verbosity/Redundancy:**
    * **Action:** Review all `logger.debug`, `logger.info`, `logger.warning` calls within `sqlspec.statement.sql.SQL` and related private methods. With a clearer "process once" flow and better state management, some diagnostic logging might become less critical or could be consolidated. Ensure warnings are actionable and errors are descriptive.

5. **Docstrings and Type Hinting:**
    * **Action:** While the codebase is generally well-typed, perform a pass on all refactored private methods and new internal classes (like `_ProcessedState`) to ensure clear docstrings explaining their role and precise type hints.
    * **TODO:** Specifically review `sqlspec.driver.py` for any `pyright: ignore` comments that might be resolvable with more precise typing or small refactors, especially concerning `RowT`, `ModelDTOT`, and generic interactions.

6. **Private Method Review (`_` prefix):**
    * **Action:** After major refactoring, review all methods prefixed with a single underscore (`_`) in `SQL` and related classes. Ensure they are genuinely intended for internal use. If any have become de facto utility functions that could be static or moved to a utils module (and are not tied to instance state), consider refactoring.

7. **Consistency in `SQLConfig` Usage:**
    * **Action:** Ensure that `SQLConfig` instances are treated as immutable once associated with an `SQL` object's processing cycle, or that changes robustly trigger reprocessing. The `config_snapshot` in `_ProcessedState` aids this.

8. **Review `SQL.to_expression` Static Method:**
    * **Current:** Handles parsing of strings, `SQL` objects, and `exp.Expression` inputs.
    * **Action:** This method is critical. Ensure its logic for auto-detecting scripts and handling different input types remains robust and is well-tested, especially given its role in the initial parsing stage of `_ensure_processed()`.

9. **Examine `sqlspec.statement.filters.py`:**
    * **TODO:** Review the `StatementFilter` protocol and its application in `SQL._apply_filters`. Ensure the interaction with the new `_ProcessedState` invalidation is clean and efficient. Consider if filters could also be integrated more directly into the `StatementPipeline` concept, perhaps as a specific type of transformer, if that simplifies the overall flow, though the current separation is also logical.

10. **Code Style and Pythonic Idioms:**
    * **Action:** After structural changes, run linters/formatters (like Ruff, Black if used in the project) to ensure consistency. Look for opportunities to use more Pythonic idioms where clarity is improved (e.g., comprehensions, generator expressions, `isinstance` checks). (Ref: [SQLFluff - The SQL Linter for humans](https://sqlfluff.com/) for general linting ideas, though this is for Python code).

These pipeline enhancements, combined with the `SQL` class internal refactor, would lead to a highly robust, efficient, and maintainable SQL processing system within `sqlspec`.

---

## Section XII: Pipeline Architecture Enhancements (for `sqlspec.statement.pipelines`)

**(Building upon the "Process Once" model in the `SQL` class)**

The refactoring of the `SQL` class to a "process once" model provides an opportunity to enhance the `sqlspec.statement.pipelines` architecture for better clarity, configurability, and robustness.

1. **Declarative Pipeline Definition in `SQLConfig`:**
    * **Current:** `SQLConfig` holds lists of instantiated processor objects (e.g., `transformers: Optional[list[ProcessorProtocol[exp.Expression]]]`).
    * **Proposed Enhancement:** Allow `SQLConfig` to alternatively define pipelines using lists of processor *classes* or unique string identifiers/names. The `StatementPipeline` (or `SQLConfig.get_statement_pipeline()`) would then be responsible for instantiating these processors.
    * **Benefits:**
        * More declarative configuration (easier to manage in settings files or code).
        * Reduces risk of unintentional state sharing if processor instances were inadvertently reused across different `SQLConfig` objects (though processors should ideally be stateless).
        * Easier to conditionally enable/disable specific processors by name/class.
    * **TODO:** Design how processor classes/names would be mapped to actual implementations and how their own configurations (if any) would be passed.

2. **Refined `SQLProcessingContext` Management:**
    * **Current:** `SQLProcessingContext` is mutable; processors modify attributes like `current_expression` or `merged_parameters` in place.
    * **Proposed Enhancement (Option A - Full Immutability):** Make `SQLProcessingContext` immutable. Each processor takes a context and returns a *new* context instance (or a tuple of the modified parts, e.g., `(new_expression, new_parameters)`). `StatementPipeline` would thread the context through. This is a significant change.
    * **Proposed Enhancement (Option B - Controlled Mutation with Clear Output):** If full immutability is too disruptive, ensure processors clearly document what parts of the context they modify and what their explicit outputs are. `StatementPipeline.execute_pipeline` would return a more structured object encapsulating all key outputs rather than just modifying the input context and returning a simple result.
    * **Benefits:** Improved predictability, easier debugging, facilitates understanding data flow, potentially safer for concurrency if ever needed.
    * **TODO:** Evaluate the trade-offs. Option B might be more pragmatic. The goal is that the `_ensure_processed` method in `SQL` receives a clear, consolidated set of results from the pipeline run.

3. **Granular Control & Conditional Execution of Processors:**
    * **Current:** Processors run based on `enable_parsing`, `enable_validation`, etc., and the lists in `SQLConfig`.
    * **Proposed Enhancement:** Introduce a mechanism for processors to declare prerequisites or for the pipeline to conditionally skip processors based on the state of the `SQLProcessingContext` (e.g., skip a complex validator if a basic syntax error was already found by an earlier, cheaper one).
    * Allow `SQLConfig` to more finely specify *which* individual processors (from the defaults or custom lists) should run or be excluded for a particular `SQL` object. (Ref: Concept of breaking down complex tasks, similar to [How to turn a 1000-line messy SQL into a modular, & easy-to-maintain data pipeline? - StartDataEngineering](https://www.startdataengineering.com/post/quick-scalable-business-value-pipeline/)).
    * **Benefits:** More efficient processing, more flexible customization.
    * **TODO:** Design how conditional execution would be specified (e.g., processor metadata, pipeline configuration).

4. **Standardized Richer Results from Pipeline Stages:**
    * **Current:** `ValidationResult` captures validation issues. Transformers modify the expression/parameters. Analyzers produce `StatementAnalysis`.
    * **Proposed Enhancement:** `StatementPipeline.execute_pipeline` should return a comprehensive result object that encapsulates:
        * The final transformed expression.
        * The final merged parameters and parameter info.
        * A consolidated list/dictionary of all issues (errors, warnings, info messages) from *all* processors (validators, transformers, analyzers), perhaps categorized by severity or processor type.
        * The analysis result.
    * This structured result would directly populate the `_ProcessedState` in the `SQL` class.
    * **Benefits:** Centralized reporting, easier for `SQL` class to consume pipeline outputs.
    * **TODO:** Design this comprehensive pipeline result object.

5. **Early Exit / Short-Circuiting in `StatementPipeline`:**
    * **Current:** `SQLConfig.strict_mode` causes exceptions to be raised, which stops processing.
    * **Proposed Enhancement:** Allow the `StatementPipeline` to optionally short-circuit if a processor (especially a validator) reports a critical, unrecoverable error, even if `strict_mode` is off. This would prevent running subsequent, potentially costly or irrelevant, processors. The pipeline result would indicate this early exit.
    * **Benefits:** Performance improvement in error cases.
    * **TODO:** Define criteria for "critical, unrecoverable error" and how processors signal this.

6. **Review and Simplify `ProcessorProtocol`:**
    * **Current:** `ProcessorProtocol` defines `process(context: SQLProcessingContext) -> None` (for transformers typically modifying context in-place) or methods like `validate(...)` and `analyze(...)`.
    * **Proposed Enhancement:** If moving towards more functional processors (see point 2), the protocol might change to `process(context: SQLProcessingContext) -> NewContextOrRelevantOutput`. Ensure clarity on whether a processor is a transformer, validator, or analyzer, possibly through distinct protocols inheriting from a base, or through metadata.
    * **Benefits:** Clearer contracts for processors.
    * **TODO:** Evaluate the current `ProcessorProtocol` against the desired data flow.

7. **Testability of Individual Processors:**
    * **Action:** Ensure that the design of `SQLProcessingContext` and `ProcessorProtocol` facilitates easy unit testing of individual pipeline components in isolation. The "modular SQL" article's emphasis on testing small units is applicable here.
    * **TODO:** Review existing processor tests and identify any difficulties stemming from complex context setup.
