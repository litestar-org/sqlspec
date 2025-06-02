## [REF-012] Deprecated - Unified Pipeline Architecture (Old)

This section is now superseded by [REF-011: SQLStatement & StatementPipeline: Unified SQL Processing](./011_sqlstatement_pipeline.md) which details the `SQLProcessingContext` and staged `StatementPipeline` approach.

The `UnifiedProcessor` concept, while aiming for similar goals of efficient, multi-stage processing, has been refined into the more explicit staged pipeline managed by `StatementPipeline` and orchestrated by the `SQL` object through `SQLProcessingContext`.

**REASON FOR DEPRECATION**: The new model with `SQLProcessingContext` offers:

- **Clearer Data Flow**: Explicit context object for inter-stage communication.
- **Improved Stage Separation**: Distinct transformation, validation, and analysis stages are more clearly defined and managed by `StatementPipeline`.
- **Enhanced Extensibility**: Adding new processors to specific stages is more straightforward via `SQLConfig`.
- **Better Adherence to "Process Once" Principles**: Each *type* of operation (transform, validate, analyze) is more clearly delineated as a single phase operating on the evolving state held in the context.

---
