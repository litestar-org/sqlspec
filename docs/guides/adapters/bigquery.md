# Google BigQuery Adapter Guide

This guide provides specific instructions for the `bigquery` adapter.

## Key Information

-   **Driver:** `google-cloud-bigquery`
-   **Parameter Style:** `named` (e.g., `@name`)

## Best Practices

-   **Authentication:** BigQuery requires authentication with Google Cloud. For local development, the easiest way is to use the Google Cloud CLI and run `gcloud auth application-default login`.
-   **Project and Dataset:** Always specify the project and dataset in your queries or configure them in the `sqlspec` connection settings.
-   **Cost:** Be mindful that BigQuery is a cloud data warehouse and queries are billed based on the amount of data scanned. Avoid `SELECT *` on large tables. Use partitioned and clustered tables to reduce query costs.

## Common Issues

-   **`google.api_core.exceptions.Forbidden: 403`**: Authentication or permission issue. Ensure your service account or user has the necessary BigQuery roles (e.g., `BigQuery User`, `BigQuery Data Viewer`).
-   **`google.api_core.exceptions.NotFound: 404`**: Table or dataset not found. Double-check your project ID, dataset ID, and table names in your queries.
