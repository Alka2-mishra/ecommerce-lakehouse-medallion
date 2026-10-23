# Running in Microsoft Fabric or Databricks

This repo uses a local Spark entrypoint to make the transformations reviewable and reproducible. For a managed lakehouse:

1. Create a Lakehouse.
2. Upload the source CSVs to its Files area (or use a supported public-data download process).
3. Adapt the input path to the platform's mounted Files path.
4. Run Bronze, Silver and Gold as separate notebook cells/notebooks.
5. Save tables as Delta tables in the Lakehouse.
6. Use the Lakehouse SQL analytics endpoint / Databricks SQL warehouse for Power BI connectivity.

## Fabric note
In a Fabric notebook, use the attached Lakehouse's Files and Tables paths. Prefer writing Delta tables into the Lakehouse `Tables` area to make them discoverable in the SQL analytics endpoint.

## Databricks note
Use a Unity Catalog volume or DBFS-supported location for files and managed Delta tables. Configure the Power BI connector to the SQL warehouse or supported Databricks SQL endpoint.

This guide is intentionally platform-neutral; exact UI options depend on workspace configuration and account/trial availability.
