"""RetailPulse lakehouse pipeline.

The same code runs against a local Spark session or a Databricks workspace.
Only `config` and `session` know which mode is active; the transformation
functions in `bronze` / `silver` / `gold` are pure DataFrame code.
"""
