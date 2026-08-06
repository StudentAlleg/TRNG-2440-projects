"""The three medallion layers, one module each.

Every module holds its transformation functions and its `main()`:

    python -m retailpulse.medallion.bronze
    python -m retailpulse.medallion.silver
    python -m retailpulse.medallion.gold

The transformation functions are pure DataFrame -> DataFrame code. Nothing here
creates a session, reads a path or writes a table, which is what keeps the same
code valid both locally and on Databricks.
"""
