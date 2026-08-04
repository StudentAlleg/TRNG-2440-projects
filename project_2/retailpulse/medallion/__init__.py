"""The three medallion layers, one module each.

Every module holds its transformation functions *and* its `main()`:

    python -m retailpulse.medallion.bronze
    python -m retailpulse.medallion.silver
    python -m retailpulse.medallion.gold

The transformation functions are pure DataFrame -> DataFrame code -- nothing
here creates a session, reads a path or writes a table directly, which is what
makes the same code valid locally and on Databricks, and unit-testable.
"""
