"""Smoke test: proves local Spark + Delta + the table read/write path work.

Run this before writing any transforms -- if it passes, the stack is sound and
every later failure is your logic, not your environment.

Add your own tests alongside it: the layer functions take DataFrames and return
DataFrames, so you can build a 5-row input with spark.createDataFrame() and
assert on the result without touching a CSV.
"""

from retailpulse.config import RAW_FILES
from retailpulse.medallion.bronze import read_raw, to_bronze
from retailpulse.session import ensure_namespace, save


def test_local_spark_delta_roundtrip(spark, cfg):
    ensure_namespace(spark, cfg)

    bronze = to_bronze(read_raw(spark, cfg, "customers"), RAW_FILES["customers"])
    save(bronze, cfg, "bronze_customers")

    loaded = spark.table(cfg.table("bronze_customers"))
    assert loaded.count() == 500
    assert {"source_file", "ingestion_timestamp"} <= set(loaded.columns)
