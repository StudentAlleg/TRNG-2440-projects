"""Run configuration, loaded from the environment / .env file."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

LOCAL = "local"
DATABRICKS = "databricks"

# project_2/, the directory holding .env, warehouse/ and output/
PROJECT_ROOT = Path(__file__).resolve().parents[1]

RAW_FILES = {
    "customers": "customers_500.csv",
    "products": "products_500.csv",
    "sales_orders": "sales_orders_500.csv",
}

# The CSVs ship in the case-study repo, which sits next to project_2/. Defaulting
# to it means a fresh clone runs without a .env; RETAILPULSE_LOCAL_RAW overrides
# it if the data moves.
DEFAULT_LOCAL_RAW = "../retailpulse-pyspark-sparksql-kafka-airflow-case-study/datasets"


@dataclass(frozen=True)
class RunConfig:
    mode: str
    catalog: str
    schema: str
    volume: str
    local_raw: Path
    local_warehouse: Path
    kafka_bootstrap: str
    kafka_topic: str
    kafka_group: str
    consumed_events_path: Path

    @property
    def is_local(self) -> bool:
        return self.mode == LOCAL

    @property
    def namespace(self) -> str:
        return f"{self.catalog}.{self.schema}"

    def table(self, name: str) -> str:
        """Fully-qualified table name, identical shape in both modes."""
        return f"{self.catalog}.{self.schema}.{name}"

    def raw_path(self, dataset: str) -> str:
        """Location of one raw CSV. `dataset` is a key of RAW_FILES.

        The only path that differs between modes: a local folder versus a Unity
        Catalog volume.
        """
        filename = RAW_FILES[dataset]
        if self.is_local:
            return (self.local_raw / filename).as_posix()
        return f"/Volumes/{self.catalog}/{self.schema}/{self.volume}/{filename}"


def _path(value: str) -> Path:
    p = Path(value).expanduser()
    return p if p.is_absolute() else (PROJECT_ROOT / p).resolve()


def load_config(mode: str | None = None) -> RunConfig:
    """Build a RunConfig. An explicit `mode` overrides RETAILPULSE_MODE."""
    load_dotenv(PROJECT_ROOT / ".env")

    resolved_mode = (mode or os.getenv("RETAILPULSE_MODE", LOCAL)).strip().lower()
    if resolved_mode not in (LOCAL, DATABRICKS):
        raise ValueError(
            f"RETAILPULSE_MODE must be '{LOCAL}' or '{DATABRICKS}', got {resolved_mode!r}"
        )

    default_catalog = "spark_catalog" if resolved_mode == LOCAL else "workspace"

    return RunConfig(
        mode=resolved_mode,
        catalog=os.getenv("RETAILPULSE_CATALOG", default_catalog),
        schema=os.getenv("RETAILPULSE_SCHEMA", "retail_fresher"),
        volume=os.getenv("RETAILPULSE_VOLUME", "retail_raw"),
        local_raw=_path(os.getenv("RETAILPULSE_LOCAL_RAW", DEFAULT_LOCAL_RAW)),
        local_warehouse=_path(os.getenv("RETAILPULSE_LOCAL_WAREHOUSE", "./warehouse")),
        kafka_bootstrap=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        kafka_topic=os.getenv("KAFKA_TOPIC", "retail-sales-summary"),
        kafka_group=os.getenv("KAFKA_CONSUMER_GROUP", "retailpulse-consumer"),
        consumed_events_path=_path(
            os.getenv("CONSUMED_EVENTS_PATH", "./output/consumed_events.jsonl")
        ),
    )
