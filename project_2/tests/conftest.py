"""Session-scoped local Spark fixture.

Tests always run in local mode and never touch the workspace.
"""

import shutil
import tempfile
from pathlib import Path

import pytest

from retailpulse.config import DEFAULT_LOCAL_RAW, PROJECT_ROOT, RunConfig
from retailpulse.session import get_spark


@pytest.fixture(scope="session")
def cfg(tmp_path_factory) -> RunConfig:
    """A local RunConfig writing to a throwaway warehouse, in its own schema.

    Built by hand rather than via load_config() so a stray .env cannot point the
    tests at the workspace.
    """
    warehouse = tmp_path_factory.mktemp("warehouse")
    return RunConfig(
        mode="local",
        catalog="spark_catalog",
        schema="retail_fresher_test",
        volume="retail_raw",
        local_raw=(PROJECT_ROOT / DEFAULT_LOCAL_RAW).resolve(),
        local_warehouse=warehouse,
        kafka_bootstrap="localhost:9092",
        kafka_topic="retail-sales-summary-test",
        kafka_group="retailpulse-test",
        consumed_events_path=warehouse / "consumed_events.jsonl",
    )


@pytest.fixture(scope="session")
def spark(cfg):
    session = get_spark(cfg)
    session.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.namespace}")
    yield session
    session.stop()
    shutil.rmtree(Path(tempfile.gettempdir()) / "metastore_db", ignore_errors=True)
