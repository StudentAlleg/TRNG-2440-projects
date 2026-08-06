"""Part F: weekly orchestration.

    Trigger Databricks Job -> Wait for Databricks completion -> Read Gold event
    rows -> Publish events to Kafka -> Consume a sample of events -> Finish

Schedule `0 9 * * 3` (every Wednesday 09:00), catchup=False.

Written with the TaskFlow API, so a decorated function's name becomes its
task_id and its return value is pushed to XCom. Edges that carry no data still
use `>>`.

Two things worth knowing before editing this file:

  * apache-airflow-providers-databricks has no job-run sensor, only SQL and
    partition sensors. There is no DatabricksJobRunSensor to import. Hence the
    hook plus `@task.sensor`, which also keeps the wait as its own graph node.
    Reschedule mode releases the worker slot between pokes, which matters on
    LocalExecutor where a two-hour poke would hold a slot the whole time.

  * Airflow does the Kafka work rather than the Databricks job, because the
    workspace cannot reach a broker on this host. Inside the container the
    broker is kafka:29092, set as KAFKA_BOOTSTRAP_SERVERS in docker-compose.yml.
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Any

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.operators.empty import EmptyOperator
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow.sensors.base import PokeReturnValue

# The retailpulse package is mounted here by docker-compose.yml.
sys.path.insert(0, "/opt/airflow/project")

log = logging.getLogger(__name__)

DEFAULT_ARGS = {"owner": "owen", "retries": 1}
DATABRICKS_CONN_ID = "databricks_default"


@dag(
    dag_id="retailpulse_weekly_orchestration",
    description="Weekly RetailPulse lakehouse refresh and sales-summary event publication",
    start_date=datetime(2026, 1, 1),
    schedule="0 9 * * 3",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["retailpulse", "databricks", "kafka"],
)
def retailpulse_weekly_orchestration():
    @task
    def trigger_databricks_job() -> int:
        """Start the Bronze -> Silver -> Gold job; return its run id."""
        job_id = os.environ["DATABRICKS_JOB_ID"]
        hook = DatabricksHook(databricks_conn_id=DATABRICKS_CONN_ID)
        run_id = hook.run_now({"job_id": int(job_id)})
        log.info("started job %s -> run %s: %s", job_id, run_id, hook.get_run_page_url(run_id))
        return run_id

    @task.sensor(poke_interval=60, timeout=timedelta(hours=2), mode="reschedule")
    def wait_for_databricks_completion(databricks_run_id: int) -> PokeReturnValue:
        """Poll the run until it reaches a terminal state; fail if unsuccessful.

        The argument is deliberately not called `run_id`. That name is a
        reserved Airflow context key, and a task declaring it dies with "The key
        'run_id' in args is a part of kwargs and therefore reserved" before the
        body ever runs.
        """
        hook = DatabricksHook(databricks_conn_id=DATABRICKS_CONN_ID)
        state = hook.get_run_state(databricks_run_id)
        log.info(
            "run %s: %s (%s)", databricks_run_id, state.life_cycle_state, state.state_message
        )

        if not state.is_terminal:
            return PokeReturnValue(is_done=False)
        if not state.is_successful:
            # AirflowFailException, not AirflowException: a run that finished
            # FAILED won't pass on a retry, so don't burn one polling again.
            raise AirflowFailException(
                f"Databricks run {databricks_run_id} finished "
                f"{state.result_state}: {state.state_message}"
            )
        return PokeReturnValue(is_done=True, xcom_value=state.result_state)

    @task
    def read_gold_event_rows() -> list[dict[str, Any]]:
        """Turn gold_monthly_category_sales rows into Part E event payloads."""
        from retailpulse.config import load_config
        from retailpulse.kafka_io import read_gold_events

        events = read_gold_events(load_config())
        if not events:
            raise AirflowFailException("gold_monthly_category_sales produced no event rows")
        log.info("read %d event rows", len(events))
        return events

    @task
    def publish_events_to_kafka(events: list[dict[str, Any]]) -> int:
        """Produce one message per event; return the delivered count."""
        from retailpulse.config import load_config
        from retailpulse.kafka_io import publish_events

        cfg = load_config()
        delivered = publish_events(cfg, events)
        log.info("delivered %d/%d events to %s", delivered, len(events), cfg.kafka_topic)
        return delivered

    @task
    def consume_sample_events(max_messages: int = 20) -> int:
        """Read a sample back and append it to consumed_events.jsonl."""
        from retailpulse.config import load_config
        from retailpulse.kafka_io import consume_events

        cfg = load_config()
        consumed = consume_events(cfg, max_messages=max_messages)
        log.info("consumed %d events -> %s", consumed, cfg.consumed_events_path)
        return consumed

    finish = EmptyOperator(task_id="finish")

    databricks_run_id = trigger_databricks_job()
    waited = wait_for_databricks_completion(databricks_run_id)
    events = read_gold_event_rows()
    published = publish_events_to_kafka(events)
    consumed = consume_sample_events()

    waited >> events
    published >> consumed >> finish


retailpulse_weekly_orchestration()
