"""Part F -- weekly orchestration.

Required flow:

    Trigger Databricks Job -> Wait for Databricks completion -> Read Gold event
    rows -> Publish events to Kafka -> Consume a sample of events -> Finish

Schedule `0 9 * * 3` (every Wednesday 09:00), catchup=False.

Written with the TaskFlow API: `@dag` on the factory function, `@task` on each
step.  A decorated function's name becomes its task_id, its return value is
pushed to XCom automatically, and passing one call's result into another
declares both the data flow and the dependency.  Edges that carry no data
(the sensor -> read, publish -> consume) still use `>>`.

Notes on the six tasks:

  trigger_databricks_job
      DatabricksHook.run_now() rather than DatabricksRunNowOperator, so the run
      id comes back as a plain return value and the wait stays its own node in
      the graph.  The classic operator with wait_for_termination=False and
      `.output` would work identically -- TaskFlow and classic operators mix
      freely in one DAG.  Needs DATABRICKS_JOB_ID in the environment.

  wait_for_databricks_completion
      NOTE: apache-airflow-providers-databricks has no job-run sensor -- only
      SQL and partition sensors. Don't go looking for DatabricksJobRunSensor.
      `@task.sensor(mode="reschedule")` is the TaskFlow PythonSensor: poll
      get_run_state() and return PokeReturnValue(is_done=...).  Reschedule mode
      releases the worker slot between pokes, which matters on LocalExecutor
      where a two-hour poke would otherwise hold a slot the whole time.

  read_gold_event_rows / publish_events_to_kafka / consume_sample_events
      Thin wrappers over retailpulse.kafka_io -- the same functions the CLI
      calls, so a green DAG run and `python -m retailpulse.kafka_io publish`
      exercise identical code.  Airflow does the Kafka work because the
      Databricks workspace cannot reach a broker on this host; the broker is at
      kafka:29092 from inside the container (already set as
      KAFKA_BOOTSTRAP_SERVERS in docker-compose.yml).

  finish
      EmptyOperator -- nothing to run, so there is nothing to decorate.
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
    def wait_for_databricks_completion(run_id: int) -> PokeReturnValue:
        """Poll the run until it reaches a terminal state; fail if unsuccessful."""
        state = DatabricksHook(databricks_conn_id=DATABRICKS_CONN_ID).get_run_state(run_id)
        log.info("run %s: %s (%s)", run_id, state.life_cycle_state, state.state_message)

        if not state.is_terminal:
            return PokeReturnValue(is_done=False)
        if not state.is_successful:
            # AirflowFailException, not AirflowException: a run that finished
            # FAILED will not pass on a retry, so don't burn one polling again.
            raise AirflowFailException(
                f"Databricks run {run_id} finished {state.result_state}: {state.state_message}"
            )
        return PokeReturnValue(is_done=True, xcom_value=state.result_state)

    @task
    def read_gold_event_rows() -> list[dict[str, Any]]:
        """gold_monthly_category_sales -> Part E event payloads."""
        # Imported here, not at module level: the scheduler re-parses this file
        # every few seconds and the Spark/Databricks-Connect import is slow.
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

    run_id = trigger_databricks_job()
    waited = wait_for_databricks_completion(run_id)
    events = read_gold_event_rows()
    published = publish_events_to_kafka(events)
    consumed = consume_sample_events()

    waited >> events
    published >> consumed >> finish


retailpulse_weekly_orchestration()
