"""Part F -- weekly orchestration.

Required flow:

    Trigger Databricks Job -> Wait for Databricks completion -> Read Gold event
    rows -> Publish events to Kafka -> Consume a sample of events -> Finish

Schedule `0 9 * * 3` (every Wednesday 09:00), catchup=False.

The task graph below is wired in the required order with placeholders. Replace
each placeholder with a real operator:

  trigger_databricks_job
      DatabricksRunNowOperator, conn id `databricks_default`, job_id from the
      DATABRICKS_JOB_ID env var. Pass wait_for_termination=False so the wait is
      its own node in the graph. It pushes the run id to XCom under key
      "run_id".

  wait_for_databricks_completion
      NOTE: apache-airflow-providers-databricks has no job-run sensor -- only
      SQL and partition sensors. Don't go looking for DatabricksJobRunSensor.
      Use a PythonSensor (mode="reschedule") whose callable pulls that run_id
      and polls DatabricksHook(...).get_run_state(run_id); the returned
      RunState exposes .is_terminal and .is_successful.

  read_gold_event_rows
      PythonOperator. Import retailpulse inside the callable, not at module
      level -- the DAG file is parsed constantly and Spark imports are slow.
      load_config() -> kafka_io.read_gold_events(cfg), which reads
      gold_monthly_category_sales and builds the payloads. Return the list so
      it lands in XCom.

  publish_events_to_kafka / consume_sample_events
      PythonOperator calling retailpulse.kafka_io. Airflow does the Kafka work
      because the Databricks workspace cannot reach a broker on this host. The
      broker is at kafka:29092 from inside the container (already set as
      KAFKA_BOOTSTRAP_SERVERS in docker-compose.yml).

  finish
      EmptyOperator.
"""

from __future__ import annotations

import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator

# The retailpulse package is mounted here by docker-compose.yml.
sys.path.insert(0, "/opt/airflow/project")

DEFAULT_ARGS = {"owner": "owen", "retries": 1}
DATABRICKS_CONN_ID = "databricks_default"

with DAG(
    dag_id="retailpulse_weekly_orchestration",
    description="Weekly RetailPulse lakehouse refresh and sales-summary event publication",
    start_date=datetime(2026, 1, 1),
    schedule="0 9 * * 3",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["retailpulse", "databricks", "kafka"],
) as dag:
    trigger_databricks_job = EmptyOperator(task_id="trigger_databricks_job")
    wait_for_databricks = EmptyOperator(task_id="wait_for_databricks_completion")
    read_gold_event_rows = EmptyOperator(task_id="read_gold_event_rows")
    publish_events_to_kafka = EmptyOperator(task_id="publish_events_to_kafka")
    consume_sample_events = EmptyOperator(task_id="consume_sample_events")
    finish = EmptyOperator(task_id="finish")

    (
        trigger_databricks_job
        >> wait_for_databricks
        >> read_gold_event_rows
        >> publish_events_to_kafka
        >> consume_sample_events
        >> finish
    )
