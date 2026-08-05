# RetailPulse — Weekly Sales Intelligence and Event Notification

Implementation of `retailpulse-pyspark-sparksql-kafka-airflow-case-study`.

The pipeline runs in two modes from one codebase:

- **`local`** — open-source Spark on this machine, managed Delta tables in `./warehouse`. Used for development and tests.
- **`databricks`** — a Databricks Connect session against the workspace, managed Delta tables in Unity Catalog.

Only `config.py` and `session.py` know which mode is active. The transformation functions in
`medallion/bronze.py` / `silver.py` / `gold.py` are pure `DataFrame -> DataFrame` code and are identical in
both modes.

## Layout

```
project_2/
├── retailpulse/
│   ├── config.py        RunConfig from .env; table names; raw CSV paths (folder vs /Volumes)
│   ├── session.py       local SparkSession+Delta | DatabricksSession; schema + table writes
│   ├── medallion/
│   │   ├── bronze.py    Part A   — raw CSV -> bronze_* tables
│   │   ├── silver.py    Part B   — clean/cast/dedupe/enrich -> silver_* tables
│   │   └── gold.py      Part B.14 + C — aggregates and window functions -> gold_* tables
│   ├── sql_checks.py    Part D   — runs sql/ and the sql/checks/ reconciliation queries
│   ├── events.py        Gold row -> Part E event payload (Spark-free, unit-testable)
│   └── kafka_io.py      Part E   — producer / consumer
├── sql/                 Part D scripts + sql/checks/ reconciliation queries
├── dags/                retailpulse_weekly_orchestration.py (Part F)
├── docker-compose.yml   Kafka by default; Airflow behind the `airflow` profile
├── tests/               pytest against a local Spark session
└── requirements/        local.txt · databricks.txt
```

Each layer module holds both its transformation functions and its `main()`, so the whole of Bronze
is one file, Silver is one file, Gold is one file.

## Setup

`pyspark` and `databricks-connect` **cannot coexist in one environment**, so there are two venvs.
Both need **Python 3.12** — `databricks-connect` must match the runtime's Python minor version, and
3.13 wheels are still unreliable. Java 17 or 21 is required for local Spark.

```powershell
cd project_2
Copy-Item env.example .env      # then edit it

py -3.12 -m venv .venv-local
.\.venv-local\Scripts\Activate.ps1
pip install -r requirements/local.txt
```

```powershell
py -3.12 -m venv .venv-dbx
.\.venv-dbx\Scripts\Activate.ps1
pip install -r requirements/databricks.txt
```

Local Spark on Windows needs `winutils.exe` and `hadoop.dll` on `HADOOP_HOME`. If that turns into a
fight, run local mode under WSL2 instead — the code is unchanged.

## Running

Run from `project_2/`; there is nothing to install, `-m` finds the package in the current directory.

```powershell
python -m retailpulse.medallion.bronze
python -m retailpulse.medallion.silver
python -m retailpulse.medallion.gold
python -m retailpulse.sql_checks

# against the workspace (from .venv-dbx)
python -m retailpulse.medallion.bronze --mode databricks
```

`--mode` overrides `RETAILPULSE_MODE` for a single run, which makes it easy to build a table locally
and then reproduce it in the workspace.

### Kafka (Part E)

```powershell
docker compose up -d
python -m retailpulse.kafka_io publish
python -m retailpulse.kafka_io consume --max 20     # writes output/consumed_events.jsonl
```

### Airflow (Part F)

```powershell
docker compose --profile airflow up -d
# http://localhost:8080  ->  admin / admin
```

The `airflow` profile starts Postgres, the webserver and the scheduler alongside the Kafka services
that `up -d` already brings; compose reads `.env` from this directory automatically. The DAG is
`retailpulse_weekly_orchestration`, scheduled `0 9 * * 3` with `catchup=False`. It needs
`DATABRICKS_JOB_ID` in `.env`, pointing at a job you create in the workspace that runs
Bronze → Silver → Gold. Airflow does the Kafka work itself because the workspace cannot reach a
broker on this host.

### Tests

```powershell
pytest
```

`tests/test_stack.py` spins up a real local Spark + Delta session and writes a Delta table.