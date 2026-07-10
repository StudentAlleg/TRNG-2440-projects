# Weather API

Extracts, cleans, and loads weather data from the Open Meteo API into a Postgres database.

## Setup

1. Install dependencies:
   ```
   pip install -r requirements.txt
   ```
2. Start the database:
   ```
   docker compose up -d
   ```
3. Make sure a `.env` file exists with the Postgres connection settings (`POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, `POSTGRES_PORT`, `POSTGRES_IP`).

## Running

```
python main.py
```

### Options

| Flag | Description | Default |
| --- | --- | --- |
| `-s`, `--start` | Start date for the data pull | `2026-05-01` |
| `-e`, `--end` | End date for the data pull | `2026-06-01` |
| `-l`, `--location` | Path to the locations JSON file | `data/locations.json` |
| `-o`, `--out` | Path to write/read the raw API output | `data/api_out.json` |
| `--no-extract` | Skip calling the API and reuse the existing `--out` file | off |
| `--reset-db` | Drop existing tables and re-initialize the database before loading | off |

### Example: reset the database and reload data

```
python main.py --reset-db
```

## AI Usage
Used Claude CLI in a limited fashion. Design was done primarily by hand. LLM used for some, query tweaks for the first 2 analytical queries. Generating the WeatherDAO based off of LocationDAO, and the README.md.
