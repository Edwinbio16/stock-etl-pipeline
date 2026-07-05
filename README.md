# Stock Market ETL Pipeline

A Python ETL (Extract, Transform, Load) pipeline that ingests daily stock history from the Alpha Vantage API, transforms the response, and loads structured records into a SQLite database. It backfills ~100 days of history per ticker on the first run and can be re-run — or **run automatically on a schedule** — to keep a queryable time series of market data up to date.

The project exists in two forms:

- **`pipeline.py`** — the original standalone script. Fetches, transforms, and loads in a single linear run.
- **`prefect_pipeline.py`** — the same ETL logic re-architected with [Prefect](https://www.prefect.io/), adding orchestration, automatic retries, structured logging, and one-line scheduling. This is the version intended to run unattended.

## What it does

**Extract** — calls the Alpha Vantage `TIME_SERIES_DAILY` REST endpoint for a configurable list of tickers (AAPL, TSLA, MSFT, NVDA). Each call returns roughly 100 days of daily history (`outputsize=compact`), parsed from the nested JSON response.

**Transform** — for every day in the response, extracts a defined set of fields: `symbol`, `date`, `open`, `high`, `low`, and `close` (stored as `price`).

**Load** — writes each daily record into a SQLite database using parameterised SQL, with `INSERT OR REPLACE` keyed on `(symbol, date)`. The pipeline is idempotent: re-running it overwrites overlapping days and adds new ones, never creating duplicates.

## Orchestration and scheduling (Prefect)

`prefect_pipeline.py` wraps each stage — extract, transform, load — in a Prefect `@task`, orchestrated by a `@flow`. This turns a linear script into an observable, resilient pipeline:

- **Automatic retries.** The extract task is configured with `retries=3, retry_delay_seconds=20`. Instead of catching an error and *skipping* a ticker, a failed extract *raises*, and Prefect re-runs it automatically. The 20-second delay is long enough for Alpha Vantage's per-minute rate-limit window to clear, so a throttled ticker typically recovers on the retry rather than being lost. This is a strict upgrade over silent skips: the pipeline moves from "skip and hope" to "retry and recover."
- **Structured logging.** Each task run is individually logged and timestamped, so it's clear exactly which ticker failed, why, and whether it recovered.
- **Scheduling.** The `serve()` call at the bottom of the file registers the flow on a cron schedule (`0 18 * * 1-5` — 18:00, Monday to Friday) and runs it as a long-lived process, firing each run automatically with no manual trigger.
- **Observability.** When run against a local Prefect server (`prefect server start`), every run appears in the Prefect dashboard at `http://127.0.0.1:4200`, with per-task status, timing, and logs.

## Tech stack

- **Python** — `requests` (HTTP), `sqlite3` (database), `python-dotenv` (config)
- **Prefect** — workflow orchestration, retries, scheduling, and observability
- **SQLite** — local relational database
- **Alpha Vantage API** — free-tier market data source

## Project structure

| File | Purpose |
| --- | --- |
| `setup_db.py` | One-time script that creates the database and `stocks` table |
| `pipeline.py` | The original standalone ETL run |
| `prefect_pipeline.py` | The orchestrated, schedulable version (Prefect tasks + flow + cron) |
| `.env` | Holds the API key (not committed — see Setup) |

Setup and ingestion are kept in separate files to maintain a clean separation of concerns. The `stocks` table uses a composite primary key of `(symbol, date)`, which is what makes the `INSERT OR REPLACE` re-run behaviour possible.

## Setup

Clone the repo and install dependencies:

```bash
pip install requests python-dotenv prefect
```

Get a free Alpha Vantage API key from https://www.alphavantage.co/support/#api-key

Create a `.env` file in the project root with your key:

```
ALPHA_VANTAGE_KEY=your_key_here
```

Create the database (run once):

```bash
python setup_db.py
```

### Run once

```bash
python prefect_pipeline.py
```

(with the `serve()` block commented out and the direct `stock_etl()` call active)

### Run on a schedule

1. In one terminal, start a local Prefect server:
   ```bash
   prefect server start
   ```
2. Point Prefect at it (one-time):
   ```bash
   prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
   ```
3. In a second terminal, start the scheduled service (with the `serve()` block active):
   ```bash
   python prefect_pipeline.py
   ```

The flow now fires automatically on its cron schedule. Runs are visible live in the dashboard at `http://127.0.0.1:4200`.

## Querying the data

Once the pipeline has run, the data can be queried with SQL. For example:

```sql
-- Average price per stock
SELECT symbol, AVG(price) FROM stocks GROUP BY symbol;

-- Highest price recorded
SELECT symbol, MAX(price) FROM stocks;

-- 5-day moving average of closing price (window function)
SELECT symbol, date, price,
       AVG(price) OVER (PARTITION BY symbol ORDER BY date
                        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS moving_avg_5d
FROM stocks ORDER BY symbol, date;
```

## Roadmap

Planned next steps to extend this into a fuller deployment story:

- **Migrate from SQLite to PostgreSQL** (switching the upsert to `ON CONFLICT`).
- **Containerise** the pipeline and its dependencies with Docker.

## Notes

The Alpha Vantage free tier is limited to 25 requests per day and a few requests per minute. In `prefect_pipeline.py`, a rate-limited ticker raises and is retried automatically (up to three times, 20 seconds apart) rather than skipped, so throttled requests usually recover within the same run.

The API key is loaded from a `.env` file and excluded from version control via `.gitignore`, so no credentials are committed to the repository.
