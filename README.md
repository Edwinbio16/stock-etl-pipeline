# Stock Market ETL Pipeline

A Python ETL pipeline that ingests daily stock history from the Alpha Vantage API, transforms the response, and loads it into a **PostgreSQL** database. It is **orchestrated with Prefect**, giving it automatic retries, per-task logging, an observable task graph, and a weekday cron schedule.

The pipeline backfills roughly 100 days of history per ticker on the first run, and can be re-run — or run automatically on a schedule — to keep a queryable time series of market data up to date. Re-running never creates duplicates.

## What it does

**Extract** — calls the Alpha Vantage `TIME_SERIES_DAILY` endpoint for a configurable list of tickers (AAPL, TSLA, MSFT, NVDA). Each call returns ~100 days of daily history (`outputsize=compact`), parsed from a nested JSON response.

**Transform** — flattens the response into row tuples of `symbol`, `date`, `open`, `high`, `low`, and `close` (stored as `price`).

**Load** — upserts each row into PostgreSQL using parameterised SQL and `ON CONFLICT (symbol, date) DO UPDATE`. The pipeline is idempotent: overlapping days are overwritten, new days are added, duplicates are never created.

## Orchestration (Prefect)

Each stage — extract, transform, load — is a Prefect `@task`, wrapped in a `@flow`. This is what turns a linear script into a resilient, observable pipeline:

- **Automatic retries.** The extract task is configured with `retries=3, retry_delay_seconds=20`. A failed extract *raises* rather than being silently skipped, and Prefect re-runs it. The 20-second delay is long enough for Alpha Vantage's per-minute rate limit to clear, so a throttled ticker recovers on the retry instead of being lost. This has been observed working in practice: during a live run, NVDA hit the API rate limit, was retried automatically, and completed with no data loss and no manual intervention.
- **Per-task observability.** Every task run is individually tracked, timed, and logged, so it's clear exactly which ticker failed, why, and whether it recovered. A four-ticker run produces 12 tracked task runs (4 tickers × 3 stages), visible in the Prefect UI.
- **Scheduling.** `stock_etl.serve(name="stock-etl-weekday", cron="0 18 * * 1-5")` registers the flow to run automatically at 18:00, Monday to Friday, against a running Prefect server.

## Database

PostgreSQL 18, with a composite primary key on `(symbol, date)`:

| Column | Type | Notes |
|---|---|---|
| `symbol` | `TEXT` | part of composite PK |
| `date` | `DATE` | part of composite PK |
| `open` | `NUMERIC` | |
| `high` | `NUMERIC` | |
| `low` | `NUMERIC` | |
| `price` | `NUMERIC` | daily close |

`date` is a real `DATE` type rather than text, so the database understands chronology — date ordering and arithmetic are correct by construction, which matters for the window function queries below. `NUMERIC` is used for prices rather than a floating-point type to avoid rounding error.

The composite primary key is what makes the upsert work: it's the conflict target that `ON CONFLICT (symbol, date) DO UPDATE` keys on.

## Setup

Requires Python 3.10+ and a running PostgreSQL server.

```bash
pip install -r requirements.txt
```

Create a database (e.g. `stocks`), then create a `.env` file in the project root:

```
ALPHA_VANTAGE_KEY=your_api_key
DB_NAME=stocks
DB_USER=postgres
DB_PASSWORD=your_password
DB_HOST=localhost
DB_PORT=5432
```

`.env` is gitignored and never committed.

## Running

Create the table (once):

```bash
python setup_db.py
```

Run the pipeline once:

```bash
python prefect_pipeline.py
```

To run it on a schedule instead, comment out the direct `stock_etl()` call at the bottom of `prefect_pipeline.py` and uncomment the `serve()` block. Start a Prefect server in a separate terminal:

```bash
prefect server start
```

The dashboard is then available at `http://127.0.0.1:4200`, where flow runs, task graphs, retries, and the registered deployment schedule can all be inspected.

## Querying the data

The `date`/`NUMERIC` schema supports window functions directly. `window_practice.py` and `window_practice_real.py` contain worked examples against the real dataset, including:

- **7-day moving average** of closing price per ticker
- **Day-over-day change**, using `LAG` to compare each row against the previous trading day
- **Ranking** tickers by performance within a period

Example — moving average per ticker:

```sql
SELECT
    symbol,
    date,
    price,
    AVG(price) OVER (
        PARTITION BY symbol
        ORDER BY date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS moving_avg_7d
FROM stocks
ORDER BY symbol, date;
```

## Project structure

| File | Purpose |
|---|---|
| `prefect_pipeline.py` | The pipeline. Prefect-orchestrated, PostgreSQL-backed. **This is the current version.** |
| `setup_db.py` | Creates the PostgreSQL `stocks` table. |
| `window_practice.py` | Window function queries against sample data. |
| `window_practice_real.py` | Window function queries against the real dataset. |
| `pipeline.py` | The original standalone script (SQLite, no orchestration). Kept for reference to show the project's starting point; superseded by `prefect_pipeline.py`. |

## Notes

- The free Alpha Vantage tier is rate limited (25 requests/day, with a per-minute burst limit). The flow sleeps between tickers to stay within it, and the retry logic handles the case where it doesn't.
- `outputsize=compact` returns ~100 days. Switching to `full` returns 20+ years of history.

## Roadmap

- **Docker** — containerise the pipeline alongside a PostgreSQL container so the whole stack runs anywhere with a single command.
