# Stock Market ETL Pipeline

A Python ETL (Extract, Transform, Load) pipeline that ingests daily stock quotes from the Alpha Vantage API, transforms the response, and loads structured records into a SQLite database. Built to run daily and accumulate a queryable time series of market data.

## What it does

- **Extract** — calls the Alpha Vantage `GLOBAL_QUOTE` REST endpoint for a configurable list of tickers and parses the nested JSON response.
- **Transform** — extracts a defined set of fields (symbol, date, open, high, low, price) from each response.
- **Load** — inserts each record into a SQLite database using parameterised SQL statements.

The pipeline is resilient: each API call and extraction is wrapped in `try/except`, so a bad or rate-limited response for one ticker is logged and skipped without bringing down the whole run.

## Tech stack

- **Python** — `requests` (HTTP), `sqlite3` (database), `python-dotenv` (config)
- **SQLite** — local relational database
- **Alpha Vantage API** — free-tier market data source

## Project structure

| File | Purpose |
|------|---------|
| `setup_db.py` | One-time script that creates the database and `stocks` table |
| `pipeline.py` | The daily ETL run — fetches, transforms, and loads data |
| `.env` | Holds the API key (not committed — see Setup) |

Setup and ingestion are kept in separate files to maintain a clean separation of concerns.

## Setup

1. **Clone the repo** and install dependencies:
   ```bash
   pip install requests python-dotenv
   ```

2. **Get a free Alpha Vantage API key** from https://www.alphavantage.co/support/#api-key

3. **Create a `.env` file** in the project root with your key:
   ```
   ALPHA_VANTAGE_KEY=your_key_here
   ```

4. **Create the database** (run once):
   ```bash
   python setup_db.py
   ```

5. **Run the pipeline:**
   ```bash
   python pipeline.py
   ```

## Example output

```
Successfully fetched AAPL: $307.34
Skipping TSLA: API didn't return expected data (likely rate limit)
Successfully fetched MSFT: $416.67
Pipeline finished — data saved to database!
```

## Querying the data

Once the pipeline has run, the data can be queried with SQL. For example:

```sql
-- Average closing price per stock
SELECT symbol, AVG(price) FROM stocks GROUP BY symbol;

-- Highest price recorded
SELECT symbol, MAX(price) FROM stocks;
```

## Notes

- The Alpha Vantage free tier is limited to 25 requests per day, so some tickers may be skipped on a given run — the pipeline handles this gracefully.
- The API key is loaded from a `.env` file and excluded from version control via `.gitignore`, so no credentials are committed to the repository.

## Possible next steps

- Schedule the pipeline to run automatically (cron / Task Scheduler / GitHub Actions)
- Add logging via Python's `logging` module
- Add an analysis script to surface trends over time
- Migrate from SQLite to PostgreSQL for a multi-user setup