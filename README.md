# Stock Market ETL Pipeline

A Python ETL (Extract, Transform, Load) pipeline that ingests daily stock history from the Alpha Vantage API, transforms the response, and loads structured records into a SQLite database. It backfills ~100 days of history per ticker on the first run and can be re-run daily to keep a queryable time series of market data up to date.

## What it does

- **Extract** — calls the Alpha Vantage `TIME_SERIES_DAILY` REST endpoint for a configurable list of tickers. Each call returns roughly 100 days of daily history (`outputsize=compact`), parsed from the nested JSON response.
- **Transform** — for every day in the response, extracts a defined set of fields (symbol, date, open, high, low, close-as-price).
- **Load** — writes each daily record into a SQLite database using parameterised SQL statements, with `INSERT OR REPLACE` keyed on `(symbol, date)` so the pipeline can be re-run safely: overlapping days are overwritten and new days are added, never duplicated.

The pipeline is resilient: each API call and extraction is wrapped in `try/except`, so a bad or rate-limited response for one ticker is logged and skipped without bringing down the whole run. To stay under the free tier's per-minute rate limit, it pauses briefly (`time.sleep`) between tickers.

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

Setup and ingestion are kept in separate files to maintain a clean separation of concerns. The `stocks` table uses a composite primary key of `(symbol, date)`, which is what makes the `INSERT OR REPLACE` re-run behaviour possible.

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
Database setup complete.
Successfully fetched AAPL: 100 days of history
Successfully fetched TSLA: 100 days of history
Successfully fetched MSFT: 100 days of history
Successfully fetched NVDA: 100 days of history
Pipeline finished — data saved to database!
```

A full run takes about a minute because of the deliberate pauses between tickers (see Notes).

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

## Notes

- The Alpha Vantage free tier is limited to 25 requests per day **and** a few requests per minute. The pipeline pauses ~15 seconds between tickers to avoid the per-minute limit; if a request is still throttled, that ticker is skipped gracefully and can be picked up on the next run.
- The API key is loaded from a `.env` file and excluded from version control via `.gitignore`, so no credentials are committed to the repository.