"""
Stock Market ETL - Prefect-orchestrated version
=================================================
Same Extract -> Transform -> Load logic as pipeline.py, but each stage is now a
Prefect @task and the whole run is a @flow. What this buys you over the plain
script:

  * Automatic retries on the API call (replaces the manual try/except skip).
  * Structured logging visible in the Prefect UI instead of bare print().
  * An observable task graph - you can see exactly which ticker failed and why.
  * A one-line path to scheduling (see the __main__ block at the bottom).

Run a single run:   python prefect_pipeline.py
Run on a schedule:  uncomment the serve() call at the bottom, then run the same command.
"""
import os
import psycopg2

import requests
from dotenv import load_dotenv
from prefect import flow, task, get_run_logger

load_dotenv()

STOCKS = ["AAPL", "TSLA", "MSFT", "NVDA"]
DB_PATH = "stock_data.db"


# ---------------------------------------------------------------------------
# EXTRACT - retries=3 means Prefect re-runs this task up to 3 times on ANY
# exception (network drop, 500, rate-limit) with a 20s gap. That 20s pause is
# also long enough for the per-minute rate-limit window to clear, so a throttled
# ticker often succeeds on the retry instead of being skipped outright.
# ---------------------------------------------------------------------------
@task(retries=3, retry_delay_seconds=20)
def extract(ticker: str) -> dict:
    logger = get_run_logger()

    api_key = os.getenv("ALPHA_VANTAGE_KEY")
    if not api_key:
        # Fail loudly with a useful message instead of an AttributeError.
        raise ValueError("ALPHA_VANTAGE_KEY is not set - add it to your .env file")
    api_key = api_key.strip()

    url = (
        "https://www.alphavantage.co/query"
        "?function=TIME_SERIES_DAILY"
        f"&symbol={ticker}"
        "&outputsize=compact"
        f"&apikey={api_key}"
    )

    logger.info(f"Fetching {ticker} from Alpha Vantage")
    response = requests.get(url, timeout=30)
    response.raise_for_status()  # turn a 4xx/5xx into an exception Prefect can retry
    data = response.json()

    if "Time Series (Daily)" not in data:
        # Rate-limit / invalid-ticker responses come back as a note, not data.
        note = data.get("Note") or data.get("Information") or data.get("Error Message") or data
        raise RuntimeError(f"No daily series for {ticker}: {note}")

    return {"ticker": ticker, "series": data["Time Series (Daily)"]}


# ---------------------------------------------------------------------------
# TRANSFORM - flatten the nested JSON into clean (symbol, date, o, h, l, c, v)
# tuples ready for insertion. Pure function, no I/O, so it's trivially testable.
# ---------------------------------------------------------------------------
@task
def transform(payload: dict) -> list:
    ticker = payload["ticker"]
    series = payload["series"]

    rows = []
    for date, values in series.items():
        rows.append((
            ticker,
            date,
            float(values["1. open"]),
            float(values["2. high"]),
            float(values["3. low"]),
            float(values["4. close"]),
        ))
    return rows


# ---------------------------------------------------------------------------
# LOAD - idempotent upsert. INSERT OR REPLACE against the composite (symbol,
# date) primary key means re-running the pipeline never creates duplicates:
# same day's data just overwrites itself.
# ---------------------------------------------------------------------------
@task
def load(rows: list) -> int:
    logger = get_run_logger()

    conn = psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
   )
    cursor = conn.cursor()
    cursor.executemany(
        """
        INSERT INTO stocks (symbol, date, open, high, low, price)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, date) DO UPDATE SET
        open = EXCLUDED.open,
        high = EXCLUDED.high,
        low = EXCLUDED.low,
        price = EXCLUDED.price
        """,
        rows,

   )
    conn.commit()
    written=cursor.rowcount
    conn.close()
    logger.info(f"Loaded {written} rows")
    return written


# ---------------------------------------------------------------------------
# FLOW - the orchestrator. Loops the tickers, runs the three tasks in order,
# and reports a total. Because each ticker's extract retries independently, one
# flaky ticker no longer takes down the whole run.
# ---------------------------------------------------------------------------
@flow(name="stock-etl")
def stock_etl():
    logger = get_run_logger()
    total = 0

    for ticker in STOCKS:
        payload = extract(ticker)
        rows = transform(payload)
        total += load(rows)

    logger.info(f"Run complete - {total} rows across {len(STOCKS)} tickers")
    return total


if __name__ == "__main__":
    # --- Option A: run once, right now ---
    stock_etl()

    # --- Option B: run on a schedule ---
    #stock_etl.serve(
        #name="stock-etl-weekday",
        #cron="0 18 * * 1-5",
    #)
