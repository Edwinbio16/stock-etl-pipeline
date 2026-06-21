import requests
import sqlite3
import os
import time
from dotenv import load_dotenv

# Load the API key from the .env file. .strip() removes any accidental
# spaces around the key (our .env has a space after the "=").
load_dotenv()
API_KEY = os.getenv("ALPHA_VANTAGE_KEY").strip()

stocks = ["AAPL", "TSLA", "MSFT", "NVDA"]

conn = sqlite3.connect("stock_data.db")
cursor = conn.cursor()

for ticker in stocks:
    # --- EXTRACT -----------------------------------------------------------
    # TIME_SERIES_DAILY returns ~100 days of history in ONE call.
    # outputsize=compact = latest 100 days (use "full" for 20+ years).
    url = (
        "https://www.alphavantage.co/query"
        "?function=TIME_SERIES_DAILY"
        f"&symbol={ticker}"
        "&outputsize=compact"
        f"&apikey={API_KEY}"
    )

    try:
        response = requests.get(url)
        data = response.json()
        # The history lives under this key. If it's missing, the API returned
        # an error/rate-limit message instead, so we skip this ticker.
        time_series = data["Time Series (Daily)"]
    except KeyError:
        print(f"Skipping {ticker}: API didn't return expected data (likely rate limit or invalid ticker)")
        continue

    # --- TRANSFORM + LOAD --------------------------------------------------
    # Loop over EVERY day in the response (this is the key change from the
    # old version, which only handled one day).
    rows_written = 0
    for day, values in time_series.items():
        cursor.execute(
            "INSERT OR REPLACE INTO stocks (symbol, date, open, high, low, price) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (
                ticker,
                day,                       # the date is the dictionary key
                values["1. open"],
                values["2. high"],
                values["3. low"],
                values["4. close"],        # we store the CLOSE as "price"
            ),
        )
        rows_written += 1

    print(f"Successfully fetched {ticker}: {rows_written} days of history")

    # The free tier rate-limits requests per MINUTE. Pause between tickers so
    # we don't get throttled (which would silently skip the next ticker).
    time.sleep(15)

conn.commit()
conn.close()
print("Pipeline finished — data saved to database!")
