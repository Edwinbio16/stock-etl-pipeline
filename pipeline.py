import requests
import sqlite3
import os
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("ALPHA_VANTAGE_KEY")

stocks = ["AAPL", "TSLA", "MSFT", "NVDA"]

conn = sqlite3.connect("stock_data.db")
cursor = conn.cursor()

for ticker in stocks:
    url = "https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol=" + ticker + "&apikey=" + API_KEY
    try:
        response = requests.get(url)
        data = response.json()

        price = data["Global Quote"]["05. price"]
        symbol = data["Global Quote"]["01. symbol"]
        open_price = data["Global Quote"]["02. open"]
        high_price = data["Global Quote"]["03. high"]
        low_price = data["Global Quote"]["04. low"]
        latest_trading_day = data["Global Quote"]["07. latest trading day"]
    except KeyError:
        print(f"Skipping {ticker}: API didn't return expected data (likely rate limit or invalid ticker)")
        continue

    print(f"Successfully fetched {ticker}: ${price}")
    cursor.execute(
        "INSERT OR REPLACE INTO stocks (symbol, date, open, high, low, price) VALUES (?, ?, ?, ?, ?, ?)",
        (symbol, latest_trading_day, open_price, high_price, low_price, price)
    )

conn.commit()
conn.close()
print("Pipeline finished — data saved to database!")