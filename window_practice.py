"""
SQL Window Functions — Week 1 Practice
=======================================
This script builds a SAMPLE dataset (12 trading days x 3 tickers) in an
in-memory SQLite database, then runs the 3 practice problems so you can SEE
window functions working. Your real stock_data.db is NOT touched.

Run it with:   python window_practice.py
"""

import sqlite3

# ---------------------------------------------------------------------------
# 1. Build a throwaway in-memory database (":memory:" = nothing saved to disk)
# ---------------------------------------------------------------------------
conn = sqlite3.connect(":memory:")
cur = conn.cursor()

cur.execute("""
    CREATE TABLE stocks (
        symbol TEXT,
        date   TEXT,
        close_price REAL,
        PRIMARY KEY (symbol, date)
    )
""")

# 12 days of made-up-but-realistic closing prices for 3 tickers.
sample = [
    # AAPL
    ("AAPL", "2026-06-01", 300.00), ("AAPL", "2026-06-02", 303.50),
    ("AAPL", "2026-06-03", 301.20), ("AAPL", "2026-06-04", 305.80),
    ("AAPL", "2026-06-05", 307.34), ("AAPL", "2026-06-08", 309.10),
    ("AAPL", "2026-06-09", 306.40), ("AAPL", "2026-06-10", 311.75),
    ("AAPL", "2026-06-11", 314.20), ("AAPL", "2026-06-12", 312.90),
    ("AAPL", "2026-06-15", 318.05), ("AAPL", "2026-06-16", 320.40),
    # MSFT
    ("MSFT", "2026-06-01", 410.00), ("MSFT", "2026-06-02", 412.30),
    ("MSFT", "2026-06-03", 408.90), ("MSFT", "2026-06-04", 415.60),
    ("MSFT", "2026-06-05", 416.67), ("MSFT", "2026-06-08", 419.20),
    ("MSFT", "2026-06-09", 414.80), ("MSFT", "2026-06-10", 421.50),
    ("MSFT", "2026-06-11", 423.10), ("MSFT", "2026-06-12", 420.75),
    ("MSFT", "2026-06-15", 425.90), ("MSFT", "2026-06-16", 428.40),
    # NVDA
    ("NVDA", "2026-06-01", 120.00), ("NVDA", "2026-06-02", 124.50),
    ("NVDA", "2026-06-03", 122.10), ("NVDA", "2026-06-04", 128.30),
    ("NVDA", "2026-06-05", 131.00), ("NVDA", "2026-06-08", 129.40),
    ("NVDA", "2026-06-09", 133.80), ("NVDA", "2026-06-10", 138.20),
    ("NVDA", "2026-06-11", 135.60), ("NVDA", "2026-06-12", 140.10),
    ("NVDA", "2026-06-15", 144.30), ("NVDA", "2026-06-16", 142.00),
]
cur.executemany("INSERT INTO stocks VALUES (?, ?, ?)", sample)
conn.commit()


def run(title, query):
    """Helper: print a titled query and its results as a simple table."""
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)
    cur.execute(query)
    cols = [d[0] for d in cur.description]
    print(" | ".join(f"{c:>12}" for c in cols))
    print("-" * 70)
    for row in cur.fetchall():
        cells = []
        for v in row:
            cells.append(f"{v:>12.2f}" if isinstance(v, float) else f"{str(v):>12}")
        print(" | ".join(cells))


# ---------------------------------------------------------------------------
# PROBLEM 1 — 5-day moving average
# ---------------------------------------------------------------------------
run("PROBLEM 1: 5-day moving average of close_price", """
    SELECT
        symbol,
        date,
        close_price,
        AVG(close_price) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS moving_avg_5d
    FROM stocks
    ORDER BY symbol, date
""")

# ---------------------------------------------------------------------------
# PROBLEM 2 — day-over-day % change using LAG
# ---------------------------------------------------------------------------
run("PROBLEM 2: day-over-day % change", """
    SELECT
        symbol,
        date,
        close_price,
        LAG(close_price, 1) OVER (
            PARTITION BY symbol ORDER BY date
        ) AS prev_close,
        (close_price - LAG(close_price, 1) OVER (PARTITION BY symbol ORDER BY date))
            / LAG(close_price, 1) OVER (PARTITION BY symbol ORDER BY date) * 100
            AS pct_change
    FROM stocks
    ORDER BY symbol, date
""")

# ---------------------------------------------------------------------------
# PROBLEM 3 — rank tickers by weekly % gain
# ---------------------------------------------------------------------------
run("PROBLEM 3: rank tickers by weekly % gain", """
    WITH weekly AS (
        SELECT
            symbol,
            strftime('%W', date) AS week,
            -- first close of the week for this ticker
            FIRST_VALUE(close_price) OVER (
                PARTITION BY symbol, strftime('%W', date) ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS first_close,
            -- last close of the week for this ticker
            LAST_VALUE(close_price) OVER (
                PARTITION BY symbol, strftime('%W', date) ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS last_close
        FROM stocks
    ),
    gains AS (
        SELECT DISTINCT
            week,
            symbol,
            (last_close - first_close) / first_close * 100 AS weekly_gain
        FROM weekly
    )
    SELECT
        week,
        symbol,
        weekly_gain,
        RANK() OVER (PARTITION BY week ORDER BY weekly_gain DESC) AS rank_in_week
    FROM gains
    ORDER BY week, rank_in_week
""")

conn.close()
print("\nDone.")
