"""
SQL Window Functions — Week 1 Practice, on REAL data
=====================================================
Runs the 3 practice problems against your actual stock_data.db (400 rows of
real history). Output is sliced to recent rows so it stays readable.

Run it with:   python window_practice_real.py

Note on column names: the practice problems say "close_price" and "ticker";
your real table calls these "price" and "symbol". Same data, different labels.
"""

import sqlite3

conn = sqlite3.connect("stock_data.db")
cur = conn.cursor()


def run(title, query, params=()):
    print("\n" + "=" * 74)
    print(title)
    print("=" * 74)
    cur.execute(query, params)
    cols = [d[0] for d in cur.description]
    print(" | ".join(f"{c:>12}" for c in cols))
    print("-" * 74)
    for row in cur.fetchall():
        cells = [f"{v:>12.2f}" if isinstance(v, float) else f"{str(v):>12}" for v in row]
        print(" | ".join(cells))


# ---------------------------------------------------------------------------
# PROBLEM 1 — 5-day moving average (showing NVDA's most recent 10 days)
# ---------------------------------------------------------------------------
run("PROBLEM 1: 5-day moving average — NVDA, last 10 days", """
    SELECT * FROM (
        SELECT
            symbol, date, price,
            ROUND(AVG(price) OVER (
                PARTITION BY symbol ORDER BY date
                ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
            ), 2) AS moving_avg_5d
        FROM stocks
        WHERE symbol = 'NVDA'
        ORDER BY date DESC
        LIMIT 10
    ) ORDER BY date
""")

# ---------------------------------------------------------------------------
# PROBLEM 2 — day-over-day % change (showing NVDA's most recent 10 days)
# ---------------------------------------------------------------------------
run("PROBLEM 2: day-over-day % change — NVDA, last 10 days", """
    SELECT * FROM (
        SELECT
            symbol, date, price,
            ROUND(LAG(price) OVER (PARTITION BY symbol ORDER BY date), 2) AS prev_close,
            ROUND((price - LAG(price) OVER (PARTITION BY symbol ORDER BY date))
                  / LAG(price) OVER (PARTITION BY symbol ORDER BY date) * 100, 2) AS pct_change
        FROM stocks
        WHERE symbol = 'NVDA'
        ORDER BY date DESC
        LIMIT 10
    ) ORDER BY date
""")

# ---------------------------------------------------------------------------
# PROBLEM 3 — rank tickers by weekly % gain (showing the 4 most recent weeks)
# ---------------------------------------------------------------------------
run("PROBLEM 3: weekly % gain ranking — 4 most recent weeks", """
    WITH weekly AS (
        SELECT
            symbol,
            strftime('%Y-%W', date) AS week,
            FIRST_VALUE(price) OVER (
                PARTITION BY symbol, strftime('%Y-%W', date) ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS first_close,
            LAST_VALUE(price) OVER (
                PARTITION BY symbol, strftime('%Y-%W', date) ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS last_close
        FROM stocks
    ),
    gains AS (
        SELECT DISTINCT
            week, symbol,
            ROUND((last_close - first_close) / first_close * 100, 2) AS weekly_gain
        FROM weekly
    ),
    ranked AS (
        SELECT
            week, symbol, weekly_gain,
            RANK() OVER (PARTITION BY week ORDER BY weekly_gain DESC) AS rank_in_week
        FROM gains
    )
    SELECT week, symbol, weekly_gain, rank_in_week
    FROM ranked
    WHERE week IN (SELECT DISTINCT week FROM ranked ORDER BY week DESC LIMIT 4)
    ORDER BY week DESC, rank_in_week
""")

conn.close()
print("\nDone.")
