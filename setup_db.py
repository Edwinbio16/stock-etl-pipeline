import sqlite3

connection = sqlite3.connect("stock_data.db")
cursor = connection.cursor()

cursor.execute("""
    CREATE TABLE IF NOT EXISTS stocks (
        symbol TEXT,
        date TEXT,
        open REAL,
        high REAL,
        low REAL,
        price REAL,
        PRIMARY KEY (symbol, date)
    )
""")

connection.commit()
connection.close()
print("Database setup complete.")