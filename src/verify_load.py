
import sqlite3
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
database_path = project_root / "data" / "processed" / "etl_database.db"

with sqlite3.connect(database_path) as connection:
    cursor = connection.cursor()

    for table in ["customers", "sales", "load_summary"]:
        print(f"\n--- {table.upper()} ---")
        cursor.execute(f"SELECT * FROM {table}")
        for row in cursor.fetchall():
            print(row)

    print("\n--- RECORD COUNTS ---")
    cursor.execute("SELECT COUNT(*) FROM customers")
    print("Customers in database:", cursor.fetchone()[0])

    cursor.execute("SELECT COUNT(*) FROM sales")
    print("Sales in database:", cursor.fetchone()[0])
