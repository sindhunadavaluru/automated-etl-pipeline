
import sqlite3
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
database_path = project_root / "data" / "processed" / "etl_database.db"

with sqlite3.connect(database_path) as connection:
    cursor = connection.cursor()

    cursor.execute("""
        SELECT run_id, started_at, ended_at, status,
               duration_seconds, source_sales,
               valid_sales, rejected_sales
        FROM pipeline_run_history
        ORDER BY started_at DESC
    """)

    for row in cursor.fetchall():
        print(row)
