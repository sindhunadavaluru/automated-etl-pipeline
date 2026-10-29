
import sqlite3
from pathlib import Path


def load_data(customers_df, valid_sales_df, database_path):
    """Load Spark DataFrames into SQLite without converting to Pandas."""

    database_path = Path(database_path)
    database_path.parent.mkdir(parents=True, exist_ok=True)

    customer_columns = customers_df.columns
    sales_columns = valid_sales_df.columns

    customer_rows = [
        tuple(row[column] for column in customer_columns)
        for row in customers_df.collect()
    ]

    sales_rows = [
        tuple(row[column] for column in sales_columns)
        for row in valid_sales_df.collect()
    ]

    with sqlite3.connect(database_path) as connection:
        cursor = connection.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS load_summary (
                table_name TEXT PRIMARY KEY,
                row_count INTEGER NOT NULL
            )
        """)

        # Re-create target tables for this initial full-load version.
        cursor.execute("DROP TABLE IF EXISTS customers")
        cursor.execute("DROP TABLE IF EXISTS sales")

        customer_definitions = ", ".join(
            f'"{column}" TEXT' for column in customer_columns
        )
        sales_definitions = ", ".join(
            f'"{column}" TEXT' for column in sales_columns
        )

        cursor.execute(f"CREATE TABLE customers ({customer_definitions})")
        cursor.execute(f"CREATE TABLE sales ({sales_definitions})")

        customer_placeholders = ", ".join(
            "?" for _ in customer_columns
        )
        sales_placeholders = ", ".join(
            "?" for _ in sales_columns
        )

        cursor.executemany(
            f"INSERT INTO customers VALUES ({customer_placeholders})",
            customer_rows
        )

        cursor.executemany(
            f"INSERT INTO sales VALUES ({sales_placeholders})",
            sales_rows
        )

        cursor.executemany("""
            INSERT OR REPLACE INTO load_summary (table_name, row_count)
            VALUES (?, ?)
        """, [
            ("customers", len(customer_rows)),
            ("sales", len(sales_rows))
        ])

    print(f"Database saved to: {database_path}")
    print(f"Customers loaded: {len(customer_rows)}")
    print(f"Valid sales loaded: {len(sales_rows)}")
