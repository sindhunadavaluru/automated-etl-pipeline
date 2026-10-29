
from pathlib import Path
from pyspark.sql import SparkSession


def extract_data(spark, input_dir):
    """Read customer and sales CSV files into Spark DataFrames."""

    input_path = Path(input_dir)

    customers_path = input_path / "customers.csv"
    sales_path = input_path / "sales.csv"

    # Check that both source files exist
    if not customers_path.exists():
        raise FileNotFoundError(f"Customer file not found: {customers_path}")

    if not sales_path.exists():
        raise FileNotFoundError(f"Sales file not found: {sales_path}")

    # Read CSV files and use the first row as column headers
    customers_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(str(customers_path))
    )

    sales_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(str(sales_path))
    )

    return customers_df, sales_df


if __name__ == "__main__":
    project_root = Path(__file__).resolve().parent.parent
    input_dir = project_root / "data" / "input"

    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("AutomatedETLPipeline")
        .getOrCreate()
    )

    try:
        customers_df, sales_df = extract_data(spark, input_dir)

        print("CUSTOMERS DATA")
        customers_df.show()

        print("SALES DATA")
        sales_df.show()

        print("Customer record count:", customers_df.count())
        print("Sales record count:", sales_df.count())

    finally:
        spark.stop()
