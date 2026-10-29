
import os
import sys
from pathlib import Path

# 1. Identify the Python interpreter in the active environment
PROJECT_ROOT = Path(__file__).resolve().parent.parent
PYTHON_EXE = sys.executable

# 2. Tell PySpark which Python executable to use
os.environ["PYSPARK_PYTHON"] = PYTHON_EXE
os.environ["PYSPARK_DRIVER_PYTHON"] = PYTHON_EXE

# 3. Import PySpark and the transformation function
from pyspark.sql import SparkSession

sys.path.insert(0, str(PROJECT_ROOT / "src"))
from transform import transform_data


def test_transform_counts():
    """Verify valid and rejected records are counted correctly."""

    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("ETLTransformTests")
        .config("spark.pyspark.python", PYTHON_EXE)
        .config("spark.pyspark.driver.python", PYTHON_EXE)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        # Sample customer data
        customers = spark.createDataFrame(
            [
                ("C101", "Ravi", "Hyderabad"),
                ("C102", "Priya", "Chennai"),
            ],
            ["customer_id", "name", "city"],
        )

        # Sample sales data containing duplicate and invalid records
        sales = spark.createDataFrame(
            [
                ("S001", "C101", 1500, "2026-10-25"),
                ("S002", "C102", 2500, "2026-10-25"),
                ("S002", "C102", 2500, "2026-10-25"),
                ("S003", "C101", -500, "2026-10-26"),
                ("S004", "C999", 900, "2026-10-26"),
                ("S005", "C102", None, "2026-10-27"),
            ],
            ["sale_id", "customer_id", "amount", "sale_date"],
        )

        # Execute the transformation
        clean_customers, valid_sales, rejected_sales = transform_data(
            customers, sales
        )

        # Validate the expected record counts
        assert clean_customers.count() == 2
        assert valid_sales.count() == 2
        assert rejected_sales.count() == 3

    finally:
        spark.stop()
