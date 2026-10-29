
from pathlib import Path
from pyspark.sql import SparkSession

from extract import extract_data
from transform import transform_data


project_root = Path(__file__).resolve().parent.parent
input_dir = project_root / "data" / "input"

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("ETLTransformationTest")
    .getOrCreate()
)

try:
    customers_df, sales_df = extract_data(spark, input_dir)

    clean_customers, valid_sales, rejected_sales = transform_data(
        customers_df, sales_df
    )

    # Verify expected record counts
    assert clean_customers.count() == 5, "Customer count is incorrect"
    assert valid_sales.count() == 6, "Valid sales count is incorrect"
    assert rejected_sales.count() == 3, "Rejected sales count is incorrect"

    # Verify invalid amounts are not loaded as valid sales
    assert valid_sales.filter("amount <= 0").count() == 0
    assert valid_sales.filter("amount IS NULL").count() == 0

    print("PASS: Customer count is correct")
    print("PASS: Valid sales count is correct")
    print("PASS: Rejected sales count is correct")
    print("PASS: Invalid amounts are excluded")
    print("\nAll transformation tests passed!")

finally:
    spark.stop()
