
from pyspark.sql import functions as F


def transform_data(customers_df, sales_df):
    # Remove duplicate customer IDs
    clean_customers = customers_df.dropDuplicates(["customer_id"])

    # Remove duplicate sales IDs (keep one record per sale_id)
    deduplicated_sales = sales_df.dropDuplicates(["sale_id"])

    # Find sales whose customer IDs exist
    customer_ids = clean_customers.select("customer_id").distinct()

    sales_with_customer = deduplicated_sales.join(
        customer_ids.withColumn("customer_exists", F.lit(True)),
        on="customer_id",
        how="left"
    )

    # Define valid records
    valid_condition = (
        F.col("sale_id").isNotNull()
        & F.col("customer_id").isNotNull()
        & F.col("amount").isNotNull()
        & (F.col("amount") > 0)
        & F.col("sale_date").isNotNull()
        & F.col("customer_exists").isNotNull()
    )

    valid_sales = (
        sales_with_customer
        .filter(valid_condition)
        .drop("customer_exists")
    )

    # Everything that fails the validation rules is rejected
    rejected_sales = (
        sales_with_customer
        .filter(~valid_condition)
        .drop("customer_exists")
    )

    return clean_customers, valid_sales, rejected_sales
