
from pathlib import Path
import sqlite3
import time
from datetime import datetime, timezone

from pyspark.sql import SparkSession

from extract import extract_data
from transform import transform_data
from load import load_data
from logger_config import setup_logger


def main():
    project_root = Path(__file__).resolve().parent.parent
    input_dir = project_root / "data" / "input"
    database_path = project_root / "data" / "processed" / "etl_database.db"

    logger = setup_logger(project_root)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    started_at = datetime.now(timezone.utc).isoformat()
    start_time = time.time()

    status = "FAILED"
    error_message = None
    source_customers = 0
    source_sales = 0
    clean_customers_count = 0
    valid_sales_count = 0
    rejected_sales_count = 0

    spark = None

    try:
        logger.info("Pipeline run %s started", run_id)

        spark = (
            SparkSession.builder
            .master("local[*]")
            .appName("AutomatedETLPipeline")
            .getOrCreate()
        )

        logger.info("STEP 1: EXTRACT")
        customers_df, sales_df = extract_data(spark, input_dir)

        source_customers = customers_df.count()
        source_sales = sales_df.count()
        logger.info(
            "Extracted customers=%s, sales=%s",
            source_customers,
            source_sales
        )

        logger.info("STEP 2: TRANSFORM")
        clean_customers, valid_sales, rejected_sales = transform_data(
            customers_df, sales_df
        )

        clean_customers_count = clean_customers.count()
        valid_sales_count = valid_sales.count()
        rejected_sales_count = rejected_sales.count()

        logger.info(
            "Transformed customers=%s, valid_sales=%s, rejected_sales=%s",
            clean_customers_count,
            valid_sales_count,
            rejected_sales_count
        )

        logger.info("STEP 3: LOAD")
        load_data(clean_customers, valid_sales, database_path)

        status = "SUCCESS"
        logger.info("Pipeline run %s completed successfully", run_id)

    except Exception as exc:
        error_message = str(exc)
        logger.exception("Pipeline run %s failed", run_id)

    finally:
        if spark is not None:
            spark.stop()

        ended_at = datetime.now(timezone.utc).isoformat()
        duration_seconds = round(time.time() - start_time, 2)

        # Record every run, including failures, in SQLite.
        try:
            database_path.parent.mkdir(parents=True, exist_ok=True)

            with sqlite3.connect(database_path) as connection:
                connection.execute("""
                    CREATE TABLE IF NOT EXISTS pipeline_run_history (
                        run_id TEXT PRIMARY KEY,
                        started_at TEXT,
                        ended_at TEXT,
                        status TEXT,
                        duration_seconds REAL,
                        source_customers INTEGER,
                        source_sales INTEGER,
                        clean_customers INTEGER,
                        valid_sales INTEGER,
                        rejected_sales INTEGER,
                        error_message TEXT
                    )
                """)

                connection.execute("""
                    INSERT INTO pipeline_run_history VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                    )
                """, (
                    run_id,
                    started_at,
                    ended_at,
                    status,
                    duration_seconds,
                    source_customers,
                    source_sales,
                    clean_customers_count,
                    valid_sales_count,
                    rejected_sales_count,
                    error_message
                ))

        except Exception:
            logger.exception("Could not save pipeline run history")
            status = "FAILED"

        logger.info(
            "Run summary: status=%s, duration_seconds=%s",
            status,
            duration_seconds
        )

    if status != "SUCCESS":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
