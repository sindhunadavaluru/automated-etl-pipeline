# Automated ETL Pipeline Using PySpark and SQLite

## Project Overview

This project implements a local ETL (Extract, Transform, Load) pipeline using Python, PySpark, and SQLite. It extracts customer and sales data from CSV files, applies data-cleaning and validation rules, and loads the processed results into a SQLite database.

The pipeline also maintains execution logs and run history, including successful and failed runs.

## Technologies Used

- Python 3.11
- PySpark
- SQLite
- CSV
- Logging and exception handling
- pytest (installed for testing)

## Project Architecture

```text
Input CSV Files
      |
      v
   Extract
      |
      v
  Transform
      |
      +----> Valid Sales
      |
      +----> Rejected Sales
      |
      v
     Load
      |
      v
  SQLite Database
      |
      +----> Load Summary
      |
      +----> Pipeline Run History

Execution Logs --> logs/etl_pipeline.log
```

## Data Sources

- `data/input/customers.csv`
- `data/input/sales.csv`

## Transformation Rules

- Remove duplicate customer IDs.
- Remove duplicate sales IDs, retaining one record per sale ID.
- Reject records with missing sale IDs or customer IDs.
- Reject records with missing, zero, or negative amounts.
- Reject records with missing sale dates.
- Reject sales associated with customer IDs that do not exist in the customer data.

## Data Loading

The processed data is stored in `data/processed/etl_database.db`.

Database tables include:

- `customers`
- `sales`
- `load_summary`
- `pipeline_run_history`

The current implementation replaces the target customer and sales tables on each successful load (full refresh).

## Logging and Error Handling

The pipeline records execution details, errors, duration, record counts, and run status.

A missing input file raises a clear error. The pipeline catches the exception, logs the failure, and attempts to save the failed run to the pipeline history table.

## How to Run

Run the pipeline from the project root using the Python 3.11 virtual environment:

```powershell
.\.venv311\Scripts\python.exe src/pipeline.py
```

Verify the loaded data:

```powershell
.\.venv311\Scripts\python.exe src/verify_load.py
```

Verify pipeline execution history:

```powershell
.\.venv311\Scripts\python.exe src/verify_history.py
```

Run transformation validation checks:

```powershell
.\.venv311\Scripts\python.exe src/test_transform.py
```

Test missing input file handling:

```powershell
.\.venv311\Scripts\python.exe src/test_extract_failure.py
```

## Verified Results

Using the current sample data:

- Source customers: 5
- Source sales: 10
- Clean customers: 5
- Valid sales: 6
- Rejected sales: 3

One duplicate sale record is removed during deduplication, so it is not included in the rejected-sales count.

Successful pipeline runs and a simulated missing-input failure have been recorded in the run history.

## Future Enhancements

- Add automated unit tests with pytest.
- Add data-quality metrics and configurable validation rules.
- Add incremental loading.
- Add scheduling and pipeline monitoring.
- Extend the pipeline to cloud storage and a cloud data platform.
