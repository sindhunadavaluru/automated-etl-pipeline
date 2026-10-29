
from pathlib import Path
from extract import extract_data

project_root = Path(__file__).resolve().parent.parent
missing_input_dir = project_root / "data" / "input_does_not_exist"

try:
    extract_data(None, missing_input_dir)
except FileNotFoundError as exc:
    print("PASS: Missing input files are detected.")
    print("Error:", exc)
else:
    raise AssertionError("Expected FileNotFoundError was not raised")
