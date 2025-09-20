import pathlib
import csv

# locate input_files folder relative to this script
CURRENT_DIR = pathlib.Path(__file__).resolve().parent
PARENT_DIR = CURRENT_DIR.parent
INPUT_DIR = PARENT_DIR / "input_files"

# iterate through all CSV files in the folder
for csv_file in INPUT_DIR.glob("*.csv"):
    with open(csv_file, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader, None)  # skip header
        row_count = sum(1 for _ in reader)  # count remaining rows

    print(f"{csv_file.name}: {row_count} rows")
