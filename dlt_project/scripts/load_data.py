# scripts/load_data.py
import duckdb
import pathlib
import sys
from datetime import date

DB = "dbt_project/reports/sources/final/dev.duckdb"
CURRENT_DIR = pathlib.Path(__file__).resolve().parent
PARENT_DIR = CURRENT_DIR.parent
INPUT_DIR = PARENT_DIR / "input_files/full_load"
conn = duckdb.connect(DB)

# get load_date: either user input (first arg) or today's date
if len(sys.argv) > 1:
    load_date = sys.argv[1]
else:
    load_date = str(date.today())

print(f"Using load_date = {load_date}")

# create raw schema
conn.execute("CREATE SCHEMA IF NOT EXISTS raw;")

files = {
    "ports": "ports.csv",
    "regions": "regions.csv",
    "exchange_rates": "exchange_rates.csv",
    "datapoints": "datapoints_1.csv",
    "charges": "charges_1.csv"
}

for table, fname in files.items():
    full_fname = f"DE_casestudy_{fname}"
    path = INPUT_DIR / full_fname
    print("Loading", table, path)
    # drop + recreate with load_date
    conn.execute(f"DROP TABLE IF EXISTS raw.{table};")
    conn.execute(f"""
        CREATE TABLE raw.{table} AS
        SELECT *, DATE '{load_date}' AS load_date
        FROM read_csv_auto('{path}', header=True);
    """)

print("Done loading raw files into", DB)
