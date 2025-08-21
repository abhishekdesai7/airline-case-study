import duckdb, pandas as pd, pathlib as p

ROOT = p.Path(__file__).resolve().parents[1]
db_path = ROOT / "warehouse" / "condor.duckdb"
db_path.parent.mkdir(parents=True, exist_ok=True)

xls = ROOT / "data_raw" / "Condor - Analytics Engineer 2025 Business Case DataSet_Final.xlsx"

booking = pd.read_excel(xls, sheet_name="Booking")
passenger = pd.read_excel(xls, sheet_name="Passenger")
flight = pd.read_excel(xls, sheet_name="Flight")

# Normalize columns
def norm(df):
    df.columns = (df.columns.str.strip()
                            .str.lower()
                            .str.replace(' ', '_')
                            .str.replace('(', '', regex=False)
                            .str.replace(')', '', regex=False))
    return df

booking, passenger, flight = map(norm, [booking, passenger, flight])

con = duckdb.connect(str(db_path))
con.execute("CREATE SCHEMA IF NOT EXISTS raw;")
con.register("booking_df", booking)
con.register("passenger_df", passenger)
con.register("flight_df", flight)

con.execute("CREATE OR REPLACE TABLE raw.booking AS SELECT * FROM booking_df;")
con.execute("CREATE OR REPLACE TABLE raw.passenger AS SELECT * FROM passenger_df;")
con.execute("CREATE OR REPLACE TABLE raw.flight AS SELECT * FROM flight_df;")
con.close()

print("Ingest complete → warehouse/condor.duckdb")
