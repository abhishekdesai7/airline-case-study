import duckdb, pathlib as p

ROOT = p.Path(__file__).resolve().parents[1]
db = duckdb.connect(str(ROOT / "warehouse" / "condor.duckdb"))

db.execute("CREATE SCHEMA IF NOT EXISTS config;")
db.execute("CREATE OR REPLACE TABLE config.params AS SELECT 25.0 AS variable_cost_per_seat_leg, 0.15 AS connection_value_pct;")


for f in [
    "00_schema.sql",
    "05_params.sql",          # <-- NEW: must run before KPIs
    "10_models_mart.sql",
    "20_kpis_core.sql",
    "30_kpis_diagnostics.sql",
    "99_checks.sql"
]:
    sql_path = ROOT / "sql" / f
    db.execute(sql_path.read_text())
    print(f"Executed: {f}")

db.close()
print("All views/checks created.")
