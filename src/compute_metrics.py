import duckdb, yaml, pathlib as p, pandas as pd

ROOT = p.Path(__file__).resolve().parents[1]
cfg = yaml.safe_load((ROOT / "config" / "metrics.yaml").read_text())
db = duckdb.connect(str(ROOT / "warehouse" / "condor.duckdb"))

# Re-define macros dynamically using config values
vcpsl = float(cfg['costs']['variable_cost_per_seat_leg'])
cvpct = float(cfg['uplifts']['connection_value_pct_of_ticket_rev'])
db.execute(f"CREATE OR REPLACE MACRO variable_cost_per_seat_leg() AS {vcpsl};")
db.execute(f"CREATE OR REPLACE MACRO connection_value_pct()        AS {cvpct};")

queries = {
    "fwlf_overall": "SELECT * FROM kpi.fwlf_overall;",
    "fwlf_by_segment": "SELECT * FROM kpi.fwlf_by_segment;",
    "pacs_leg": "SELECT * FROM kpi.pacs_leg;",
    "yalf_overall": "SELECT * FROM kpi.yalf_overall;",
    "aras_overall": "SELECT * FROM kpi.aras_overall;",
    "leg_detail": "SELECT * FROM mart.leg_with_yield_index;"
}

outdir = ROOT / "data_derived"
outdir.mkdir(exist_ok=True, parents=True)
for name, q in queries.items():
    df = db.execute(q).df()
    df.to_csv(outdir / f"{name}.csv", index=False)
    print("Exported:", name)

db.close()
