import duckdb
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# Paths
ROOT = Path(__file__).resolve().parents[1]
DB   = ROOT / "warehouse" / "condor.duckdb"
REP  = ROOT / "reports"
REP.mkdir(parents=True, exist_ok=True)

con = duckdb.connect(str(DB))

# Ensure views exist (you already ran the SQL file)
# If needed, run: duckdb warehouse/condor.duckdb ".read sql/partB_exploration.sql"

# --- Pull dataframes ---
fwlf_seg   = con.execute("SELECT origin, destination, fwlf FROM kpi_fwlf_by_segment;").df()
lf_time    = con.execute("SELECT timeofday, avg_lf FROM exp_lf_by_time;").df()
anc_seg    = con.execute("SELECT origin, destination, anc_share FROM exp_anc_share_segment;").df()
cancel_seg = con.execute("SELECT origin, destination, cancel_rate FROM exp_cancel_rate_segment;").df()
leg_detail = con.execute("SELECT * FROM mart_leg_revpp;").df()
pacs_leg   = con.execute("""SELECT flightnumber, flight_date, origin, destination, pacs_per_seat_leg
                            FROM kpi_pacs_leg;""").df()

# --- 1) FWLF by Segment (bar) ---
plt.figure(figsize=(8,4))
labels = fwlf_seg['origin'] + "→" + fwlf_seg['destination']
plt.bar(labels, fwlf_seg['fwlf'])
plt.title("FWLF by Segment")
plt.ylabel("FWLF")
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.savefig(REP / "fwlf_by_segment.png")
plt.close()

# --- 2) Avg Load Factor by Time of Day (bar) ---
plt.figure(figsize=(6,4))
order = ['Morning','Afternoon','Evening']
lf_time_sorted = lf_time.set_index('timeofday').reindex(order).reset_index()
plt.bar(lf_time_sorted['timeofday'], lf_time_sorted['avg_lf'])
plt.title("Average Load Factor by Time of Day")
plt.ylabel("Load Factor")
plt.tight_layout()
plt.savefig(REP / "lf_by_time_of_day.png")
plt.close()

# --- 3) Ancillary Share by Segment (bar) ---
plt.figure(figsize=(8,4))
labels = anc_seg['origin'] + "→" + anc_seg['destination']
plt.bar(labels, anc_seg['anc_share'])
plt.title("Ancillary Revenue Share by Segment")
plt.ylabel("Ancillary Share of Total Revenue")
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.savefig(REP / "anc_share_by_segment.png")
plt.close()

# --- 4) Cancellation Rate by Segment (bar) ---
plt.figure(figsize=(8,4))
labels = cancel_seg['origin'] + "→" + cancel_seg['destination']
plt.bar(labels, cancel_seg['cancel_rate'])
plt.title("Cancellation Rate by Segment")
plt.ylabel("Cancellation Rate")
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.savefig(REP / "cancel_rate_by_segment.png")
plt.close()

# --- 5) Revenue per Passenger vs Load Factor (scatter) ---
plt.figure(figsize=(6,4))
# guard against division issues already handled in SQL view; still dropna to be safe
plot_df = leg_detail[['leg_lf','rev_per_pax']].dropna()
plt.scatter(plot_df['leg_lf'], plot_df['rev_per_pax'], alpha=0.6)
plt.title("Revenue per Passenger vs Load Factor (Leg)")
plt.xlabel("Load Factor")
plt.ylabel("Revenue per Pax (€)")
plt.tight_layout()
plt.savefig(REP / "rev_per_pax_vs_lf.png")
plt.close()

# --- 6) PACS per Leg: Top 20 (bar) ---
plt.figure(figsize=(10,4))
pacs_top = pacs_leg.sort_values('pacs_per_seat_leg', ascending=False).head(20).copy()
labels = (pacs_top['origin'] + "→" + pacs_top['destination'] + " " 
          + pacs_top['flight_date'].astype(str))
plt.bar(labels, pacs_top['pacs_per_seat_leg'])
plt.title("Top 20 PACS per Seat-Leg")
plt.ylabel("€ per seat-leg")
plt.xticks(rotation=90)
plt.tight_layout()
plt.savefig(REP / "pacs_top20.png")
plt.close()

# --- 7) PACS per Leg: Bottom 20 (bar) ---
plt.figure(figsize=(10,4))
pacs_bottom = pacs_leg.sort_values('pacs_per_seat_leg', ascending=True).head(20).copy()
labels = (pacs_bottom['origin'] + "→" + pacs_bottom['destination'] + " " 
          + pacs_bottom['flight_date'].astype(str))
plt.bar(labels, pacs_bottom['pacs_per_seat_leg'])
plt.title("Bottom 20 PACS per Seat-Leg")
plt.ylabel("€ per seat-leg")
plt.xticks(rotation=90)
plt.tight_layout()
plt.savefig(REP / "pacs_bottom20.png")
plt.close()

print("Saved charts to:", REP)