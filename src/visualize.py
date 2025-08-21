import pandas as pd, matplotlib.pyplot as plt, pathlib as p

ROOT = p.Path(__file__).resolve().parents[1]
derived = ROOT / "data_derived"
reports = ROOT / "reports"
reports.mkdir(exist_ok=True, parents=True)

fwlf_seg = pd.read_csv(derived / "fwlf_by_segment.csv")
leg = pd.read_csv(derived / "leg_detail.csv")
pacs = pd.read_csv(derived / "pacs_leg.csv")

# 1) FWLF by segment (bar)
plt.figure()
labels = fwlf_seg['origin'] + "→" + fwlf_seg['destination']
plt.bar(labels, fwlf_seg['fwlf'])
plt.title("FWLF by Segment")
plt.ylabel("FWLF")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig(reports / "fwlf_by_segment.png")
plt.close()

# 2) Revenue per Pax vs Load Factor (scatter)
leg['rev_per_pax'] = leg['total_revenue'] / leg['pax']
plt.figure()
plt.scatter(leg['leg_lf'], leg['rev_per_pax'], alpha=0.6)
plt.title("Revenue per Pax vs Load Factor (Leg)")
plt.xlabel("Load Factor")
plt.ylabel("Revenue per Pax (€)")
plt.tight_layout()
plt.savefig(reports / "rev_per_pax_vs_lf.png")
plt.close()

# 3) PACS per leg (bar top 20)
pacs_sorted = pacs.sort_values("pacs_per_seat_leg", ascending=False).head(20)
plt.figure()
labels = pacs_sorted['origin'] + "→" + pacs_sorted['destination'] + " " + pacs_sorted['flight_date'].astype(str)
plt.bar(labels, pacs_sorted['pacs_per_seat_leg'])
plt.title("Top 20 PACS per Seat-Leg")
plt.ylabel("€ per seat-leg")
plt.xticks(rotation=90)
plt.tight_layout()
plt.savefig(reports / "pacs_top20.png")
plt.close()

print("Charts saved to reports/")
