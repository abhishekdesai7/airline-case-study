import argparse
from pathlib import Path
import re
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# ---------- Helpers
def clean_cols(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = out.columns.str.strip().str.replace(r"\s+", "_", regex=True).str.lower()
    return out

def normalize_booking_id(x):
    if pd.isna(x): return np.nan
    s = str(x).strip()
    s_num = re.sub(r"\D+", "", s)             # keep digits only
    return s_num if s_num else np.nan

def norm_text_channel(s):
    if pd.isna(s): return s
    s2 = str(s).strip().lower()
    mapping = {
        "condor app": "condor app",
        "condor-app": "condor app",
        "app": "condor app",
        "website": "website",
        "web": "website",
        "call center": "call center",
        "travel agency": "travel agency",
        "ota": "ota",
        "online travel agency": "ota",
    }
    return mapping.get(s2, s2)

def ensure_dirs(base: Path):
    (base / "tables").mkdir(parents=True, exist_ok=True)
    (base / "charts").mkdir(parents=True, exist_ok=True)

def save_chart(figpath: Path):
    plt.tight_layout()
    plt.savefig(figpath)
    plt.close()

# ---------- Analysis & KPIs
def run_analysis(xlsx_path: Path, out_dir: Path):
    ensure_dirs(out_dir)

    tables_dir = out_dir / "tables"
    charts_dir = out_dir / "charts"


    # Load sheets
    xl = pd.ExcelFile(xlsx_path)
    booking = clean_cols(pd.read_excel(xl, "Booking"))
    passenger = clean_cols(pd.read_excel(xl, "Passenger"))
    flight = clean_cols(pd.read_excel(xl, "Flight"))

    # Normalize keys & fields
    booking["bookingid_norm"] = booking["bookingid_(pnr)"].apply(normalize_booking_id)
    passenger["bookingid_norm"] = passenger["bookingid"].apply(normalize_booking_id)

    # Dates / numerics
    for c in ["reservation_date", "cancellation_date", "flight_date"]:
        booking[c] = pd.to_datetime(booking[c], errors="coerce")
    flight["flightdate"] = pd.to_datetime(flight["flightdate"], errors="coerce")

    rev_cols = [
        "revenue_per_booking_(ticket)",
        "revenue_per_booking_(ancilliary_pre_check_in)",
        "revenue_per_booking_(ancilliary_at_check_in)",
    ]
    for c in rev_cols:
        booking[c] = pd.to_numeric(booking[c], errors="coerce")
    booking["total_revenue"] = booking[rev_cols].sum(axis=1, skipna=True)
    booking["passengercount"] = pd.to_numeric(booking["passengercount"], errors="coerce")

    # Canonical routing & channel/device normalization
    booking["origin"] = booking["origin"].astype(str).str.upper().str.strip()
    booking["destination"] = booking["destination"].astype(str).str.upper().str.strip()
    booking["routing_canonical"] = booking["origin"] + "-" + booking["destination"]

    passenger["booking_channel_norm"] = passenger["booking_channel"].apply(norm_text_channel)
    passenger["device_used_norm"] = passenger["device_used"].astype(str).str.strip().str.lower()

    # Flags & derived
    booking["is_cancelled"] = booking["cancellation_date"].notna()
    booking["flight_dow"] = booking["flight_date"].dt.day_name()
    booking["days_after_booking_to_cancel"] = (booking["cancellation_date"] - booking["reservation_date"]).dt.days
    booking["days_before_flight_to_cancel"] = (booking["flight_date"] - booking["cancellation_date"]).dt.days
    booking["lead_time_days"] = (booking["flight_date"] - booking["reservation_date"]).dt.days
    booking["ancillary_total"] = booking["revenue_per_booking_(ancilliary_pre_check_in)"].fillna(0) + \
                                 booking["revenue_per_booking_(ancilliary_at_check_in)"].fillna(0)

    # ---- Join Passenger → Booking (primary channel by PNR)
    pnr_channel = (passenger
                   .groupby("bookingid_norm")
                   .agg(primary_channel=("booking_channel_norm", lambda s: next((v for v in s if pd.notna(v)), np.nan)),
                        devices=("device_used_norm", lambda s: pd.Series([v for v in s if pd.notna(v)]).unique().tolist()))
                   .reset_index())
    booking_enriched = booking.merge(pnr_channel[["bookingid_norm", "primary_channel"]], on="bookingid_norm", how="left")

    # ---- Leg table: Booking × Flight
    leg = (booking.groupby(["flightnumber", "flight_date", "origin", "destination"], dropna=False)
           .agg(pax=("passengercount", "sum"),
                revenue=("total_revenue", "sum"),
                cancels=("is_cancelled", "sum"))
           .reset_index())
    leg = leg.merge(flight[["flightnumber", "flightdate", "availablecapacity", "timeofday", "routetype"]],
                    left_on=["flightnumber", "flight_date"],
                    right_on=["flightnumber", "flightdate"],
                    how="left").drop(columns=["flightdate"])
    leg["load_factor"] = np.where(leg["availablecapacity"] > 0, leg["pax"] / leg["availablecapacity"], np.nan)
    leg["rev_per_pax"] = np.where(leg["pax"] > 0, leg["revenue"] / leg["pax"], np.nan)

    # ---- Core tables
    # A) Cancellations by DOW (count and rate)
    dow_order = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
    cxl_by_dow = (booking[booking["is_cancelled"]]
                  .groupby("flight_dow").size()
                  .reindex(dow_order).fillna(0).astype(int).reset_index(name="cancel_count"))
    bookings_by_dow = (booking.groupby("flight_dow").size()
                       .reindex(dow_order).fillna(0).astype(int).reset_index(name="bookings"))
    cxl_rate_by_dow = bookings_by_dow.merge(cxl_by_dow, on="flight_dow", how="left")
    cxl_rate_by_dow["cancel_rate"] = np.where(cxl_rate_by_dow["bookings"]>0,
                                              cxl_rate_by_dow["cancel_count"]/cxl_rate_by_dow["bookings"], np.nan)

    # B) Cxl distributions (after booking / before flight)
    cxl_after_booking_dist = (booking[booking["is_cancelled"]]
                              .groupby("days_after_booking_to_cancel").size()
                              .reset_index(name="count").sort_values("days_after_booking_to_cancel"))
    cxl_before_flight_dist = (booking[booking["is_cancelled"]]
                              .groupby("days_before_flight_to_cancel").size()
                              .reset_index(name="count").sort_values("days_before_flight_to_cancel"))

    # C) Channel cancellation rate
    cxl_by_channel = (booking_enriched.groupby("primary_channel")
                      .agg(bookings=("bookingid_norm","count"),
                           cancels=("is_cancelled","sum")).reset_index())
    cxl_by_channel["cancel_rate"] = np.where(cxl_by_channel["bookings"]>0,
                                             cxl_by_channel["cancels"]/cxl_by_channel["bookings"], np.nan)

    # D) LF by DOW & time of day
    lf_by_date = (leg.groupby("flight_date").agg(load=("load_factor","mean")).reset_index())
    lf_by_date["dow"] = lf_by_date["flight_date"].dt.day_name()
    lf_by_dow = (lf_by_date.groupby("dow")["load"].mean()
                 .reindex(dow_order).reset_index().rename(columns={"load":"avg_load_factor"}))
    lf_by_tod = leg.groupby("timeofday")["load_factor"].mean().reset_index().rename(columns={"load_factor":"avg_load_factor"})

    # D1) --- LF by Segment & Day of Week (long + wide tables)
    # Build DOW on leg grain first (use same dow_order defined above)
    leg['dow'] = leg['flight_date'].dt.day_name()
    lf_seg_dow = (leg.groupby(['origin', 'destination', 'dow'])['load_factor']
                     .mean()
                     .reset_index())
    lf_seg_dow['segment'] = lf_seg_dow['origin'] + '→' + lf_seg_dow['destination']
    lf_seg_dow['dow'] = pd.Categorical(lf_seg_dow['dow'], categories=dow_order, ordered=True)

    # Pivot to wide for charting: rows = DOW, columns = segment
    lf_seg_dow_wide = (lf_seg_dow.pivot_table(index='dow',
                                              columns='segment',
                                              values='load_factor',
                                              aggfunc='mean')
                                  .sort_index())

    # Save both long and wide tables
    lf_seg_dow.to_csv(tables_dir / "lf_by_segment_by_dow_long.csv", index=False)
    lf_seg_dow_wide.to_csv(tables_dir / "lf_by_segment_by_dow_wide.csv", index=True)

    # E) Segment KPIs (OD level)
    seg = (booking.groupby(["origin","destination"])
           .agg(bookings=("bookingid_norm","count"),
                cancels=("is_cancelled","sum"),
                pax=("passengercount","sum"),
                revenue=("total_revenue","sum")).reset_index())
    seg["cancel_rate"] = np.where(seg["bookings"]>0, seg["cancels"]/seg["bookings"], np.nan)
    seg["rev_per_pax"] = np.where(seg["pax"]>0, seg["revenue"]/seg["pax"], np.nan)
    anc_denom = booking.groupby(["origin","destination"])["total_revenue"].sum().reset_index(name="denom")
    anc_num = booking.groupby(["origin","destination"])["ancillary_total"].sum().reset_index(name="num")
    seg = seg.merge(anc_denom, on=["origin","destination"]).merge(anc_num, on=["origin","destination"])
    seg["anc_share"] = np.where(seg["denom"]>0, seg["num"]/seg["denom"], np.nan)

    # F) Directional imbalance (LF OD vs reverse OD)
    lf_seg = leg.groupby(["origin","destination"])["load_factor"].mean().reset_index()
    lf_seg_rev = lf_seg.rename(columns={"origin":"destination", "destination":"origin", "load_factor":"load_factor_rev"})
    imb = lf_seg.merge(lf_seg_rev, on=["origin","destination"], how="left")
    imb["lf_diff_vs_reverse"] = imb["load_factor"] - imb["load_factor_rev"]

    # G) Rev/LF correlation (leg grain)
    rev_lf_corr = np.nan
    corr_df = leg[["load_factor","rev_per_pax"]].dropna()
    if corr_df.shape[0] > 2:
        rev_lf_corr = float(corr_df.corr().iloc[0,1])

    # ------------ Save tables
    tables_dir = out_dir / "tables"
    cxl_rate_by_dow.to_csv(tables_dir / "cancellation_by_dow.csv", index=False)
    cxl_after_booking_dist.to_csv(tables_dir / "cancellation_days_after_booking.csv", index=False)
    cxl_before_flight_dist.to_csv(tables_dir / "cancellation_days_before_flight.csv", index=False)
    cxl_by_channel.sort_values("bookings", ascending=False).to_csv(tables_dir / "cancellation_by_channel.csv", index=False)
    lf_by_dow.to_csv(tables_dir / "lf_by_dow.csv", index=False)
    lf_by_tod.to_csv(tables_dir / "lf_by_time_of_day.csv", index=False)
    seg.sort_values("revenue", ascending=False).to_csv(tables_dir / "segment_kpis.csv", index=False)
    imb.sort_values("lf_diff_vs_reverse", ascending=False).to_csv(tables_dir / "directional_imbalance.csv", index=False)
    pd.DataFrame({"rev_lf_corr":[rev_lf_corr]}).to_csv(tables_dir / "rev_lf_correlation.csv", index=False)
    booking.to_csv(tables_dir / "booking_clean.csv", index=False)
    leg.to_csv(tables_dir / "leg_table.csv", index=False)

    # ------------ Charts (matplotlib; one chart per figure, default style/colors)
    charts_dir = out_dir / "charts"

    # 1) Cancellations by DOW (bars)
    plt.figure(figsize=(7,4))
    plt.bar(cxl_rate_by_dow["flight_dow"], cxl_rate_by_dow["cancel_count"])
    plt.title("Cancellations by Flight Day of Week (count)")
    plt.xticks(rotation=30)
    save_chart(charts_dir / "cxl_by_dow_count.png")

    plt.figure(figsize=(7,4))
    plt.bar(cxl_rate_by_dow["flight_dow"], cxl_rate_by_dow["cancel_rate"])
    plt.title("Cancellation Rate by Flight Day of Week")
    plt.xticks(rotation=30)
    save_chart(charts_dir / "cxl_by_dow_rate.png")

    # 2) Cancellation distributions (lines)
    plt.figure(figsize=(7,4))
    plt.plot(cxl_after_booking_dist["days_after_booking_to_cancel"], cxl_after_booking_dist["count"])
    plt.title("Cancellations — Days After Booking")
    plt.xlabel("Days after booking"); plt.ylabel("Count")
    save_chart(charts_dir / "cxl_days_after_booking.png")

    plt.figure(figsize=(7,4))
    plt.plot(cxl_before_flight_dist["days_before_flight_to_cancel"], cxl_before_flight_dist["count"])
    plt.title("Cancellations — Days Before Flight")
    plt.xlabel("Days before flight"); plt.ylabel("Count")
    save_chart(charts_dir / "cxl_days_before_flight.png")

    # 3) LF by DOW and Time of Day
    plt.figure(figsize=(7,4))
    plt.bar(lf_by_dow["dow"], lf_by_dow["avg_load_factor"])
    plt.title("Average Load Factor by Day of Week")
    plt.xticks(rotation=30)
    save_chart(charts_dir / "lf_by_dow.png")

    plt.figure(figsize=(6,4))
    plt.bar(lf_by_tod["timeofday"], lf_by_tod["avg_load_factor"])
    plt.title("Average Load Factor by Time of Day")
    save_chart(charts_dir / "lf_by_time_of_day.png")

    # Chart: Load Factor by Segment and Day of Week (multi-line)
    plt.figure(figsize=(9,5))
    for segment_label in lf_seg_dow_wide.columns:
        plt.plot(lf_seg_dow_wide.index.astype(str), lf_seg_dow_wide[segment_label])

    plt.title("Load Factor by Segment and Day of Week")
    plt.xlabel("Day of Week")
    plt.ylabel("Average Load Factor")
    plt.legend(title="Segment", bbox_to_anchor=(1.02, 1), loc="upper left", borderaxespad=0.)
    save_chart(charts_dir / "lf_by_segment_by_dow.png")


    # 4) Revenue per pax vs LF (scatter)
    plt.figure(figsize=(6,4))
    plt.scatter(leg["load_factor"], leg["rev_per_pax"])
    plt.title("Revenue per Passenger vs Load Factor (Leg Level)")
    plt.xlabel("Load Factor"); plt.ylabel("Revenue per Passenger (€)")
    save_chart(charts_dir / "rev_per_pax_vs_lf.png")

    # 5) Ancillary share by top segments (barh)
    seg_top = seg.sort_values("revenue", ascending=False).head(8).copy()
    seg_top["od"] = seg_top["origin"] + "→" + seg_top["destination"]
    plt.figure(figsize=(7,4))
    plt.barh(seg_top["od"], seg_top["anc_share"])
    plt.title("Ancillary Share — Top Segments by Revenue")
    plt.gca().invert_yaxis()
    save_chart(charts_dir / "anc_share_top_segments.png")

    # ------------ Auto-generated insights & recommendations (markdown)
    insights = []
    # A) Zero-cancel days
    zero_cancel_days = cxl_rate_by_dow.loc[cxl_rate_by_dow["cancel_count"]==0, "flight_dow"].dropna().tolist()
    if zero_cancel_days:
        insights.append(f"- No cancellations recorded on: {', '.join(zero_cancel_days)} → Avoid overbooking on these days; use standard buffers only.")

    # B) Highest cancel day & rate
    cx_max = cxl_rate_by_dow.loc[cxl_rate_by_dow["cancel_rate"].fillna(-1).idxmax()]
    if pd.notna(cx_max["cancel_rate"]):
        insights.append(f"- Highest cancellation rate: {cx_max['flight_dow']} (rate={cx_max['cancel_rate']:.1%}) → tighten fare fences and increase overbooking buffers on this weekday.")

    # C) Days-after-booking median / p90
    if booking["is_cancelled"].any():
        med_after = booking.loc[booking["is_cancelled"], "days_after_booking_to_cancel"].median()
        p90_after = booking.loc[booking["is_cancelled"], "days_after_booking_to_cancel"].quantile(0.90)
        insights.append(f"- Median days after booking to cancel ≈ {med_after:.0f}, P90 ≈ {p90_after:.0f} → design refund rules & retention nudges within these windows.")

    # D) Days-before-flight median / p90
        med_before = booking.loc[booking["is_cancelled"], "days_before_flight_to_cancel"].median()
        p90_before = booking.loc[booking["is_cancelled"], "days_before_flight_to_cancel"].quantile(0.90)
        insights.append(f"- Median days before flight to cancel ≈ {med_before:.0f}, P90 ≈ {p90_before:.0f} → calibrate overbooking and release of low fares accordingly.")

    # E) Channels with highest cancel rate
    chan_sorted = cxl_by_channel.sort_values("cancel_rate", ascending=False)
    chan_sorted = chan_sorted[chan_sorted["bookings"]>25]  # ignore tiny samples
    if not chan_sorted.empty and pd.notna(chan_sorted.iloc[0]["cancel_rate"]):
        insights.append(f"- Channel with highest cancel rate: {chan_sorted.iloc[0]['primary_channel']} (≈ {chan_sorted.iloc[0]['cancel_rate']:.1%}) → steer demand to direct channels or adjust partner SLAs.")

    # F) LF by DOW: lowest day
    lf_low = lf_by_dow.loc[lf_by_dow["avg_load_factor"].idxmin()]
    lf_high = lf_by_dow.loc[lf_by_dow["avg_load_factor"].idxmax()]
    insights.append(f"- Lowest average LF: {lf_low['dow']} (≈ {lf_low['avg_load_factor']:.1%}); Highest: {lf_high['dow']} (≈ {lf_high['avg_load_factor']:.1%}) → re-time or stimulate low days; protect yield on high days.")

    # G) Time of day
    if lf_by_tod["avg_load_factor"].notna().any():
        tod_low = lf_by_tod.loc[lf_by_tod["avg_load_factor"].idxmin()]
        tod_high = lf_by_tod.loc[lf_by_tod["avg_load_factor"].idxmax()]
        insights.append(f"- Time-of-day LF: weakest={tod_low['timeofday']} (≈ {tod_low['avg_load_factor']:.1%}), strongest={tod_high['timeofday']} (≈ {tod_high['avg_load_factor']:.1%}) → bank timing & pricing by day-part.")

    # H) Rev vs LF correlation
    if not np.isnan(rev_lf_corr):
        if rev_lf_corr < 0.1:
            insights.append(f"- Rev/Pax vs LF correlation ≈ {rev_lf_corr:.2f} → filling extra seats may dilute yield; prioritize yield protection at higher LF.")
        else:
            insights.append(f"- Rev/Pax vs LF correlation ≈ {rev_lf_corr:.2f} → some headroom to fill seats without heavy dilution; targeted promos OK.")

    # I) Directional imbalance (largest absolute difference)
    if not imb.empty:
        imb["abs_diff"] = imb["lf_diff_vs_reverse"].abs()
        top_imb = imb.sort_values("abs_diff", ascending=False).head(1).iloc[0]
        insights.append(f"- Largest directional imbalance: {top_imb['origin']}→{top_imb['destination']} vs reverse (ΔLF≈ {top_imb['lf_diff_vs_reverse']:.1%}) → asymmetric pricing or gauge.")

    # J) Ancillary share opportunities
    if not seg_top.empty:
        low_anc = seg_top.sort_values("anc_share", ascending=True).head(1).iloc[0]
        high_anc = seg_top.sort_values("anc_share", ascending=False).head(1).iloc[0]
        insights.append(f"- Ancillary share: low on {low_anc['origin']}→{low_anc['destination']} (≈ {low_anc['anc_share']:.1%}), high on {high_anc['origin']}→{high_anc['destination']} (≈ {high_anc['anc_share']:.1%}) → target bundles where low.")

    # K) Last-minute cancels (<=1 day)
    last_minute_share = np.nan
    canc = booking[booking["is_cancelled"]]
    if not canc.empty:
        last_minute_share = (canc["days_before_flight_to_cancel"]<=1).mean()
        insights.append(f"- Last-minute cancellations (≤1 day) ≈ {last_minute_share:.1%} of cancels → raise buffers & pre-emptive reaccommodation in this window.")

    # Compose KPI suggestions (quick)
    insights.append("- KPI pack to track weekly: Cancellation Rate by DOW & Channel, Median/P90 cancel timing, LF by DOW & Time-of-Day, Rev/Pax vs LF, Directional LF Imbalance, Ancillary Share by OD.")

    # Write insights.md
    with open(out_dir / "insights.md", "w", encoding="utf-8") as f:
        f.write("# Condor — Auto-Generated Insights & Recommendations\n\n")
        for line in insights:
            f.write(f"{line}\n")

    return {
        "tables_dir": str(tables_dir),
        "charts_dir": str(charts_dir),
        "insights_path": str(out_dir / "insights.md"),
    }

# ---------- CLI
if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("excel_path", type=Path, help="Path to the Condor Excel file")
    ap.add_argument("--out", type=Path, default=Path("./reports"), help="Output folder")
    args = ap.parse_args()
    res = run_analysis(args.excel_path, args.out)
    print("Tables:", res["tables_dir"])
    print("Charts:", res["charts_dir"])
    print("Insights:", res["insights_path"])
