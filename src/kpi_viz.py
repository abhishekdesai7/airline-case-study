#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Condor KPIs & Visuals — FWLF, PACS, ARAS, CALF
Generates polished charts for airline operations, route planning & management reporting.

Outputs (created under ./report_imgs and ./report_tables):
  report_imgs/fwlf_calf_by_dow.png
  report_imgs/pacs_by_dow.png
  report_imgs/aras_by_route_top10.png
  report_imgs/dashboard_composite.png
  report_tables/kpi_summary_by_dow.csv

Usage:
  python condor_kpis_viz.py "Condor - Analytics Engineer 2025 Business Case DataSet_Final.xlsx"

Notes:
- PACS assumptions (easy to tweak in code): 25% connectivity credit, €40 variable cost per seat-leg.
- The script is resilient to minor column casing/spacing differences.
"""

import sys
import re
from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick

# ------------------------- Config / helpers
PALETTE = {
    "fwlf": "#2563eb",   # blue
    "calf": "#16a34a",   # green
    "pacs": "#ea580c",   # orange
    "aras": "#7c3aed",   # purple
    "grid": "#e5e7eb",   # light gray
    "axis": "#111827"    # dark gray for lines
}
DOW_ORDER = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

def clean_cols(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = out.columns.str.strip().str.lower()
    return out

def normalize_booking_id(x):
    if pd.isna(x): return np.nan
    s = str(x).strip()
    s_num = re.sub(r"\D+", "", s)
    return s_num if s_num else np.nan

def ensure_dirs(base: Path):
    (base / "report_imgs").mkdir(parents=True, exist_ok=True)
    (base / "report_tables").mkdir(parents=True, exist_ok=True)

def save_chart(figpath: Path):
    plt.tight_layout()
    plt.savefig(figpath, bbox_inches="tight")
    plt.close()

def _beautify_axes(ax, ylabel=None, is_ratio=False):
    ax.set_facecolor("white")
    ax.grid(axis="y", color=PALETTE["grid"], linestyle="--", linewidth=0.7, alpha=0.9)
    if ylabel:
        ax.set_ylabel(ylabel, fontsize=11)
    ax.tick_params(axis="x", labelrotation=30, labelsize=9)
    ax.tick_params(axis="y", labelsize=9)
    if is_ratio:
        ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0))

def _add_bar_labels(ax, rects, fmt="{:.1%}", offset=0.01, is_currency=False):
    for r in rects:
        height = r.get_height()
        if np.isnan(height):
            continue
        label = (f"€{height:,.1f}" if is_currency else fmt.format(height))
        ax.text(
            r.get_x() + r.get_width() / 2,
            height + (offset if height >= 0 else -offset),
            label,
            ha="center",
            va="bottom" if height >= 0 else "top",
            fontsize=9
        )

# ------------------------- Core logic
def main(xlsx_path: Path):
    out_dir = Path(".")
    ensure_dirs(out_dir)
    img_dir = out_dir / "report_imgs"
    tbl_dir = out_dir / "report_tables"

    # ---- Load sheets
    xl = pd.ExcelFile(xlsx_path)
    booking = clean_cols(pd.read_excel(xl, "Booking"))
    passenger = clean_cols(pd.read_excel(xl, "Passenger"))
    flight = clean_cols(pd.read_excel(xl, "Flight"))

    # ---- Normalize IDs, dates, numerics
    # Booking IDs for join
    if "bookingid (pnr)" in booking.columns:
        booking["bookingid_norm"] = booking["bookingid (pnr)"].apply(normalize_booking_id)
    elif "bookingid" in booking.columns:
        booking["bookingid_norm"] = booking["bookingid"].apply(normalize_booking_id)
    else:
        booking["bookingid_norm"] = np.nan

    if "bookingid" in passenger.columns:
        passenger["bookingid_norm"] = passenger["bookingid"].apply(normalize_booking_id)

    # Dates
    for c in ["reservation date", "cancellation date", "flight date"]:
        if c in booking.columns:
            booking[c] = pd.to_datetime(booking[c], errors="coerce")
    if "flightdate" in flight.columns:
        flight["flightdate"] = pd.to_datetime(flight["flightdate"], errors="coerce")

    # OD codes
    for c in ["origin", "destination"]:
        if c in booking.columns:
            booking[c] = booking[c].astype(str).str.upper().str.strip()

    # Revenue fields
    rev_cols = [
        "revenue per booking (ticket)",
        "revenue per booking (ancilliary pre check in)",
        "revenue per booking (ancilliary at check in)"
    ]
    for c in rev_cols:
        booking[c] = pd.to_numeric(booking.get(c, 0.0), errors="coerce")
    booking["ancillary_total"] = booking["revenue per booking (ancilliary pre check in)"].fillna(0) + \
                                 booking["revenue per booking (ancilliary at check in)"].fillna(0)
    booking["total_revenue"] = booking[rev_cols].sum(axis=1, skipna=True)
    booking["passengercount"] = pd.to_numeric(booking.get("passengercount", np.nan), errors="coerce")
    booking["is_cancelled"] = booking.get("cancellation date", pd.NaT).notna()
    booking["flight_dow"] = booking.get("flight date", pd.NaT).dt.day_name()

    # ---- Primary channel per PNR (optional enrichment)
    if "booking channel" in passenger.columns:
        passenger["booking_channel_norm"] = (
            passenger["booking channel"].astype(str).str.strip().str.lower()
            .replace({
                "condor app": "condor app",
                "condor-app": "condor app",
                "app": "condor app",
                "web": "website",
                "website": "website",
                "call center": "call center",
                "travel agency": "travel agency",
                "ota": "ota",
                "online travel agency": "ota",
            })
        )
        pnr_channel = (
            passenger.groupby("bookingid_norm")
            .agg(primary_channel=("booking_channel_norm", lambda s: next((v for v in s if pd.notna(v)), np.nan)))
            .reset_index()
        )
        booking = booking.merge(pnr_channel, on="bookingid_norm", how="left")

    # ---- Build leg table (pax/revenue per flight leg) and join capacity
    leg = (
        booking.groupby(["flightnumber", "flight date", "origin", "destination"], dropna=False)
        .agg(
            pax=("passengercount", "sum"),
            revenue=("total_revenue", "sum"),
            cancels=("is_cancelled", "sum"),
        )
        .reset_index()
    )
    # Merge capacity
    flight_view = flight[[c for c in ["flightnumber", "flightdate", "availablecapacity", "origin", "destination"] if c in flight.columns]].copy()
    # prefer join on flightnumber + date
    leg = leg.merge(
        flight_view.drop(columns=[c for c in ["origin", "destination"] if c in flight_view.columns]),
        left_on=["flightnumber", "flight date"],
        right_on=["flightnumber", "flightdate"],
        how="left",
    )
    if "flightdate" in leg.columns:
        leg = leg.drop(columns=["flightdate"])

    # Derived
    leg["dow"] = leg["flight date"].dt.day_name()
    leg["availablecapacity"] = pd.to_numeric(leg["availablecapacity"], errors="coerce")
    leg["load_factor"] = np.where(leg["availablecapacity"] > 0, leg["pax"] / leg["availablecapacity"], np.nan)
    leg["rev_per_pax"] = np.where(leg["pax"] > 0, leg["revenue"] / leg["pax"], np.nan)

    # Safe weekday ordering
    # normalize DOW values to category so sorting is consistent even with missing days
    for df in [leg, booking]:
        if "dow" in df.columns:
            df["dow"] = pd.Categorical(df["dow"], categories=DOW_ORDER, ordered=True)

    # =========================
    # Metric 1: FWLF by Day of Week
    # =========================
    fwlf_dow = (
        leg.groupby("dow")
        .apply(lambda x: (x["pax"].sum() / x["availablecapacity"].sum()) if x["availablecapacity"].sum() > 0 else np.nan)
        .reindex(DOW_ORDER)
        .rename("fwlf")
        .reset_index()
    )

    # =========================
    # Metric 4: CALF by Day of Week (realized LF after cancellations)
    # =========================
    # realized pax proxy: booked pax - cancellations at booking-grain, summed to leg-grain
    realized = (
        booking.assign(realized_pax=lambda d: d["passengercount"].fillna(0) - d["is_cancelled"].astype(int))
        .groupby(["flightnumber", "flight date"])
        .agg(realized_pax=("realized_pax", "sum"))
        .reset_index()
    )
    leg2 = leg.merge(realized, on=["flightnumber", "flight date"], how="left")
    leg2["calf"] = np.where(leg2["availablecapacity"] > 0, leg2["realized_pax"] / leg2["availablecapacity"], np.nan)
    calf_dow = leg2.groupby("dow")["calf"].mean().reindex(DOW_ORDER).reset_index()

    # =========================
    # Metric 2: PACS by Day of Week
    # assumptions: 25% connectivity credit for feeder FRA→FCO; €40 variable cost per seat-leg
    # =========================
    connection_value_pct = 0.25
    variable_cost_per_seat_leg = 40.0

    fra_fco = leg[(leg["origin"] == "FRA") & (leg["destination"] == "FCO")].copy()
    fco_pmo = leg[(leg["origin"] == "FCO") & (leg["destination"] == "PMO")].copy()
    onward_rev_map = fco_pmo.groupby("dow")["revenue"].mean().to_dict() if not fco_pmo.empty else {}

    pacs_vals = []
    for _, r in leg.iterrows():
        rev = r["revenue"]
        # credit connection value to FRA->FCO feeder
        if r["origin"] == "FRA" and r["destination"] == "FCO":
            rev += connection_value_pct * onward_rev_map.get(r["dow"], 0.0)
        seats = r["availablecapacity"] if pd.notna(r["availablecapacity"]) else 0
        pacs = (rev - seats * variable_cost_per_seat_leg) / seats if seats and seats > 0 else np.nan
        pacs_vals.append({"dow": r["dow"], "pacs": pacs})
    pacs_dow = pd.DataFrame(pacs_vals).groupby("dow")["pacs"].mean().reindex(DOW_ORDER).reset_index()

    # =========================
    # Metric 3: ARAS by Route (Top 10 by revenue)
    # ARAS here is ancillary share (ancillary / total revenue) as dataset lacks OD seat capacity.
    # =========================
    seg = (
        booking.groupby(["origin", "destination"])
        .agg(revenue=("total_revenue", "sum"), ancillary=("ancillary_total", "sum"))
        .reset_index()
    )
    seg["aras"] = np.where(seg["revenue"] > 0, seg["ancillary"] / seg["revenue"], np.nan)
    seg["od"] = seg["origin"] + "→" + seg["destination"]
    seg_top = seg.sort_values("revenue", ascending=False).head(10).copy()

    # =========================
    # ---- Charts ----
    # 1) FWLF + CALF grouped by DOW
    merged_fc = fwlf_dow.merge(calf_dow, on="dow", how="outer")
    merged_fc["dow"] = pd.Categorical(merged_fc["dow"], categories=DOW_ORDER, ordered=True)
    merged_fc = merged_fc.sort_values("dow").reset_index(drop=True)
    x = np.arange(len(merged_fc["dow"]))
    bar_w = 0.38

    fig, ax = plt.subplots(figsize=(10, 6), facecolor="white")
    r1 = ax.bar(x - bar_w / 2, merged_fc["fwlf"], width=bar_w, color=PALETTE["fwlf"], label="FWLF (utilization)")
    r2 = ax.bar(x + bar_w / 2, merged_fc["calf"], width=bar_w, color=PALETTE["calf"], label="CALF (realized)")

    ax.set_title("FWLF vs CALF by Day of Week", fontsize=14, pad=14)
    ax.set_xticks(x, merged_fc["dow"])
    _beautify_axes(ax, ylabel="Load Factor", is_ratio=True)
    ax.legend(frameon=False, loc="upper left")
    _add_bar_labels(ax, r1, fmt="{:.1%}")
    _add_bar_labels(ax, r2, fmt="{:.1%}")
    save_chart(img_dir / "fwlf_calf_by_dow.png")

    # 2) PACS by DOW
    fig, ax = plt.subplots(figsize=(10, 6), facecolor="white")
    bars = ax.bar(pacs_dow["dow"], pacs_dow["pacs"], color=PALETTE["pacs"], label="PACS (€ per seat-leg)")
    ax.axhline(0, color=PALETTE["axis"], linewidth=0.8, alpha=0.6)
    ax.set_title("PACS by Day of Week (€/seat-leg)", fontsize=14, pad=14)
    _beautify_axes(ax, ylabel="€ per seat-leg", is_ratio=False)
    ax.legend(frameon=False, loc="upper left")
    _add_bar_labels(ax, bars, is_currency=True, fmt="{:.1f}")
    save_chart(img_dir / "pacs_by_dow.png")

    # 3) ARAS by Route (Top 10) — horizontal, sorted, with network average
    seg_top_sorted = seg_top.sort_values("aras", ascending=True).copy()
    network_avg_aras = seg["aras"].mean(skipna=True)

    fig, ax = plt.subplots(figsize=(10, 6), facecolor="white")
    bars = ax.barh(seg_top_sorted["od"], seg_top_sorted["aras"], color=PALETTE["aras"], label="Ancillary share")
    ax.axvline(network_avg_aras, color=PALETTE["axis"], linestyle="--", linewidth=1.0, alpha=0.7,
               label=f"Network avg ({network_avg_aras:.1%})")
    ax.set_title("ARAS — Ancillary Revenue Share by Route (Top 10 by Revenue)", fontsize=14, pad=14)
    _beautify_axes(ax, ylabel=None, is_ratio=False)
    ax.xaxis.set_major_formatter(mtick.PercentFormatter(1.0))
    ax.legend(frameon=False, loc="lower right")
    for r in bars:
        width = r.get_width()
        if pd.notna(width):
            ax.text(width + 0.01, r.get_y() + r.get_height() / 2, f"{width:.1%}", va="center", ha="left", fontsize=9)
    save_chart(img_dir / "aras_by_route_top10.png")

    # 4) Composite 1-page dashboard (optional)
    fig = plt.figure(figsize=(14, 10), facecolor="white")
    gs = fig.add_gridspec(2, 2, wspace=0.25, hspace=0.35)

    # FWLF vs CALF
    ax1 = fig.add_subplot(gs[0, 0])
    r1 = ax1.bar(x - bar_w / 2, merged_fc["fwlf"], width=bar_w, color=PALETTE["fwlf"], label="FWLF")
    r2 = ax1.bar(x + bar_w / 2, merged_fc["calf"], width=bar_w, color=PALETTE["calf"], label="CALF")
    ax1.set_title("FWLF vs CALF (by DOW)", fontsize=12, pad=10)
    ax1.set_xticks(x, merged_fc["dow"])
    _beautify_axes(ax1, ylabel="Load Factor", is_ratio=True)
    ax1.legend(frameon=False, loc="upper left")
    _add_bar_labels(ax1, r1, fmt="{:.1%}")
    _add_bar_labels(ax1, r2, fmt="{:.1%}")

    # PACS
    ax2 = fig.add_subplot(gs[0, 1])
    bars2 = ax2.bar(pacs_dow["dow"], pacs_dow["pacs"], color=PALETTE["pacs"], label="PACS (€)")
    ax2.axhline(0, color=PALETTE["axis"], linewidth=0.8, alpha=0.6)
    ax2.set_title("PACS (€/seat-leg) by DOW", fontsize=12, pad=10)
    _beautify_axes(ax2, ylabel="€ per seat-leg", is_ratio=False)
    ax2.legend(frameon=False, loc="upper left")
    _add_bar_labels(ax2, bars2, is_currency=True, fmt="{:.1f}")

    # ARAS
    ax3 = fig.add_subplot(gs[1, :])
    bars3 = ax3.bar(seg_top_sorted["od"], seg_top_sorted["aras"], color=PALETTE["aras"], label="ARAS share")
    ax3.axhline(network_avg_aras, color=PALETTE["axis"], linestyle="--", linewidth=1.0, alpha=0.7,
                label=f"Network avg ({network_avg_aras:.1%})")
    ax3.set_title("ARAS by Route (Top 10 by Revenue)", fontsize=12, pad=10)
    _beautify_axes(ax3, ylabel="Ancillary share", is_ratio=False)
    ax3.yaxis.set_major_formatter(mtick.PercentFormatter(1.0))
    ax3.tick_params(axis="x", labelrotation=30, labelsize=9)
    for r in bars3:
        h = r.get_height()
        if pd.notna(h):
            ax3.text(r.get_x() + r.get_width() / 2, h + 0.01, f"{h:.1%}", ha="center", va="bottom", fontsize=9)
    ax3.legend(frameon=False, loc="upper left")

    save_chart(img_dir / "dashboard_composite.png")

    # ---- Save executive table
    kpi_tbl = (
        fwlf_dow.rename(columns={"fwlf": "FWLF"})
        .merge(pacs_dow.rename(columns={"pacs": "PACS_€/seat"}), on="dow", how="outer")
        .merge(calf_dow.rename(columns={"calf": "CALF"}), on="dow", how="outer")
    )
    kpi_tbl.to_csv(tbl_dir / "kpi_summary_by_dow.csv", index=False)

    print("Saved charts to:", img_dir.resolve())
    print("Saved table to:", (tbl_dir / "kpi_summary_by_dow.csv").resolve())

# ------------------------- CLI
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print('Usage: python kpi_viz.py "Condor - Analytics Engineer 2025 Business Case DataSet_Final.xlsx"')
        sys.exit(1)
    main(Path(sys.argv[1]))
