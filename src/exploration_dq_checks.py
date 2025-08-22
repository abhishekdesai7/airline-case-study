
import pandas as pd
import numpy as np
import re
import argparse
from pathlib import Path

REV_COLS = [
    'revenue_per_booking_(ticket)',
    'revenue_per_booking_(ancilliary_pre_check_in)',
    'revenue_per_booking_(ancilliary_at_check_in)'
]

def clean_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = (df.columns
                  .str.strip()
                  .str.replace(r'\s+', '_', regex=True)
                  .str.lower())
    return df

def normalize_booking_id(x):
    if pd.isna(x):
        return np.nan
    s = str(x).strip()
    s_num = re.sub(r'\D+', '', s)  # keep digits only
    return s_num if s_num != '' else np.nan

def load_excel(path: Path):
    xl = pd.ExcelFile(path)
    booking = pd.read_excel(xl, "Booking")
    passenger = pd.read_excel(xl, "Passenger")
    flight = pd.read_excel(xl, "Flight")
    booking = clean_cols(booking)
    passenger = clean_cols(passenger)
    flight = clean_cols(flight)
    return booking, passenger, flight

def add_derived(booking: pd.DataFrame, passenger: pd.DataFrame, flight: pd.DataFrame):
    booking = booking.copy()
    passenger = passenger.copy()
    flight = flight.copy()

    # IDs
    booking['bookingid_norm'] = booking['bookingid_(pnr)'].apply(normalize_booking_id)
    passenger['bookingid_norm'] = passenger['bookingid'].apply(normalize_booking_id)

    # Dates
    for c in ['reservation_date','cancellation_date','flight_date']:
        if c in booking.columns:
            booking[c] = pd.to_datetime(booking[c], errors='coerce')
    if 'flightdate' in flight.columns:
        flight['flightdate'] = pd.to_datetime(flight['flightdate'], errors='coerce')

    # Numeric
    for c in REV_COLS:
        if c in booking.columns:
            booking[c] = pd.to_numeric(booking[c], errors='coerce')
    if 'passengercount' in booking.columns:
        booking['passengercount'] = pd.to_numeric(booking['passengercount'], errors='coerce')

    # Canonical routing
    booking['routing_canonical'] = (
        booking['origin'].astype(str).str.upper().str.strip()
        + '-' + booking['destination'].astype(str).str.upper().str.strip()
    )

    return booking, passenger, flight

def dq_checks(booking: pd.DataFrame, passenger: pd.DataFrame, flight: pd.DataFrame):
    out = {}

    # 1) Canceled with ancillary at check-in
    if {'cancellation_date','revenue_per_booking_(ancilliary_at_check_in)'}.issubset(booking.columns):
        mask = booking['cancellation_date'].notna() & (booking['revenue_per_booking_(ancilliary_at_check_in)'] > 0)
        out['canceled_with_checkin_ancillary'] = booking.loc[mask].copy()

    # 2) Cancellation before reservation
    if {'cancellation_date','reservation_date'}.issubset(booking.columns):
        mask = booking['cancellation_date'].notna() & booking['reservation_date'].notna() & (booking['cancellation_date'] < booking['reservation_date'])
        out['cancellation_before_reservation'] = booking.loc[mask].copy()

    # 2b) Reservation after flight date
    if {'reservation_date','flight_date'}.issubset(booking.columns):
        mask = booking['reservation_date'].notna() & booking['flight_date'].notna() & (booking['reservation_date'] > booking['flight_date'])
        out['reservation_after_flight'] = booking.loc[mask].copy()

    # 3) Same booking multiple channels/devices
    if 'bookingid_norm' in passenger.columns and 'booking_channel' in passenger.columns:
        multi_channel = passenger.groupby('bookingid_norm')['booking_channel'].nunique(dropna=False)
        ids = multi_channel[(multi_channel.index.notna()) & (multi_channel > 1)].index
        out['same_booking_multiple_channels'] = passenger[passenger['bookingid_norm'].isin(ids)].copy()

    if 'bookingid_norm' in passenger.columns and 'device_used' in passenger.columns:
        multi_device = passenger.groupby('bookingid_norm')['device_used'].nunique(dropna=False)
        ids = multi_device[(multi_device.index.notna()) & (multi_device > 1)].index
        out['same_booking_multiple_devices'] = passenger[passenger['bookingid_norm'].isin(ids)].copy()

    # 4) Revenue but zero passengers
    booking['total_revenue'] = booking[REV_COLS].sum(axis=1, skipna=True)
    mask = (booking['passengercount'] == 0) & (booking['total_revenue'] > 0)
    out['revenue_with_zero_passengers'] = booking.loc[mask].copy()

    # 5) Negative revenue or passengers
    neg_rev = (booking[REV_COLS] < 0).any(axis=1)
    out['negative_revenue_fields'] = booking.loc[neg_rev].copy()
    neg_pax = booking['passengercount'] < 0
    out['negative_passengers'] = booking.loc[neg_pax].copy()

    # 6) Capacity joins
    leg = (booking.groupby(['flightnumber','flight_date','origin','destination'], dropna=False)
           .agg(pax=('passengercount','sum'))
           .reset_index())
    leg = leg.merge(flight[['flightnumber','flightdate','availablecapacity']],
                    left_on=['flightnumber','flight_date'],
                    right_on=['flightnumber','flightdate'],
                    how='left')
    leg['capacity_anomaly'] = (leg['pax'] > leg['availablecapacity']) & leg['availablecapacity'].notna()
    out['pax_exceeds_capacity'] = leg.loc[leg['capacity_anomaly']].copy()
    out['missing_capacity_for_leg'] = leg.loc[leg['availablecapacity'].isna()].copy()
    out['capacity_out_of_range'] = leg.loc[leg['availablecapacity'].notna() & ((leg['availablecapacity'] < 160) | (leg['availablecapacity'] > 200))].copy()

    # 7) Routing inconsistent with OD
    routing_upper = booking['routing'].astype(str).str.upper().str.replace(' ', '')
    od1 = booking['origin'].astype(str).str.upper().str.replace(' ', '') + '-' + booking['destination'].astype(str).str.upper().str.replace(' ', '')
    od2 = booking['origin'].astype(str).str.upper().str.replace(' ', '') + '→' + booking['destination'].astype(str).str.upper().str.replace(' ', '')
    inconsistent = ~(routing_upper.eq(od1) | routing_upper.eq(od2))
    out['routing_inconsistent_with_od'] = booking.loc[inconsistent].copy()

    # 8) Duplicate legs within PNR
    dup_cols = ['bookingid_norm','flightnumber','flight_date','origin','destination']
    dup_mask = booking.duplicated(subset=dup_cols, keep=False)
    out['duplicate_legs_within_pnr'] = booking.loc[dup_mask].copy()

    # 9) Cancellation with positive pax
    mask = booking['cancellation_date'].notna() & (booking['passengercount'] > 0)
    out['cancellation_with_positive_pax'] = booking.loc[mask].copy()

    # 10) Missing flight date
    out['missing_flight_date'] = booking.loc[booking['flight_date'].isna()].copy()

    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("excel_path", type=Path, help="Path to the Condor Excel dataset")
    ap.add_argument("--out", type=Path, default=Path("out"), help="Directory to write anomaly CSVs")
    args = ap.parse_args()

    args.out.mkdir(parents=True, exist_ok=True)
    booking, passenger, flight = load_excel(args.excel_path)
    booking, passenger, flight = add_derived(booking, passenger, flight)

    anomalies = dq_checks(booking, passenger, flight)

    # Summary
    summary = {k: len(v) for k,v in anomalies.items()}
    pd.DataFrame(list(summary.items()), columns=["check","count"]).sort_values(by="count", ascending=False).to_csv(args.out/"dq_summary.csv", index=False)

    # Save each anomaly CSV
    for k, df in anomalies.items():
        df.to_csv(args.out/f"{k}.csv", index=False)

    print("Wrote DQ outputs to:", args.out.resolve())

if __name__ == "__main__":
    main()