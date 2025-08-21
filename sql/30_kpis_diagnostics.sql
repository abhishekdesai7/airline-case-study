-- SRM (needs passenger-legs and unique passengers)
-- Approximation: passenger-legs = SUM(pax); unique pax requires PNR-level stitching.
-- Here we approximate with booking grain if BookingID present; otherwise report seat reuse proxy per leg.

CREATE OR REPLACE VIEW kpi.srm_proxy AS
SELECT
  SUM(pax)*1.0 / NULLIF(COUNT(DISTINCT flightnumber || '_' || CAST(flight_date AS VARCHAR)),0) AS passenger_legs_per_flight
FROM mart.leg;
-- (If true unique pax available, replace denominator with unique passengers.)

-- CALF
CREATE OR REPLACE VIEW kpi.calf_overall AS
SELECT SUM(pax)*1.0 / NULLIF(SUM(seats),0) AS calf
FROM mart.leg; -- refine by subtracting modeled no-shows if available

-- OTRI (needs OTP; if missing, plug operational OTP feed later)
-- Placeholder: join an OTP table when available. Here we expose structure only.
CREATE OR REPLACE VIEW kpi.otri_placeholder AS
SELECT 0.0 AS otri;  -- replace with LF * OTP once OTP data lands
