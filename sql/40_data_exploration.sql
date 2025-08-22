-- =========================================================
-- Condor Case Study – Part B: Exploratory Insights & KPIs
-- DuckDB SQL (run with: duckdb warehouse/condor.duckdb ".read sql/partB_exploration.sql")
-- Assumes raw.booking / raw.flight columns are snake_case (as produced by your ingest step)
-- =========================================================

------------------------------------------------------------
-- 0) Schemas
------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS mart;
CREATE SCHEMA IF NOT EXISTS kpi;

------------------------------------------------------------
-- 1) Leg-level mart (aggregate Booking -> leg grain; join Flight)
------------------------------------------------------------
CREATE OR REPLACE VIEW mart_leg AS
WITH agg AS (
  SELECT
    flightnumber,
    flight_date::DATE AS flight_date,
    origin,
    destination,
    SUM(passengercount) AS pax,
    SUM(
      revenue_per_booking_ticket
      + revenue_per_booking_ancilliary_pre_check_in
      + revenue_per_booking_ancilliary_at_check_in
    ) AS total_revenue,
    SUM(CASE WHEN cancellation_date IS NOT NULL THEN 1 ELSE 0 END) AS cancels
  FROM raw.booking
  GROUP BY 1,2,3,4
)
SELECT
  a.*,
  f.availablecapacity AS seats,
  f.timeofday,
  f.routetype,
  CASE WHEN f.availablecapacity > 0 THEN a.pax * 1.0 / f.availablecapacity END AS leg_lf
FROM agg a
LEFT JOIN raw.flight f
  ON a.flightnumber = f.flightnumber
 AND a.flight_date  = f.flightdate;

------------------------------------------------------------
-- 2) Convenience views
------------------------------------------------------------
-- Segment aggregation
CREATE OR REPLACE VIEW mart_segment AS
SELECT origin, destination,
       SUM(pax)   AS pax,
       SUM(seats) AS seats,
       SUM(total_revenue) AS total_revenue
FROM mart_leg
GROUP BY 1,2;

-- Revenue-per-passenger (leg)
CREATE OR REPLACE VIEW mart_leg_revpp AS
SELECT *,
       CASE WHEN pax > 0 THEN total_revenue * 1.0 / pax END AS rev_per_pax
FROM mart_leg;

------------------------------------------------------------
-- 3) Core KPIs
------------------------------------------------------------
-- 3.1 FWLF (overall and by segment)
CREATE OR REPLACE VIEW kpi_fwlf_overall AS
SELECT SUM(pax) * 1.0 / NULLIF(SUM(seats),0) AS fwlf
FROM mart_leg;

CREATE OR REPLACE VIEW kpi_fwlf_by_segment AS
SELECT origin, destination,
       SUM(pax) * 1.0 / NULLIF(SUM(seats),0) AS fwlf
FROM mart_leg
GROUP BY 1,2;

-- 3.2 PACS (parameterized via a params CTE; adjust numbers as needed)
CREATE OR REPLACE VIEW kpi_pacs_leg AS
WITH params AS (
  SELECT 25.0::DOUBLE AS variable_cost_per_seat_leg, 0.15::DOUBLE AS connection_value_pct
)
SELECT
  l.flightnumber,
  l.flight_date,
  l.origin,
  l.destination,
  l.seats,
  l.total_revenue,
  (l.total_revenue * p.connection_value_pct) AS connection_value,
  ( l.total_revenue
    + (l.total_revenue * p.connection_value_pct)
    - (l.seats * p.variable_cost_per_seat_leg)
  ) / NULLIF(l.seats,0) AS pacs_per_seat_leg
FROM mart_leg l
CROSS JOIN params p;

-- 3.3 YALF (yield-adjusted load factor using a simple proxy based on rev_per_pax quantiles)
CREATE OR REPLACE VIEW mart_leg_yield_index AS
WITH ranked AS (
  SELECT *,
         NTILE(5) OVER (PARTITION BY origin, destination ORDER BY rev_per_pax) AS q5
  FROM mart_leg_revpp
)
SELECT *,
       (q5 - 1) / 4.0 AS yield_index   -- scaled 0.0..1.0
FROM ranked;

CREATE OR REPLACE VIEW kpi_yalf_overall AS
SELECT SUM(pax * yield_index) * 1.0 / NULLIF(SUM(seats),0) AS yalf
FROM mart_leg_yield_index;

-- 3.4 ARAS (ancillary per available seat)
CREATE OR REPLACE VIEW kpi_aras_overall AS
WITH anc AS (
  SELECT
    flightnumber,
    flight_date::DATE AS flight_date,
    SUM(revenue_per_booking_ancilliary_pre_check_in
      + revenue_per_booking_ancilliary_at_check_in) AS ancillary_rev
  FROM raw.booking
  GROUP BY 1,2
)
SELECT SUM(anc.ancillary_rev) * 1.0 / NULLIF(SUM(l.seats),0) AS aras
FROM anc
JOIN mart_leg l USING (flightnumber, flight_date);

------------------------------------------------------------
-- 4) Diagnostics & Exploration tables for Part B
------------------------------------------------------------
-- 4.1 Avg load factor by time of day
CREATE OR REPLACE VIEW exp_lf_by_time AS
SELECT timeofday, AVG(leg_lf) AS avg_lf
FROM mart_leg
GROUP BY 1;

-- 4.2 Ancillary share by segment
CREATE OR REPLACE VIEW exp_anc_share_segment AS
WITH anc AS (
  SELECT
    flightnumber,
    flight_date::DATE AS flight_date,
    origin,
    destination,
    SUM(revenue_per_booking_ancilliary_pre_check_in
      + revenue_per_booking_ancilliary_at_check_in) AS anc_rev
  FROM raw.booking
  GROUP BY 1,2,3,4
)
SELECT
  a.origin,
  a.destination,
  SUM(a.anc_rev) * 1.0 / NULLIF(SUM(l.total_revenue),0) AS anc_share
FROM anc a
JOIN mart_leg l
  ON a.flightnumber = l.flightnumber AND a.flight_date = l.flight_date
GROUP BY 1,2;

-- 4.3 Cancellation rate by segment
CREATE OR REPLACE VIEW exp_cancel_rate_segment AS
SELECT origin, destination,
       AVG(CASE WHEN (pax + cancels) > 0 THEN cancels * 1.0 / (pax + cancels) END) AS cancel_rate
FROM mart_leg
GROUP BY 1,2;

-- 4.4 Outliers: lowest/highest LF legs
CREATE OR REPLACE VIEW exp_low_lf_legs AS
SELECT flightnumber, flight_date, origin, destination, leg_lf, total_revenue
FROM mart_leg
ORDER BY leg_lf ASC NULLS LAST
LIMIT 10;

CREATE OR REPLACE VIEW exp_high_lf_legs AS
SELECT flightnumber, flight_date, origin, destination, leg_lf, total_revenue
FROM mart_leg
ORDER BY leg_lf DESC NULLS LAST
LIMIT 10;

-- 4.5 Outliers: highest/lowest revenue per pax
CREATE OR REPLACE VIEW exp_high_yield_legs AS
SELECT flightnumber, flight_date, origin, destination, rev_per_pax, leg_lf
FROM mart_leg_revpp
ORDER BY rev_per_pax DESC NULLS LAST
LIMIT 10;

CREATE OR REPLACE VIEW exp_low_yield_legs AS
SELECT flightnumber, flight_date, origin, destination, rev_per_pax, leg_lf
FROM mart_leg_revpp
WHERE rev_per_pax IS NOT NULL
ORDER BY rev_per_pax ASC
LIMIT 10;

-- 4.6 FWLF by segment (already created) – keep as is

------------------------------------------------------------
-- 5) Quick verification selects (uncomment to run manually)
-- SELECT * FROM kpi_fwlf_overall;
-- SELECT * FROM kpi_fwlf_by_segment ORDER BY fwlf DESC;
-- SELECT * FROM kpi_pacs_leg ORDER BY pacs_per_seat_leg DESC LIMIT 20;
-- SELECT * FROM kpi_yalf_overall;
-- SELECT * FROM kpi_aras_overall;
-- SELECT * FROM exp_lf_by_time;
-- SELECT * FROM exp_anc_share_segment ORDER BY anc_share DESC;
-- SELECT * FROM exp_cancel_rate_segment ORDER BY cancel_rate DESC;
-- SELECT * FROM exp_low_lf_legs;
-- SELECT * FROM exp_high_yield_legs;
