-- 1) FWLF overall + by segment
CREATE OR REPLACE VIEW kpi.fwlf_overall AS
SELECT SUM(pax)*1.0 / NULLIF(SUM(seats),0) AS fwlf FROM mart.leg;

CREATE OR REPLACE VIEW kpi.fwlf_by_segment AS
SELECT origin, destination,
       SUM(pax)*1.0 / NULLIF(SUM(seats),0) AS fwlf
FROM mart.leg GROUP BY 1,2;

-- 2) PACS (parameterized via SETs)
-- Defaults; you can override at runtime from Python or DuckDB CLI
-- PACS with inline params (change numbers as needed or feed from Python)
CREATE OR REPLACE VIEW kpi.pacs_leg AS
WITH p AS (
  SELECT 
    0.15::DOUBLE AS connection_value_pct,        -- 15% of ticket revenue as connection value proxy
    25::DOUBLE  AS variable_cost_per_seat_leg    -- € per seat-leg
)
SELECT
  l.flightnumber, l.flight_date, l.origin, l.destination,
  l.seats, l.total_revenue,
  (l.total_revenue * p.connection_value_pct) AS connection_value,
  ( l.total_revenue
    + l.total_revenue * p.connection_value_pct
    - l.seats * p.variable_cost_per_seat_leg
  ) / NULLIF(l.seats,0) AS pacs_per_seat_leg
FROM mart.leg l
CROSS JOIN p;



-- 3) YALF (proxy YieldIndex using revenue-per-pax quantiles per segment)
-- Compute rev_per_pax safely
CREATE OR REPLACE VIEW mart.leg_with_revpp AS
SELECT *, CASE WHEN pax>0 THEN total_revenue*1.0/pax END AS rev_per_pax
FROM mart.leg;

-- Rank into 0..1 index by segment (simple proxy when fare class is absent)
CREATE OR REPLACE VIEW mart.leg_with_yield_index AS
WITH ranked AS (
  SELECT *,
         NTILE(5) OVER (PARTITION BY origin, destination ORDER BY rev_per_pax) AS q5
  FROM mart.leg_with_revpp
)
SELECT *,
       (q5-1)/4.0 AS yield_index   -- 0.0 .. 1.0
FROM ranked;

CREATE OR REPLACE VIEW kpi.yalf_overall AS
SELECT SUM(pax*yield_index)*1.0 / NULLIF(SUM(seats*1.0),0) AS yalf
FROM mart.leg_with_yield_index;

-- 4) ARAS
CREATE OR REPLACE VIEW kpi.aras_overall AS
WITH anc AS (
  SELECT
    flightnumber, "flight_date"::DATE AS flight_date,
    SUM("revenue_per_booking_ancilliary_pre_check_in"
      + "revenue_per_booking_ancilliary_at_check_in") AS ancillary_rev
  FROM raw.booking GROUP BY 1,2
)
SELECT SUM(anc.ancillary_rev)*1.0 / NULLIF(SUM(l.seats),0) AS aras
FROM anc
JOIN mart.leg l USING (flightnumber, flight_date);
