-- Aggregate booking to leg grain
CREATE OR REPLACE VIEW mart.leg AS
WITH agg AS (
  SELECT
    flightnumber,
    "flight_date"::DATE AS flight_date,
    origin, destination,
    SUM(passengercount) AS pax,
    SUM("revenue_per_booking_ticket"
      + "revenue_per_booking_ancilliary_pre_check_in"
      + "revenue_per_booking_ancilliary_at_check_in") AS total_revenue,
    SUM(CASE WHEN "cancellation_date" IS NOT NULL THEN 1 ELSE 0 END) AS cancels
  FROM raw.booking
  GROUP BY 1,2,3,4
)
SELECT
  a.*,
  f.availablecapacity AS seats,
  f.timeofday,
  f.routetype,
  CASE WHEN f.availablecapacity>0 THEN a.pax*1.0/f.availablecapacity END AS leg_lf
FROM agg a
LEFT JOIN raw.flight f
  ON a.flightnumber = f.flightnumber
 AND a.flight_date  = f.flightdate;

-- Segment summary (optional)
CREATE OR REPLACE VIEW mart.segment AS
SELECT origin, destination,
       SUM(pax) AS pax, SUM(seats) AS seats,
       SUM(total_revenue) AS total_revenue
FROM mart.leg
GROUP BY 1,2;
