-- No negative pax
SELECT COUNT(*) AS cnt_bad_pax FROM mart.leg WHERE pax < 0;

-- Seats not null & positive
SELECT COUNT(*) AS cnt_bad_seats FROM mart.leg WHERE seats IS NULL OR seats <= 0;

-- LF bounds
SELECT COUNT(*) AS cnt_bad_lf FROM mart.leg WHERE leg_lf < 0 OR leg_lf > 1.2; -- >1.0 indicates data issues/overbooking anomalies
