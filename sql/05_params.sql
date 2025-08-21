CREATE SCHEMA IF NOT EXISTS cfg;

-- Single-row table holding KPI parameters
CREATE TABLE IF NOT EXISTS cfg.params AS
SELECT 
  25.0        AS variable_cost_per_seat_leg,          -- € per seat-leg
  0.15        AS connection_value_pct_of_ticket_rev;  -- 15% of ticket revenue
