# airline-case-study
Condor Case Study

# Airline Case Study – Condor Analytics Challenge

This project reimagines how airlines measure route performance beyond the traditional Seat Load Factor (SLF).  
It has been built as part of the **Condor Airlines Analytics Engineer case study** and demonstrates expertise in  
**data modeling, KPI design, and analytics storytelling**.

-- Highlights
- Designed **new KPIs**: PACS, FWLF, YALF, ARAS, SRM, CALF, OTRI.
- Built a **reproducible data pipeline** with Python + DuckDB + SQL.
- Produced **visual insights**: load factor patterns, revenue vs utilization, and profitability signals.
- Created **data quality checks** to ensure reliable outputs.
- Delivered a structured framework for **route planning, pricing, and operations decisions**.

-- Example Insights
- Stopover segments can hide strong demand if only SLF is used.  
- Ancillary revenues (bags, seats, meals) meaningfully shift profitability.  
- Reliability (delays, no-shows) directly impacts commercial outcomes.  

-- Tools
- **Python** (pandas, matplotlib)  
- **DuckDB** (lightweight OLAP engine)  
- **SQL models** for KPI calculations  
- **Matplotlib** for free, clean visuals  

---

-- Structure
- `data_raw/` – input XLSX (provided)  
- `warehouse/` – DuckDB DB with marts & KPIs  
- `sql/` – modular SQL models  
- `src/` – Python scripts (ETL, metrics, visualization)  
- `reports/` – charts & QA outputs  
