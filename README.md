# Last-Mile Delivery Network Capacity, Depot Forecasting & Data Quality Analytics Engine

An end-to-end analytics, data quality governance, statistical modeling, and capacity forecasting platform for multi-region last-mile delivery networks, connecting **Supply Planning**, **Inventory/Depot Origins**, **Carrier Handoff**, and **Demand Forecasting**.

## Architecture & Capabilities

1. **Automated Data Quality Profiling, Cleansing & Quarantine (`run_pipeline.py`)**
   - Verifies raw multi-domain CSV inputs via SHA-256 checksums (`orders`, `order_items`, `sellers`, `customers` — **314,627 total records**).
   - Enforces **10/10 automated data quality & integrity gates**, quarantining **3,160** incomplete/non-delivered orders (`188` carrier/delivery chronology inversions, `9` missing timestamps, `2,963` non-delivered statuses) while maintaining **100% row conservation** (`96,281` promoted + `3,160` quarantined = `99,441` raw orders).
2. **Normalized Star-Schema Analytical Mart (`sql/lane_capacity_kpi_mart.sql`, `sql/spark_last_mile_etl.py`)**
   - Models `dim_depot`, `dim_destination`, `fct_last_mile_shipments`, and `dq_quarantine_log` in SQLite (with matching PySpark + Parquet partitioning spec) using SQL Joins, CTEs, and Window Functions (`RANK() OVER (...)`) to rank **99 high-volume origin-destination lanes** for escalation triage.
3. **Exploratory Data Analysis, Hypothesis Testing & Walk-Forward Capacity Forecasting**
   - **Peak-Load Capacity Stress:** Two-proportion $Z$-test proves peak-load weeks (`>= 1,800` orders) suffer a **15.484%** SLA breach rate vs. **6.821%** during balanced weeks ($Z = 35.17$, $p < 10^{-100}$).
   - **Intra-State Depot Fulfillment Advantage:** Cross-state long-haul shipments exhibit **9.281%** late rate vs. **6.065%** for same-state fulfillment ($Z = 17.53$, $p < 10^{-60}$).
   - **Last-Mile Transit Bottleneck:** Welch's $t$-test confirms breached shipments average **25.685 days** in last-mile transit vs. **7.889 days** on-time ($t = 88.29$).
   - **Walk-Forward Demand Forecasting (`65` holdout weeks):** Expanding-window EWMA + AR(1) blend achieves **13.781% MAPE** vs. **16.473%** on the 4-week rolling baseline (**16.34% forecast error reduction**).
4. **Tableau, Power BI & Excel Executive Reporting**
   - Exports `outputs/tableau_last_mile_lane_kpis.csv`, `outputs/tableau_weekly_capacity_forecast.csv`, and formula-driven `outputs/last_mile_capacity_dq_scorecard.xlsx`.

## Public Data Sources

- **Primary Dataset:** Olist Multi-Domain E-Commerce Orders, Items, Depots (`sellers`), and Destinations (`customers`) (`314,627` records across 4 relational CSVs, `https://github.com/olist/work-at-olist-data`).
- **Backup Dataset:** DataCo Smart Supply Chain for Big Data Analysis (`180,519` logistics order & delivery records, `https://data.mendeley.com/datasets/8gx2fvg2k6/5`).

## Quickstart

```bash
python3 run_pipeline.py
python3 -m unittest discover -s tests -v
```
