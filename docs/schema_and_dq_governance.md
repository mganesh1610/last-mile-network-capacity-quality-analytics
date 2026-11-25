# Schema & Data Quality Governance Specification

## Star Schema Tables

- `dim_depot(seller_id PK, origin_city, origin_state)` — 3,095 fulfillment origin nodes
- `dim_destination(customer_id PK, dest_city, dest_state)` — 99,441 destination delivery records
- `fct_last_mile_shipments(order_id PK, customer_id FK, seller_id FK, origin_state, dest_state, lane_id, purchase_ts, week_key, month_key, item_count, order_gmv, freight_cost, handoff_days, last_mile_days, total_cycle_days, promised_days, sla_variance_days, is_late, is_multi_depot)` — 96,281 validated shipments
- `dq_quarantine_log(order_id PK, order_status, dq_reason)` — 3,160 quarantined exception rows

## Automated Governance Gates (10/10 Passed)

1. `sha256_raw_source_verification`
2. `sqlite_integrity_check_ok`
3. `foreign_key_referential_integrity`
4. `order_row_conservation_100pct`
5. `chronology_non_negative_transit`
6. `zero_null_primary_foreign_keys`
7. `non_empty_quarantine_audit_log`
8. `lane_mart_minimum_support_threshold`
9. `walk_forward_leakage_free_horizon`
10. `forecast_beats_naive_baseline`
