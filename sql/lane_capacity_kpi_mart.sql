WITH lane_base AS (
        SELECT
            lane_id,
            origin_state,
            dest_state,
            COUNT(*) AS shipment_count,
            SUM(item_count) AS total_units,
            ROUND(SUM(order_gmv), 2) AS total_gmv,
            ROUND(SUM(freight_cost), 2) AS total_freight,
            ROUND(AVG(handoff_days), 3) AS avg_handoff_days,
            ROUND(AVG(last_mile_days), 3) AS avg_last_mile_days,
            ROUND(AVG(total_cycle_days), 3) AS avg_total_cycle_days,
            ROUND(AVG(sla_variance_days), 3) AS avg_sla_variance_days,
            SUM(is_late) AS late_shipments,
            ROUND(100.0 * SUM(is_late) / COUNT(*), 3) AS late_rate_pct
        FROM fct_last_mile_shipments
        GROUP BY lane_id, origin_state, dest_state
        HAVING COUNT(*) >= 50
    )
    SELECT
        lane_id,
        origin_state,
        dest_state,
        shipment_count,
        total_units,
        total_gmv,
        total_freight,
        avg_handoff_days,
        avg_last_mile_days,
        avg_total_cycle_days,
        avg_sla_variance_days,
        late_shipments,
        late_rate_pct,
        RANK() OVER (ORDER BY late_rate_pct DESC, shipment_count DESC) AS escalation_priority_rank
    FROM lane_base
    ORDER BY shipment_count DESC;
