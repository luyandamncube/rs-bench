-- workloads\clickstream\q04_funnel_agg.sql
SELECT
    device_type,
    COUNT(DISTINCT session_id) AS session_count,
    SUM(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END) AS page_view_count,
    SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS add_to_cart_count,
    SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchase_count,
    SUM(CASE WHEN event_type = 'purchase' THEN revenue ELSE 0 END) AS purchase_revenue
FROM ${table_name}
GROUP BY device_type
ORDER BY purchase_revenue DESC, purchase_count DESC, device_type ASC;
