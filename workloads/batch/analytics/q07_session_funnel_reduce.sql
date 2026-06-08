-- workloads\clickstream\q07_session_funnel_reduce.sql
WITH session_facts AS (
    SELECT
        session_id,
        country_code,
        device_type,
        MAX(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END) AS saw_page_view,
        MAX(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS saw_add_to_cart,
        MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS saw_purchase,
        SUM(CASE WHEN event_type = 'purchase' THEN revenue ELSE 0 END) AS session_revenue
    FROM ${table_name}
    GROUP BY session_id, country_code, device_type
)
SELECT
    country_code,
    device_type,
    COUNT(*) AS session_group_count,
    SUM(saw_page_view) AS sessions_with_page_view,
    SUM(saw_add_to_cart) AS sessions_with_add_to_cart,
    SUM(saw_purchase) AS sessions_with_purchase,
    SUM(session_revenue) AS purchase_revenue
FROM session_facts
GROUP BY country_code, device_type
ORDER BY purchase_revenue DESC, sessions_with_purchase DESC, country_code ASC, device_type ASC;
