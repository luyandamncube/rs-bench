-- workloads\clickstream\q05_sessionized_topn.sql
SELECT
    country_code,
    device_type,
    COUNT(*) AS event_count,
    COUNT(DISTINCT session_id) AS session_count,
    AVG(latency_ms) AS avg_latency_ms,
    SUM(revenue) AS total_revenue
FROM ${table_name}
GROUP BY country_code, device_type
HAVING COUNT(*) >= 20
ORDER BY total_revenue DESC, session_count DESC, avg_latency_ms ASC
LIMIT 10;
