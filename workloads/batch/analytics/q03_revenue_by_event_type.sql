-- workloads\clickstream\q03_revenue_by_event_type.sql
SELECT event_type, SUM(revenue) AS total_revenue
FROM ${table_name}
GROUP BY event_type
ORDER BY total_revenue DESC;