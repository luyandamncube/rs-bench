-- workloads\clickstream\q02_events_by_device.sql
SELECT device_type, COUNT(*) AS event_count
FROM ${table_name}
GROUP BY device_type
ORDER BY event_count DESC;