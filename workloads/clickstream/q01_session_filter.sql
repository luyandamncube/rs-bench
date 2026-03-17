-- workloads\clickstream\q01_session_filter.sql
SELECT COUNT(*) AS row_count
FROM ${table_name}
WHERE country_code = 'US';